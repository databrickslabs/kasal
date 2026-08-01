"""What a turn is asking the flow to PRODUCE, and the least work that produces it.

Two decisions in a flow get called "routing", and they are not the same thing:

* A **router** asks *"given what the flow computed, which branch is valid?"* It
  reads state, it runs during execution, and it would exist even if flows never
  held a conversation. It is the flow's LOGIC. Nothing here changes it.
* A **outcome** answers *"given what the person asked, what must this turn
  produce?"* It reads the turn's text, it is decided before execution, and it
  exists only because a conversation asks the same graph for different things on
  different turns. It is the flow's ENTRY.

Keeping them apart is the point. Selection tried inside a router cannot work: a
linear flow has no router to hook into, a router that already HAS conditions
could never select, and selection concerns the whole graph while a router sees
only its own children.

The model
=========

A **outcome** is a crew that produces something a person would ask for — the
terminal crews by default, plus any the author marks. Everything else is
**material**: work that exists to feed an outcome.

A turn is then a build request::

    turn: "now turn that into a mindmap"
      target   = mindmap                      (selection)
      required = mindmap + its ancestors      (the graph)
      gather   ✓ in state -> reused
      features ✓ in state -> reused
      mindmap  ✗          -> runs

Everything outside ``required`` never fires. Everything inside it that is
already in state, with a matching content hash, is reused. Routers inside it
route exactly as they do now.
"""

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

#: Below this the selection is a decline, and the turn runs the whole flow.
#: A slow correct answer beats a fast one to a question nobody asked.
OUTCOME_CONFIDENCE_THRESHOLD = 0.6

#: DB-backed prompt, seeded like every other one so a group can override it and
#: GEPA can optimise it.
OUTCOME_TEMPLATE = "select_flow_outcome"


@dataclass
class OutcomeChoice:
    """What a turn decided to do: produce something, or nothing at all.

    Three answers, not two. "Produce X" narrows the run. "I could not tell"
    runs the whole flow. And "this needs no work" is the one the design was
    missing: a turn asking ABOUT what an earlier turn produced should cost
    nothing, and until now it cost a crew.
    """

    outcome: Optional[str] = None
    confidence: float = 0.0
    reason: str = ""
    #: The turn is answerable from what the flow already holds. No crew runs.
    answer_from_state: bool = False


def crew_entries(flow_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Every crew in the flow, as the config describes it."""
    if not isinstance(flow_config, dict):
        return []
    entries = list(flow_config.get("listeners") or []) + list(
        flow_config.get("startingPoints") or []
    )
    return [e for e in entries if isinstance(e, dict) and e.get("crewName")]


def terminal_crews(flow_config: Dict[str, Any]) -> Set[str]:
    """Crews nothing else listens to.

    The single definition of "this crew's output is the end of a branch". Both
    outcome selection (what a turn may ask for) and reuse (what must never be
    replayed) depend on it, and two implementations would eventually disagree.
    """
    entries = crew_entries(flow_config)
    task_owner: Dict[str, str] = {}
    for entry in entries:
        for task in entry.get("tasks") or []:
            if isinstance(task, dict) and task.get("id"):
                task_owner[str(task["id"])] = str(entry["crewName"])
        if entry.get("taskId"):
            task_owner[str(entry["taskId"])] = str(entry["crewName"])

    feeds_someone: Set[str] = set()
    for entry in entries:
        for task_id in entry.get("listenToTaskIds") or []:
            owner = task_owner.get(str(task_id))
            if owner:
                feeds_someone.add(owner)
    return {str(e["crewName"]) for e in entries} - feeds_someone


def outcome_crews(flow_config: Dict[str, Any]) -> Set[str]:
    """The crews a turn may ask for.

    Default: the crews nothing listens to. A crew feeding another exists to
    produce material for it, so asking for it directly is rarely what a person
    means; a crew at the end of a branch is the artefact.

    An explicit ``outcome: true`` on a crew overrides the default, because a flow
    can legitimately have a mid-graph crew that is worth asking for on its own.
    """
    entries = crew_entries(flow_config)

    # Writing a line about what a crew delivers IS marking it askable — a
    # separate "this is an outcome" flag would be a second thing to remember
    # that says nothing the description does not already say.
    authored = flow_config.get("outcomes") if isinstance(flow_config, dict) else None
    described = {
        str(name) for name, text in (authored or {}).items() if str(text or "").strip()
    }
    if described:
        return described

    return terminal_crews(flow_config)


def outcome_descriptions(flow_config: Dict[str, Any]) -> Dict[str, str]:
    """What each outcome produces, in words.

    From an explicit ``outcomeDescription`` when the author wrote one, else from the
    crew's task text — the only other place a flow says what a crew is for.
    Truncated: this is read to tell outcomes apart, not to study them.
    """
    # Written on the flow's publish page, one line per crew IN THIS FLOW.
    # Deliberately per-flow rather than per-crew: the same crew can deliver
    # something narrower as a step here than it does on its own, and requiring
    # each crew to be PUBLISHED just to describe it would fill the routing
    # catalogue with steps nobody should call directly — and make "documented"
    # mean "exposed".
    authored = flow_config.get("outcomes") if isinstance(flow_config, dict) else None
    authored = authored if isinstance(authored, dict) else {}

    described: Dict[str, str] = {}
    for entry in crew_entries(flow_config):
        name = str(entry["crewName"])
        explicit = str(
            authored.get(name) or entry.get("outcomeDescription") or ""
        ).strip()
        if explicit:
            described[name] = explicit
            continue
        parts = []
        for task in entry.get("tasks") or []:
            if not isinstance(task, dict):
                continue
            title = str(task.get("name") or "").strip()
            # `expected_output` says what comes OUT; `description` says how the
            # work is done. For telling outcomes apart, the former is the
            # signal — but it is per-TASK, so a multi-task crew reads as a list
            # of steps. Derived, and weaker than a line someone wrote.
            detail = " ".join(
                str(
                    task.get("expected_output") or task.get("description") or ""
                ).split()
            )[:200]
            parts.append(
                f"{title}: {detail}" if title and detail else (title or detail)
            )
        described[name] = " | ".join(p for p in parts if p)
    return described


def render_outcomes(outcomes: Set[str], descriptions: Dict[str, str]) -> str:
    """The outcomes, as the model sees them."""
    lines: List[str] = []
    for index, outcome in enumerate(sorted(outcomes), start=1):
        lines.append(f"{index}. outcome: {outcome}")
        lines.append(
            f"   produces: {(descriptions.get(outcome) or '').strip() or '(not described)'}"
        )
    return "\n".join(lines)


def build_outcome_messages(
    question: str,
    outcomes: Set[str],
    descriptions: Dict[str, str],
    system_prompt: str,
    recent: str = "",
) -> List[Dict[str, str]]:
    """The outcomes go in the USER message; the rules stay in the template.

    Same split as the chat capability router, for the same reason: the outcomes are
    per-flow data, and baking them into the template would tune every
    optimisation run against one flow.
    """
    return [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": (
                f"Outcomes:\n{render_outcomes(outcomes, descriptions)}\n\n"
                + (f"The conversation so far:\n{recent}\n\n" if recent else "")
                + f"The turn:\n{question}"
            ),
        },
    ]


def parse_outcome(raw: Any, outcomes: Set[str]) -> OutcomeChoice:
    """``(outcome, confidence, why)`` — or ``(None, …)`` to run the whole flow.

    An outcome the flow does not have is discarded rather than trusted: naming
    something that does not exist would otherwise filter the graph down to
    nothing and the turn would produce no answer at all.
    """
    payload = raw
    if isinstance(raw, str):
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if not match:
            return OutcomeChoice(reason="no JSON in the response")
        try:
            payload = json.loads(match.group(0))
        except json.JSONDecodeError as exc:
            return OutcomeChoice(reason=f"unparseable response: {exc}")
    if not isinstance(payload, dict):
        return OutcomeChoice(reason="response was not an object")

    outcome = payload.get("outcome")
    reason = str(payload.get("reason") or "")
    try:
        confidence = float(payload.get("confidence", 0.0))
    except (TypeError, ValueError):
        confidence = 0.0

    # Retrieval first: it is the only answer that means "run nothing", and a
    # model naming both an outcome and retrieval is telling us the turn is
    # already answered.
    if payload.get("answer_from_state") and confidence >= OUTCOME_CONFIDENCE_THRESHOLD:
        return OutcomeChoice(
            confidence=confidence,
            reason=reason or "answerable from what the flow already holds",
            answer_from_state=True,
        )

    if not outcome or str(outcome) not in {str(o) for o in outcomes}:
        return OutcomeChoice(
            confidence=confidence,
            reason=reason or f"named an outcome that does not exist: {outcome!r}",
        )
    if confidence < OUTCOME_CONFIDENCE_THRESHOLD:
        return OutcomeChoice(
            confidence=confidence,
            reason=reason or "not confident enough to narrow the turn",
        )
    return OutcomeChoice(str(outcome), confidence, reason)


def render_recent(messages: Any, limit: int = 6) -> str:
    """The last few turns, so a fragment can be matched.

    "make that a quiz" names its artefact and matches on its own. "and for
    Germany?" and "shorter" name nothing, and without what came before they
    match nothing — the selection declines and the whole flow runs. The
    conversation is the difference between narrowing a follow-up and giving up
    on it.
    """
    if not isinstance(messages, list) or not messages:
        return ""
    lines = []
    for entry in messages[-limit:]:
        if not isinstance(entry, dict):
            continue
        role = str(entry.get("role") or "?")
        text = " ".join(str(entry.get("content") or "").split())[:200]
        if text:
            lines.append(f"{role}: {text}")
    return "\n".join(lines)


async def select_outcome(
    question: str,
    flow_config: Dict[str, Any],
    group_context: Any = None,
    model: Optional[str] = None,
    recent: Any = None,
) -> OutcomeChoice:
    """Which outcome this turn wants, or None to run the flow as it runs today.

    Every failure returns None: no question, one outcome, no template, no model, an
    unusable answer. Declining costs time; choosing wrongly produces a confident
    answer to a question nobody asked.
    """
    outcomes = outcome_crews(flow_config)
    if not question or not outcomes:
        return OutcomeChoice(reason="nothing to narrow")

    try:
        from src.services.catalog.templates import TemplateService

        system_prompt = await TemplateService.get_effective_template_content(
            OUTCOME_TEMPLATE, group_context
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("[flow-outcome] prompt unavailable: %s", exc)
        return OutcomeChoice(reason="prompt unavailable")

    try:
        from src.services.llm.manager import LLMManager

        response = await LLMManager.completion(
            messages=build_outcome_messages(
                question,
                outcomes,
                outcome_descriptions(flow_config),
                system_prompt,
                render_recent(recent),
            ),
            model=model,
        )
        content = (
            response["choices"][0]["message"]["content"]
            if isinstance(response, dict)
            else str(response)
        )
    except Exception as exc:  # noqa: BLE001 — an outage must not narrow the flow
        logger.warning("[flow-outcome] selection call failed: %s", exc)
        return OutcomeChoice(reason="model unavailable")

    choice = parse_outcome(content, outcomes)
    logger.info(
        "[flow-outcome] turn=%r -> outcome=%s answer_from_state=%s (%.2f) %s",
        (question or "")[:80],
        choice.outcome,
        choice.answer_from_state,
        choice.confidence,
        choice.reason,
    )
    return choice


def build_registry(
    method_crews: Dict[str, str], identities: Dict[str, str]
) -> Dict[str, Dict[str, Any]]:
    """``crew -> {method, identity}``: what to trigger, and what it WAS.

    Binding an outcome to a crew by name alone is not enough. A name is stable
    while everything behind it changes — swap the tasks, re-model the agents,
    and the same name now delivers something else. The identity is a content
    hash of exactly those things, so an outcome carries proof of the crew it was
    described against.

    That is what makes the trigger trustworthy rather than merely fast: a stored
    answer is replayed only when the hash still matches, and a crew edited
    mid-conversation re-runs instead of serving work produced by a different
    crew under the same name.
    """
    registry: Dict[str, Dict[str, Any]] = {}
    for method, crew in (method_crews or {}).items():
        registry[str(crew)] = {
            "method": method,
            "identity": (identities or {}).get(str(crew)),
        }
    return registry


def trigger_for(registry: Dict[str, Dict[str, Any]], outcome: str) -> Optional[str]:
    """The one method that produces this outcome."""
    entry = (registry or {}).get(str(outcome)) or {}
    return entry.get("method")


def identity_of(registry: Dict[str, Dict[str, Any]], crew: str) -> Optional[str]:
    """The hash of the crew as it was when the flow was compiled."""
    return ((registry or {}).get(str(crew)) or {}).get("identity")


def methods_for_crews(method_crews: Dict[str, str], crew_names: Set[str]) -> Set[str]:
    """The generated methods that run the given crews."""
    return {
        method for method, crew in (method_crews or {}).items() if crew in crew_names
    }
