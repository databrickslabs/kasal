"""Deep Research mode → what the generated crew actually does differently.

``deep`` differed from ``research`` by exactly one string — ``reasoning_effort``
"high" versus "medium" — and on a model without a native reasoning budget the
two were byte-identical. Everything else the mode implies was either not built
or built and never switched on:

* A guardrail was GENERATED for every task (the templates require one, the
  generator persists it, the executor is fully wired to consume it) and then
  dropped on the floor by both crew-config builders, so deep runs executed
  ungated against a criterion already written for them.
* No execution caps of any kind were set, so every mode ran on identical bare
  engine defaults.

**Deep does not reconfigure tools.** It briefly did: the mode carried the
catalog id ``"31"`` and repointed ``PerplexityTool`` at ``sonar-deep-research``.
That was removed, and the removal is the point of this note. Two reasons, and
the second is the expensive one:

* **Vendor knowledge does not belong in a mode.** A hardcoded id and class name
  meant exactly one tool got the treatment, every other research tool got
  nothing, and a reseed that renumbered the catalog would have disabled it in
  silence. If a tool has a slower, deeper setting, that fact belongs with the
  tool — where the parameter names are, and where a second tool can declare it
  without editing this file.
* **It was the cause of the runaway deep runs.** ``sonar-deep-research`` answers
  in minutes rather than seconds, so a single round fanning out to a handful of
  searches blew the per-call budget several times over; the overrun was then
  retried, and each retry paid it again. Deep now uses whatever the tool is
  configured with, exactly as research does.

What still separates deep from research is the crew shape, the reasoning
budget, the execution caps below, the generated guardrail, and the envelope
contract — none of which name a vendor.

**Scope: this touches ``deep`` and NOTHING else.** ``chat`` and ``research``
keep exactly the behaviour they have today, including their bare engine
defaults and their ungated tasks. The original proposal applied the guardrail
and budget work to research as well; that was deliberately narrowed, because
deep is the mode being changed and adding a judge call plus retries to research
would slow a mode nobody asked to change. Widening later is a one-line edit to
``GATED_MODES`` — but it IS a behaviour change to research, so make it on
purpose.

Kept as a separate module because ``generation/crews.py`` is already over the
file-size ceiling.
"""

import copy
import logging
from typing import Any, Dict, List, Optional

from src.schemas.deep_research import (
    DEEP_RESEARCH_ENVELOPE_SCHEMA,
    DEFAULT_DEEP_GATE,
)
from src.services.execution.config.budget_profile import resolve_budget_profile

logger = logging.getLogger(__name__)

#: The only mode this pass touches. See the scope note in the module docstring
#: before adding to it — every other mode keeps today's behaviour exactly.
GATED_MODES = ("deep",)

#: Marker left on a decorated task so a second pass is a no-op.
#:
#: This runs from TWO places on purpose: generation (so the persisted config
#: shows what the run will actually do) and execution config adaptation (so a
#: crew re-run from the UI, which never goes through generation, is gated
#: identically). Without the marker the second pass would append the contract to
#: the description a second time.
_APPLIED_MARKER = "_answer_mode"


def apply_answer_mode(
    mode: Optional[str],
    agents_yaml: Dict[str, Dict[str, Any]],
    tasks_yaml: Dict[str, Dict[str, Any]],
    generated_tasks: Dict[str, Dict[str, Any]],
) -> None:
    """Apply the answer mode's behaviour to a generated crew config, in place.

    ``generated_tasks`` maps the ``task_<id>`` key to the raw generated task, so
    the fields the builder drops (notably ``llm_guardrail``) can be recovered
    without re-querying.
    """
    normalized = (mode or "chat").strip().lower()
    if normalized not in GATED_MODES:
        return

    pending = {
        key: entry
        for key, entry in tasks_yaml.items()
        if entry.get(_APPLIED_MARKER) != normalized
    }
    if not pending:
        return

    profile = resolve_budget_profile(normalized)

    for spec in agents_yaml.values():
        _apply_agent_budget(spec, profile)

    for key, entry in pending.items():
        source = generated_tasks.get(key) or {}
        _apply_task_verification(entry, source, profile)
        entry[_APPLIED_MARKER] = normalized

    logger.info(
        "Deep Research: %d task(s) gated, caps max_iter=%d "
        "max_execution_time=%ds run_wall_clock=%ds retries=%d",
        len(pending),
        profile.max_iter,
        profile.max_execution_time,
        profile.run_wall_clock,
        profile.guardrail_max_retries,
    )


def _apply_agent_budget(spec: Dict[str, Any], profile) -> None:
    """Per-agent caps: the mode's numbers are a FLOOR, not a default.

    This was ``setdefault`` — "an explicit value in the plan is a decision, and
    a mode default should not overrule it". True for a value the plan raises;
    wrong for one that lowers it. A crew re-run from the UI carries whatever
    ``max_execution_time`` its saved agents hold (the form ships 300), and that
    silently undercut deep mode's budget. The agent had been configured before
    anyone chose an answer mode; choosing deep is the later, more specific
    instruction.

    So: raise, never lower. A plan asking for MORE than the mode still wins.
    """
    for field in ("max_iter", "max_execution_time"):
        spec[field] = max(_positive(spec.get(field)), getattr(profile, field))


def _positive(value: Any) -> int:
    """``value`` as a positive int, or 0 when it is absent or unusable.

    0 is the identity for ``max`` here, so a missing, null, non-numeric or
    nonsensical (<= 0) value falls through to the profile rather than raising
    mid-generation over a field the user cannot see.
    """
    try:
        number = int(value)
    except (TypeError, ValueError):
        return 0
    return number if number > 0 else 0


def _apply_task_verification(
    entry: Dict[str, Any],
    source: Dict[str, Any],
    profile,
) -> None:
    """Turn on the verification that was already generated for this task."""
    # THE two-line fix the audit found: the guardrail exists, has always
    # existed, and was simply never copied into the task entry.
    guardrail = source.get("llm_guardrail")
    if guardrail:
        entry["llm_guardrail"] = guardrail

    entry["max_retries"] = profile.guardrail_max_retries

    # Degrade rather than abort. Losing a six-task run because task four could
    # not satisfy a judge on the third attempt — or because one agent blew its
    # clock — throws away everything already produced. The failure stays
    # visible: the output is annotated, and the guardrail/budget events fire
    # either way.
    entry["guardrail_on_exhausted"] = "degrade"
    entry["on_budget_exceeded"] = "degrade"

    # Every deep task answers in the same envelope, and a free, deterministic
    # rule on that envelope decides whether the next task may proceed.
    entry["output_schema"] = copy.deepcopy(DEEP_RESEARCH_ENVELOPE_SCHEMA)
    entry["output_schema_name"] = "DeepResearchEnvelope"
    entry["gate"] = copy.deepcopy(DEFAULT_DEEP_GATE)
    entry["description"] = _with_envelope_instruction(entry.get("description", ""))
    # output_file defaults to a .md path; with a JSON contract the artifact is
    # JSON, and the engine writes json_dict when output_json is set.
    output_file = entry.get("output_file")
    if isinstance(output_file, str) and output_file.endswith(".md"):
        entry["output_file"] = output_file[:-3] + ".json"


def _with_envelope_instruction(description: str) -> str:
    """Tell the agent about the contract up front.

    The engine appends the schema to ``expected_output`` at execution time, but
    that is a formatting instruction. This is the part that changes the WORK:
    an agent that knows every claim needs a source gathers sources as it goes
    instead of reverse-engineering citations at the end to satisfy a gate.
    """
    return (
        f"{description}\n\n"
        "=== DEEP RESEARCH CONTRACT ===\n"
        "Answer as a JSON object with `summary`, `findings`, `open_questions` "
        "and `limitations`.\n"
        "Every entry in `findings` needs a specific `claim`, the `evidence` "
        "behind it, the `source` it came from (a URL or tool result you "
        "actually consulted), and your `confidence` from 0 to 1.\n"
        "Do not invent sources. If you cannot source a claim, leave it out and "
        "record the gap in `open_questions` instead."
    ).strip()
