"""Routing one chat prompt to an ALREADY PUBLISHED crew or flow.

The "Use existing" half of ChatMode. Everything else in ``chat/`` builds
something new; this picks something that already exists, binds its inputs from
the sentence the user typed, and hands back the same ``execute_crew`` /
``execute_flow`` result the slash-command path produces.

**The route catalog is data, not code.** It is read off the publication registry
(``services/publications/``), filtered to the ``chat`` protocol — so adding a
route means publishing a crew, not shipping a handler, and one router spans every
published crew AND flow in the workspace without a deploy.

``description`` is the only matching signal. ``external_name`` is a wire
identifier, ``input_schema`` is for extraction AFTER the pick, and
``entity_type`` only selects which dispatch branch builds the result — the user
never says "crew" or "flow".

A sibling of ``dispatcher.py`` rather than more surface on it: that file is well
past the size ceiling, and this is a self-contained decision with its own prompt,
its own failure modes and its own tests.

**The prompt is not in this file.** It is the ``route_capability`` row in
``prompt_templates``, seeded from ``seeds/prompt_templates.py`` like every other
prompt in the product — editable in the UI, overridable per group, and
optimizable by GEPA (wired in ``prompt_optimization/config.py`` against the
``capability-route`` llmlog endpoint). What lives here is the mechanical part:
what the model is shown, and what is done with what it says. Those are the parts
an optimiser must not be able to change.

The hard part is not picking — it is NOT INVENTING
=================================================

Extraction models fill plausible gaps rather than reporting absence. "Run the
risk review for DACH" with no quarter mentioned will frequently come back with
``quarter: "Q3"``, because Q3 is plausible. Nothing looks missing, no card
renders, the run completes cleanly, and the answer is for the wrong quarter.
That is worse than a missing value because it is invisible.

Three mitigations, and only the second and third are mechanical:

1. the prompt says emit ``null``, do not infer, do not default;
2. every value must arrive with a ``source_span`` — the model must QUOTE the text
   it took the value from — and a span that is not in the message drops the
   value (``_span_is_quoted``). This is what turns "do not fabricate" from an
   instruction into a guarantee;
3. a field the capability did not declare is dropped outright, so an invented
   VARIABLE is impossible even if an invented value slips through.

The router never supplies a default. Defaults come only from a flow's
``StateConfig.initialValues``, where they are authored and auditable.
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from src.schemas.crew_publication import PublishedCapability

# The row this prompt normally comes from is seeded from this constant. Imported
# as the fallback rather than copied — see build_route_messages.
from src.seeds.prompt_templates import ROUTE_CAPABILITY_TEMPLATE
from src.utils.prompt_utils import robust_json_parser

logger = logging.getLogger(__name__)

#: The protocol a publication must carry to be chat-routable. Publishing to MCP
#: or A2A does not make something reachable from a chat prompt, and vice versa.
CHAT_PROTOCOL = "chat"

#: Below this the router declines to pick and offers to build instead.
#:
#: A guess until Phase 5 produces trial data. It is deliberately high: with
#: "Use existing" selected the user asked to run something they already have, so
#: the cost of declining is one extra click, while the cost of a confident wrong
#: pick is a full crew run against the wrong capability.
ROUTE_CONFIDENCE_THRESHOLD = 0.7

_WHITESPACE = re.compile(r"\s+")


@dataclass
class RouteDecision:
    """What the router concluded, before anything is resolved or run."""

    #: The ``external_name`` picked, or None when nothing matched well enough.
    capability: Optional[str]
    confidence: float
    #: Field name -> value, already span-validated and filtered to declared
    #: fields. A field the user did not state is ABSENT, never guessed.
    inputs: Dict[str, Any] = field(default_factory=dict)
    #: One sentence from the model, for the log and for Phase 5's trials table.
    reason: str = ""
    #: The [answer N] this request works FROM, when it acts on an earlier one
    #: ("turn this into a deck"). None for a fresh request — and None whenever
    #: the model named something that is not an assistant turn in the window it
    #: was shown, for the same reason an unquoted value is dropped: a number it
    #: could not have read is a number it made up.
    refers_to: Optional[int] = None
    #: Values dropped by validation, with why. Empty on a clean route; anything
    #: in here is a prompt bug worth seeing, not a user error.
    dropped: Dict[str, str] = field(default_factory=dict)

    @property
    def is_confident(self) -> bool:
        return self.capability is not None and self.confidence >= (
            ROUTE_CONFIDENCE_THRESHOLD
        )


#: Confidence attached to a turn that continues a capability already holding the
#: conversation. Deliberately just over the threshold rather than 1.0: this is a
#: structural inference — "the previous answer came from a capability that
#: expects the next turn" — not a semantic match the model made, and a trials
#: table should be able to tell the two apart.
CONTINUATION_CONFIDENCE = ROUTE_CONFIDENCE_THRESHOLD


def held_conversation(
    turns: List[Any], capabilities: List[PublishedCapability]
) -> Optional[PublishedCapability]:
    """The capability currently holding this conversation, if any.

    The most recent assistant turn decides it. An older one does not: once the
    user has been answered by something else — or by the chat itself — the
    conversation has moved, and dragging it back would be worse than declining.

    Returns None unless that capability still exists, is still visible to this
    group, and still declares itself conversational. All three are read fresh
    from the catalogue, so unpublishing a flow or turning its conversation off
    takes effect on the next turn rather than being remembered from history.
    """
    last_answer = next(
        (turn for turn in reversed(turns) if getattr(turn, "role", "") == "assistant"),
        None,
    )
    name = getattr(last_answer, "capability", None) if last_answer else None
    if not name:
        return None
    for capability in capabilities:
        if capability.name == name and getattr(capability, "conversational", False):
            return capability
    return None


def continue_decision(capability: PublishedCapability, message: str) -> "RouteDecision":
    """Route this turn to the capability already holding the conversation.

    No inputs are extracted. The turn's text reaches the flow as its user
    message — that is what a conversational flow reads — and inventing input
    values from a fragment is exactly the guessing the extraction rules forbid.
    A value the flow needs and does not have is asked for, as always.
    """
    return RouteDecision(
        capability=capability.name,
        confidence=CONTINUATION_CONFIDENCE,
        inputs={},
        reason="continues the conversation this capability is already holding",
    )


def declared_fields(capability: PublishedCapability) -> Tuple[List[str], List[str]]:
    """``(all field names, required field names)`` a capability declares.

    Reads ``input_schema``, which is authored in the publish dialog. Returns
    ``([], [])`` when there is none — every publication created before that
    editor existed is in that state, and the honest consequence is that the
    router extracts nothing for it and the user is asked for each variable. That
    is a worse experience, not a wrong answer, and re-publishing fixes it.

    An ABSENT ``required`` array is not an empty one: absent means nobody has
    said, so everything counts as required; empty means the publisher said
    nothing is.
    """
    schema = capability.input_schema or {}
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        return [], []
    names = [str(k) for k in properties]
    required = schema.get("required")
    if required is None:
        return names, list(names)
    if not isinstance(required, list):
        return names, []
    return names, [str(r) for r in required if str(r) in names]


def render_route_catalog(capabilities: List[PublishedCapability]) -> str:
    """The candidate list, as the model sees it.

    Rendered into the USER message rather than the system prompt, the way
    ``detect_intent`` appends its tool catalog. Two reasons, and the second is
    the operative one: the catalog is per-workspace data that changes whenever
    someone publishes, and the system prompt is a GEPA-optimizable template. Bake
    the catalog into the template and every optimisation run would be tuning
    against one workspace's crews.
    """
    lines: List[str] = []
    for index, cap in enumerate(capabilities, start=1):
        names, required = declared_fields(cap)
        lines.append(f"{index}. name: {cap.name}")
        lines.append(f"   description: {cap.description}")
        if getattr(cap, "conversational", False):
            # Says what the capability IS, not what to do about it — the rule
            # for what to do lives in the prompt, where GEPA can tune it.
            lines.append(
                "   holds a conversation: follow-up turns continue this "
                "capability's own state"
            )
        if names:
            properties = (cap.input_schema or {}).get("properties") or {}
            rendered = []
            for name in names:
                spec = properties.get(name) or {}
                note = spec.get("description") if isinstance(spec, dict) else None
                flag = "required" if name in required else "optional"
                rendered.append(f"{name} ({flag})" + (f" — {note}" if note else ""))
            lines.append("   inputs: " + "; ".join(rendered))
        else:
            lines.append("   inputs: none declared")
    return "\n".join(lines)


def build_route_messages(
    message: str,
    capabilities: List[PublishedCapability],
    system_prompt: Optional[str] = None,
    conversation: str = "",
) -> List[Dict[str, str]]:
    """The routing call: an optimizable system prompt, the catalog, the conversation.

    ``system_prompt`` comes from the ``route_capability`` template row, so it is
    editable in the UI and optimizable by GEPA like every other prompt here.
    Falls back to the seed the row is created from — never to a second copy.

    ``conversation`` is what stops a turn being read as though nothing came
    before it. Without it "what is this Aviation sector" is a plausible news
    request and runs a whole crew to answer a question the text on screen
    already answers.
    """
    parts = []
    if conversation:
        # First: the message means what it means IN this conversation, and
        # reading the catalog first invites matching the sentence against
        # descriptions before understanding what was asked.
        parts.append(f"CONVERSATION SO FAR\n{conversation}")
    parts.append(f"CAPABILITIES\n{render_route_catalog(capabilities)}")
    parts.append(f"USER MESSAGE\n{message}")

    return [
        {"role": "system", "content": system_prompt or ROUTE_CAPABILITY_TEMPLATE},
        {"role": "user", "content": "\n\n".join(parts)},
    ]


def _normalize(text: str) -> str:
    """Collapse whitespace and case, for span comparison only."""
    return _WHITESPACE.sub(" ", text).strip().lower()


def _span_is_quoted(span: Any, message: str) -> bool:
    """Is ``span`` genuinely lifted out of ``message``?

    Compared with whitespace collapsed and case folded. A model that quotes
    "DACH" where the user typed "dach", or that normalises a line break to a
    space, has still quoted; failing those would drop good values and teach
    nobody anything. Anything looser and the check stops being a check.
    """
    if not isinstance(span, str) or not span.strip():
        return False
    return _normalize(span) in _normalize(message)


def _as_turn_index(value: Any) -> Optional[int]:
    """``refers_to`` as a usable turn number, or None.

    Anything that is not a positive integer is None rather than an error: the
    caller resolves it against the turns it actually rendered, so a number out
    of range simply binds nothing.
    """
    try:
        index = int(value)
    except (TypeError, ValueError):
        return None
    return index if index > 0 else None


def parse_route_response(
    parsed: Any, message: str, capabilities: List[PublishedCapability]
) -> Optional[RouteDecision]:
    """Model output -> a validated decision, or None if it was unusable.

    Takes the already-parsed object (the model chain hands one back) but accepts
    a raw string too, so a test can hand it exactly what a model emitted.

    Everything that survives here is either quoted from the user's message or
    absent. Nothing is defaulted and nothing is inferred.
    """
    if isinstance(parsed, str):
        try:
            parsed = robust_json_parser(parsed)
        except Exception as exc:
            logger.warning("[capability_router] unparseable routing output: %s", exc)
            return None
    if not isinstance(parsed, dict):
        logger.warning("[capability_router] routing output was not a JSON object")
        return None

    raw_name = parsed.get("capability")
    name = str(raw_name).strip() if isinstance(raw_name, str) else None
    if not name or name.lower() in {"null", "none"}:
        return RouteDecision(
            capability=None,
            confidence=0.0,
            reason=str(parsed.get("reason") or ""),
            refers_to=_as_turn_index(parsed.get("refers_to")),
        )

    match = next((c for c in capabilities if c.name == name), None)
    if match is None:
        # A name that is not in the catalog we just handed it. Not a user error —
        # the prompt or the model is at fault, and it is worth seeing in the log.
        logger.warning(
            "[capability_router] model named %r, which is not in the route catalog",
            name,
        )
        return RouteDecision(capability=None, confidence=0.0, reason="unknown name")

    try:
        confidence = max(0.0, min(1.0, float(parsed.get("confidence", 0.0))))
    except (TypeError, ValueError):
        confidence = 0.0

    allowed, _ = declared_fields(match)
    inputs: Dict[str, Any] = {}
    dropped: Dict[str, str] = {}

    raw_inputs = parsed.get("inputs")
    if isinstance(raw_inputs, dict):
        for key, entry in raw_inputs.items():
            key = str(key)
            if key not in allowed:
                # An invented variable. Impossible to bind, so drop it rather
                # than pass an unknown key downstream where a flow would swallow
                # it silently.
                dropped[key] = "not declared by this capability"
                continue
            if entry is None:
                continue  # The correct answer for "the user did not say".
            if not isinstance(entry, dict):
                dropped[key] = "not a {value, source_span} object"
                continue
            value = entry.get("value")
            if value is None or (isinstance(value, str) and not value.strip()):
                continue
            if not _span_is_quoted(entry.get("source_span"), message):
                dropped[key] = "source_span is not in the user's message"
                continue
            inputs[key] = value

    if dropped:
        logger.warning(
            "[capability_router] dropped %d extracted value(s) for %s: %s",
            len(dropped),
            name,
            dropped,
        )

    return RouteDecision(
        capability=name,
        confidence=confidence,
        inputs=inputs,
        reason=str(parsed.get("reason") or ""),
        dropped=dropped,
        refers_to=_as_turn_index(parsed.get("refers_to")),
    )
