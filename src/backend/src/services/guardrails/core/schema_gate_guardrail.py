"""Gate 1 — the output really is JSON matching the schema.

Without this, a task told to emit JSON that emits prose simply proceeds: the
engine's ``structured_from_raw`` returns ``None`` on an unparseable or invalid
answer, ``_shape_output`` logs a warning and falls back to ``OutputFormat.RAW``,
and the next task consumes the prose. No exception, no event, no retry — the
contract exists only as an instruction in the prompt.

This guardrail makes it a contract. It is the cheapest check in the stack (no
LLM call, no I/O), so it runs first: an expensive judge should never grade
output that a free parse would have rejected.

The rejection message names what was wrong — missing field, wrong type, not JSON
at all — because the engine replays guardrail feedback into the next attempt,
and "expected JSON matching <schema>" produces a better retry than "invalid".
"""

import json
import logging
from typing import Any, Dict, Optional, Type

from pydantic import BaseModel, ValidationError

from src.services.guardrails.base_guardrail import BaseGuardrail

logger = logging.getLogger(__name__)

#: How much of the offending output to quote back. Enough for the model to see
#: what it did (a markdown fence, a preamble sentence), not enough to blow up
#: the retry prompt with the whole rejected answer — which the engine already
#: appends separately.
_ECHO_CHARS = 400


class SchemaGateGuardrail(BaseGuardrail):
    """Reject task output that is not a valid instance of ``model``."""

    def __init__(self, model: Type[BaseModel], config: Optional[Dict[str, Any]] = None):
        super().__init__(config or {})
        self.model = model
        self.description = f"output matches the {model.__name__} schema"

    def validate(self, output: Any) -> Dict[str, Any]:
        raw = _text_of(output)
        if not raw.strip():
            return _fail("the output was empty; return a JSON object", self.model)

        parsed, parse_error = _parse_json(raw)
        if parse_error is not None:
            return _fail(
                f"the output was not valid JSON ({parse_error}). "
                f"Return ONLY the JSON object — no prose, no markdown fences. "
                f"Received: {raw[:_ECHO_CHARS]!r}",
                self.model,
            )

        try:
            self.model.model_validate(parsed)
        except ValidationError as exc:
            return _fail(_describe(exc), self.model)

        return {"valid": True, "feedback": ""}


def _text_of(output: Any) -> str:
    """The task's text, whatever shape the output object is."""
    if isinstance(output, str):
        return output
    for attribute in ("raw", "content", "output"):
        value = getattr(output, attribute, None)
        if isinstance(value, str):
            return value
    return str(output)


def _parse_json(raw: str) -> tuple[Any, Optional[str]]:
    """Parse, tolerating the two things models do to JSON in practice: wrapping
    it in a markdown fence, and putting a sentence in front of it.

    Tolerated at the GATE only. The engine's own parse is what decides whether
    ``json_dict`` gets populated, so being lenient here would pass output the
    engine then fails to shape — the leniency below is limited to stripping
    wrappers, never to accepting a different shape.
    """
    text = raw.strip()
    if text.startswith("```"):
        fence_end = text.find("\n")
        text = text[fence_end + 1 :] if fence_end != -1 else text[3:]
        if text.rstrip().endswith("```"):
            text = text.rstrip()[:-3]
        text = text.strip()

    try:
        return json.loads(text), None
    except json.JSONDecodeError as exc:
        start, end = text.find("{"), text.rfind("}")
        if start != -1 and end > start:
            try:
                return json.loads(text[start : end + 1]), None
            except json.JSONDecodeError:
                pass
        return None, str(exc)


def _describe(exc: ValidationError) -> str:
    """Turn Pydantic's error list into instructions the model can act on."""
    problems = []
    for error in exc.errors()[:8]:
        location = ".".join(str(part) for part in error["loc"]) or "(root)"
        problems.append(f"{location}: {error['msg']}")
    return "the JSON did not match the required schema — " + "; ".join(problems)


def _fail(reason: str, model: Type[BaseModel]) -> Dict[str, Any]:
    schema = json.dumps(model.model_json_schema(), indent=2)
    return {
        "valid": False,
        "feedback": f"{reason}\n\nRequired schema:\n{schema}",
    }
