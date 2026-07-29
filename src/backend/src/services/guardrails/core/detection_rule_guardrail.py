"""Gate 2 — a declarative rule over the parsed output.

Gate 1 proves the answer is the right SHAPE. This proves it is not empty,
thin, or uncited — an envelope with zero findings and a one-line summary parses
perfectly and says nothing.

Three properties, each learned from what the existing machinery gets wrong:

* **Deterministic and free.** No LLM call. A gate that costs a judge call per
  task will be switched off the first time someone looks at the bill.
* **Explainable.** A failure reads "findings has 2 items, needs at least 3;
  findings[1].source is empty" — the engine replays that into the next attempt,
  and a specific objection is the only reason a retry beats the first try.
* **A small closed vocabulary.** ``min_items``, ``max_items``, ``not_empty``,
  ``min``, ``max``, ``min_length``, ``one_of``, ``matches``. Anything that needs
  judgement belongs in the LLM judge above it, not in a rule language slowly
  growing into one.
"""

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from src.services.guardrails.base_guardrail import BaseGuardrail

logger = logging.getLogger(__name__)

#: Sentinel for "the path does not exist", distinct from a present ``None``.
_MISSING = object()


class DetectionRuleGuardrail(BaseGuardrail):
    """Evaluate a task's ``gate.require`` list against its parsed output."""

    def __init__(self, gate: Dict[str, Any], config: Optional[Dict[str, Any]] = None):
        super().__init__(config or {})
        self.requirements: List[Dict[str, Any]] = [
            r for r in (gate or {}).get("require", []) if isinstance(r, dict)
        ]
        self.description = "output satisfies the task's acceptance rule"

    def validate(self, output: Any) -> Dict[str, Any]:
        if not self.requirements:
            return {"valid": True, "feedback": ""}

        parsed = _parsed_of(output)
        if parsed is None:
            # NOT "defer to the schema gate". Each guardrail in the stack runs
            # its OWN retry loop to completion before the next one starts, so by
            # the time this gate is running the schema gate has finished and is
            # no longer watching. Passing unparsed output here let a task escape
            # BOTH gates by emitting valid JSON on the first attempt and prose
            # on the retry — which is exactly what a live run did.
            logger.warning("detection rule: no structured output to evaluate")
            return {
                "valid": False,
                "feedback": (
                    "the answer was not a JSON object, so its acceptance "
                    "criteria could not be checked. Return ONLY the JSON object "
                    "described by the schema — no prose, no markdown."
                ),
            }

        problems: List[str] = []
        for requirement in self.requirements:
            problems.extend(_check(parsed, requirement))

        if problems:
            return {
                "valid": False,
                "feedback": (
                    "the answer did not meet this task's acceptance criteria — "
                    + "; ".join(problems)
                ),
            }
        return {"valid": True, "feedback": ""}


def _parsed_of(output: Any) -> Optional[Any]:
    """The structured form of the output, if the engine produced one.

    Reads ``json_dict`` / ``pydantic`` rather than re-parsing ``raw`` on
    purpose: this gate should judge exactly what the engine will hand
    downstream, not its own second opinion about the text.
    """
    json_dict = getattr(output, "json_dict", None)
    if isinstance(json_dict, (dict, list)):
        return json_dict
    model = getattr(output, "pydantic", None)
    if model is not None and hasattr(model, "model_dump"):
        return model.model_dump()
    if isinstance(output, (dict, list)):
        return output
    return None


def _resolve(value: Any, path: str) -> List[Tuple[str, Any]]:
    """Resolve a dotted path to ``(label, value)`` pairs.

    ``[*]`` fans out: ``findings[*].source`` on three findings yields three
    pairs labelled ``findings[0].source`` … so a failure can name the offender
    instead of the collection.
    """
    results: List[Tuple[str, Any]] = [("", value)]
    for segment in path.split("."):
        key, iterate = (
            (segment[:-3], True) if segment.endswith("[*]") else (segment, False)
        )
        stepped: List[Tuple[str, Any]] = []
        for label, current in results:
            child = (
                current.get(key, _MISSING) if isinstance(current, dict) else _MISSING
            )
            child_label = f"{label}.{key}" if label else key
            if not iterate:
                stepped.append((child_label, child))
                continue
            if not isinstance(child, list):
                stepped.append((child_label, _MISSING if child is _MISSING else child))
                continue
            for index, item in enumerate(child):
                stepped.append((f"{child_label}[{index}]", item))
        results = stepped
    return results


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple, dict, set)):
        return len(value) == 0
    return False


def _is_number(value: Any) -> bool:
    # bool is an int in Python; ``confidence: true`` is not a confidence.
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_bound(value: Any) -> bool:
    return _is_number(value)


def _check(parsed: Any, requirement: Dict[str, Any]) -> List[str]:
    path = str(requirement.get("path") or "").strip()
    if not path:
        return []

    problems: List[str] = []
    for label, value in _resolve(parsed, path):
        if value is _MISSING:
            problems.append(f"{label} is missing")
            continue
        problems.extend(_apply(label, value, requirement))
    return problems


def _apply(label: str, value: Any, requirement: Dict[str, Any]) -> List[str]:
    problems: List[str] = []

    minimum_items = requirement.get("min_items")
    if isinstance(minimum_items, int):
        count = len(value) if isinstance(value, (list, tuple, dict, str)) else 0
        if count < minimum_items:
            problems.append(
                f"{label} has {count} items, needs at least {minimum_items}"
            )

    maximum_items = requirement.get("max_items")
    if isinstance(maximum_items, int) and isinstance(value, (list, tuple, dict, str)):
        if len(value) > maximum_items:
            problems.append(
                f"{label} has {len(value)} items, allows at most {maximum_items}"
            )

    if requirement.get("not_empty") is True and _is_empty(value):
        problems.append(f"{label} is empty")

    minimum_length = requirement.get("min_length")
    if isinstance(minimum_length, int) and isinstance(value, str):
        if len(value.strip()) < minimum_length:
            problems.append(
                f"{label} is {len(value.strip())} characters, "
                f"needs at least {minimum_length}"
            )

    lower, upper = requirement.get("min"), requirement.get("max")
    if _is_bound(lower) or _is_bound(upper):
        if not _is_number(value):
            problems.append(f"{label} is not a number")
        else:
            if _is_bound(lower) and value < lower:
                problems.append(f"{label} is {value}, below the required {lower}")
            if _is_bound(upper) and value > upper:
                problems.append(f"{label} is {value}, above the allowed {upper}")

    allowed = requirement.get("one_of")
    if isinstance(allowed, list) and allowed and value not in allowed:
        problems.append(f"{label} is {value!r}, must be one of {allowed}")

    pattern = requirement.get("matches")
    if isinstance(pattern, str) and pattern:
        try:
            if not re.search(pattern, str(value)):
                problems.append(f"{label} does not match the required pattern")
        except re.error:
            # A malformed rule must not fail the task — the author of the rule
            # made the mistake, and the agent cannot fix it by trying again.
            logger.warning("skipping gate rule with invalid regex %r", pattern)

    return problems
