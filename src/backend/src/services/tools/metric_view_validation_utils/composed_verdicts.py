"""The verdict of a measure that is only arithmetic over other measures of its view.

Such a measure (``MEASURE(a) - MEASURE(b)``) has no column aggregate of its own. Its SQL and
its DAX are compared as shapes (``measure_expression_tree``) and the names they call agree
(``measure_references``). The value is then the values of the measures it reads, so its
verdict is the weakest verdict of those measures, read transitively:

- every referenced measure VALID gives VALID;
- any EQUIVALENT, and nothing worse, gives EQUIVALENT;
- a REVIEW, an INVALID, an unknown status, a referenced measure the view does not have, or a
  cycle keeps REVIEW, and the reason names the measure that holds it there.

A shape difference is not resolved here: it stays what the validator said.
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Set

from .constants import STATUS_EQUIVALENT, STATUS_REVIEW, STATUS_VALID
from .prior_year_check import loose_name

COMPOSED_DIFFERENCE = "Composed of measures only"


def _combine(statuses: Iterable[Optional[str]]) -> str:
    """The weakest of the referenced verdicts; an unjudged reference is REVIEW."""
    seen = list(statuses)
    if not seen or any(s not in (STATUS_VALID, STATUS_EQUIVALENT) for s in seen):
        return STATUS_REVIEW
    return STATUS_EQUIVALENT if STATUS_EQUIVALENT in seen else STATUS_VALID


def resolve_composed_verdicts(evaluated: List[dict], view_names: Set[str]) -> None:
    """Sets the verdict of each composed measure of ``evaluated`` (the validator's list of
    judged measures of one view, edited in place) from the measures it reads.

    ``view_names`` holds the loose names of every measure of the view; a reference outside
    it is not a measure of the view and is not judged.
    """
    by_name: Dict[str, dict] = {}
    for entry in evaluated:
        verdict = entry.get("measure_eval_result")
        if isinstance(verdict, dict) and entry.get("measure_name"):
            by_name[loose_name(entry["measure_name"])] = verdict

    def effective(key: str, path: frozenset) -> Optional[str]:
        """The verdict a referenced measure gives its readers; None when it is not judged."""
        verdict = by_name.get(key)
        if verdict is None or key not in view_names or key in path:
            return None
        refs = verdict.get("composed_refs") or []
        if not refs:
            return str(verdict.get("status"))
        return _combine(effective(ref, path | {key}) for ref in refs)

    for key, verdict in by_name.items():
        refs = verdict.get("composed_refs") or []
        if not refs:
            continue
        outcome = _combine(effective(ref, frozenset({key})) for ref in refs)
        parts = [
            f"{_display(by_name, ref)} ({effective(ref, frozenset({key})) or 'not judged'})"
            for ref in refs
        ]
        if outcome in (STATUS_VALID, STATUS_EQUIVALENT):
            verdict["status"] = outcome
            verdict["is_valid"] = outcome == STATUS_VALID
            verdict["confidence"] = "high" if outcome == STATUS_VALID else "medium"
            verdict["differences"] = [
                d
                for d in verdict.get("differences", [])
                if not d.startswith(COMPOSED_DIFFERENCE)
            ]
            verdict.setdefault("similarities", []).append(
                "arithmetic over measures: the shape matches the DAX; takes the verdict of "
                + ", ".join(parts)
            )
        else:
            verdict.setdefault("recommendations", []).append(
                "arithmetic over measures: takes the verdict of " + ", ".join(parts)
            )


def _display(by_name: Dict[str, dict], key: str) -> str:
    """The measure's name as the validator reports it, or the key when it is not judged."""
    verdict = by_name.get(key)
    if verdict is None:
        return key
    return str(verdict.get("measure_name") or key)
