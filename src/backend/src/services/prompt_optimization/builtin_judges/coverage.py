"""Whether a run has enough labels for a judge that needs them.

A label judge (Correctness, ExpectationsGuidelines) counts only when at least
half of the dataset's rows are labelled for it AND at least 3 rows are — or
every row, when the dataset has fewer than 3. On sparse labels a judge that
grades a few rows adds noise to the objective rather than signal. A crew run
has one row, so for crews this reads "labelled or not".

A judge below the floor is left out of the run with a reason the dialog shows.
A judge above it still skips each unlabelled row before calling mlflow (see
``runner``): mlflow raises on a missing label, and a raise aborts the whole
metric call.
"""

from __future__ import annotations

from typing import Dict, List, Mapping, Sequence, Tuple

from src.services.prompt_optimization.builtin_judges.catalog import BuiltinJudge

MIN_FRACTION = 0.5
MIN_ROWS = 3


def is_labelled(judge: BuiltinJudge, row: Mapping[str, object]) -> bool:
    """Whether ``row`` carries any of the labels ``judge`` reads."""
    return any(bool(row.get(field)) for field in judge.label_fields)


def skip_reason(
    judge: BuiltinJudge, rows: Sequence[Mapping[str, object]]
) -> str | None:
    """Why ``judge`` cannot count on ``rows``, or None when it can."""
    if not judge.label_fields:
        return None
    total = len(rows)
    labelled = sum(1 for row in rows if is_labelled(judge, row))
    if labelled == 0:
        return f"{judge.label} skipped: not labelled"
    if labelled < min(MIN_ROWS, total) or labelled < MIN_FRACTION * total:
        return (
            f"{judge.label} skipped: {labelled} of {total} rows labelled "
            f"(needs at least half, and at least {MIN_ROWS})"
        )
    return None


def split_by_coverage(
    judges: Sequence[BuiltinJudge], rows: Sequence[Mapping[str, object]]
) -> Tuple[List[BuiltinJudge], Dict[str, str]]:
    """``(judges that count, {judge id: skip reason})``."""
    usable: List[BuiltinJudge] = []
    skipped: Dict[str, str] = {}
    for judge in judges:
        reason = skip_reason(judge, rows)
        if reason is None:
            usable.append(judge)
        else:
            skipped[judge.id] = reason
    return usable, skipped
