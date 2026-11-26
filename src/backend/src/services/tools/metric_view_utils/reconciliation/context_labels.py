"""Calendar labels per period, and the cells they leave out (pure, pandas).

A measure whose DAX reads a calendar label with SELECTEDVALUE (for example the
latest month or latest week) needs that label as a single value per period. The
metric view carries the label as a column (``extra_group_by: [{pbi, ucmv}]``), so
each period's PBI query filters the label column to the value the view has for
that period.

A period whose label is NULL in the metric view (for example a future week) has
no value to filter on: it is not queried in PBI and not compared. The counts
are reported per measure.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

import pandas as pd

from .mapping import Measure
from .ucmv_query import ucmv_context_alias
from .weeks import week_key

# {period: {PBI label column: label value}}
ContextLabels = dict[str, dict[str, object]]


def labels_from_ucmv(
    ucmv_df: pd.DataFrame, measures: Iterable[Measure]
) -> ContextLabels:
    """Label per period for every ``extra_group_by`` label column of ``measures``.

    A period without a label (NULL) is absent from the result. A label that is
    not one value per period is an error: the PBI filter would pick one of the
    values silently.
    """
    refs = sorted({ref for m in measures for ref in (m.ucmv_context_columns or {})})
    labels: ContextLabels = {}
    for ref in refs:
        alias = ucmv_context_alias(ref)
        if alias not in ucmv_df.columns:
            raise ValueError(
                f"UCMV result has no label column for {ref} (expected {alias})"
            )
        per_period = ucmv_df[["period", alias]].dropna().drop_duplicates()
        ambiguous = per_period["period"][per_period["period"].duplicated()].unique()
        if len(ambiguous):
            raise ValueError(
                f"{ref}: more than one UCMV label in periods {list(ambiguous[:5])}"
            )
        for period, value in zip(per_period["period"], per_period[alias]):
            labels.setdefault(str(period), {})[ref] = value
    return labels


def periods_without_label(
    periods: Iterable[str], labels: ContextLabels, refs: Iterable[str]
) -> list[str]:
    """Periods that lack a label for at least one of ``refs`` (sorted)."""
    needed = list(refs)
    return sorted(
        (p for p in periods if any(r not in labels.get(str(p), {}) for r in needed)),
        key=str,
    )


def drop_skipped_cells(
    long_df: pd.DataFrame,
    skipped: Mapping[str, list[str]],
    grain: str,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Remove the cells of periods that were not queried in PBI.

    ``skipped`` is ``{measure: periods}``. Returns the frame without those cells
    and, per measure, the number of cells removed.
    """
    if not skipped or long_df.empty:
        return long_df, {}
    norm = week_key if grain == "week" else str
    dropped_keys = {
        (measure, norm(period))
        for measure, periods in skipped.items()
        for period in periods
    }
    keep = [
        (m, norm(p)) not in dropped_keys
        for m, p in zip(long_df["ucmv_kbi_name"], long_df["period"])
    ]
    removed = long_df[[not k for k in keep]]
    counts = removed["ucmv_kbi_name"].value_counts().to_dict()
    return long_df[keep].reset_index(drop=True), {
        str(k): int(v) for k, v in counts.items()
    }
