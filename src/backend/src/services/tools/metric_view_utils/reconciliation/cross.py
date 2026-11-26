"""Per-measure period x member breakdown of the measures below 100 % (pure, pandas).

The period and member tables in a report show each axis on its own. A pattern
that only appears for one member in some periods is visible only in the cells of
both axes together. This module gives one row per (period, member) cell of every
measure with a misaligned cell, with the cell's class as 0/1 counts.

Values never leave this module: it reads them through
:func:`statistics.value_free_cells`, which keeps only flags, keys and the
relative difference of comparable cells.
"""

from __future__ import annotations

import pandas as pd

from .statistics import value_free_cells

_MEASURE_KEY = "ucmv_kbi_name"


def cross_breakdown(long_df: pd.DataFrame) -> dict[str, list[dict[str, object]]]:
    """``{measure: [cell rows]}`` for every measure with at least one misaligned cell.

    Each cell row has ``period``, ``dimension`` (the member), and the counts
    ``aligned``, ``missing_in_uc`` (PBI has a value, the metric view does not),
    ``extra_in_uc`` (the metric view has a value, PBI does not) and ``differ``
    (both have a value and they are not within tolerance), plus
    ``relative_difference`` for the comparable cells (``None`` otherwise).
    """
    if long_df.empty:
        return {}
    cells = value_free_cells(long_df)
    fully_aligned = cells.groupby(_MEASURE_KEY)["is_aligned"].transform("all")
    cells = cells[~fully_aligned.astype(bool)].copy()
    if cells.empty:
        return {}
    cells["aligned"] = cells["is_aligned"].astype(int)
    cells["missing_in_uc"] = cells["is_missing"].astype(int)
    cells["extra_in_uc"] = cells["is_extra"].astype(int)
    cells["differ"] = (
        ~cells["is_aligned"] & ~cells["is_missing"] & ~cells["is_extra"]
    ).astype(int)
    out: dict[str, list[dict[str, object]]] = {}
    for record in cells.to_dict("records"):
        relative = record["relative_difference"]
        out.setdefault(str(record[_MEASURE_KEY]), []).append(
            {
                "period": str(record["period"]),
                "dimension": str(record["dimension"]),
                "aligned": int(record["aligned"]),
                "missing_in_uc": int(record["missing_in_uc"]),
                "extra_in_uc": int(record["extra_in_uc"]),
                "differ": int(record["differ"]),
                "relative_difference": (None if pd.isna(relative) else float(relative)),
            }
        )
    return out
