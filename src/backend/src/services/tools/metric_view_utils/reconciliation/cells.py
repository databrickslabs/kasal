"""Cell groups for a reconciliation comparison (pure, pandas).

Every cell (one per measure x period x dimension member) falls into exactly one
group, decided from the two sides' values:

- ``comparable``: both sides have a non-zero value. Only these have a meaningful
  relative difference.
- ``missing_in_uc``: Power BI has a non-zero value, the metric view is empty or
  zero, and the cell is not aligned.
- ``extra_in_uc``: the metric view has a non-zero value, Power BI is empty or
  zero, and the cell is not aligned.
- ``both_empty``: anything else. Neither side has a usable value, or a one-sided
  cell is within tolerance (which counts as aligned).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

CELL_COMPARABLE = "comparable"
CELL_MISSING_IN_UC = "missing_in_uc"
CELL_EXTRA_IN_UC = "extra_in_uc"
CELL_BOTH_EMPTY = "both_empty"

CELL_GROUPS = (
    CELL_COMPARABLE,
    CELL_MISSING_IN_UC,
    CELL_EXTRA_IN_UC,
    CELL_BOTH_EMPTY,
)


def _has_value(series: pd.Series) -> pd.Series:
    """True where the cell is a number that is not zero."""
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.notna() & numeric.ne(0)


def classify_cells(
    ucmv_value: pd.Series, pbi_value: pd.Series, within_tolerance: pd.Series
) -> pd.Series:
    """Return the cell group (one of :data:`CELL_GROUPS`) for every row."""
    ucmv_has = _has_value(ucmv_value)
    pbi_has = _has_value(pbi_value)
    aligned = within_tolerance.fillna(False).astype(bool)
    groups = np.select(
        [
            ucmv_has & pbi_has,
            pbi_has & ~ucmv_has & ~aligned,
            ucmv_has & ~pbi_has & ~aligned,
        ],
        [CELL_COMPARABLE, CELL_MISSING_IN_UC, CELL_EXTRA_IN_UC],
        default=CELL_BOTH_EMPTY,
    )
    return pd.Series(groups, index=ucmv_value.index)
