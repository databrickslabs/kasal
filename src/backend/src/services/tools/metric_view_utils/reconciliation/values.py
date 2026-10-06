"""PBI value parsing (pure).

Power BI's Execute Queries API returns some measures as display strings — a
DAX ``FIXED(x, 2) & "%"`` renders as ``"1,605.9 %"`` — so a value_format tells
the reconciler how to turn each cell back into a number.

Three formats:

- ``raw_number``: numeric already (or a plain numeric string).
- ``percent_string``: ``"5.29%"`` -> ``0.0529`` (strip the ``%`` and any
  thousands separators added by ``FIXED()``, then divide by 100).
- ``percent_string_unscaled``: ``"0.06%"`` -> ``0.06`` (strip the ``%`` only, no
  divide) — for a specific PBI-model defect where a SWITCH branch is missing the
  ``*100`` its siblings have. Never a first guess; use only when confirmed.

``coerce`` (the default) turns an unparseable cell into NaN rather than raising,
so one mismapped measure returning a differently-shaped string cannot take down
the whole batch. ``strict=True`` raises — used for snapshot equality, where an
invalid numeric must not silently become a matching blank.
"""

from __future__ import annotations

import pandas as pd


def strip_percent(series: pd.Series) -> pd.Series:
    """ "1,605.9 %" -> "1605.9": drop the ``%`` suffix and thousands separators
    ``FIXED()`` adds (a ratio of 1000% or more would otherwise not parse and
    compare as a spurious blank)."""
    return (
        series.astype(str)
        .str.rstrip("%")
        .str.replace(",", "", regex=False)
        .str.strip()
        .where(series.notna())
    )


def parse_value(
    series: pd.Series, value_format: str, strict: bool = False
) -> pd.Series:
    """Parse a PBI result column into numbers according to ``value_format``."""
    errors = "raise" if strict else "coerce"
    if value_format == "percent_string":
        return pd.to_numeric(strip_percent(series), errors=errors) / 100
    if value_format == "percent_string_unscaled":
        return pd.to_numeric(strip_percent(series), errors=errors)
    return pd.to_numeric(series, errors=errors)
