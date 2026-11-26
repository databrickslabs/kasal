"""Week-grain period keys (pure).

A week period is a fiscal ``YYYYWW`` key. Calendars spell the same week in
several ways: ``WW.YYYY`` (e.g. "41.2026"), ``YYYY-WW`` / ``YYYYWW`` text, or a
float such as ``202641.0`` once a blank calendar row makes the column numeric.
Every spelling normalises to ``YYYYWW`` before the two sides are compared.

Fiscal week keys do not follow ISO weeks, so the running week cannot be derived
from today's date. The metric view keeps the running week as its latest week,
which is still open: :func:`closed_weeks` drops it.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

_WEEK_FIRST = re.compile(r"^(\d{1,2})\.(\d{4})$")  # "41.2026" -> 202641
_YEAR_FIRST = re.compile(
    r"^(\d{4})[-/ .W]?(\d{1,2})$"
)  # "2026-41", "2026W41", "202641"
_FLOAT_KEY = re.compile(
    r"^(\d{6})\.0+$"
)  # "202641.0" (a blank row made the column float)
_WEEK_KEY = re.compile(r"^\d{6}$")


def week_key(value: object) -> str:
    """The week as a ``YYYYWW`` string. Values that are not a week (blank, NaN,
    ``<NA>``) come back as their text and fail :func:`is_week_key`."""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    text = str(value).strip()
    float_key = _FLOAT_KEY.match(text)
    if float_key:
        return float_key.group(1)
    week_first = _WEEK_FIRST.match(text)
    if week_first:
        return f"{week_first.group(2)}{int(week_first.group(1)):02d}"
    year_first = _YEAR_FIRST.match(text)
    if year_first:
        return f"{year_first.group(1)}{int(year_first.group(2)):02d}"
    return text


def is_week_key(key: str) -> bool:
    """True for a normalised ``YYYYWW`` key."""
    return bool(_WEEK_KEY.match(key))


def closed_weeks(periods: Iterable[object]) -> set[str]:
    """Every week key before the latest one. Blank keys are ignored; the latest
    week (the open running week) is never returned."""
    keys = sorted({key for key in map(week_key, periods) if is_week_key(key)})
    return set(keys[:-1])
