"""Grain-aware period-range helpers (pure).

Deliberately minimal: month-grain fiscper ("YYYYNNN") plus the grain-independent
operations (filtering a period to its reference years, excluding the still-open
current period, and normalising a snapshot load date). Extend when a real
quarter-/year-grain UCMV enters scope rather than guessing the shape now.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Optional


def matches_reference_years(period: str, reference_years: list) -> bool:
    """True if ``period`` (fiscper "YYYYNNN" for month grain, or a bare year)
    falls within ``reference_years`` — matched on the leading 4 characters,
    which is correct for both shapes. An empty ``reference_years`` means "no year
    filter": every period matches (reconciliation across all years)."""
    if not reference_years:
        return True
    ref = {str(y) for y in reference_years}
    return str(period)[:4] in ref


def current_period(reference_date: Optional[date] = None) -> str:
    """The current, still-open period, "YYYYNNN"-shaped (month grain)."""
    d = reference_date or datetime.now(timezone.utc).date()
    return f"{d.year:04d}{d.month:03d}"


def is_closed_period(period: str, reference_date: Optional[date] = None) -> bool:
    """True if ``period`` is fully closed (strictly before the current one).

    The current, still-accruing period can have partial data on only one side
    (a mid-month UCMV or PBI refresh), which is not blank on either side and
    produces a spurious mismatch — so it is excluded from comparison entirely.
    """
    return str(period) < current_period(reference_date)


def normalize_snapshot_date(value) -> str:
    """Accept only exact dates or midnight timestamps; never silently truncate
    intraday data."""
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        return value.isoformat()
    else:
        parsed = datetime.fromisoformat(str(value))
    if (
        parsed.hour
        or parsed.minute
        or parsed.second
        or parsed.microsecond
        or parsed.tzinfo
    ):
        raise ValueError(
            f"Expected an unzoned snapshot date at midnight, got {value!r}"
        )
    return parsed.date().isoformat()
