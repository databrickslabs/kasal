"""Fold a Power BI report's period logic (closed periods, MTD / YTD default)
into EVERY metric view of that report — never as a shared/global calendar object.

Why this exists
---------------
Power BI reports show CLOSED fiscal periods only; the latest closed month is the
default for MTD and YTD runs from period 001 up to it. In the report this lives in
the calendar table's calculated columns (``Past_flag``, ``Latest_Month_Label``,
``Latest_Year_Label``, evaluated at ``TODAY()`` on refresh). A migrated Genie space
answers "this month" / "this year" as a plain filter only if every metric view
carries those labels.

Design decision (per the migration owner): the period columns are folded directly
into each UCMV of the report — there is NO standalone, cross-report calendar
dimension object in Unity Catalog. Fiscal calendars differ per Power BI model, so a
single global calendar would collide across reports (20 reports → 20 competing
calendars). Each report's UCMVs reference the report's OWN calendar table (through
the calendar join they already have) and expose the columns under one canonical set
of names, so a query generalises across that report's views without a shared object.

This module is pure: it detects which of a calendar table's columns implement the
period logic and returns canonical dimension dicts (the same
``{"name", "expr", "comment", ...}`` shape ``join_detector.get_dim_dimensions``
produces) to merge into every view that joins that calendar. Wiring lives in the
pipeline; the detection and canonical metadata live here so they are testable in
isolation and identical across every view of the report.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# ── Canonical period dimensions ──────────────────────────────────────────────
# name  → the stable UCMV dimension name every view of the report exposes.
# match → regexes (case-insensitive, matched against the calendar column name with
#         non-alphanumerics stripped) that identify the source column.
# The comment / display_name / synonyms mirror the hand-verified gold-standard
# views so Genie resolves "this month" / "this year" / "closed period" consistently.


@dataclass(frozen=True)
class PeriodDimension:
    """One canonical period column to fold into every metric view of a report."""

    name: str
    match: tuple  # regex patterns (already compiled) against normalised column names
    comment: str
    display_name: str
    synonyms: tuple


def _norm(col: str) -> str:
    """Lower-case and strip non-alphanumerics, so ``Latest_Month_Label``,
    ``latest month label`` and ``LatestMonthLabel`` all compare equal."""
    return re.sub(r"[^a-z0-9]", "", (col or "").lower())


def _p(*patterns: str) -> tuple:
    return tuple(re.compile(p) for p in patterns)


# Order matters: closed-period labels are matched before the plain fiscal columns,
# and the more specific fiscal patterns (fiscper) before the looser ones.
_PERIOD_DIMENSIONS: tuple = (
    PeriodDimension(
        name="past_flag",
        # 'pastflag' / 'isclosed' / 'closedflag' / 'closedperiod'
        match=_p(r"^pastflag$", r"^isclosed$", r"^closedflag$", r"^closedperiodflag$"),
        comment=(
            "1 = CLOSED fiscal period (Power BI Past_flag; a period closes once the "
            "first week of the next period has ended), 0 = open or future. Power BI "
            "only offers closed periods; filter past_flag = 1 unless the user "
            "explicitly asks for the running period."
        ),
        display_name="Closed Period Flag",
        synonyms=("closed period", "past period"),
    ),
    PeriodDimension(
        name="latest_month_label",
        match=_p(r"^latestmonthlabel$", r"^latestmonth$"),
        comment=(
            "Power BI Latest_Month_Label: 'Latest Month' marks the latest CLOSED "
            "fiscal period — the report's default month when the user names no "
            'period; every other period shows its month abbreviation. "Latest / '
            'current / this month" and MTD without a named month = '
            "latest_month_label = 'Latest Month'."
        ),
        display_name="Latest Month Label",
        synonyms=("latest month", "current month", "last closed month", "this month"),
    ),
    PeriodDimension(
        name="latest_year_label",
        match=_p(r"^latestyearlabel$", r"^latestyear$"),
        comment=(
            "Power BI Latest_Year_Label: 'Latest Year' marks the fiscal year of the "
            "latest closed period — the report's default year; other years show the "
            'year itself. "Current / this year" and YTD without a named year = '
            "latest_year_label = 'Latest Year'."
        ),
        display_name="Latest Year Label",
        synonyms=("latest year", "current year", "this year"),
    ),
    PeriodDimension(
        name="fiscper",
        match=_p(r"^fiscper$", r"^fiscalyearperiod$", r"^yearperiod$"),
        comment=(
            "Fiscal year + period as one string 'YYYYPPP' (e.g. '2025007' = period "
            "007 of 2025). Sorts chronologically."
        ),
        display_name="Fiscal Year/Period",
        synonyms=("fiscal period", "year period"),
    ),
    PeriodDimension(
        name="fiscal_year",
        match=_p(r"^fiscalyear$", r"^fiscyear$"),
        comment=(
            "Fiscal year (e.g. 2024, 2025) from the report's calendar. Never apply "
            "date functions to it; compare by casting to INT."
        ),
        display_name="Fiscal Year",
        synonyms=("year", "FY", "reporting year"),
    ),
    PeriodDimension(
        name="fiscal_month",
        match=_p(r"^fiscalmonth$", r"^fiscalperiod$", r"^fiscmonth$"),
        comment=(
            "Fiscal month (period number within the fiscal year, e.g. 001-012) from "
            "the report's calendar. Never apply date functions to it; compare by "
            "casting to INT."
        ),
        display_name="Fiscal Month",
        synonyms=("month", "period"),
    ),
)

# A calendar table is only worth folding period labels from if it exposes at least
# one of the CLOSED-PERIOD columns — a plain date table with just a year column is
# not the report's period calendar.
_CLOSED_PERIOD_NAMES = frozenset(
    {"past_flag", "latest_month_label", "latest_year_label"}
)


_CANONICAL_NAMES = frozenset(pd.name for pd in _PERIOD_DIMENSIONS)


@dataclass
class PeriodConfig:
    """Per-model overrides for period-column detection. The default name patterns
    are the CCH/Total-SC vocabulary; a model that names its columns differently
    (e.g. ``IsCurrentMonth`` for the latest-month flag) passes an explicit map so
    the fold still fires. Defaults change nothing."""

    # canonical period name -> exact source column that implements it. Takes
    # precedence over pattern detection.
    column_overrides: dict

    def __init__(self, column_overrides=None):
        self.column_overrides = {
            str(k).strip(): str(v).strip()
            for k, v in (column_overrides or {}).items()
            if str(k).strip() in _CANONICAL_NAMES and str(v).strip()
        }

    @classmethod
    def from_dict(cls, data) -> "PeriodConfig":
        """Build from a crew-supplied dict. Recognised key: ``column_overrides``
        (``{canonical_period_name: source_column}``). Junk is ignored."""
        if not isinstance(data, dict):
            return cls()
        return cls(column_overrides=data.get("column_overrides"))


def detect_period_columns(
    available_columns, config: "PeriodConfig | None" = None
) -> dict:
    """Map each canonical period name to the calendar column that implements it.

    ``available_columns`` is the calendar table's column names (any iterable of
    strings). Returns ``{canonical_name: source_column}``. Explicit
    ``config.column_overrides`` win; otherwise the first source column that
    matches a canonical pattern wins, and each source column is claimed by at
    most one canonical name (so 'fiscal_year' does not also swallow 'fiscper')."""
    cols = [c for c in (available_columns or []) if isinstance(c, str) and c.strip()]
    overrides = (config.column_overrides if config else {}) or {}
    claimed: set = set()
    resolved: dict = {}

    # Explicit overrides first (only if the named column actually exists).
    col_by_lower = {c.lower(): c for c in cols}
    for canonical, src in overrides.items():
        actual = col_by_lower.get(str(src).lower())
        if actual is not None:
            resolved[canonical] = actual
            claimed.add(actual)

    for pd in _PERIOD_DIMENSIONS:
        if pd.name in resolved:
            continue
        for col in cols:
            if col in claimed:
                continue
            if any(pat.match(_norm(col)) for pat in pd.match):
                resolved[pd.name] = col
                claimed.add(col)
                break
    return resolved


def has_closed_period_logic(
    available_columns, config: "PeriodConfig | None" = None
) -> bool:
    """True if the calendar exposes at least one closed-period column (past_flag /
    latest_month_label / latest_year_label). Used to decide whether a table is the
    report's period calendar at all."""
    resolved = detect_period_columns(available_columns, config)
    return any(name in resolved for name in _CLOSED_PERIOD_NAMES)


def fold_period_dimensions(
    calendar_alias: str,
    available_columns,
    existing_dim_names=None,
    config: "PeriodConfig | None" = None,
) -> list:
    """Return canonical period dimension dicts to fold into a metric view.

    - ``calendar_alias``: the join alias of the report's calendar dimension in this
      view (dimensions reference ``<alias>.<column>``).
    - ``available_columns``: the calendar table's column names.
    - ``existing_dim_names``: names already emitted for this view; a canonical name
      already present is skipped, so folding never produces a duplicate dimension.

    Returns ``[]`` unless the calendar carries closed-period logic — a plain date
    table contributes nothing here. Each dict matches
    ``join_detector.get_dim_dimensions``' shape plus ``display_name`` / ``synonyms``.
    """
    if not calendar_alias:
        return []
    resolved = detect_period_columns(available_columns, config)
    if not any(name in resolved for name in _CLOSED_PERIOD_NAMES):
        return []

    existing = {n.strip().lower() for n in (existing_dim_names or set()) if n}
    out: list = []
    for pd in _PERIOD_DIMENSIONS:
        src = resolved.get(pd.name)
        if src is None or pd.name.lower() in existing:
            continue
        out.append(
            {
                "name": pd.name,
                "expr": f"{calendar_alias}.{src}",
                "comment": pd.comment,
                "display_name": pd.display_name,
                "synonyms": list(pd.synonyms),
            }
        )
    return out


# A calendar column that already holds the matching date/period of the PRIOR
# year/period — so a prior-year measure is a calendar self-join on this key rather
# than a date-shift computation (the exact lookup the DAX-LLM time-intelligence
# rule and the skill corpus point at). date_py, fiscper_py, py_date, prev_year, …
# Matched against the RAW lower-cased name (not _norm, which strips the separators
# the `_py` / `py_` conventions rely on).
_PRIOR_PERIOD_RE = re.compile(
    r"(_py$|^py_|prior[_]?(year|period|date|month)|"
    r"prev(ious)?[_]?(year|period|date|month)|(last|ly)[_]?year)"
)


def detect_prior_period_column(available_columns) -> "str | None":
    """Return a calendar column holding the PRIOR-period/year key, or None.

    Phase-2 time-intelligence can self-join the calendar on this to resolve
    SAMEPERIODLASTYEAR exactly (vs the window-function fallback the DAX-LLM uses
    when it is absent).
    """
    for col in available_columns or []:
        if not isinstance(col, str) or not col.strip():
            continue
        if _PRIOR_PERIOD_RE.search(col.strip().lower()):
            return col
    return None
