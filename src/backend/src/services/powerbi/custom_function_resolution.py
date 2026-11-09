"""Inline known custom DAX library functions into real per-fact-table SQL.

Unlike a generic SWITCH-branch dispatcher (``switch_decomposition.py``), these
are NAMED, reusable functions from the tenant's own corporate DAX code
library — confirmed via a tenant-wide Admin Scan covering ~150 datasets that
share the same ``fx_*`` naming convention — so recognizing a specific function
by name is the right approach here, not inferring intent from arbitrary DAX
shape the way SWITCH-branch resolution does.

``fx_OTCKPI(code_a, code_b)`` is the one resolved so far, confirmed live on
``otc_management``'s ``Fact_OTC`` (whose own source table is literally named
``...otc010_otc_subkbis`` — "sub-KBIs"): a classic EAV-style ratio lookup —
``code_a``/``code_b`` are standardized KBI codes (e.g. ``"KIOM03503"``) and the
function returns ``SUM(value WHERE code=code_a) / SUM(value WHERE
code=code_b)``. The EAV table and its code/value columns are discovered per
model from ``admin_tables`` (never hardcoded to Fact_OTC/bic_csubkbi/fltp), so
this also resolves ``fx_OTCKPI`` on any of this tenant's other reports that
reuse the same corporate DAX library function against their own sub-KBI table.
Restricted to the unambiguous case (exactly one EAV-shaped table in the model)
— with more than one candidate there is no way to know which table a given
call targets without report-specific knowledge, and a wrong guess produces a
plausible-looking but silently incorrect number, which is worse than a TODO.

Not handled here (different, harder shapes — left as documented gaps):
``fx_GetMeasureByName`` (a true dynamic measure-name dispatcher — no static
DAX can express which measure it resolves to; needs the model's real
Calculation Group/UDF definition, unavailable via the REST Admin Scanner,
Fabric TMDL, or the TMSCHEMA_* DMVs on this tenant), ``fx_TimeIntelligencePY``/
``fx_TimeIntelligencePM`` (period-shifting wrappers — need date-filter
rewriting, not a plain aggregate), and ``fx_SparkLineSVG``/``fx_GaugeSVG``
(render an SVG string — a display artifact, not a business measure).

``to_snake_case`` is duplicated locally (``_to_snake_case``) rather than
imported from ``pipeline_config`` — this module has no dependency on it at
all, since ``pipeline_config.py`` is also loaded standalone, by file path, via
``generate_config.py``'s CLI fallback (no real parent package in that
context), and it is what calls INTO this module, not the other way round. See
``switch_decomposition.py``'s docstring for the full explanation.
"""

from __future__ import annotations

import re
from collections import defaultdict


def _to_snake_case(name: str) -> str:
    """Convert PascalCase/camelCase/mixed to snake_case.

    Kept identical to (and independently of) ``pipeline_config.to_snake_case``
    — see this module's own docstring for why it isn't imported from there.
    """
    s = re.sub(r"%", "_pct", name)
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    s = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s)
    s = re.sub(r"[\s\-]+", "_", s)
    return s.lower().strip("_")


# Column-name substrings that mark a column as a DIMENSION, not the EAV table's
# value column — confirmed against Fact_OTC's real columns (fiscper, comp_code,
# country, bic_ccusthie4, fiscper_date all matched, leaving exactly `fltp` as
# the value column by elimination). Deliberately name-based rather than
# type-based: the Admin Scanner's column info here carries no reliable
# numeric-vs-text signal to check instead.
_DIMENSION_NAME_HINTS = (
    "period",
    "fiscper",
    "date",
    "country",
    "comp_code",
    "company",
    "hie",
    "customer",
    "region",
    "plant",
    "year",
    "month",
)

_FX_OTCKPI_CALL = re.compile(r'fx_OTCKPI\s*\(\s*"([^"]+)"\s*,\s*"([^"]+)"\s*\)')


def _find_kbi_code_value_columns(
    admin_tables: dict[str, dict],
) -> dict[str, tuple[str, str]]:
    """``{table_name: (code_column, value_column)}`` for EAV-shaped KBI tables.

    A candidate table has exactly one column whose name contains "kbi" (the
    code column) and, after removing every column matching
    ``_DIMENSION_NAME_HINTS``, exactly one column left (the value column) — a
    table with zero or more than one remaining column is ambiguous and is
    skipped rather than guessed at.
    """
    found: dict[str, tuple[str, str]] = {}
    for table_name, info in admin_tables.items():
        # Real warehouse M-Query only — excludes a DAX CALCULATED table (e.g.
        # AI_KBI_Compare, itself a DISTINCT(SELECTCOLUMNS(FILTER(...))) derived
        # FROM the selector table) whose "KBI"-named column is a display label,
        # not a code an EAV fact table can be aggregated against.
        mquery = (info.get("mquery_expression") or "").strip()
        if not mquery.lower().startswith("let"):
            continue
        cols = [c.get("name", "") for c in info.get("columns", [])]
        code_candidates = [c for c in cols if "kbi" in c.lower()]
        if len(code_candidates) != 1:
            continue
        code_col = code_candidates[0]
        remaining = [c for c in cols if c != code_col]
        non_dim = [
            c
            for c in remaining
            if not any(hint in c.lower() for hint in _DIMENSION_NAME_HINTS)
        ]
        if len(non_dim) == 1:
            found[table_name] = (code_col, non_dim[0])
    return found


def derive_fx_otckpi_resolutions(
    measures: list[dict], admin_tables: dict[str, dict]
) -> dict[str, list[dict]]:
    """Resolve every ``fx_OTCKPI(code_a, code_b)`` measure to real SQL.

    Returns the same ``{table: [{name, raw_expr, comment}, ...]}`` shape as
    ``switch_decomposition.derive_switch_decompositions`` — mergeable into
    ``switch_decompositions``, which ``table_processor.py``'s Step 6 already
    consumes, so no new downstream wiring is needed.
    """
    kbi_tables = _find_kbi_code_value_columns(admin_tables)
    if len(kbi_tables) != 1:
        return {}
    table_name, (code_col, value_col) = next(iter(kbi_tables.items()))
    code_col_snake = _to_snake_case(code_col)
    value_col_snake = _to_snake_case(value_col)

    out: dict[str, list[dict]] = defaultdict(list)
    for m in measures:
        dax = m.get("expression", "") or ""
        name = m.get("measure_name", "")
        if not dax or "fx_OTCKPI" not in dax:
            continue
        match = _FX_OTCKPI_CALL.search(dax)
        if not match:
            continue
        code_a, code_b = match.group(1), match.group(2)
        num = (
            f"SUM(CASE WHEN source.{code_col_snake} = '{code_a}' "
            f"THEN source.{value_col_snake} END)"
        )
        den = (
            f"SUM(CASE WHEN source.{code_col_snake} = '{code_b}' "
            f"THEN source.{value_col_snake} END)"
        )
        out[table_name].append(
            {
                "name": _to_snake_case(name),
                # TRUE PBI display name — see switch_decomposition.py's
                # identical comment for why this must not be omitted.
                "original_name": name,
                "raw_expr": f"{num} / NULLIF({den}, 0)",
                "comment": (
                    f"fx_OTCKPI('{code_a}','{code_b}') resolved against "
                    f"{table_name}.{code_col}/{value_col}"
                ),
                # Not a named-measure reference (unlike switch_decomposition's
                # "direct"/"composite") — each operand is a code-filtered raw
                # column aggregate, so each source is a dict, not a plain
                # measure-name string. See pbi_ucmv_mapping.py for how this
                # shape maps onto mapping_schema.py's `raw_column` pbi_kind.
                "pbi_kind": "composite",
                "pbi_operator": "divide",
                "pbi_sources": [
                    {
                        "kind": "raw_column",
                        "table": table_name,
                        "column": code_col,
                        "value_column": value_col,
                        "filter_value": code_a,
                    },
                    {
                        "kind": "raw_column",
                        "table": table_name,
                        "column": code_col,
                        "value_column": value_col,
                        "filter_value": code_b,
                    },
                ],
            }
        )
    return dict(out)
