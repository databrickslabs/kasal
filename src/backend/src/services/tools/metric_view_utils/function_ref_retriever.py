"""Per-batch DAX function-reference retrieval.

The always-on skill corpus (skills/dax/*.md) teaches translation *judgment* plus
deep worked examples for the ~40 high-frequency functions that carry ~99% of real
measures. This module supplies the LONG TAIL on demand: it detects which DAX
functions a batch of measures actually uses and injects a deep, UCMV-legal
reference block for exactly those — and ONLY those the static prefix does not
already cover deeply (so CALCULATE/SUM/DIVIDE are never re-taught).

Ground truth = skills/dax/function_reference.json (386 entries parsed from the
392-function DAX reference). Curated UCMV-legal overrides below pin the exact form
+ dax_class for the non-obvious tail (statistical aggregates, PRODUCT, PATH,
financial closed-forms, text/date traps); everything else renders from the base
entry with an explicit "adapt per §0" caveat.

Deterministic, local, no network. Fail-open: any error → "" (no injection), never
blocks a translation.
"""
from __future__ import annotations

import json
import logging
import os
import re
from collections.abc import Iterable

logger = logging.getLogger(__name__)

_DIR = os.path.dirname(__file__)
_DATA_PATH = os.path.join(_DIR, "skills", "dax", "function_reference.json")
# Static corpus files whose fenced examples already teach a function deeply.
_PREFIX_FILES = (
    os.path.join(_DIR, "skills", "dax", "FUNCTION_REFERENCE.md"),
    os.path.join(_DIR, "skills", "dax", "PATTERNS.md"),
    os.path.join(_DIR, "skills", "dax", "UNSUPPORTED.md"),
)

# Function-call token: an UPPERCASE identifier (optionally dotted, e.g. NORM.DIST)
# immediately followed by '('. Matches DAX function calls, not [column] refs.
_TOKEN = re.compile(r"\b([A-Z][A-Z0-9]*(?:\.[A-Z0-9]+)*)\s*\(")

_MAX_RENDER = 40  # cap injected entries per batch (defensive)


def _load_base() -> dict[str, dict]:
    try:
        with open(_DATA_PATH, encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as e:  # noqa: BLE001
        logger.warning("[FUNC_REF] base data unavailable (%s): %s", _DATA_PATH, e)
        return {}


def _load_prefix_deep(base_keys: Iterable[str]) -> set[str]:
    """Names that already appear inside a fenced ``` example in the static prefix.

    Those are taught deeply in the always-on corpus, so retrieval must not
    re-inject them. Auto-syncs: promoting a function to a worked prefix example
    drops it from retrieval with no code change here.
    """
    blocks = ""
    for path in _PREFIX_FILES:
        try:
            with open(path, encoding="utf-8") as fh:
                blocks += "\n".join(re.findall(r"```.*?```", fh.read(), re.DOTALL))
        except Exception:  # noqa: BLE001
            continue
    return {k for k in base_keys if re.search(r"\b" + re.escape(k) + r"\b", blocks)}


_BASE = _load_base()
_PREFIX_DEEP = _load_prefix_deep(_BASE.keys())

# ── Curated UCMV-legal overrides (the tail traps) ────────────────────────────
# Each: exact SQL form for a measure expr (source.-qualified, single-source),
# the dax_class to return, and a one-line note. Keyed by the JSON lookup name.
# Head functions already deep in the prefix are intentionally omitted here.
_CURATED: dict[str, dict[str, str | None]] = {
    # Aggregation (non-obvious)
    "PRODUCT": {"sql": "EXP(SUM(LN(source.col)))  -- col > 0 only", "cls": "translatable_direct",
                "note": "no native product aggregate; log-sum-exp identity, positive values only."},
    "PRODUCTX": {"sql": "EXP(SUM(LN(source.factor)))  -- factor > 0", "cls": "translatable_direct",
                 "note": "per-row expr collapses into the aggregate; positive values only."},
    "COUNTBLANK": {"sql": "COUNT(1) - COUNT(source.col)", "cls": "translatable_direct",
                   "note": "count of NULLs."},
    "APPROXIMATEDISTINCTCOUNT": {"sql": "approx_count_distinct(source.col)", "cls": "translatable_direct", "note": ""},
    # Statistical aggregates (these DO appear in measures)
    "MEDIAN": {"sql": "median(source.col)", "cls": "translatable_direct", "note": "or approx_percentile(source.col, 0.5) on large data."},
    "MEDIANX": {"sql": "median(source.col)", "cls": "translatable_direct", "note": "iterator collapses to the aggregate."},
    "PERCENTILE.INC": {"sql": "percentile(source.col, 0.75)", "cls": "translatable_direct", "note": "inclusive."},
    "PERCENTILEX.INC": {"sql": "percentile(source.col, 0.75)", "cls": "translatable_direct", "note": ""},
    "PERCENTILE.EXC": {"sql": "percentile_disc(0.75) WITHIN GROUP (ORDER BY source.col)", "cls": "translatable_direct", "note": "exclusive."},
    "PERCENTILEX.EXC": {"sql": "percentile_disc(0.75) WITHIN GROUP (ORDER BY source.col)", "cls": "translatable_direct", "note": ""},
    "STDEV.S": {"sql": "stddev_samp(source.col)", "cls": "translatable_direct", "note": ""},
    "STDEV.P": {"sql": "stddev_pop(source.col)", "cls": "translatable_direct", "note": ""},
    "STDEVX.S": {"sql": "stddev_samp(source.col)", "cls": "translatable_direct", "note": ""},
    "STDEVX.P": {"sql": "stddev_pop(source.col)", "cls": "translatable_direct", "note": ""},
    "VAR.S": {"sql": "var_samp(source.col)", "cls": "translatable_direct", "note": ""},
    "VAR.P": {"sql": "var_pop(source.col)", "cls": "translatable_direct", "note": ""},
    "VARX.S": {"sql": "var_samp(source.col)", "cls": "translatable_direct", "note": ""},
    "VARX.P": {"sql": "var_pop(source.col)", "cls": "translatable_direct", "note": ""},
    "GEOMEAN": {"sql": "EXP(AVG(LN(source.col)))  -- col > 0", "cls": "translatable_direct", "note": ""},
    "GEOMEANX": {"sql": "EXP(AVG(LN(source.col)))  -- col > 0", "cls": "translatable_direct", "note": ""},
    "RANKX": {"sql": None, "cls": "architecture_change",
              "note": "measures aggregate, they don't rank inline. Rank in the dashboard, or precompute RANK() OVER (ORDER BY <agg> DESC) as a source dimension."},
    "RANK.EQ": {"sql": None, "cls": "architecture_change",
                "note": "same as RANKX — dashboard rank or source-view ROW_NUMBER/RANK precompute."},
    # Math traps
    "QUOTIENT": {"sql": "DIV(source.a, source.b)", "cls": "translatable_direct", "note": "integer division."},
    "LOG": {"sql": "LOG(base, source.n)", "cls": "translatable_direct", "note": "ARG ORDER FLIPS vs DAX LOG(n, base)."},
    "INT": {"sql": "FLOOR(source.x)", "cls": "translatable_direct", "note": ""},
    "ROUNDDOWN": {"sql": "FLOOR(source.x)", "cls": "translatable_direct", "note": "or TRUNC for toward-zero."},
    "ROUNDUP": {"sql": "CEIL(source.x)", "cls": "translatable_direct", "note": ""},
    "CEILING": {"sql": "CEIL(source.x / sig) * sig", "cls": "translatable_direct", "note": "DAX takes a significance arg."},
    "FLOOR": {"sql": "FLOOR(source.x / sig) * sig", "cls": "translatable_direct", "note": "DAX takes a significance arg."},
    "ACOT": {"sql": "ATAN(1.0 / source.x)", "cls": "translatable_direct", "note": "no native cot family."},
    "COT": {"sql": "1.0 / TAN(source.x)", "cls": "translatable_direct", "note": ""},
    "FACT": {"sql": "FACTORIAL(source.n)", "cls": "translatable_direct", "note": ""},
    "SQRTPI": {"sql": "SQRT(source.x * PI())", "cls": "translatable_direct", "note": ""},
    "CURRENCY": {"sql": "CAST(source.x AS DECIMAL(19,4))", "cls": "translatable_direct", "note": ""},
    "MROUND": {"sql": "ROUND(source.x / m) * m", "cls": "translatable_direct", "note": "round to nearest multiple m."},
    "EVEN": {"sql": None, "cls": "architecture_change", "note": "round up to even integer — arithmetic UDF or source precompute."},
    "ODD": {"sql": None, "cls": "architecture_change", "note": "round up to odd integer — arithmetic UDF or source precompute."},
    "GCD": {"sql": None, "cls": "architecture_change", "note": "no native GCD — UDF."},
    "LCM": {"sql": None, "cls": "architecture_change", "note": "no native LCM — UDF."},
    "RANDBETWEEN": {"sql": "FLOOR(RAND() * (hi - lo + 1)) + lo", "cls": "translatable_direct", "note": ""},
    # Text traps
    "FIND": {"sql": "INSTR(source.hay, 'needle')", "cls": "translatable_direct", "note": "ARG ORDER SWAPS (DAX needle-first); FIND is case-sensitive."},
    "SEARCH": {"sql": "INSTR(LOWER(source.hay), LOWER('needle'))", "cls": "translatable_direct", "note": "ARG ORDER SWAPS; SEARCH is case-insensitive → LOWER both sides."},
    "REPLACE": {"sql": "OVERLAY(source.text PLACING 'new' FROM start FOR len)", "cls": "translatable_direct", "note": "positional REPLACE; NOT value-based (that's SUBSTITUTE → REPLACE())."},
    "SUBSTITUTE": {"sql": "REPLACE(source.text, 'old', 'new')", "cls": "translatable_direct", "note": "value-based replace."},
    "FIXED": {"sql": "format_number(source.x, d)", "cls": "display_layer", "note": "returns a STRING; if a rounded number is intended use ROUND(source.x, d) and flag."},
    "COMBINEVALUES": {"sql": "CONCAT_WS('delim', source.a, source.b)", "cls": "translatable_direct", "note": ""},
    "VALUE": {"sql": "TRY_CAST(source.s AS DOUBLE)", "cls": "translatable_direct", "note": "text → number."},
    "CONCATENATEX": {"sql": None, "cls": "display_layer", "note": "string aggregation for a label, not a metric. If a real delimited value: array_join(collect_list(source.col), 'd')."},
    "UNICHAR": {"sql": "CHR(source.n)", "cls": "translatable_direct", "note": ""},
    "UNICODE": {"sql": "ASCII(source.s)", "cls": "translatable_direct", "note": ""},
    # Date traps
    "DATEDIFF": {"sql": "DATEDIFF(source.b, source.a)  -- DAY; months_between(source.b, source.a) for MONTH/QTR/YEAR", "cls": "translatable_direct", "note": "ARG ORDER SWAPS vs DAX; unit-dependent function."},
    "EDATE": {"sql": "add_months(source.d, n)", "cls": "translatable_direct", "note": ""},
    "EOMONTH": {"sql": "last_day(add_months(source.d, n))", "cls": "translatable_direct", "note": ""},
    "YEARFRAC": {"sql": "datediff(source.b, source.a) / 365.25", "cls": "translatable_direct", "note": "approximation; day-count basis ignored — flag if basis matters."},
    "NETWORKDAYS": {"sql": None, "cls": "architecture_change", "note": "business-day count — source-view calendar precompute or UDF."},
    "WEEKDAY": {"sql": "DAYOFWEEK(source.d)", "cls": "translatable_direct", "note": ""},
    "WEEKNUM": {"sql": "WEEKOFYEAR(source.d)", "cls": "translatable_direct", "note": ""},
    # Parent-child (recursive → source view)
    "PATH": {"sql": None, "cls": "architecture_change", "note": "recursive CTE in the source view exposing a `path` column: WITH RECURSIVE anc AS (... UNION ALL ...)."},
    "PATHITEM": {"sql": "element_at(split(source.path, '|'), n)", "cls": "translatable_direct", "note": "requires `path` precomputed (see PATH)."},
    "PATHITEMREVERSE": {"sql": "element_at(split(source.path, '|'), -n)", "cls": "translatable_direct", "note": "requires `path`."},
    "PATHLENGTH": {"sql": "size(split(source.path, '|'))", "cls": "translatable_direct", "note": "requires `path`."},
    "PATHCONTAINS": {"sql": "array_contains(split(source.path, '|'), 'x')", "cls": "translatable_direct", "note": "requires `path`."},
    # Relationship
    "RELATEDTABLE": {"sql": None, "cls": "architecture_change", "note": "aggregate over a declared join; if it needs a grain change, precompute in source. Never a cross-table subquery."},
    # Table manipulation (tail)
    "TOPN": {"sql": None, "cls": "architecture_change", "note": "row-selection; source-view ROW_NUMBER() OVER (ORDER BY … DESC) filtered =1 (or QUALIFY). Never a bare MAX."},
    "GENERATE": {"sql": None, "cls": "architecture_change", "note": "lateral join belongs in the source view."},
    "GENERATEALL": {"sql": None, "cls": "architecture_change", "note": "outer lateral join in the source view."},
    "CROSSJOIN": {"sql": None, "cls": "architecture_change", "note": "set/join op belongs in the source view, not a measure."},
    "UNION": {"sql": None, "cls": "architecture_change", "note": "UNION ALL belongs in the source view."},
    "INTERSECT": {"sql": None, "cls": "architecture_change", "note": "set op in the source view."},
    "EXCEPT": {"sql": None, "cls": "architecture_change", "note": "set op in the source view."},
    # Financial closed-forms (inline arithmetic); iterative ones decline
    "SLN": {"sql": "(source.cost - source.salvage) / source.life", "cls": "translatable_direct", "note": "straight-line depreciation."},
    "EFFECT": {"sql": "POWER(1 + source.rate / source.n, source.n) - 1", "cls": "translatable_direct", "note": ""},
    "NOMINAL": {"sql": "source.n * (POWER(1 + source.eff, 1.0/source.n) - 1)", "cls": "translatable_direct", "note": ""},
    "RRI": {"sql": "POWER(source.fv / source.pv, 1.0/source.n) - 1", "cls": "translatable_direct", "note": ""},
    "PDURATION": {"sql": "LOG(1 + source.rate, source.fv / source.pv)", "cls": "translatable_direct", "note": ""},
    "XIRR": {"sql": None, "cls": "architecture_change", "note": "iterative solve — existing UC UDF or source precompute. Never invent a UDF name."},
    "RATE": {"sql": None, "cls": "architecture_change", "note": "iterative — UDF."},
    "XNPV": {"sql": None, "cls": "architecture_change", "note": "cash-flow sum over dates — source-view aggregate or UDF."},
    # Information context tests → decline
    "HASONEVALUE": {"sql": None, "cls": "unsupported", "note": "filter-context test; no metric-view meaning."},
    "ISFILTERED": {"sql": None, "cls": "unsupported", "note": "visual filter-context test — skip guard."},
    "ISCROSSFILTERED": {"sql": None, "cls": "unsupported", "note": "filter-context test — skip."},
    "NAMEOF": {"sql": None, "cls": "unsupported", "note": "metadata name reference — not a metric."},
}


def detect_functions(dax_expressions: Iterable[str]) -> list[str]:
    """Return the sorted set of known DAX function names used across the expressions."""
    found: set[str] = set()
    for expr in dax_expressions:
        if not expr:
            continue
        for tok in _TOKEN.findall(expr):
            if tok in _BASE:
                found.add(tok)
    return sorted(found)


def _render_entry(name: str) -> str:
    base = _BASE.get(name, {})
    cat = base.get("category", "")
    feas = base.get("feasibility", "")
    dax_ex = base.get("dax_example", "")
    cur = _CURATED.get(name)
    if cur:
        cls = cur["cls"]
        head = f"### {name} — {cat} · {feas} → {cls}"
        sql = cur["sql"]
        sql_line = f"  SQL:  {sql}" if sql else "  SQL:  (not a measure expr — see note)"
        note = f"\n  note: {cur['note']}" if cur.get("note") else ""
        return f"{head}\n  DAX:  {dax_ex}\n{sql_line}{note}"
    # base (uncurated) — carry the generic SQL with an explicit adaptation caveat
    cls = base.get("default_dax_class", "")
    raw = base.get("sql_reference_raw", "")
    head = f"### {name} — {cat} · {feas} → {cls} (default)"
    return (f"{head}\n  DAX:  {dax_ex}\n"
            f"  SQL (generic — adapt per §0: source.-qualify args, single-source only): {raw}")


def render_function_refs(dax_expressions: Iterable[str], max_render: int = _MAX_RENDER) -> str:
    """Build a markdown reference block for the tail functions used in this batch.

    Skips functions already taught deeply in the always-on prefix. Returns '' when
    nothing tail-relevant is used (the common case for simple batches). Fail-open.
    """
    try:
        if not _BASE:
            return ""
        names = [n for n in detect_functions(dax_expressions) if n not in _PREFIX_DEEP]
        if not names:
            return ""
        names = names[:max_render]
        curated = [n for n in names if n in _CURATED]
        logger.info(
            "[FUNC_REF] injected %d tail function ref(s): %s (curated: %s)",
            len(names), names, curated,
        )
        body = "\n\n".join(_render_entry(n) for n in names)
        return (
            "\n## DAX function reference (functions in these measures not already "
            "covered above)\n"
            "Authoritative UCMV-legal forms — follow these; obey §0 (source.-qualify, "
            "single-source). Entries marked (default) are generic references to adapt.\n\n"
            f"{body}\n"
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("[FUNC_REF] render failed (fail-open): %s", e)
        return ""
