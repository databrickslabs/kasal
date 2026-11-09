"""Diagnosis for runs that produce no views, and the opt-in best-effort drafts.

When nothing generates, :func:`diagnose_zero_views` explains why, from two
deterministic signals: real source tables present, and measures that reduce to
a table aggregate. :func:`build_best_effort_views` drafts thin views from the
measures that already resolved to real SQL, and only when the user supplied a
``fact_source_map``. It never fabricates SQL.
"""

from __future__ import annotations

import re
from typing import TypedDict

from .shared_types import as_dict, as_list, as_str

_AGG = (
    "SUM",
    "COUNT",
    "AVG",
    "MIN",
    "MAX",
    "DIVIDE",
    "CALCULATE",
    "SUMX",
    "COUNTX",
)


class BestEffortReport(TypedDict):
    """Coverage report for a best-effort draft: what was emitted and what was not."""

    mode: str
    warning: str
    sources_supplied: int
    tables_emitted: int
    measures_emitted: int
    measures_skipped_unresolved: int
    tables_without_source: list[str]
    skipped_measures: list[str]


def _measure_resolutions(config: object) -> dict[str, object]:
    """``config['measure_resolutions']`` as a dict, or empty."""
    return as_dict(as_dict(config).get("measure_resolutions"))


def diagnose_zero_views(
    mquery_entries: object,
    measures: object,
    config: object,
) -> dict[str, object]:
    """Explain WHY 0 UC Metric Views were generated, and what to do about it.

    A thin-report model otherwise silently yields a fallback JSON and no
    views, with no signal to the user about the cause or fix. This turns
    that into an actionable diagnosis. It classifies the run by two
    deterministic signals:
      * source tables present?  → any mquery entry with real transpiled SQL /
        an M source expression (not raw/empty).
      * measures resolvable?    → any config.measure_resolutions with real
        aggregatable SQL (not TODO).

    Returns {case, reason, recommended_action, signals} — see the customer
    guide thin-report-and-source-resolution.md for the three cases.
    """
    # Signal 1: do we have any real source table?
    has_source = False
    for entry in as_list(mquery_entries):
        if not isinstance(entry, dict):
            continue
        sql = as_str(
            entry.get("transpiled_sql") or entry.get("mquery_expression")
        ).strip()
        if sql and sql not in ("{}", "null"):
            has_source = True
            break

    # Signal 2: any measure that resolved to real aggregatable SQL?
    resolvable = 0
    for res in _measure_resolutions(config).values():
        base = as_str(as_dict(res).get("base_expr"))
        if (
            base
            and not base.strip().upper().startswith("TODO")
            and base.strip().upper().startswith(_AGG)
        ):
            resolvable += 1

    signals: dict[str, object] = {
        "has_source_tables": has_source,
        "resolvable_measures": resolvable,
        "total_measures": len(as_list(measures)),
    }

    if has_source:
        # Sources exist but still 0 views — a translation/allocation issue,
        # not a thin-report problem. Leave it to the normal migration report.
        return {
            "case": "sources_present_no_views",
            "reason": (
                "Source tables were found but no metric view was emitted — "
                "likely all measures were untranslatable DAX. See the "
                "migration report / not-emitted notes."
            ),
            "recommended_action": "Review the not-emitted measures; no source mapping needed.",
            "signals": signals,
        }
    # No source tables → thin report (case B) or hand-entered (case C).
    if resolvable > 0:
        action = (
            "This looks like a THIN REPORT on an upstream semantic model — its "
            "source tables are not in what was extracted. PREFERRED: re-run "
            "against the upstream model's dataset_id (see Power BI lineage). "
            "OR: enable allow_best_effort and supply fact_source_map to draft "
            f"views for the {resolvable} resolvable measure(s) — tables/measures "
            "may be missing. See thin-report-and-source-resolution.md."
        )
    else:
        action = (
            "This looks like a THIN REPORT (or a report-logic-only model): no "
            "source tables AND no measures reduce to a table aggregate (mostly "
            "selector / measure-on-measure DAX). Re-run against the upstream "
            "semantic model's dataset_id if one exists; otherwise these measures "
            "cannot be converted to metric views. See "
            "thin-report-and-source-resolution.md."
        )
    return {
        "case": "thin_report_no_source_tables",
        "reason": (
            "No source tables (M-Queries) were found in the extracted model, so "
            "there is no physical table to build a UC Metric View on."
        ),
        "recommended_action": action,
        "signals": signals,
    }


def _to_snake(name: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z]+", "_", str(name)).strip("_").lower()
    return s or "measure"


def _allocated_table(m: dict[str, object]) -> str:
    return (
        as_str(m.get("proposed_allocation") or m.get("table_name") or m.get("table"))
        or "__unassigned__"
    )


def _measure_tables(measures: object) -> dict[str, str]:
    """Measure name -> its allocated PBI table (from the measures list)."""
    measure_table: dict[str, str] = {}
    for m in as_list(measures):
        if isinstance(m, dict):
            nm = as_str(m.get("measure_name") or m.get("original_name"))
            if nm:
                measure_table[nm] = _allocated_table(m)
    return measure_table


def build_best_effort_views(
    fact_source_map: object,
    config: object,
    measures: object,
) -> tuple[dict, BestEffortReport]:
    """Best-effort UCMVs for a THIN-REPORT model (no M-Query source tables).

    Opt-in fallback used ONLY when the normal path produced 0 views AND the
    human supplied `fact_source_map` (PBI table -> physical
    catalog.schema.table). See docs/powerbi/thin-report-and-source-
    resolution.md — the PREFERRED fix is to point Kasal at the upstream
    semantic model; this is for when that model is unreachable.

    Emits one thin metric view per supplied source, containing only measures
    that ALREADY resolved to real aggregatable SQL in
    `config['measure_resolutions']` (reusing the pipeline's own base_expr +
    base_filters). It NEVER fabricates SQL:
    - measures with a `TODO`/empty resolution are skipped (documented, not emitted);
    - tables not in `fact_source_map` are skipped;
    so the "never emit silently-wrong SQL" contract holds.

    Every emitted measure is flagged `TODO: verify` because it is a DRAFT
    produced without a validated transpiled source. Returns (views,
    coverage_report) where coverage_report makes the GAP explicit —
    tables/measures that were skipped.
    """
    views: dict = {}
    report: BestEffortReport = {
        "mode": "best_effort",
        "warning": (
            "DRAFT views built from a thin report without validated source "
            "tables. Tables/measures may be MISSING; every emitted measure "
            'is marked "TODO: verify". Prefer converting the upstream '
            "semantic model — see thin-report-and-source-resolution.md."
        ),
        "sources_supplied": 0,
        "tables_emitted": 0,
        "measures_emitted": 0,
        "measures_skipped_unresolved": 0,
        "tables_without_source": [],
        "skipped_measures": [],
    }
    if not isinstance(fact_source_map, dict) or not fact_source_map:
        return views, report
    resolutions = _measure_resolutions(config)
    report["sources_supplied"] = len(fact_source_map)

    measure_table = _measure_tables(measures)

    # group RESOLVED measures by their allocated PBI table
    by_table: dict[str, list[tuple[str, dict[str, object]]]] = {}
    for mname, res_obj in resolutions.items():
        res = as_dict(res_obj)
        base = as_str(res.get("base_expr"))
        if (
            (not base)
            or base.strip().upper().startswith("TODO")
            or not base.strip().upper().startswith(_AGG)
        ):
            report["measures_skipped_unresolved"] += 1
            report["skipped_measures"].append(mname)
            continue
        by_table.setdefault(measure_table.get(mname, "__unassigned__"), []).append(
            (mname, res)
        )

    for pbi_table, source in fact_source_map.items():
        rows = by_table.get(pbi_table, [])
        if not rows:
            report["tables_without_source"].append(pbi_table)
            continue
        measures_out: list[dict[str, object]] = []
        for mname, res in rows:
            expr = as_str(res.get("base_expr"))
            filters = as_list(res.get("base_filters"))
            if filters:
                expr = f"{expr} FILTER (WHERE {' AND '.join(str(f) for f in filters)})"  # joins resolved predicates (escaped literals) — nosemgrep: tmp.databricks.sql-injection.fstring-where
            measures_out.append(
                {
                    "name": _to_snake(mname),
                    "expr": expr,
                    "comment": (
                        "BEST-EFFORT DRAFT — no validated source. TODO: verify. "
                        f"From DAX '{mname}'."
                    ),
                }
            )
        views[pbi_table] = {
            "version": "1.1",
            "source": source,
            "comment": (
                "BEST-EFFORT UC Metric View (thin-report model). "
                f"{len(measures_out)} measures drafted from resolved SQL; "
                "NOT validated against a transpiled source. Review before "
                "deploy. Some measures/tables from the original model may "
                "be MISSING."
            ),
            "measures": measures_out,
        }
        report["tables_emitted"] += 1
        report["measures_emitted"] += len(measures_out)
    return views, report
