"""The full Genie space check: runs every check and assembles one report.

``ok`` is False when any verdict failed. ``review`` verdicts are hints and never
flip it. ``check_sql=False`` skips every statement that touches the warehouse
(examples, benchmarks, snippets); the describe-based checks still run.
"""

from __future__ import annotations

from src.services.tools.genie_space_sync_utils.checks_mapping import check_mappings
from src.services.tools.genie_space_sync_utils.checks_space import (
    check_data_sources,
    check_identifiers,
    check_percent_scaling,
    check_snippets,
    check_sql_items,
    table_names,
)
from src.services.tools.genie_space_sync_utils.metric_views import ViewCatalog
from src.services.tools.genie_space_sync_utils.sql_results import SqlFn
from src.services.tools.genie_space_sync_utils.verdicts import FAIL, Verdict, summarize


def build_report(
    serialized: dict[str, object],
    mappings: dict[str, str],
    sql_fn: SqlFn,
    check_sql: bool,
) -> dict[str, object]:
    catalog = ViewCatalog(sql_fn)
    tables = table_names(serialized)
    verdicts: list[Verdict] = []
    verdicts += check_data_sources(serialized, catalog)
    verdicts += check_percent_scaling(serialized, catalog)
    verdicts += check_identifiers(serialized, catalog)
    if check_sql:
        verdicts += check_sql_items(serialized, sql_fn)
        verdicts += check_snippets(serialized, sql_fn)
    verdicts += check_mappings(mappings, catalog, tables)
    summary = summarize(verdicts)
    return {
        "sql_checked": check_sql,
        "tables": len(tables),
        "summary": summary,
        "ok": summary[FAIL] == 0,
        "verdicts": [verdict.as_dict() for verdict in verdicts],
        "catalog_errors": dict(catalog.errors),
    }
