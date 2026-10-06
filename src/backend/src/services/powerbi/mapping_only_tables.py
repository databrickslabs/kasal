"""Draft ``mapping_only_tables`` entries for tables with no lakehouse-native SQL.

Split out of ``pipeline_config.py`` (over the file-size ceiling) as its own
seam, following the same pattern as ``switch_decomposition.py`` /
``calculation_groups.py`` / ``custom_function_resolution.py``.

``to_snake_case`` is duplicated locally rather than imported from
``pipeline_config`` — see this package's other split-out modules for why:
``pipeline_config.py`` is also loaded standalone, by file path, via
``generate_config.py``'s CLI fallback, where it has no real parent package —
an import back from here to there would raise ``ModuleNotFoundError`` in that
context. Keeping this module a self-contained leaf avoids the cycle entirely.
"""

from __future__ import annotations

import re
from typing import Any

# Kept identical to (and independently of, for the same CLI-standalone-loading
# reason above) the "external" branch of
# ``metric_view_utils.mquery_parser.classify_mquery_source`` — a non-warehouse
# connector (dataflow / file / AAS) that Fabric can scan the M-Query of but
# that has no lakehouse-native SQL, so the transpiler correctly cannot resolve
# a physical source table on its own.
_EXTERNAL_MQUERY_RE = re.compile(
    r"\b(Access\.Database|Excel\.Workbook|PowerBI\.Dataflows|Dataflows|"
    r"AnalysisServices\.Database|SharePoint\.|Web\.Contents|Folder\.Files)\b"
)


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


def derive_mapping_only_tables(
    measures: list[dict],
    admin_tables: dict[str, dict],
) -> dict[str, dict]:
    """Tables with no lakehouse-native SQL but real measures → a draft entry
    each, so ``mapping_only_tables`` becomes a genuine "just land the data
    here" starting point instead of a total skip.

    Two distinct cases land here, both handled the same way from this point
    on (a ``source_table`` the tool can point a view at, so ``pipeline.py``'s
    Phase 1b measure-driven-fact promotion builds the rest — dimensions and
    measures — straight from the DAX, exactly as it already does for any
    other fact table):

      * the table isn't in the admin scan at all (measures reference a name
        Fabric never reported — typically a renamed/removed table), and
      * the table WAS scanned, so its M-Query is known, but that M-Query is a
        non-warehouse external source (Excel/SharePoint/Dataflow/AAS) with no
        SQL equivalent — previously a silent ``skip_category: external`` with
        no further coverage even though the report's own measures for it are
        fully known.

    The proposed ``source_table`` is a landing-pad name (``{catalog}.{schema}.
    <table>``, substituted with the run's real catalog/schema downstream) —
    once someone lands that data there, the emitted view/SQL/measures need no
    further changes.
    """
    mapping_only: dict[str, dict] = {}

    allocated_tables: set[str] = set()
    for m in measures:
        table = m.get("table_name", "")
        if table:
            allocated_tables.add(table)

    admin_table_names = set(admin_tables.keys())
    not_scanned = allocated_tables - admin_table_names

    external_scanned: set[str] = set()
    for tbl in allocated_tables & admin_table_names:
        info = admin_tables.get(tbl) or {}
        mquery = (
            info.get("mquery_expression") or info.get("mquery") or ""
            if isinstance(info, dict)
            else ""
        )
        if mquery and _EXTERNAL_MQUERY_RE.search(mquery):
            external_scanned.add(tbl)

    for tbl in sorted(not_scanned | external_scanned):
        tbl_measures = [
            m["measure_name"] for m in measures if m.get("table_name") == tbl
        ]
        entry: dict[str, Any] = {
            "source_table": "{catalog}.{schema}." + _to_snake_case(tbl),
            "dimensions": [],
            "aggregate_columns": [],
            "_hint": f"{len(tbl_measures)} measures: {', '.join(tbl_measures[:5])}",
        }
        if tbl in external_scanned:
            entry["_hint"] = (
                f"external source (Excel/SharePoint/Dataflow/AAS), not scanned "
                f"as warehouse SQL — {entry['_hint']}. Land this table's data at "
                f"the given source_table (matching the columns its {len(tbl_measures)} "
                "measure(s) reference) and the draft view/SQL need no further changes."
            )
        mapping_only[tbl] = entry

    return mapping_only
