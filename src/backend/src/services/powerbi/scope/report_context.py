"""The rest of the triage evidence next to the scope table: filters, page scopes, links, inventory.

``build_report_scope`` returns these alongside the rows, so a reviewer (and the
reconciliation mapping drafts) see in one place:

- every report / page / visual filter and slicer selection, and the page scopes
  they form (pages with identical filter sets), each with the tables it reaches;
- per measure, the page filters every page drawing it shares — the mapping's
  ``extra_filter`` (the config generator hands these on as
  ``page_filters_by_measure``);
- every URL in a visual, the reports it links to, and sibling reports on the
  same semantic model;
- a table inventory: partition kind, source class, columns / measures,
  calculated columns, how the report uses it, and the scope decision.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping

from src.services.powerbi.scope.closure import Closure, Seeds
from src.services.powerbi.scope.parts import PartTree
from src.services.powerbi.scope.report_filters import (
    collect_filters,
    measure_page_filters,
    page_scopes,
)
from src.services.powerbi.scope.report_links import related_reports, report_links
from src.services.powerbi.scope.tmdl_model import Model, source_class


def inventory(model: Model, seeds: Seeds, closure: Closure, rows: list[dict]) -> list[dict[str, object]]:
    """One row per table: what it is, where its data comes from, and how the report uses it."""
    decisions = {r["pbi_table"]: r["decision"] for r in rows if r["object_type"] == "table"}
    drawn = Counter(k[1] for k in seeds.drawn if k[0] == "c")
    filtered = Counter(k[1] for k in seeds.filters if k[0] == "c")
    out: list[dict[str, object]] = []
    for name in sorted(model.tables, key=str.lower):
        table = model.tables[name]
        out.append(
            {
                "table": name,
                "partition": table.kind,
                "source": source_class(table, model),
                "columns": len(table.columns),
                "calculated_columns": sum(1 for dax in table.columns.values() if dax),
                "measures": len(table.measures),
                "calculation_items": len(table.calc_items),
                "columns_drawn": drawn[name],
                "columns_filter_only": filtered[name],
                "reached": name in closure.tables,
                "decision": decisions.get(name, ""),
            }
        )
    return out


def report_context(
    model: Model,
    seeds: Seeds,
    closure: Closure,
    report: PartTree,
    rows: list[dict],
    *,
    scan_result: Mapping[str, object] | None = None,
    dataset_id: str = "",
    report_id: str = "",
) -> dict[str, object]:
    filters = collect_filters(report)
    scopes = page_scopes(filters, model)
    links = report_links(report)
    return {
        "filters": filters,
        "page_scopes": scopes,
        "measure_page_filters": measure_page_filters(seeds.drawn, scopes, model),
        "links": links,
        "related_reports": related_reports(links, scan_result, dataset_id, report_id),
        "inventory": inventory(model, seeds, closure, rows),
    }
