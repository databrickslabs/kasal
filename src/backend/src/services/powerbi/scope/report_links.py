"""URLs in a report's visuals, the Power BI reports they link to, and sibling reports on the same model.

Linked reports (navigation buttons / URLs to other reports) are separate models,
so separate schemas and Genie spaces: they are listed for a person to decide,
never translated automatically. A button whose action type is not ``WebUrl``
keeps an old URL without using it — such URLs are marked unused.

Sibling reports (other reports on the same semantic model) belong in the visual
inventory too, or the schema serves one report and breaks for the others. They
come from the Admin Scanner payload the Pipeline Config Generator already holds
(``workspaces[].reports[].datasetId``), so no extra API call is made.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from typing import TypedDict

from src.services.powerbi.scope import report_reachability as rr
from src.services.powerbi.scope.parts import PartTree, arr, text_at

_REPORT_URL = re.compile(r"app\.powerbi\.com/groups/([0-9a-f-]{36})/reports/([0-9a-f-]{36})", re.I)
_SCHEMA_URL = "https://developer.microsoft.com/json-schemas"


class LinkRow(TypedDict):
    page: str
    page_hidden: bool
    visual_type: str
    visual_hidden: bool
    kind: str
    url: str


class ReportRef(TypedDict):
    workspace_id: str
    report_id: str
    name: str
    dataset_id: str


def _urls(node: object, path: str = "") -> Iterator[tuple[str, str]]:
    if isinstance(node, dict):
        for key, value in node.items():
            yield from _urls(value, f"{path}/{key}")
    elif isinstance(node, list):
        for value in node:
            yield from _urls(value, path)
    elif isinstance(node, str) and "://" in node and not node.startswith(_SCHEMA_URL):
        yield path, node.strip("'")


def url_kind(path: str, action: str | None) -> str:
    """How a URL found at ``path`` of a visual is used."""
    if "/visualLink/" in path and path.endswith("/webUrl/expr/Literal/Value"):
        return "button WebUrl action" if action == "WebUrl" else f"unused (button action is {action})"
    if "/textRuns/" in path or path.endswith("/url"):
        return "text hyperlink"
    return "other"


def report_links(report: PartTree) -> list[LinkRow]:
    pages = rr.load_pages(report)
    rows: list[LinkRow] = []
    for visual in rr.load_visuals(report):
        links = arr(visual.raw, "visual", "visualContainerObjects", "visualLink")
        actions = [rr.literal(text_props) for text_props in (_type_prop(e) for e in links)]
        page = pages.get(visual.page)
        for path, url in _urls(visual.raw):
            rows.append(
                {
                    "page": page.name if page else visual.page,
                    "page_hidden": bool(page and page.hidden),
                    "visual_type": visual.type,
                    "visual_hidden": visual.hidden,
                    "kind": url_kind(path, actions[0] if actions else None),
                    "url": url,
                }
            )
    return sorted(rows, key=lambda r: (r["kind"], r["page"], r["url"]))


def _type_prop(entry: object) -> object:
    props = entry.get("properties") if isinstance(entry, dict) else None
    return props.get("type") if isinstance(props, dict) else None


def linked_report_ids(rows: list[LinkRow]) -> list[tuple[str, str]]:
    """Sorted (workspace id, report id) of every Power BI report URL that is actually used."""
    return sorted(
        {
            (m.group(1).lower(), m.group(2).lower())
            for r in rows
            if not r["kind"].startswith("unused")
            for m in [_REPORT_URL.search(r["url"])]
            if m
        }
    )


def _scanned_reports(scan_result: Mapping[str, object] | None) -> list[ReportRef]:
    found: list[ReportRef] = []
    for workspace in arr(scan_result, "workspaces"):
        ws_id = (text_at(workspace, "id") or "").lower()
        for rep in arr(workspace, "reports"):
            found.append(
                {
                    "workspace_id": ws_id,
                    "report_id": (text_at(rep, "id") or "").lower(),
                    "name": text_at(rep, "name") or "",
                    "dataset_id": (text_at(rep, "datasetId") or "").lower(),
                }
            )
    return found


def related_reports(
    rows: list[LinkRow],
    scan_result: Mapping[str, object] | None,
    dataset_id: str,
    report_id: str = "",
) -> dict[str, list[ReportRef]]:
    """``{siblings, linked}``: other reports on the same model, and the reports the visuals link to."""
    scanned = _scanned_reports(scan_result)
    by_id = {r["report_id"]: r for r in scanned}
    siblings = [
        r for r in scanned if r["dataset_id"] == dataset_id.lower() and r["report_id"] != report_id.lower()
    ]
    linked: list[ReportRef] = []
    for workspace, rep in linked_report_ids(rows):
        known = by_id.get(rep)
        linked.append(known or {"workspace_id": workspace, "report_id": rep, "name": "", "dataset_id": ""})
    return {"siblings": siblings, "linked": linked}
