"""Which report pages a viewer can actually reach, and which visuals they can see (PBIR).

A hidden page is not unreachable: a button, a bookmark, a drillthrough or a
tooltip can still show it. Reachability starts from the visible tabs and the
bookmark pane and follows page-navigation / bookmark / drillthrough buttons, page
navigators set to show hidden pages, visual tooltips (chosen, or automatic through
the tooltip page's fields) and drillthrough fields used on reached pages. A button
whose target page comes from a measure leads to the pages named in that measure's
DAX or the tables it reads (the caller passes those texts). A hidden visual (or
one in a hidden group) counts only if a bookmark shows it.

Pages reached only through a drillthrough on a MEASURE are ``measure-DT``: whether
a viewer gets there depends on the measure, so a person checks it.
"""

from __future__ import annotations

import json
import re
from collections import deque
from dataclasses import dataclass, field
from src.services.powerbi.scope.parts import JsonObj, PartTree, arr, obj, text_at

_NAVIGATION_TARGET = {
    "PageNavigation": "navigationSection",
    "Drillthrough": "drillthroughSection",
}
_DAX_STRING = re.compile(r'"([^"\n]*)"')

REACHABLE = "reachable"
MEASURE_DT = "measure-DT"
UNREACHED = "UNREACHED"


@dataclass
class Visual:
    page: str
    name: str
    type: str
    hidden: bool
    raw: JsonObj
    parent: str = ""  # visual group holding it


@dataclass
class Page:
    id: str
    name: str
    hidden: bool
    tooltip: bool
    drillthrough_fields: list[str] = field(default_factory=list)
    measure_fields: list[str] = field(default_factory=list)
    tooltip_fields: list[str] = field(default_factory=list)


def literal(prop: object) -> str | None:
    """Value of a ``{"expr": {"Literal": {"Value": "'x'"}}}`` property; None if absent."""
    value = text_at(prop, "expr", "Literal", "Value")
    return value.strip("'") if value is not None else None


def _field_name(expr: object) -> str:
    for kind in ("Measure", "Column"):
        node = obj(expr, kind)
        if node:
            entity = text_at(node, "Expression", "SourceRef", "Entity") or "?"
            return f"{entity}[{text_at(node, 'Property')}]"
    return "?"


def _segment(path: str, index: int) -> str:
    parts = path.split("/")
    return parts[index] if len(parts) > index else ""


def load_pages(report: PartTree) -> dict[str, Page]:
    pages = {}
    for path in report.glob("pages/*/page.json"):
        raw = report.json(path)
        page_id = _segment(path, 1)
        binding = obj(raw, "pageBinding")
        exprs = [
            obj(p, "fieldExpr")
            for p in arr(binding, "parameters")
            if obj(p, "fieldExpr")
        ]
        drillthrough = binding.get("type") == "Drillthrough"
        pages[page_id] = Page(
            id=page_id,
            name=text_at(raw, "displayName") or page_id,
            hidden=raw.get("visibility") == "HiddenInViewMode",
            tooltip=raw.get("type") == "Tooltip",
            drillthrough_fields=[_field_name(e) for e in exprs] if drillthrough else [],
            measure_fields=[_field_name(e) for e in exprs if "Measure" in e]
            if drillthrough
            else [],
            tooltip_fields=[_field_name(e) for e in exprs]
            if binding.get("type") == "Tooltip"
            else [],
        )
    return pages


def load_visuals(report: PartTree) -> list[Visual]:
    visuals = []
    for path in report.glob("pages/*/visuals/*/visual.json"):
        raw = report.json(path)
        kind = text_at(raw, "visual", "visualType") or (
            "group" if "visualGroup" in raw else "?"
        )
        visuals.append(
            Visual(
                _segment(path, 1),
                text_at(raw, "name") or _segment(path, 3),
                kind,
                bool(raw.get("isHidden")),
                raw,
                text_at(raw, "parentGroupName") or "",
            )
        )
    return visuals


def revealed_visuals(report: PartTree) -> set[str]:
    """Visuals some bookmark shows (listed in its state without ``display: {mode: hidden}``)."""
    shown = set()
    for path in report.glob("bookmarks/*.bookmark.json"):
        sections = obj(report.json(path), "explorationState", "sections")
        for section in sections.values():
            for name, state in obj(section, "visualContainers").items():
                if text_at(state, "singleVisual", "display", "mode") != "hidden":
                    shown.add(name)
    return shown


def load_bookmarks(report: PartTree) -> dict[str, tuple[str, str | None]]:
    """bookmark name -> (display name, page id it opens)."""
    found = {}
    for path in report.glob("bookmarks/*.bookmark.json"):
        raw = report.json(path)
        name = text_at(raw, "name") or path.rsplit("/", 1)[-1].split(".")[0]
        found[name] = (
            text_at(raw, "displayName") or "",
            text_at(raw, "explorationState", "activeSection"),
        )
    return found


def _link_properties(visual: Visual) -> list[JsonObj]:
    links = arr(visual.raw, "visual", "visualContainerObjects", "visualLink")
    return [obj(entry, "properties") for entry in links]


def _tooltips(visual: Visual) -> list[JsonObj]:
    tips = arr(visual.raw, "visual", "visualContainerObjects", "visualTooltip")
    return [obj(entry, "properties") for entry in tips]


def _tooltip_pages(visual: Visual) -> list[str]:
    return [
        page for props in _tooltips(visual) if (page := literal(props.get("section")))
    ]


def _uses_field(visual: Visual, field_name: str) -> bool:
    entity, _, prop = field_name.partition("[")
    text = json.dumps(obj(visual.raw, "visual", "query"))
    return f'"Property": "{prop[:-1]}"' in text and f'"Entity": "{entity}"' in text


def _measure_of(prop: object) -> str | None:
    return text_at(prop, "expr", "Measure", "Property")


def navigation_measures(visuals: list[Visual]) -> set[str]:
    """Measures that compute the target page of a navigation / drillthrough button."""
    return {
        measure
        for visual in visuals
        for props in _link_properties(visual)
        for key in _NAVIGATION_TARGET.values()
        if (measure := _measure_of(props.get(key)))
    }


def measure_page_targets(
    pages: dict[str, Page], measure_texts: dict[str, str]
) -> dict[str, set[str]]:
    """measure -> page ids named (display name or id) as a string in its text."""
    by_name = {page.name: pid for pid, page in pages.items()} | {
        pid: pid for pid in pages
    }
    return {
        measure: {by_name[s] for s in _DAX_STRING.findall(text) if s in by_name}
        for measure, text in measure_texts.items()
    }


def _visual_edges(
    visual: Visual, bookmarks: dict, targets: dict
) -> list[tuple[str | None, str]]:
    edges: list[tuple[str | None, str]] = []
    for props in _link_properties(visual):
        if literal(props.get("show")) == "false":
            continue
        action = literal(props.get("type"))
        if action in _NAVIGATION_TARGET:
            edges += _navigation_edges(
                props.get(_NAVIGATION_TARGET[action]), f"{action} button", targets
            )
        elif action == "Bookmark" and literal(props.get("bookmark")):
            display, page = bookmarks.get(literal(props.get("bookmark")), ("?", None))
            edges.append((page, f"bookmark button '{display}'"))
    edges += [(page, "tooltip") for page in _tooltip_pages(visual)]
    return edges


def _navigation_edges(
    prop: object, how: str, targets: dict
) -> list[tuple[str | None, str]]:
    measure = _measure_of(prop)
    if measure is None:
        return [(literal(prop), how)]
    pages = targets.get(measure)
    if not pages:
        return [(f"(target from measure [{measure}] not resolved)", how)]
    return [(page, f"{how} via measure [{measure}]") for page in sorted(pages)]


def _navigator_edges(visual: Visual, pages: dict[str, Page]) -> list[tuple[str, str]]:
    """Hidden pages a page navigator lists (only with "show hidden pages" on)."""
    if visual.type != "pageNavigator":
        return []
    entries = [obj(e) for e in arr(visual.raw, "visual", "objects", "pages")]
    if not any(
        literal(obj(e, "properties").get("showHiddenPages")) == "true"
        for e in entries
        if not e.get("selector")
    ):
        return []
    off = {
        text_at(e, "selector", "id")
        for e in entries
        if e.get("selector")
        and literal(obj(e, "properties").get("showPage")) == "false"
    }
    return [
        (pid, "page navigator")
        for pid, page in pages.items()
        if page.hidden and not page.tooltip and pid not in off
    ]


def _auto_tooltip(visual: Visual) -> bool:
    """The visual's tooltip is a report page Power BI picks by field (the default)."""
    for props in _tooltips(visual):
        if (
            literal(props.get("show")) == "false"
            or literal(props.get("type")) == "Default"
            or literal(props.get("section"))
        ):
            return False
    return True


def _clickable(
    visual: Visual, groups: dict[str, Visual], revealed: set[str] | frozenset[str]
) -> bool:
    if visual.name in revealed:
        return True
    node: Visual | None = visual
    seen: set[str] = set()
    while node is not None and node.name not in seen:
        if node.hidden and node.name not in revealed:
            return False
        seen.add(node.name)
        node = groups.get(node.parent)
    return True


def visible_visuals(
    visuals: list[Visual], revealed: set[str] | frozenset[str]
) -> list[Visual]:
    """The visuals a viewer can see: a hidden visual no bookmark shows draws nothing."""
    groups = {v.name: v for v in visuals if v.type == "group"}
    return [v for v in visuals if _clickable(v, groups, revealed)]


@dataclass
class Reachability:
    pages: dict[str, Page]
    reached: dict[str, list[str]]  # page id -> entry points
    broken: list[tuple]  # links to pages missing from the report


def reachability(
    pages: dict[str, Page],
    visuals: list[Visual],
    bookmarks: dict,
    revealed: set[str] | frozenset[str] = frozenset(),
    targets: dict | None = None,
) -> Reachability:
    """page id -> entry points, breadth-first from tabs and the bookmark pane."""
    visuals = visible_visuals(visuals, revealed)
    reached: dict[str, list[str]] = {pid: [] for pid in pages}
    queue: deque[str] = deque()

    def reach(target: str, how: str) -> None:
        if not reached[target]:
            queue.append(target)
        reached[target].append(how)

    for pid, page in pages.items():
        if not page.hidden:
            reach(pid, "tab")
    for display, page_id in bookmarks.values():
        if page_id in pages:
            reach(page_id, f"bookmark pane '{display}'")
    by_page: dict[str, list[Visual]] = {}
    for visual in visuals:
        by_page.setdefault(visual.page, []).append(visual)
    broken: list[tuple] = []
    seen: set[str] = set()
    while queue:
        source = queue.popleft()
        if source in seen:
            continue
        seen.add(source)
        links = (bookmarks, targets or {})
        for target, how in _edges_from(
            source, by_page.get(source, []), pages, links, broken
        ):
            reach(target, how)
    return Reachability(pages, reached, broken)


def _edges_from(
    source: str,
    visuals: list[Visual],
    pages: dict[str, Page],
    links: tuple,
    broken: list,
) -> list:
    if pages[source].tooltip:  # shown on hover: nothing on it can be clicked
        return []
    origin = pages[source].name
    edges = []
    for visual in visuals:
        note = " (hidden visual)" if visual.hidden else ""
        for target, how in _visual_edges(visual, *links) + _navigator_edges(
            visual, pages
        ):
            if target in pages:
                edges.append((target, f"{how} on '{origin}'{note}"))
            else:
                broken.append(
                    (origin, visual.type, visual.name, how, target, visual.hidden)
                )
    return edges + _field_edges(source, visuals, pages)


def _field_edges(
    source: str, visuals: list[Visual], pages: dict[str, Page]
) -> list[tuple[str, str]]:
    """Drillthrough pages and automatic tooltip pages a visual of ``source`` opens through a field."""
    origin = pages[source].name
    tooltip_visuals = [v for v in visuals if _auto_tooltip(v)]
    edges = []
    for target, page in pages.items():
        if target == source:
            continue
        for field_name in page.drillthrough_fields:
            if any(_uses_field(v, field_name) for v in visuals):
                kind = (
                    "measure drillthrough"
                    if field_name in page.measure_fields
                    else "drillthrough"
                )
                edges.append((target, f"{kind} from '{origin}' via {field_name}"))
        for field_name in page.tooltip_fields:
            if any(_uses_field(v, field_name) for v in tooltip_visuals):
                edges.append(
                    (target, f"automatic tooltip on '{origin}' via {field_name}")
                )
    return edges


def report_reachability(
    report: PartTree, measure_texts: dict[str, str] | None = None
) -> Reachability:
    """reachability() of a report; ``measure_texts``: measure -> DAX plus the tables it reads."""
    pages, visuals = load_pages(report), load_visuals(report)
    targets = measure_page_targets(pages, measure_texts or {})
    return reachability(
        pages, visuals, load_bookmarks(report), revealed_visuals(report), targets
    )


def page_status(entries: list[str]) -> str:
    """reachable; ``measure-DT`` if only a drillthrough on a measure leads there; else UNREACHED."""
    if any(not e.startswith("measure drillthrough") for e in entries):
        return REACHABLE
    return MEASURE_DT if entries else UNREACHED


def page_rows(result: Reachability) -> list[dict[str, str]]:
    """One row per page: status, flags, how it is reached (for the migration report)."""
    rows = []
    for pid, page in result.pages.items():
        flags = ["hidden" if page.hidden else "visible"]
        if page.tooltip:
            flags.append("tooltip")
        if page.drillthrough_fields:
            flags.append("drillthrough")
        rows.append(
            {
                "page": page.name,
                "page_id": pid,
                "status": page_status(result.reached[pid]),
                "flags": ",".join(flags),
                "reached_by": "; ".join(sorted(set(result.reached[pid]))),
            }
        )
    return rows
