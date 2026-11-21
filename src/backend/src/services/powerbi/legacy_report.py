"""PBIR-Legacy report definitions (one ``report.json`` with ``sections``) converted to PBIR parts.

The report readers (visual usage, the scope closure, the references tool) read the PBIR layout:
``definition/pages/<page>/page.json``, ``.../visuals/<visual>/visual.json``,
``definition/bookmarks/*.bookmark.json``, ``definition/report.json``. Fabric converts a legacy report on request
(``getDefinition?format=PBIR``), but some reports fail with ``FailedToExportReport``. For those the legacy
definition is fetched and converted here, so every reader works unchanged.

Converted: pages (display name, visibility, type, page binding, filters), visuals (type, fields per role with
``queryRef`` resolved through the ``prototypeQuery``, aliases replaced by the table, objects, container objects,
filters, group membership, hidden state), visual groups, bookmarks (groups flattened), report filters and the
page order. Layout, themes and resources are not needed by the readers and are not converted.

Parts are ``{"path", "payload", "payloadType"}`` with base64 payloads, the shape the service returns, so this
runs before the payloads are decoded.
"""

from __future__ import annotations

import base64
import json

CONVERSION_FAILURE = (
    "FailedToExportReport"  # errorCode of a failed format=PBIR conversion
)
_PAGES = "definition/pages/"
_REPORT_PATHS = ("report.json", "definition/report.json")

Json = dict[str, object]


def is_conversion_failure(message: str) -> bool:
    """True when an error message is the service's failed PBIR conversion."""
    return CONVERSION_FAILURE in (message or "")


def _as_dict(value: object) -> Json:
    return value if isinstance(value, dict) else {}


def _as_list(value: object) -> list[object]:
    return value if isinstance(value, list) else []


def _json(text: object, default: object) -> object:
    """A JSON value that the legacy model stores either as an object or as a JSON string."""
    if isinstance(text, (dict, list)):
        return text
    if not isinstance(text, str) or not text:
        return default
    try:
        return json.loads(text)
    except ValueError:
        return default


def _part(path: str, content: Json) -> Json:
    encoded = base64.b64encode(json.dumps(content).encode("utf-8")).decode("ascii")
    return {"path": path, "payload": encoded, "payloadType": "InlineBase64"}


def _with_entities(node: object, aliases: dict[str, object]) -> object:
    """Replace ``SourceRef: {Source: <alias>}`` by ``SourceRef: {Entity: <table>}`` throughout an expression."""
    if isinstance(node, dict):
        if set(node) == {"Source"} and node["Source"] in aliases:
            return {"Entity": aliases[str(node["Source"])]}
        return {key: _with_entities(value, aliases) for key, value in node.items()}
    if isinstance(node, list):
        return [_with_entities(value, aliases) for value in node]
    return node


def _query_state(single: Json) -> Json:
    """PBIR ``queryState`` from the legacy projections and the prototype query."""
    query = _as_dict(single.get("prototypeQuery"))
    aliases = {
        str(source.get("Name")): source.get("Entity")
        for source in (_as_dict(s) for s in _as_list(query.get("From")))
    }
    selects = {
        str(item.get("Name")): item
        for item in (_as_dict(s) for s in _as_list(query.get("Select")))
    }
    state: Json = {}
    for role, projections in _as_dict(single.get("projections")).items():
        converted: list[object] = []
        for projection in (_as_dict(p) for p in _as_list(projections)):
            ref = projection.get("queryRef")
            item = selects.get(str(ref))
            if item is None:
                continue
            field = {
                key: value
                for key, value in item.items()
                if key not in ("Name", "NativeReferenceName")
            }
            entry: Json = {"field": _with_entities(field, aliases), "queryRef": ref}
            if "active" in projection:
                entry["active"] = projection["active"]
            converted.append(entry)
        state[role] = {"projections": converted}
    return state


def _filters(text: object) -> Json:
    """PBIR ``filterConfig`` from a legacy filter list (``expression`` becomes ``field``)."""
    filters: list[object] = []
    for legacy in (_as_dict(f) for f in _as_list(_json(text, []))):
        converted: Json = {
            key: value for key, value in legacy.items() if key != "expression"
        }
        if "expression" in legacy:
            converted["field"] = legacy["expression"]
        filters.append(converted)
    return {"filters": filters} if filters else {}


def _page(section: Json) -> Json:
    config = _as_dict(_json(section.get("config"), {}))
    page: Json = {
        "name": section.get("name"),
        "displayName": section.get("displayName", section.get("name")),
    }
    if config.get("visibility") == 1:
        page["visibility"] = "HiddenInViewMode"
    binding = _as_dict(config.get("pageBinding") or section.get("pageBinding"))
    if binding:
        page["pageBinding"] = binding
    if binding.get("type") == "Tooltip" or config.get("type") in ("Tooltip", 1):
        page["type"] = "Tooltip"
    filter_config = _filters(section.get("filters"))
    if filter_config:
        page["filterConfig"] = filter_config
    return page


def _visual(container: Json) -> Json:
    config = _as_dict(_json(container.get("config"), {}))
    raw: Json = {"name": config.get("name") or container.get("id")}
    if config.get("parentGroupName"):
        raw["parentGroupName"] = config["parentGroupName"]
    if "singleVisualGroup" in config:
        group = _as_dict(config["singleVisualGroup"])
        raw["visualGroup"] = {
            key: value for key, value in group.items() if key != "isHidden"
        }
        if group.get("isHidden"):
            raw["isHidden"] = True
        return raw
    single = _as_dict(config.get("singleVisual"))
    raw["visual"] = {
        "visualType": single.get("visualType", "unknown"),
        "query": {"queryState": _query_state(single)},
        "objects": single.get("objects") or {},
        "visualContainerObjects": single.get("vcObjects") or {},
    }
    if _as_dict(single.get("display")).get("mode") == "hidden":
        raw["isHidden"] = True
    filter_config = _filters(container.get("filters"))
    if filter_config:
        raw["filterConfig"] = filter_config
    return raw


def _bookmarks(report_config: Json) -> list[Json]:
    """Bookmarks that carry an ``explorationState``; bookmark groups (``children``) are flattened."""
    found: list[Json] = []
    for bookmark in (_as_dict(b) for b in _as_list(report_config.get("bookmarks"))):
        found += [
            child
            for child in (_as_dict(c) for c in _as_list(bookmark.get("children")))
            if "explorationState" in child
        ]
        if "explorationState" in bookmark:
            found.append(bookmark)
    return found


def _legacy_report(parts: list[Json]) -> tuple[Json | None, Json]:
    legacy = next((p for p in parts if p.get("path") in _REPORT_PATHS), None)
    if legacy is None:
        return None, {}
    payload = base64.b64decode(str(legacy.get("payload", ""))).decode("utf-8")
    return legacy, _as_dict(_json(payload, {}))


def to_pbir_parts(parts: list[Json]) -> list[Json]:
    """The definition's parts with the PBIR-layout parts added.

    Returned unchanged when the definition has no legacy ``report.json`` with ``sections`` (a PBIR definition).
    """
    legacy, report = _legacy_report(parts)
    sections = [_as_dict(s) for s in _as_list(report.get("sections"))]
    if not sections:
        return parts
    report_config = _as_dict(_json(report.get("config"), {}))
    ordered = sorted(sections, key=lambda s: _ordinal(s))
    converted = [p for p in parts if p is not legacy]
    converted.append(
        _part(
            "definition/report.json", {"filterConfig": _filters(report.get("filters"))}
        )
    )
    active = report_config.get("activeSectionIndex", 0)
    order = [str(s.get("name")) for s in ordered]
    active_index = active if isinstance(active, int) else 0
    active_page = (
        order[active_index]
        if 0 <= active_index < len(order)
        else (order[0] if order else "")
    )
    converted.append(
        _part(
            _PAGES + "pages.json", {"pageOrder": order, "activePageName": active_page}
        )
    )
    for section in ordered:
        page = str(section.get("name"))
        converted.append(_part(f"{_PAGES}{page}/page.json", _page(section)))
        for container in (
            _as_dict(c) for c in _as_list(section.get("visualContainers"))
        ):
            visual = _visual(container)
            converted.append(
                _part(f"{_PAGES}{page}/visuals/{visual['name']}/visual.json", visual)
            )
    for bookmark in _bookmarks(report_config):
        converted.append(
            _part(
                f"definition/bookmarks/{bookmark.get('name')}.bookmark.json", bookmark
            )
        )
    return converted


def _ordinal(section: Json) -> int:
    value = section.get("ordinal", 0)
    return value if isinstance(value, int) else 0
