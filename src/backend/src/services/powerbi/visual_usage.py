"""Tally which report page(s)/visual(s) draw or filter each PBI field.

Ported from the reference ``dqa/pbi_visual_usage.py`` (the
daar-data-quality-analytics-genai repo's standalone visual-usage tool),
adapted to reuse data Kasal already has in memory instead of making a second
API call: ``pipeline_config_generator_tool.py``'s "API 4" already fetches
the Fabric ``getDefinition`` endpoint (the same one the reference module
calls directly) via ``extract_report_definition``, and already base64-decodes
+ JSON-parses every PBIR part — so ``derive_visual_usage_index`` here just
walks that already-decoded ``parts`` list rather than re-fetching or
re-decoding anything. Confirmed live against otc_management's real PBIR
payload that the part-path convention (``definition/pages/<pageId>/
page.json``, ``definition/pages/<pageId>/visuals/<visualId>/visual.json``)
and the field-reference shape (``field.Measure.Property`` /
``field.Column.Property``, ``visual.query.queryState.<role>.projections``,
``visual.filterConfig.filters``) match the reference module's assumptions
exactly — this is standard PBIR, not a tenant-specific quirk.

Only a token with the Report Definition scope can call ``getDefinition`` at
all (the reference module's own docstring confirms: SP tokens succeed, SA
tokens 403 with InsufficientScopes) — but since this module never makes the
call itself, that requirement lives entirely with whichever caller supplies
``report_def`` (``pipeline_config_generator_tool.py``, which already resolves
the right credential for "API 4").
"""

from __future__ import annotations

from collections import defaultdict

# Every PBIR part (page.json / visual.json) lives under this path prefix.
_PAGES_PREFIX = "definition/pages/"


def _field_name(field_ref: dict) -> str | None:
    """``{Measure: {..., Property: "X"}}`` or ``{Column: {...}}`` -> ``"X"``."""
    if "Measure" in field_ref:
        return field_ref["Measure"].get("Property")
    if "Column" in field_ref:
        return field_ref["Column"].get("Property")
    return None


def _page_display_names(parts: list[dict]) -> dict[str, str]:
    """``{page_id: displayName}`` for every ``page.json`` part.

    Falls back to the raw page_id (never raises) when a page's own JSON
    couldn't be decoded, so a lookup miss downstream is harmless.
    """
    page_names: dict[str, str] = {}
    for part in parts:
        path = part.get("path", "")
        if not (path.startswith(_PAGES_PREFIX) and path.endswith("/page.json")):
            continue
        segments = path.split("/")
        if len(segments) < 3:
            continue
        page_id = segments[2]
        payload = part.get("payload")
        if isinstance(payload, dict):
            page_names[page_id] = payload.get("displayName", page_id)
    return page_names


def _visual_usages_in_part(
    part: dict, page_names: dict[str, str]
) -> list[dict[str, str]]:
    """Every drawn/filter field occurrence in one ``visual.json`` part.

    A field that's both drawn and filtered on the same visual is reported
    once, as "drawn" (the more informative of the two roles).
    """
    path = part.get("path", "")
    segments = path.split("/")
    page_id = segments[2] if len(segments) > 2 else ""
    page_name = page_names.get(page_id) or page_id

    vj = part.get("payload")
    if not isinstance(vj, dict):
        return []

    visual = vj.get("visual", {})
    visual_type = visual.get("visualType", "unknown")

    occurrences: list[dict[str, str]] = []
    seen_drawn: set[str] = set()
    query_state = visual.get("query", {}).get("queryState", {})
    for role_block in query_state.values():
        for proj in role_block.get("projections", []):
            name = _field_name(proj.get("field", {}))
            if name:
                occurrences.append(
                    {
                        "field": name,
                        "page": page_name,
                        "visual_type": visual_type,
                        "role": "drawn",
                    }
                )
                seen_drawn.add(name)

    for filt in vj.get("filterConfig", {}).get("filters", []):
        name = _field_name(filt.get("field", {}))
        if name and name not in seen_drawn:
            occurrences.append(
                {
                    "field": name,
                    "page": page_name,
                    "visual_type": visual_type,
                    "role": "filter",
                }
            )

    return occurrences


def derive_visual_usage_index(
    report_def: dict | None,
) -> dict[str, list[dict[str, str]]]:
    """``{field_name: [{page, visual_type, role}, ...]}`` across the report.

    ``field_name`` is the PBI measure/column's ORIGINAL (non-snake-cased)
    name, matching ``measures_json``'s ``original_name`` — the caller joins
    on that, not on the UCMV's snake_case measure name.
    """
    if not report_def:
        return {}
    definition = report_def.get("definition", report_def)
    parts = definition.get("parts", [])
    if not parts:
        return {}

    page_names = _page_display_names(parts)
    usage: dict[str, list[dict[str, str]]] = defaultdict(list)
    for part in parts:
        path = part.get("path", "")
        if not (path.startswith(_PAGES_PREFIX) and path.endswith("visual.json")):
            continue
        for occ in _visual_usages_in_part(part, page_names):
            field = occ.pop("field")
            usage[field].append(occ)
    return dict(usage)
