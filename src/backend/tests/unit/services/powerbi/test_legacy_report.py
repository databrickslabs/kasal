"""PBIR-Legacy report.json -> PBIR parts: pages, visuals (fields per role), groups, filters, bookmarks, order."""

import base64
import json

from src.services.powerbi import legacy_report
from src.services.powerbi.visual_usage import _visual_usages_in_part


def _payload(content):
    return base64.b64encode(json.dumps(content).encode()).decode()


def _decoded(parts):
    return {p["path"]: json.loads(base64.b64decode(p["payload"])) for p in parts}


VISUAL = {
    "name": "v1",
    "parentGroupName": "g1",
    "singleVisual": {
        "visualType": "card",
        "projections": {"Values": [{"queryRef": "Sales.Net Revenue"}]},
        "prototypeQuery": {
            "From": [{"Name": "s", "Entity": "Sales", "Type": 0}],
            "Select": [
                {
                    "Measure": {
                        "Expression": {"SourceRef": {"Source": "s"}},
                        "Property": "Net Revenue",
                    },
                    "Name": "Sales.Net Revenue",
                    "NativeReferenceName": "Net Revenue",
                }
            ],
        },
        "objects": {},
        "vcObjects": {
            "visualLink": [
                {
                    "properties": {
                        "show": {"expr": {"Literal": {"Value": "true"}}},
                        "type": {"expr": {"Literal": {"Value": "'PageNavigation'"}}},
                        "navigationSection": {"expr": {"Literal": {"Value": "'p2'"}}},
                    }
                }
            ]
        },
    },
}
FILTER = [
    {
        "name": "f1",
        "type": "Categorical",
        "expression": {
            "Column": {
                "Expression": {"SourceRef": {"Entity": "Dim"}},
                "Property": "country",
            }
        },
        "filter": {
            "Version": 2,
            "From": [{"Name": "d", "Entity": "Dim", "Type": 0}],
            "Where": [],
        },
    }
]
REPORT = {
    "config": json.dumps(
        {
            "activeSectionIndex": 0,
            "bookmarks": [
                {
                    "name": "grp",
                    "displayName": "Group",
                    "children": [
                        {
                            "name": "b1",
                            "displayName": "Show p2",
                            "explorationState": {"activeSection": "p2", "sections": {}},
                        }
                    ],
                }
            ],
        }
    ),
    "filters": json.dumps(FILTER),
    "sections": [
        {
            "name": "p2",
            "displayName": "Detail",
            "ordinal": 1,
            "config": json.dumps({"visibility": 1}),
            "filters": "[]",
            "visualContainers": [],
        },
        {
            "name": "p1",
            "displayName": "Overview",
            "ordinal": 0,
            "config": "{}",
            "filters": json.dumps(FILTER),
            "visualContainers": [
                {"config": json.dumps(VISUAL), "filters": json.dumps(FILTER)},
                {
                    "config": json.dumps(
                        {
                            "name": "g1",
                            "singleVisualGroup": {
                                "displayName": "Group",
                                "isHidden": True,
                            },
                        }
                    )
                },
            ],
        },
    ],
}
PARTS = [
    {"path": "report.json", "payload": _payload(REPORT), "payloadType": "InlineBase64"},
    {"path": "definition.pbir", "payload": _payload({})},
]


def _converted():
    return _decoded(
        p for p in legacy_report.to_pbir_parts(PARTS) if p["path"] != "definition.pbir"
    )


def test_pages_order_visibility_and_filters():
    parts = _converted()
    assert parts["definition/pages/pages.json"] == {
        "pageOrder": ["p1", "p2"],
        "activePageName": "p1",
    }
    assert parts["definition/pages/p2/page.json"]["visibility"] == "HiddenInViewMode"
    assert "visibility" not in parts["definition/pages/p1/page.json"]
    page_filter = parts["definition/pages/p1/page.json"]["filterConfig"]["filters"][0]
    assert page_filter["field"]["Column"]["Property"] == "country"
    assert "expression" not in page_filter
    assert parts["definition/report.json"]["filterConfig"]["filters"][0]["name"] == "f1"


def test_visual_fields_resolve_aliases_to_tables():
    visual = _converted()["definition/pages/p1/visuals/v1/visual.json"]
    [projection] = visual["visual"]["query"]["queryState"]["Values"]["projections"]
    assert projection["field"] == {
        "Measure": {
            "Expression": {"SourceRef": {"Entity": "Sales"}},
            "Property": "Net Revenue",
        }
    }
    assert visual["parentGroupName"] == "g1"
    assert visual["visual"]["visualContainerObjects"]["visualLink"]


def test_groups_and_bookmarks():
    parts = _converted()
    group = parts["definition/pages/p1/visuals/g1/visual.json"]
    assert group["visualGroup"] == {"displayName": "Group"}
    assert group["isHidden"] is True
    assert (
        parts["definition/bookmarks/b1.bookmark.json"]["explorationState"][
            "activeSection"
        ]
        == "p2"
    )


def test_the_readers_see_the_converted_fields():
    converted = legacy_report.to_pbir_parts(PARTS)
    visual_part = next(p for p in converted if p["path"].endswith("v1/visual.json"))
    decoded_visual = {
        "path": visual_part["path"],
        "payload": _converted()[visual_part["path"]],
    }
    usages = _visual_usages_in_part(decoded_visual, {"p1": "Overview"})
    fields = {(u["field"], u["role"]) for u in usages}
    assert ("Net Revenue", "drawn") in fields
    assert ("country", "filter") in fields


def test_a_pbir_definition_is_left_unchanged():
    pbir = [
        {"path": "definition/pages/p1/page.json", "payload": _payload({"name": "p1"})}
    ]
    assert legacy_report.to_pbir_parts(pbir) == pbir


def test_conversion_failure_is_recognised():
    assert legacy_report.is_conversion_failure("Report_Report_FailedToExportReport: x")
    assert not legacy_report.is_conversion_failure("HTTP 404: not found")
