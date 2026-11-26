"""Report definition (PBIR): which pages a viewer can reach, and which visuals they can see."""

import base64
import json

from src.services.powerbi.scope import report_reachability as rr
from src.services.powerbi.scope.parts import PartTree


def _lit(value: str) -> dict:
    return {"expr": {"Literal": {"Value": f"'{value}'"}}}


def _link(action: str, **props) -> dict:
    show = {"expr": {"Literal": {"Value": "true"}}}
    return {
        "visualContainerObjects": {
            "visualLink": [
                {"properties": {"show": show, "type": _lit(action), **props}}
            ]
        }
    }


def _measure_expr(name: str) -> dict:
    return {
        "expr": {
            "Measure": {
                "Expression": {"SourceRef": {"Entity": "Nav"}},
                "Property": name,
            }
        }
    }


def _definition() -> PartTree:
    measure = {
        "Measure": {
            "Expression": {"SourceRef": {"Entity": "Fact"}},
            "Property": "Sales",
        }
    }
    column = {
        "Column": {
            "Expression": {"SourceRef": {"Entity": "Dim"}},
            "Property": "Category",
        }
    }
    hidden = {"visibility": "HiddenInViewMode"}
    pages = {
        "home": {"displayName": "Home"},
        "nav": {"displayName": "Navigated", **hidden},
        "bm": {"displayName": "Bookmarked", **hidden},
        "tip": {"displayName": "Tip", **hidden, "type": "Tooltip"},
        "dt": {
            "displayName": "Detail",
            **hidden,
            "pageBinding": {
                "type": "Drillthrough",
                "parameters": [{"fieldExpr": column}],
            },
        },
        "mdt": {
            "displayName": "Measure detail",
            **hidden,
            "pageBinding": {
                "type": "Drillthrough",
                "parameters": [{"fieldExpr": measure}],
            },
        },
        "orphan": {"displayName": "Orphan", **hidden},
    }
    files: dict = {f"pages/{pid}/page.json": page for pid, page in pages.items()}
    files["bookmarks/b1.bookmark.json"] = {
        "name": "b1",
        "displayName": "B one",
        "explorationState": {"activeSection": "bm"},
    }
    query = {
        "queryState": {
            "Values": {"projections": [{"field": column}, {"field": measure}]}
        }
    }
    tooltip = {"visualTooltip": [{"properties": {"section": _lit("tip")}}]}
    visuals = {
        "v1": {
            "visualType": "actionButton",
            **_link("PageNavigation", navigationSection=_lit("nav")),
        },
        "v2": {
            "visualType": "actionButton",
            **_link("Bookmark"),
        },  # info button: no target
        "v3": {
            "visualType": "actionButton",
            **_link("PageNavigation", navigationSection=_lit("gone")),
        },
        "v4": {"visualType": "card", "query": query, "visualContainerObjects": tooltip},
    }
    for vid, visual in visuals.items():
        files[f"pages/home/visuals/{vid}/visual.json"] = {"name": vid, "visual": visual}
    return PartTree(files)


def test_hidden_pages_are_reached_through_buttons_bookmarks_tooltips_and_drillthrough():
    tree = _definition()
    result = rr.reachability(
        rr.load_pages(tree), rr.load_visuals(tree), rr.load_bookmarks(tree)
    )
    status = {
        result.pages[pid].name: rr.page_status(e) for pid, e in result.reached.items()
    }
    assert status == {
        "Home": "reachable",
        "Navigated": "reachable",
        "Bookmarked": "reachable",
        "Tip": "reachable",
        "Detail": "reachable",
        "Measure detail": "measure-DT",
        "Orphan": "UNREACHED",
    }
    # the info button (bookmark action without a bookmark) is not a broken link
    assert [b[4] for b in result.broken] == ["gone"]


def test_measure_buttons_navigators_automatic_tooltips_and_hidden_buttons():
    column = {
        "Column": {
            "Expression": {"SourceRef": {"Entity": "Dim"}},
            "Property": "Category",
        }
    }
    hidden = {"visibility": "HiddenInViewMode"}
    pages = {
        "home": {"displayName": "Home"},
        "byname": {"displayName": "By Name", **hidden},
        "listed": {"displayName": "Listed", **hidden},
        "off": {"displayName": "Switched Off", **hidden},
        "tip": {
            "displayName": "Auto Tip",
            **hidden,
            "type": "Tooltip",
            "pageBinding": {"type": "Tooltip", "parameters": [{"fieldExpr": column}]},
        },
        "secret": {"displayName": "Secret", **hidden},
        "shown": {"displayName": "Shown", **hidden},
    }
    files: dict = {f"pages/{pid}/page.json": page for pid, page in pages.items()}
    false = {"expr": {"Literal": {"Value": "false"}}}
    navigator = {
        "pages": [
            {
                "properties": {
                    "showHiddenPages": {"expr": {"Literal": {"Value": "true"}}}
                }
            },
            {"properties": {"showPage": false}, "selector": {"id": "off"}},
            {"properties": {"showPage": false}, "selector": {"id": "secret"}},
            {"properties": {"showPage": false}, "selector": {"id": "shown"}},
        ]
    }
    query = {"queryState": {"Category": {"projections": [{"field": column}]}}}
    button = "actionButton"
    visuals = {
        "measure": {
            "visual": {
                "visualType": button,
                **_link("PageNavigation", navigationSection=_measure_expr("Go")),
            }
        },
        "unknown": {
            "visual": {
                "visualType": button,
                **_link("PageNavigation", navigationSection=_measure_expr("Nope")),
            }
        },
        "nav": {"visual": {"visualType": "pageNavigator", "objects": navigator}},
        "chart": {"visual": {"visualType": "barChart", "query": query}},
        "hidden": {
            "isHidden": True,
            "visual": {
                "visualType": button,
                **_link("PageNavigation", navigationSection=_lit("secret")),
            },
        },
        "grouped": {
            "parentGroupName": "g",
            "visual": {
                "visualType": button,
                **_link("PageNavigation", navigationSection=_lit("shown")),
            },
        },
        "g": {"isHidden": True, "visualGroup": {"displayName": "g"}},
    }
    for vid, visual in visuals.items():
        files[f"pages/home/visuals/{vid}/visual.json"] = {"name": vid, **visual}
    revealing = {"singleVisual": {"visualType": button}}
    files["bookmarks/b.bookmark.json"] = {
        "name": "b",
        "explorationState": {
            "activeSection": "home",
            "sections": {"home": {"visualContainers": {"grouped": revealing}}},
        },
    }
    tree = PartTree(files)

    assert rr.navigation_measures(rr.load_visuals(tree)) == {"Go", "Nope"}
    texts = {"Go": 'SWITCH(SELECTEDVALUE(Nav[k]), 1, "By Name", 2, "Not A Page")'}
    result = rr.report_reachability(tree, texts)
    status = {
        result.pages[pid].name: rr.page_status(e) for pid, e in result.reached.items()
    }
    assert status == {
        "Home": "reachable",
        "By Name": "reachable",
        "Listed": "reachable",
        "Switched Off": "UNREACHED",
        "Auto Tip": "reachable",
        "Secret": "UNREACHED",
        "Shown": "reachable",
    }
    assert [b[4] for b in result.broken] == [
        "(target from measure [Nope] not resolved)"
    ]


def test_part_tree_reads_fabric_parts_and_skips_packaged_resources():
    page = {"displayName": "Home"}
    parts = [
        {
            "path": "definition/pages/home/page.json",
            "payload": base64.b64encode(json.dumps(page).encode()).decode(),
        },
        {
            "path": "definition/pages/home/visuals/v/visual.json",
            "payload": {"name": "v"},
        },
        {"path": "CustomVisuals/PRIV_X/resources/PRIV_X.pbiviz.json", "payload": "{}"},
    ]
    tree = PartTree.from_report_definition({"definition": {"parts": parts}})
    assert tree.glob("pages/*/page.json") == ["pages/home/page.json"]
    assert tree.json("pages/home/page.json") == page
    assert tree.glob("pages/*/visuals/*/visual.json") == [
        "pages/home/visuals/v/visual.json"
    ]
    assert not tree.glob("CustomVisuals/*/resources/*")
    assert tree.glob("pages/*.json") == []  # one segment per *
