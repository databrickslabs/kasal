"""Filters, page scopes, measure page filters, links / related reports, inventory and the scope check."""

from src.services.powerbi.scope import report_filters as rf
from src.services.powerbi.scope import report_links as rl
from src.services.powerbi.scope.parts import PartTree
from src.services.powerbi.scope.report_context import report_context
from src.services.powerbi.scope.scope_check import check_scope
from src.services.powerbi.scope.scope_table import build_scope
from src.services.powerbi.scope.tmdl_model import Model, Table

DATABRICKS_M = 'Value.NativeQuery(Databricks.Catalogs("h", "/p", []), "select 1")'
WS = "11111111-1111-1111-1111-111111111111"
REP = "22222222-2222-2222-2222-222222222222"
DS = "33333333-3333-3333-3333-333333333333"


def _column(table, name):
    return {"Column": {"Expression": {"SourceRef": {"Entity": table}}, "Property": name}}


def _measure(name, entity="Fact"):
    return {"Measure": {"Expression": {"SourceRef": {"Entity": entity}}, "Property": name}}


def _in_filter(table, column, values):
    where = {
        "Condition": {
            "In": {
                "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "d"}}, "Property": column}}],
                "Values": [[{"Literal": {"Value": f"'{v}'"}}] for v in values],
            }
        }
    }
    return {"field": _column(table, column), "filter": {"From": [{"Name": "d", "Entity": table}], "Where": [where]}}


def _visual(*fields, kind="tableEx", extra=None):
    projections = [{"field": f} for f in fields]
    visual = {"visualType": kind, "query": {"queryState": {"Values": {"projections": projections}}}, **(extra or {})}
    return {"visual": visual}


def _report():
    lines = {"filterConfig": {"filters": [_in_filter("Line", "type", ["A", "B"])]}}
    files = {
        "pages/p1/page.json": {"displayName": "Operations", **lines},
        "pages/p2/page.json": {"displayName": "Detail", **lines},
        "pages/p3/page.json": {
            "displayName": "Other",
            "filterConfig": {"filters": [_in_filter("Line", "type", ["A", "B", "C"])]},
        },
        "pages/p1/visuals/v1/visual.json": {"name": "v1", **_visual(_measure("Output"), _measure("Shared"))},
        "pages/p2/visuals/v2/visual.json": {"name": "v2", **_visual(_measure("Output"))},
        "pages/p3/visuals/v3/visual.json": {"name": "v3", **_visual(_measure("Shared"))},
        "pages/p3/visuals/v4/visual.json": {
            "name": "v4",
            **_visual(
                kind="actionButton",
                extra={
                    "visualContainerObjects": {
                        "visualLink": [
                            {
                                "properties": {
                                    "type": {"expr": {"Literal": {"Value": "'WebUrl'"}}},
                                    "webUrl": {
                                        "expr": {
                                            "Literal": {
                                                "Value": f"'https://app.powerbi.com/groups/{WS}/reports/{REP}/x'"
                                            }
                                        }
                                    },
                                }
                            }
                        ]
                    }
                },
            ),
        },
        "pages/p1/visuals/s1/visual.json": {
            "name": "s1",
            **_visual(
                _column("Line", "type"),
                kind="slicer",
                extra={"objects": {"general": [{"properties": {"filter": {"filter": _in_filter("Line", "type", ["A"])["filter"]}}}]}},
            ),
        },
    }
    return PartTree(files)


def _model():
    fact = Table("Fact", "m", DATABRICKS_M, columns={"line": None, "x": None}, measures={"Output": "SUM(Fact[x])", "Shared": "1"})
    line = Table("Line", "m", DATABRICKS_M, columns={"line": None, "type": None})
    other = Table("Other", "m", DATABRICKS_M, columns={"y": None})
    return Model({"Fact": fact, "Line": line, "Other": other}, [("Fact", "line", "Line", "line", True)])


def test_filters_of_every_level_including_slicer_selections():
    rows = rf.collect_filters(_report())
    levels = sorted({(r["level"], r["page"]) for r in rows})
    assert ("slicer", "Operations") in levels and ("page", "Other") in levels
    page_row = next(r for r in rows if r["level"] == "page" and r["page"] == "Operations")
    assert page_row["condition"] == "Line[type] IN (A; B)"
    assert page_row["in_filter"] == {"table": "Line", "column": "type", "values": ["A", "B"]}


def test_pages_with_identical_filters_form_one_scope_that_reaches_related_facts():
    model = _model()
    scopes = rf.page_scopes(rf.collect_filters(_report()), model)
    names = [s["scope"] for s in scopes]
    assert "Other" in names
    operations = next(s for s in scopes if "Operations" in s["pages"])
    assert "Fact" in operations["reaches"] and "Other" not in operations["reaches"]


def test_a_measure_gets_page_filters_only_when_all_its_pages_share_them():
    model = _model()
    report = _report()
    from src.services.powerbi.scope.closure import collect_seeds

    seeds = collect_seeds(report, model)
    # Detail has no slicer: give it the same filter set as Operations by dropping the slicer row
    rows = [r for r in rf.collect_filters(report) if r["level"] != "slicer"]
    scopes = rf.page_scopes(rows, model)
    filters = rf.measure_page_filters(seeds.drawn, scopes, model)
    assert filters["Output"] == [{"table": "Line", "column": "type", "values": ["A", "B"]}]
    assert "Shared" not in filters  # drawn under two different scopes: a person splits it


def test_links_linked_reports_and_siblings():
    links = rl.report_links(_report())
    assert links[0]["kind"] == "button WebUrl action"
    scan = {
        "workspaces": [
            {
                "id": WS,
                "reports": [
                    {"id": REP, "name": "Linked One", "datasetId": "44444444-4444-4444-4444-444444444444"},
                    {"id": "55555555-5555-5555-5555-555555555555", "name": "Sibling", "datasetId": DS},
                    {"id": "66666666-6666-6666-6666-666666666666", "name": "This", "datasetId": DS},
                ],
            }
        ]
    }
    related = rl.related_reports(links, scan, DS, "66666666-6666-6666-6666-666666666666")
    assert [r["name"] for r in related["siblings"]] == ["Sibling"]
    assert [r["name"] for r in related["linked"]] == ["Linked One"]
    unused = rl.url_kind("/visual/visualContainerObjects/visualLink/properties/webUrl/expr/Literal/Value", "PageNavigation")
    assert unused == "unused (button action is PageNavigation)"


def test_report_context_carries_inventory_and_everything_else():
    model = _model()
    report = _report()
    from src.services.powerbi.scope.closure import close, collect_seeds

    seeds = collect_seeds(report, model)
    closure = close(model, seeds)
    rows = build_scope(model, seeds, closure)
    context = report_context(model, seeds, closure, report, rows, dataset_id=DS)
    inventory = {r["table"]: r for r in context["inventory"]}
    assert inventory["Fact"]["source"] == "databricks" and inventory["Fact"]["measures"] == 2
    assert set(context) == {"filters", "page_scopes", "measure_page_filters", "links", "related_reports", "inventory"}


def test_scope_check_finds_vocabulary_link_and_stale_problems():
    rows = [
        {"object_type": "measure", "pbi_table": "Fact", "name": "Output", "decision": "keep", "mirrored_as": "ucm_fact.output"},
        {"object_type": "measure", "pbi_table": "Fact", "name": "Typo", "decision": "keep", "mirrored_as": "ucm_fact.outptu"},
        {"object_type": "measure", "pbi_table": "Fact", "name": "Old", "decision": "drop", "mirrored_as": "ucm_fact.old"},
        {"object_type": "measure", "pbi_table": "Fact", "name": "Draft", "decision": "review", "mirrored_as": ""},
        {"object_type": "table", "pbi_table": "Gone", "name": "", "decision": "keep", "mirrored_as": ""},
    ]
    problems = check_scope(
        rows,
        deployed_measures={"ucm_fact.output", "ucm_fact.old"},
        model_measures={"Output", "Typo", "Old", "Draft"},
        model_tables={"Fact"},
    )
    text = "\n".join(problems)
    assert "outptu' is not a measure" in text
    assert "dropped, but mirrored" in text
    assert "decision 'review' not in" in text
    assert "table Gone: keep without mirrored_as" in text
    assert "table Gone: the model no longer has this table" in text
    assert not check_scope(rows[:1], deployed_measures={"ucm_fact.output"})
    assert not [p for p in check_scope(rows[3:4], draft=True) if "decision" in p]
