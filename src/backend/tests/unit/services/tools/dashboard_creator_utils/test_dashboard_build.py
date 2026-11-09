"""Dashboard rebuild: visual rules, metric-view datasets, filter widgets from slicers, layout and
the per-visual coverage report."""

from src.services.tools.dashboard_creator_utils.build import build_dashboard, coverage
from src.services.tools.dashboard_creator_utils.report_items import resolve_slicers, split_report_items
from src.services.tools.dashboard_creator_utils.visual_types import RULES, rule_for

VIEW = "main.sales.ucm_orders"


def _m(visual_type, dims=("region",), measures=("revenue",), page="Sales", vid="v1", view: str | None = VIEW):
    return {
        "visual_id": vid,
        "page_name": page,
        "visual_type": visual_type,
        "chart_title": f"{visual_type} title",
        "ucmv_view": view,
        "dimensions": list(dims),
        "measures": list(measures),
    }


def _widgets(dashboard):
    return [entry["widget"] for page in dashboard["pages"] for entry in page["layout"]]


def test_every_rule_has_a_known_status():
    assert {r.status for r in RULES.values()} <= {
        "mapped", "approximated", "table_fallback", "filter", "decorative", "unsupported",
    }
    assert rule_for("somethingNew").status == "table_fallback"


def test_widgets_read_the_metric_view_with_measure_fields():
    dashboard, outcomes = build_dashboard([_m("clusteredColumnChart", dims=("region", "year"))])
    assert dashboard["datasets"] == [{"name": "ds_ucm_orders", "displayName": "Ucm Orders", "asset_name": VIEW}]
    [widget] = _widgets(dashboard)
    fields = widget["queries"][0]["query"]["fields"]
    assert {"name": "revenue", "expression": "MEASURE(`revenue`)"} in fields
    spec = widget["spec"]
    assert spec["widgetType"] == "bar" and spec["mark"] == {"layout": "group"}
    assert spec["encodings"]["x"]["fieldName"] == "region"
    assert spec["encodings"]["color"]["fieldName"] == "year"
    assert outcomes[0]["status"] == "mapped"


def test_bar_charts_are_horizontal_and_columns_vertical():
    dashboard, _ = build_dashboard([_m("barChart", vid="a"), _m("columnChart", vid="b")])
    bar, column = (w["spec"]["encodings"] for w in _widgets(dashboard))
    assert bar["y"]["fieldName"] == "region" and bar["x"]["fieldName"] == "revenue"
    assert column["x"]["fieldName"] == "region" and column["y"]["fieldName"] == "revenue"


def test_pie_scatter_area_counter_and_table():
    mappings = [
        _m("donutChart", vid="1"),
        _m("scatterChart", measures=("revenue", "margin"), vid="2"),
        _m("stackedAreaChart", vid="3"),
        _m("card", dims=(), vid="4"),
        _m("pivotTable", dims=("region", "product"), vid="5"),
    ]
    dashboard, outcomes = build_dashboard(mappings)
    types = [w["spec"]["widgetType"] for w in _widgets(dashboard)]
    assert types == ["pie", "scatter", "area", "counter", "table"]
    by_id = {o["visual_id"]: o for o in outcomes}
    assert by_id["1"]["status"] == "approximated" and "donut" in by_id["1"]["note"]
    assert by_id["5"]["status"] == "approximated" and "matrix" in by_id["5"]["note"]


def test_missing_fields_downgrade_the_chart_and_say_so():
    dashboard, outcomes = build_dashboard(
        [_m("pieChart", dims=(), vid="1"), _m("scatterChart", vid="2"), _m("lineChart", measures=(), vid="3")]
    )
    assert [w["spec"]["widgetType"] for w in _widgets(dashboard)] == ["counter", "bar", "table"]
    assert all(o["status"] == "approximated" and o["note"] for o in outcomes)


def test_several_measures_without_a_series_share_the_value_axis():
    dashboard, _ = build_dashboard([_m("lineChart", measures=("revenue", "cost"))])
    [widget] = _widgets(dashboard)
    assert [f["fieldName"] for f in widget["spec"]["encodings"]["y"]["fields"]] == ["revenue", "cost"]


def test_slicers_become_filter_widgets_over_every_dataset_with_the_dimension():
    other = "main.sales.ucm_returns"
    mappings = [_m("lineChart", vid="1"), _m("tableEx", vid="2", view=other)]
    slicers = [
        {"visual_id": "s1", "page_name": "Sales", "visual_type": "slicer",
         "fields": [{"column": "Region", "dimension": "region", "views": [VIEW, other]}]},
        {"visual_id": "s2", "page_name": "Sales", "visual_type": "slicer",
         "fields": [{"column": "Order Date", "dimension": "order_date", "views": [VIEW]}]},
        {"visual_id": "s3", "page_name": "Sales", "visual_type": "slicer",
         "fields": [{"column": "Colour", "dimension": "", "views": []}]},
    ]
    dashboard, outcomes = build_dashboard(mappings, slicers)
    region, order_date = [w for w in _widgets(dashboard) if w["spec"]["widgetType"].startswith("filter")]
    assert region["spec"]["widgetType"] == "filter-multi-select"
    assert {q["query"]["datasetName"] for q in region["queries"]} == {"ds_ucm_orders", "ds_ucm_returns"}
    assert [f["queryName"] for f in region["spec"]["encodings"]["fields"]] == [q["name"] for q in region["queries"]]
    assert order_date["spec"]["widgetType"] == "filter-date-range-picker"
    s3 = next(o for o in outcomes if o["visual_id"] == "s3")
    assert s3["status"] == "unmapped" and "Colour" in s3["note"]


def test_the_sql_mode_has_one_grouped_dataset_per_visual_and_reports_filters():
    slicers = [{"visual_id": "s1", "page_name": "Sales",
                "fields": [{"column": "Region", "dimension": "region", "views": [VIEW]}]}]
    dashboard, outcomes = build_dashboard(
        [_m("lineChart", vid="1"), _m("tableEx", vid="2")], slicers, dataset_mode="sql"
    )
    assert len(dashboard["datasets"]) == 2
    assert all("GROUP BY ALL" in d["query"] and "asset_name" not in d for d in dashboard["datasets"])
    [widget, _] = _widgets(dashboard)
    assert {"name": "revenue", "expression": "SUM(`revenue`)"} in widget["queries"][0]["query"]["fields"]
    assert any(o["visual_id"] == "s1" and o["status"] == "unmapped" for o in outcomes)


def test_layout_never_overlaps_and_starts_filters_on_top():
    mappings = [_m("card", dims=(), vid=str(i)) for i in range(4)] + [_m("tableEx", vid="t"), _m("lineChart", vid="l")]
    slicers = [{"visual_id": "s", "page_name": "Sales", "fields": [{"column": "r", "dimension": "region", "views": [VIEW]}]}]
    dashboard, _ = build_dashboard(mappings, slicers)
    cells = set()
    for entry in dashboard["pages"][0]["layout"]:
        p = entry["position"]
        assert p["x"] + p["width"] <= 6
        for x in range(p["x"], p["x"] + p["width"]):
            for y in range(p["y"], p["y"] + p["height"]):
                assert (x, y) not in cells
                cells.add((x, y))
    first = dashboard["pages"][0]["layout"][0]
    assert first["widget"]["spec"]["widgetType"].startswith("filter") and first["position"]["y"] == 0


def test_decorative_unsupported_and_unmapped_visuals_are_reported_not_built():
    mappings = [_m("textbox", vid="t"), _m("qnaVisual", vid="q"), _m("lineChart", vid="n", view=None)]
    dashboard, outcomes = build_dashboard(mappings)
    assert dashboard["pages"] == []
    assert [o["status"] for o in outcomes] == ["decorative", "unsupported", "unmapped"]
    summary = coverage(outcomes + [{"status": "mapped"}])
    assert summary["by_status"]["decorative"] == 1
    assert summary["migrated_pct"] == round(100 * 1 / 3, 1)  # decorative not in scope


def test_report_items_split_and_slicer_resolution():
    report = {
        "visuals": [
            {"visual_id": "a", "page_name": "P", "visual_type": "lineChart", "measures": ["Revenue"], "columns": ["Region"]},
            {"visual_id": "b", "page_name": "P", "visual_type": "tableEx", "measures": [], "columns": ["Product"]},
            {"visual_id": "c", "page_name": "P", "visual_type": "slicer", "columns": ["Region"], "tables": ["Geo"]},
            {"visual_id": "d", "page_name": "P", "visual_type": "textbox"},
            {"visual_id": "e", "page_name": "P", "visual_type": "cardVisual"},
        ]
    }
    visuals, slicers, skipped = split_report_items(report)
    assert [v["visual_id"] for v in visuals] == ["a", "b"]  # a columns-only table is kept
    assert [s["visual_id"] for s in slicers] == ["c"]
    assert [(s["visual_id"], s["status"]) for s in skipped] == [("d", "decorative"), ("e", "unmapped")]
    summaries = [{"view_name": VIEW, "dimensions": ["geo_region", "product"]}]
    [resolved] = resolve_slicers(slicers, summaries)
    assert resolved["fields"] == [{"column": "Region", "dimension": "geo_region", "views": [VIEW]}]
