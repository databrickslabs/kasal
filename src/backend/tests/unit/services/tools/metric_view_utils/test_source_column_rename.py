"""A source-view column named like the metric view's fact alias (``source``) is exposed as
``source_``: the view's DDL re-exposes it, and every reference the generator makes uses the new
name. Without that rename, a reference to a column the fact lacks reports
INVALID_EXTRACT_BASE_FIELD_TYPE on ``source`` (confirmed on a warehouse)."""

import yaml

from src.services.tools.metric_view_utils.mquery_parser import MQueryParser
from src.services.tools.metric_view_utils.pipeline import MetricViewPipeline
from src.services.tools.metric_view_utils.source_layer_emitter import (
    TableSource,
    ViewSpec,
    emit_view,
)

MQUERY = [
    {
        "table_name": "Fact_Sales",
        "transpiled_sql": "SELECT plant_code, source, SUM(amount) AS amount FROM cat.sch.sales GROUP BY plant_code, source",
        "validation_passed": "Yes",
    },
]
MEASURES = [
    {
        "measure_name": "Total Amount",
        "dax_expression": "SUM(Fact_Sales[amount])",
        "original_name": "Total Amount",
        "proposed_allocation": "Fact_Sales",
    },
    {
        "measure_name": "Source Rows",
        "dax_expression": "COUNT(Fact_Sales[source])",
        "original_name": "Source Rows",
        "proposed_allocation": "Fact_Sales",
    },
]


def test_the_view_re_exposes_the_renamed_column_under_its_new_name():
    spec = ViewSpec(
        table_name="Fact_Sales",
        target_view="`c`.`s`.`src_fact_sales`",
        source=TableSource(full_table_name="`cat`.`sch`.`sales`"),
        renamed_columns={"source": "source_"},
    )
    ddl = emit_view(spec).ddl
    assert "SELECT * EXCEPT (source), source AS source_" in ddl
    assert (
        "FROM src" in ddl
    )  # the base is kept as a CTE: the rename is a select, not a copy


def test_a_view_without_renames_is_unchanged():
    spec = ViewSpec(
        table_name="Fact_Sales",
        target_view="`c`.`s`.`src_fact_sales`",
        source=TableSource(full_table_name="`cat`.`sch`.`sales`"),
    )
    assert "EXCEPT" not in emit_view(spec).ddl


def _pipeline(source_columns: dict) -> MetricViewPipeline:
    pipeline = MetricViewPipeline(
        mapping=[dict(m) for m in MEASURES],
        mquery_tables=MQueryParser().parse_json(MQUERY),
        config={"read_from_source_layer": True},
        source_columns=source_columns,
    )
    pipeline.run()
    return pipeline


def test_the_fact_references_use_the_new_name_and_the_view_exposes_it():
    pipeline = _pipeline({"Fact_Sales": ["plant_code", "source", "amount"]})
    out = pipeline.emit_all_yaml(catalog="tgt", schema="ucmv")
    doc = yaml.safe_load(out["Fact_Sales"])
    exprs = {m["name"]: m["expr"] for m in doc["measures"]}
    assert exprs["source_rows"] == "COUNT(source.source_)"
    assert exprs["total_amount"] == "SUM(source.amount)"
    ddl = pipeline.source_layer_views["Fact_Sales"]["ddl"]
    assert "source AS source_" in ddl


def test_the_fact_column_set_holds_the_new_name_not_the_old():
    pipeline = _pipeline({"Fact_Sales": ["plant_code", "source", "amount"]})
    spec = pipeline.all_specs["Fact_Sales"]
    assert "source_" in spec.fact_columns and "source" not in spec.fact_columns


def test_the_renamed_references_are_guarded_against_the_new_column_set():
    # `source_` is the view's column now: the reference is kept, not treated as missing.
    pipeline = _pipeline({"Fact_Sales": ["plant_code", "source", "amount"]})
    pipeline.emit_all_yaml(catalog="tgt", schema="ucmv")
    assert not [
        g
        for g in pipeline._limitations.get("source_column_gaps", [])
        if g["kind"] == "measure"
    ]


def test_a_reference_spelled_differently_from_the_view_column_uses_the_view_spelling():
    # The model calls the column plant_code; the deployed view has it as PlantCode. Before,
    # the measure was reported as reading a column the view does not have.
    measures = [
        {
            "measure_name": "Plant A Amount",
            "dax_expression": 'CALCULATE(SUM(Fact_Sales[amount]), Fact_Sales[plant_code] = "A")',
            "original_name": "Plant A Amount",
            "proposed_allocation": "Fact_Sales",
        }
    ]
    pipeline = MetricViewPipeline(
        mapping=measures,
        mquery_tables=MQueryParser().parse_json(MQUERY),
        config={"read_from_source_layer": True},
        source_columns={"Fact_Sales": ["PlantCode", "amount"]},
    )
    pipeline.run()
    view = yaml.safe_load(pipeline.emit_all_yaml(catalog="tgt", schema="ucmv")["Fact_Sales"])
    by_name = {m["name"]: m["expr"] for m in view["measures"]}
    assert "source.PlantCode" in by_name["plant_a_amount"]
    assert "plant_code" not in by_name["plant_a_amount"]
    assert not [
        g for g in pipeline._limitations.get("source_column_gaps", []) if g["kind"] == "measure"
    ]
