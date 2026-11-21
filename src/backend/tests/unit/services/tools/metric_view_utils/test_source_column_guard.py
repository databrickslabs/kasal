"""The guard drops what reads a column a bound view does not have, when those columns are known:
joins, dimensions and measures, each with its reason. Pure cases first, then through the pipeline.
"""

import yaml

from src.services.tools.metric_view_utils.data_classes import (
    MetricViewSpec,
    TranslationResult,
)
from src.services.tools.metric_view_utils.mquery_parser import MQueryParser
from src.services.tools.metric_view_utils.pipeline import MetricViewPipeline
from src.services.tools.metric_view_utils.source_column_guard import (
    guard_bound_spec,
    guard_view,
    view_key_by_name,
)

JOIN = {
    "name": "dim_plant",
    "source": "tgt.ucmv.src_dim_plant",
    "on": "source.plant_code = dim_plant.plant_code",
}
KEY_BY_VIEW = {"tgt.ucmv.src_dim_plant": "Dim_Plant"}
COLUMNS = {
    "Dim_Plant": frozenset({"plant_code", "plant_name"}),
    "Fact_Sales": frozenset({"plant_code", "amount"}),
}


def _measure(name: str, sql: str | None) -> TranslationResult:
    return TranslationResult(
        measure_name=name,
        original_name=name,
        sql_expr=sql,
        is_translatable=sql is not None,
        skip_reason="",
        dax_expression="",
        confidence="high",
        category="single_table",
    )


def _guard(**overrides):
    kwargs = {
        "joins": [dict(JOIN)],
        "dimensions": [
            {"name": "plant_name", "expr": "dim_plant.plant_name"},
            {"name": "code", "expr": "source.plant_code"},
        ],
        "measures": [_measure("total", "SUM(source.amount)")],
        "fact_columns": COLUMNS["Fact_Sales"],
        "columns_by_key": COLUMNS,
        "key_by_view": KEY_BY_VIEW,
    }
    kwargs.update(overrides)
    return guard_view("Fact_Sales", **kwargs)


def test_everything_the_views_have_is_kept():
    result = _guard()
    assert [j["name"] for j in result.joins] == ["dim_plant"]
    assert [d["name"] for d in result.dimensions] == ["plant_name", "code"]
    assert [m.measure_name for m in result.measures] == ["total"]
    assert result.gaps == []


def test_a_join_on_a_fact_column_the_fact_lacks_is_dropped_with_what_reads_it():
    result = _guard(fact_columns=frozenset({"amount"}))
    assert result.joins == []
    assert (
        result.dimensions == []
    )  # plant_name reads the dropped join; code reads plant_code
    kinds = sorted((g.kind, g.name) for g in result.gaps)
    assert kinds == [
        ("dimension", "code"),
        ("dimension", "plant_name"),
        ("join", "dim_plant"),
    ]


def test_a_dimension_reading_a_join_column_the_view_lacks_is_dropped():
    result = _guard(columns_by_key={**COLUMNS, "Dim_Plant": frozenset({"plant_code"})})
    assert [j["name"] for j in result.joins] == ["dim_plant"]
    assert [d["name"] for d in result.dimensions] == ["code"]
    (gap,) = result.gaps
    assert (gap.kind, gap.name) == ("dimension", "plant_name")
    assert "plant_name" in gap.reason and "dim_plant" in gap.reason


def test_a_dimension_of_a_dropped_join_reads_the_fact_column_when_the_fact_has_it():
    result = _guard(
        joins=[dict(JOIN, on="source.plant_code = dim_plant.missing")],
        dimensions=[{"name": "plant", "expr": "dim_plant.plant_code"}],
        columns_by_key={**COLUMNS, "Dim_Plant": frozenset({"plant_code"})},
    )
    assert result.joins == []
    assert result.dimensions == [{"name": "plant", "expr": "source.plant_code"}]
    assert [(g.kind, g.name) for g in result.gaps] == [("join", "dim_plant")]


def test_a_measure_on_a_fact_column_the_fact_lacks_is_removed_with_its_reason():
    result = _guard(fact_columns=frozenset({"plant_code"}))
    assert result.measures == []
    (removed,) = result.removed_measures
    assert removed.is_translatable is False and removed.sql_expr is None
    assert "amount" in removed.skip_reason


def test_unknown_columns_mean_no_check():
    result = _guard(fact_columns=None, columns_by_key={})
    assert [m.measure_name for m in result.measures] == ["total"]
    assert result.removed_measures == [] and result.gaps == []


def test_guard_bound_spec_does_not_mutate_its_input():
    spec = MetricViewSpec(
        fact_table_key="Fact_Sales",
        source_table="tgt.ucmv.src_fact_sales",
        view_name="v",
        comment="",
        joins=[dict(JOIN)],
        dimensions=[{"name": "plant_name", "expr": "dim_plant.plant_name"}],
        measures=[_measure("total", "SUM(source.amount)")],
        untranslatable=[],
    )
    columns = {**COLUMNS, "Dim_Plant": frozenset({"plant_code"})}
    guarded, result = guard_bound_spec(
        "Fact_Sales", spec, columns_by_key=columns, key_by_view=KEY_BY_VIEW
    )
    assert guarded.dimensions == []
    assert len(spec.dimensions) == 1
    assert len(result.gaps) == 1


def test_a_dropped_dimension_name_is_not_taken_over_by_another_join():
    # `period` reads dim_a, whose view lacks the key: dropped. A later `period` reads dim_b, a valid
    # join. Before the guard the writer kept the first; the later one must not replace it.
    result = _guard(
        joins=[
            dict(JOIN, name="dim_a", on="source.plant_code = dim_a.missing_key"),
            dict(
                JOIN,
                name="dim_b",
                source="tgt.ucmv.src_dim_b",
                on="source.plant_code = dim_b.plant_code",
            ),
        ],
        dimensions=[
            {"name": "period", "expr": "dim_a.period"},
            {"name": "period", "expr": "dim_b.period"},
            {"name": "code", "expr": "source.plant_code"},
        ],
        columns_by_key={**COLUMNS, "Dim_B": frozenset({"plant_code", "period"})},
        key_by_view={**KEY_BY_VIEW, "tgt.ucmv.src_dim_b": "Dim_B"},
    )
    assert [d["expr"] for d in result.dimensions] == ["source.plant_code"]
    assert [(g.kind, g.name) for g in result.gaps if g.kind == "dimension"] == [
        ("dimension", "period"),
        ("dimension", "period"),
    ]


def test_a_later_dimension_of_a_kept_name_is_unaffected():
    result = _guard(
        dimensions=[
            {"name": "plant_name", "expr": "dim_plant.plant_name"},
            {"name": "plant_name", "expr": "source.plant_code"},
        ],
        columns_by_key=COLUMNS,
    )
    assert [d["expr"] for d in result.dimensions] == [
        "dim_plant.plant_name",
        "source.plant_code",
    ]


def test_view_key_by_name_matches_the_bound_view_names():
    assert view_key_by_name({"Dim_Plant": "`tgt`.`ucmv`.`src_dim_plant`"}) == {
        "tgt.ucmv.src_dim_plant": "Dim_Plant"
    }


MQUERY = [
    {
        "table_name": "Fact_Sales",
        "transpiled_sql": "SELECT plant_code, SUM(amount) AS amount FROM cat.sch.sales GROUP BY plant_code",
        "validation_passed": "Yes",
    },
    {
        "table_name": "Dim_Plant",
        "transpiled_sql": "SELECT plant_code, plant_name FROM cat.sch.plant",
        "validation_passed": "Yes",
    },
]
MEASURES = [
    {
        "measure_name": "Total Amount",
        "dax_expression": "SUM(Fact_Sales[amount])",
        "original_name": "Total Amount",
        "proposed_allocation": "Fact_Sales",
    }
]
JOIN_KEY_MAP = {
    "Dim_Plant": {
        "alias": "dim_plant",
        "join_key": "plant_code",
        "dim_key": "plant_code",
        "source_table": "cat.sch.plant",
        "dim_columns": [{"name": "plant_name", "expr": "plant_name"}],
    }
}


def _pipeline(source_columns: dict) -> MetricViewPipeline:
    pipeline = MetricViewPipeline(
        mapping=MEASURES,
        mquery_tables=MQueryParser().parse_json(MQUERY),
        config={"join_key_map": JOIN_KEY_MAP, "read_from_source_layer": True},
        source_columns=source_columns,
    )
    pipeline.run()
    return pipeline


def test_the_pipeline_drops_a_joined_dimension_the_view_lacks_and_records_it():
    pipeline = _pipeline(
        {
            "Fact_Sales": ["plant_code", "amount"],
            "Dim_Plant": ["plant_code"],
        }
    )
    doc = yaml.safe_load(
        pipeline.emit_all_yaml(catalog="tgt", schema="ucmv")["Fact_Sales"]
    )
    assert "plant_name" not in {d["name"] for d in doc.get("dimensions") or []}
    assert [j["name"] for j in doc.get("joins") or []] == ["dim_plant"]
    gaps = pipeline._limitations["source_column_gaps"]
    assert [(g["kind"], g["name"]) for g in gaps] == [("dimension", "plant_name")]


def test_the_pipeline_moves_a_measure_the_fact_view_lacks_to_untranslated():
    # `amount` exists only in the dimension's view: the allocator leaves the measure on the
    # fact (no fact-resident column), and the guard finds the fact's view lacks `amount`.
    pipeline = _pipeline(
        {
            "Fact_Sales": ["plant_code"],
            "Dim_Plant": ["plant_code", "plant_name", "amount"],
        }
    )
    pipeline.emit_all_yaml(catalog="tgt", schema="ucmv")  # the guard runs when binding
    spec = pipeline.all_specs["Fact_Sales"]
    assert "total_amount" not in {m.measure_name for m in spec.measures}
    assert "total_amount" in {m.measure_name for m in spec.untranslatable}
    # The base measure `amount` reads the same missing column, so it is removed too.
    names = {
        g["name"]
        for g in pipeline._limitations["source_column_gaps"]
        if g["kind"] == "measure"
    }
    assert {"total_amount", "amount"} <= names


def test_without_source_columns_nothing_is_guarded():
    pipeline = _pipeline({})
    assert "source_column_gaps" not in pipeline._limitations
