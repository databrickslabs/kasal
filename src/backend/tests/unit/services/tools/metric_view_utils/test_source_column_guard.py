"""The guard drops what reads a column a bound view does not have, when those columns are known:
joins, dimensions and measures, each with its reason. Pure cases first, then through the pipeline.
"""

import yaml

from src.services.tools.metric_view_utils.data_classes import (
    MetricViewSpec,
    TranslationResult,
)
from src.services.tools.metric_view_utils import prior_year
from src.services.tools.metric_view_utils.mquery_parser import MQueryParser
from src.services.tools.metric_view_utils.pipeline import MetricViewPipeline
from src.services.tools.metric_view_utils.yaml_known_columns import (
    drop_phantom_dimensions,
)
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


def test_a_valid_dimension_listed_before_a_dropped_one_of_the_same_name_is_removed_too():
    # The valid alternative comes first in the list: it must not be the one the view emits under
    # the name, nor is the dropped one replaced by it.
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
            {"name": "period", "expr": "dim_b.period"},
            {"name": "period", "expr": "dim_a.period"},
            {"name": "code", "expr": "source.plant_code"},
        ],
        columns_by_key={**COLUMNS, "Dim_B": frozenset({"plant_code", "period"})},
        key_by_view={**KEY_BY_VIEW, "tgt.ucmv.src_dim_b": "Dim_B"},
    )
    assert [d["expr"] for d in result.dimensions] == ["source.plant_code"]
    assert sorted(g.kind for g in result.gaps) == ["dimension", "dimension", "join"]


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


def _dimension_of_dropped_join(expr, fact_columns):
    return _guard(
        joins=[dict(JOIN, on="source.plant_code = dim_plant.missing")],
        dimensions=[{"name": "key", "expr": expr}],
        fact_columns=frozenset(fact_columns),
        columns_by_key={**COLUMNS, "Dim_Plant": frozenset({"plant_code"})},
    )


def test_a_dimension_wrapping_a_column_of_a_dropped_join_reads_the_fact_column():
    # The period key of a prior-year window is a cast of the calendar's column; the fact has it.
    result = _dimension_of_dropped_join(
        "TRY_CAST(dim_plant.plant_code AS INT)", {"plant_code", "amount"}
    )
    assert result.dimensions == [
        {"name": "key", "expr": "TRY_CAST(source.plant_code AS INT)"}
    ]
    assert [(g.kind, g.name) for g in result.gaps if g.kind != "measure"] == [
        ("join", "dim_plant")
    ]


def test_a_derived_date_of_a_dropped_join_column_reads_the_fact_column():
    result = _dimension_of_dropped_join(
        "DATE_TRUNC('MONTH', TRY_CAST(dim_plant.plant_code AS DATE))", {"plant_code"}
    )
    assert result.dimensions == [
        {
            "name": "key",
            "expr": "DATE_TRUNC('MONTH', TRY_CAST(source.plant_code AS DATE))",
        }
    ]


def test_a_wrapped_column_the_fact_lacks_still_drops_the_dimension_with_its_reason():
    result = _dimension_of_dropped_join(
        "TRY_CAST(dim_plant.plant_name AS INT)", {"amount"}
    )
    assert result.dimensions == []
    (gap,) = [g for g in result.gaps if g.kind == "dimension"]
    assert "dim_plant" in gap.reason


def test_a_rebound_dimension_reading_a_missing_column_of_a_kept_join_is_still_dropped():
    result = _guard(
        joins=[
            dict(
                JOIN,
                name="dim_b",
                source="tgt.ucmv.src_dim_b",
                on="source.plant_code = dim_b.plant_code",
            ),
            dict(JOIN, name="dim_plant", on="source.plant_code = dim_plant.missing"),
        ],
        dimensions=[
            {"name": "key", "expr": "CONCAT(dim_plant.plant_code, dim_b.period)"}
        ],
        fact_columns=frozenset({"plant_code"}),
        columns_by_key={
            **COLUMNS,
            "Dim_B": frozenset({"plant_code"}),
            "Dim_Plant": frozenset({"plant_code"}),
        },
        key_by_view={**KEY_BY_VIEW, "tgt.ucmv.src_dim_b": "Dim_B"},
    )
    # The rebound text reads a column of dim_b that its view lacks: the dimension is dropped.
    # The first reason found is the dropped join it also reads (dim_plant).
    assert result.dimensions == []
    (gap,) = [g for g in result.gaps if g.kind == "dimension"]
    assert "dim_plant" in gap.reason


def _prior(name, sql, dax="", original=None):
    measure = _measure(name, sql)
    measure.dax_expression = dax
    measure.original_name = original or name
    return measure


CAL_JOIN = {
    "name": "dim_cal",
    "source": "tgt.ucmv.src_dim_cal",
    "on": "source.date_key = dim_cal.date_key",
}
PY = "CALCULATE([Sales], SAMEPERIODLASTYEAR(Calendar[Date]))"


def _window_spec_of(fact_columns):
    """A fact with a calendar joined on a date the fact lacks, a prior-year window ordered by
    the calendar's period, and a measure that reads the window."""
    mapping = [
        {
            "measure_name": "Sales PY",
            "original_name": "Sales PY",
            "dax_expression": PY,
            "proposed_allocation": "Fact",
            "all_allocations": [{"table": "Fact", "role": "primary"}],
        }
    ]
    prior_year.take_out_shifts(mapping, {})
    spec = MetricViewSpec(
        fact_table_key="Fact",
        source_table="c.s.fact",
        view_name="fact",
        comment="",
        joins=[dict(CAL_JOIN)],
        dimensions=[
            {"name": "fiscper", "expr": "dim_cal.fiscper"},
            {"name": "country", "expr": "source.country"},
        ],
        measures=[
            _prior("sales", "SUM(source.amount)", original="Sales"),
            _prior("sales_py", "MEASURE(sales)", "[Sales]", "Sales PY"),
            _prior("growth", "MEASURE(sales) - MEASURE(sales_py)"),
        ],
        untranslatable=[],
        fact_columns=list(fact_columns),
    )
    prior_year.add_windows({"Fact": spec}, mapping, {})
    return spec


def _guard_window_spec(spec, fact_columns):
    return guard_bound_spec(
        "Fact",
        spec,
        columns_by_key={
            "Fact": frozenset(fact_columns),
            "Dim_Cal": frozenset({"date_key", "fiscper"}),
        },
        key_by_view={"tgt.ucmv.src_dim_cal": "Dim_Cal"},
    )


def _emit_window_checks(guarded):
    by_name = {m.measure_name: m for m in guarded.measures}
    base = [by_name["sales"]]
    dax = [by_name["sales_py"], by_name["growth"]]
    drop_phantom_dimensions(guarded, base, [], dax, [])
    return dax


def test_a_window_and_its_reader_survive_a_dropped_calendar_join_when_the_fact_has_the_period():
    spec = _window_spec_of(["fiscper", "country", "amount"])
    guarded, result = _guard_window_spec(spec, ["fiscper", "country", "amount"])
    assert [(g.kind, g.name) for g in result.gaps] == [("join", "dim_cal")]
    assert guarded.joins == []
    dax = _emit_window_checks(guarded)
    assert [d["name"] for d in guarded.dimensions] == [
        "fiscper",
        "fiscper_key",
        "country",
    ]
    assert guarded.dimensions[0]["expr"] == "source.fiscper"
    assert guarded.dimensions[1]["expr"] == "TRY_CAST(source.fiscper AS INT)"
    assert [m.measure_name for m in dax] == ["sales_py", "growth"]
    assert dax[0].window_spec["order"] == "fiscper_key"
    assert guarded.untranslatable == []


def test_a_window_whose_period_the_fact_lacks_is_reported_with_its_reason():
    # Without the fact's own period, the key is a read through the dropped join: the guard
    # drops it, and the window that orders by it is reported with the reason.
    spec = _window_spec_of(["country", "amount"])
    guarded, result = _guard_window_spec(spec, ["country", "amount"])
    reasons = {(g.kind, g.name) for g in result.gaps}
    assert ("dimension", "fiscper_key") in reasons
    dax = _emit_window_checks(guarded)
    assert dax == []
    gone = {m.measure_name: m.skip_reason for m in guarded.untranslatable}
    assert "fiscper_key" in gone["sales_py"]
    assert "window measure" in gone["growth"]
