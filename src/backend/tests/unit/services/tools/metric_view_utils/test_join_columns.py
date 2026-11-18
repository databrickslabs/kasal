"""A Power BI relationship on a column the model computes (Fact[date_key]) became a join the
Databricks source cannot satisfy, and the whole metric view failed on deploy."""

import yaml

from src.services.tools.metric_view_utils.data_classes import (
    MetricViewSpec,
    TranslationResult,
)
from src.services.tools.metric_view_utils.join_columns import (
    missing_join_columns,
    select_list_columns,
)
from src.services.tools.metric_view_utils.yaml_emitter import emit_yaml

NATIVE = """SELECT
    co.fiscper, co.comp_code, /* co.region, */
    sum(volume) as volume, -- note
    sum(underlying_ebit) AS ebit
FROM md.co012 AS co WHERE co.x = '"& Filter &"' GROUP BY co.fiscper, co.comp_code"""


def test_the_select_list_of_a_native_query_is_known():
    assert select_list_columns(NATIVE) == {"fiscper", "comp_code", "volume", "ebit"}


def test_a_star_or_an_unnamed_expression_means_unknown():
    assert select_list_columns("SELECT * FROM t") is None
    assert select_list_columns("SELECT a, b + 1 FROM t") is None


def test_missing_fact_side_columns():
    assert missing_join_columns(
        "source.date_key = cal.date_id", ["fiscper", "comp_code"]
    ) == ["date_key"]
    assert (
        missing_join_columns("LPAD(source.comp_code, 4, '0') = g.k", ["comp_code"])
        == []
    )
    assert (
        missing_join_columns("source.date_key = cal.date_id", []) == []
    )  # unknown: keep


def _measure(name, expr):
    return TranslationResult(
        measure_name=name,
        original_name=name,
        sql_expr=expr,
        is_translatable=True,
        skip_reason="",
        dax_expression="",
        confidence="high",
        category="base",
    )


def test_the_emitter_drops_the_join_and_its_dimensions_and_says_why():
    spec = MetricViewSpec(
        fact_table_key="Fact",
        source_table="prod.md.co012",
        view_name="fact",
        comment="c",
        joins=[
            {
                "name": "dim_calendar",
                "source": "prod.md.cal",
                "join_on": "source.date_key = dim_calendar.date_id",
            },
            {
                "name": "dim_geo",
                "source": "prod.md.geo",
                "join_on": "source.comp_code = dim_geo.comp_code",
            },
        ],
        dimensions=[
            {"name": "month", "expr": "dim_calendar.month"},
            {"name": "country", "expr": "dim_geo.country"},
        ],
        measures=[
            _measure("volume", "SUM(source.volume)"),
            _measure(
                "geo_volume", "SUM(source.volume) FILTER (WHERE dim_geo.country <> '')"
            ),
        ],
        untranslatable=[],
        fact_columns=["fiscper", "comp_code", "volume"],
    )
    out = yaml.safe_load(emit_yaml(spec))
    assert [j["name"] for j in out["joins"]] == ["dim_geo"]
    assert all("dim_calendar" not in d["expr"] for d in out.get("dimensions") or [])

    assert (
        "Join dim_calendar dropped: the source has no column date_key" in out["comment"]
    )


def test_a_dimension_the_fact_has_itself_moves_to_the_fact():
    from src.services.tools.metric_view_utils.yaml_emitter import _drop_unusable_joins

    spec = MetricViewSpec(
        fact_table_key="Fact",
        source_table="prod.md.co012",
        view_name="fact",
        comment="c",
        joins=[
            {
                "name": "dim_calendar",
                "source": "prod.md.cal",
                "join_on": "source.date_key = dim_calendar.date_id",
            }
        ],
        dimensions=[
            {"name": "fiscper", "expr": "dim_calendar.fiscper"},
            {"name": "month", "expr": "dim_calendar.month"},
        ],
        measures=[],
        untranslatable=[],
        fact_columns=["fiscper", "volume"],
    )
    _drop_unusable_joins(spec, set())
    assert spec.joins == []
    assert spec.dimensions == [
        {"name": "fiscper", "expr": "source.fiscper"}
    ]  # the period survives
