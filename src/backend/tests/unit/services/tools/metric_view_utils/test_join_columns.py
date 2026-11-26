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


CTE_NATIVE = """WITH aggregated AS (
    SELECT fiscper, unit, SUM(kbi_value) AS value FROM mart.pl GROUP BY fiscper, unit
),
cy AS (SELECT fiscper, unit, value AS lc_cy FROM aggregated WHERE kind = 'select')
SELECT cy.unit, cy.fiscper AS fiscper, cy.lc_cy AS result FROM cy"""


def _m(sql: str, steps: str = "") -> str:
    return (
        'let\n    v = Value.NativeQuery(conn, "'
        + sql
        + '", null, [EnableFolding=true])'
        + steps
        + "\nin\n    v"
    )


def test_the_output_of_a_cte_query_is_its_final_select():
    from src.services.tools.metric_view_utils.join_columns import native_output_columns

    assert native_output_columns(CTE_NATIVE) == {"unit", "fiscper", "result"}
    assert native_output_columns("(SELECT a FROM t) UNION (SELECT a FROM u)") is None


def test_an_aggregate_computed_only_inside_a_cte_is_not_a_base_measure():
    """SUM(kbi_value) AS value lives in a CTE; the query returns result. A base measure
    SUM(source.value) failed on deploy with UNRESOLVED_COLUMN."""
    from src.services.tools.metric_view_utils.mquery_parser import MQueryParser

    tables = MQueryParser().parse_json(
        [
            {
                "table_name": "FT",
                "transpiled_sql": _m(CTE_NATIVE),
                "validation_passed": "Yes",
            }
        ]
    )
    assert [a["name"] for a in tables["FT"].aggregate_columns] == []


def test_the_fact_columns_are_unknown_when_power_query_reshapes_them():
    from src.services.tools.metric_view_utils.data_classes import TableInfo
    from src.services.tools.metric_view_utils.join_columns import known_fact_columns

    def info(m: str) -> TableInfo:
        t = TableInfo("FT", "mart.pl", [], [], [], True, "")
        t.raw_transpiled_sql = m
        return t

    assert known_fact_columns(info(_m(CTE_NATIVE))) == ["fiscper", "result", "unit"]
    renamed = ',\n    r = Table.RenameColumns(v, {{"result", "value"}})'
    assert known_fact_columns(info(_m(CTE_NATIVE, renamed))) == []


def _spec(**fields):
    base = dict(
        fact_table_key="Fact_Q",
        source_table="prod.q.kbis",
        view_name="fact_q",
        comment="",
        joins=[],
        dimensions=[],
        measures=[],
        untranslatable=[],
    )
    return MetricViewSpec(**{**base, **fields})


def test_the_facts_own_key_of_a_join_becomes_a_dimension():
    """The model relates Fact[FormatedDate] to the calendar; the join was built on
    formated_date, which the source does not have, and dropped. The view was left without any
    date: nothing to slice by, and no period to compare with Power BI by."""
    from src.services.tools.metric_view_utils.join_columns import (
        add_join_key_dimensions,
    )

    spec = _spec(
        joins=[
            {
                "name": "dim_calendar",
                "source": "c.s.cal",
                "join_on": "source.formated_date = dim_calendar.date_id",
            },
            {
                "name": "dim_geo",
                "source": "c.s.geo",
                "join_on": "source.comp_code = dim_geo.comp_code",
            },
            {
                "name": "dim_x",
                "source": "c.s.x",
                "join_on": "source.date_key = dim_x.id",
            },
        ],
        dimensions=[{"name": "comp_code", "expr": "dim_geo.comp_code"}],
        fact_columns=["formateddate", "comp_code", "kbi_value"],
    )
    assert add_join_key_dimensions(spec) == ["formateddate"]
    assert spec.dimensions[-1]["expr"] == "source.formateddate"
    assert add_join_key_dimensions(spec) == []  # once
    assert (
        add_join_key_dimensions(_spec(joins=spec.joins)) == []
    )  # columns unknown: nothing


def test_a_step_that_removes_columns_makes_the_fact_columns_unknown():
    from src.services.tools.metric_view_utils.data_classes import TableInfo
    from src.services.tools.metric_view_utils.join_columns import known_fact_columns

    info = TableInfo("FT", "mart.pl", [], [], [], True, "")
    info.raw_transpiled_sql = _m(
        CTE_NATIVE, ',\n    r = Table.RemoveColumns(v, {"unit"})'
    )
    assert known_fact_columns(info) == []


def test_a_dimension_on_a_column_the_native_query_returns_is_not_a_phantom():
    """With the view reading its source-layer view there is no inline SQL to take the columns
    from: every dimension on a column no measure touches was dropped."""
    from types import SimpleNamespace

    from src.services.tools.metric_view_utils.yaml_known_columns import (
        drop_phantom_dimensions,
    )

    measure = SimpleNamespace(measure_name="qty", sql_expr="SUM(source.qty)")
    spec = _spec(
        dimensions=[
            {"name": "salesorg", "expr": "source.salesorg"},
            {"name": "ghost", "expr": "source.not_there"},
            {"name": "comp_code", "expr": "source.comp_code"},
            {"name": "comp_code", "expr": "dim_geo.comp_code"},
        ],
        fact_columns=["salesorg", "comp_code", "qty"],
    )
    drop_phantom_dimensions(spec, [measure], [], [], [])
    assert [(d["name"], d["expr"]) for d in spec.dimensions] == [
        ("salesorg", "source.salesorg"),
        ("comp_code", "dim_geo.comp_code"),  # the join's dimension keeps its name
    ]


def test_a_measure_that_filters_on_the_join_key_does_not_stop_the_key_dimension():
    """A translated measure filtered on the fact's date. The key was then not added as a
    dimension, the calendar join was dropped for a column the calendar lacks, and the view was
    left with no period: none of its measures could be compared with Power BI."""
    from types import SimpleNamespace

    from src.services.tools.metric_view_utils.join_columns import (
        add_join_key_dimensions,
    )

    spec = _spec(
        joins=[
            {
                "name": "dim_calendar",
                "source": "c.s.cal",
                "join_on": "source.OrderDate = dim_calendar.date_id",
            }
        ],
        dimensions=[{"name": "region", "expr": "source.region"}],
        fact_columns=["orderdate", "region", "amount"],
    )
    spec.measures = [
        SimpleNamespace(
            measure_name="early_amount",
            sql_expr="SUM(source.amount) FILTER (WHERE source.OrderDate < DATE'2026-01-01')",
        )
    ]
    assert add_join_key_dimensions(spec) == ["orderdate"]

