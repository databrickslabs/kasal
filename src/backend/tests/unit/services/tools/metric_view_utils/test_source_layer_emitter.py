"""
Unit tests for source_layer_emitter.py

Uses OTC-derived fixtures:
 - Dim_Country  (filter, dedup, replace, append-rows steps)
 - Calendar445  (native SQL source + calc columns)
 - Fact_OTC     (partition_by, native SQL, filter on sub-select)
 - Edge cases    (empty steps, bad TableSource, untranslatable calc column)
"""

import pytest

from src.services.tools.metric_view_utils.source_layer_emitter import (
    AppendRowsStep,
    CalcColumn,
    DeduplicateStep,
    EmittedView,
    FilterStep,
    GroupByStep,
    ReplaceStep,
    TableSource,
    ViewSpec,
    _sql_lit,
    emit_view,
    emit_views,
)


# ---------------------------------------------------------------------------
# _sql_lit helper
# ---------------------------------------------------------------------------


def test_sql_lit_none():
    assert _sql_lit(None) == "NULL"


def test_sql_lit_bool():
    assert _sql_lit(True) == "TRUE"
    assert _sql_lit(False) == "FALSE"


def test_sql_lit_int():
    assert _sql_lit(42) == "42"
    assert _sql_lit(-7) == "-7"


def test_sql_lit_float():
    assert _sql_lit(3.14) == "3.14"


def test_sql_lit_string():
    assert _sql_lit("hello") == "'hello'"


def test_sql_lit_string_with_single_quote():
    assert _sql_lit("O'Brien") == "'O\\'Brien'"


# ---------------------------------------------------------------------------
# TableSource validation
# ---------------------------------------------------------------------------


def test_table_source_requires_one_of_two():
    with pytest.raises(ValueError):
        TableSource()


def test_table_source_both_set_raises():
    with pytest.raises(ValueError):
        TableSource(full_table_name="a.b.c", native_sql="SELECT 1")


def test_table_source_full_table_name_ok():
    ts = TableSource(full_table_name="`catalog`.`schema`.`t`")
    assert ts.full_table_name == "`catalog`.`schema`.`t`"


def test_table_source_native_sql_ok():
    ts = TableSource(native_sql="SELECT id FROM t WHERE x = 1")
    assert ts.native_sql is not None


# ---------------------------------------------------------------------------
# Simple: no steps, table source
# ---------------------------------------------------------------------------


def test_emit_view_no_steps_table_source():
    spec = ViewSpec(
        table_name="Dim_Simple",
        target_view="`main`.`idor`.`dim_simple`",
        source=TableSource(full_table_name="`datalake`.`udm`.`dim_table`"),
    )
    result = emit_view(spec)
    assert isinstance(result, EmittedView)
    assert result.error is None
    assert "CREATE OR REPLACE VIEW" in result.ddl
    assert "`main`.`idor`.`dim_simple`" in result.ddl
    assert not result.todo_steps


def test_emit_view_no_steps_native_sql():
    native = "SELECT id, name FROM `dl`.`sch`.`raw`\nWHERE active = 1"
    spec = ViewSpec(
        table_name="Fact_NPS",
        target_view="`main`.`idor`.`fact_nps`",
        source=TableSource(native_sql=native),
        comment="OTC NPS fact.",
    )
    result = emit_view(spec)
    assert result.error is None
    assert "COMMENT" in result.ddl
    assert "OTC NPS fact." in result.ddl
    assert "active = 1" in result.ddl


# ---------------------------------------------------------------------------
# FilterStep
# ---------------------------------------------------------------------------


def test_filter_in():
    spec = ViewSpec(
        table_name="Dim_Country",
        target_view="`main`.`idor`.`dim_country`",
        source=TableSource(full_table_name="`dl`.`udm`.`cust_exp_dim_company`"),
        m_steps=[
            FilterStep("region", "IN", ["REGION 1", "REGION 2", "Italy"]),
        ],
    )
    result = emit_view(spec)
    assert result.error is None
    assert "region IN ('REGION 1', 'REGION 2', 'Italy')" in result.ddl
    assert "WITH src AS" in result.ddl


def test_filter_not_in():
    spec = ViewSpec(
        table_name="Dim_Country",
        target_view="`main`.`s`.`v`",
        source=TableSource(full_table_name="`dl`.`s`.`t`"),
        m_steps=[
            FilterStep("country_abbreviation", "NOT IN", ["RU", "BY", "MUL"]),
        ],
    )
    result = emit_view(spec)
    assert "country_abbreviation NOT IN ('RU', 'BY', 'MUL')" in result.ddl


def test_filter_is_null():
    spec = ViewSpec(
        table_name="T",
        target_view="`a`.`b`.`v`",
        source=TableSource(full_table_name="`a`.`b`.`t`"),
        m_steps=[FilterStep("col", "IS NULL")],
    )
    result = emit_view(spec)
    assert "col IS NULL" in result.ddl


def test_filter_equality():
    spec = ViewSpec(
        table_name="T",
        target_view="`a`.`b`.`v`",
        source=TableSource(full_table_name="`a`.`b`.`t`"),
        m_steps=[FilterStep("type", "=", "ZRU")],
    )
    result = emit_view(spec)
    assert "type = 'ZRU'" in result.ddl


# ---------------------------------------------------------------------------
# ReplaceStep
# ---------------------------------------------------------------------------


def test_replace_step_uses_except_syntax():
    spec = ViewSpec(
        table_name="Dim_Country",
        target_view="`m`.`s`.`v`",
        source=TableSource(full_table_name="`dl`.`s`.`t`"),
        m_steps=[ReplaceStep("country_id", "ROI", "IE")],
    )
    result = emit_view(spec)
    assert result.error is None
    assert "* EXCEPT (country_id)" in result.ddl
    assert "replace(country_id, 'ROI', 'IE') AS country_id" in result.ddl


def test_chained_replace_steps():
    """Two ReplaceStep on same column produce two CTEs, chaining correctly."""
    spec = ViewSpec(
        table_name="T",
        target_view="`m`.`s`.`v`",
        source=TableSource(full_table_name="`dl`.`s`.`t`"),
        m_steps=[
            ReplaceStep("country_id", "ROI", "IE"),
            ReplaceStep("country_id", "CR", "HR"),
        ],
    )
    result = emit_view(spec)
    assert result.error is None
    # Both replace expressions must appear
    assert "replace(country_id, 'ROI', 'IE')" in result.ddl
    assert "replace(country_id, 'CR', 'HR')" in result.ddl


# ---------------------------------------------------------------------------
# DeduplicateStep
# ---------------------------------------------------------------------------


def test_dedup_step_generates_row_number():
    spec = ViewSpec(
        table_name="Dim_Country",
        target_view="`m`.`s`.`v`",
        source=TableSource(full_table_name="`dl`.`s`.`t`"),
        m_steps=[DeduplicateStep(["country_abbreviation"])],
    )
    result = emit_view(spec)
    assert result.error is None
    assert "ROW_NUMBER() OVER" in result.ddl
    assert "PARTITION BY country_abbreviation" in result.ddl
    assert "_rn = 1" in result.ddl
    # dedup CTE should not expose _rn column
    assert "* EXCEPT (_rn)" in result.ddl


def test_dedup_step_with_order_expr():
    spec = ViewSpec(
        table_name="T",
        target_view="`m`.`s`.`v`",
        source=TableSource(full_table_name="`dl`.`s`.`t`"),
        m_steps=[
            DeduplicateStep(
                ["country_abbreviation"],
                order_expr="CASE WHEN country_id = 'EE' THEN 0 ELSE 1 END, country_id",
            )
        ],
    )
    result = emit_view(spec)
    assert "CASE WHEN country_id = 'EE' THEN 0 ELSE 1 END" in result.ddl


# ---------------------------------------------------------------------------
# AppendRowsStep
# ---------------------------------------------------------------------------


def test_append_rows_step():
    spec = ViewSpec(
        table_name="Dim_Country",
        target_view="`m`.`s`.`v`",
        source=TableSource(full_table_name="`dl`.`s`.`t`"),
        m_steps=[
            AppendRowsStep(
                columns=[
                    ("region", "STRING"),
                    ("country", "STRING"),
                    ("country_id", "STRING"),
                ],
                rows=[
                    {"region": "REGION 1", "country": "Lithuania", "country_id": "LT"},
                    {"region": "REGION 1", "country": "Latvia", "country_id": "LV"},
                ],
            )
        ],
    )
    result = emit_view(spec)
    assert result.error is None
    assert "UNION ALL" in result.ddl
    assert "'Lithuania'" in result.ddl
    assert "'LT'" in result.ddl
    assert "_appended" in result.ddl


def test_append_rows_empty_is_noop():
    spec = ViewSpec(
        table_name="T",
        target_view="`m`.`s`.`v`",
        source=TableSource(full_table_name="`dl`.`s`.`t`"),
        m_steps=[AppendRowsStep(columns=[("id", "INT")], rows=[])],
    )
    result = emit_view(spec)
    assert result.error is None
    assert "UNION ALL" not in result.ddl


# ---------------------------------------------------------------------------
# GroupByStep
# ---------------------------------------------------------------------------


def test_groupby_step():
    spec = ViewSpec(
        table_name="Fact_CE",
        target_view="`m`.`s`.`v`",
        source=TableSource(full_table_name="`dl`.`s`.`fact_ce`"),
        m_steps=[
            GroupByStep(
                group_cols=["country_id", "fiscper_date"],
                aggregations={
                    "CustomerPulsedTotal": "CAST(SUM(distinct_customer_pulsed) AS DOUBLE)"
                },
            )
        ],
    )
    result = emit_view(spec)
    assert result.error is None
    assert "GROUP BY country_id, fiscper_date" in result.ddl
    assert "SUM(distinct_customer_pulsed)" in result.ddl
    assert "CustomerPulsedTotal" in result.ddl


# ---------------------------------------------------------------------------
# CalcColumn
# ---------------------------------------------------------------------------


def test_calc_column_translated():
    spec = ViewSpec(
        table_name="Calendar445",
        target_view="`m`.`s`.`cal445`",
        source=TableSource(full_table_name="`dl`.`s`.`dim_calendar`"),
        calc_columns=[
            CalcColumn(
                "week445_label",
                "concat(CAST(week_445 AS STRING), ' Y', CAST(year AS STRING))",
            ),
            CalcColumn(
                "period_label",
                "date_format(make_date(year, month, 1), 'MMMM yyyy')",
            ),
        ],
    )
    result = emit_view(spec)
    assert result.error is None
    assert "week445_label" in result.ddl
    assert "period_label" in result.ddl
    assert "SELECT *," in result.ddl
    assert not result.todo_steps


def test_calc_column_todo_generates_comment():
    spec = ViewSpec(
        table_name="Calendar445",
        target_view="`m`.`s`.`cal445`",
        source=TableSource(full_table_name="`dl`.`s`.`dim_cal`"),
        calc_columns=[
            CalcColumn(
                "date_flag",
                "-- depends on Fact_DCC",
                translation_status="todo",
                comment="depends on Fact_DCC_Overview_Aggregated",
            )
        ],
    )
    result = emit_view(spec)
    assert result.error is None
    assert result.todo_steps  # at least one todo recorded
    assert "TODO" in result.ddl


# ---------------------------------------------------------------------------
# Materialized view + partition_by + comment
# ---------------------------------------------------------------------------


def test_materialized_view_ddl():
    spec = ViewSpec(
        table_name="Fact_NPS",
        target_view="`main`.`idor`.`otc_ucm_fact_nps`",
        source=TableSource(native_sql="SELECT * FROM `dl`.`udm`.`cust_exp_nps`"),
        materialized=True,
        partition_by=["country_id", "fiscper"],
        comment="OTC: reproduces PBI Fact_NPS.",
    )
    result = emit_view(spec)
    assert result.error is None
    assert "CREATE OR REPLACE MATERIALIZED VIEW" in result.ddl
    assert "PARTITIONED BY (country_id, fiscper)" in result.ddl
    assert "COMMENT" in result.ddl
    assert "OTC: reproduces PBI Fact_NPS." in result.ddl


def test_or_replace_false():
    spec = ViewSpec(
        table_name="T",
        target_view="`m`.`s`.`v`",
        source=TableSource(full_table_name="`dl`.`s`.`t`"),
        or_replace=False,
    )
    result = emit_view(spec)
    assert "CREATE VIEW" in result.ddl
    assert "OR REPLACE" not in result.ddl


# ---------------------------------------------------------------------------
# Combined OTC Dim_Country fixture
# ---------------------------------------------------------------------------


def test_dim_country_full_pipeline():
    """Reproduce the OTC Dim_Country source-layer view shape end-to-end."""
    spec = ViewSpec(
        table_name="Dim_Country",
        target_view="`dc_adb-landing-zone-002`.`idor`.`otc_ucm_dim_country`",
        source=TableSource(
            full_table_name=(
                "`dc_datalake_prod_001`.`udm_datamart_cust_exp`.`cust_exp_dim_company`"
            )
        ),
        m_steps=[
            # S5: region filter
            FilterStep(
                "region",
                "IN",
                ["Italy", "REGION 1", "REGION 2", "REGION 3"],
            ),
            # S5: dedup on country_abbreviation, EE first for Baltics
            DeduplicateStep(
                ["country_abbreviation"],
                order_expr="CASE WHEN country_id = 'EE' THEN 0 ELSE 1 END, country_id",
            ),
            # S5: exclude RU/BY/MUL/FV
            FilterStep("country_abbreviation", "NOT IN", ["RU", "BY", "MUL", "FV"]),
            # S5: country text replaces
            ReplaceStep("country", "Baltics", "Estonia"),
            ReplaceStep("country", "Republic of Ireland", "Ireland"),
            # S5: country_id remaps
            ReplaceStep("country_id", "ROI", "IE"),
            ReplaceStep("country_id", "CR", "HR"),
            # S8: append 5 hard-coded rows
            AppendRowsStep(
                columns=[
                    ("region_sequence_id", "BIGINT"),
                    ("region", "STRING"),
                    ("market_segment", "STRING"),
                    ("business_unit", "STRING"),
                    ("business_unit_abbreviation", "STRING"),
                    ("country", "STRING"),
                    ("country_abbreviation", "STRING"),
                    ("geo_country", "STRING"),
                    ("country_id", "STRING"),
                ],
                rows=[
                    {
                        "region_sequence_id": 2,
                        "region": "REGION 1",
                        "market_segment": "Developing",
                        "business_unit": "BU Poland & Baltics",
                        "business_unit_abbreviation": "PL & BAL",
                        "country": "Lithuania",
                        "country_abbreviation": "LT",
                        "geo_country": "Lithuania",
                        "country_id": "LT",
                    },
                    {
                        "region_sequence_id": 2,
                        "region": "REGION 1",
                        "market_segment": "Developing",
                        "business_unit": "BU Poland & Baltics",
                        "business_unit_abbreviation": "PL & BAL",
                        "country": "Latvia",
                        "country_abbreviation": "LV",
                        "geo_country": "Latvia",
                        "country_id": "LV",
                    },
                ],
            ),
        ],
        materialized=True,
        comment="OTC Management: reproduces PBI Dim_Country.",
    )
    result = emit_view(spec)

    assert result.error is None
    assert result.table_name == "Dim_Country"
    assert "CREATE OR REPLACE MATERIALIZED VIEW" in result.ddl
    assert "WITH src AS" in result.ddl
    assert "ROW_NUMBER() OVER" in result.ddl
    assert "CASE WHEN country_id = 'EE' THEN 0 ELSE 1 END" in result.ddl
    assert "country_abbreviation NOT IN" in result.ddl
    assert "replace(country, 'Baltics', 'Estonia')" in result.ddl
    assert "replace(country_id, 'ROI', 'IE')" in result.ddl
    assert "UNION ALL" in result.ddl
    assert "'Lithuania'" in result.ddl
    assert not result.todo_steps


# ---------------------------------------------------------------------------
# emit_views batch
# ---------------------------------------------------------------------------


def test_emit_views_batch():
    specs = [
        ViewSpec(
            table_name=f"Table{i}",
            target_view=f"`m`.`s`.`view{i}`",
            source=TableSource(full_table_name=f"`dl`.`s`.`t{i}`"),
        )
        for i in range(3)
    ]
    results = emit_views(specs)
    assert len(results) == 3
    assert all(r.error is None for r in results)


def test_emit_views_one_error_does_not_abort_others():
    specs = [
        ViewSpec(
            table_name="Good",
            target_view="`m`.`s`.`good`",
            source=TableSource(full_table_name="`dl`.`s`.`t`"),
        ),
        # Bad: no source → will get error inside emit_view
        ViewSpec(
            table_name="Bad",
            target_view="`m`.`s`.`bad`",
            source=TableSource.__new__(TableSource),  # bypass __post_init__
        ),
        ViewSpec(
            table_name="AlsoGood",
            target_view="`m`.`s`.`also_good`",
            source=TableSource(full_table_name="`dl`.`s`.`t2`"),
        ),
    ]
    # The bad spec may or may not trigger an error — just check that we get 3 results
    results = emit_views(specs)
    assert len(results) == 3
    # Good ones should be fine
    assert results[0].error is None
    assert results[2].error is None


# ---------------------------------------------------------------------------
# Fail-open: bad ViewSpec still returns EmittedView with error set
# ---------------------------------------------------------------------------


def test_fail_open_on_exception():
    """If something unexpected blows up, emit_view returns error, not an exception."""
    spec = ViewSpec(
        table_name="Problematic",
        target_view="`m`.`s`.`v`",
        source=TableSource.__new__(TableSource),  # bypass __post_init__
    )
    # Manually corrupt source so the builder will fail
    spec.source.full_table_name = None
    spec.source.native_sql = None

    result = emit_view(spec)
    assert result.error is not None
    assert "_placeholder" in result.ddl
