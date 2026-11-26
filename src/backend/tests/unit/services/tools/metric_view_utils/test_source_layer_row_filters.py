"""Row filters on source-layer views: DDL, validation, and ALTER for snapshots."""

import pytest

from src.services.tools.metric_view_utils.source_layer_emitter import (
    CalcColumn,
    RowFilter,
    TableSource,
    ViewSpec,
    alter_row_filter_sql,
    emit_view,
    row_filter_arguments_sql,
    row_filter_problems,
)

FN = "main.governance.row_filter_fn"


def _spec(row_filter=None, materialized=True):
    return ViewSpec(
        table_name="Dim Country",
        target_view="`cat`.`sch`.`src_dim_country`",
        source=TableSource(full_table_name="`raw`.`sales`.`countries`"),
        calc_columns=[CalcColumn(name="label", sql_expr="upper(name)")],
        materialized=materialized,
        row_filter=row_filter,
    )


def test_materialized_view_header_carries_row_filter_constants_then_columns():
    rf = RowFilter(function=FN, columns=["company_code"], constants=["src_a=obj.1"])
    ddl = emit_view(_spec(rf)).ddl
    assert ddl.startswith("CREATE OR REPLACE MATERIALIZED VIEW")
    assert (
        "WITH ROW FILTER `main`.`governance`.`row_filter_fn` "
        "ON ('src_a=obj.1', `company_code`)" in ddl
    )
    assert ddl.index("WITH ROW FILTER") < ddl.index("\nAS\n")


def test_no_row_filter_leaves_ddl_unchanged():
    ddl = emit_view(_spec(None)).ddl
    assert "ROW FILTER" not in ddl


def test_row_filter_on_plain_view_is_a_clear_error():
    result = emit_view(_spec(RowFilter(function=FN, columns=["c"]), materialized=False))
    assert result.error is not None
    assert "MATERIALIZED" in result.error
    assert result.ddl.startswith("-- ERROR")


def test_constant_over_255_characters_is_reported_without_echoing_it():
    secret_like = "x" * 256
    problems = row_filter_problems(RowFilter(function=FN, constants=[secret_like]))
    assert len(problems) == 1
    assert "256 characters" in problems[0]
    assert secret_like not in problems[0]


def test_constant_of_exactly_255_characters_is_allowed():
    assert row_filter_problems(RowFilter(function=FN, constants=["y" * 255])) == []


@pytest.mark.parametrize(
    "function",
    ["fn", "a.b", "a.b.c.d", "a.b.c; DROP TABLE x", "a.b.`c d`"],
)
def test_function_name_must_be_three_plain_parts(function):
    assert row_filter_problems(RowFilter(function=function, columns=["c"]))


def test_column_must_be_a_plain_identifier():
    problems = row_filter_problems(RowFilter(function=FN, columns=["a b"]))
    assert any("not a plain identifier" in p for p in problems)


def test_needs_at_least_one_argument():
    problems = row_filter_problems(RowFilter(function=FN))
    assert any("at least one" in p for p in problems)


def test_string_constants_are_escaped():
    args = row_filter_arguments_sql(RowFilter(function=FN, constants=["it's"]))
    assert args == "'it\\'s'"


def test_invalid_row_filter_refuses_to_render_arguments():
    with pytest.raises(ValueError, match="invalid row filter"):
        row_filter_arguments_sql(RowFilter(function="bad", columns=["c"]))


def test_alter_statement_for_existing_table():
    sql = alter_row_filter_sql(
        "`cat`.`sch`.`pbi_targets`", RowFilter(function=FN, columns=["company_code"])
    )
    assert sql == (
        "ALTER TABLE `cat`.`sch`.`pbi_targets` SET ROW FILTER "
        "`main`.`governance`.`row_filter_fn` ON (`company_code`)"
    )
