"""Tests for shared utility functions."""

import pytest

from src.services.tools.metric_view_utils.utils import (
    col_to_readable,
    load_mapping,
    spark_sql_compat,
    to_snake_case,
    yaml_scalar,
)


class TestToSnakeCase:
    def test_basic_spaces(self):
        assert to_snake_case("Total Sales") == "total_sales"

    def test_percent_sign(self):
        assert to_snake_case("Sales %") == "sales_pct"

    def test_special_chars_stripped(self):
        result = to_snake_case("Revenue (BEV)")
        assert result == "revenue_bev"

    def test_already_snake(self):
        assert to_snake_case("total_sales") == "total_sales"

    def test_mixed_case_lowered(self):
        assert to_snake_case("TotalSalesRevenue") == "totalsalesrevenue"

    def test_leading_trailing_whitespace(self):
        assert to_snake_case("  hello world  ") == "hello_world"

    def test_multiple_underscores_collapsed(self):
        assert to_snake_case("a___b") == "a_b"

    def test_empty_string(self):
        assert to_snake_case("") == ""

    def test_single_word(self):
        assert to_snake_case("Revenue") == "revenue"

    def test_multiple_percents(self):
        assert to_snake_case("% Rate %") == "pct_rate_pct"

    def test_hyphens_replaced(self):
        assert to_snake_case("year-to-date") == "year_to_date"


class TestColToReadable:
    def test_basic(self):
        assert col_to_readable("comp_code") == "Comp code"

    def test_single_word(self):
        assert col_to_readable("region") == "Region"

    def test_multiple_underscores(self):
        assert col_to_readable("total_net_value") == "Total net value"

    def test_empty(self):
        assert col_to_readable("") == ""


class TestSparkSqlCompat:
    def test_getdate_replacement(self):
        result = spark_sql_compat("WHERE date > GETDATE()")
        assert "CURRENT_DATE()" in result
        assert "GETDATE" not in result

    def test_isnull_replacement(self):
        result = spark_sql_compat("ISNULL(col, 0)")
        assert "COALESCE(col, 0)" in result
        assert "ISNULL" not in result

    def test_int_cast_replacement(self):
        result = spark_sql_compat("INT(col)")
        assert "CAST(col AS INT)" in result
        assert "INT(" not in result

    def test_convert_replacement(self):
        result = spark_sql_compat("CONVERT(INT, col)")
        assert "CAST(col AS INT)" in result
        assert "CONVERT" not in result

    def test_no_rewrite_2part_by_default(self):
        result = spark_sql_compat("FROM schema.table", catalog="cat", schema="sch")
        assert "schema.table" in result

    def test_rewrite_2part_when_enabled(self):
        result = spark_sql_compat(
            "FROM myschema.mytable",
            catalog="cat",
            schema="sch",
            rewrite_2part_tables=True,
        )
        assert "cat.sch.myschema__mytable" in result

    def test_block_comment_stripped(self):
        result = spark_sql_compat("/* comment */ SELECT 1")
        assert "/* comment */" not in result
        assert "SELECT 1" in result

    def test_nested_int_cast(self):
        result = spark_sql_compat("INT(INT(col))")
        assert "CAST(CAST(col AS INT) AS INT)" in result

    def test_no_op_when_nothing_to_rewrite(self):
        expr = "SUM(source.amount)"
        assert spark_sql_compat(expr) == expr

    def test_isnull_case_insensitive(self):
        result = spark_sql_compat("isnull(val, 0)")
        assert "COALESCE(val, 0)" in result


class TestLoadMapping:
    def test_list_passthrough(self):
        data = [{"name": "x"}]
        result = load_mapping(data)
        assert result == data

    def test_file_path_raises_on_missing(self):
        with pytest.raises(FileNotFoundError):
            load_mapping("/nonexistent/file.json")


class TestYamlScalarUtil:
    def test_no_newline(self):
        assert yaml_scalar("hello") == "hello"

    def test_with_newline(self):
        result = yaml_scalar("line1\nline2")
        assert result.startswith("|-")
        assert "line1" in result
        assert "line2" in result

    def test_empty_line_in_multiline(self):
        result = yaml_scalar("a\n\nb")
        assert result.startswith("|-")
