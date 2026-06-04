"""Tests for shared utility functions."""
import pytest
from src.engines.crewai.tools.custom.metric_view_utils.utils import (
    to_snake_case, col_to_readable, spark_sql_compat, unflatten_table_name,
    load_mapping, yaml_scalar,
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
            catalog="cat", schema="sch",
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


class TestUnflattenTableName:
    def test_triple_underscore_unflattened(self):
        result = unflatten_table_name("cat.sch.cat__sch__tbl", catalog="cat", schema="sch")
        assert result == "cat.sch.tbl"

    def test_no_flatten_passes_through(self):
        result = unflatten_table_name("cat.sch.regular_table")
        assert result == "cat.sch.regular_table"

    def test_no_matching_prefix(self):
        result = unflatten_table_name("other.prefix.cat__sch__tbl", catalog="cat", schema="sch")
        # Prefix "cat.sch." doesn't match "other.prefix...", so remainder = full name
        # "other.prefix.cat__sch__tbl" splits on __ -> ["other.prefix.cat", "sch", "tbl"] (3 parts)
        # -> joined as "other.prefix.cat.sch.tbl"
        assert result == "other.prefix.cat.sch.tbl"

    def test_two_parts_not_unflattened(self):
        result = unflatten_table_name("cat.sch.a__b", catalog="cat", schema="sch")
        # Only 2 parts after split — returns flat_name unchanged
        assert result == "cat.sch.a__b"

    def test_empty_catalog_schema(self):
        result = unflatten_table_name("a__b__c")
        # prefix is "." — doesn't match, remainder = full name
        # a__b__c splits into 3 parts -> "a.b.c"
        assert result == "a.b.c"


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
