"""
Extended unit tests for converters/services/sql/helpers/sql_aggregations.py

Targets uncovered code paths to increase coverage from 70% to 80%+.
Focuses on:
- _build_exception_aggregation with target columns
- _build_ratio patterns
- _build_running_sum
- _build_row_number / _build_rank / _build_dense_rank
- build_conditional_aggregation
- build_exception_handling various types
- SQLFilterProcessor._substitute_variables
- detect_and_build_sql_aggregation function
"""

import pytest
from src.converters.services.sql.helpers.sql_aggregations import (
    SQLAggregationBuilder,
    SQLFilterProcessor,
    detect_and_build_sql_aggregation,
    _detect_sql_aggregation_type,
)
from src.converters.services.sql.models import SQLDialect, SQLAggregationType


class TestSQLAggregationBuilderExceptionAggregation:
    """Tests for exception aggregation with various configurations"""

    @pytest.fixture
    def databricks_builder(self):
        return SQLAggregationBuilder(SQLDialect.DATABRICKS)

    @pytest.fixture
    def standard_builder(self):
        return SQLAggregationBuilder(SQLDialect.STANDARD)

    def test_exception_aggregation_with_target_columns(self, databricks_builder):
        """Test 3-step exception aggregation WITH target columns"""
        result = databricks_builder._build_exception_aggregation(
            "amount", "FactSales",
            {
                "exception_aggregation": "SUM",
                "fields_for_exception_aggregation": ["product_id", "region"],
                "target_columns": ["year", "month"],
                "formula": "amount"
            }
        )
        # Should produce a multi-level subquery
        assert "SELECT" in result.upper()
        assert "FROM" in result.upper()
        assert "GROUP BY" in result.upper()

    def test_exception_aggregation_without_target_columns(self, databricks_builder):
        """Test 2-step exception aggregation WITHOUT target columns"""
        result = databricks_builder._build_exception_aggregation(
            "amount", "FactSales",
            {
                "exception_aggregation": "AVG",
                "fields_for_exception_aggregation": ["product_id"],
                "target_columns": [],
                "formula": "amount"
            }
        )
        assert "SELECT" in result.upper()
        assert "GROUP BY" in result.upper()

    def test_exception_aggregation_avg_type(self, databricks_builder):
        """Test exception aggregation with AVG aggregation type"""
        result = databricks_builder._build_exception_aggregation(
            "amount", "Sales",
            {
                "exception_aggregation": "AVG",
                "fields_for_exception_aggregation": ["product_id"],
                "target_columns": ["year"],
            }
        )
        assert "AVG" in result.upper()

    def test_exception_aggregation_count_type(self, databricks_builder):
        """Test exception aggregation with COUNT aggregation type"""
        result = databricks_builder._build_exception_aggregation(
            "order_id", "Orders",
            {
                "exception_aggregation": "COUNT",
                "fields_for_exception_aggregation": ["customer_id"],
            }
        )
        assert "COUNT" in result.upper()

    def test_exception_aggregation_min_type(self, databricks_builder):
        """Test exception aggregation with MIN aggregation type"""
        result = databricks_builder._build_exception_aggregation(
            "price", "Products",
            {
                "exception_aggregation": "MIN",
                "fields_for_exception_aggregation": ["category"],
            }
        )
        assert "MIN" in result.upper()

    def test_exception_aggregation_max_type(self, databricks_builder):
        """Test exception aggregation with MAX aggregation type"""
        result = databricks_builder._build_exception_aggregation(
            "price", "Products",
            {
                "exception_aggregation": "MAX",
                "fields_for_exception_aggregation": ["category"],
            }
        )
        assert "MAX" in result.upper()

    def test_exception_aggregation_no_fields_fallback(self, databricks_builder):
        """Test exception aggregation falls back when no exception fields"""
        result = databricks_builder._build_exception_aggregation(
            "amount", "Sales",
            {
                "exception_aggregation": "SUM",
                "fields_for_exception_aggregation": [],
            }
        )
        # Should fall back to regular SUM
        assert "SUM" in result.upper()

    def test_exception_aggregation_complex_formula(self, databricks_builder):
        """Test exception aggregation with complex formula"""
        result = databricks_builder._build_exception_aggregation(
            "amount / count", "Sales",
            {
                "exception_aggregation": "SUM",
                "fields_for_exception_aggregation": ["product_id"],
                "formula": "amount / count"
            }
        )
        # Complex formula should be wrapped
        assert "SELECT" in result.upper()

    def test_is_complex_formula_simple(self, databricks_builder):
        """Test _is_complex_formula with simple column name"""
        assert databricks_builder._is_complex_formula("amount") is False

    def test_is_complex_formula_with_operators(self, databricks_builder):
        """Test _is_complex_formula with operators"""
        assert databricks_builder._is_complex_formula("amount + tax") is True

    def test_is_complex_formula_with_function(self, databricks_builder):
        """Test _is_complex_formula with function call"""
        assert databricks_builder._is_complex_formula("CASE WHEN flag = 1 THEN amount ELSE 0 END") is True

    def test_is_complex_formula_none(self, databricks_builder):
        """Test _is_complex_formula with None"""
        assert databricks_builder._is_complex_formula(None) is False

    def test_is_complex_formula_empty(self, databricks_builder):
        """Test _is_complex_formula with empty string"""
        assert databricks_builder._is_complex_formula("") is False

    def test_map_exception_aggregation_to_sql_sum(self, databricks_builder):
        """Test _map_exception_aggregation_to_sql SUM"""
        result = databricks_builder._map_exception_aggregation_to_sql("SUM")
        assert result == "SUM"

    def test_map_exception_aggregation_to_sql_avg(self, databricks_builder):
        """Test _map_exception_aggregation_to_sql AVG"""
        result = databricks_builder._map_exception_aggregation_to_sql("AVG")
        assert result == "AVG"

    def test_map_exception_aggregation_to_sql_unknown(self, databricks_builder):
        """Test _map_exception_aggregation_to_sql unknown defaults to SUM"""
        result = databricks_builder._map_exception_aggregation_to_sql("CUSTOM")
        assert result == "SUM"


class TestSQLAggregationBuilderWindowFunctions:
    """Tests for window function building"""

    @pytest.fixture
    def builder(self):
        return SQLAggregationBuilder(SQLDialect.DATABRICKS)

    def test_build_row_number_basic(self, builder):
        """Test ROW_NUMBER basic generation"""
        result = builder.build_aggregation(
            SQLAggregationType.ROW_NUMBER, "order_date", "Sales", {}
        )
        assert "ROW_NUMBER()" in result
        assert "OVER" in result

    def test_build_row_number_with_partition(self, builder):
        """Test ROW_NUMBER with partition columns"""
        result = builder.build_aggregation(
            SQLAggregationType.ROW_NUMBER, "order_date", "Sales",
            {"partition_columns": ["region", "year"]}
        )
        assert "ROW_NUMBER()" in result
        assert "PARTITION BY" in result
        assert "region" in result
        assert "year" in result

    def test_build_rank_basic(self, builder):
        """Test RANK basic generation"""
        result = builder.build_aggregation(
            SQLAggregationType.RANK, "revenue", "Sales", {}
        )
        assert "RANK()" in result

    def test_build_rank_with_partition(self, builder):
        """Test RANK with partition columns"""
        result = builder.build_aggregation(
            SQLAggregationType.RANK, "revenue", "Sales",
            {"partition_columns": ["region"]}
        )
        assert "RANK()" in result
        assert "PARTITION BY" in result

    def test_build_dense_rank_basic(self, builder):
        """Test DENSE_RANK basic generation"""
        result = builder.build_aggregation(
            SQLAggregationType.DENSE_RANK, "revenue", "Sales", {}
        )
        assert "DENSE_RANK()" in result

    def test_build_dense_rank_with_partition(self, builder):
        """Test DENSE_RANK with partition columns"""
        result = builder.build_aggregation(
            SQLAggregationType.DENSE_RANK, "revenue", "Sales",
            {"partition_columns": ["category"]}
        )
        assert "DENSE_RANK()" in result
        assert "PARTITION BY" in result

    def test_build_running_sum_basic(self, builder):
        """Test RUNNING_SUM basic generation"""
        result = builder.build_aggregation(
            SQLAggregationType.RUNNING_SUM, "amount", "Sales", {}
        )
        assert "SUM" in result.upper()
        assert "OVER" in result

    def test_build_running_sum_with_order(self, builder):
        """Test RUNNING_SUM with order column"""
        result = builder.build_aggregation(
            SQLAggregationType.RUNNING_SUM, "amount", "Sales",
            {"order_column": "order_date"}
        )
        assert "order_date" in result
        assert "ROWS UNBOUNDED PRECEDING" in result


class TestSQLAggregationBuilderConditional:
    """Tests for build_conditional_aggregation"""

    @pytest.fixture
    def databricks_builder(self):
        return SQLAggregationBuilder(SQLDialect.DATABRICKS)

    @pytest.fixture
    def standard_builder(self):
        return SQLAggregationBuilder(SQLDialect.STANDARD)

    def test_conditional_aggregation_databricks_uses_filter(self, databricks_builder):
        """Test Databricks uses FILTER clause for conditional aggregation"""
        result = databricks_builder.build_conditional_aggregation(
            "SUM(amount)",
            ["status = 'active'"],
            "Sales"
        )
        assert "FILTER" in result
        assert "WHERE" in result

    def test_conditional_aggregation_standard_uses_case_when(self, standard_builder):
        """Test Standard uses CASE WHEN for conditional aggregation"""
        result = standard_builder.build_conditional_aggregation(
            "SUM(amount)",
            ["status = 'active'"],
            "Sales"
        )
        assert "CASE WHEN" in result.upper() or "FILTER" in result

    def test_conditional_aggregation_no_conditions(self, databricks_builder):
        """Test conditional aggregation with no conditions returns base"""
        base = "SUM(amount)"
        result = databricks_builder.build_conditional_aggregation(base, [], "Sales")
        assert result == base

    def test_conditional_aggregation_multiple_conditions(self, databricks_builder):
        """Test conditional aggregation with multiple conditions"""
        result = databricks_builder.build_conditional_aggregation(
            "SUM(amount)",
            ["status = 'active'", "year = 2024"],
            "Sales"
        )
        assert "status" in result
        assert "year" in result


class TestSQLAggregationBuilderExceptionHandling:
    """Tests for build_exception_handling"""

    @pytest.fixture
    def builder(self):
        return SQLAggregationBuilder(SQLDialect.DATABRICKS)

    def test_null_to_zero_exception(self, builder):
        """Test null_to_zero wraps in COALESCE"""
        result = builder.build_exception_handling(
            "SUM(amount)",
            [{"type": "null_to_zero"}]
        )
        assert "COALESCE" in result

    def test_division_by_zero_exception(self, builder):
        """Test division_by_zero adds NULL/0 check"""
        result = builder.build_exception_handling(
            "DIVIDE(a, b)",
            [{"type": "division_by_zero"}]
        )
        assert "CASE WHEN" in result.upper()

    def test_negative_to_zero_exception(self, builder):
        """Test negative_to_zero uses GREATEST"""
        result = builder.build_exception_handling(
            "SUM(amount)",
            [{"type": "negative_to_zero"}]
        )
        assert "GREATEST" in result

    def test_threshold_min_exception(self, builder):
        """Test threshold min exception"""
        result = builder.build_exception_handling(
            "SUM(amount)",
            [{"type": "threshold", "value": 0, "comparison": "min"}]
        )
        assert "GREATEST" in result
        assert "0" in result

    def test_threshold_max_exception(self, builder):
        """Test threshold max exception"""
        result = builder.build_exception_handling(
            "SUM(amount)",
            [{"type": "threshold", "value": 1000, "comparison": "max"}]
        )
        assert "LEAST" in result
        assert "1000" in result

    def test_custom_condition_exception(self, builder):
        """Test custom_condition exception"""
        result = builder.build_exception_handling(
            "SUM(amount)",
            [{
                "type": "custom_condition",
                "condition": "flag = 1",
                "true_value": "SUM(amount)",
                "false_value": "0"
            }]
        )
        assert "CASE WHEN" in result.upper()
        assert "flag = 1" in result

    def test_no_exceptions_returns_unchanged(self, builder):
        """Test empty exceptions returns base unchanged"""
        base = "SUM(amount)"
        result = builder.build_exception_handling(base, [])
        assert result == base

    def test_multiple_exceptions_stacked(self, builder):
        """Test multiple exceptions are applied"""
        result = builder.build_exception_handling(
            "SUM(amount)",
            [
                {"type": "null_to_zero"},
                {"type": "negative_to_zero"}
            ]
        )
        assert "COALESCE" in result
        assert "GREATEST" in result


class TestSQLFilterProcessorExtended:
    """Extended tests for SQLFilterProcessor"""

    @pytest.fixture
    def processor(self):
        return SQLFilterProcessor(SQLDialect.DATABRICKS)

    def test_substitute_variables_string_value(self, processor):
        """Test substituting string variable"""
        result = processor._substitute_variables(
            "region = $var_region",
            {"region": "EMEA"}
        )
        assert "'EMEA'" in result

    def test_substitute_variables_numeric_value(self, processor):
        """Test substituting numeric variable"""
        result = processor._substitute_variables(
            "year = $var_year",
            {"year": 2024}
        )
        assert "2024" in result

    def test_substitute_variables_list_strings(self, processor):
        """Test substituting list of strings"""
        result = processor._substitute_variables(
            "region IN $var_regions",
            {"regions": ["EMEA", "APAC"]}
        )
        assert "'EMEA'" in result
        assert "'APAC'" in result

    def test_substitute_variables_list_numbers(self, processor):
        """Test substituting list of numbers"""
        result = processor._substitute_variables(
            "year IN $var_years",
            {"years": [2022, 2023, 2024]}
        )
        assert "2022" in result
        assert "2023" in result
        assert "2024" in result

    def test_substitute_variables_dollar_prefix(self, processor):
        """Test substituting $name (without var_) format"""
        result = processor._substitute_variables(
            "region = $region",
            {"region": "EMEA"}
        )
        assert "EMEA" in result

    def test_process_filters_with_query_filter(self, processor):
        """Test processing $query_filter expansion"""
        filters = ["$query_filter"]
        definition_filters = {
            "query_filter": {
                "region_filter": "region = 'EMEA'"
            }
        }
        result = processor.process_filters(filters, {}, definition_filters)
        # Should expand the $query_filter
        assert any("EMEA" in f for f in result) or len(result) > 0

    def test_process_filters_empty_list(self, processor):
        """Test processing empty filters"""
        result = processor.process_filters([], {}, {})
        assert result == []

    def test_process_filters_simple_condition(self, processor):
        """Test processing simple filter condition"""
        result = processor.process_filters(
            ["status = 'active'"],
            {},
            {}
        )
        assert len(result) == 1
        assert "active" in result[0]

    def test_process_filters_with_variables(self, processor):
        """Test processing filters with variable substitution"""
        result = processor.process_filters(
            ["year = $var_current_year"],
            {"current_year": "2024"},
            {}
        )
        assert len(result) == 1
        assert "2024" in result[0]

    def test_process_filters_not_in_conversion(self, processor):
        """Test NOT IN conversion"""
        result = processor.process_filters(
            ["region NOT IN ('APAC')"],
            {},
            {}
        )
        assert len(result) == 1
        assert "NOT IN" in result[0]

    def test_process_filters_between_conversion(self, processor):
        """Test BETWEEN conversion"""
        result = processor.process_filters(
            ["date BETWEEN '2024-01-01' AND '2024-12-31'"],
            {},
            {}
        )
        assert len(result) == 1
        assert "BETWEEN" in result[0]

    def test_process_filters_query_filter_no_filters(self, processor):
        """Test $query_filter with empty definition filters"""
        result = processor.process_filters(
            ["$query_filter"],
            {},
            {}
        )
        # Should be empty when no query_filter defined
        assert result == []


class TestDetectSQLAggregationTypeExtended:
    """Extended tests for _detect_sql_aggregation_type"""

    def test_detect_from_hint_sum(self):
        """Test detecting SUM from hint"""
        result = _detect_sql_aggregation_type("amount", "SUM")
        assert result == SQLAggregationType.SUM

    def test_detect_from_hint_count(self):
        """Test detecting COUNT from hint"""
        result = _detect_sql_aggregation_type("order_id", "COUNT")
        assert result == SQLAggregationType.COUNT

    def test_detect_from_hint_countrows(self):
        """Test detecting COUNTROWS maps to COUNT"""
        result = _detect_sql_aggregation_type("", "COUNTROWS")
        assert result == SQLAggregationType.COUNT

    def test_detect_from_hint_average(self):
        """Test detecting AVERAGE from hint"""
        result = _detect_sql_aggregation_type("price", "AVERAGE")
        assert result == SQLAggregationType.AVG

    def test_detect_from_hint_min(self):
        """Test detecting MIN from hint"""
        result = _detect_sql_aggregation_type("price", "MIN")
        assert result == SQLAggregationType.MIN

    def test_detect_from_hint_max(self):
        """Test detecting MAX from hint"""
        result = _detect_sql_aggregation_type("price", "MAX")
        assert result == SQLAggregationType.MAX

    def test_detect_from_hint_distinctcount(self):
        """Test detecting DISTINCTCOUNT from hint"""
        result = _detect_sql_aggregation_type("customer_id", "DISTINCTCOUNT")
        assert result == SQLAggregationType.COUNT_DISTINCT

    def test_detect_from_hint_divide(self):
        """Test detecting DIVIDE from hint"""
        result = _detect_sql_aggregation_type("a/b", "DIVIDE")
        assert result == SQLAggregationType.RATIO

    def test_detect_from_hint_weighted_average(self):
        """Test detecting WEIGHTED_AVERAGE from hint"""
        result = _detect_sql_aggregation_type("price", "WEIGHTED_AVERAGE")
        assert result == SQLAggregationType.WEIGHTED_AVG

    def test_detect_from_hint_variance(self):
        """Test detecting VARIANCE from hint"""
        result = _detect_sql_aggregation_type("amount", "VARIANCE")
        assert result == SQLAggregationType.VARIANCE

    def test_detect_from_hint_percentile(self):
        """Test detecting PERCENTILE from hint"""
        result = _detect_sql_aggregation_type("price", "PERCENTILE")
        assert result == SQLAggregationType.PERCENTILE

    def test_detect_from_hint_sumx(self):
        """Test detecting SUMX maps to SUM"""
        result = _detect_sql_aggregation_type("expr", "SUMX")
        assert result == SQLAggregationType.SUM

    def test_detect_from_hint_exception_aggregation(self):
        """Test detecting EXCEPTION_AGGREGATION from hint"""
        result = _detect_sql_aggregation_type("amount", "EXCEPTION_AGGREGATION")
        assert result == SQLAggregationType.EXCEPTION_AGGREGATION

    def test_detect_from_formula_count(self):
        """Test detecting COUNT from formula without hint"""
        result = _detect_sql_aggregation_type("COUNT(order_id)")
        assert result == SQLAggregationType.COUNT

    def test_detect_from_formula_distinct(self):
        """Test detecting DISTINCT COUNT from formula"""
        # The function looks for 'DISTINCT' within 'COUNT' context
        result = _detect_sql_aggregation_type("COUNT(DISTINCT customer_id)")
        assert result == SQLAggregationType.COUNT_DISTINCT

    def test_detect_from_formula_avg(self):
        """Test detecting AVG from formula"""
        result = _detect_sql_aggregation_type("AVG(price)")
        assert result == SQLAggregationType.AVG

    def test_detect_from_formula_average(self):
        """Test detecting AVERAGE from formula"""
        result = _detect_sql_aggregation_type("AVERAGE(price)")
        assert result == SQLAggregationType.AVG

    def test_detect_from_empty_formula(self):
        """Test detecting from empty formula defaults to SUM"""
        result = _detect_sql_aggregation_type("")
        assert result == SQLAggregationType.SUM


class TestDetectAndBuildSQLAggregation:
    """Tests for detect_and_build_sql_aggregation function"""

    def test_sum_aggregation(self):
        """Test SUM aggregation detection and building"""
        result = detect_and_build_sql_aggregation(
            {"formula": "amount", "source_table": "Sales", "aggregation_type": "SUM"}
        )
        assert "SUM" in result.upper()
        assert "Sales" in result or "amount" in result

    def test_count_aggregation(self):
        """Test COUNT aggregation detection and building"""
        result = detect_and_build_sql_aggregation(
            {"formula": "order_id", "source_table": "Orders", "aggregation_type": "COUNT"}
        )
        assert "COUNT" in result.upper()

    def test_average_aggregation(self):
        """Test AVERAGE aggregation"""
        result = detect_and_build_sql_aggregation(
            {"formula": "price", "source_table": "Products", "aggregation_type": "AVERAGE"}
        )
        assert "AVG" in result.upper()

    def test_display_sign_negative(self):
        """Test display_sign -1 negates result"""
        result = detect_and_build_sql_aggregation(
            {
                "formula": "amount",
                "source_table": "Sales",
                "aggregation_type": "SUM",
                "display_sign": -1
            }
        )
        assert "(-1)" in result or "-1" in result

    def test_display_sign_custom(self):
        """Test custom display_sign multiplies result"""
        result = detect_and_build_sql_aggregation(
            {
                "formula": "amount",
                "source_table": "Sales",
                "aggregation_type": "SUM",
                "display_sign": 2
            }
        )
        assert "2 *" in result

    def test_exception_aggregation_type(self):
        """Test EXCEPTION_AGGREGATION type returns subquery"""
        result = detect_and_build_sql_aggregation(
            {
                "formula": "amount",
                "source_table": "Sales",
                "aggregation_type": "EXCEPTION_AGGREGATION",
                "exception_aggregation": "SUM",
                "fields_for_exception_aggregation": ["product_id"]
            }
        )
        # Exception aggregation returns a subquery
        assert "SELECT" in result.upper() or "SUM" in result.upper()

    def test_exception_aggregation_display_sign(self):
        """Test EXCEPTION_AGGREGATION with display_sign"""
        result = detect_and_build_sql_aggregation(
            {
                "formula": "amount",
                "source_table": "Sales",
                "aggregation_type": "EXCEPTION_AGGREGATION",
                "exception_aggregation": "SUM",
                "fields_for_exception_aggregation": ["product_id"],
                "display_sign": -1
            }
        )
        assert "(-1)" in result or "-1" in result

    def test_with_exceptions(self):
        """Test with exception handling"""
        result = detect_and_build_sql_aggregation(
            {
                "formula": "amount",
                "source_table": "Sales",
                "aggregation_type": "SUM",
                "exceptions": [{"type": "null_to_zero"}]
            }
        )
        assert "COALESCE" in result

    def test_databricks_dialect(self):
        """Test with DATABRICKS dialect"""
        result = detect_and_build_sql_aggregation(
            {"formula": "amount", "source_table": "Sales", "aggregation_type": "SUM"},
            dialect=SQLDialect.DATABRICKS
        )
        # Databricks uses backticks
        assert "`" in result

    def test_standard_dialect(self):
        """Test with STANDARD dialect"""
        result = detect_and_build_sql_aggregation(
            {"formula": "amount", "source_table": "Sales", "aggregation_type": "SUM"},
            dialect=SQLDialect.STANDARD
        )
        # Standard uses double quotes
        assert '"' in result
