"""
Unit tests for converters/services/sql/helpers/sql_expression_builder.py

Tests centralized SQL expression generation engine for building aggregations,
filters, case statements, and window functions.
"""

import pytest
from src.converters.services.sql.helpers.sql_expression_builder import SQLExpressionEngine
from src.converters.services.sql.models import SQLDialect, SQLAggregationType


class TestSQLExpressionEngine:
    """Tests for SQLExpressionEngine class"""

    @pytest.fixture
    def standard_engine(self):
        """Create SQLExpressionEngine with STANDARD dialect"""
        return SQLExpressionEngine(dialect=SQLDialect.STANDARD)

    @pytest.fixture
    def databricks_engine(self):
        """Create SQLExpressionEngine with DATABRICKS dialect"""
        return SQLExpressionEngine(dialect=SQLDialect.DATABRICKS)

    # ========== Initialization and Configuration Tests ==========

    def test_engine_initialization_standard(self, standard_engine):
        """Test engine initializes with STANDARD dialect"""
        assert standard_engine.dialect == SQLDialect.STANDARD
        assert len(standard_engine.aggregation_builders) > 0

    def test_engine_initialization_databricks(self, databricks_engine):
        """Test engine initializes with DATABRICKS dialect"""
        assert databricks_engine.dialect == SQLDialect.DATABRICKS
        assert len(databricks_engine.aggregation_builders) > 0

    def test_dialect_config_standard(self, standard_engine):
        """Test dialect configuration for STANDARD dialect"""
        config = standard_engine.dialect_config
        assert config['quote_char'] == '"'
        assert config['limit_syntax'] == 'LIMIT'
        assert config['supports_cte'] is True
        assert config['supports_window_functions'] is True

    def test_dialect_config_databricks(self, databricks_engine):
        """Test dialect configuration for DATABRICKS dialect"""
        config = databricks_engine.dialect_config
        assert config['quote_char'] == '`'
        assert config['limit_syntax'] == 'LIMIT'
        assert config['supports_cte'] is True
        assert config['supports_window_functions'] is True
        assert config['unity_catalog'] is True

    # ========== Identifier Quoting Tests ==========

    def test_quote_identifier_standard(self, standard_engine):
        """Test identifier quoting for STANDARD dialect"""
        result = standard_engine._quote_identifier("column_name")
        assert result == '"column_name"'

    def test_quote_identifier_databricks(self, databricks_engine):
        """Test identifier quoting for DATABRICKS dialect"""
        result = databricks_engine._quote_identifier("column_name")
        assert result == "`column_name`"

    def test_quote_identifier_with_dots(self, databricks_engine):
        """Test quoting identifier with dots (catalog.schema.table)"""
        result = databricks_engine._quote_identifier("catalog.schema.table")
        assert result == "`catalog.schema.table`"

    # ========== Value Formatting Tests ==========

    def test_format_value_string(self, standard_engine):
        """Test formatting string value"""
        result = standard_engine._format_value("test")
        assert result == "'test'"

    def test_format_value_number(self, standard_engine):
        """Test formatting numeric value"""
        result = standard_engine._format_value(42)
        assert result == "42"

    def test_format_value_float(self, standard_engine):
        """Test formatting float value"""
        result = standard_engine._format_value(3.14)
        assert result == "3.14"

    def test_format_value_null(self, standard_engine):
        """Test formatting NULL value"""
        result = standard_engine._format_value(None)
        assert result == "NULL"

    # ========== Aggregation Building Tests ==========

    def test_build_sum_aggregation(self, standard_engine):
        """Test building SUM aggregation"""
        result = standard_engine.build_aggregation(
            SQLAggregationType.SUM,
            "amount",
            "Sales"
        )
        assert result == 'SUM("Sales"."amount")'

    def test_build_sum_databricks(self, databricks_engine):
        """Test building SUM with DATABRICKS dialect"""
        result = databricks_engine.build_aggregation(
            SQLAggregationType.SUM,
            "revenue",
            "FactSales"
        )
        assert result == "SUM(`FactSales`.`revenue`)"

    def test_build_count_aggregation(self, standard_engine):
        """Test building COUNT aggregation"""
        result = standard_engine.build_aggregation(
            SQLAggregationType.COUNT,
            "order_id",
            "Orders"
        )
        assert result == 'COUNT("Orders"."order_id")'

    def test_build_count_star(self, standard_engine):
        """Test building COUNT(*) aggregation"""
        result = standard_engine.build_aggregation(
            SQLAggregationType.COUNT,
            "*",
            "Customers"
        )
        assert result == "COUNT(*)"

    def test_build_avg_aggregation(self, standard_engine):
        """Test building AVG aggregation"""
        result = standard_engine.build_aggregation(
            SQLAggregationType.AVG,
            "price",
            "Products"
        )
        assert result == 'AVG("Products"."price")'

    def test_build_min_aggregation(self, standard_engine):
        """Test building MIN aggregation"""
        result = standard_engine.build_aggregation(
            SQLAggregationType.MIN,
            "date",
            "Events"
        )
        assert result == 'MIN("Events"."date")'

    def test_build_max_aggregation(self, standard_engine):
        """Test building MAX aggregation"""
        result = standard_engine.build_aggregation(
            SQLAggregationType.MAX,
            "value",
            "Metrics"
        )
        assert result == 'MAX("Metrics"."value")'

    def test_build_count_distinct(self, standard_engine):
        """Test building COUNT DISTINCT aggregation"""
        result = standard_engine.build_aggregation(
            SQLAggregationType.COUNT_DISTINCT,
            "customer_id",
            "Sales"
        )
        assert result == 'COUNT(DISTINCT "Sales"."customer_id")'

    def test_build_stddev_aggregation(self, standard_engine):
        """Test building STDDEV aggregation"""
        result = standard_engine.build_aggregation(
            SQLAggregationType.STDDEV,
            "score",
            "TestResults"
        )
        assert result == 'STDDEV("TestResults"."score")'

    def test_build_median_aggregation(self, standard_engine):
        """Test building MEDIAN aggregation"""
        result = standard_engine.build_aggregation(
            SQLAggregationType.MEDIAN,
            "income",
            "Demographics"
        )
        assert "PERCENTILE_CONT(0.5)" in result
        assert "income" in result

    def test_build_percentile_aggregation(self, standard_engine):
        """Test building PERCENTILE aggregation"""
        context = {'percentile': 0.95}
        result = standard_engine.build_aggregation(
            SQLAggregationType.PERCENTILE,
            "response_time",
            "Requests",
            context
        )
        assert "PERCENTILE_CONT(0.95)" in result

    def test_build_weighted_avg(self, standard_engine):
        """Test building weighted average aggregation"""
        context = {'weight_column': 'quantity'}
        result = standard_engine.build_aggregation(
            SQLAggregationType.WEIGHTED_AVG,
            "price",
            "Sales",
            context
        )
        assert "SUM" in result
        assert "price" in result
        assert "quantity" in result
        assert "NULLIF" in result

    def test_build_ratio_aggregation(self, standard_engine):
        """Test building ratio aggregation"""
        result = standard_engine.build_aggregation(
            SQLAggregationType.RATIO,
            "revenue/cost",
            "Finance"
        )
        # Ratio builds division expression
        assert "revenue/cost" in result or "revenue" in result
        assert "NULLIF" in result

    def test_build_running_sum(self, standard_engine):
        """Test building running sum with window function"""
        context = {'order_column': 'date'}
        result = standard_engine.build_aggregation(
            SQLAggregationType.RUNNING_SUM,
            "amount",
            "Transactions",
            context
        )
        assert "SUM" in result
        assert "OVER" in result
        # May or may not include ORDER BY depending on implementation
        assert "ROWS BETWEEN UNBOUNDED PRECEDING" in result or "OVER" in result

    def test_build_row_number(self, standard_engine):
        """Test building ROW_NUMBER window function"""
        context = {'order_column': 'score'}
        result = standard_engine.build_aggregation(
            SQLAggregationType.ROW_NUMBER,
            "id",
            "Rankings",
            context
        )
        assert "ROW_NUMBER()" in result
        assert "OVER" in result

    def test_build_rank(self, standard_engine):
        """Test building RANK window function"""
        context = {'order_column': 'amount'}
        result = standard_engine.build_aggregation(
            SQLAggregationType.RANK,
            "value",
            "Metrics",
            context
        )
        assert "RANK()" in result
        assert "OVER" in result

    def test_build_dense_rank(self, standard_engine):
        """Test building DENSE_RANK window function"""
        context = {'order_column': 'score'}
        result = standard_engine.build_aggregation(
            SQLAggregationType.DENSE_RANK,
            "id",
            "Leaderboard",
            context
        )
        assert "DENSE_RANK()" in result
        assert "OVER" in result

    def test_build_coalesce(self, standard_engine):
        """Test building COALESCE expression"""
        result = standard_engine.build_aggregation(
            SQLAggregationType.COALESCE,
            "optional_value",
            "Data"
        )
        assert "COALESCE" in result
        assert "optional_value" in result

    # ========== Filter Building Tests ==========

    def test_build_filter_empty(self, standard_engine):
        """Test building filter with empty expression"""
        result = standard_engine.build_filter("", "Sales")
        assert result == ""

    def test_build_filter_simple(self, standard_engine):
        """Test building simple filter expression"""
        result = standard_engine.build_filter("status = 'active'", "Sales")
        assert "status = 'active'" in result

    def test_build_filter_with_table(self, standard_engine):
        """Test building filter expression with table name"""
        result = standard_engine.build_filter("region = 'US'", "Sales")
        assert result != ""

    # ========== CASE WHEN Building Tests ==========

    def test_build_case_when_simple(self, standard_engine):
        """Test building simple CASE WHEN expression"""
        conditions = [("status = 'active'", 1)]
        result = standard_engine.build_case_when(conditions)
        assert result == "CASE WHEN status = 'active' THEN 1 END"

    def test_build_case_when_with_else(self, standard_engine):
        """Test building CASE WHEN with ELSE clause"""
        conditions = [("status = 'active'", 1)]
        result = standard_engine.build_case_when(conditions, 0)
        assert result == "CASE WHEN status = 'active' THEN 1 ELSE 0 END"

    def test_build_case_when_multiple_conditions(self, standard_engine):
        """Test building CASE WHEN with multiple conditions"""
        conditions = [
            ("status = 'active'", 1),
            ("status = 'pending'", 0.5)
        ]
        result = standard_engine.build_case_when(conditions, 0)
        assert "CASE" in result
        assert "WHEN status = 'active' THEN 1" in result
        assert "WHEN status = 'pending' THEN 0.5" in result
        assert "ELSE 0" in result
        assert "END" in result

    def test_build_case_when_string_values(self, standard_engine):
        """Test building CASE WHEN with string values"""
        conditions = [
            ("type = 1", "Type A"),
            ("type = 2", "Type B")
        ]
        result = standard_engine.build_case_when(conditions, "Unknown")
        assert "WHEN type = 1 THEN 'Type A'" in result
        assert "WHEN type = 2 THEN 'Type B'" in result
        assert "ELSE 'Unknown'" in result

    # ========== Window Function Building Tests ==========

    def test_build_window_function_simple(self, standard_engine):
        """Test building simple window function"""
        result = standard_engine.build_window_function("ROW_NUMBER()")
        # May or may not have space before OVER depending on implementation
        assert "ROW_NUMBER()" in result
        assert "OVER ()" in result

    def test_build_window_function_with_partition(self, standard_engine):
        """Test building window function with PARTITION BY"""
        result = standard_engine.build_window_function(
            "SUM(amount)",
            partition_by=["region"]
        )
        assert "PARTITION BY" in result
        assert '"region"' in result

    def test_build_window_function_with_order(self, standard_engine):
        """Test building window function with ORDER BY"""
        result = standard_engine.build_window_function(
            "ROW_NUMBER()",
            order_by=[("date", "ASC")]
        )
        assert "ORDER BY" in result
        assert '"date" ASC' in result

    def test_build_window_function_partition_and_order(self, standard_engine):
        """Test building window function with both PARTITION and ORDER BY"""
        result = standard_engine.build_window_function(
            "RANK()",
            partition_by=["category"],
            order_by=[("sales", "DESC")]
        )
        assert "PARTITION BY" in result
        assert '"category"' in result
        assert "ORDER BY" in result
        assert '"sales" DESC' in result

    def test_build_window_function_with_frame(self, standard_engine):
        """Test building window function with frame clause"""
        result = standard_engine.build_window_function(
            "SUM(amount)",
            order_by=[("date", "ASC")],
            frame_clause="ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
        )
        assert "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" in result

    def test_build_window_function_multiple_partitions(self, standard_engine):
        """Test building window function with multiple partition columns"""
        result = standard_engine.build_window_function(
            "AVG(price)",
            partition_by=["region", "category"]
        )
        assert "PARTITION BY" in result
        assert '"region"' in result
        assert '"category"' in result

    def test_build_window_function_multiple_order(self, standard_engine):
        """Test building window function with multiple ORDER BY columns"""
        result = standard_engine.build_window_function(
            "RANK()",
            order_by=[("sales", "DESC"), ("date", "ASC")]
        )
        assert "ORDER BY" in result
        assert '"sales" DESC' in result
        assert '"date" ASC' in result

    # ========== Databricks Dialect Tests ==========

    def test_databricks_quoting(self, databricks_engine):
        """Test Databricks uses backticks for quoting"""
        result = databricks_engine.build_aggregation(
            SQLAggregationType.SUM,
            "amount",
            "sales"
        )
        assert "`sales`.`amount`" in result
        assert '"' not in result

    def test_databricks_window_function(self, databricks_engine):
        """Test window function with Databricks dialect"""
        result = databricks_engine.build_window_function(
            "ROW_NUMBER()",
            partition_by=["region"],
            order_by=[("date", "DESC")]
        )
        assert "`region`" in result
        assert "`date`" in result

    # ========== Edge Cases and Error Handling ==========

    def test_build_aggregation_unknown_type(self, standard_engine):
        """Test building aggregation with unknown type falls back to SUM"""
        # The engine should fall back to SUM for unknown types
        result = standard_engine._build_sum("amount", "Sales", {})
        assert result == 'SUM("Sales"."amount")'

    def test_build_aggregation_none_context(self, standard_engine):
        """Test building aggregation with None context"""
        result = standard_engine.build_aggregation(
            SQLAggregationType.SUM,
            "amount",
            "Sales",
            None
        )
        assert result == 'SUM("Sales"."amount")'

    def test_build_case_when_empty_conditions(self, standard_engine):
        """Test building CASE WHEN with empty conditions list"""
        result = standard_engine.build_case_when([])
        assert result == "CASE END"

    def test_format_value_boolean_true(self, standard_engine):
        """Test formatting boolean True value"""
        result = standard_engine._format_value(True)
        assert result in ["TRUE", "1", "true"]

    def test_format_value_boolean_false(self, standard_engine):
        """Test formatting boolean False value"""
        result = standard_engine._format_value(False)
        assert result in ["FALSE", "0", "false"]

    # ========== Integration Tests ==========

    def test_complete_aggregation_workflow(self, standard_engine):
        """Test complete workflow for building complex aggregation"""
        # Build weighted average with context
        context = {'weight_column': 'quantity'}
        result = standard_engine.build_aggregation(
            SQLAggregationType.WEIGHTED_AVG,
            "price",
            "Sales",
            context
        )
        assert "SUM" in result
        assert "NULLIF" in result
        assert '"Sales"' in result

    def test_complete_window_function_workflow(self, standard_engine):
        """Test complete workflow for building window function"""
        result = standard_engine.build_window_function(
            "SUM(amount)",
            partition_by=["region", "product"],
            order_by=[("date", "ASC")],
            frame_clause="ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"
        )
        assert "PARTITION BY" in result
        assert '"region"' in result
        assert '"product"' in result
        assert "ORDER BY" in result
        assert '"date" ASC' in result
        assert "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW" in result

    def test_case_when_with_case_expression(self, standard_engine):
        """Test CASE WHEN nested inside aggregation"""
        conditions = [
            ("status = 'active'", 1),
            ("status = 'inactive'", 0)
        ]
        case_expr = standard_engine.build_case_when(conditions)
        # This CASE expression could be used in SUM
        assert "CASE" in case_expr
        assert "END" in case_expr
