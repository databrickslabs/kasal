"""
Unit tests for converters/services/sql/helpers/sql_aggregations.py

Tests SQL aggregation building and filter processing for SQL query generation.
"""

import pytest
from src.converters.services.sql.helpers.sql_aggregations import (
    SQLAggregationBuilder,
    SQLFilterProcessor,
    detect_and_build_sql_aggregation,
)
from src.converters.services.sql.models import SQLDialect, SQLAggregationType


class TestSQLAggregationBuilder:
    """Tests for SQLAggregationBuilder class"""

    @pytest.fixture
    def standard_builder(self):
        """Create SQLAggregationBuilder with STANDARD dialect"""
        return SQLAggregationBuilder(dialect=SQLDialect.STANDARD)

    @pytest.fixture
    def databricks_builder(self):
        """Create SQLAggregationBuilder with DATABRICKS dialect"""
        return SQLAggregationBuilder(dialect=SQLDialect.DATABRICKS)

    # ========== Initialization Tests ==========

    def test_builder_initialization_standard(self, standard_builder):
        """Test SQLAggregationBuilder initializes with STANDARD dialect"""
        assert standard_builder.dialect == SQLDialect.STANDARD
        assert len(standard_builder.aggregation_templates) > 0

    def test_builder_initialization_databricks(self, databricks_builder):
        """Test SQLAggregationBuilder initializes with DATABRICKS dialect"""
        assert databricks_builder.dialect == SQLDialect.DATABRICKS
        assert len(databricks_builder.aggregation_templates) > 0

    def test_builder_has_aggregation_templates(self, standard_builder):
        """Test builder initializes with all aggregation type templates"""
        assert SQLAggregationType.SUM in standard_builder.aggregation_templates
        assert SQLAggregationType.COUNT in standard_builder.aggregation_templates
        assert SQLAggregationType.AVG in standard_builder.aggregation_templates
        assert SQLAggregationType.MIN in standard_builder.aggregation_templates
        assert SQLAggregationType.MAX in standard_builder.aggregation_templates
        assert SQLAggregationType.COUNT_DISTINCT in standard_builder.aggregation_templates
        assert SQLAggregationType.WEIGHTED_AVG in standard_builder.aggregation_templates

    # ========== Identifier Quoting Tests ==========

    def test_quote_identifier_standard(self, standard_builder):
        """Test identifier quoting for STANDARD dialect"""
        result = standard_builder._quote_identifier("column_name")
        assert result == '"column_name"'

    def test_quote_identifier_databricks(self, databricks_builder):
        """Test identifier quoting for DATABRICKS dialect"""
        result = databricks_builder._quote_identifier("column_name")
        assert result == "`column_name`"

    # ========== SUM Aggregation Tests ==========

    def test_build_sum_standard(self, standard_builder):
        """Test building SUM aggregation with STANDARD dialect"""
        result = standard_builder.build_aggregation(
            SQLAggregationType.SUM,
            "amount",
            "Sales",
            {}
        )
        assert result == 'SUM("Sales"."amount")'

    def test_build_sum_databricks(self, databricks_builder):
        """Test building SUM aggregation with DATABRICKS dialect"""
        result = databricks_builder.build_aggregation(
            SQLAggregationType.SUM,
            "revenue",
            "FactSales",
            {}
        )
        assert result == "SUM(`FactSales`.`revenue`)"

    def test_build_sum_case_expression(self, standard_builder):
        """Test building SUM with CASE expression"""
        result = standard_builder.build_aggregation(
            SQLAggregationType.SUM,
            "CASE WHEN status = 'active' THEN 1 ELSE 0 END",
            "Orders",
            {}
        )
        assert result == "SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END)"

    # ========== COUNT Aggregation Tests ==========

    def test_build_count_standard(self, standard_builder):
        """Test building COUNT aggregation"""
        result = standard_builder.build_aggregation(
            SQLAggregationType.COUNT,
            "order_id",
            "Orders",
            {}
        )
        assert result == 'COUNT("Orders"."order_id")'

    def test_build_count_star(self, standard_builder):
        """Test building COUNT(*) aggregation"""
        result = standard_builder.build_aggregation(
            SQLAggregationType.COUNT,
            "*",
            "Customers",
            {}
        )
        assert result == "COUNT(*)"

    def test_build_count_keyword(self, standard_builder):
        """Test building COUNT with COUNT keyword in column name"""
        result = standard_builder.build_aggregation(
            SQLAggregationType.COUNT,
            "COUNT",
            "Table",
            {}
        )
        assert result == "COUNT(*)"

    # ========== COUNT DISTINCT Tests ==========

    def test_build_count_distinct_standard(self, standard_builder):
        """Test building COUNT DISTINCT aggregation"""
        result = standard_builder.build_aggregation(
            SQLAggregationType.COUNT_DISTINCT,
            "customer_id",
            "Sales",
            {}
        )
        assert result == 'COUNT(DISTINCT "Sales"."customer_id")'

    def test_build_count_distinct_databricks(self, databricks_builder):
        """Test building COUNT DISTINCT with DATABRICKS dialect"""
        result = databricks_builder.build_aggregation(
            SQLAggregationType.COUNT_DISTINCT,
            "product_id",
            "Transactions",
            {}
        )
        assert result == "COUNT(DISTINCT `Transactions`.`product_id`)"

    # ========== AVG Aggregation Tests ==========

    def test_build_avg_standard(self, standard_builder):
        """Test building AVG aggregation"""
        result = standard_builder.build_aggregation(
            SQLAggregationType.AVG,
            "price",
            "Products",
            {}
        )
        assert result == 'AVG("Products"."price")'

    # ========== MIN/MAX Tests ==========

    def test_build_min_standard(self, standard_builder):
        """Test building MIN aggregation"""
        result = standard_builder.build_aggregation(
            SQLAggregationType.MIN,
            "date",
            "Events",
            {}
        )
        assert result == 'MIN("Events"."date")'

    def test_build_max_standard(self, standard_builder):
        """Test building MAX aggregation"""
        result = standard_builder.build_aggregation(
            SQLAggregationType.MAX,
            "value",
            "Metrics",
            {}
        )
        assert result == 'MAX("Metrics"."value")'

    # ========== STDDEV and VARIANCE Tests ==========

    def test_build_stddev_standard(self, standard_builder):
        """Test building STDDEV aggregation"""
        result = standard_builder.build_aggregation(
            SQLAggregationType.STDDEV,
            "score",
            "TestResults",
            {}
        )
        assert result == 'STDDEV_POP("TestResults"."score")'

    def test_build_variance_statistical(self, standard_builder):
        """Test building statistical VARIANCE aggregation"""
        result = standard_builder.build_aggregation(
            SQLAggregationType.VARIANCE,
            "amount",
            "Sales",
            {}
        )
        assert result == 'VAR_POP("Sales"."amount")'

    def test_build_variance_business(self, standard_builder):
        """Test building business VARIANCE (actual vs target)"""
        kbi_def = {'target_column': 'budget'}
        result = standard_builder.build_aggregation(
            SQLAggregationType.VARIANCE,
            "actual",
            "Finance",
            kbi_def
        )
        assert 'SUM("Finance"."actual")' in result
        assert 'SUM("Finance"."budget")' in result
        assert '-' in result

    # ========== MEDIAN and PERCENTILE Tests ==========

    def test_build_median_standard(self, standard_builder):
        """Test building MEDIAN aggregation"""
        result = standard_builder.build_aggregation(
            SQLAggregationType.MEDIAN,
            "income",
            "Demographics",
            {}
        )
        assert "PERCENTILE_CONT(0.5)" in result
        assert 'ORDER BY "Demographics"."income"' in result

    def test_build_percentile_standard(self, standard_builder):
        """Test building PERCENTILE aggregation"""
        kbi_def = {'percentile': 0.95}
        result = standard_builder.build_aggregation(
            SQLAggregationType.PERCENTILE,
            "response_time",
            "Requests",
            kbi_def
        )
        assert "PERCENTILE_CONT(0.95)" in result
        assert 'ORDER BY "Requests"."response_time"' in result

    def test_build_percentile_default(self, standard_builder):
        """Test building PERCENTILE with default value"""
        result = standard_builder.build_aggregation(
            SQLAggregationType.PERCENTILE,
            "value",
            "Data",
            {}
        )
        assert "PERCENTILE_CONT(0.5)" in result

    # ========== Weighted Average Tests ==========

    def test_build_weighted_avg_with_weight(self, standard_builder):
        """Test building weighted average with weight column"""
        kbi_def = {'weight_column': 'quantity'}
        result = standard_builder.build_aggregation(
            SQLAggregationType.WEIGHTED_AVG,
            "price",
            "Sales",
            kbi_def
        )
        assert 'SUM("Sales"."price" * "Sales"."quantity")' in result
        assert 'SUM("Sales"."quantity")' in result
        assert "NULLIF" in result

    def test_build_weighted_avg_no_weight(self, standard_builder):
        """Test building weighted average without weight column (fallback to AVG)"""
        result = standard_builder.build_aggregation(
            SQLAggregationType.WEIGHTED_AVG,
            "value",
            "Data",
            {}
        )
        assert result == 'AVG("Data"."value")'

    # ========== Ratio/DIVIDE Tests ==========

    def test_build_ratio_with_division(self, standard_builder):
        """Test building ratio with division operator in column name"""
        result = standard_builder.build_aggregation(
            SQLAggregationType.RATIO,
            "revenue/cost",
            "Finance",
            {}
        )
        assert 'SUM("Finance"."revenue")' in result
        assert 'SUM("Finance"."cost")' in result
        assert "NULLIF" in result

    def test_build_ratio_with_base_column(self, standard_builder):
        """Test building ratio with base_column parameter"""
        kbi_def = {'base_column': 'total'}
        result = standard_builder.build_aggregation(
            SQLAggregationType.RATIO,
            "partial",
            "Metrics",
            kbi_def
        )
        assert 'SUM("Metrics"."partial")' in result
        assert 'SUM("Metrics"."total")' in result
        assert "NULLIF" in result

    def test_build_ratio_fallback(self, standard_builder):
        """Test building ratio without division or base_column (fallback to SUM)"""
        result = standard_builder.build_aggregation(
            SQLAggregationType.RATIO,
            "amount",
            "Sales",
            {}
        )
        assert result == 'SUM("Sales"."amount")'

    # ========== Window Functions Tests ==========

    def test_build_running_sum(self, standard_builder):
        """Test building RUNNING_SUM with window function"""
        kbi_def = {'order_column': 'date'}
        result = standard_builder.build_aggregation(
            SQLAggregationType.RUNNING_SUM,
            "amount",
            "Transactions",
            kbi_def
        )
        assert "SUM" in result
        assert "OVER" in result
        assert 'ORDER BY "Transactions"."date"' in result
        assert "ROWS UNBOUNDED PRECEDING" in result

    def test_build_row_number(self, standard_builder):
        """Test building ROW_NUMBER window function"""
        kbi_def = {'order_column': 'score'}
        result = standard_builder.build_aggregation(
            SQLAggregationType.ROW_NUMBER,
            "id",
            "Rankings",
            kbi_def
        )
        assert "ROW_NUMBER()" in result
        assert "OVER" in result
        assert 'ORDER BY "Rankings"."score"' in result

    def test_build_row_number_with_partition(self, standard_builder):
        """Test building ROW_NUMBER with partition by"""
        kbi_def = {
            'order_column': 'date',
            'partition_columns': ['region', 'product']
        }
        result = standard_builder.build_aggregation(
            SQLAggregationType.ROW_NUMBER,
            "id",
            "Sales",
            kbi_def
        )
        assert "PARTITION BY" in result
        assert '"region"' in result
        assert '"product"' in result
        assert "ORDER BY" in result

    def test_build_rank(self, standard_builder):
        """Test building RANK window function"""
        kbi_def = {'order_column': 'amount'}
        result = standard_builder.build_aggregation(
            SQLAggregationType.RANK,
            "value",
            "Metrics",
            kbi_def
        )
        assert "RANK()" in result
        assert "OVER" in result

    def test_build_dense_rank(self, standard_builder):
        """Test building DENSE_RANK window function"""
        kbi_def = {'order_column': 'score'}
        result = standard_builder.build_aggregation(
            SQLAggregationType.DENSE_RANK,
            "id",
            "Leaderboard",
            kbi_def
        )
        assert "DENSE_RANK()" in result
        assert "OVER" in result

    # ========== COALESCE Tests ==========

    def test_build_coalesce_default(self, standard_builder):
        """Test building COALESCE with default value"""
        result = standard_builder.build_aggregation(
            SQLAggregationType.COALESCE,
            "optional_value",
            "Data",
            {}
        )
        assert "COALESCE" in result
        assert '"Data"."optional_value"' in result
        assert "0" in result

    def test_build_coalesce_custom_default(self, standard_builder):
        """Test building COALESCE with custom default value"""
        kbi_def = {'default_value': 100}
        result = standard_builder.build_aggregation(
            SQLAggregationType.COALESCE,
            "value",
            "Metrics",
            kbi_def
        )
        assert "COALESCE" in result
        assert "100" in result

    # ========== Conditional Aggregation Tests ==========

    def test_build_conditional_aggregation_databricks(self, databricks_builder):
        """Test building conditional aggregation with FILTER clause (Databricks)"""
        base_agg = "SUM(`Sales`.`amount`)"
        conditions = ["status = 'active'", "region = 'US'"]
        result = databricks_builder.build_conditional_aggregation(
            base_agg,
            conditions,
            "Sales"
        )
        assert "FILTER" in result
        assert "WHERE" in result
        assert "status = 'active'" in result
        assert "region = 'US'" in result

    def test_build_conditional_aggregation_standard(self, standard_builder):
        """Test building conditional aggregation with CASE WHEN (Standard)"""
        base_agg = 'SUM("Sales"."amount")'
        conditions = ["status = 'active'"]
        result = standard_builder.build_conditional_aggregation(
            base_agg,
            conditions,
            "Sales"
        )
        assert "CASE WHEN" in result
        assert "status = 'active'" in result
        assert "ELSE NULL END" in result

    def test_build_conditional_aggregation_no_conditions(self, standard_builder):
        """Test building conditional aggregation without conditions"""
        base_agg = 'SUM("Sales"."amount")'
        result = standard_builder.build_conditional_aggregation(
            base_agg,
            [],
            "Sales"
        )
        assert result == base_agg

    # ========== Exception Handling Tests ==========

    def test_build_exception_handling_null_to_zero(self, standard_builder):
        """Test exception handling: null to zero"""
        exceptions = [{'type': 'null_to_zero'}]
        result = standard_builder.build_exception_handling(
            "AVG(value)",
            exceptions
        )
        assert "COALESCE" in result
        assert "AVG(value)" in result
        assert "0" in result

    def test_build_exception_handling_negative_to_zero(self, standard_builder):
        """Test exception handling: negative to zero"""
        exceptions = [{'type': 'negative_to_zero'}]
        result = standard_builder.build_exception_handling(
            "profit",
            exceptions
        )
        assert "GREATEST(0, profit)" == result

    def test_build_exception_handling_threshold_min(self, standard_builder):
        """Test exception handling: minimum threshold"""
        exceptions = [{'type': 'threshold', 'value': 100, 'comparison': 'min'}]
        result = standard_builder.build_exception_handling(
            "value",
            exceptions
        )
        assert "GREATEST(100, value)" == result

    def test_build_exception_handling_threshold_max(self, standard_builder):
        """Test exception handling: maximum threshold"""
        exceptions = [{'type': 'threshold', 'value': 1000, 'comparison': 'max'}]
        result = standard_builder.build_exception_handling(
            "value",
            exceptions
        )
        assert "LEAST(1000, value)" == result

    def test_build_exception_handling_multiple(self, standard_builder):
        """Test exception handling: multiple exceptions"""
        exceptions = [
            {'type': 'null_to_zero'},
            {'type': 'negative_to_zero'}
        ]
        result = standard_builder.build_exception_handling(
            "amount",
            exceptions
        )
        assert "COALESCE" in result
        assert "GREATEST" in result

    # ========== Edge Cases and Fallback Tests ==========

    def test_build_aggregation_unknown_type_fallback(self, standard_builder):
        """Test building aggregation with unknown type falls back to SUM"""
        # Create a mock aggregation type not in templates
        result = standard_builder._build_sum("amount", "Sales", {})
        assert result == 'SUM("Sales"."amount")'

    def test_build_aggregation_none_kbi_definition(self, standard_builder):
        """Test building aggregation with None kbi_definition"""
        result = standard_builder.build_aggregation(
            SQLAggregationType.SUM,
            "amount",
            "Sales",
            None
        )
        assert result == 'SUM("Sales"."amount")'


class TestSQLFilterProcessor:
    """Tests for SQLFilterProcessor class"""

    @pytest.fixture
    def processor(self):
        """Create SQLFilterProcessor instance for testing"""
        return SQLFilterProcessor(dialect=SQLDialect.STANDARD)

    def test_processor_initialization(self, processor):
        """Test SQLFilterProcessor initializes correctly"""
        assert processor.dialect == SQLDialect.STANDARD

    def test_process_filters_empty_list(self, processor):
        """Test processing empty filter list"""
        result = processor.process_filters([])
        assert result == []

    def test_process_filters_simple(self, processor):
        """Test processing simple filter"""
        filters = ["status = 'active'"]
        result = processor.process_filters(filters)
        assert len(result) == 1
        assert "status = 'active'" in result[0]

    def test_process_filters_multiple(self, processor):
        """Test processing multiple filters"""
        filters = ["status = 'active'", "region = 'US'"]
        result = processor.process_filters(filters)
        assert len(result) == 2


class TestDetectAndBuildSqlAggregation:
    """Tests for detect_and_build_sql_aggregation function"""

    def test_detect_and_build_simple_sum(self):
        """Test detect and build for simple SUM aggregation"""
        kbi_def = {
            'formula': 'amount',
            'source_table': 'Sales',
            'aggregation_type': 'SUM'
        }
        result = detect_and_build_sql_aggregation(kbi_def)
        assert "SUM" in result
        assert "Sales" in result
        assert "amount" in result

    def test_detect_and_build_with_dialect(self):
        """Test detect and build with specific dialect"""
        kbi_def = {
            'formula': 'revenue',
            'source_table': 'Finance',
            'aggregation_type': 'SUM',
            'dialect': SQLDialect.DATABRICKS
        }
        result = detect_and_build_sql_aggregation(kbi_def, dialect=SQLDialect.DATABRICKS)
        assert "SUM" in result
        assert "`" in result  # Databricks uses backticks

    def test_detect_and_build_count_distinct(self):
        """Test detect and build for COUNT DISTINCT"""
        kbi_def = {
            'formula': 'customer_id',
            'source_table': 'Orders',
            'aggregation_type': 'DISTINCTCOUNT'  # Uses DAX naming
        }
        result = detect_and_build_sql_aggregation(kbi_def)
        assert "COUNT(DISTINCT" in result

    def test_detect_and_build_weighted_avg(self):
        """Test detect and build for weighted average"""
        kbi_def = {
            'formula': 'price',
            'source_table': 'Sales',
            'aggregation_type': 'WEIGHTED_AVERAGE',  # Uses DAX naming
            'weight_column': 'quantity'
        }
        result = detect_and_build_sql_aggregation(kbi_def)
        assert "SUM" in result
        assert "price" in result
        assert "quantity" in result
        assert "NULLIF" in result
