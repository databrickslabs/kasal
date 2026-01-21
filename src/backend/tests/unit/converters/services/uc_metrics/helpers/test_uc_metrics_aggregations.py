"""
Unit tests for converters/services/uc_metrics/helpers/uc_metrics_aggregations.py

Tests UC Metrics aggregation builders for Spark SQL / Unity Catalog Metrics Store.
Covers SAP BW exception aggregation and constant selection patterns.
"""

import pytest
from src.converters.services.uc_metrics.helpers.uc_metrics_aggregations import (
    UCMetricsAggregationBuilder,
    detect_and_build_aggregation
)
from src.converters.base.models import KPI


class TestUCMetricsAggregationBuilder:
    """Tests for UCMetricsAggregationBuilder class"""

    @pytest.fixture
    def builder(self):
        """Create builder for testing"""
        return UCMetricsAggregationBuilder(dialect="spark")

    @pytest.fixture
    def simple_kpi(self):
        """Simple KPI for testing"""
        return KPI(
            description="Revenue",
            technical_name="revenue",
            formula="amount",
            aggregation_type="SUM"
        )

    # ========== Initialization Tests ==========

    def test_builder_initialization_spark(self, builder):
        """Test builder initializes with Spark dialect"""
        assert builder.dialect == "spark"

    def test_builder_initialization_default(self):
        """Test builder initializes with default dialect"""
        builder = UCMetricsAggregationBuilder()
        assert builder.dialect == "spark"

    # ========== Build Measure Expression Tests ==========

    def test_build_measure_expression_sum(self, builder):
        """Test building SUM expression"""
        kpi = KPI(
            description="Revenue",
            technical_name="revenue",
            formula="amount",
            aggregation_type="SUM"
        )
        result = builder.build_measure_expression(kpi)
        assert result == "SUM(amount)"

    def test_build_measure_expression_count(self, builder):
        """Test building COUNT expression"""
        kpi = KPI(
            description="Row Count",
            technical_name="count",
            formula="*",
            aggregation_type="COUNT"
        )
        result = builder.build_measure_expression(kpi)
        assert result == "COUNT(*)"

    def test_build_measure_expression_distinctcount(self, builder):
        """Test building DISTINCTCOUNT expression"""
        kpi = KPI(
            description="Unique Customers",
            technical_name="unique_customers",
            formula="customer_id",
            aggregation_type="DISTINCTCOUNT"
        )
        result = builder.build_measure_expression(kpi)
        assert result == "COUNT(DISTINCT customer_id)"

    def test_build_measure_expression_average(self, builder):
        """Test building AVERAGE expression"""
        kpi = KPI(
            description="Average Price",
            technical_name="avg_price",
            formula="price",
            aggregation_type="AVERAGE"
        )
        result = builder.build_measure_expression(kpi)
        assert result == "AVG(price)"

    def test_build_measure_expression_min(self, builder):
        """Test building MIN expression"""
        kpi = KPI(
            description="Min Date",
            technical_name="min_date",
            formula="order_date",
            aggregation_type="MIN"
        )
        result = builder.build_measure_expression(kpi)
        assert result == "MIN(order_date)"

    def test_build_measure_expression_max(self, builder):
        """Test building MAX expression"""
        kpi = KPI(
            description="Max Date",
            technical_name="max_date",
            formula="order_date",
            aggregation_type="MAX"
        )
        result = builder.build_measure_expression(kpi)
        assert result == "MAX(order_date)"

    def test_build_measure_expression_calculated(self, builder):
        """Test building CALCULATED expression (no aggregation wrapper)"""
        kpi = KPI(
            description="Profit",
            technical_name="profit",
            formula="[revenue] - [cost]",
            aggregation_type="CALCULATED"
        )
        result = builder.build_measure_expression(kpi)
        assert result == "[revenue] - [cost]"

    def test_build_measure_expression_unknown_type(self, builder):
        """Test building expression with unknown aggregation type (defaults to SUM)"""
        kpi = KPI(
            description="Test",
            technical_name="test",
            formula="value",
            aggregation_type="UNKNOWN"
        )
        result = builder.build_measure_expression(kpi)
        assert result == "SUM(value)"

    def test_build_measure_expression_no_aggregation_type(self, builder):
        """Test building expression with no aggregation type (defaults to SUM)"""
        kpi = KPI(
            description="Test",
            technical_name="test",
            formula="value"
        )
        result = builder.build_measure_expression(kpi)
        assert result == "SUM(value)"

    def test_build_measure_expression_empty_formula(self, builder):
        """Test building expression with empty formula (defaults to 1)"""
        kpi = KPI(
            description="Count",
            technical_name="count",
            formula="",  # Empty string formula
            aggregation_type="COUNT"
        )
        result = builder.build_measure_expression(kpi)
        assert result == "COUNT(1)"

    # ========== Build Measure Expression With Filter Tests ==========

    def test_build_measure_expression_with_filter_no_filter(self, builder, simple_kpi):
        """Test building expression without filter"""
        result = builder.build_measure_expression_with_filter(simple_kpi, None)
        assert result == "SUM(amount)"

    def test_build_measure_expression_with_filter_simple(self, builder):
        """Test building expression with simple filter"""
        kpi = KPI(
            description="Revenue",
            technical_name="revenue",
            formula="amount",
            aggregation_type="SUM"
        )
        result = builder.build_measure_expression_with_filter(kpi, "region = 'US'")
        assert "SUM(amount)" in result
        assert "FILTER (WHERE region = 'US')" in result

    def test_build_measure_expression_with_filter_complex(self, builder):
        """Test building expression with complex filter"""
        kpi = KPI(
            description="Revenue",
            technical_name="revenue",
            formula="amount",
            aggregation_type="SUM"
        )
        result = builder.build_measure_expression_with_filter(
            kpi,
            "region = 'US' AND status = 'active'"
        )
        assert "SUM(amount)" in result
        assert "FILTER (WHERE region = 'US' AND status = 'active')" in result

    def test_build_measure_expression_with_filter_negative_sign(self, builder):
        """Test building expression with display_sign = -1"""
        kpi = KPI(
            description="Cost",
            technical_name="cost",
            formula="cost_amount",
            aggregation_type="SUM",
            display_sign=-1
        )
        result = builder.build_measure_expression_with_filter(kpi, None)
        assert "(-1) * SUM(cost_amount)" in result

    def test_build_measure_expression_with_filter_and_negative_sign(self, builder):
        """Test building expression with filter and display_sign = -1"""
        kpi = KPI(
            description="Cost",
            technical_name="cost",
            formula="cost_amount",
            aggregation_type="SUM",
            display_sign=-1
        )
        result = builder.build_measure_expression_with_filter(kpi, "region = 'US'")
        assert "(-1) *" in result
        assert "SUM(cost_amount)" in result
        assert "FILTER (WHERE region = 'US')" in result

    def test_build_measure_expression_with_filter_count(self, builder):
        """Test building COUNT expression with filter"""
        kpi = KPI(
            description="Count",
            technical_name="count",
            formula="id",
            aggregation_type="COUNT"
        )
        result = builder.build_measure_expression_with_filter(kpi, "status = 'active'")
        assert "COUNT(id)" in result
        assert "FILTER (WHERE status = 'active')" in result

    def test_build_measure_expression_with_filter_distinctcount(self, builder):
        """Test building DISTINCTCOUNT expression with filter"""
        kpi = KPI(
            description="Unique Customers",
            technical_name="unique_customers",
            formula="customer_id",
            aggregation_type="DISTINCTCOUNT"
        )
        result = builder.build_measure_expression_with_filter(kpi, "year = 2023")
        assert "COUNT(DISTINCT customer_id)" in result
        assert "FILTER (WHERE year = 2023)" in result

    def test_build_measure_expression_with_filter_average(self, builder):
        """Test building AVERAGE expression with filter"""
        kpi = KPI(
            description="Average Price",
            technical_name="avg_price",
            formula="price",
            aggregation_type="AVERAGE"
        )
        result = builder.build_measure_expression_with_filter(kpi, "category = 'A'")
        assert "AVG(price)" in result
        assert "FILTER (WHERE category = 'A')" in result

    # ========== Apply Exceptions To Formula Tests ==========

    def test_apply_exceptions_negative_to_zero(self, builder):
        """Test applying negative_to_zero exception"""
        exceptions = [{"type": "negative_to_zero"}]
        result = builder.apply_exceptions_to_formula("revenue", exceptions)
        assert "CASE WHEN revenue < 0 THEN 0 ELSE revenue END" in result

    def test_apply_exceptions_null_to_zero(self, builder):
        """Test applying null_to_zero exception"""
        exceptions = [{"type": "null_to_zero"}]
        result = builder.apply_exceptions_to_formula("revenue", exceptions)
        assert "COALESCE(revenue, 0)" in result

    def test_apply_exceptions_division_by_zero(self, builder):
        """Test applying division_by_zero exception"""
        exceptions = [{"type": "division_by_zero"}]
        result = builder.apply_exceptions_to_formula("revenue / quantity", exceptions)
        assert "CASE WHEN" in result
        assert "= 0 THEN 0" in result
        assert "/ (quantity) END" in result

    def test_apply_exceptions_multiple(self, builder):
        """Test applying multiple exceptions"""
        exceptions = [
            {"type": "null_to_zero"},
            {"type": "negative_to_zero"}
        ]
        result = builder.apply_exceptions_to_formula("revenue", exceptions)
        assert "COALESCE" in result
        assert "CASE WHEN" in result

    def test_apply_exceptions_empty(self, builder):
        """Test applying empty exceptions list"""
        result = builder.apply_exceptions_to_formula("revenue", [])
        assert result == "revenue"

    # ========== Build Exception Aggregation With Window Tests ==========

    def test_build_exception_aggregation_basic(self, builder):
        """Test building exception aggregation without window fields"""
        kpi = KPI(
            description="Revenue",
            technical_name="revenue",
            formula="amount",
            aggregation_type="SUM",
            exception_aggregation="SUM"
        )
        result, window_config = builder.build_exception_aggregation_with_window(kpi, None)
        assert "SUM(" in result
        assert "amount" in result
        assert isinstance(window_config, list)
        assert len(window_config) == 0

    def test_build_exception_aggregation_with_fields(self, builder):
        """Test building exception aggregation with window fields"""
        kpi = KPI(
            description="Inventory",
            technical_name="inventory",
            formula="stock_level",
            aggregation_type="SUM",
            exception_aggregation="SUM",
            fields_for_exception_aggregation=["fiscal_period", "product_id"]
        )
        result, window_config = builder.build_exception_aggregation_with_window(kpi, None)
        assert "SUM(" in result
        assert "stock_level" in result
        assert len(window_config) == 2
        assert window_config[0]["order"] == "fiscal_period"
        assert window_config[0]["semiadditive"] == "last"
        assert window_config[1]["order"] == "product_id"

    def test_build_exception_aggregation_count(self, builder):
        """Test building exception aggregation with COUNT"""
        kpi = KPI(
            description="Count",
            technical_name="count",
            formula="*",
            aggregation_type="SUM",
            exception_aggregation="COUNT"
        )
        result, window_config = builder.build_exception_aggregation_with_window(kpi, None)
        assert "COUNT(" in result

    def test_build_exception_aggregation_avg(self, builder):
        """Test building exception aggregation with AVG"""
        kpi = KPI(
            description="Average",
            technical_name="avg",
            formula="value",
            aggregation_type="SUM",
            exception_aggregation="AVG"
        )
        result, window_config = builder.build_exception_aggregation_with_window(kpi, None)
        assert "AVG(" in result

    def test_build_exception_aggregation_min(self, builder):
        """Test building exception aggregation with MIN"""
        kpi = KPI(
            description="Minimum",
            technical_name="min",
            formula="value",
            aggregation_type="SUM",
            exception_aggregation="MIN"
        )
        result, window_config = builder.build_exception_aggregation_with_window(kpi, None)
        assert "MIN(" in result

    def test_build_exception_aggregation_max(self, builder):
        """Test building exception aggregation with MAX"""
        kpi = KPI(
            description="Maximum",
            technical_name="max",
            formula="value",
            aggregation_type="SUM",
            exception_aggregation="MAX"
        )
        result, window_config = builder.build_exception_aggregation_with_window(kpi, None)
        assert "MAX(" in result

    def test_build_exception_aggregation_negative_sign(self, builder):
        """Test building exception aggregation with display_sign = -1"""
        kpi = KPI(
            description="Cost",
            technical_name="cost",
            formula="amount",
            aggregation_type="SUM",
            exception_aggregation="SUM",
            display_sign=-1
        )
        result, window_config = builder.build_exception_aggregation_with_window(kpi, None)
        assert "(-1) *" in result
        assert "SUM(" in result

    # ========== Build Constant Selection Measure Tests ==========

    def test_build_constant_selection_basic(self, builder):
        """Test building constant selection measure"""
        kpi = KPI(
            description="Inventory",
            technical_name="inventory",
            formula="stock_level",
            aggregation_type="SUM",
            fields_for_constant_selection=["fiscal_period"]
        )
        result, window_config = builder.build_constant_selection_measure(kpi, [])
        assert "SUM(stock_level)" in result
        assert len(window_config) == 1
        assert window_config[0]["order"] == "fiscal_period"
        assert window_config[0]["semiadditive"] == "last"

    def test_build_constant_selection_with_filters(self, builder):
        """Test building constant selection with filters"""
        kpi = KPI(
            description="Inventory",
            technical_name="inventory",
            formula="stock_level",
            aggregation_type="SUM",
            fields_for_constant_selection=["fiscal_period"]
        )
        filters = ["region = 'US'", "status = 'active'"]
        result, window_config = builder.build_constant_selection_measure(kpi, filters)
        assert "SUM(stock_level)" in result
        assert "FILTER" in result
        assert "region = 'US'" in result
        assert "status = 'active'" in result

    def test_build_constant_selection_count(self, builder):
        """Test building constant selection with COUNT"""
        kpi = KPI(
            description="Count",
            technical_name="count",
            formula="id",
            aggregation_type="COUNT",
            fields_for_constant_selection=["fiscal_period"]
        )
        result, window_config = builder.build_constant_selection_measure(kpi, [])
        assert "COUNT(id)" in result
        assert len(window_config) == 1

    def test_build_constant_selection_average(self, builder):
        """Test building constant selection with AVERAGE"""
        kpi = KPI(
            description="Average",
            technical_name="avg",
            formula="value",
            aggregation_type="AVERAGE",
            fields_for_constant_selection=["fiscal_period"]
        )
        result, window_config = builder.build_constant_selection_measure(kpi, [])
        assert "AVG(value)" in result

    def test_build_constant_selection_min(self, builder):
        """Test building constant selection with MIN"""
        kpi = KPI(
            description="Minimum",
            technical_name="min",
            formula="value",
            aggregation_type="MIN",
            fields_for_constant_selection=["fiscal_period"]
        )
        result, window_config = builder.build_constant_selection_measure(kpi, [])
        assert "MIN(value)" in result

    def test_build_constant_selection_max(self, builder):
        """Test building constant selection with MAX"""
        kpi = KPI(
            description="Maximum",
            technical_name="max",
            formula="value",
            aggregation_type="MAX",
            fields_for_constant_selection=["fiscal_period"]
        )
        result, window_config = builder.build_constant_selection_measure(kpi, [])
        assert "MAX(value)" in result

    def test_build_constant_selection_negative_sign(self, builder):
        """Test building constant selection with display_sign = -1"""
        kpi = KPI(
            description="Cost",
            technical_name="cost",
            formula="amount",
            aggregation_type="SUM",
            display_sign=-1,
            fields_for_constant_selection=["fiscal_period"]
        )
        result, window_config = builder.build_constant_selection_measure(kpi, [])
        assert "(-1) *" in result
        assert "SUM(amount)" in result

    def test_build_constant_selection_multiple_fields(self, builder):
        """Test building constant selection with multiple fields"""
        kpi = KPI(
            description="Inventory",
            technical_name="inventory",
            formula="stock_level",
            aggregation_type="SUM",
            fields_for_constant_selection=["fiscal_period", "product_id", "warehouse_id"]
        )
        result, window_config = builder.build_constant_selection_measure(kpi, [])
        assert len(window_config) == 3
        assert window_config[0]["order"] == "fiscal_period"
        assert window_config[1]["order"] == "product_id"
        assert window_config[2]["order"] == "warehouse_id"
        assert all(w["semiadditive"] == "last" for w in window_config)


class TestDetectAndBuildAggregation:
    """Tests for detect_and_build_aggregation convenience function"""

    def test_detect_and_build_sum(self):
        """Test detecting and building SUM aggregation"""
        kpi = KPI(
            description="Revenue",
            technical_name="revenue",
            formula="amount",
            aggregation_type="SUM"
        )
        result = detect_and_build_aggregation(kpi)
        assert result == "SUM(amount)"

    def test_detect_and_build_count(self):
        """Test detecting and building COUNT aggregation"""
        kpi = KPI(
            description="Row Count",
            technical_name="count",
            formula="*",
            aggregation_type="COUNT"
        )
        result = detect_and_build_aggregation(kpi)
        assert result == "COUNT(*)"

    def test_detect_and_build_distinctcount(self):
        """Test detecting and building DISTINCTCOUNT aggregation"""
        kpi = KPI(
            description="Unique Customers",
            technical_name="unique_customers",
            formula="customer_id",
            aggregation_type="DISTINCTCOUNT"
        )
        result = detect_and_build_aggregation(kpi)
        assert result == "COUNT(DISTINCT customer_id)"

    def test_detect_and_build_average(self):
        """Test detecting and building AVERAGE aggregation"""
        kpi = KPI(
            description="Average Price",
            technical_name="avg_price",
            formula="price",
            aggregation_type="AVERAGE"
        )
        result = detect_and_build_aggregation(kpi)
        assert result == "AVG(price)"

    def test_detect_and_build_min(self):
        """Test detecting and building MIN aggregation"""
        kpi = KPI(
            description="Min Date",
            technical_name="min_date",
            formula="order_date",
            aggregation_type="MIN"
        )
        result = detect_and_build_aggregation(kpi)
        assert result == "MIN(order_date)"

    def test_detect_and_build_max(self):
        """Test detecting and building MAX aggregation"""
        kpi = KPI(
            description="Max Date",
            technical_name="max_date",
            formula="order_date",
            aggregation_type="MAX"
        )
        result = detect_and_build_aggregation(kpi)
        assert result == "MAX(order_date)"

    def test_detect_and_build_calculated(self):
        """Test detecting and building CALCULATED (no wrapper)"""
        kpi = KPI(
            description="Profit",
            technical_name="profit",
            formula="[revenue] - [cost]",
            aggregation_type="CALCULATED"
        )
        result = detect_and_build_aggregation(kpi)
        assert result == "[revenue] - [cost]"

    def test_detect_and_build_default(self):
        """Test detecting and building with no aggregation type (defaults to SUM)"""
        kpi = KPI(
            description="Value",
            technical_name="value",
            formula="amount"
        )
        result = detect_and_build_aggregation(kpi)
        assert result == "SUM(amount)"
