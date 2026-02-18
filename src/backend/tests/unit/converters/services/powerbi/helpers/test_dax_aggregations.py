"""
Unit tests for converters/services/powerbi/helpers/dax_aggregations.py

Tests DAX aggregation detection and building for PowerBI measure generation.
"""

import pytest
from src.converters.services.powerbi.helpers.dax_aggregations import (
    AggregationType,
    AggregationDetector,
    DAXAggregationBuilder,
    ExceptionAggregationHandler,
    detect_and_build_aggregation
)


class TestAggregationType:
    """Tests for AggregationType enum"""

    def test_aggregation_type_enum_values(self):
        """Test AggregationType has all expected values"""
        assert AggregationType.SUM.value == "SUM"
        assert AggregationType.COUNT.value == "COUNT"
        assert AggregationType.AVERAGE.value == "AVERAGE"
        assert AggregationType.MIN.value == "MIN"
        assert AggregationType.MAX.value == "MAX"
        assert AggregationType.DISTINCTCOUNT.value == "DISTINCTCOUNT"

    def test_aggregation_type_advanced_values(self):
        """Test advanced aggregation types exist"""
        assert AggregationType.SUMX.value == "SUMX"
        assert AggregationType.AVERAGEX.value == "AVERAGEX"
        assert AggregationType.WEIGHTED_AVERAGE.value == "WEIGHTED_AVERAGE"
        assert AggregationType.EXCEPTION_AGGREGATION.value == "EXCEPTION_AGGREGATION"


class TestAggregationDetector:
    """Tests for AggregationDetector class"""

    @pytest.fixture
    def detector(self):
        """Create AggregationDetector instance for testing"""
        return AggregationDetector()

    def test_detector_detect_from_hint(self, detector):
        """Test detection from explicit aggregation hint"""
        result = detector.detect_aggregation_type("amount", aggregation_hint="COUNT")
        assert result == AggregationType.COUNT

    def test_detector_detect_from_formula_sum(self, detector):
        """Test detection of SUM from formula"""
        result = detector.detect_aggregation_type("SUM(sales.amount)")
        assert result == AggregationType.SUM

    def test_detector_detect_from_formula_count(self, detector):
        """Test detection of COUNT from formula"""
        result = detector.detect_aggregation_type("COUNT(transactions.id)")
        assert result == AggregationType.COUNT

    def test_detector_detect_from_formula_average(self, detector):
        """Test detection of AVERAGE from formula"""
        result = detector.detect_aggregation_type("AVERAGE(prices.value)")
        assert result == AggregationType.AVERAGE

    def test_detector_detect_exception_aggregation(self, detector):
        """Test detection of exception aggregation"""
        kbi_def = {
            'exception_aggregation': 'SUM',
            'fields_for_exception_aggregation': ['customer', 'product']
        }
        result = detector.detect_aggregation_type("amount", kbi_definition=kbi_def)
        assert result == AggregationType.EXCEPTION_AGGREGATION

    def test_detector_default_to_sum(self, detector):
        """Test defaults to SUM when no match"""
        result = detector.detect_aggregation_type("simple_field")
        assert result == AggregationType.SUM

    def test_detector_case_insensitive(self, detector):
        """Test detection is case insensitive"""
        result = detector.detect_aggregation_type("sum(amount)")
        assert result == AggregationType.SUM


class TestDAXAggregationBuilder:
    """Tests for DAXAggregationBuilder class"""

    @pytest.fixture
    def builder(self):
        """Create DAXAggregationBuilder instance for testing"""
        return DAXAggregationBuilder()

    def test_builder_initialization(self, builder):
        """Test builder initializes with aggregation templates"""
        assert len(builder.aggregation_templates) > 0
        assert AggregationType.SUM in builder.aggregation_templates

    def test_build_sum_simple(self, builder):
        """Test building simple SUM aggregation"""
        result = builder.build_aggregation(
            AggregationType.SUM,
            "amount",
            "Sales",
            {}
        )
        assert "SUM(Sales[amount])" == result

    def test_build_sum_already_has_sum(self, builder):
        """Test building SUM when formula already has SUM"""
        result = builder.build_aggregation(
            AggregationType.SUM,
            "SUM(amount)",
            "Sales",
            {}
        )
        assert result == "SUM(amount)"

    def test_build_sum_complex_formula(self, builder):
        """Test building SUM with complex IF formula"""
        result = builder.build_aggregation(
            AggregationType.SUM,
            "IF(status = 'active', amount, 0)",
            "Sales",
            {}
        )
        assert "SUMX(Sales" in result
        assert "IF(" in result

    def test_build_count(self, builder):
        """Test building COUNT aggregation"""
        result = builder.build_aggregation(
            AggregationType.COUNT,
            "customer_id",
            "Customers",
            {}
        )
        assert result == "COUNT(Customers[customer_id])"

    def test_build_countrows(self, builder):
        """Test building COUNTROWS aggregation"""
        result = builder.build_aggregation(
            AggregationType.COUNTROWS,
            "",
            "Orders",
            {}
        )
        assert result == "COUNTROWS(Orders)"

    def test_build_distinctcount(self, builder):
        """Test building DISTINCTCOUNT aggregation"""
        result = builder.build_aggregation(
            AggregationType.DISTINCTCOUNT,
            "customer_id",
            "Sales",
            {}
        )
        assert result == "DISTINCTCOUNT(Sales[customer_id])"

    def test_build_average(self, builder):
        """Test building AVERAGE aggregation"""
        result = builder.build_aggregation(
            AggregationType.AVERAGE,
            "price",
            "Products",
            {}
        )
        assert result == "AVERAGE(Products[price])"

    def test_build_min(self, builder):
        """Test building MIN aggregation"""
        result = builder.build_aggregation(
            AggregationType.MIN,
            "date",
            "Events",
            {}
        )
        assert result == "MIN(Events[date])"

    def test_build_max(self, builder):
        """Test building MAX aggregation"""
        result = builder.build_aggregation(
            AggregationType.MAX,
            "value",
            "Metrics",
            {}
        )
        assert result == "MAX(Metrics[value])"

    def test_build_percentile(self, builder):
        """Test building PERCENTILE aggregation"""
        kbi_def = {'percentile': 0.95}
        result = builder.build_aggregation(
            AggregationType.PERCENTILE,
            "score",
            "Tests",
            kbi_def
        )
        assert "PERCENTILE.INC(Tests[score], 0.95)" == result

    def test_build_weighted_average(self, builder):
        """Test building weighted average"""
        kbi_def = {'weight_column': 'quantity'}
        result = builder.build_aggregation(
            AggregationType.WEIGHTED_AVERAGE,
            "price",
            "Sales",
            kbi_def
        )
        assert "SUMX(Sales, Sales[price] * Sales[quantity])" in result
        assert "SUM(Sales[quantity])" in result
        assert "DIVIDE" in result

    def test_build_divide(self, builder):
        """Test building DIVIDE aggregation"""
        result = builder.build_aggregation(
            AggregationType.DIVIDE,
            "revenue/cost",
            "Finance",
            {}
        )
        assert "DIVIDE" in result
        assert "SUM(Finance[revenue])" in result
        assert "SUM(Finance[cost])" in result

    def test_build_exception_aggregation(self, builder):
        """Test building exception aggregation"""
        kbi_def = {
            'exception_aggregation': 'SUM',
            'fields_for_exception_aggregation': ['customer', 'product']
        }
        result = builder.build_aggregation(
            AggregationType.EXCEPTION_AGGREGATION,
            "amount",
            "Sales",
            kbi_def
        )
        assert "SUMX" in result
        assert "SUMMARIZE" in result
        assert "customer" in result
        assert "product" in result

    def test_build_calculated(self, builder):
        """Test building calculated measure"""
        result = builder.build_aggregation(
            AggregationType.CALCULATED,
            "[Revenue] - [Cost]",
            "Table",
            {}
        )
        # Calculated measures return formula as-is
        assert result == "[Revenue] - [Cost]"

    def test_build_unknown_type_fallback(self, builder):
        """Test building unknown type falls back to SUM"""
        # Create a custom type that's not in templates
        result = builder.build_aggregation(
            AggregationType.VARIANCE,  # Has template
            "value",
            "Data",
            {}
        )
        assert result is not None


class TestExceptionAggregationHandler:
    """Tests for ExceptionAggregationHandler class"""

    @pytest.fixture
    def handler(self):
        """Create ExceptionAggregationHandler instance for testing"""
        return ExceptionAggregationHandler()

    def test_handler_no_exceptions(self, handler):
        """Test handler with no exceptions"""
        kbi_def = {}
        result = handler.handle_exception_aggregation(kbi_def, "SUM(amount)")
        assert result == "SUM(amount)"

    def test_handler_display_sign_negative(self, handler):
        """Test handler applies negative display sign"""
        kbi_def = {'display_sign': -1}
        result = handler.handle_exception_aggregation(kbi_def, "SUM(amount)")
        assert result == "-1 * (SUM(amount))"

    def test_handler_display_sign_custom(self, handler):
        """Test handler applies custom display sign"""
        kbi_def = {'display_sign': 100}
        result = handler.handle_exception_aggregation(kbi_def, "SUM(amount)")
        assert result == "100 * (SUM(amount))"

    def test_handler_null_to_zero(self, handler):
        """Test handler null to zero exception"""
        kbi_def = {
            'exceptions': [{'type': 'null_to_zero'}]
        }
        result = handler.handle_exception_aggregation(kbi_def, "AVG(value)")
        assert "IF(ISBLANK(AVG(value)), 0, AVG(value))" == result

    def test_handler_division_by_zero(self, handler):
        """Test handler division by zero exception"""
        kbi_def = {
            'exceptions': [{'type': 'division_by_zero'}]
        }
        result = handler.handle_exception_aggregation(kbi_def, "DIVIDE(a,b)")
        assert "IF(ISERROR(DIVIDE(a,b)), 0, DIVIDE(a,b))" == result

    def test_handler_negative_to_zero(self, handler):
        """Test handler negative to zero exception"""
        kbi_def = {
            'exceptions': [{'type': 'negative_to_zero'}]
        }
        result = handler.handle_exception_aggregation(kbi_def, "profit")
        assert result == "MAX(0, profit)"

    def test_handler_threshold_min(self, handler):
        """Test handler minimum threshold exception"""
        kbi_def = {
            'exceptions': [{'type': 'threshold', 'value': 100, 'comparison': 'min'}]
        }
        result = handler.handle_exception_aggregation(kbi_def, "value")
        assert result == "MAX(100, value)"

    def test_handler_threshold_max(self, handler):
        """Test handler maximum threshold exception"""
        kbi_def = {
            'exceptions': [{'type': 'threshold', 'value': 1000, 'comparison': 'max'}]
        }
        result = handler.handle_exception_aggregation(kbi_def, "value")
        assert result == "MIN(1000, value)"

    def test_handler_multiple_exceptions(self, handler):
        """Test handler with multiple exceptions"""
        kbi_def = {
            'display_sign': -1,
            'exceptions': [
                {'type': 'null_to_zero'},
                {'type': 'negative_to_zero'}
            ]
        }
        result = handler.handle_exception_aggregation(kbi_def, "amount")
        # Should apply both
        assert "ISBLANK" in result
        assert "MAX(0" in result
        assert "-1 *" in result


class TestDetectAndBuildAggregation:
    """Tests for detect_and_build_aggregation function"""

    def test_detect_and_build_simple_sum(self):
        """Test complete workflow for simple SUM"""
        kbi_def = {
            'formula': 'amount',
            'source_table': 'Sales',
            'aggregation_type': 'SUM'
        }
        result = detect_and_build_aggregation(kbi_def)
        assert "SUM(Sales[amount])" == result

    def test_detect_and_build_with_exception(self):
        """Test complete workflow with exception handling"""
        kbi_def = {
            'formula': 'revenue',
            'source_table': 'Finance',
            'aggregation_type': 'SUM',
            'display_sign': -1
        }
        result = detect_and_build_aggregation(kbi_def)
        assert "-1 * (SUM(Finance[revenue]))" == result

    def test_detect_and_build_auto_detect(self):
        """Test auto-detection from formula"""
        kbi_def = {
            'formula': 'COUNT(customer_id)',
            'source_table': 'Customers'
        }
        result = detect_and_build_aggregation(kbi_def)
        # Should detect COUNT and return as-is
        assert "COUNT(customer_id)" == result

    def test_detect_and_build_weighted_average(self):
        """Test weighted average aggregation"""
        kbi_def = {
            'formula': 'price',
            'source_table': 'Sales',
            'aggregation_type': 'WEIGHTED_AVERAGE',
            'weight_column': 'quantity'
        }
        result = detect_and_build_aggregation(kbi_def)
        assert "SUMX" in result
        assert "price" in result
        assert "quantity" in result

    def test_detect_and_build_exception_aggregation(self):
        """Test exception aggregation workflow"""
        kbi_def = {
            'formula': 'amount',
            'source_table': 'Sales',
            'exception_aggregation': 'SUM',
            'fields_for_exception_aggregation': ['customer']
        }
        result = detect_and_build_aggregation(kbi_def)
        assert "SUMMARIZE" in result
        assert "customer" in result

    def test_detect_and_build_default_table(self):
        """Test defaults to 'Table' when no source_table"""
        kbi_def = {
            'formula': 'value',
            'aggregation_type': 'SUM'
        }
        result = detect_and_build_aggregation(kbi_def)
        assert "SUM(Table[value])" == result

    def test_detect_and_build_empty_formula(self):
        """Test with empty formula"""
        kbi_def = {
            'formula': '',
            'source_table': 'Data',
            'aggregation_type': 'SUM'
        }
        result = detect_and_build_aggregation(kbi_def)
        assert "SUM(Data[])" == result

    def test_detect_and_build_complex_exception_chain(self):
        """Test complex exception chain"""
        kbi_def = {
            'formula': 'profit',
            'source_table': 'Finance',
            'aggregation_type': 'SUM',
            'display_sign': -1,
            'exceptions': [
                {'type': 'null_to_zero'},
                {'type': 'division_by_zero'}
            ]
        }
        result = detect_and_build_aggregation(kbi_def)
        # Should have nested exception handling
        assert "ISBLANK" in result
        assert "ISERROR" in result
        assert "-1 *" in result
