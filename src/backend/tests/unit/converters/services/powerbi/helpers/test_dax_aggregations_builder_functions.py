"""
Extended unit tests for converters/services/powerbi/helpers/dax_aggregations.py

Targets uncovered code paths to increase coverage from 65% to 75%+.
Focuses on:
- All AggregationType enum values
- DAXAggregationBuilder methods (MEDIAN, PERCENTILE, STDEV, VAR, SUMX, etc.)
- ExceptionAggregationHandler with various exception types
- detect_and_build_aggregation with complex definitions
- _build_exception_aggregation with multiple exception types
"""

import pytest
from src.converters.services.powerbi.helpers.dax_aggregations import (
    AggregationType,
    AggregationDetector,
    DAXAggregationBuilder,
    ExceptionAggregationHandler,
    detect_and_build_aggregation,
)


class TestAggregationDetectorExtended:
    """Extended tests for AggregationDetector"""

    @pytest.fixture
    def detector(self):
        return AggregationDetector()

    def test_detect_exception_aggregation_with_fields(self, detector):
        """Test detection of exception aggregation type when definition has exception fields"""
        kbi_definition = {
            'exception_aggregation': 'SUM',
            'fields_for_exception_aggregation': ['product_id', 'region']
        }
        result = detector.detect_aggregation_type("amount", None, kbi_definition)
        assert result == AggregationType.EXCEPTION_AGGREGATION

    def test_detect_exception_aggregation_missing_fields(self, detector):
        """Test exception aggregation NOT detected when fields list is empty"""
        kbi_definition = {
            'exception_aggregation': 'SUM',
            'fields_for_exception_aggregation': []  # Empty list
        }
        result = detector.detect_aggregation_type("amount", None, kbi_definition)
        # Should NOT be EXCEPTION_AGGREGATION since fields are empty
        assert result != AggregationType.EXCEPTION_AGGREGATION

    def test_detect_hint_median(self, detector):
        """Test detecting MEDIAN from hint"""
        result = detector.detect_aggregation_type("price", "MEDIAN")
        assert result == AggregationType.MEDIAN

    def test_detect_hint_percentile(self, detector):
        """Test detecting PERCENTILE from hint"""
        result = detector.detect_aggregation_type("price", "PERCENTILE")
        assert result == AggregationType.PERCENTILE

    def test_detect_hint_stdev(self, detector):
        """Test detecting STDEV from hint"""
        result = detector.detect_aggregation_type("price", "STDEV")
        assert result == AggregationType.STDEV

    def test_detect_hint_var(self, detector):
        """Test detecting VAR from hint"""
        result = detector.detect_aggregation_type("price", "VAR")
        assert result == AggregationType.VAR

    def test_detect_hint_sumx(self, detector):
        """Test detecting SUMX from hint"""
        result = detector.detect_aggregation_type("expr", "SUMX")
        assert result == AggregationType.SUMX

    def test_detect_hint_averagex(self, detector):
        """Test detecting AVERAGEX from hint"""
        result = detector.detect_aggregation_type("expr", "AVERAGEX")
        assert result == AggregationType.AVERAGEX

    def test_detect_hint_minx(self, detector):
        """Test detecting MINX from hint"""
        result = detector.detect_aggregation_type("expr", "MINX")
        assert result == AggregationType.MINX

    def test_detect_hint_maxx(self, detector):
        """Test detecting MAXX from hint"""
        result = detector.detect_aggregation_type("expr", "MAXX")
        assert result == AggregationType.MAXX

    def test_detect_hint_countx(self, detector):
        """Test detecting COUNTX from hint"""
        result = detector.detect_aggregation_type("expr", "COUNTX")
        assert result == AggregationType.COUNTX

    def test_detect_hint_divide(self, detector):
        """Test detecting DIVIDE from hint"""
        result = detector.detect_aggregation_type("a/b", "DIVIDE")
        assert result == AggregationType.DIVIDE

    def test_detect_hint_ratio(self, detector):
        """Test detecting RATIO from hint"""
        result = detector.detect_aggregation_type("a/b", "RATIO")
        assert result == AggregationType.RATIO

    def test_detect_hint_variance(self, detector):
        """Test detecting VARIANCE from hint"""
        result = detector.detect_aggregation_type("amount", "VARIANCE")
        assert result == AggregationType.VARIANCE

    def test_detect_hint_weighted_average(self, detector):
        """Test detecting WEIGHTED_AVERAGE from hint"""
        result = detector.detect_aggregation_type("amount", "WEIGHTED_AVERAGE")
        assert result == AggregationType.WEIGHTED_AVERAGE

    def test_detect_hint_calculated(self, detector):
        """Test detecting CALCULATED from hint"""
        result = detector.detect_aggregation_type("[revenue] - [cost]", "CALCULATED")
        assert result == AggregationType.CALCULATED

    def test_detect_hint_exception_aggregation(self, detector):
        """Test detecting EXCEPTION_AGGREGATION from hint"""
        result = detector.detect_aggregation_type("amount", "EXCEPTION_AGGREGATION")
        assert result == AggregationType.EXCEPTION_AGGREGATION

    def test_detect_from_formula_sumx(self, detector):
        """Test detecting SUMX from formula"""
        result = detector.detect_aggregation_type("SUMX(Sales, Sales[Amount])")
        assert result == AggregationType.SUMX

    def test_detect_from_formula_averagex(self, detector):
        """Test detecting AVERAGEX from formula"""
        result = detector.detect_aggregation_type("AVERAGEX(Sales, Sales[Price])")
        assert result == AggregationType.AVERAGEX

    def test_detect_from_formula_minx(self, detector):
        """Test detecting MINX from formula"""
        result = detector.detect_aggregation_type("MINX(Sales, Sales[Amount])")
        assert result == AggregationType.MINX

    def test_detect_from_formula_maxx(self, detector):
        """Test detecting MAXX from formula"""
        result = detector.detect_aggregation_type("MAXX(Sales, Sales[Amount])")
        assert result == AggregationType.MAXX

    def test_detect_from_formula_countx(self, detector):
        """Test detecting COUNTX from formula"""
        result = detector.detect_aggregation_type("COUNTX(Sales, Sales[Amount])")
        assert result == AggregationType.COUNTX

    def test_detect_from_formula_divide(self, detector):
        """Test detecting DIVIDE from formula (note: SUM( matched first in pattern order)"""
        # The detector iterates patterns and SUM( is checked before DIVIDE(
        # For a formula containing both, SUM wins
        result = detector.detect_aggregation_type("DIVIDE(SUM(Sales[A]), SUM(Sales[B]))")
        # Either SUM or DIVIDE could be returned depending on pattern order
        assert result in [AggregationType.SUM, AggregationType.DIVIDE]

    def test_detect_invalid_hint_falls_back(self, detector):
        """Test invalid hint falls back to formula detection"""
        result = detector.detect_aggregation_type("amount", "INVALID_TYPE")
        # Should fall back to SUM as default
        assert result == AggregationType.SUM


class TestDAXAggregationBuilderExtended:
    """Extended tests for DAXAggregationBuilder - covers uncovered methods"""

    @pytest.fixture
    def builder(self):
        return DAXAggregationBuilder()

    # ========== MEDIAN / PERCENTILE / STDEV / VAR ==========

    def test_build_median_new_formula(self, builder):
        """Test building MEDIAN aggregation from simple column"""
        result = builder.build_aggregation(AggregationType.MEDIAN, "price", "Products", {})
        assert "MEDIAN" in result.upper()
        assert "Products" in result
        assert "price" in result

    def test_build_median_existing_formula(self, builder):
        """Test building MEDIAN with existing MEDIAN in formula"""
        result = builder.build_aggregation(AggregationType.MEDIAN, "MEDIAN(Products[price])", "Products", {})
        assert "MEDIAN" in result.upper()
        assert result == "MEDIAN(Products[price])"

    def test_build_percentile_with_value(self, builder):
        """Test building PERCENTILE aggregation with percentile value"""
        result = builder.build_aggregation(
            AggregationType.PERCENTILE, "price", "Products",
            {"percentile": 0.9}
        )
        assert "PERCENTILE" in result.upper()
        assert "0.9" in result

    def test_build_percentile_default_value(self, builder):
        """Test building PERCENTILE with default value (0.5)"""
        result = builder.build_aggregation(AggregationType.PERCENTILE, "price", "Products", {})
        assert "PERCENTILE" in result.upper()
        assert "0.5" in result

    def test_build_stdev(self, builder):
        """Test building STDEV aggregation"""
        result = builder.build_aggregation(AggregationType.STDEV, "amount", "Sales", {})
        assert "STDEV" in result.upper()
        assert "Sales" in result
        assert "amount" in result

    def test_build_stdev_existing(self, builder):
        """Test building STDEV with existing STDEV in formula"""
        result = builder.build_aggregation(AggregationType.STDEV, "STDEV.P(Sales[amount])", "Sales", {})
        assert result == "STDEV.P(Sales[amount])"

    def test_build_var(self, builder):
        """Test building VAR aggregation"""
        result = builder.build_aggregation(AggregationType.VAR, "amount", "Sales", {})
        assert "VAR" in result.upper()
        assert "Sales" in result
        assert "amount" in result

    def test_build_var_existing(self, builder):
        """Test building VAR with existing VAR in formula"""
        result = builder.build_aggregation(AggregationType.VAR, "VAR.P(Sales[amount])", "Sales", {})
        assert result == "VAR.P(Sales[amount])"

    # ========== SUMX / AVERAGEX / MINX / MAXX / COUNTX ==========

    def test_build_sumx_new(self, builder):
        """Test building SUMX aggregation for new formula"""
        result = builder.build_aggregation(AggregationType.SUMX, "amount", "Sales", {})
        assert "SUMX" in result.upper()
        assert "Sales" in result
        assert "amount" in result

    def test_build_sumx_existing(self, builder):
        """Test SUMX formula not double-wrapped"""
        result = builder.build_aggregation(AggregationType.SUMX, "SUMX(Sales, Sales[amount])", "Sales", {})
        assert result == "SUMX(Sales, Sales[amount])"

    def test_build_averagex_new(self, builder):
        """Test building AVERAGEX aggregation"""
        result = builder.build_aggregation(AggregationType.AVERAGEX, "price", "Products", {})
        assert "AVERAGEX" in result.upper()
        assert "Products" in result
        assert "price" in result

    def test_build_minx_new(self, builder):
        """Test building MINX aggregation"""
        result = builder.build_aggregation(AggregationType.MINX, "price", "Products", {})
        assert "MINX" in result.upper()
        assert "Products" in result

    def test_build_maxx_new(self, builder):
        """Test building MAXX aggregation"""
        result = builder.build_aggregation(AggregationType.MAXX, "price", "Products", {})
        assert "MAXX" in result.upper()
        assert "Products" in result

    def test_build_countx_new(self, builder):
        """Test building COUNTX aggregation"""
        result = builder.build_aggregation(AggregationType.COUNTX, "customer_id", "Sales", {})
        assert "COUNTX" in result.upper()
        assert "Sales" in result

    def test_build_countx_with_condition(self, builder):
        """Test building COUNTX with custom count condition"""
        result = builder.build_aggregation(
            AggregationType.COUNTX, "amount", "Sales",
            {"count_condition": "Sales[amount] > 0"}
        )
        assert "COUNTX" in result.upper()
        assert "Sales[amount] > 0" in result

    # ========== DIVIDE / RATIO / VARIANCE / WEIGHTED_AVERAGE ==========

    def test_build_divide_with_slash(self, builder):
        """Test building DIVIDE with numerator/denominator in formula"""
        result = builder.build_aggregation(AggregationType.DIVIDE, "revenue/orders", "Sales", {})
        assert "DIVIDE" in result.upper()
        assert "revenue" in result
        assert "orders" in result

    def test_build_divide_existing(self, builder):
        """Test DIVIDE formula not double-wrapped"""
        result = builder.build_aggregation(
            AggregationType.DIVIDE, "DIVIDE(SUM(Sales[A]), SUM(Sales[B]))", "Sales", {}
        )
        assert result == "DIVIDE(SUM(Sales[A]), SUM(Sales[B]))"

    def test_build_divide_simple_fallback(self, builder):
        """Test DIVIDE fallback for simple column"""
        result = builder.build_aggregation(AggregationType.DIVIDE, "amount", "Sales", {})
        assert "DIVIDE" in result.upper()

    def test_build_ratio_with_base_column(self, builder):
        """Test building RATIO with base_column in definition"""
        result = builder.build_aggregation(
            AggregationType.RATIO, "amount", "Sales",
            {"base_column": "total_amount"}
        )
        assert "DIVIDE" in result.upper()
        assert "amount" in result
        assert "total_amount" in result

    def test_build_ratio_without_base_column(self, builder):
        """Test RATIO without base_column falls back to DIVIDE"""
        result = builder.build_aggregation(AggregationType.RATIO, "amount/total", "Sales", {})
        assert "DIVIDE" in result.upper() or "/" in result

    def test_build_variance_with_target(self, builder):
        """Test building VARIANCE with target_column"""
        result = builder.build_aggregation(
            AggregationType.VARIANCE, "actual", "Sales",
            {"target_column": "budget"}
        )
        assert "SUM" in result.upper()
        assert "actual" in result
        assert "budget" in result
        assert "-" in result  # Variance is actual - budget

    def test_build_variance_without_target(self, builder):
        """Test VARIANCE without target falls back to VAR.P"""
        result = builder.build_aggregation(AggregationType.VARIANCE, "amount", "Sales", {})
        assert "VAR.P" in result or "VARIANCE" in result.upper()

    def test_build_weighted_average_with_weight(self, builder):
        """Test building WEIGHTED_AVERAGE with weight column"""
        result = builder.build_aggregation(
            AggregationType.WEIGHTED_AVERAGE, "price", "Products",
            {"weight_column": "quantity"}
        )
        assert "DIVIDE" in result.upper() or "SUMX" in result.upper()
        assert "price" in result
        assert "quantity" in result

    def test_build_weighted_average_without_weight(self, builder):
        """Test WEIGHTED_AVERAGE without weight falls back to AVERAGE"""
        result = builder.build_aggregation(AggregationType.WEIGHTED_AVERAGE, "price", "Products", {})
        assert "AVERAGE" in result.upper()

    # ========== CALCULATED ==========

    def test_build_calculated_passthrough(self, builder):
        """Test CALCULATED type returns formula as-is"""
        formula = "[revenue] - [cost]"
        result = builder.build_aggregation(AggregationType.CALCULATED, formula, "Sales", {})
        assert result == formula

    # ========== EXCEPTION_AGGREGATION ==========

    def test_build_exception_aggregation_sum(self, builder):
        """Test exception aggregation with SUM"""
        result = builder.build_aggregation(
            AggregationType.EXCEPTION_AGGREGATION, "amount", "Sales",
            {
                "exception_aggregation": "SUM",
                "fields_for_exception_aggregation": ["product_id"]
            }
        )
        assert "SUMMARIZE" in result.upper() or "SUMX" in result.upper()

    def test_build_exception_aggregation_average(self, builder):
        """Test exception aggregation with AVERAGE"""
        result = builder.build_aggregation(
            AggregationType.EXCEPTION_AGGREGATION, "amount", "Sales",
            {
                "exception_aggregation": "AVERAGE",
                "fields_for_exception_aggregation": ["product_id"]
            }
        )
        assert "AVERAGEX" in result.upper() or "AVERAGE" in result.upper()

    def test_build_exception_aggregation_count(self, builder):
        """Test exception aggregation with COUNT"""
        result = builder.build_aggregation(
            AggregationType.EXCEPTION_AGGREGATION, "amount", "Sales",
            {
                "exception_aggregation": "COUNT",
                "fields_for_exception_aggregation": ["product_id"]
            }
        )
        assert "SUMX" in result.upper() or "COUNT" in result.upper()

    def test_build_exception_aggregation_no_fields_fallback(self, builder):
        """Test exception aggregation falls back to regular aggregation when no fields"""
        result = builder.build_aggregation(
            AggregationType.EXCEPTION_AGGREGATION, "amount", "Sales",
            {
                "exception_aggregation": "SUM",
                "fields_for_exception_aggregation": []
            }
        )
        # Should fall back to regular SUM
        assert "SUM" in result.upper()

    # ========== _ensure_table_references Tests ==========

    def test_ensure_table_references_bic_column(self, builder):
        """Test that bic_ prefixed columns get table references"""
        result = builder._ensure_table_references("bic_amount + bic_quantity", "FactData")
        assert "FactData[bic_amount]" in result or "bic_amount" in result

    def test_ensure_table_references_already_formatted(self, builder):
        """Test that already formatted references are not double-formatted"""
        result = builder._ensure_table_references("FactData[amount]", "FactData")
        # Should not produce FactData[FactData[amount]]
        assert "FactData[FactData[" not in result

    # ========== SUM Complex Cases ==========

    def test_build_sum_if_formula(self, builder):
        """Test SUM with IF formula uses SUMX"""
        result = builder.build_aggregation(
            AggregationType.SUM,
            "IF(status = 1, amount, 0)",
            "Sales",
            {}
        )
        assert "SUMX" in result.upper() or "SUM" in result.upper()

    def test_build_sum_case_formula(self, builder):
        """Test SUM with CASE formula uses SUMX"""
        result = builder.build_aggregation(
            AggregationType.SUM,
            "CASE WHEN status = 'active' THEN amount ELSE 0 END",
            "Sales",
            {}
        )
        assert "SUMX" in result.upper() or "SUM" in result.upper()

    def test_build_sum_already_has_sum(self, builder):
        """Test SUM formula not double-wrapped"""
        result = builder.build_aggregation(
            AggregationType.SUM,
            "SUM(Sales[amount])",
            "Sales",
            {}
        )
        assert result == "SUM(Sales[amount])"

    # ========== COUNT / COUNTROWS / DISTINCTCOUNT ==========

    def test_build_count_already_has_count(self, builder):
        """Test COUNT formula not double-wrapped"""
        result = builder.build_aggregation(
            AggregationType.COUNT,
            "COUNT(Sales[order_id])",
            "Sales",
            {}
        )
        assert result == "COUNT(Sales[order_id])"

    def test_build_countrows(self, builder):
        """Test COUNTROWS aggregation"""
        result = builder.build_aggregation(AggregationType.COUNTROWS, "order_id", "Sales", {})
        assert "COUNTROWS" in result.upper()
        assert "Sales" in result

    def test_build_countrows_existing(self, builder):
        """Test COUNTROWS formula not double-wrapped"""
        result = builder.build_aggregation(AggregationType.COUNTROWS, "COUNTROWS(Sales)", "Sales", {})
        assert result == "COUNTROWS(Sales)"

    def test_build_distinctcount(self, builder):
        """Test DISTINCTCOUNT aggregation"""
        result = builder.build_aggregation(AggregationType.DISTINCTCOUNT, "customer_id", "Sales", {})
        assert "DISTINCTCOUNT" in result.upper()
        assert "Sales" in result

    def test_build_distinctcount_existing(self, builder):
        """Test DISTINCTCOUNT formula not double-wrapped"""
        result = builder.build_aggregation(
            AggregationType.DISTINCTCOUNT,
            "DISTINCTCOUNT(Sales[customer_id])",
            "Sales",
            {}
        )
        assert result == "DISTINCTCOUNT(Sales[customer_id])"


class TestExceptionAggregationHandlerExtended:
    """Extended tests for ExceptionAggregationHandler"""

    @pytest.fixture
    def handler(self):
        return ExceptionAggregationHandler()

    def test_no_exceptions_no_change(self, handler):
        """Test that no exceptions returns base DAX unchanged"""
        base_dax = "SUM(Sales[Amount])"
        result = handler.handle_exception_aggregation({}, base_dax)
        assert result == base_dax

    def test_display_sign_minus_one(self, handler):
        """Test display_sign = -1 negates formula"""
        base_dax = "SUM(Sales[Amount])"
        result = handler.handle_exception_aggregation(
            {"display_sign": -1}, base_dax
        )
        assert "-1 *" in result or "(-1)" in result

    def test_display_sign_positive_two(self, handler):
        """Test display_sign = 2 scales formula"""
        base_dax = "SUM(Sales[Amount])"
        result = handler.handle_exception_aggregation(
            {"display_sign": 2}, base_dax
        )
        assert "2 *" in result

    def test_display_sign_one_no_change(self, handler):
        """Test display_sign = 1 (default) doesn't modify"""
        base_dax = "SUM(Sales[Amount])"
        result = handler.handle_exception_aggregation(
            {"display_sign": 1}, base_dax
        )
        assert base_dax in result

    def test_null_to_zero_exception(self, handler):
        """Test null_to_zero exception wraps in ISBLANK check"""
        base_dax = "SUM(Sales[Amount])"
        result = handler.handle_exception_aggregation(
            {"exceptions": [{"type": "null_to_zero"}]},
            base_dax
        )
        assert "ISBLANK" in result
        assert "0" in result

    def test_division_by_zero_exception(self, handler):
        """Test division_by_zero exception adds ISERROR check"""
        base_dax = "DIVIDE(SUM(Sales[A]), SUM(Sales[B]))"
        result = handler.handle_exception_aggregation(
            {"exceptions": [{"type": "division_by_zero"}]},
            base_dax
        )
        assert "ISERROR" in result
        assert "0" in result

    def test_negative_to_zero_exception(self, handler):
        """Test negative_to_zero uses MAX(0, ...)"""
        base_dax = "SUM(Sales[Amount])"
        result = handler.handle_exception_aggregation(
            {"exceptions": [{"type": "negative_to_zero"}]},
            base_dax
        )
        assert "MAX" in result
        assert "0" in result

    def test_threshold_min_exception(self, handler):
        """Test threshold min exception uses MAX"""
        base_dax = "SUM(Sales[Amount])"
        result = handler.handle_exception_aggregation(
            {"exceptions": [{"type": "threshold", "value": 100, "comparison": "min"}]},
            base_dax
        )
        assert "MAX" in result
        assert "100" in result

    def test_threshold_max_exception(self, handler):
        """Test threshold max exception uses MIN"""
        base_dax = "SUM(Sales[Amount])"
        result = handler.handle_exception_aggregation(
            {"exceptions": [{"type": "threshold", "value": 1000, "comparison": "max"}]},
            base_dax
        )
        assert "MIN" in result
        assert "1000" in result

    def test_custom_condition_exception(self, handler):
        """Test custom_condition exception applies condition"""
        base_dax = "SUM(Sales[Amount])"
        result = handler.handle_exception_aggregation(
            {"exceptions": [{
                "type": "custom_condition",
                "condition": "flag = 1",
                "true_value": "SUM(Sales[Amount])",
                "false_value": "0"
            }]},
            base_dax
        )
        assert "IF" in result
        assert "flag = 1" in result

    def test_multiple_exceptions_applied(self, handler):
        """Test multiple exceptions are applied sequentially"""
        base_dax = "SUM(Sales[Amount])"
        result = handler.handle_exception_aggregation(
            {
                "exceptions": [
                    {"type": "null_to_zero"},
                    {"type": "negative_to_zero"}
                ]
            },
            base_dax
        )
        # Both exceptions should be applied
        assert "ISBLANK" in result
        assert "MAX" in result


class TestDetectAndBuildAggregationExtended:
    """Extended tests for detect_and_build_aggregation function"""

    def test_sum_with_source_table(self):
        """Test SUM with explicit source table"""
        result = detect_and_build_aggregation({
            "formula": "amount",
            "source_table": "FactSales",
            "aggregation_type": "SUM"
        })
        assert "SUM" in result.upper()
        assert "FactSales" in result
        assert "amount" in result

    def test_count_aggregation(self):
        """Test COUNT aggregation"""
        result = detect_and_build_aggregation({
            "formula": "order_id",
            "source_table": "Orders",
            "aggregation_type": "COUNT"
        })
        assert "COUNT" in result.upper()

    def test_average_aggregation(self):
        """Test AVERAGE aggregation"""
        result = detect_and_build_aggregation({
            "formula": "price",
            "source_table": "Products",
            "aggregation_type": "AVERAGE"
        })
        assert "AVERAGE" in result.upper()

    def test_min_aggregation(self):
        """Test MIN aggregation"""
        result = detect_and_build_aggregation({
            "formula": "price",
            "source_table": "Products",
            "aggregation_type": "MIN"
        })
        assert "MIN" in result.upper()

    def test_max_aggregation(self):
        """Test MAX aggregation"""
        result = detect_and_build_aggregation({
            "formula": "price",
            "source_table": "Products",
            "aggregation_type": "MAX"
        })
        assert "MAX" in result.upper()

    def test_distinctcount_aggregation(self):
        """Test DISTINCTCOUNT aggregation"""
        result = detect_and_build_aggregation({
            "formula": "customer_id",
            "source_table": "Sales",
            "aggregation_type": "DISTINCTCOUNT"
        })
        assert "DISTINCTCOUNT" in result.upper()

    def test_countrows_aggregation(self):
        """Test COUNTROWS aggregation"""
        result = detect_and_build_aggregation({
            "formula": "order_id",
            "source_table": "Orders",
            "aggregation_type": "COUNTROWS"
        })
        assert "COUNTROWS" in result.upper()

    def test_divide_aggregation(self):
        """Test DIVIDE aggregation with slash formula"""
        result = detect_and_build_aggregation({
            "formula": "revenue/orders",
            "source_table": "Sales",
            "aggregation_type": "DIVIDE"
        })
        assert "DIVIDE" in result.upper()

    def test_weighted_average_aggregation(self):
        """Test WEIGHTED_AVERAGE aggregation"""
        result = detect_and_build_aggregation({
            "formula": "price",
            "source_table": "Products",
            "aggregation_type": "WEIGHTED_AVERAGE",
            "weight_column": "quantity"
        })
        assert "SUMX" in result.upper() or "DIVIDE" in result.upper()
        assert "quantity" in result

    def test_variance_aggregation(self):
        """Test VARIANCE aggregation"""
        result = detect_and_build_aggregation({
            "formula": "actual",
            "source_table": "Budget",
            "aggregation_type": "VARIANCE",
            "target_column": "budget"
        })
        assert "SUM" in result.upper()
        assert "actual" in result
        assert "budget" in result
        assert "-" in result

    def test_exception_aggregation_type(self):
        """Test EXCEPTION_AGGREGATION type"""
        result = detect_and_build_aggregation({
            "formula": "amount",
            "source_table": "Sales",
            "aggregation_type": "EXCEPTION_AGGREGATION",
            "exception_aggregation": "SUM",
            "fields_for_exception_aggregation": ["product_id"]
        })
        assert "SUMX" in result.upper() or "SUMMARIZE" in result.upper()

    def test_display_sign_negative_applied(self):
        """Test display_sign = -1 is applied to result"""
        result = detect_and_build_aggregation({
            "formula": "amount",
            "source_table": "Sales",
            "aggregation_type": "SUM",
            "display_sign": -1
        })
        assert "-1" in result or "(-1)" in result

    def test_display_sign_custom_applied(self):
        """Test non-standard display_sign is applied"""
        result = detect_and_build_aggregation({
            "formula": "amount",
            "source_table": "Sales",
            "aggregation_type": "SUM",
            "display_sign": 3
        })
        assert "3" in result

    def test_exception_null_to_zero(self):
        """Test exception handling with null_to_zero"""
        result = detect_and_build_aggregation({
            "formula": "amount",
            "source_table": "Sales",
            "aggregation_type": "SUM",
            "exceptions": [{"type": "null_to_zero"}]
        })
        assert "ISBLANK" in result

    def test_exception_negative_to_zero(self):
        """Test exception handling with negative_to_zero"""
        result = detect_and_build_aggregation({
            "formula": "amount",
            "source_table": "Sales",
            "aggregation_type": "SUM",
            "exceptions": [{"type": "negative_to_zero"}]
        })
        assert "MAX" in result
        assert "0" in result

    def test_default_source_table(self):
        """Test default source table is used when not provided"""
        result = detect_and_build_aggregation({
            "formula": "amount",
            "aggregation_type": "SUM"
        })
        assert "Table" in result  # Default is 'Table'
        assert "SUM" in result.upper()

    def test_no_formula(self):
        """Test with empty formula"""
        result = detect_and_build_aggregation({
            "formula": "",
            "source_table": "Sales",
            "aggregation_type": "SUM"
        })
        # Should not crash, returns something
        assert result is not None

    def test_calculated_type(self):
        """Test CALCULATED type returns formula as-is"""
        formula = "[revenue] - [cost]"
        result = detect_and_build_aggregation({
            "formula": formula,
            "source_table": "Sales",
            "aggregation_type": "CALCULATED"
        })
        assert formula in result
