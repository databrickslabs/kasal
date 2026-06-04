"""Tests for conditional formatting with business logic detection."""
import pytest
from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator


@pytest.fixture
def translator():
    return DaxTranslator()


class TestConditionalFormattingDetection:
    """Validate that Color/FORMAT measures with business logic are not rejected."""

    def test_pure_color_rejected(self, translator):
        """Pure color measure with no aggregation should be rejected."""
        result = translator.translate(
            {'measure_name': 'Status_Color', 'original_name': 'Status_Color',
             'dax_expression': 'IF(TRUE(), "green", "red")'},
            'fact')
        assert result.is_translatable is False
        assert 'Color' in result.skip_reason
        assert 'display-only' in result.skip_reason

    def test_color_with_measure_ref_not_rejected(self, translator):
        """Color measure referencing another measure should NOT be rejected."""
        result = translator.translate(
            {'measure_name': 'Margin_Color', 'original_name': 'Margin_Color',
             'dax_expression': 'IF([Margin] < 0.1, "red", "green")'},
            'fact')
        # Should NOT be rejected by quick-reject (has measure ref [Margin])
        # It may still fail to translate for other reasons, but not display-only
        assert 'display-only' not in (result.skip_reason or '')

    def test_color_with_aggregation_not_rejected(self, translator):
        """Color measure with SUM/DIVIDE should NOT be rejected."""
        result = translator.translate(
            {'measure_name': 'KPI_Color', 'original_name': 'KPI_Color',
             'dax_expression': 'IF(DIVIDE(SUM(Sales[Actual]), SUM(Sales[Target])) < 0.9, "red", "green")'},
            'fact')
        assert 'display-only' not in (result.skip_reason or '')

    def test_color_with_sum_not_rejected(self, translator):
        """Color measure with SUM should NOT be rejected by quick-reject."""
        result = translator.translate(
            {'measure_name': 'Revenue_Color', 'original_name': 'Revenue_Color',
             'dax_expression': 'IF(SUM(Sales[Revenue]) > 1000000, "green", "red")'},
            'fact')
        assert 'display-only' not in (result.skip_reason or '')

    def test_color_suffix_with_measure_ref(self, translator):
        """Measure ending with _Color and containing [MeasureRef] not rejected."""
        result = translator.translate(
            {'measure_name': 'Traffic_Light_Color', 'original_name': 'Traffic_Light_Color',
             'dax_expression': 'IF([Margin] < 0.1, "At Risk", IF([Margin] < 0.2, "Warning", "Healthy"))'},
            'fact')
        assert 'display-only' not in (result.skip_reason or '')

    def test_pure_format_rejected(self, translator):
        """Pure FORMAT wrapper with no aggregation should be rejected."""
        result = translator.translate(
            {'measure_name': 'Formatted', 'original_name': 'Formatted',
             'dax_expression': 'FORMAT(12345, "#,##0")'},
            'fact')
        assert result.is_translatable is False
        assert 'FORMAT' in result.skip_reason

    def test_format_wrapping_sum_not_rejected(self, translator):
        """FORMAT wrapping a SUM should not be fully rejected — contains business logic."""
        result = translator.translate(
            {'measure_name': 'Revenue_Fmt', 'original_name': 'Revenue Fmt',
             'dax_expression': 'FORMAT(SUM(Sales[Revenue]), "$#,##0")'},
            'fact')
        # The FORMAT wrapper is display-only, but the inner SUM is business logic.
        # Should not be rejected as "display-only".
        if not result.is_translatable:
            assert 'display-only' not in (result.skip_reason or '')

    def test_format_with_divide_not_rejected(self, translator):
        """FORMAT wrapping a DIVIDE should not be rejected as display-only."""
        result = translator.translate(
            {'measure_name': 'Pct_Fmt', 'original_name': 'Pct Fmt',
             'dax_expression': 'FORMAT(DIVIDE(SUM(Sales[Actual]), SUM(Sales[Target])), "0.0%")'},
            'fact')
        if not result.is_translatable:
            assert 'display-only' not in (result.skip_reason or '')

    def test_isblank_blank_with_no_agg_rejected(self, translator):
        """ISBLANK+BLANK without aggregation should still be rejected."""
        result = translator.translate(
            {'measure_name': 'Guard', 'original_name': 'Guard',
             'dax_expression': 'IF(ISBLANK(x), BLANK(), x)'},
            'fact')
        assert result.is_translatable is False

    def test_non_color_name_not_affected(self, translator):
        """Measures without COLOR in name should not be affected by this logic."""
        result = translator.translate(
            {'measure_name': 'Total_Sales', 'original_name': 'Total Sales',
             'dax_expression': 'SUM(Sales[Amount])'},
            'fact')
        assert result.is_translatable is True
        assert result.sql_expr == 'SUM(source.Amount)'

    def test_color_name_case_insensitive(self, translator):
        """COLOR detection should be case-insensitive."""
        result = translator.translate(
            {'measure_name': 'status_COLOR', 'original_name': 'status_COLOR',
             'dax_expression': 'IF(TRUE(), "green", "red")'},
            'fact')
        assert result.is_translatable is False
        assert 'display-only' in result.skip_reason

    def test_format_only_wrapping_literal_rejected(self, translator):
        """FORMAT wrapping a literal string (no aggregation) should be rejected."""
        result = translator.translate(
            {'measure_name': 'Label', 'original_name': 'Label',
             'dax_expression': 'FORMAT(TODAY(), "YYYY-MM-DD")'},
            'fact')
        assert result.is_translatable is False
        assert 'FORMAT' in result.skip_reason
