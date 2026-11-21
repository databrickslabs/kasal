"""Tests for the display-format derivation (measure_format)."""

import pytest

from src.services.tools.metric_view_utils.measure_format import (
    derive_measure_format,
    expr_scales_by_100,
    format_from_pbi_format_string,
)

PCT = {"type": "percentage", "decimal_places": {"type": "exact", "places": 2}}
NUM2 = {"type": "number", "decimal_places": {"type": "max", "places": 2}}
NUM4 = {"type": "number", "decimal_places": {"type": "max", "places": 4}}


class TestPbiFormatString:
    @pytest.mark.parametrize("fs", ["0.00%", "0%", "0.0%;-0.0%;0.0%", '"0.00%"'])
    def test_percent_maps_to_fraction_standard(self, fs):
        assert format_from_pbi_format_string(fs) == PCT

    def test_plain_number(self):
        assert format_from_pbi_format_string("#,##0.00") == NUM2

    def test_euro_symbol_maps_to_known_currency(self):
        fmt = format_from_pbi_format_string("€#,##0.00")
        assert fmt["type"] == "currency"
        assert fmt["currency_code"] == "EUR"

    def test_bare_dollar_is_not_a_currency(self):
        assert format_from_pbi_format_string("$#,##0")["type"] == "number"

    @pytest.mark.parametrize("fs", [None, "", "General", "d-mmm-yy", "text"])
    def test_non_numeric_formats_return_none(self, fs):
        assert format_from_pbi_format_string(fs) is None


class TestDeriveMeasureFormat:
    def test_pbi_percent_wins_over_name(self):
        fmt = derive_measure_format("fill_rate", "SUM(a)/SUM(b)", "0.00%")
        assert fmt == PCT

    def test_percent_name_gets_fraction_standard(self):
        assert derive_measure_format("fill_rate", "SUM(a)/SUM(b)") == PCT

    def test_percent_name_with_x100_is_number_not_double_scaled(self):
        assert derive_measure_format("fill_rate", "100*SUM(a)/SUM(b)") == NUM2

    def test_pp_difference_is_number_even_with_percent_word(self):
        assert derive_measure_format("fill_rate_vs_bp_pp", "SUM(a)") == NUM2

    def test_aggregate_amount_is_number_max_2(self):
        assert derive_measure_format("total_cost", "SUM(src.cost)") == NUM2

    def test_per_unit_aggregate_uses_4_places(self):
        assert derive_measure_format("cost_per_case", "SUM(a)") == NUM4

    def test_per_unit_pbi_number_uses_4_places(self):
        assert derive_measure_format("cost_per_case", None, "#,##0.00") == NUM4

    def test_bare_division_without_name_is_not_a_percentage(self):
        assert derive_measure_format("ratio_a", "a/b") == PCT  # name carries ratio
        assert derive_measure_format("widget_mix", "a/b") is None

    def test_nothing_derivable_returns_none(self):
        assert derive_measure_format("mystery", "src.col") is None

    def test_result_is_fresh_each_call(self):
        first = derive_measure_format("fill_rate")
        first["type"] = "changed"
        assert derive_measure_format("fill_rate")["type"] == "percentage"


class TestExprScales:
    @pytest.mark.parametrize("expr", ["100*SUM(a)", "SUM(a) * 100", "SUM(a)*100.0"])
    def test_detects_x100(self, expr):
        assert expr_scales_by_100(expr)

    @pytest.mark.parametrize(
        "expr", ["SUM(a)/SUM(b)", "SUM(a)*1000", "SUM(a) + 1100", "'100 * x'"]
    )
    def test_ignores_other_factors_and_literals(self, expr):
        assert not expr_scales_by_100(expr)
