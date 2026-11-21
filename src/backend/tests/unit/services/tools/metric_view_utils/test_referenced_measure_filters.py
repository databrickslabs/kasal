"""A CALCULATE over a referenced measure keeps that measure's own base filters.

A referenced measure resolved to ``SUM(source.v) FILTER (WHERE version = ... AND type = ...)``
(a switch branch, or any measure whose DAX filters). The dropped-filter paths summed every
version and every type; each path must carry ``base_filters`` into the emitted SQL.
"""

import pytest

from src.services.tools.metric_view_utils.dax_translator import DaxTranslator

BASE = {
    "base_expr": "SUM(source.v)",
    "base_filters": ["ver = '0000'", "kind = 'Company Code'"],
}


def _translate(dax: str):
    t = DaxTranslator({"measure_resolutions": {"Ref": BASE}})
    return t.translate(
        {"measure_name": "m", "dax_expression": dax, "original_name": "m"},
        "Fact",
    )


def _assert_filters_kept(sql: str) -> None:
    assert "ver = '0000'" in sql
    assert "kind = 'Company Code'" in sql


class TestReferencedMeasureFilters:
    def test_calculate_with_a_literal_filter_keeps_base_filters(self):
        r = _translate('CALCULATE([Ref], Fact[code] = "X", Fact[y] <> "z")')
        assert r.is_translatable, r.skip_reason
        _assert_filters_kept(r.sql_expr)
        assert "code = 'X'" in r.sql_expr

    def test_code_ratio_over_a_referenced_measure_keeps_base_filters(self):
        dax = (
            'var a = CALCULATE([Ref], Fact[code] IN {"A", "B"})\n'
            'var b = CALCULATE([Ref], Fact[code] IN {"C"})\n'
            "return DIVIDE(a, b)"
        )
        r = _translate(dax)
        assert r.is_translatable, r.skip_reason
        assert r.sql_expr.count("ver = '0000'") == 2
        assert r.sql_expr.count("kind = 'Company Code'") == 2

    def test_filtered_referenced_measure_is_not_a_distinct_count(self):
        # The NPS shape used to fall back to COUNT(DISTINCT id), which drops the filters.
        from src.services.tools.metric_view_utils import dax_patterns

        t = DaxTranslator({"measure_resolutions": {"Ref": BASE}})
        assert dax_patterns._resolve_nps_var("[Ref]", "score", "id", t) is None

    @pytest.mark.parametrize(
        "dax",
        [
            'CALCULATE([Ref], Fact[code] = "X")',
            'CALCULATE([Ref], Fact[code] = "X", Fact[y] <> "z")',
        ],
    )
    def test_every_calculate_shape_keeps_base_filters(self, dax):
        r = _translate(dax)
        assert r.is_translatable, r.skip_reason
        _assert_filters_kept(r.sql_expr)
