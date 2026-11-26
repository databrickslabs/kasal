"""SUMX over a row sum adds the non-blank terms, as DAX does.

``SUM(source.a + source.b)`` dropped every row where one term was NULL. Each term is COALESCEd
to 0, and a set where every row is all-blank is NULL (DAX blank), not 0.
"""

from src.services.tools.metric_view_utils.dax_sumx_terms import match_sumx_terms
from src.services.tools.metric_view_utils.dax_translator import DaxTranslator


def _translate(dax: str):
    return DaxTranslator().translate(
        {"measure_name": "m", "dax_expression": dax, "original_name": "m"},
        "Fact",
    )


class TestSumxTerms:
    def test_three_terms_are_coalesced_and_all_blank_is_null(self):
        r = _translate("SUMX(Fact, Fact[a] + Fact[b] + Fact[c])")
        assert r.is_translatable, r.skip_reason
        assert r.sql_expr == (
            "CASE WHEN COUNT(COALESCE(source.a, source.b, source.c)) = 0 THEN NULL "
            "ELSE SUM(COALESCE(source.a, 0) + COALESCE(source.b, 0) + COALESCE(source.c, 0)) END"
        )

    def test_subtracted_term_keeps_its_sign(self):
        assert match_sumx_terms("SUMX(Fact, Fact[a] - Fact[b])") == {
            "terms": [("+", "a"), ("-", "b")]
        }
        r = _translate("SUMX(Fact, Fact[a] - Fact[b])")
        assert "SUM(COALESCE(source.a, 0) - COALESCE(source.b, 0))" in r.sql_expr

    def test_leading_minus_is_kept(self):
        r = _translate("SUMX(Fact, -Fact[a] + Fact[b])")
        assert "SUM(-COALESCE(source.a, 0) + COALESCE(source.b, 0))" in r.sql_expr

    def test_calculate_wrapper_must_be_closed(self):
        assert match_sumx_terms("CALCULATE(SUMX(Fact, Fact[a] + Fact[b]))") is not None
        assert match_sumx_terms("CALCULATE(SUMX(Fact, Fact[a] + Fact[b])") is None

    def test_single_term_and_non_column_rows_are_not_claimed(self):
        assert match_sumx_terms("SUMX(Fact, Fact[a])") is None
        assert match_sumx_terms("SUMX(Fact, Fact[a] * Fact[b])") is None
        assert match_sumx_terms("SUMX(Fact, Fact[a] + Fact[b] * 2)") is None

    def test_inside_a_divide_numerator(self):
        r = _translate("DIVIDE(CALCULATE(SUMX(Fact, Fact[a] + Fact[b])), SUM(Fact[c]))")
        assert r.is_translatable, r.skip_reason
        assert "COALESCE(source.a, 0) + COALESCE(source.b, 0)" in r.sql_expr
        assert "NULLIF(SUM(source.c), 0)" in r.sql_expr
