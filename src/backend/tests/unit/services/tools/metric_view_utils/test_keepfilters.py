"""KEEPFILTERS(<predicate>) in a CALCULATE is the plain predicate, when it reads the fact.

In a metric view measure's FILTER (WHERE ...) a predicate already intersects with the query's
filters, which is what KEEPFILTERS asks for. A KEEPFILTERS on a model dimension stays refused:
its column is not a fact column and the view has no join to it.
"""

from src.services.tools.metric_view_utils.dax_preclean import unwrap_keepfilters
from src.services.tools.metric_view_utils.dax_translator import DaxTranslator

REF = {"Ref": {"base_expr": "SUM(source.v)", "base_filters": []}}
RELATIONSHIPS = [{"from_table": "Fact", "to_table": "Cal", "is_active": True}]


def _translate(dax: str, relationships=None):
    t = DaxTranslator({"measure_resolutions": REF})
    t.set_model_relationships(relationships or [])
    return t.translate(
        {"measure_name": "m", "dax_expression": dax, "original_name": "m"},
        "Fact",
    )


class TestUnwrap:
    def test_accepted_keepfilters_is_replaced_by_its_predicate(self):
        assert unwrap_keepfilters(
            "CALCULATE([Ref], KEEPFILTERS(Fact[x] < 3))", lambda p: True
        ) == ("CALCULATE([Ref], Fact[x] < 3)")

    def test_nested_parentheses_are_kept_inside(self):
        assert unwrap_keepfilters(
            "KEEPFILTERS(Fact[x] IN {1, 2} && (Fact[y] = 3))", lambda p: True
        ) == ("Fact[x] IN {1, 2} && (Fact[y] = 3)")

    def test_refused_keepfilters_is_left_as_written(self):
        text = "CALCULATE([Ref], KEEPFILTERS(Cal[x] < 3))"
        assert unwrap_keepfilters(text, lambda p: False) == text

    def test_unbalanced_text_is_left_as_written(self):
        text = "CALCULATE([Ref], KEEPFILTERS(Fact[x] = 1"
        assert unwrap_keepfilters(text, lambda p: True) == text


class TestMeasureLevel:
    def test_keepfilters_on_a_fact_column_is_the_plain_predicate(self):
        r = _translate("CALCULATE([Ref], KEEPFILTERS(Fact[yr] < 2026))")
        assert r.is_translatable, r.skip_reason
        assert "FILTER (WHERE source.yr < 2026)" in r.sql_expr

    def test_keepfilters_on_a_model_dimension_is_refused(self):
        # The calendar column is not on the fact, and the view has no join to it here.
        r = _translate(
            'CALCULATE([Ref], KEEPFILTERS(Cal[fiscper] < "2026001"))', RELATIONSHIPS
        )
        assert not r.is_translatable
        assert "fiscper" not in (r.sql_expr or "")

    def test_keepfilters_on_a_divide_wrapper_is_applied_to_each_reference(self):
        dax = (
            "CALCULATE(DIVIDE("
            'CALCULATE([Ref], Fact[code] IN {"A"}), '
            'CALCULATE([Ref], Fact[code] IN {"B"}), BLANK()), '
            "KEEPFILTERS(Fact[yr] < 2026))"
        )
        r = _translate(dax)
        assert r.is_translatable, r.skip_reason
        assert r.sql_expr.count("source.yr < 2026") == 2, r.sql_expr
        assert "CALCULATE" not in r.sql_expr
        assert "NULLIF" in r.sql_expr

    def test_outer_filter_on_a_model_dimension_is_not_dropped_and_not_translated(self):
        # Before this, the outer filter was taken off the wrapper and lost without a trace.
        dax = (
            "CALCULATE(DIVIDE("
            'CALCULATE([Ref], Fact[code] IN {"A"}), '
            'CALCULATE([Ref], Fact[code] IN {"B"}), BLANK()), '
            'Cal[fiscper] < "2026001")'
        )
        r = _translate(dax, RELATIONSHIPS)
        assert not r.is_translatable
