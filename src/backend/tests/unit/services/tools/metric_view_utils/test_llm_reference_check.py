"""An LLM translation that calls a sibling measure is rejected; an inlined reference is not.

The rule: every MEASURE() the SQL calls must be a measure the DAX reads (directly, one level
down, or a base column the DAX names as Table[col]). The live run's pairs: no aligned measure is
rejected by it (measured offline, see the commit message).
"""

from src.services.tools.metric_view_utils.data_classes import TranslationResult
from src.services.tools.metric_view_utils.dax_llm_fallback import _apply_parsed
from src.services.tools.metric_view_utils.llm_reference_check import (
    column_violation,
    missing_references,
    reference_violation,
)

VIEW = {"Sales": "sales", "Other Sales": "other_sales", "Mid": "mid", "Top": "top"}


class TestSiblingMeasure:
    def test_a_sibling_measure_is_a_violation(self):
        reason = reference_violation("[Sales] * 2", "MEASURE(other_sales) * 2", VIEW)
        assert reason and "other_sales" in reason

    def test_the_referenced_measure_is_allowed(self):
        assert reference_violation("[Sales] * 2", "MEASURE(sales) * 2", VIEW) is None

    def test_a_base_column_named_in_the_dax_is_allowed(self):
        dax = "DIVIDE(CALCULATE(SUM(Fact[Weight_g])), 1)"
        assert reference_violation(dax, "MEASURE(weight_g) / 1", VIEW) is None

    def test_one_level_reference_is_allowed_with_the_referenced_dax(self):
        dax, sql = "[Top]", "MEASURE(mid) * 2"
        assert reference_violation(dax, sql, VIEW, {"Top": "[Mid] * 2"}) is None
        assert reference_violation(dax, sql, VIEW) is not None

    def test_a_reference_the_view_does_not_list_is_still_its_name(self):
        assert reference_violation("[Unlisted]", "MEASURE(unlisted)", VIEW) is None
        assert reference_violation("[Unlisted]", "MEASURE(sales)", VIEW) is not None


class TestInlinedReference:
    def test_an_inlined_reference_is_reported_not_rejected(self):
        dax, sql = "[Sales] / 2", "SUM(source.v) / 2"
        assert reference_violation(dax, sql, VIEW) is None
        assert missing_references(dax, sql, VIEW) == ["sales"]

    def test_comment_lines_are_not_references(self):
        dax = "// uses [Other Sales]\n[Sales]"
        assert missing_references(dax, "MEASURE(sales)", VIEW) == []


class TestAcceptance:
    def _measure(self, dax: str) -> TranslationResult:
        return TranslationResult(
            measure_name="m",
            original_name="M",
            sql_expr=None,
            is_translatable=False,
            skip_reason="No matching pattern",
            dax_expression=dax,
            confidence="none",
            category="unassigned",
        )

    def test_a_sibling_translation_is_not_accepted(self):
        m = self._measure("[Sales] * 2")
        _apply_parsed(
            m,
            {"success": True, "sql_expr": "MEASURE(other_sales) * 2"},
            original_to_snake=VIEW,
        )
        assert not m.is_translatable
        assert m.sql_expr is None
        assert m.skip_reason.startswith("LLM translation rejected:")

    def test_the_referenced_translation_is_accepted(self):
        m = self._measure("[Sales] * 2")
        _apply_parsed(
            m,
            {"success": True, "sql_expr": "MEASURE(sales) * 2"},
            original_to_snake=VIEW,
        )
        assert m.is_translatable
        assert m.sql_expr == "MEASURE(sales) * 2"


COLUMNS = frozenset(
    {"number_of_deliveries_final", "number_of_deliveries", "bic_chversion"}
)


class TestColumnSubstitution:
    def test_the_exact_column_of_the_view_is_read(self):
        dax = "CALCULATE(DISTINCTCOUNT(Fact[number_of_deliveries_final]))"
        sql = "COUNT(DISTINCT source.number_of_deliveries_final)"
        assert column_violation(dax, sql, COLUMNS) is None

    def test_a_sibling_column_read_instead_is_a_violation(self):
        dax = "CALCULATE(DISTINCTCOUNT(Fact[number_of_deliveries_final]))"
        sql = "COUNT(DISTINCT source.number_of_deliveries)"
        reason = column_violation(dax, sql, COLUMNS)
        assert reason and "number_of_deliveries_final" in reason

    def test_a_filter_reads_its_column_bare(self):
        dax = 'CALCULATE(SUM(Fact[number_of_deliveries]), Fact[bic_chversion] = "0000")'
        sql = "SUM(source.number_of_deliveries) FILTER (WHERE bic_chversion = '0000')"
        assert column_violation(dax, sql, COLUMNS) is None

    def test_a_column_the_view_does_not_have_is_not_checked(self):
        dax = "SUM(Fact[not_in_view])"
        assert (
            column_violation(dax, "SUM(source.number_of_deliveries)", COLUMNS) is None
        )

    def test_a_sibling_column_translation_is_not_accepted(self):
        m = TranslationResult(
            measure_name="m",
            original_name="M",
            sql_expr=None,
            is_translatable=False,
            skip_reason="No matching pattern",
            dax_expression="SUM(Fact[number_of_deliveries_final])",
            confidence="none",
            category="unassigned",
        )
        _apply_parsed(
            m,
            {"success": True, "sql_expr": "SUM(source.number_of_deliveries)"},
            view_columns=COLUMNS,
        )
        assert not m.is_translatable
        assert m.skip_reason.startswith("LLM translation rejected:")


VIEW_TWO_LEVELS = {
    "Total": "total",
    "Contrib": "contrib",
    "Mat Glass": "mat_glass",
    "Mat Metal": "mat_metal",
    "Sibling": "sibling",
}
VIEW_DAX = {"Contrib": "[Mat Glass] * 2", "Mat Metal": "[Sibling] + 1"}


class TestOneLevelRuleInTheTranslationPath:
    """The one-level rule reaches the translation path: the view's measure DAX is passed to the
    reference check, so a measure a referenced measure reads is accepted, not rejected."""

    def _measure(self, dax: str) -> TranslationResult:
        return TranslationResult(
            measure_name="m",
            original_name="M",
            sql_expr=None,
            is_translatable=False,
            skip_reason="No matching pattern",
            dax_expression=dax,
            confidence="none",
            category="unassigned",
        )

    def test_a_measure_one_level_below_a_referenced_one_is_accepted(self):
        m = self._measure("[Contrib] + [Total]")
        _apply_parsed(
            m,
            {"success": True, "sql_expr": "MEASURE(mat_glass) * 2 + MEASURE(total)"},
            original_to_snake=VIEW_TWO_LEVELS,
            measure_dax=VIEW_DAX,
        )
        assert m.is_translatable, m.skip_reason

    def test_a_measure_outside_the_referenced_closure_is_still_rejected(self):
        m = self._measure("[Contrib] + [Total]")
        _apply_parsed(
            m,
            {"success": True, "sql_expr": "MEASURE(sibling) * 2 + MEASURE(total)"},
            original_to_snake=VIEW_TWO_LEVELS,
            measure_dax=VIEW_DAX,
        )
        assert not m.is_translatable
        assert "sibling" in m.skip_reason

    def test_without_the_view_dax_a_second_level_measure_is_rejected(self):
        m = self._measure("[Contrib] + [Total]")
        _apply_parsed(
            m,
            {"success": True, "sql_expr": "MEASURE(mat_glass) * 2 + MEASURE(total)"},
            original_to_snake=VIEW_TWO_LEVELS,
        )
        assert not m.is_translatable
        assert "mat_glass" in m.skip_reason
