"""An LLM translation that calls a sibling measure is rejected; an inlined reference is not.

The rule: every MEASURE() the SQL calls must be a measure the DAX reads (directly, one level
down, or a base column the DAX names as Table[col]). The live run's pairs: no aligned measure is
rejected by it (measured offline, see the commit message).
"""

from src.services.tools.metric_view_utils.data_classes import TranslationResult
from src.services.tools.metric_view_utils.dax_llm_fallback import _apply_parsed
from src.services.tools.metric_view_utils.llm_reference_check import (
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
