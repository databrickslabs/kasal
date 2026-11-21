"""A measure the previous verify-numbers cycle flagged goes to the DAX LLM with its hint. On the
rule path (llm_first's trivial fast path) it got the same SQL again, so a refine cycle could not
improve it: in a field run cycle 2 equalled cycle 1 at 81.22 %."""

from types import SimpleNamespace

from src.services.tools.metric_view_utils.data_classes import TranslationResult
from src.services.tools.metric_view_utils.table_processor import _route_dax_measures


class _RuleTranslator:
    def translate(self, dm, table_key, trivial_only=False):
        return TranslationResult(
            measure_name=dm["name"], original_name=dm["original"], sql_expr="SUM(source.x)",
            is_translatable=True, skip_reason="", dax_expression="SUM(T[x])", confidence="high",
            category="dax",
        )


def _route(feedback, llm=True):
    ctx = SimpleNamespace(
        translator=_RuleTranslator(), cross_table_measures=[],
        llm_config={"use_llm_fallback": llm, "refinement_feedback_by_measure": feedback},
    )
    translated, untranslatable = [], []
    measures = [{"name": "volume", "original": "Volume"}, {"name": "share", "original": "Share %"}]
    _route_dax_measures(measures, "Fact", ctx, True, llm, lambda s: s, set(), translated, untranslatable)
    return [m.measure_name for m in translated], [m.measure_name for m in untranslatable]


def test_a_flagged_measure_goes_to_the_llm():
    assert _route({"share": "only 14.5% of cells matched Power BI"}) == (["volume"], ["share"])
    assert _route({"Share %": "hint by Power BI name"}) == (["volume"], ["share"])


def test_without_feedback_or_llm_nothing_changes():
    assert _route({}) == (["volume", "share"], [])
    assert _route({"share": "hint"}, llm=False) == (["volume", "share"], [])
