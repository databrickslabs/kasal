"""EXTERNALMEASURE (a measure of another model) is never translated, in part or in whole.

A SWITCH whose one branch is EXTERNALMEASURE(...) used to lose that branch silently and ship
the other one. A measure whose DAX reaches EXTERNALMEASURE, directly or through a referenced
measure, is reported as not translatable with that reason, and the LLM is not asked for it.
"""

from src.services.powerbi.switch_decomposition import (
    _resolve_referenced_measure_dax,
    derive_switch_decompositions,
    reaches_external_measure,
)
from src.services.tools.metric_view_utils.dax_translator import DaxTranslator

EXT = "EXTERNALMEASURE(\"Sales Amount\", 'Remote')"


class TestDirect:
    def test_direct_external_measure_is_reported_with_its_reason(self):
        r = DaxTranslator().translate(
            {"measure_name": "m", "dax_expression": f"{EXT} + 1", "original_name": "M"},
            "Fact",
        )
        assert not r.is_translatable
        assert r.category == "external_measure"
        assert r.skip_reason.startswith("EXTERNALMEASURE")
        assert r.sql_expr is None


class TestReach:
    MEASURES = {
        "Ext": {"expression": EXT},
        "Mid": {"expression": "CALCULATE([Ext], Fact[x] = 1)"},
        "Top": {"expression": "[Mid] * 2"},
        "Plain": {"expression": "SUM(Fact[v])"},
    }

    def test_direct_and_transitive_reach(self):
        assert reaches_external_measure(EXT, self.MEASURES)
        assert reaches_external_measure("[Mid] * 2", self.MEASURES)
        assert reaches_external_measure("[Top]", self.MEASURES)

    def test_a_measure_without_external_reach_is_not_claimed(self):
        assert not reaches_external_measure("[Plain] + 1", self.MEASURES)
        assert not reaches_external_measure("SUM(Fact[v])", self.MEASURES)

    def test_resolver_never_gives_sql_for_an_external_measure(self):
        assert _resolve_referenced_measure_dax(f"CALCULATE({EXT})") is None


class TestSwitchParent:
    def test_switch_with_an_external_option_is_not_decomposed_in_part(self):
        measures = [
            {
                "measure_name": "Pick",
                "expression": (
                    "VAR x = SELECTEDVALUE(Sel[Choice]) RETURN "
                    'SWITCH(TRUE(), x = "Alpha", [Ext], x = "Beta", [Local])'
                ),
                "table_name": "Sel",
            },
            {"measure_name": "Ext", "expression": EXT, "table_name": "Fact"},
            {
                "measure_name": "Local",
                "expression": 'CALCULATE(SUM(Fact[v]), Fact[k] = "b")',
                "table_name": "Fact",
            },
        ]
        decompositions = derive_switch_decompositions(measures, {"Fact"})
        assert decompositions.get("Fact", []) == []
        assert decompositions.get("Sel", []) == []
