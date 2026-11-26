"""A decomposed SWITCH option is compared with its selector, never with the parent alone.

``SWITCH(TRUE(), x = "Alpha", [A], x = "Beta", [B])`` with ``x = SELECTEDVALUE(Sel[Choice])``
gives one metric-view measure per option. Each option's mapping is ``switch`` against the
parent with that option's selector literal. An option whose condition is not exactly one
selector comparison is ``unresolved`` (not compared), never the parent without a selector.
"""

from src.services.powerbi.switch_decomposition import (
    derive_switch_decompositions,
)
from src.services.tools.metric_view_utils.data_classes import TranslationResult
from src.services.tools.metric_view_utils.pbi_ucmv_mapping import _measure_pbi_kind

PARENT_VAR = (
    "VAR x = SELECTEDVALUE(Sel[Choice]) RETURN "
    'SWITCH(TRUE(), x = "Alpha", [Ma], x = "Beta", [Mb])'
)
MEASURES = [
    {"measure_name": "Pick", "expression": PARENT_VAR, "table_name": "Sel"},
    {
        "measure_name": "Ma",
        "expression": 'CALCULATE(SUM(Fact[v]), Fact[k] = "a")',
        "table_name": "Fact",
    },
    {
        "measure_name": "Mb",
        "expression": 'CALCULATE(SUM(Fact[v]), Fact[k] = "b")',
        "table_name": "Fact",
    },
]


def _entries(measures):
    return derive_switch_decompositions(measures, {"Fact"})["Fact"]


def _by_name(entries):
    return {e["name"]: e for e in entries}


class TestOptionSelectors:
    def test_each_option_is_switch_against_the_parent_with_its_literal(self):
        entries = _by_name(_entries(MEASURES))
        assert set(entries) == {"pick__ma", "pick__mb"}
        alpha = entries["pick__ma"]
        assert alpha["pbi_kind"] == "switch"
        assert alpha["pbi_sources"] == [
            {
                "pbi_measure": "Pick",
                "switch_selector_table": "Sel",
                "switch_selector_column": "Choice",
                "switch_selector_value": "Alpha",
            }
        ]
        beta = entries["pick__mb"]
        assert beta["pbi_sources"][0]["switch_selector_value"] == "Beta"

    def test_direct_selectedvalue_comparison_is_read_too(self):
        dax = (
            'SWITCH(TRUE(), SELECTEDVALUE(Sel[Choice]) = "Alpha", [Ma], '
            'SELECTEDVALUE(Sel[Choice]) = "Beta", [Mb])'
        )
        entries = _by_name(
            _entries(
                [{"measure_name": "Pick", "expression": dax, "table_name": "Sel"}]
                + MEASURES[1:]
            )
        )
        assert entries["pick__ma"]["pbi_kind"] == "switch"
        assert entries["pick__ma"]["pbi_sources"][0]["switch_selector_value"] == "Alpha"

    def test_variable_name_is_matched_case_insensitively(self):
        dax = (
            "VAR x = SELECTEDVALUE(Sel[Choice]) RETURN "
            'SWITCH(TRUE(), X = "Alpha", [Ma], x = "Beta", [Mb])'
        )
        entries = _by_name(
            _entries(
                [{"measure_name": "Pick", "expression": dax, "table_name": "Sel"}]
                + MEASURES[1:]
            )
        )
        assert entries["pick__ma"]["pbi_sources"][0]["switch_selector_value"] == "Alpha"

    def test_compound_condition_is_unresolved_not_compared(self):
        dax = (
            "VAR x = SELECTEDVALUE(Sel[Choice]) VAR y = SELECTEDVALUE(Sel[Other]) RETURN "
            'SWITCH(TRUE(), x = "Alpha" && y = "P", [Ma], x = "Beta", [Mb])'
        )
        entries = _by_name(
            _entries(
                [{"measure_name": "Pick", "expression": dax, "table_name": "Sel"}]
                + MEASURES[1:]
            )
        )
        assert entries["pick__ma"]["pbi_kind"] == "unresolved"
        assert entries["pick__ma"]["pbi_sources"] == []
        # the option that has one selector literal is still compared
        assert entries["pick__mb"]["pbi_kind"] == "switch"

    def test_numeric_condition_is_unresolved(self):
        dax = (
            "VAR x = SELECTEDVALUE(Sel[Choice]) RETURN "
            "SWITCH(TRUE(), x = 1, [Ma], x = 2, [Mb])"
        )
        entries = _by_name(
            _entries(
                [{"measure_name": "Pick", "expression": dax, "table_name": "Sel"}]
                + MEASURES[1:]
            )
        )
        assert {e["pbi_kind"] for e in entries.values()} == {"unresolved"}


class TestMappingDraft:
    def _result(self, **kwargs):
        base = dict(
            measure_name="pick__ma",
            original_name="Pick",
            sql_expr="SUM(source.v)",
            is_translatable=True,
            skip_reason="",
            dax_expression="SWITCH decomposition",
            category="switch_decomposition",
            confidence="high",
        )
        base.update(kwargs)
        return TranslationResult(**base)

    def test_switch_option_maps_to_the_parent_with_its_selector(self):
        entry = _entries(MEASURES)[0]
        result = self._result(
            pbi_kind=entry["pbi_kind"], pbi_sources=entry["pbi_sources"]
        )
        mapped = _measure_pbi_kind(result)
        assert mapped["kind"] == "switch"
        assert mapped["pbi_measure"] == "Pick"
        assert mapped["switch_selector_table"] == "Sel"
        assert mapped["switch_selector_column"] == "Choice"
        assert mapped["switch_selector_value"] == "Alpha"

    def test_unresolved_option_is_not_compared_with_the_parent(self):
        result = self._result(pbi_kind="unresolved", pbi_sources=[])
        mapped = _measure_pbi_kind(result)
        assert mapped["kind"] == "unresolved"
        assert "pbi_measure" not in mapped
        assert "no single selector literal" in mapped["raw_hint"]
