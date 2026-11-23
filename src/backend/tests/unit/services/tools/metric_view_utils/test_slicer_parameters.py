"""Selector measures become metric view parameters (config switch ``slicer_parameters``).

Names are invented: a disconnected selector ``Pick_Horizon[Horizon]`` and the measures
``Revenue MTD`` / ``Revenue YTD`` over an ``Orders`` fact.
"""

import pytest

from src.services.tools.metric_view_utils.data_classes import (
    MetricViewSpec,
    TranslationResult,
)
from src.services.tools.metric_view_utils.slicer_parameter_dax import (
    DispatchDecline,
    parse_selector_dispatch,
    reads_query_grouping,
    selector_refs,
)
from src.services.tools.metric_view_utils.slicer_parameter_yaml import (
    parameter_block_lines,
)
from src.services.tools.metric_view_utils.slicer_parameters import (
    add_slicer_parameters,
    slicer_parameters_enabled,
)
from src.services.tools.metric_view_utils.yaml_emitter import emit_yaml

RELATIONSHIPS = [{"from_table": "Orders", "to_table": "Customers"}]
ON = {"slicer_parameters": True}

DISPATCH = (
    'SWITCH(SELECTEDVALUE(Pick_Horizon[Horizon], "MTD"), '
    '"MTD", [Revenue MTD], "YTD", [Revenue YTD])'
)


def _measure(name, dax, table="Orders"):
    return {
        "measure_name": name,
        "original_name": name,
        "dax_expression": dax,
        "proposed_allocation": table,
    }


def _mapping():
    return [
        _measure("Revenue MTD", "SUM(Orders[Amount])"),
        _measure("Revenue YTD", "SUM(Orders[Amount])"),
        _measure("Revenue View", DISPATCH, table="Pick_Horizon"),
    ]


def _spec(measures=None, untranslatable=None):
    return MetricViewSpec(
        fact_table_key="Orders",
        source_table="cat.sch.orders",
        view_name="orders_view",
        comment="Orders",
        joins=[],
        dimensions=[{"name": "region", "expr": "source.region", "comment": "Region"}],
        measures=(
            measures
            if measures is not None
            else [
                TranslationResult(
                    measure_name="revenue_mtd",
                    original_name="Revenue MTD",
                    sql_expr="SUM(source.amount)",
                    is_translatable=True,
                    skip_reason="",
                    dax_expression="SUM(Orders[Amount])",
                    confidence="high",
                    category="single_table",
                )
            ]
        ),
        untranslatable=untranslatable or [],
        fact_columns=["amount", "region"],
    )


def _untranslated_view_measure():
    return TranslationResult(
        measure_name="revenue_view",
        original_name="Revenue View",
        sql_expr=None,
        is_translatable=False,
        skip_reason="SELECTEDVALUE (requires slicer context)",
        dax_expression=DISPATCH,
        confidence="none",
        category="unassigned",
    )


# ── DAX shape ────────────────────────────────────────────────────────────


def test_switch_on_disconnected_selector_reads_options_and_alternate():
    dispatch = parse_selector_dispatch(DISPATCH)
    assert dispatch.shape == "SWITCH"
    assert dispatch.selector.table == "Pick_Horizon"
    assert dispatch.selector.column == "Horizon"
    assert dispatch.selector.alternate == "MTD"
    assert [o.value for o in dispatch.options] == ["MTD", "YTD"]
    assert dispatch.options[1].result.strip() == "[Revenue YTD]"
    assert dispatch.otherwise is None


def test_switch_with_otherwise_branch():
    dax = 'SWITCH(SELECTEDVALUE(Pick_Horizon[Horizon]), "MTD", [Revenue MTD], [Revenue YTD])'
    dispatch = parse_selector_dispatch(dax)
    assert dispatch.selector.alternate is None
    assert [o.value for o in dispatch.options] == ["MTD"]
    assert dispatch.otherwise.strip() == "[Revenue YTD]"


def test_variable_bound_selector_is_expanded():
    dax = (
        "VAR s = SELECTEDVALUE(Pick_Horizon[Horizon]) "
        'RETURN SWITCH(s, "MTD", [Revenue MTD], "YTD", [Revenue YTD])'
    )
    dispatch = parse_selector_dispatch(dax)
    assert dispatch.selector.table == "Pick_Horizon"
    assert [o.value for o in dispatch.options] == ["MTD", "YTD"]


def test_if_on_selector_equality():
    dax = (
        'IF(SELECTEDVALUE(Pick_Horizon[Horizon]) = "YTD", [Revenue YTD], [Revenue MTD])'
    )
    dispatch = parse_selector_dispatch(dax)
    assert dispatch.shape == "IF"
    assert [o.value for o in dispatch.options] == ["YTD"]
    assert dispatch.otherwise.strip() == "[Revenue MTD]"


def test_switch_on_a_condition_is_declined():
    with pytest.raises(DispatchDecline, match="condition"):
        parse_selector_dispatch("SWITCH(TRUE(), [Revenue MTD] > 0, [Revenue YTD])")


def test_non_text_option_is_declined():
    dax = "SWITCH(SELECTEDVALUE(Pick_Horizon[Horizon]), 1, [Revenue MTD])"
    with pytest.raises(DispatchDecline, match="text literal"):
        parse_selector_dispatch(dax)


def test_inequality_is_not_an_equality_dispatch():
    dax = 'IF(SELECTEDVALUE(Pick_Horizon[Horizon]) <> "YTD", [Revenue YTD], [Revenue MTD])'
    with pytest.raises(DispatchDecline, match="not an equality"):
        parse_selector_dispatch(dax)


def test_grouping_functions_are_detected():
    assert reads_query_grouping("IF(ISFILTERED(Orders[Region]), 1, 0)")
    assert reads_query_grouping("HASONEVALUE(Orders[Region])")
    assert not reads_query_grouping(DISPATCH)


def test_selector_refs_finds_quoted_and_plain_tables():
    refs = selector_refs(
        "SELECTEDVALUE('Selector Act'[Pick]) + SELECTEDVALUE(Pick_Horizon[Horizon])"
    )
    assert [(r.table, r.column) for r in refs] == [
        ("Selector Act", "Pick"),
        ("Pick_Horizon", "Horizon"),
    ]


# ── switch and configuration ─────────────────────────────────────────────


def test_switch_off_by_default_changes_nothing():
    specs = {"Orders": _spec()}
    report = add_slicer_parameters(specs, _mapping(), {}, relationships=RELATIONSHIPS)
    assert report == {}
    assert specs["Orders"].parameters == []
    assert all(
        m.measure_name != "revenue_view__selector" for m in specs["Orders"].measures
    )


def test_switch_flag_accepts_string_true_only():
    assert slicer_parameters_enabled({"slicer_parameters": "true"})
    assert not slicer_parameters_enabled({"slicer_parameters": "false"})
    assert not slicer_parameters_enabled(None)


def test_no_relationships_means_no_selector_is_assumed_disconnected():
    specs = {"Orders": _spec()}
    report = add_slicer_parameters(specs, _mapping(), ON, relationships=[])
    assert report["declined"] == {"no relationships supplied": 1}
    assert specs["Orders"].parameters == []


# ── the pass ─────────────────────────────────────────────────────────────


def test_selector_measure_becomes_parameter_measure_with_measure_refs():
    specs = {"Orders": _spec(untranslatable=[_untranslated_view_measure()])}
    report = add_slicer_parameters(specs, _mapping(), ON, relationships=RELATIONSHIPS)

    spec = specs["Orders"]
    assert report["translated_count"] == 1
    assert spec.parameters == [
        {
            "name": "pick_horizon_horizon",
            "data_type": "string",
            "default": "MTD",
            "allowed_values": ["MTD", "YTD"],
            "selector": "Pick_Horizon[Horizon]",
        }
    ]
    param_measure = next(
        m for m in spec.measures if m.measure_name == "revenue_view__selector"
    )
    # Revenue MTD is a measure of this view: referenced; Revenue YTD is not: inline SQL.
    assert param_measure.sql_expr == (
        "CASE pick_horizon_horizon WHEN 'MTD' THEN MEASURE(revenue_mtd) "
        "WHEN 'YTD' THEN SUM(source.amount) END"
    )
    assert param_measure.pbi_kind == "slicer_parameter"
    # The view no longer lists the PBI measure as untranslatable.
    assert spec.untranslatable == []


def test_branch_that_is_not_translatable_names_the_branch():
    mapping = _mapping()
    mapping[2] = _measure(
        "Revenue View",
        'SWITCH(SELECTEDVALUE(Pick_Horizon[Horizon], "MTD"), "MTD", [Revenue MTD], '
        '"YTD", DIVIDE([Revenue YTD], 2))',
        table="Pick_Horizon",
    )
    specs = {"Orders": _spec(untranslatable=[_untranslated_view_measure()])}
    report = add_slicer_parameters(specs, mapping, ON, relationships=RELATIONSHIPS)

    assert report["translated_count"] == 0
    assert specs["Orders"].parameters == []
    assert [d["measure"] for d in report["declined_measures"]] == ["Revenue View"]
    assert 'option "YTD"' in report["declined_measures"][0]["reason"]
    assert [u.measure_name for u in specs["Orders"].untranslatable] == ["revenue_view"]


def test_selector_on_connected_table_is_not_a_parameter():
    mapping = _mapping()
    mapping[2] = _measure(
        "Revenue View",
        'SWITCH(SELECTEDVALUE(Customers[Segment]), "A", [Revenue MTD], [Revenue YTD])',
        table="Customers",
    )
    specs = {"Orders": _spec()}
    report = add_slicer_parameters(specs, mapping, ON, relationships=RELATIONSHIPS)
    # A selector on a connected table is not a candidate at all: no disconnected SELECTEDVALUE.
    assert report["candidates"] == 0
    assert report["translated_count"] == 0
    assert specs["Orders"].parameters == []


def test_grouping_measure_is_declined_even_with_a_selector():
    mapping = _mapping()
    mapping[2] = _measure(
        "Revenue View",
        'IF(ISFILTERED(Orders[Region]), SELECTEDVALUE(Pick_Horizon[Horizon]) = "YTD")',
        table="Pick_Horizon",
    )
    specs = {"Orders": _spec()}
    report = add_slicer_parameters(specs, mapping, ON, relationships=RELATIONSHIPS)
    assert report["translated_count"] == 0
    assert any("query's grouping" in reason for reason in report["declined"])


def test_measure_already_translated_in_view_is_not_duplicated():
    existing = TranslationResult(
        measure_name="revenue_view",
        original_name="Revenue View",
        sql_expr="SUM(source.amount)",
        is_translatable=True,
        skip_reason="",
        dax_expression=DISPATCH,
        confidence="high",
        category="single_table",
    )
    specs = {"Orders": _spec(measures=[existing])}
    report = add_slicer_parameters(specs, _mapping(), ON, relationships=RELATIONSHIPS)
    assert report["translated_count"] == 0
    assert specs["Orders"].parameters == []


# ── YAML ─────────────────────────────────────────────────────────────────


def _emitted(specs):
    return emit_yaml(specs["Orders"])


def test_emitted_yaml_declares_only_parameters_a_measure_reads():
    specs = {"Orders": _spec(untranslatable=[_untranslated_view_measure()])}
    add_slicer_parameters(specs, _mapping(), ON, relationships=RELATIONSHIPS)
    yaml_text = _emitted(specs)

    assert "\nparameters:\n" in yaml_text
    assert "  - name: pick_horizon_horizon\n" in yaml_text
    assert "    data_type: string\n" in yaml_text
    assert "    default: 'MTD'\n" in yaml_text
    assert "CASE pick_horizon_horizon WHEN 'MTD' THEN MEASURE(revenue_mtd)" in yaml_text
    # Parameters are declared before the measures that read them, and no view-level
    # materialization is written for a view that has parameters.
    assert yaml_text.index("parameters:") < yaml_text.index("measures:")
    assert "materialization" not in yaml_text


def test_yaml_without_parameters_is_unchanged():
    specs = {"Orders": _spec()}
    assert "parameters:" not in _emitted(specs)


def test_parameter_not_read_by_a_written_measure_is_not_declared():
    params = [
        {
            "name": "unused_param",
            "data_type": "string",
            "default": "A",
            "allowed_values": ["A"],
            "selector": "T[c]",
        }
    ]
    assert parameter_block_lines(params, ["SUM(source.amount)"]) == []


def test_parameter_default_with_quote_is_escaped():
    params = [
        {
            "name": "p",
            "data_type": "string",
            "default": "It's",
            "allowed_values": ["It's", "No"],
            "selector": "T[c]",
        }
    ]
    lines = parameter_block_lines(params, ["CASE p WHEN 'It''s' THEN 1 END"])
    assert "    default: 'It''s'" in lines


def test_option_value_that_cannot_be_a_literal_is_declined():
    mapping = _mapping()
    mapping[2] = _measure(
        "Revenue View",
        'SWITCH(SELECTEDVALUE(Pick_Horizon[Horizon]), "M;TD", [Revenue MTD])',
        table="Pick_Horizon",
    )
    specs = {"Orders": _spec()}
    report = add_slicer_parameters(specs, mapping, ON, relationships=RELATIONSHIPS)
    assert report["translated_count"] == 0
    assert any(
        "cannot be written as a literal" in reason for reason in report["declined"]
    )
