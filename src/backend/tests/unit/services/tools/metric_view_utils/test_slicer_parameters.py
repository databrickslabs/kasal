"""Selector measures become metric view parameters (config switch ``slicer_parameters``).

Names are invented: a disconnected selector ``Pick_Horizon[Horizon]`` and ``Pick_Region[Region]``,
a fact ``Orders`` with ``Amount`` and ``Region``, and the measures ``Revenue MTD`` / ``Revenue YTD``.
"""

import pytest

import yaml

from src.services.tools.metric_view_utils.data_classes import (
    MetricViewSpec,
    TranslationResult,
)
from src.services.tools.metric_view_utils.dax_translator import DaxTranslator
from src.services.tools.metric_view_utils.slicer_branch import branch_sql
from src.services.tools.metric_view_utils.slicer_parameter_dax import (
    DispatchDecline,
    expanded_body,
    parse_case,
    reads_query_grouping,
    rewrite_selectors,
    selector_refs,
)
from src.services.tools.metric_view_utils.slicer_parameter_text import (
    substitute,
    token_for,
    with_parameter_lines,
)
from src.services.tools.metric_view_utils.slicer_parameter_yaml import (
    parameter_block_lines,
)
from src.services.tools.metric_view_utils.slicer_parameters import (
    SlicerReport,
    connected_tables,
    finish_view,
    plan_view,
    rehome_candidates,
    slicer_parameters_enabled,
)
from src.services.tools.metric_view_utils.source_column_guard import guard_view
from src.services.tools.metric_view_utils.yaml_emitter import emit_yaml

RELATIONSHIPS = [{"from_table": "Orders", "to_table": "Customers"}]
ON = {"slicer_parameters": True}


def _row(name, dax, table="Orders"):
    return {
        "measure_name": name,
        "original_name": name,
        "dax_expression": dax,
        "proposed_allocation": table,
    }


def _mapping(extra=()):
    rows = [
        _row("Revenue MTD", "SUM(Orders[Amount])"),
        _row("Revenue YTD", "SUM(Orders[Amount])"),
        _row(
            "Revenue View",
            'SWITCH(SELECTEDVALUE(Pick_Horizon[Horizon], "MTD"), '
            '"MTD", [Revenue MTD], "YTD", [Revenue YTD])',
            table="Pick_Horizon",
        ),
    ]
    rows.extend(extra)
    return rows


def _run(rows, relationships=RELATIONSHIPS):
    """Plan, translate the rows as process_table would, then finish. Returns the spec too."""
    translator = DaxTranslator({})
    translator.set_model_relationships(relationships)
    translator.set_fact_joins([])
    plan = plan_view(
        "Orders",
        rows,
        rows,
        connected=connected_tables(relationships),
        taken_names=set(),
        llm_enabled=False,
        trivial_only=False,
    )
    spec = MetricViewSpec(
        fact_table_key="Orders",
        source_table="cat.sch.orders",
        view_name="orders_view",
        comment="Orders",
        joins=[],
        dimensions=[],
        measures=[],
        untranslatable=[],
        # The translator writes the columns as the model names them (source.Amount).
        fact_columns=["Amount", "Region"],
    )
    for row in plan.process_rows:
        result = translator.translate(row, "Orders")
        (spec.measures if result.is_translatable else spec.untranslatable).append(
            result
        )
    report = SlicerReport()
    finish_view(plan, spec, translator, report)
    return plan, spec, report


def _named(spec, name):
    return next(m for m in spec.measures if m.measure_name == name)


# ── DAX ──────────────────────────────────────────────────────────────────


def test_rewrite_replaces_each_selector_by_its_token_and_registers_the_alternate():
    seen = []

    def register(ref):
        seen.append((ref.table, ref.column, ref.alternate))
        return "pick_horizon_horizon"

    text = rewrite_selectors(
        'SWITCH(SELECTEDVALUE(Pick_Horizon[Horizon], "MTD"), "MTD", [Revenue MTD])',
        register,
    )
    assert text == 'SWITCH("__sel_pick_horizon_horizon", "MTD", [Revenue MTD])'
    assert seen == [("Pick_Horizon", "Horizon", "MTD")]


def test_switch_on_the_selector_value_is_a_case_shape():
    shape = parse_case(
        'SWITCH("__sel_p", "MTD", [Revenue MTD], "YTD", [Revenue YTD], [Other])'
    )
    assert shape.param == "p"
    assert [v for v, _ in shape.options] == ["MTD", "YTD"]
    assert shape.otherwise.strip() == "[Other]"


def test_condition_form_switch_true_is_a_case_shape():
    dax = (
        'SWITCH(TRUE(), "__sel_p" = "Actual", [Revenue MTD], '
        '"__sel_p" = "Unit Case", DIVIDE([Revenue MTD], 2))'
    )
    shape = parse_case(dax)
    assert shape.param == "p"
    assert [v for v, _ in shape.options] == ["Actual", "Unit Case"]


def test_if_with_the_selector_on_the_right_is_a_case_shape():
    shape = parse_case('IF("YTD" = "__sel_p", [Revenue YTD], [Revenue MTD])')
    assert shape.shape == "IF"
    assert shape.param == "p"
    assert shape.options[0][0] == "YTD"


def test_condition_on_two_selectors_is_refused():
    dax = 'SWITCH(TRUE(), "__sel_p" = "A", [X], "__sel_q" = "B", [Y])'
    with pytest.raises(DispatchDecline, match="more than one selector"):
        parse_case(dax)


def test_a_condition_that_is_not_an_equality_is_refused():
    with pytest.raises(DispatchDecline, match="not an equality"):
        parse_case('IF("__sel_p" <> "A", [X], [Y])')


def test_a_var_block_is_read_with_case_insensitive_names():
    dax = "var a = [Revenue MTD]\nvar b = [Revenue YTD]\nRETURN DIVIDE(A, B, BLANK())"
    assert expanded_body(dax).replace(" ", "").startswith("DIVIDE((")


def test_grouping_functions_are_detected_and_selectors_listed():
    assert reads_query_grouping("IF(ISFILTERED(Orders[Region]), 1, 0)")
    refs = selector_refs(
        "SELECTEDVALUE('Selector Act'[Pick]) + SELECTEDVALUE(Pick_Horizon[Horizon])"
    )
    assert [(r.table, r.column) for r in refs] == [
        ("Selector Act", "Pick"),
        ("Pick_Horizon", "Horizon"),
    ]


# ── text and prompt ──────────────────────────────────────────────────────


def test_substitute_turns_quoted_and_bare_tokens_into_the_parameter_name():
    sql = (
        "CASE WHEN source.Region = '__sel_pick_region' THEN 1 END OR __sel_pick_region"
    )
    assert substitute(sql, {"pick_region"}) == (
        "CASE WHEN source.Region = pick_region THEN 1 END OR pick_region"
    )


def test_substitute_leaves_a_token_of_an_unknown_name_visible():
    assert substitute("x = '__sel_other'", {"pick_region"}) == "x = '__sel_other'"


def test_llm_context_names_each_parameter_and_says_to_write_it_bare():
    params = [
        {
            "name": "pick_horizon_horizon",
            "allowed_values": ["MTD", "YTD"],
            "default": "MTD",
        }
    ]
    text = with_parameter_lines("- source table: x", params)
    assert "pick_horizon_horizon: values 'MTD', 'YTD'; default 'MTD'" in text
    assert "Write the bare name" in text
    assert "NOT columns" in text
    assert with_parameter_lines("- source table: x", []) == "- source table: x"


def test_token_literal_is_the_parameter_as_a_dax_string():
    assert token_for("pick_horizon_horizon") == '"__sel_pick_horizon_horizon"'


# ── branch composition ───────────────────────────────────────────────────


def _branch(text):
    refs = {"A": "SUM(source.a)", "B": "SUM(source.b)"}
    return branch_sql(
        text,
        lambda name: refs[name],
        lambda t: f"TRANSLATOR({t})",
    )


def test_branch_difference_of_measures_is_composed():
    assert _branch("[A] - [B]") == "(SUM(source.a)) - (SUM(source.b))"


def test_divide_with_blank_alternate_is_composed_with_nullif():
    assert _branch("DIVIDE([A], [B], BLANK())") == (
        "(SUM(source.a)) / NULLIF((SUM(source.b)), 0)"
    )


def test_divide_with_another_alternate_is_refused():
    with pytest.raises(DispatchDecline, match="alternate other than BLANK"):
        _branch("DIVIDE([A], [B], 0)")


def test_bare_division_is_not_composed_and_goes_to_the_translator():
    assert _branch("[A] / [B]") == "TRANSLATOR([A] / [B])"


def test_numeric_constant_branch_is_its_own_sql():
    assert _branch("2") == "2"


# ── the plan and the finish, with the real translator ────────────────────


def test_condition_form_becomes_a_case_over_the_parameter():
    _, spec, report = _run(_mapping())
    measure = _named(spec, "revenue_view")
    assert measure.sql_expr == (
        "CASE pick_horizon_horizon WHEN 'MTD' THEN SUM(source.Amount) "
        "WHEN 'YTD' THEN SUM(source.Amount) END"
    )
    assert measure.pbi_kind == "slicer_parameter"
    assert spec.parameters == [
        {
            "name": "pick_horizon_horizon",
            "data_type": "string",
            "default": "MTD",
            "allowed_values": ["MTD", "YTD"],
            "selector": "Pick_Horizon[Horizon]",
        }
    ]
    assert report.best["Revenue View"][0] == "rule"
    assert [u.measure_name for u in spec.untranslatable] == []


def test_selector_used_as_a_filter_value_goes_through_the_translator_with_the_bare_name():
    rows = _mapping(
        [
            _row(
                "Revenue Region",
                "CALCULATE(SUM(Orders[Amount]), Orders[Region] = "
                'SELECTEDVALUE(Pick_Region[Region], "East"))',
            )
        ]
    )
    _, spec, report = _run(rows)
    measure = _named(spec, "revenue_region")
    assert "source.Region = pick_region_region" in measure.sql_expr
    assert "__sel_" not in measure.sql_expr
    assert report.best["Revenue Region"][0] == "translated"
    region = next(p for p in spec.parameters if p["name"] == "pick_region_region")
    assert region["default"] == "East"
    assert region["allowed_values"] == []


def test_two_selectors_in_one_measure_give_two_parameters():
    rows = _mapping(
        [
            _row(
                "Revenue Mix",
                'SWITCH(SELECTEDVALUE(Pick_Horizon[Horizon], "MTD"), '
                '"MTD", CALCULATE(SUM(Orders[Amount]), Orders[Region] = '
                'SELECTEDVALUE(Pick_Region[Region], "East")), "YTD", [Revenue YTD])',
            )
        ]
    )
    _, spec, report = _run(rows)
    names = sorted(p["name"] for p in spec.parameters)
    assert names == ["pick_horizon_horizon", "pick_region_region"]
    measure = (
        _named(spec, "revenue_mix__selector")
        if report.best["Revenue Mix"][0] != "rule"
        else _named(spec, "revenue_mix")
    )
    assert "CASE pick_horizon_horizon" in measure.sql_expr
    assert "source.Region = pick_region_region" in measure.sql_expr


def test_divide_branch_is_composed_and_the_choice_is_a_case():
    rows = _mapping(
        [
            _row(
                "Revenue Ratio",
                'SWITCH(SELECTEDVALUE(Pick_Horizon[Horizon], "MTD"), '
                '"MTD", DIVIDE([Revenue MTD], [Revenue YTD], BLANK()), "YTD", [Revenue YTD])',
            )
        ]
    )
    _, spec, _ = _run(rows)
    measure = _named(spec, "revenue_ratio")
    assert (
        "WHEN 'MTD' THEN (SUM(source.Amount)) / NULLIF((SUM(source.Amount)), 0)"
        in measure.sql_expr
    )


def test_grouping_measure_is_declined_with_its_reason():
    rows = _mapping(
        [
            _row(
                "Revenue Flag",
                'IF(ISFILTERED(Orders[Region]), SELECTEDVALUE(Pick_Horizon[Horizon]) = "YTD", FALSE())',
            )
        ]
    )
    _, _, report = _run(rows)
    outcome, reason = report.best["Revenue Flag"]
    assert outcome == "declined"
    assert "query's grouping" in reason


def test_selector_with_a_relationship_is_declined():
    rows = _mapping(
        [
            _row(
                "Revenue Connected",
                'SWITCH(SELECTEDVALUE(Pick_Horizon[Horizon], "MTD"), "MTD", [Revenue MTD]) '
                "+ SELECTEDVALUE(Customers[Segment])",
            )
        ]
    )
    _, _, report = _run(rows)
    assert report.best["Revenue Connected"][0] == "declined"
    assert "relationship" in report.best["Revenue Connected"][1]


def test_external_measure_is_declined():
    rows = _mapping(
        [
            _row(
                "Revenue External",
                'EXTERNALMEASURE("m", Orders[Amount]) + SELECTEDVALUE(Pick_Horizon[Horizon])',
            )
        ]
    )
    _, _, report = _run(rows)
    assert report.best["Revenue External"] == ("declined", "reads an EXTERNALMEASURE")


def test_numeric_selector_is_declined_as_not_a_text_parameter():
    rows = _mapping(
        [
            _row(
                "Revenue Index",
                "SWITCH(SELECTEDVALUE(Pick_Num[Index]), 1, [Revenue MTD], 2, [Revenue YTD])",
            )
        ]
    )
    _, _, report = _run(rows)
    outcome, reason = report.best["Revenue Index"]
    assert outcome == "declined"
    assert "numeric selector" in reason


def test_switch_off_does_nothing():
    assert slicer_parameters_enabled({}) is False
    assert slicer_parameters_enabled({"slicer_parameters": "false"}) is False
    assert slicer_parameters_enabled({"slicer_parameters": True}) is True


def test_rehome_moves_a_holder_measure_to_the_view_it_reads():
    rows = [
        _row(
            "Revenue Holder",
            'SWITCH(SELECTEDVALUE(Pick_Horizon[Horizon]), "A", [Revenue MTD])',
            table="Measures",
        ),
        _row("Revenue MTD", "SUM(Orders[Amount])"),
    ]
    moved = rehome_candidates(rows, {"Orders"}, connected={"Orders", "Customers"})
    assert moved == {"Revenue Holder": "Orders"}
    assert rows[0]["proposed_allocation"] == "Orders"


def test_rehome_leaves_a_measure_that_reads_a_table_without_a_view():
    # Planning has no view. Its measure is not moved to the view of a helper it references:
    # the columns it reads are not there, and it would be written against the wrong table.
    rows = [
        _row(
            "Plan Value",
            'VAR s = [Start] RETURN IF(SELECTEDVALUE(Pick_Horizon[Horizon]) = "A", '
            "SUMX(Planning, Planning[Value]))",
            table="Measures",
        ),
        _row("Start", "MIN(Orders[Day])"),
    ]
    moved = rehome_candidates(rows, {"Orders"}, connected={"Orders", "Customers"})
    assert moved == {}
    assert rows[0]["proposed_allocation"] == "Measures"


# ── the guard and the YAML ───────────────────────────────────────────────


def _guard(sql, parameter_names):
    measure = TranslationResult(
        measure_name="revenue_view",
        original_name="Revenue View",
        sql_expr=sql,
        is_translatable=True,
        skip_reason="",
        dax_expression="",
        confidence="medium",
        category="single_table",
    )
    return guard_view(
        "Orders",
        joins=[],
        dimensions=[],
        measures=[measure],
        fact_columns=frozenset({"amount", "region"}),
        columns_by_key={},
        key_by_view={},
        parameter_names=frozenset(parameter_names),
    )


def test_guard_accepts_a_parameter_name_read_bare():
    result = _guard(
        "CASE pick_horizon_horizon WHEN 'MTD' THEN SUM(source.amount) END",
        {"pick_horizon_horizon"},
    )
    assert [m.measure_name for m in result.measures] == ["revenue_view"]
    assert result.gaps == []


def test_guard_still_rejects_an_unknown_bare_column():
    result = _guard(
        "CASE pick_horizon_horizon WHEN 'MTD' THEN SUM(unknown_col) END",
        {"pick_horizon_horizon"},
    )
    assert result.measures == []
    assert any("unknown_col" in gap.reason for gap in result.gaps)


def test_emitted_yaml_declares_a_read_parameter_and_no_materialization():
    _, spec, _ = _run(_mapping())
    text = emit_yaml(spec)
    assert "\nparameters:\n" in text
    assert "  - name: pick_horizon_horizon\n" in text
    # A SQL string literal inside the YAML scalar: a bare YAML string is refused on deploy
    # ("default value must be a constant expression").
    assert '    default: "\'MTD\'"\n' in text
    assert yaml.safe_load(text)["parameters"][0]["default"] == "'MTD'"
    assert "materialization" not in text


def test_unread_parameter_is_not_declared():
    params = [
        {
            "name": "unused",
            "data_type": "string",
            "default": "A",
            "allowed_values": ["A"],
            "selector": "T[c]",
        }
    ]
    assert parameter_block_lines(params, ["SUM(source.amount)"]) == []


def test_the_llm_table_context_carries_the_view_parameters():
    from types import SimpleNamespace

    from src.services.tools.metric_view_utils.data_classes import TableInfo
    from src.services.tools.metric_view_utils.table_processor import _llm_context

    table_info = TableInfo(
        table_name="Orders",
        source_table="cat.sch.orders",
        aggregate_columns=[{"name": "Amount", "source_col": "Amount"}],
        group_by_columns=["Region"],
        calculated_columns=[],
        is_fact=True,
        full_sql="",
    )
    params = [{"name": "pick_horizon_horizon", "allowed_values": ["MTD", "YTD"], "default": "MTD"}]
    ctx = SimpleNamespace(source_columns={}, slicer_parameters=params)
    context = _llm_context(ctx, "Orders", table_info, "cat.sch.orders", [])
    assert "- source table: cat.sch.orders" in context
    assert "pick_horizon_horizon: values 'MTD', 'YTD'" in context
    assert "Write the bare name" in context


def test_the_llm_reference_checks_accept_a_bare_parameter_name():
    from src.services.tools.metric_view_utils.llm_reference_check import (
        column_violation,
        reference_violation,
    )

    dax = 'CALCULATE(SUM(Orders[Amount]), Orders[Region] = "__sel_pick_region_region")'
    sql = "SUM(CASE WHEN source.Region = pick_region_region THEN source.Amount END)"
    assert column_violation(dax, sql, frozenset({"amount", "region"})) is None
    assert reference_violation(dax, sql, {}) is None
