"""A metric view resolves MEASURE(x) only against measures defined before the reader."""

from types import SimpleNamespace

from src.services.tools.metric_view_utils.measure_arithmetic import _result_only
from src.services.tools.metric_view_utils.measure_order import order_by_dependencies
from src.services.tools.metric_view_utils.sql_measure_sanitizer import (
    detect_silent_wrong,
)


def _m(name, sql):
    return SimpleNamespace(measure_name=name, sql_expr=sql)


def _names(measures):
    return [m.measure_name for m in measures]


def test_a_measure_moves_behind_the_measures_it_reads():
    measures = [
        _m("ratio", "MEASURE(num) / NULLIF(MEASURE(den), 0)"),
        _m("other", "SUM(source.x)"),
        _m("num", "SUM(source.a)"),
        _m("ratio_pct", "100 * MEASURE(ratio)"),
        _m("den", "SUM(source.b)"),
    ]
    order_by_dependencies(measures)
    assert _names(measures) == ["other", "num", "den", "ratio", "ratio_pct"]


def test_an_order_that_already_works_is_not_touched():
    measures = [
        _m("a", "SUM(source.a)"),
        _m("b", "MEASURE(a) * 2"),
        _m("c", "SUM(source.c)"),
    ]
    before = list(measures)
    order_by_dependencies(measures)
    assert measures == before


def test_a_cycle_and_a_reference_outside_the_list_keep_their_place():
    measures = [_m("a", "MEASURE(b)"), _m("b", "MEASURE(a)"), _m("c", "MEASURE(base)")]
    order_by_dependencies(measures)
    assert _names(measures) == ["c", "a", "b"]


def test_a_resolved_variable_block_is_its_return_expression():
    assert _result_only("var ret = MEASURE(x)\nreturn MEASURE(x)") == "MEASURE(x)"
    assert _result_only("return [A] - [B]") == "[A] - [B]"
    assert _result_only("[A] - [B]") == "[A] - [B]"
    # a variable the resolver does not inline is still read: no expression
    assert _result_only("var a = CALCULATE([X], T[c] = 1)\nreturn a * 2") is None
    assert _result_only("var a = [X]") is None


def test_a_variable_block_left_in_the_sql_is_rejected():
    assert detect_silent_wrong("var ret= MEASURE(x) ret")
    assert not detect_silent_wrong("MEASURE(x) / NULLIF(MEASURE(y), 0)")


# ── DAX arithmetic over measures, as SQL ───────────────────────────────────


def test_a_sum_or_difference_of_measures_counts_a_blank_operand_as_zero():
    from src.services.tools.metric_view_utils.measure_arithmetic import _blank_as_zero

    assert _blank_as_zero("MEASURE(a) - MEASURE(b) - MEASURE(c)") == (
        "CASE WHEN COALESCE(MEASURE(a), MEASURE(b), MEASURE(c)) IS NOT NULL "
        "THEN (COALESCE(MEASURE(a), 0) - COALESCE(MEASURE(b), 0) - COALESCE(MEASURE(c), 0)) END"
    )
    assert _blank_as_zero("(MEASURE(b) - MEASURE(a)) * 100") == (
        "CASE WHEN COALESCE(MEASURE(b), MEASURE(a)) IS NOT NULL "
        "THEN (COALESCE(MEASURE(b), 0) - COALESCE(MEASURE(a), 0)) * 100 END"
    )
    # products, quotients and single measures are left alone
    for expr in (
        "MEASURE(a) / NULLIF(MEASURE(b), 0)",
        "MEASURE(a) * MEASURE(b)",
        "MEASURE(a)",
        "MEASURE(a) - 1",
    ):
        assert _blank_as_zero(expr) == expr


def test_a_division_keeps_its_operands_together_and_its_alternate_result():
    from src.services.tools.metric_view_utils.measure_arithmetic import _division

    assert (
        _division("DIVIDE(a, b)", "MEASURE(a)", "MEASURE(b)")
        == "MEASURE(a) / NULLIF(MEASURE(b), 0)"
    )
    # a + b + c / d would divide only c
    assert (
        _division("DIVIDE(x, y)", "MEASURE(a) + MEASURE(b) + MEASURE(c)", "MEASURE(d)")
        == "(MEASURE(a) + MEASURE(b) + MEASURE(c)) / NULLIF(MEASURE(d), 0)"
    )
    assert (
        _division("DIVIDE(x, y)", "MEASURE(a)", "MEASURE(b) - MEASURE(c)")
        == "MEASURE(a) / NULLIF(MEASURE(b) - MEASURE(c), 0)"
    )
    # the alternate applies to a zero or blank denominator, not to a blank numerator
    assert (
        _division("DIVIDE(MEASURE(a), MEASURE(b), 0)", "MEASURE(a)", "MEASURE(b)")
        == "CASE WHEN NULLIF(MEASURE(b), 0) IS NULL THEN 0 ELSE MEASURE(a) / MEASURE(b) END"
    )
    # a blank alternate is the default
    assert (
        _division("DIVIDE(MEASURE(a), MEASURE(b), NULL)", "MEASURE(a)", "MEASURE(b)")
        == "MEASURE(a) / NULLIF(MEASURE(b), 0)"
    )


def test_float_noise_on_a_large_value_is_not_a_mismatch_but_a_real_difference_is():
    import pandas as pd

    from src.services.tools.metric_view_utils.reconciliation.compare import (
        _within_tolerance,
    )

    measure = SimpleNamespace(tolerance_kind="absolute", tolerance_value=0.001)
    cells = pd.DataFrame(
        {
            "ucmv_value": [7.0e12, 7.0e12, 10.0, 10.0, None],
            "pbi_value": [7.0e12 + 0.004, 7.0e12 + 10000.0, 10.0005, 10.01, 0.0],
        }
    )
    cells["delta"] = cells["ucmv_value"].fillna(0) - cells["pbi_value"].fillna(0)
    assert _within_tolerance(cells, measure).tolist() == [
        True,
        False,
        True,
        False,
        True,
    ]


def test_the_rule_based_division_keeps_a_summed_numerator_together():
    from src.services.tools.metric_view_utils.measure_arithmetic import _operand

    assert (
        _operand("SUM(source.a) + SUM(source.b)") == "(SUM(source.a) + SUM(source.b))"
    )
    assert _operand("SUM(source.a) FILTER (WHERE source.k = 'x-y')") == (
        "SUM(source.a) FILTER (WHERE source.k = 'x-y')"
    )
    assert _operand("ABS(SUM(source.a) - SUM(source.b))") == (
        "ABS(SUM(source.a) - SUM(source.b))"
    )
