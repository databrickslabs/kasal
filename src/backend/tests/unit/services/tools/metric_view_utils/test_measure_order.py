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
