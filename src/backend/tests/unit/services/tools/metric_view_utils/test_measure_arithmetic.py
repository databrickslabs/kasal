"""Measure arithmetic: a measure that is arithmetic over measures of the view, resolved after
the LLM (or any later translator) has translated the measures it reads."""

from src.services.tools.metric_view_utils.data_classes import TranslationResult
from src.services.tools.metric_view_utils.dax_var_chain import (
    extract_divide_args,
    resolve_var_chain,
)
from src.services.tools.metric_view_utils.measure_arithmetic import (
    _if_blank_to_case,
    resolve_measure_arithmetic,
)


def _result(name, dax, sql=None):
    return TranslationResult(
        measure_name=name,
        original_name=name,
        sql_expr=sql,
        is_translatable=sql is not None,
        skip_reason="" if sql is not None else "LLM declined",
        dax_expression=dax,
        confidence="high",
        category="single_table" if sql is not None else "unassigned",
    )


def _run(translated, untranslatable, base_names):
    original_to_snake = {m.original_name: m.measure_name for m in translated}
    return resolve_measure_arithmetic(
        translated,
        untranslatable,
        base_names,
        original_to_snake,
        {},
        resolve_var_chain_fn=resolve_var_chain,
        extract_divide_args_fn=extract_divide_args,
    )


def test_ratio_over_measure_translated_later_is_resolved():
    # The LLM translated the two bases after the arithmetic pass ran; the late pass
    # resolves the ratio.
    translated = [
        _result("kbi_phc", "x", sql="SUM(source.phc)"),
        _result("kbi_ordered", "y", sql="SUM(source.ordered)"),
    ]
    untranslatable = [_result("kbi_pct", "DIVIDE([KBI phc], [KBI ordered], 0)")]
    base_names = {"kbi_phc", "kbi_ordered"}
    translated[0].original_name = "KBI phc"
    translated[1].original_name = "KBI ordered"
    out_t, out_u = _run(translated, untranslatable, base_names)
    assert out_u == []
    resolved = out_t[-1]
    assert resolved.measure_name == "kbi_pct"
    assert resolved.sql_expr == "MEASURE(kbi_phc) / NULLIF(MEASURE(kbi_ordered), 0)"
    assert resolved.category == "measure_arithmetic"
    assert "kbi_pct" in base_names


def test_chain_resolves_in_later_iteration():
    # b reads a, c reads b: a is translated, so b resolves first, then c.
    translated = [_result("a", "x", sql="SUM(source.a)")]
    translated[0].original_name = "A"
    untranslatable = [
        _result("c", "[B] * 2"),
        _result("b", "[A] + 1"),
    ]
    untranslatable[0].original_name = "C"
    untranslatable[1].original_name = "B"
    out_t, out_u = _run(translated, untranslatable, {"a"})
    assert out_u == []
    assert {m.measure_name for m in out_t} == {"a", "b", "c"}


def test_unresolved_reference_stays_untranslatable():
    translated = [_result("a", "x", sql="SUM(source.a)")]
    translated[0].original_name = "A"
    untranslatable = [_result("ratio", "[A] / [Missing]")]
    untranslatable[0].original_name = "Ratio"
    out_t, out_u = _run(translated, untranslatable, {"a"})
    assert [m.measure_name for m in out_u] == ["ratio"]
    assert len(out_t) == 1


def test_dax_only_construct_is_not_emitted():
    # SELECTEDVALUE is DAX-only: the measure is a slicer dispatch, never SQL.
    translated = [_result("a", "x", sql="SUM(source.a)")]
    translated[0].original_name = "A"
    untranslatable = [_result("switchy", "SWITCH(SELECTEDVALUE(T[x]), 1, [A], 0)")]
    untranslatable[0].original_name = "Switchy"
    out_t, out_u = _run(translated, untranslatable, {"a"})
    assert [m.measure_name for m in out_u] == ["switchy"]


def test_if_blank_guard_becomes_case():
    sql = _if_blank_to_case("IF(ISBLANK(MEASURE(x)), NULL, MEASURE(p) * 2)")
    assert sql == "CASE WHEN MEASURE(x) IS NULL THEN NULL ELSE (MEASURE(p) * 2) END"


def test_if_blank_guard_without_closing_paren_is_left_alone():
    expr = "IF(ISBLANK(MEASURE(x)), NULL, MEASURE(p)"
    assert _if_blank_to_case(expr) == expr
