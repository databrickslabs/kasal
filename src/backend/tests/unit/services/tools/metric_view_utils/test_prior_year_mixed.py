"""Prior-year calls inside a measure that also reads this year (prior_year_mixed.py)."""

from src.services.tools.metric_view_utils import prior_year, prior_year_mixed
from src.services.tools.metric_view_utils.data_classes import (
    MetricViewSpec,
    TranslationResult,
)
from src.services.tools.metric_view_utils.reconciliation.mapping_assembly import (
    _measures,
)

PY_CALL = "CALCULATE([X], SAMEPERIODLASTYEAR(C[d]))"


def _entry(name, dax, home="Fact"):
    return {
        "measure_name": name,
        "original_name": name,
        "dax_expression": dax,
        "proposed_allocation": home,
        "all_allocations": [{"table": home, "role": "primary"}],
    }


def _split(mapping):
    count = prior_year_mixed.split_mixed_shifts(mapping, {})
    return count


def test_variable_mix_references_a_helper():
    mapping = [
        _entry(
            "Diff",
            f"var a = {PY_CALL} var b = [X] return (b - a) * 100",
        )
    ]
    assert _split(mapping) == 1
    outer = mapping[0]
    assert "SAMEPERIODLASTYEAR" not in outer["dax_expression"]
    assert "[X PY]" in outer["dax_expression"]
    assert "[X]" in outer["dax_expression"]
    helper = mapping[-1]
    assert helper["original_name"] == "X PY"
    assert helper["dax_expression"] == PY_CALL
    assert helper[prior_year_mixed.HELPER_OF] == "Diff"
    assert helper["proposed_allocation"] == "Fact"


def test_isblank_guard_references_a_helper():
    mapping = [_entry("Guarded", f"IF(ISBLANK([X]), BLANK(), {PY_CALL})")]
    assert _split(mapping) == 1
    assert mapping[0]["dax_expression"] == "IF(ISBLANK([X]), BLANK(), [X PY])"


def test_composite_body_gets_a_named_helper():
    mapping = [
        _entry("Scaled", "CALCULATE(DIVIDE([A], [B]), SAMEPERIODLASTYEAR(C[d])) * [W]")
    ]
    assert _split(mapping) == 1
    assert mapping[0]["dax_expression"] == "[Scaled (prior year)] * [W]"
    assert mapping[-1]["original_name"] == "Scaled (prior year)"
    assert mapping[-1]["dax_expression"].startswith("CALCULATE(DIVIDE")


def test_model_measure_with_the_same_shifted_call_is_referenced():
    model = _entry("X PY", PY_CALL)
    mapping = [model, _entry("Diff", f"var a = {PY_CALL} var b = [X] return b - a")]
    assert _split(mapping) == 1
    assert "[X PY]" in mapping[1]["dax_expression"]
    assert len(mapping) == 2  # no helper added


def test_helper_name_avoids_a_measure_that_exists():
    other = _entry("X PY", "[Something else]")
    mapping = [other, _entry("Diff", f"IF(ISBLANK([X]), BLANK(), {PY_CALL})")]
    _split(mapping)
    assert mapping[-1]["original_name"] == "X PY 2"
    assert "[X PY 2]" in mapping[1]["dax_expression"]


def test_call_using_a_variable_of_the_measure_is_left_alone():
    dax = "var k = 1 return CALCULATE([X] * k, SAMEPERIODLASTYEAR(C[d])) + [X]"
    mapping = [_entry("Uses var", dax)]
    assert _split(mapping) == 0
    assert mapping[0]["dax_expression"] == dax


def test_whole_prior_year_measure_is_not_split():
    mapping = [_entry("Whole", PY_CALL)]
    assert _split(mapping) == 0
    assert len(mapping) == 1


def test_disabled_by_config_changes_nothing():
    mapping = [_entry("Diff", f"IF(ISBLANK([X]), BLANK(), {PY_CALL})")]
    count = prior_year_mixed.split_mixed_shifts(mapping, {"prior_year_measures": False})
    assert count == 0
    assert len(mapping) == 1


def test_helper_is_taken_out_and_marked_for_its_window():
    mapping = [_entry("Diff", f"IF(ISBLANK([X]), BLANK(), {PY_CALL})")]
    _split(mapping)
    prior_year.take_out_shifts(mapping, {})
    helper = mapping[-1]
    assert helper["dax_expression"] == "[X]"
    assert helper[prior_year.MARK]["helper_of"] == "Diff"


def _spec(measures, untranslatable=()):
    spec = MetricViewSpec(
        fact_table_key="Fact",
        source_table="fact",
        view_name="mv_fact",
        comment="",
        joins=[],
        dimensions=[],
        measures=[],
        untranslatable=[],
    )
    for name, original, sql in measures:
        spec.measures.append(
            TranslationResult(
                measure_name=name,
                original_name=original,
                sql_expr=sql,
                is_translatable=True,
                skip_reason="",
                dax_expression="",
                confidence="high",
                category="single_table",
            )
        )
    for name, original in untranslatable:
        spec.untranslatable.append(
            TranslationResult(
                measure_name=name,
                original_name=original,
                sql_expr=None,
                is_translatable=False,
                skip_reason="declined",
                dax_expression="",
                confidence="low",
                category="unassigned",
            )
        )
    return spec


def test_unread_helper_is_dropped_and_read_helper_kept():
    mapping = [
        {
            prior_year_mixed.HELPER_OF: "Outer",
            "original_name": "Unread PY",
            "measure_name": "Unread PY",
        },
        {
            prior_year_mixed.HELPER_OF: "Outer",
            "original_name": "X PY",
            "measure_name": "X PY",
        },
    ]
    spec = _spec(
        measures=[
            ("x_py", "X PY", "MEASURE(x)"),
            ("outer", "Outer", "MEASURE(x_py) * 2"),
        ],
        untranslatable=[("unread_py", "Unread PY")],
    )
    dropped = prior_year_mixed.drop_unused_helpers({"Fact": spec}, mapping)
    assert dropped == 1
    assert [m.measure_name for m in spec.measures] == ["x_py", "outer"]
    assert spec.untranslatable == []


def test_reconciliation_does_not_compare_a_helper():
    spec = {"measures": [{"name": "diff"}, {"name": "x_py"}]}
    resolved = [
        {
            "measure_name": "diff",
            "original_name": "Diff",
            "dax_expression": "var a ...",
        },
        {
            "measure_name": "x_py",
            "original_name": "X PY",
            "dax_expression": "CALCULATE(...)",
            "helper_of": "Diff",
        },
    ]
    rows = _measures(spec, resolved)
    assert [r["ucmv_measure"] for r in rows] == ["diff"]
