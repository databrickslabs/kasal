"""Rerun pins: a previous generator output reused measure by measure. Made-up names and DAX only."""

from src.services.tools.metric_view_utils import prior_year
from src.services.tools.metric_view_utils.data_classes import (
    MetricViewSpec,
    TranslationResult,
)
from src.services.tools.uc_metric_view_generator_utils.previous_run_pins import (
    PIN_COMMENT,
    PinPlan,
    pin_outcome,
    pin_report,
    plan_previous_run_pins,
    stale_pins,
)

DAX = "SUM(T[amount])"


def _previous(**overrides):
    """A previous output with one view ``fact_a`` (table key) compared on two measures."""
    prev = {
        "yaml": {"fact_a": "version: '1.1'"},
        "specs_summary": {"fact_a": {"view_name": "mv_a"}},
        "best_by_view": {"fact_a": {"pct": 100.0, "cycle": 1}},
        "cycles": [{"cycle": 1, "deploy_healed": {}}],
        "reconciliation": {
            "views": {
                "fact_a": {
                    "measures": {
                        "m_good": {"pct_aligned": 100.0, "cells_total": 50},
                        "m_close": {"pct_aligned": 95.0, "cells_total": 50},
                    }
                }
            }
        },
        "resolved_measures_by_table": {
            "fact_a": [
                {
                    "measure_name": "m_good",
                    "original_name": "Good",
                    "sql_expr": "SUM(source.amount)",
                    "window": None,
                    "dax_expression": DAX,
                },
                {
                    "measure_name": "m_close",
                    "original_name": "Close",
                    "sql_expr": "SUM(source.amount) + 1",
                    "window": None,
                    "dax_expression": DAX,
                },
            ]
        },
    }
    prev.update(overrides)
    return prev


def _measures(dax=DAX):
    return [
        {"measure_name": "Good", "dax_expression": dax},
        {"measure_name": "Close", "dax_expression": DAX},
    ]


def test_locked_measure_with_unchanged_dax_is_pinned_without_window():
    plan = plan_previous_run_pins(_previous(), _measures(), {})
    assert plan.pins == {("fact_a", "m_good"): "SUM(source.amount)"}
    (entry,) = plan.overrides["fact_a"]
    assert entry == {
        "name": "m_good",
        "original_name": "Good",
        "expr": "SUM(source.amount)",
        "comment": PIN_COMMENT,
    }
    assert "window" not in entry  # add_windows adds the window; a pin must not set one


def test_below_lock_is_not_pinned_and_the_threshold_is_configurable():
    assert (
        plan_previous_run_pins(_previous(), _measures(), {}).not_pinned[
            "below the 99% lock"
        ]
        == 1
    )
    lowered = plan_previous_run_pins(_previous(), _measures(), {"rerun_lock_pct": 90})
    assert set(lowered.pins) == {("fact_a", "m_good"), ("fact_a", "m_close")}


def test_a_measure_compared_on_too_few_cells_is_not_pinned():
    prev = _previous()
    prev["reconciliation"]["views"]["fact_a"]["measures"]["m_good"]["cells_total"] = 3
    plan = plan_previous_run_pins(prev, _measures(), {})
    assert plan.pins == {}
    assert plan.not_pinned.get("compared on fewer than 10 cells") == 1
    relaxed = plan_previous_run_pins(prev, _measures(), {"rerun_min_cells": 3})
    assert ("fact_a", "m_good") in relaxed.pins


def test_changed_dax_is_not_pinned():
    plan = plan_previous_run_pins(_previous(), _measures("SUM(T[amount2])"), {})
    assert plan.pins == {}
    assert plan.not_pinned.get("DAX changed") == 1


def test_preclean_form_of_the_dax_is_accepted():
    # The translator stores the DAX after preclean (variables inlined); the same DAX
    # written with a variable must still match the stored form.
    raw = "VAR x = SUM(T[amount])\nRETURN x"
    from src.services.tools.metric_view_utils.dax_preclean import preclean_dax

    stored = preclean_dax(raw)
    prev = _previous()
    prev["resolved_measures_by_table"]["fact_a"][0]["dax_expression"] = stored
    plan = plan_previous_run_pins(
        prev, [{"measure_name": "Good", "dax_expression": raw}], {}
    )
    assert ("fact_a", "m_good") in plan.pins


def test_healed_measure_is_not_pinned():
    prev = _previous(
        cycles=[
            {
                "cycle": 1,
                "deploy_healed": {"fact_a": ["measure m_good (error: [UNRESOLVED])"]},
            }
        ]
    )
    plan = plan_previous_run_pins(prev, _measures(), {})
    assert plan.pins == {}
    assert plan.not_pinned.get("removed by deploy healing in the previous run") == 1


def test_measure_that_failed_in_this_run_is_retranslated_not_pinned_again():
    feedback = [{"view": "fact_a", "measure": "m_good", "pct_aligned": 80.0}]
    plan = plan_previous_run_pins(_previous(), _measures(), {}, feedback)
    assert plan.pins == {}
    assert plan.not_pinned.get("failed in this run's comparison (retranslated)") == 1


def test_feedback_as_json_text_is_read_too():
    import json

    feedback = json.dumps([{"view": "fact_a", "measure": "m_good"}])
    assert plan_previous_run_pins(_previous(), _measures(), {}, feedback).pins == {}


def test_view_not_compared_or_final_yaml_from_another_cycle_is_not_pinned():
    prev = _previous(best_by_view={"fact_a": {"pct": 100.0, "cycle": 1}})
    prev["cycles"] = [
        {"cycle": 1},
        {"cycle": 2},
    ]  # SQL is from cycle 2, YAML from cycle 1
    plan = plan_previous_run_pins(prev, _measures(), {})
    assert plan.pins == {}
    assert "earlier cycle" in next(iter(plan.not_pinned))

    unseen = _previous(reconciliation={"views": {}})
    assert plan_previous_run_pins(unseen, _measures(), {}).pins == {}


def test_no_previous_output_or_no_reconciliation_pins_nothing():
    assert plan_previous_run_pins(None, _measures(), {}).pins == {}
    assert plan_previous_run_pins("not json", _measures(), {}).pins == {}
    single_pass = {"yaml": {"fact_a": "x"}, "resolved_measures_by_table": {}}
    plan = plan_previous_run_pins(single_pass, _measures(), {})
    assert plan.pins == {} and "no reconciliation" in plan.note


def test_previous_output_given_as_json_text():
    import json

    plan = plan_previous_run_pins(json.dumps(_previous()), _measures(), {})
    assert ("fact_a", "m_good") in plan.pins


def test_without_drops_only_the_named_pins():
    plan = plan_previous_run_pins(_previous(), _measures(), {"rerun_lock_pct": 90})
    kept = plan.without([("fact_a", "m_good")])
    assert set(kept.pins) == {("fact_a", "m_close")}
    assert [e["name"] for e in kept.overrides["fact_a"]] == ["m_close"]


def _spec(columns, joins=(), measures=(), dimensions=None):
    return MetricViewSpec(
        fact_table_key="fact_a",
        source_table="src",
        view_name="mv_a",
        comment="",
        joins=list(joins),
        dimensions=list(
            dimensions
            if dimensions is not None
            else [{"name": "d", "expr": "source.d"}]
        ),
        measures=list(measures),
        untranslatable=[],
        fact_columns=list(columns),
    )


def _translated(name, sql, window=None, original=None):
    return TranslationResult(
        measure_name=name,
        original_name=original or name,
        sql_expr=sql,
        is_translatable=True,
        skip_reason="",
        dax_expression="",
        confidence="high",
        category="single_table",
        window_spec=window,
    )


def _plan_for(sql, window=None):
    plan = PinPlan(
        pins={("fact_a", "m"): sql},
        windows={("fact_a", "m"): window},
    )
    return plan


def test_pin_still_fits_a_view_with_its_columns_and_alias():
    spec = _spec(
        ["amount"],
        joins=[{"name": "dim_x"}],
        measures=[_translated("m", "SUM(source.amount) + dim_x.k")],
    )
    spec.measures[0].sql_expr = "SUM(source.amount) + dim_x.k"
    assert stale_pins({"fact_a": spec}, _plan_for("SUM(source.amount) + dim_x.k")) == {}


def test_pin_reading_a_column_the_view_lost_is_stale():
    spec = _spec(["other"], measures=[_translated("m", "SUM(source.amount)")])
    stale = stale_pins({"fact_a": spec}, _plan_for("SUM(source.amount)"))
    assert stale == {("fact_a", "m"): "the view has no column source.amount"}


def test_pin_reading_a_join_alias_the_view_lost_is_stale():
    spec = _spec(["amount"], joins=[], measures=[_translated("m", "SUM(dim_x.k)")])
    stale = stale_pins({"fact_a": spec}, _plan_for("SUM(dim_x.k)"))
    assert stale == {("fact_a", "m"): "the view has no join alias dim_x"}


def test_pin_reading_a_measure_the_view_lost_is_stale():
    spec = _spec(["amount"], measures=[_translated("m", "MEASURE(gone) * 2")])
    stale = stale_pins({"fact_a": spec}, _plan_for("MEASURE(gone) * 2"))
    assert stale == {("fact_a", "m"): "the view has no measure gone"}


def test_prior_year_pin_whose_window_changed_is_stale():
    window = {
        "order": "fiscper_key",
        "range": "current",
        "offset": -1000,
        "semiadditive": "last",
    }
    spec = _spec(
        ["amount"], measures=[_translated("m", "SUM(source.amount)", window=None)]
    )
    stale = stale_pins({"fact_a": spec}, _plan_for("SUM(source.amount)", window))
    assert stale and "window the previous run had" in stale[("fact_a", "m")]


def test_a_pin_the_rules_replaced_is_not_in_effect_so_not_stale():
    spec = _spec([], measures=[_translated("m", "SUM(source.other)")])
    assert stale_pins({"fact_a": spec}, _plan_for("SUM(source.amount)")) == {}


def test_outcome_reports_reused_and_invalidated():
    plan = plan_previous_run_pins(_previous(), _measures(), {"rerun_lock_pct": 90})
    pinned = _translated("m_good", "SUM(source.amount)")
    pinned.category = "manual_override"
    pinned.skip_reason = PIN_COMMENT  # the override took effect
    spec = _spec([], measures=[pinned])
    outcome = pin_outcome({"fact_a": spec}, plan, {("fact_a", "m_close"): "gone"})
    out = outcome.as_output()
    assert out["reused_from_previous_run"] == {"fact_a": ["m_good"]}
    assert out["previous_run_pins"]["invalidated"] == {"fact_a": {"m_close": "gone"}}
    assert out["previous_run_pins"]["reused"] == 1
    assert out["previous_run_pins"]["rule_translated_same_sql"] == 0


def test_a_rule_translation_to_the_same_sql_is_not_reported_as_reused():
    plan = plan_previous_run_pins(_previous(), _measures(), {"rerun_lock_pct": 90})
    by_rule = _translated("m_good", "SUM(source.amount)")  # category single_table
    out = pin_outcome({"fact_a": _spec([], measures=[by_rule])}, plan, {}).as_output()
    assert out["reused_from_previous_run"] == {}
    assert out["previous_run_pins"]["rule_translated_same_sql"] == 1


def test_pin_report_is_empty_without_a_pipeline_outcome():
    class _Pipeline:
        pass

    assert pin_report(_Pipeline()) == {}


def _mapping():
    return [
        {
            "measure_name": "sales_py",
            "original_name": "Sales PY",
            prior_year.MARK: {
                "dax": "CALCULATE([Sales], SAMEPERIODLASTYEAR(d[date]))",
                "calendar": "cal",
                "column": "date",
                "helper_of": None,
            },
        }
    ]


def test_prior_year_pin_gets_its_window_from_add_windows_not_from_the_pin():
    """The window and its key dimension come from add_windows (the DAX mark survives a pin).
    A window set on the pin instead would demote the measure."""
    spec = _spec(
        ["fiscper", "amount"],
        dimensions=[{"name": "fiscper", "expr": "source.fiscper"}],
        measures=[_translated("sales_py", "SUM(source.amount)", original="Sales PY")],
    )
    mapping = _mapping()
    windowed = prior_year.add_windows({"fact_a": spec}, mapping, {})
    assert windowed == {"fact_a": ["sales_py"]}
    assert spec.measures[0].window_spec == {
        "order": "fiscper_key",
        "range": "current",
        "offset": -1000,
        "semiadditive": "last",
    }
    assert any(d.get("name") == "fiscper_key" for d in spec.dimensions)

    pinned_with_window = _spec(
        ["fiscper", "amount"],
        dimensions=[{"name": "fiscper", "expr": "source.fiscper"}],
        measures=[
            _translated(
                "sales_py",
                "SUM(source.amount)",
                window={"order": "x"},
                original="Sales PY",
            )
        ],
    )
    # add_windows pops the mark from the mapping it was given: a fresh mapping for the second call.
    prior_year.add_windows({"fact_a": pinned_with_window}, _mapping(), {})
    assert pinned_with_window.measures == []  # demoted: a window already on the measure
    assert [m.measure_name for m in pinned_with_window.untranslatable] == ["sales_py"]


def test_each_measure_records_how_its_sql_was_reached():
    from src.services.tools.metric_view_utils.translation_source import (
        CARRIED_FORWARD_COMMENT,
        translation_source,
    )

    assert translation_source("llm_translated", "SUM(x)", "") == "llm"
    assert translation_source("manual_override", "SUM(x)", PIN_COMMENT) == "reused"
    assert (
        translation_source("manual_override", "SUM(x)", CARRIED_FORWARD_COMMENT)
        == "carried_forward"
    )
    assert (
        translation_source("manual_override", "SUM(x)", "Manual override") == "manual"
    )
    assert translation_source("single_table", "SUM(x)", "") == "rule"
    assert translation_source("single_table", None, "not translated") == "none"
