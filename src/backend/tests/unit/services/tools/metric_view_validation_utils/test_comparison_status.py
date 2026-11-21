"""The static verdict next to the comparison with Power BI: the classes, and the rule that the
comparison never changes a verdict."""

import json

from src.services.tools.metric_view_validation_utils.comparison_status import (
    CLASSES,
    annotate,
    comparison_summary_for,
    judge,
)


def _row(status: str, pct: float = 100.0) -> dict:
    return {
        "status": status,
        "pct_aligned": pct,
        "cells_aligned": 90,
        "cells_total": 100,
    }


def test_a_flagged_measure_that_matches_says_check_the_rule():
    judged = judge("REVIEW", _row("reconciled"))
    assert judged["class"] == "flagged_matches_pbi"
    assert "check the rule, not the measure" in judged["action"]
    assert "90 of 100 cells" in judged["action"]


def test_a_flagged_measure_below_the_bar_says_fix_the_translation():
    judged = judge("INVALID", _row("below_bar", 40.0))
    assert judged["class"] == "flagged_below_bar"
    assert "fix the translation" in judged["action"]


def test_a_valid_measure_below_the_bar_says_the_static_check_cannot_see_it():
    judged = judge("VALID", _row("below_bar", 53.7))
    assert judged["class"] == "valid_below_bar"
    assert "cannot see this" in judged["action"]


def test_a_valid_measure_that_matches_is_consistent():
    assert judge("EQUIVALENT", _row("reconciled"))["class"] == "valid_matches_pbi"


def test_no_comparison_row_is_not_compared():
    judged = judge("VALID", None)
    assert judged["class"] == "not_compared"
    assert judged["pbi_status"] == "no row"


def test_a_row_that_was_not_compared_is_not_compared():
    judged = judge("REVIEW", _row("not_compared"))
    assert judged["class"] == "not_compared"


def test_annotate_adds_the_comparison_and_keeps_every_verdict():
    results = {
        "view_a": {
            "details": [
                {
                    "measure_name": "m_one",
                    "measure_eval_result": {"status": "REVIEW", "similarities": []},
                },
                {
                    "measure_name": "m_two",
                    "measure_eval_result": {"status": "VALID", "similarities": []},
                },
                {
                    "measure_name": "m_skip",
                    "measure_eval_result": {"status": "skipped", "reason": "x"},
                },
            ]
        }
    }
    rows = {
        ("view_a", "m_one"): _row("reconciled"),
        ("view_a", "m_two"): _row("below_bar", 10.0),
    }
    counts = annotate(results, rows)
    details = results["view_a"]["details"]
    assert [d["measure_eval_result"]["status"] for d in details] == [
        "REVIEW",
        "VALID",
        "skipped",
    ]
    assert (
        details[0]["measure_eval_result"]["comparison"]["class"]
        == "flagged_matches_pbi"
    )
    assert details[1]["measure_eval_result"]["comparison"]["class"] == "valid_below_bar"
    assert "comparison" not in details[2]["measure_eval_result"]
    assert counts["flagged_matches_pbi"] == 1 and counts["valid_below_bar"] == 1
    assert set(counts) == set(CLASSES)


def test_summary_without_a_reconciliation_annotates_nothing():
    results = {
        "view_a": {
            "details": [
                {"measure_name": "m_one", "measure_eval_result": {"status": "VALID"}}
            ]
        }
    }
    summary = comparison_summary_for(results, json.dumps({"yaml": {}}))
    assert summary["comparison_available"] is False
    assert all(v == 0 for v in summary["comparison_classes"].values())
    assert "comparison" not in results["view_a"]["details"][0]["measure_eval_result"]


def test_summary_with_a_reconciliation_reports_the_bar_and_the_classes():
    output = {
        "yaml": {"view_a": "measures:\n  - name: m_one\n    expr: SUM(source.x)\n"},
        "reconciliation": {
            "views": {
                "view_a": {
                    "measures": {
                        "m_one": {
                            "pct_aligned": 100.0,
                            "cells_aligned": 5,
                            "cells_total": 5,
                        }
                    },
                }
            }
        },
    }
    results = {
        "view_a": {
            "details": [
                {"measure_name": "m_one", "measure_eval_result": {"status": "INVALID"}}
            ]
        }
    }
    summary = comparison_summary_for(results, output)
    assert summary["comparison_available"] is True
    assert summary["comparison_bar_pct"] == 90.0
    assert summary["comparison_classes"]["flagged_matches_pbi"] == 1
    assert results["view_a"]["details"][0]["measure_eval_result"]["status"] == "INVALID"
