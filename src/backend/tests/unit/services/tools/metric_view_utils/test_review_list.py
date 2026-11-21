"""The review list of a verify-numbers run, and the deployer's two opt-in settings built on it."""

import json

import yaml

from src.services.tools.metric_view_deployer_tool import MetricViewDeployerTool
from src.services.tools.metric_view_deployer_utils.reconciliation_flags import (
    flag_yaml,
    measure_flags,
)
from src.services.tools.metric_view_deployer_utils.reconciliation_gate import held_back
from src.services.tools.metric_view_utils.reconciliation.review_list import (
    build_review_list,
    review_csv,
    review_summary,
    with_review,
)

VIEW = """version: '1.1'
source: prod.fm.sales
dimensions:
  - name: fiscper
    expr: source.fiscper
measures:
  - name: revenue
    expr: SUM(source.revenue)
    comment: "PBI: Revenue · Used on 3 visuals: Overview (card·drawn)"
  - name: margin_pct
    expr: SUM(source.margin) / NULLIF(SUM(source.revenue), 0)
    comment: Margin share
  - name: volume
    expr: SUM(source.volume)
  - name: =cmd
    expr: SUM(source.x)
"""
OUTPUT = {
    "yaml": {"Fact_Sales": VIEW, "Fact_Stock": VIEW, "Fact_Broken": VIEW, "Dim_X": ""},
    "pbi_ucmv_mapping": {
        "Fact_Sales": {
            "measures": [
                {"ucmv_measure": "revenue", "pbi_measure": "Revenue"},
                {"ucmv_measure": "margin_pct", "pbi_measure": "Margin %"},
            ]
        }
    },
    "reconciliation": {
        "views": {
            "Fact_Sales": {
                "summary": {"overall_cell_pct": 91.0},
                "timed_out_measures": ["volume"],
                "measures": {
                    "revenue": {
                        "pct_aligned": 100.0,
                        "cells_total": 10,
                        "cells_aligned": 10,
                        "sample_mismatches": [{"pbi_value": 4307155594.247}],
                    },
                    "margin_pct": {
                        "pct_aligned": 62.5,
                        "cells_total": 8,
                        "cells_aligned": 5,
                        "classification": "real_error",
                        "sample_mismatches": [{"pbi_value": 987654.321}],
                    },
                },
            }
        }
    },
    "cycles": [
        {
            "deploy_failures": {
                "Fact_Broken": "error: [UNRESOLVED_COLUMN] source.x cannot be resolved",
                "mapping:Fact_Stock": "no period dimension (fiscper, calmonth, ...)",
            },
            "deploy_healed": {
                "Fact_Sales": ["measure old_kpi (error: gone)", "join dim_x (e)"]
            },
            "reconcile_errors": {},
        }
    ],
    "untranslatable_items": [
        {
            "table_key": "Fact_Sales",
            "original_name": "Rank %",
            "skip_reason": "RANKX has no metric-view expression",
            "used_in_visuals": [{"page": "Overview"}],
        }
    ],
}


def _status(rows, view, measure):
    return next(r for r in rows if r["view"] == view and r["measure"] == measure)


def test_every_measure_gets_one_status_with_its_reason():
    rows = build_review_list(OUTPUT, bar=90)
    revenue = _status(rows, "Fact_Sales", "revenue")
    assert (revenue["status"], revenue["power_bi"], revenue["used_on_visuals"]) == (
        "reconciled",
        "Revenue",
        3,
    )
    margin = _status(rows, "Fact_Sales", "margin_pct")
    assert (margin["status"], margin["pct_aligned"], margin["classification"]) == (
        "below_bar",
        62.5,
        "real_error",
    )
    assert "timed out" in _status(rows, "Fact_Sales", "volume")["reason"]
    assert _status(rows, "Fact_Sales", "old_kpi")["status"] == "removed_on_deploy"
    assert _status(rows, "Fact_Sales", "Rank %")["status"] == "not_translated"
    assert "no period dimension" in _status(rows, "Fact_Stock", "revenue")["reason"]
    assert _status(rows, "Fact_Broken", "revenue")["status"] == "not_deployed"


def test_the_summary_places_each_view_against_the_bar():
    rows = build_review_list(OUTPUT, bar=90)
    summary = review_summary(OUTPUT, rows, 90)
    assert summary["Fact_Sales"]["status"] == "at_or_above_bar"
    assert (
        summary["Fact_Sales"]["measures"]["below_bar"] == 1
    )  # wrong inside a passing view
    assert summary["Fact_Stock"]["status"] == "not_compared"
    assert summary["Fact_Broken"]["status"] == "not_deployed"
    assert (
        review_summary(OUTPUT, build_review_list(OUTPUT, 95), 95)["Fact_Sales"][
            "status"
        ]
        == "below_bar"
    )


def test_the_csv_holds_statistics_only_and_no_formula():
    text = review_csv(build_review_list(OUTPUT))
    assert "4307155594" not in text and "987654" not in text  # never a measure value
    assert "'=cmd" in text  # a name cannot run as a spreadsheet formula
    assert text.splitlines()[0].startswith("view,measure,kind,scope,power_bi,status")


def test_the_bar_comes_from_the_pipeline_config_and_defaults_to_90():
    assert with_review(OUTPUT)["review_bar_pct"] == 90.0
    stricter = with_review(OUTPUT, {"review_bar_pct": 95})
    assert stricter["review_summary"]["Fact_Sales"]["status"] == "below_bar"
    assert "review_csv" in stricter and stricter["yaml"] is OUTPUT["yaml"]


def test_without_a_bar_the_deployer_holds_nothing_back():
    assert held_back(OUTPUT["yaml"], OUTPUT, None) == {}
    assert held_back(OUTPUT["yaml"], json.dumps(OUTPUT), "") == {}


def test_with_a_bar_only_compared_views_at_or_above_it_are_deployed():
    held = held_back(OUTPUT["yaml"], json.dumps(OUTPUT), 90)
    assert sorted(held) == ["Fact_Broken", "Fact_Stock"]  # Dim_X has no view at all
    assert "not compared" in held["Fact_Stock"]["message"]
    assert (
        "below the bar"
        in held_back(OUTPUT["yaml"], OUTPUT, 95)["Fact_Sales"]["message"]
    )
    # a bar that cannot be checked holds everything back
    assert len(held_back({"A": VIEW, "B": VIEW}, {"yaml": {}}, 90)) == 2


def test_the_gate_is_applied_by_the_deployer_only_when_configured():
    specs = json.dumps(OUTPUT["yaml"])
    plain = json.loads(
        MetricViewDeployerTool(ucmv_output=json.dumps(OUTPUT), dry_run=True)._run(
            yaml_specs_json=specs
        )
    )
    assert plain["deployment_results"]["Fact_Stock"]["status"] == "validated"
    gated = json.loads(
        MetricViewDeployerTool(
            ucmv_output=json.dumps(OUTPUT), dry_run=True, min_reconciled_pct=90
        )._run(yaml_specs_json=specs)
    )
    results = gated["deployment_results"]
    assert results["Fact_Sales"]["status"] == "validated"
    assert results["Fact_Stock"]["status"] == "skipped"
    assert "not compared" in results["Fact_Stock"]["message"]


def test_a_measure_that_did_not_reconcile_says_so_in_its_comment():
    flags = measure_flags(OUTPUT, 90)["Fact_Sales"]
    assert sorted(flags) == ["=cmd", "margin_pct", "volume"]
    flagged = flag_yaml(VIEW, flags)
    measures = {m["name"]: m for m in yaml.safe_load(flagged)["measures"]}
    assert measures["margin_pct"]["comment"] == (
        "Margin share [Not reconciled with Power BI: 62.5 % of cells match]"
    )
    assert measures["volume"]["comment"] == "[Not compared with Power BI]"
    assert "[Not " not in measures["revenue"]["comment"]
    assert flag_yaml(flagged, flags) == flagged  # not added twice
    assert [m["expr"] for m in yaml.safe_load(flagged)["measures"]] == [
        m["expr"] for m in yaml.safe_load(VIEW)["measures"]
    ]


def test_the_flag_reaches_the_deployed_yaml_only_when_configured():
    specs = json.dumps({"Fact_Sales": VIEW})
    plain = json.loads(
        MetricViewDeployerTool(ucmv_output=json.dumps(OUTPUT), dry_run=True)._run(
            yaml_specs_json=specs
        )
    )
    assert plain["yaml"]["Fact_Sales"] == VIEW
    flagged = json.loads(
        MetricViewDeployerTool(
            ucmv_output=json.dumps(OUTPUT),
            dry_run=True,
            flag_unreconciled_measures=True,
        )._run(yaml_specs_json=specs)
    )
    assert "[Not reconciled with Power BI: 62.5 %" in flagged["yaml"]["Fact_Sales"]


def test_a_healed_view_without_comment_lines_is_flagged_too():
    """Healing re-serialises a view: other indentation, long comments folded over lines."""
    healed = yaml.safe_dump(yaml.safe_load(VIEW), sort_keys=False, allow_unicode=True)
    flagged = flag_yaml(healed, {"margin_pct": "Not compared with Power BI"})
    measures = {m["name"]: m for m in yaml.safe_load(flagged)["measures"]}
    assert (
        measures["margin_pct"]["comment"] == "Margin share [Not compared with Power BI]"
    )
    assert yaml.safe_load(flagged)["dimensions"] == yaml.safe_load(VIEW)["dimensions"]


def test_the_overview_counts_in_scope_measures_apart_from_column_sums_and_display():
    from src.services.tools.metric_view_utils.reconciliation.review_list import (
        review_overview,
    )

    output = {
        **OUTPUT,
        # margin_pct is a translated Power BI measure; revenue and volume are column sums
        "resolved_measures_by_table": {
            "Fact_Sales": [
                {"measure_name": "margin_pct", "dax_expression": "DIVIDE([M], [R])"},
                {"measure_name": "revenue", "dax_expression": ""},
                {"measure_name": "old_kpi", "dax_expression": "[A] - [B]"},
            ]
        },
        "untranslatable_items": [
            *OUTPUT["untranslatable_items"],
            {
                "table_key": "Fact_Sales",
                "original_name": "Colour",
                "dax_class": "display_layer",
                "skip_reason": "LLM declined [display_layer]",
            },
            {
                "table_key": "Fact_Sales",
                "original_name": "Dup",
                "skip_reason": "Covered on primary table (secondary allocation)",
            },
        ],
    }
    rows = build_review_list(output)
    kinds = {
        (r["measure"], r["kind"], r["scope"]) for r in rows if r["view"] == "Fact_Sales"
    }
    assert ("revenue", "column_sum", "in_scope") in kinds
    assert ("margin_pct", "measure", "in_scope") in kinds
    assert ("Colour", "measure", "display") in kinds
    assert ("Dup", "measure", "counted_elsewhere") in kinds
    sales = review_overview(rows)["Fact_Sales"]
    # in scope: margin_pct (below the bar), old_kpi (lost on deploy), Rank % (not translated)
    assert (sales["in_scope"], sales["translated"], sales["match"]) == (3, 1, 0)
    assert (sales["below_bar"], sales["lost_on_deploy"], sales["not_translated"]) == (
        1,
        1,
        1,
    )
    assert sales["display"] == 1
    # revenue (matches), volume and =cmd are plain column sums
    assert (sales["column_sums"], sales["column_sums_match"]) == (3, 1)
    assert with_review(output)["review_overview"]["total"]["in_scope"] >= 3


def test_a_measure_translated_on_its_fact_and_listed_on_its_holder_table_counts_once():
    from src.services.tools.metric_view_utils.reconciliation.review_list import (
        build_review_list,
        review_overview,
    )

    fact = """version: '1.1'
source: c.s.fact
dimensions:
  - name: country
    expr: source.country
measures:
  - name: sales_py
    expr: MEASURE(sales)
"""
    output = {
        "yaml": {"Fact": fact},
        "resolved_measures_by_table": {
            "Fact": [
                {
                    "measure_name": "sales_py",
                    "original_name": "Sales PY",
                    "dax_expression": "CALCULATE([Sales])",
                },
                # resolved, but not in the emitted view: does not count as translated
                {
                    "measure_name": "margin_py",
                    "original_name": "Margin PY",
                    "dax_expression": "CALCULATE([Margin])",
                },
            ]
        },
        "untranslatable_items": [
            {"table_key": "Calendar", "original_name": "Sales PY", "skip_reason": "x"},
            {"table_key": "Calendar", "original_name": "Margin PY", "skip_reason": "x"},
            {"table_key": "Calendar", "original_name": "Other", "skip_reason": "x"},
        ],
    }
    rows = build_review_list(output)
    scopes = {(r["view"], r["measure"]): r["scope"] for r in rows}
    assert scopes[("Calendar", "Sales PY")] == "counted_elsewhere"
    assert scopes[("Calendar", "Margin PY")] == "in_scope"
    assert scopes[("Calendar", "Other")] == "in_scope"
    overview = review_overview(rows)
    assert overview["Calendar"]["in_scope"] == 2
    assert overview["Calendar"]["not_translated"] == 2
    assert overview["total"]["in_scope"] == 3  # Sales PY once, on its fact
