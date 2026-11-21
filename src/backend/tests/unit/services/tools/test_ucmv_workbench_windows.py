"""The workbench keeps a prior-year measure's window and key dimension, keeps its mapping flags,
leaves what the comparison did not compare open, and exports a review of the working copy.
"""

import copy

import pytest

from src.services.tools.ucmv_workbench_utils import edits
from src.services.tools.ucmv_workbench_utils.review import review_for_export
from src.services.tools.ucmv_workbench_utils.state import Workbench

VIEW = """version: 0.1
source: main.default.fact_orders
dimensions:
  - name: fiscper_key
    expr: TRY_CAST(fiscper AS INT)
measures:
  - name: orders
    expr: SUM(source.qty)
  - name: orders_py
    expr: MEASURE(orders)
    window:
      - order: fiscper_key
        range: current
        offset: -1000
        semiadditive: last
"""

OUTPUT = {
    "yaml": {"Fact_Orders": VIEW},
    "pbi_ucmv_mapping": {
        "Fact_Orders": {
            "measures": [
                {
                    "ucmv_measure": "orders",
                    "pbi_measure": "Orders",
                    "pbi_kind": "direct",
                },
                {
                    "ucmv_measure": "orders_py",
                    "pbi_measure": "Orders PY",
                    "pbi_kind": "direct",
                    "time_shifted": True,
                },
            ],
            "shifted_time_dimension": "C_Calendar.fiscper",
            "not_compared": {"orders_py": "prior year: the second pass failed"},
        }
    },
    "untranslatable_items": [],
    "resolved_measures_by_table": {
        "Fact_Orders": [
            {
                "measure_name": "orders",
                "original_name": "Orders",
                "dax_expression": "SUM(Facts[Qty])",
            }
        ]
    },
    "reconciliation": {
        "views": {
            "Fact_Orders": {
                "summary": {"overall_cell_pct": 100.0},
                "not_compared": {"orders_py": "prior year: the second pass failed"},
                "measures": {
                    "orders": {
                        "pct_aligned": 100.0,
                        "cells_total": 10,
                        "classification": "match",
                    }
                },
            }
        }
    },
    "review_bar_pct": 90.0,
    "cycles": [],
}


def _bench() -> Workbench:
    return Workbench.from_generator_output(copy.deepcopy(OUTPUT))


def _window(bench: Workbench, name: str):
    return edits.measure_entry(bench.view("Fact_Orders").yaml, name).get("window")


class TestWindowKeptThroughEdits:
    def test_replacing_the_expr_keeps_window_and_key_dimension(self):
        bench = _bench()
        bench.set_measure("Fact_Orders", "orders_py", "MEASURE(orders) * 1")
        assert _window(bench, "orders_py") == [
            {
                "order": "fiscper_key",
                "range": "current",
                "offset": -1000,
                "semiadditive": "last",
            }
        ]
        assert "fiscper_key" in edits.dimension_names(bench.view("Fact_Orders").yaml)

    def test_text_offset_survives_the_yaml_round_trip(self):
        text = VIEW.replace("offset: -1000", "offset: '-1 year'")
        new_text, _ = edits.set_measure(text, "orders_py", "MEASURE(orders)")
        entry = edits.measure_entry(new_text, "orders_py")
        assert entry["window"][0]["offset"] == "-1 year"

    def test_clear_window_removes_it_and_the_mapping_stops_marking_the_measure(self):
        bench = _bench()
        bench.set_measure(
            "Fact_Orders", "orders_py", "SUM(source.qty)", clear_window=True
        )
        assert bench.has_window("Fact_Orders", "orders_py") is False
        entries = bench.view("Fact_Orders").mapping["measures"]
        entry = next(m for m in entries if m["ucmv_measure"] == "orders_py")
        assert entry.get("time_shifted") is False

    def test_a_measure_without_a_window_is_not_marked_time_shifted(self):
        bench = _bench()
        bench.set_measure(
            "Fact_Orders", "orders_total", "SUM(source.qty)", "Orders Total"
        )
        entries = bench.view("Fact_Orders").mapping["measures"]
        entry = next(m for m in entries if m["ucmv_measure"] == "orders_total")
        assert entry.get("time_shifted") is None

    def test_the_rebound_mapping_keeps_its_time_shift_fields(self):
        mapping = OUTPUT["pbi_ucmv_mapping"]["Fact_Orders"]
        rebound = edits.rebind_mapping(mapping, "main.sandbox.fact_orders")
        assert rebound["shifted_time_dimension"] == "C_Calendar.fiscper"
        assert rebound["not_compared"] == {
            "orders_py": "prior year: the second pass failed"
        }
        assert (
            next(m for m in rebound["measures"] if m["ucmv_measure"] == "orders_py")[
                "time_shifted"
            ]
            is True
        )
        assert rebound["binding"]["ucmv_table"] == "main.sandbox.fact_orders"


class TestNotComparedStaysOpen:
    def test_a_measure_the_comparison_left_out_is_an_open_gap_with_its_reason(self):
        bench = _bench()
        gaps = bench.gaps("Fact_Orders")
        open_rows = {g["measure"]: g for g in gaps["below_100"]}
        assert open_rows["orders_py"]["classification"] == "not_compared"

    def test_a_reconciliation_that_drops_a_measure_does_not_close_its_gap(self):
        bench = _bench()
        bench.record_reconciliation(
            "Fact_Orders",
            {"orders": {"pct_aligned": 100.0}},
            {"not_compared": {"orders_py": "timed out"}},
        )
        assert bench.view("Fact_Orders").not_compared == {"orders_py": "timed out"}
        assert bench.report()["done"] is False


class TestExportReview:
    def test_the_review_counts_the_working_copy_per_view_and_lists_open_measures(self):
        review = review_for_export(_bench())
        overview = review["review_overview"]
        assert overview["Fact_Orders"]["in_scope"] == 2
        assert overview["Fact_Orders"]["match"] == 1
        assert overview["Fact_Orders"]["not_compared"] == 1
        assert overview["total"] == overview["Fact_Orders"]
        open_names = [
            (r["view"], r["measure"], r["status"])
            for r in review["review_open_measures"]
        ]
        assert open_names == [("Fact_Orders", "orders_py", "not_compared")]

    def test_the_export_carries_the_comparison_the_review_was_built_from(self):
        review = review_for_export(_bench())
        view = review["reconciliation"]["views"]["Fact_Orders"]
        assert view["measures"]["orders"]["pct_aligned"] == 100.0
        assert view["not_compared"] == {"orders_py": "prior year: the second pass failed"}

    def test_an_untouched_view_is_not_marked_changed_and_an_edited_one_is(self):
        bench = _bench()
        assert review_for_export(bench)["review_changed_views"] == []
        bench.set_measure("Fact_Orders", "orders", "SUM(source.qty) * 1")
        assert review_for_export(bench)["review_changed_views"] == ["Fact_Orders"]
        # the edited measure is not compared again yet: it is listed as open
        open_rows = review_for_export(bench)["review_open_measures"]
        assert ("Fact_Orders", "orders") in [
            (r["view"], r["measure"]) for r in open_rows
        ]

    @pytest.mark.parametrize("deployed", ["error: rejected by the sandbox"])
    def test_a_view_whose_deploy_failed_is_not_deployed_in_the_review(self, deployed):
        bench = _bench()
        bench.view("Fact_Orders").deployed = deployed
        statuses = {
            r["measure"]: r["status"]
            for r in review_for_export(bench)["review_open_measures"]
        }
        assert statuses.get("orders") == "not_deployed"
