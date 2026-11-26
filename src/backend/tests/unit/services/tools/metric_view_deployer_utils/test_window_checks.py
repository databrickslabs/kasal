"""Prior-year (window) measures in the deployer: checked by their window dimension, kept in the
flagged YAML, and flagged when the comparison left them out."""

import json

import yaml

from src.services.tools.metric_view_deployer_utils.inspection import check_one
from src.services.tools.metric_view_deployer_utils.measure_check import (
    check_view_measures,
    window_orders,
)
from src.services.tools.metric_view_deployer_utils.reconciliation_flags import (
    flag_unreconciled,
    flag_yaml,
)

VIEW = "main.default.fact_orders"
WINDOW = [
    {
        "order": "fiscper_key",
        "range": "current",
        "offset": -1000,
        "semiadditive": "last",
    }
]

SPEC = {
    "version": "0.1",
    "source": "main.default.orders",
    "dimensions": [
        {"name": "region", "expr": "region"},
        {"name": "fiscper_key", "expr": "TRY_CAST(fiscper AS INT)"},
    ],
    "measures": [
        {"name": "orders", "expr": "SUM(source.qty)"},
        {"name": "orders_py", "expr": "MEASURE(orders)", "window": WINDOW},
    ],
}


def _ok(value="1"):
    return {"success": True, "data": {"result": {"data_array": [[value]]}}}


class _Recorder:
    def __init__(self):
        self.statements = []

    def __call__(self, sql):
        self.statements.append(sql)
        return _ok()


class TestWindowMeasuresAreCheckedByTheirWindow:
    def test_window_orders_maps_each_prior_year_measure_to_its_order_dimension(self):
        assert window_orders(SPEC) == {"orders_py": "fiscper_key"}

    def test_a_window_measure_is_grouped_by_its_window_not_by_the_caller_dimension(
        self,
    ):
        run = _Recorder()
        report = check_view_measures(
            run,
            VIEW,
            ["orders", "orders_py"],
            group_by="region",
            orders=window_orders(SPEC),
        )
        plain, windowed = run.statements
        assert "GROUP BY `region`" in plain
        assert "GROUP BY `fiscper_key`" in windowed
        assert report["per_measure"]["orders_py"]["grouped_by"] == "fiscper_key"
        assert "grouped_by" not in report["per_measure"]["orders"]

    def test_a_window_measure_without_group_by_is_still_grouped_by_its_window(self):
        run = _Recorder()
        check_view_measures(
            run, VIEW, ["orders_py"], orders={"orders_py": "fiscper_key"}
        )
        assert "GROUP BY `fiscper_key`" in run.statements[0]

    def test_check_one_passes_the_windows_of_the_spec_through(self):
        run = _Recorder()
        report = check_one(run, VIEW, SPEC, None, False, 30.0)
        assert report["status"] == "ok"
        assert any("GROUP BY `fiscper_key`" in sql for sql in run.statements)


class TestWindowMeasuresInTheFlags:
    OUTPUT_VIEW = {
        "measures": {
            "orders": {
                "pct_aligned": 100.0,
                "cells_total": 10,
                "classification": "match",
            }
        },
        "summary": {"overall_cell_pct": 100.0},
    }

    def _output(self, comparison):
        return {
            "yaml": {VIEW: yaml.safe_dump(SPEC, sort_keys=False)},
            "pbi_ucmv_mapping": {},
            "reconciliation": {"views": {VIEW: comparison}},
        }

    def test_a_window_measure_the_comparison_left_out_is_flagged_and_keeps_its_window(
        self,
    ):
        output = self._output(self.OUTPUT_VIEW)
        specs = flag_unreconciled(
            {VIEW: yaml.safe_dump(SPEC, sort_keys=False)}, json.dumps(output), True
        )
        flagged = yaml.safe_load(specs[VIEW])
        measures = {m["name"]: m for m in flagged["measures"]}
        assert "Not compared with Power BI" in measures["orders_py"]["comment"]
        assert measures["orders_py"]["window"] == WINDOW
        assert "fiscper_key" in [d["name"] for d in flagged["dimensions"]]
        assert "comment" not in measures["orders"]

    def test_the_text_path_keeps_the_window_lines_and_adds_the_note(self):
        text = (
            "version: 0.1\n"
            "source: main.default.orders\n"
            "dimensions:\n"
            "  - name: fiscper_key\n"
            "    expr: TRY_CAST(fiscper AS INT)\n"
            "measures:\n"
            "  - name: orders_py\n"
            "    expr: MEASURE(orders)\n"
            "    window:\n"
            "      - order: fiscper_key\n"
            "        range: current\n"
            "        offset: -1000\n"
            "        semiadditive: last\n"
            '    comment: "Prior year of orders"\n'
        )
        flagged = flag_yaml(text, {"orders_py": "Not compared with Power BI"})
        spec = yaml.safe_load(flagged)
        assert spec["measures"][0]["window"] == WINDOW
        assert "Not compared with Power BI" in spec["measures"][0]["comment"]
