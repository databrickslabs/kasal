"""Prior-year measures in the validator: the window must carry the DAX's one-year shift."""

import json

import pytest

from src.services.tools.metric_view_validation_utils.pipeline import (
    MetricExpressionValidatorPipeline,
)
from src.services.tools.metric_view_validation_utils.prior_year_check import (
    judge_prior_year,
    loose_name,
    window_shifts_back,
)

PY_DAX = "CALCULATE([Sales Actual], SAMEPERIODLASTYEAR(Calendar[Date]))"
WINDOW = [
    {
        "order": "fiscper_key",
        "range": "current",
        "offset": -1000,
        "semiadditive": "last",
    }
]


def _measure(expr: str, window=None) -> dict:
    measure = {"name": "sales_py", "expr": expr}
    if window is not None:
        measure["window"] = window
    return measure


class TestLooseName:
    def test_case_spaces_and_underscores_are_ignored(self):
        assert loose_name("Sales Actual PY") == loose_name("sales_actual_py")

    def test_percent_is_spelled_pct(self):
        assert loose_name("Billed % PY") == loose_name("billed_pct_py")
        assert loose_name("Billed % PY") != loose_name("Billed PY")


class TestWindowShiftsBack:
    @pytest.mark.parametrize("offset", [-1000, -100, "-1000", "-1 year", "-12 months"])
    def test_negative_offsets_shift_back(self, offset):
        assert window_shifts_back([{"order": "k", "offset": offset}])

    @pytest.mark.parametrize(
        "window", [None, [], [{"order": "k"}], [{"offset": 0}], [{"offset": True}]]
    )
    def test_no_back_shift(self, window):
        assert not window_shifts_back(window)


class TestJudgePriorYear:
    def test_window_carries_the_shift_is_valid(self):
        verdict = judge_prior_year(_measure("MEASURE(sales_actual)", WINDOW), PY_DAX)
        assert verdict.status == "VALID"

    def test_missing_window_is_invalid_shift_lost(self):
        verdict = judge_prior_year(_measure("MEASURE(sales_actual)"), PY_DAX)
        assert verdict.status == "INVALID"
        assert "prior year lost" in verdict.reason

    def test_window_without_back_shift_is_invalid(self):
        window = [{"order": "fiscper_key", "range": "current", "offset": 0}]
        verdict = judge_prior_year(_measure("MEASURE(sales_actual)", window), PY_DAX)
        assert verdict.status == "INVALID"

    def test_window_reading_another_base_is_invalid(self):
        verdict = judge_prior_year(_measure("MEASURE(volume_actual)", WINDOW), PY_DAX)
        assert verdict.status == "INVALID"
        assert "volume_actual" in verdict.reason

    def test_plain_aggregate_with_window_passes_base_to_normal_rules(self):
        verdict = judge_prior_year(_measure("SUM(source.amount)", WINDOW), PY_DAX)
        assert verdict.status is None
        assert verdict.base_dax == "[Sales Actual]"

    def test_plain_aggregate_without_window_is_invalid(self):
        verdict = judge_prior_year(_measure("SUM(source.amount)"), PY_DAX)
        assert verdict.status == "INVALID"

    def test_window_on_a_measure_whose_dax_is_not_prior_year_is_review(self):
        verdict = judge_prior_year(
            _measure("MEASURE(sales_actual)", WINDOW), "[Sales Actual]"
        )
        assert verdict.status == "REVIEW"

    def test_no_window_and_not_prior_year_is_not_judged(self):
        assert (
            judge_prior_year(_measure("MEASURE(sales_actual)"), "[Sales Actual]")
            is None
        )


def _write_inputs(tmp_path, measures, dax_rows):
    yaml_text = "\n".join(
        [
            "version: 0.1",
            "source: main.default.facts",
            "dimensions:",
            "  - name: fiscper_key",
            "    expr: TRY_CAST(fiscper AS INT)",
            "measures:",
            "  - name: sales_actual",
            "    expr: SUM(source.amount)",
            *[line for m in measures for line in _yaml_measure(m)],
        ]
    )
    yaml_path = tmp_path / "view.yml"
    yaml_path.write_text(yaml_text, encoding="utf-8")
    mapping_path = tmp_path / "map.json"
    mapping_path.write_text(json.dumps(dax_rows), encoding="utf-8")
    return str(yaml_path), str(mapping_path)


def _yaml_measure(measure: dict) -> list[str]:
    lines = [f"  - name: {measure['name']}", f"    expr: {measure['expr']}"]
    if measure.get("window"):
        lines += [
            "    window:",
            f"      - order: {measure['window'][0]['order']}",
            "        range: current",
            f"        offset: {measure['window'][0]['offset']}",
            "        semiadditive: last",
        ]
    return lines


def _status(result: dict, name: str) -> str:
    for row in result.get("evaluated", []):
        if row.get("measure_name") == name:
            return row["measure_eval_result"]["status"]
    for row in result.get("skipped", []):
        if row.get("measure_name") == name:
            return "skipped"
    return "missing"


DAX_ROWS = [
    {"measure_name": "Sales Actual", "dax_expression": "SUM(Facts[Amount])"},
    {"measure_name": "Sales PY", "dax_expression": PY_DAX},
]


def _run(tmp_path, measures):
    yaml_path, mapping_path = _write_inputs(tmp_path, measures, DAX_ROWS)
    pipeline = MetricExpressionValidatorPipeline(table_mappings={"facts": "source"})
    return pipeline.run(
        metrics_view_yaml_path=yaml_path, table_mapping_json_path=mapping_path
    )


class TestPriorYearEndToEnd:
    def test_prior_year_with_window_is_not_invalid(self, tmp_path):
        result = _run(
            tmp_path,
            [{"name": "sales_py", "expr": "MEASURE(sales_actual)", "window": WINDOW}],
        )
        assert _status(result, "sales_py") == "VALID"

    def test_prior_year_without_window_is_invalid(self, tmp_path):
        result = _run(tmp_path, [{"name": "sales_py", "expr": "MEASURE(sales_actual)"}])
        assert _status(result, "sales_py") == "INVALID"

    def test_plain_aggregate_prior_year_without_window_is_not_valid_by_construction(
        self, tmp_path
    ):
        result = _run(tmp_path, [{"name": "sales_py", "expr": "SUM(source.amount)"}])
        assert _status(result, "sales_py") == "INVALID"

    def test_generated_name_matches_power_bi_name_with_spaces(self, tmp_path):
        # The DAX mapping keeps the Power BI name ("Sales PY"); the YAML uses snake_case.
        result = _run(
            tmp_path,
            [{"name": "sales_py", "expr": "MEASURE(sales_actual)", "window": WINDOW}],
        )
        assert _status(result, "sales_py") != "skipped"

    def test_window_measure_without_dax_is_still_skipped(self, tmp_path):
        yaml_path, mapping_path = _write_inputs(
            tmp_path,
            [{"name": "orders_py", "expr": "MEASURE(sales_actual)", "window": WINDOW}],
            [{"measure_name": "Sales Actual", "dax_expression": "SUM(Facts[Amount])"}],
        )
        pipeline = MetricExpressionValidatorPipeline(table_mappings={"facts": "source"})
        result = pipeline.run(
            metrics_view_yaml_path=yaml_path, table_mapping_json_path=mapping_path
        )
        assert _status(result, "orders_py") == "skipped"
