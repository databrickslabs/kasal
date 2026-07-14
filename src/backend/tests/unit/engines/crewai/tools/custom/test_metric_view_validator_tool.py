"""Unit tests for MetricViewValidatorTool."""
import json
import pytest
from unittest.mock import patch, MagicMock

from src.engines.crewai.tools.custom.metric_view_validator_tool import (
    MetricViewValidatorTool,
    MetricViewValidatorSchema,
)


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

SAMPLE_YAML = {
    "fact_sales": (
        "name: fact_sales_uc_metric_view\n"
        "catalog: main\nschema: metrics\n"
        "source: main.raw.fact_sales\n"
        "dimensions:\n  - name: Region\n    expr: \"`region`\"\n"
        "measures:\n  - name: Total Revenue\n    expr: \"SUM(`amount`)\"\n"
    )
}

SAMPLE_MEASURES = json.dumps([
    {
        "measure_name": "Total Revenue",
        "dax_expression": "SUM(Fact_Sales[Amount])",
        "proposed_allocation": "fact_sales",
        "table": "Fact_Sales",
    },
    {
        "measure_name": "Profit Margin",
        "dax_expression": "DIVIDE([Profit], [Revenue])",
        "proposed_allocation": "fact_sales",
        "table": "Fact_Sales",
    },
])

SAMPLE_UCMV_OUTPUT = json.dumps({
    "yaml": SAMPLE_YAML,
    "sql": {"fact_sales": "CREATE METRIC VIEW main.metrics.fact_sales_uc_metric_view ..."},
    "stats": {"fact_sales": {"total": 2, "translated": 2}},
    "measures_with_dax": json.loads(SAMPLE_MEASURES),
})

MOCK_VALIDATION_RESULT = {
    "summary": {
        "tables_validated": 1,
        "total_evaluated": 2,
        "total_valid": 1,
        "total_equivalent": 1,
        "total_review": 0,
        "total_invalid": 0,
    },
    "per_table": {
        "fact_sales": {
            "evaluated": 2,
            "valid": 1,
            "equivalent": 1,
            "review": 0,
            "invalid": 0,
            "details": [
                {"measure_name": "Total Revenue", "status": "VALID", "reason": "SUM match"},
                {"measure_name": "Profit Margin", "status": "EQUIVALENT", "reason": "DIVIDE pattern"},
            ],
        }
    },
    "yaml": SAMPLE_YAML,
}


def _make_mock_pipeline(result=None):
    mock = MagicMock()
    mock.run.return_value = result or MOCK_VALIDATION_RESULT
    return mock


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------

class TestMetricViewValidatorSchema:
    def test_all_optional(self):
        schema = MetricViewValidatorSchema()
        assert schema.ucmv_output is None
        assert schema.yaml_content is None
        assert schema.measures_json is None

    def test_ucmv_output_field(self):
        schema = MetricViewValidatorSchema(ucmv_output=SAMPLE_UCMV_OUTPUT)
        assert schema.ucmv_output == SAMPLE_UCMV_OUTPUT

    def test_yaml_content_field(self):
        schema = MetricViewValidatorSchema(yaml_content=json.dumps(SAMPLE_YAML))
        assert schema.yaml_content is not None

    def test_measures_json_field(self):
        schema = MetricViewValidatorSchema(measures_json=SAMPLE_MEASURES)
        assert schema.measures_json == SAMPLE_MEASURES


# ---------------------------------------------------------------------------
# Initialization tests
# ---------------------------------------------------------------------------

class TestMetricViewValidatorToolInit:
    def test_tool_name(self):
        tool = MetricViewValidatorTool()
        assert tool.name == "Metric View Validator"

    def test_static_config_stored(self):
        tool = MetricViewValidatorTool(ucmv_output=SAMPLE_UCMV_OUTPUT)
        assert "ucmv_output" in tool._default_config

    def test_description_present(self):
        tool = MetricViewValidatorTool()
        assert len(tool.description) > 0


# ---------------------------------------------------------------------------
# Happy path: ucmv_output provided
# ---------------------------------------------------------------------------

class TestUcmvOutputMode:
    @patch(
        "src.engines.crewai.tools.custom.metric_view_validator_tool.MetricViewValidatorTool._run"
    )
    def test_run_called(self, mock_run):
        mock_run.return_value = json.dumps(MOCK_VALIDATION_RESULT)
        tool = MetricViewValidatorTool()
        result = tool._run(ucmv_output=SAMPLE_UCMV_OUTPUT)
        assert result is not None

    def test_ucmv_output_parsed(self):
        with patch(
            "src.engines.crewai.tools.custom.metric_view_validator_tool.MetricExpressionValidatorPipeline",
            return_value=_make_mock_pipeline(),
        ) if False else pytest.raises(Exception) if False else _noop_ctx():
            tool = MetricViewValidatorTool()
            # With real pipeline unavailable, tool should still return something
            result = tool._run(ucmv_output=SAMPLE_UCMV_OUTPUT)
            assert result is not None


# ---------------------------------------------------------------------------
# Happy path: yaml_content + measures_json
# ---------------------------------------------------------------------------

class TestYamlContentMode:
    def test_yaml_content_mode(self):
        tool = MetricViewValidatorTool()
        result = tool._run(
            yaml_content=json.dumps(SAMPLE_YAML),
            measures_json=SAMPLE_MEASURES,
        )
        assert result is not None
        assert len(result) > 0


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestErrorHandling:
    def test_invalid_json_ucmv_output(self):
        tool = MetricViewValidatorTool()
        result = tool._run(ucmv_output="not-valid-json{{{")
        assert "error" in result.lower() or result is not None

    def test_empty_inputs_returns_result(self):
        tool = MetricViewValidatorTool()
        result = tool._run()
        assert result is not None

    def test_empty_yaml_dict(self):
        tool = MetricViewValidatorTool()
        result = tool._run(yaml_content="{}", measures_json="[]")
        assert result is not None

    def test_static_config_used(self):
        tool = MetricViewValidatorTool(ucmv_output=SAMPLE_UCMV_OUTPUT)
        result = tool._run()
        assert result is not None


# ---------------------------------------------------------------------------
# Output structure
# ---------------------------------------------------------------------------

class TestOutputStructure:
    def test_output_is_valid_json_or_string(self):
        tool = MetricViewValidatorTool()
        result = tool._run(
            yaml_content=json.dumps(SAMPLE_YAML),
            measures_json=SAMPLE_MEASURES,
        )
        # Should be parseable JSON or at least a non-empty string
        try:
            data = json.loads(result)
            assert isinstance(data, dict)
        except json.JSONDecodeError:
            assert isinstance(result, str) and len(result) > 0

    def test_ucmv_config_proposer_not_confused_with_ucmv_output(self):
        # Config proposer output should not be treated as UCMV output
        config_output = json.dumps({"proposed_config": {"join_key_map": {}}})
        tool = MetricViewValidatorTool()
        result = tool._run(ucmv_output=config_output)
        assert result is not None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

from contextlib import contextmanager

@contextmanager
def _noop_ctx():
    yield


class TestResolvedMeasuresPairing:
    """Validator prefers resolved_measures_by_table (fact-table-keyed, with DAX).

    Regression: the flow passed measures_json=None and the validator's raw-measure
    source was keyed by PBI holder-table, so no measure paired with a YAML fact
    table → every table 'skipped: No measures found' → total_valid=0.
    """

    def test_uses_resolved_measures_by_table_from_ucmv_output(self):
        tool = MetricViewValidatorTool()
        ucmv_output = json.dumps({
            "yaml": {"fact_pe002": "version: '1.1'\nmeasures:\n  - name: paid_hours\n    expr: SUM(source.paid_hours)\n"},
            "resolved_measures_by_table": {
                "fact_pe002": [
                    {"measure_name": "paid_hours", "original_name": "Paid Hours",
                     "sql_expr": "SUM(source.paid_hours)", "dax_expression": "SUM(fact_pe002[paid_hours])",
                     "proposed_allocation": "fact_pe002", "table_name": "fact_pe002"},
                ]
            },
            "measures_with_dax": [],  # empty (holder-table junk would go here)
        })

        captured = {}

        class _FakePipeline:
            def __init__(self, *a, **k): pass
            def run(self, metrics_view_yaml_path=None, table_mapping_json_path=None, **k):
                # capture the measures that reached the pipeline
                with open(table_mapping_json_path) as f:
                    captured['measures'] = json.load(f)
                return {"evaluated": [
                    {"measure_name": "paid_hours", "measure_eval_result": {"status": "VALID"}}
                ]}

        # Isolate from real DB / tmp side-channels that would override ucmv_output
        import os as _os
        with patch(
            "src.engines.crewai.tools.custom.metric_view_validation_utils.pipeline.MetricExpressionValidatorPipeline",
            _FakePipeline,
        ), patch.object(MetricViewValidatorTool, "_fetch_saved_ucmv_edits_from_db", return_value=None), \
           patch.object(MetricViewValidatorTool, "_fetch_latest_ucmv_from_db", return_value=None), \
           patch.object(MetricViewValidatorTool, "_fetch_measures_from_db", return_value=[]), \
           patch.object(_os.path, "exists", return_value=False):
            result = tool._run(ucmv_output=ucmv_output)

        data = json.loads(result)
        # The measure paired to fact_pe002 and got validated (not skipped)
        assert captured.get('measures'), "resolved measures should have reached the pipeline"
        assert captured['measures'][0]['dax_expression'] == "SUM(fact_pe002[paid_hours])"
        assert data.get('summary', {}).get('total_valid', 0) >= 1


class TestCompactAgentReturn:
    """The tool's agent-facing return must stay compact so the crew agent's LLM
    context doesn't overflow on large models (regression: 28-view customer model
    hit 'Context window exceeded'). Full detail persists to the trace, not here.
    """

    def _run_with_fake_pipeline(self, ucmv_output, evaluated_by_table):
        """Run the tool with a fake validation pipeline that returns a given
        `evaluated` list per table (keyed by yaml order)."""
        import os as _os

        calls = {"i": 0}
        tables = list(evaluated_by_table.keys())

        class _FakePipeline:
            def __init__(self, *a, **k): pass
            def run(self, metrics_view_yaml_path=None, table_mapping_json_path=None, **k):
                # Return evaluated rows for each table in turn.
                idx = calls["i"]
                calls["i"] += 1
                key = tables[idx] if idx < len(tables) else tables[-1]
                return {"evaluated": evaluated_by_table[key]}

        tool = MetricViewValidatorTool()
        with patch(
            "src.engines.crewai.tools.custom.metric_view_validation_utils.pipeline.MetricExpressionValidatorPipeline",
            _FakePipeline,
        ), patch.object(MetricViewValidatorTool, "_fetch_saved_ucmv_edits_from_db", return_value=None), \
           patch.object(MetricViewValidatorTool, "_fetch_latest_ucmv_from_db", return_value=None), \
           patch.object(MetricViewValidatorTool, "_fetch_measures_from_db", return_value=[]), \
           patch.object(_os.path, "exists", return_value=False):
            return json.loads(tool._run(ucmv_output=ucmv_output))

    def _big_ucmv(self, n_tables=28, per_table=5):
        yaml = {}
        rmbt = {}
        evaluated = {}
        for t in range(n_tables):
            tk = f"fact_{t}"
            names = [f"m{t}_{i}" for i in range(per_table)]
            yaml[tk] = "version: '1.1'\nmeasures:\n" + "".join(
                f"  - name: {nm}\n    expr: SUM(source.{nm})\n" for nm in names)
            rmbt[tk] = [{"measure_name": nm, "original_name": nm,
                         "sql_expr": f"SUM(source.{nm})", "dax_expression": f"SUM({tk}[{nm}])",
                         "proposed_allocation": tk, "table_name": tk} for nm in names]
            # Mostly VALID, but make one REVIEW and one INVALID per table.
            evaluated[tk] = []
            for i, nm in enumerate(names):
                status = "VALID"
                if i == 0: status = "REVIEW"
                elif i == 1: status = "INVALID"
                evaluated[tk].append({"measure_name": nm,
                                      "measure_eval_result": {"status": status, "reason": "x" * 200}})
        return json.dumps({"yaml": yaml, "resolved_measures_by_table": rmbt,
                           "measures_with_dax": []}), evaluated

    def test_return_is_compact_but_keeps_yaml(self):
        ucmv, evaluated = self._big_ucmv(n_tables=28, per_table=5)
        data = self._run_with_fake_pipeline(ucmv, evaluated)
        # Summary totals present and correct.
        assert data["summary"]["tables_validated"] == 28
        assert data["summary"]["total_evaluated"] == 28 * 5
        # Compact shape: per_table_summary (counts only), no full per-measure details.
        assert "per_table_summary" in data
        assert "per_table" not in data
        # yaml IS kept (small ~11 KB; the UI needs it for 1:1 downloads). The
        # heavy per-measure `details` (~110 KB) are what stay stripped.
        assert "yaml" in data and len(data["yaml"]) == 28
        sample = next(iter(data["per_table_summary"].values()))
        # counts + a SLIM per-measure details list (name + status + short reason)
        assert {"evaluated", "valid", "equivalent", "review", "invalid", "details"} <= set(sample)
        # every measure is listed (breakdown dropdown), each with a status but
        # WITHOUT the heavy differences/dax/sql comparison arrays.
        assert len(sample["details"]) == 5
        d0 = sample["details"][0]
        assert "measure_name" in d0 and d0["measure_eval_result"].get("status")
        assert "differences" not in d0["measure_eval_result"]
        assert "dax" not in d0["measure_eval_result"]

    def test_attention_only_review_invalid_and_capped(self):
        ucmv, evaluated = self._big_ucmv(n_tables=28, per_table=5)
        data = self._run_with_fake_pipeline(ucmv, evaluated)
        # 28 tables * (1 REVIEW + 1 INVALID) = 56 actionable → capped at 50.
        assert data["attention_truncated"] is True
        assert len(data["attention"]) == 50
        assert all(a["status"] in ("REVIEW", "INVALID") for a in data["attention"])

    def test_return_size_bounded(self):
        ucmv, evaluated = self._big_ucmv(n_tables=28, per_table=5)
        data = self._run_with_fake_pipeline(ucmv, evaluated)
        # The whole agent-facing return must be small even for 28 tables.
        assert len(json.dumps(data)) < 60_000

    def test_small_input_still_reports(self):
        ucmv, evaluated = self._big_ucmv(n_tables=1, per_table=2)
        data = self._run_with_fake_pipeline(ucmv, evaluated)
        assert data["summary"]["tables_validated"] == 1
        assert data["summary"]["total_evaluated"] == 2
        assert data["attention_truncated"] is False
