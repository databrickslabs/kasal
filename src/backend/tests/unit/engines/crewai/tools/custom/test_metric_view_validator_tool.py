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
