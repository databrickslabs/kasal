"""Tests for metric_view_validation_utils.pipeline (MetricExpressionValidatorPipeline)."""
import json
import textwrap
import pytest

from src.engines.crewai.tools.custom.metric_view_validation_utils.pipeline import (
    MetricExpressionValidatorPipeline,
)

# ---------------------------------------------------------------------------
# Shared fixture data
# ---------------------------------------------------------------------------

SIMPLE_YAML = textwrap.dedent("""\
    measures:
      - name: total_sales
        expr: "SUM(source.amount)"
        comment: ""
      - name: unmatched
        expr: "SUM(source.x)"
        comment: ""
""")

SAMPLE_MAPPINGS = [
    {"measure_name": "total_sales", "dax_expression": "SUM(fact[amount])"},
]


def _write_files(tmp_path):
    yaml_file = tmp_path / "mv.yaml"
    yaml_file.write_text(SIMPLE_YAML)
    json_file = tmp_path / "mapping.json"
    json_file.write_text(json.dumps(SAMPLE_MAPPINGS))
    return str(yaml_file), str(json_file)


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

class TestInit:
    def test_defaults(self):
        p = MetricExpressionValidatorPipeline()
        assert p.table_mappings == {}
        assert p.column_mappings == {}

    def test_stores_mappings(self):
        p = MetricExpressionValidatorPipeline(
            table_mappings={"fact": "source"},
            column_mappings={"amt": "amount"},
        )
        assert p.table_mappings == {"fact": "source"}
        assert p.column_mappings == {"amt": "amount"}


# ---------------------------------------------------------------------------
# run() – missing parameters
# ---------------------------------------------------------------------------

class TestRunMissingParams:
    def test_no_params_returns_error(self):
        p = MetricExpressionValidatorPipeline()
        result = p.run()
        assert "error" in result

    def test_only_databricks_expr_returns_error(self):
        p = MetricExpressionValidatorPipeline()
        result = p.run(databricks_expr="SUM(source.amount)")
        assert "error" in result

    def test_only_dax_expr_returns_error(self):
        p = MetricExpressionValidatorPipeline()
        result = p.run(dax_expr="SUM(fact[amount])")
        assert "error" in result

    def test_only_yaml_path_returns_error(self, tmp_path):
        p = MetricExpressionValidatorPipeline()
        result = p.run(metrics_view_yaml_path=str(tmp_path / "mv.yaml"))
        assert "error" in result


# ---------------------------------------------------------------------------
# run() – direct validation mode
# ---------------------------------------------------------------------------

class TestRunDirect:
    def test_valid_pair_returns_is_valid(self):
        p = MetricExpressionValidatorPipeline(table_mappings={"fact": "source"})
        result = p.run(
            databricks_expr="SUM(source.amount)",
            dax_expr="SUM(fact[amount])",
        )
        assert "is_valid" in result
        assert result["is_valid"] is True

    def test_invalid_pair_returns_is_invalid(self):
        p = MetricExpressionValidatorPipeline(table_mappings={"fact": "source"})
        result = p.run(
            databricks_expr="SUM(source.amount)",
            dax_expr="COUNT(fact[order_id])",
        )
        assert result["is_valid"] is False

    def test_status_key_present(self):
        p = MetricExpressionValidatorPipeline()
        result = p.run(
            databricks_expr="SUM(source.amount)",
            dax_expr="SUM(T[amount])",
        )
        assert result["status"] in ("VALID", "INVALID")

    def test_measure_name_attached(self):
        p = MetricExpressionValidatorPipeline()
        result = p.run(
            databricks_expr="SUM(source.amount)",
            dax_expr="SUM(T[amount])",
            measure_name="my_measure",
        )
        assert result["measure_name"] == "my_measure"

    def test_strict_mode_accepted(self):
        p = MetricExpressionValidatorPipeline(table_mappings={"fact": "source"})
        result = p.run(
            databricks_expr="SUM(source.amount)",
            dax_expr="SUM(fact[amount])",
            strict_mode=True,
        )
        assert "is_valid" in result

    def test_call_level_table_mappings_override_instance(self):
        p = MetricExpressionValidatorPipeline(table_mappings={"old_fact": "source"})
        result = p.run(
            databricks_expr="SUM(source.amount)",
            dax_expr="SUM(new_fact[amount])",
            table_mappings={"new_fact": "source"},
        )
        assert result["is_valid"] is True

    def test_call_level_mappings_merged_not_replaced(self):
        """Instance mappings should still be present unless overridden by call-level."""
        p = MetricExpressionValidatorPipeline(table_mappings={"fact_a": "source_a"})
        result = p.run(
            databricks_expr="SUM(source_a.amount)",
            dax_expr="SUM(fact_a[amount])",
            table_mappings={"fact_b": "source_b"},   # adds, not replaces
        )
        assert result["is_valid"] is True


# ---------------------------------------------------------------------------
# run() – file-based validation mode
# ---------------------------------------------------------------------------

class TestRunFileBased:
    def test_returns_skipped_and_evaluated(self, tmp_path):
        yaml_path, json_path = _write_files(tmp_path)
        p = MetricExpressionValidatorPipeline(table_mappings={"fact": "source"})
        result = p.run(
            metrics_view_yaml_path=yaml_path,
            table_mapping_json_path=json_path,
        )
        assert "skipped" in result
        assert "evaluated" in result

    def test_nonexistent_yaml_returns_error(self, tmp_path):
        _, json_path = _write_files(tmp_path)
        p = MetricExpressionValidatorPipeline()
        result = p.run(
            metrics_view_yaml_path="/nonexistent/mv.yaml",
            table_mapping_json_path=json_path,
        )
        assert "error" in result

    def test_nonexistent_json_returns_error(self, tmp_path):
        # Use a complex expression that does NOT match the "simple" pattern so that
        # the validator actually tries to load the mapping JSON file.
        complex_yaml = textwrap.dedent("""\
            measures:
              - name: ratio
                expr: "SUM(source.a) / NULLIF(COUNT(source.b), 0)"
                comment: ""
        """)
        yaml_file = tmp_path / "complex.yaml"
        yaml_file.write_text(complex_yaml)
        p = MetricExpressionValidatorPipeline()
        result = p.run(
            metrics_view_yaml_path=str(yaml_file),
            table_mapping_json_path="/nonexistent/mapping.json",
        )
        assert "error" in result

    def test_file_based_takes_priority_over_direct(self, tmp_path):
        """When both file-based and direct params are provided, file-based wins."""
        yaml_path, json_path = _write_files(tmp_path)
        p = MetricExpressionValidatorPipeline()
        result = p.run(
            metrics_view_yaml_path=yaml_path,
            table_mapping_json_path=json_path,
            databricks_expr="SUM(source.amount)",
            dax_expr="SUM(fact[amount])",
        )
        # File-based mode → skipped/evaluated keys, not is_valid
        assert "skipped" in result or "evaluated" in result


# ---------------------------------------------------------------------------
# run_as_json()
# ---------------------------------------------------------------------------

class TestRunAsJson:
    def test_returns_string(self):
        p = MetricExpressionValidatorPipeline()
        result = p.run_as_json(
            databricks_expr="SUM(source.amount)",
            dax_expr="SUM(T[amount])",
        )
        assert isinstance(result, str)

    def test_valid_json(self):
        p = MetricExpressionValidatorPipeline()
        result = p.run_as_json(
            databricks_expr="SUM(source.amount)",
            dax_expr="SUM(T[amount])",
        )
        parsed = json.loads(result)
        assert isinstance(parsed, dict)

    def test_error_case_serialised(self):
        p = MetricExpressionValidatorPipeline()
        result = p.run_as_json()   # no params → error
        parsed = json.loads(result)
        assert "error" in parsed

    def test_file_based_serialised(self, tmp_path):
        yaml_path, json_path = _write_files(tmp_path)
        p = MetricExpressionValidatorPipeline()
        result = p.run_as_json(
            metrics_view_yaml_path=yaml_path,
            table_mapping_json_path=json_path,
        )
        parsed = json.loads(result)
        assert "skipped" in parsed or "evaluated" in parsed


# ---------------------------------------------------------------------------
# Mapping merge behaviour
# ---------------------------------------------------------------------------

class TestMappingMerge:
    def test_empty_instance_with_call_overrides(self):
        p = MetricExpressionValidatorPipeline()
        result = p.run(
            databricks_expr="SUM(source.amount)",
            dax_expr="SUM(fact[amount])",
            table_mappings={"fact": "source"},
        )
        assert result["is_valid"] is True

    def test_instance_overridden_by_call(self):
        # Instance says fact→wrong, call says fact→source (correct)
        p = MetricExpressionValidatorPipeline(table_mappings={"fact": "wrong"})
        result = p.run(
            databricks_expr="SUM(source.amount)",
            dax_expr="SUM(fact[amount])",
            table_mappings={"fact": "source"},
        )
        assert result["is_valid"] is True
