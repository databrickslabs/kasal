"""Tests for MetricViewDeployerTool."""

import json
from unittest.mock import MagicMock, patch

from src.services.tools.metric_view_deployer_tool import MetricViewDeployerTool


class TestMetricViewDeployerTool:
    def test_initialization(self):
        tool = MetricViewDeployerTool()
        assert tool.name == "Metric View Deployer"

    def test_dry_run(self):
        tool = MetricViewDeployerTool()
        yaml_specs = {"fact_test": "name: test_view\nsource: cat.sch.tbl"}
        sql_specs = {"fact_test": "CREATE METRIC VIEW test_view AS SELECT * FROM tbl;"}
        result = tool._run(
            yaml_specs_json=json.dumps(yaml_specs),
            sql_specs_json=json.dumps(sql_specs),
            dry_run=True,
        )
        data = json.loads(result)
        assert data["summary"]["dry_run"] is True
        assert data["summary"]["validated"] == 1
        assert data["deployment_results"]["fact_test"]["status"] == "validated"

    def test_deploy_without_credentials(self):
        tool = MetricViewDeployerTool()
        yaml_specs = {"fact_test": "name: test"}
        sql_specs = {"fact_test": "CREATE METRIC VIEW x;"}
        result = tool._run(
            yaml_specs_json=json.dumps(yaml_specs),
            sql_specs_json=json.dumps(sql_specs),
            dry_run=False,
        )
        data = json.loads(result)
        assert data["deployment_results"]["fact_test"]["status"] == "error"
        assert "required" in data["deployment_results"]["fact_test"]["message"]

    def test_empty_input(self):
        tool = MetricViewDeployerTool()
        result = tool._run(yaml_specs_json="{}", sql_specs_json="{}", dry_run=True)
        data = json.loads(result)
        assert "error" in data  # empty specs now returns an explicit error


# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

SAMPLE_YAML = (
    "name: fact_sales_uc_metric_view\n"
    "source: main.metrics.fact_sales\n"
    "dimensions:\n"
    "  - name: region\n"
    "    expr: region\n"
    "measures:\n"
    "  - name: total_revenue\n"
    "    expr: SUM(revenue)\n"
)

SAMPLE_UCMV_OUTPUT = json.dumps(
    {
        "yaml": {
            "fact_sales": SAMPLE_YAML,
            "fact_orders": "name: fact_orders_uc_metric_view\nsource: main.metrics.fact_orders\n",
        },
        "sql": {
            "fact_sales": "CREATE METRIC VIEW main.metrics.fact_sales_uc_metric_view ...",
            "fact_orders": "CREATE METRIC VIEW main.metrics.fact_orders_uc_metric_view ...",
        },
        "deployment_results": {},
    }
)


def _mock_auth(workspace_url="https://test.azuredatabricks.net"):
    auth = MagicMock()
    auth.workspace_url = workspace_url
    auth.get_headers.return_value = {"Authorization": "Bearer test-token"}
    return auth


def _mock_exec_sql(success=True, state="SUCCEEDED"):
    """Build a mock _execute_sql_sync that returns a controlled result."""
    if success:
        return {"success": True, "state": state}
    return {
        "success": False,
        "state": "FAILED",
        "error": "SQL execution failed",
        "http_status": 400,
    }


def _describe_result(measures, dimensions):
    """Build a mock _execute_sql_sync return for `DESCRIBE ... AS JSON`: the JSON
    document sits as a single string cell in result.data_array, with measure
    columns flagged is_measure=true."""
    cols = [{"name": d, "is_measure": False} for d in dimensions]
    cols += [{"name": m, "is_measure": True} for m in measures]
    doc = json.dumps({"columns": cols})
    return {
        "success": True,
        "state": "SUCCEEDED",
        "data": {"result": {"data_array": [[doc]]}},
    }


# ---------------------------------------------------------------------------
# Dry run tests
# ---------------------------------------------------------------------------


class TestDryRun:
    def test_dry_run_returns_validated_status(self):
        """With ucmv_output containing yaml specs, dry_run=True → status validated."""
        tool = MetricViewDeployerTool()
        result = tool._run(ucmv_output=SAMPLE_UCMV_OUTPUT, dry_run=True)
        data = json.loads(result)
        assert data["summary"]["dry_run"] is True
        assert data["deployment_results"]["fact_sales"]["status"] == "validated"
        assert data["deployment_results"]["fact_orders"]["status"] == "validated"

    def test_dry_run_validated_count(self):
        tool = MetricViewDeployerTool()
        result = tool._run(ucmv_output=SAMPLE_UCMV_OUTPUT, dry_run=True)
        data = json.loads(result)
        assert data["summary"]["validated"] == 2
        assert data["summary"]["total"] == 2

    def test_dry_run_includes_view_name(self):
        tool = MetricViewDeployerTool()
        result = tool._run(
            ucmv_output=SAMPLE_UCMV_OUTPUT,
            dry_run=True,
            catalog="mycat",
            schema_name="mysch",
        )
        data = json.loads(result)
        view_name = data["deployment_results"]["fact_sales"]["view_name"]
        assert "mycat" in view_name
        assert "mysch" in view_name
        assert "fact_sales" in view_name

    def test_dry_run_includes_yaml_lines(self):
        tool = MetricViewDeployerTool()
        result = tool._run(ucmv_output=SAMPLE_UCMV_OUTPUT, dry_run=True)
        data = json.loads(result)
        # SAMPLE_YAML has multiple lines
        assert data["deployment_results"]["fact_sales"]["yaml_lines"] > 1


# ---------------------------------------------------------------------------
# Priority: manual yaml_specs_json vs ucmv_output
# ---------------------------------------------------------------------------


class TestYamlSpecsPriority:
    def test_manual_yaml_specs_takes_priority_over_ucmv_output(self):
        """yaml_specs_json set AND ucmv_output set → yaml_specs_json wins."""
        manual_specs = json.dumps({"fact_custom": "name: custom_view\n"})
        tool = MetricViewDeployerTool()
        result = tool._run(
            yaml_specs_json=manual_specs,
            ucmv_output=SAMPLE_UCMV_OUTPUT,
            dry_run=True,
        )
        data = json.loads(result)
        # Only the manual key should appear, not the ucmv_output keys
        assert "fact_custom" in data["deployment_results"]
        assert "fact_sales" not in data["deployment_results"]
        assert "fact_orders" not in data["deployment_results"]

    def test_yaml_specs_json_used_alone(self):
        manual_specs = json.dumps({"fact_pe002": SAMPLE_YAML})
        tool = MetricViewDeployerTool()
        result = tool._run(yaml_specs_json=manual_specs, dry_run=True)
        data = json.loads(result)
        assert "fact_pe002" in data["deployment_results"]


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


class TestErrorCases:
    def test_empty_yaml_content_returns_error(self):
        """yaml dict has key with empty string value → error for that key."""
        yaml_specs = json.dumps({"fact_empty": ""})
        tool = MetricViewDeployerTool()
        # Not dry_run so it hits the empty YAML check
        result = tool._run(
            yaml_specs_json=yaml_specs,
            dry_run=False,
            warehouse_id="wh-123",
        )
        data = json.loads(result)
        # Authentication will fail without real credentials, but the key should exist
        assert "fact_empty" in data["deployment_results"]
        # Should be error — either empty content or auth error
        assert data["deployment_results"]["fact_empty"]["status"] == "error"

    def test_no_yaml_specs_returns_error(self):
        """No ucmv_output and no yaml_specs_json → error JSON."""
        tool = MetricViewDeployerTool()
        result = tool._run()
        data = json.loads(result)
        assert "error" in data
        assert "ucmv_output" in data["error"] or "specs" in data["error"]

    def test_warehouse_id_required_for_deployment(self):
        """No warehouse_id → error status for each view."""
        tool = MetricViewDeployerTool()
        result = tool._run(
            ucmv_output=SAMPLE_UCMV_OUTPUT, dry_run=False, warehouse_id=None
        )
        data = json.loads(result)
        for key in ("fact_sales", "fact_orders"):
            assert data["deployment_results"][key]["status"] == "error"
            assert "warehouse_id" in data["deployment_results"][key]["message"]

    def test_invalid_json_in_yaml_specs_json_returns_error(self):
        tool = MetricViewDeployerTool()
        result = tool._run(yaml_specs_json="not-valid-json{{{")
        data = json.loads(result)
        assert "error" in data

    def test_empty_yaml_specs_json_object_falls_through_to_ucmv(self):
        """yaml_specs_json='{}' is empty → falls through to ucmv_output."""
        tool = MetricViewDeployerTool()
        result = tool._run(
            yaml_specs_json="{}", ucmv_output=SAMPLE_UCMV_OUTPUT, dry_run=True
        )
        data = json.loads(result)
        # Should use ucmv_output since yaml_specs_json is empty
        assert "fact_sales" in data["deployment_results"]


# ---------------------------------------------------------------------------
# Catalog remapping
# ---------------------------------------------------------------------------


class TestCatalogRemap:
    def test_catalog_remap_replaces_old_catalog(self):
        """catalog_remap replaces old catalog name strings in YAML content."""
        yaml_with_old_cat = json.dumps(
            {"fact_pe002": "source: dc_datalake_prod_001.schema.fact_pe002\n"}
        )
        remap = json.dumps({"dc_datalake_prod_001": "david_test_metrics"})
        tool = MetricViewDeployerTool()
        result = tool._run(
            yaml_specs_json=yaml_with_old_cat,
            catalog_remap=remap,
            dry_run=True,
        )
        data = json.loads(result)
        # The yaml key in output should have remapped content
        assert "david_test_metrics" in data["yaml"]["fact_pe002"]
        assert "dc_datalake_prod_001" not in data["yaml"]["fact_pe002"]

    def test_catalog_remap_multiple_replacements(self):
        yaml_content = json.dumps(
            {"fact_x": "source: old_cat_1.s.t\nref: old_cat_2.s.t\n"}
        )
        remap = json.dumps({"old_cat_1": "new_cat_1", "old_cat_2": "new_cat_2"})
        tool = MetricViewDeployerTool()
        result = tool._run(
            yaml_specs_json=yaml_content, catalog_remap=remap, dry_run=True
        )
        data = json.loads(result)
        assert "new_cat_1" in data["yaml"]["fact_x"]
        assert "new_cat_2" in data["yaml"]["fact_x"]


# ---------------------------------------------------------------------------
# DDL generation
# ---------------------------------------------------------------------------


class TestDdlGeneration:
    def test_yaml_to_ddl_generates_correct_sql(self):
        """_yaml_to_ddl() produces CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML AS $$..."""
        tool = MetricViewDeployerTool()
        view_name = "cat.sch.fact_pe002"
        yaml_content = "name: test_view\nsource: cat.sch.fact_table\n"
        ddl = tool._yaml_to_ddl(yaml_content, view_name)
        assert "CREATE OR REPLACE VIEW cat.sch.fact_pe002" in ddl
        assert "WITH METRICS" in ddl
        assert "LANGUAGE YAML" in ddl
        assert "AS $$" in ddl
        assert "name: test_view" in ddl
        assert "$$" in ddl

    def test_yaml_to_ddl_strips_whitespace(self):
        tool = MetricViewDeployerTool()
        yaml_content = "  \nname: v\n  "
        ddl = tool._yaml_to_ddl(yaml_content, "cat.sch.v")
        # Content should be stripped
        assert ddl.count("$$") == 2

    def test_view_name_no_suffix(self):
        """key 'fact_pe002' → view name 'cat.sch.fact_pe002' (no _uc_metric_view appended)."""
        tool = MetricViewDeployerTool()
        result = tool._run(
            yaml_specs_json=json.dumps({"fact_pe002": SAMPLE_YAML}),
            catalog="cat",
            schema_name="sch",
            dry_run=True,
        )
        data = json.loads(result)
        view_name = data["deployment_results"]["fact_pe002"]["view_name"]
        assert view_name == "cat.sch.fact_pe002"
        # Ensure no suffix was appended
        assert "_uc_metric_view" not in view_name


# ---------------------------------------------------------------------------
# Deployment path (with mocked auth + execute_sql)
# ---------------------------------------------------------------------------


class TestDeployment:
    def _make_auth_mock(self, workspace_url="https://test.azuredatabricks.net"):
        return _mock_auth(workspace_url)

    def test_execute_sql_success_returns_deployed(self):
        """Mock _execute_sql_sync returns SUCCEEDED and _check_dangerous_sql passes → status deployed.

        The flow now runs three statements: CREATE SCHEMA, the metric-view DDL,
        then a DESCRIBE for the post-deploy schema guard rail (SAMPLE_YAML declares
        one dimension + one measure, so the DESCRIBE fires)."""
        tool = MetricViewDeployerTool()
        auth = self._make_auth_mock()

        with (
            patch.object(tool, "_authenticate", return_value=auth),
            patch(
                "src.services.tools.metric_view_utils.yaml_emitter._check_dangerous_sql",
                return_value=True,
            ),
            patch.object(
                tool,
                "_execute_sql_sync",
                side_effect=[
                    {"success": True, "state": "SUCCEEDED"},  # schema
                    {"success": True, "state": "SUCCEEDED"},  # DDL
                    _describe_result(  # DESCRIBE guard rail
                        measures=["total_revenue"], dimensions=["region"]
                    ),
                ],
            ),
        ):
            result = tool._run(
                yaml_specs_json=json.dumps({"fact_sales": SAMPLE_YAML}),
                warehouse_id="wh-123",
                dry_run=False,
            )

        data = json.loads(result)
        entry = data["deployment_results"]["fact_sales"]
        assert entry["status"] == "deployed"
        assert entry["schema_check"]["schema_verified"] is True

    def test_execute_sql_http_error_returns_error(self):
        """_execute_sql_sync returns HTTP 404 → status error."""
        tool = MetricViewDeployerTool()
        auth = self._make_auth_mock()

        with (
            patch.object(tool, "_authenticate", return_value=auth),
            patch.object(
                tool,
                "_execute_sql_sync",
                side_effect=[
                    {"success": True, "state": "SUCCEEDED"},  # schema creation succeeds
                    {
                        "success": False,
                        "http_status": 404,
                        "error": "Not Found",
                    },  # DDL fails
                ],
            ),
        ):
            result = tool._run(
                yaml_specs_json=json.dumps({"fact_sales": SAMPLE_YAML}),
                warehouse_id="wh-123",
                dry_run=False,
            )

        data = json.loads(result)
        assert data["deployment_results"]["fact_sales"]["status"] == "error"

    def test_schema_ensured_before_deployment(self):
        """CREATE SCHEMA IF NOT EXISTS called before CREATE VIEW."""
        tool = MetricViewDeployerTool()
        auth = self._make_auth_mock()
        calls = []

        def mock_exec(sql, workspace_url, warehouse_id, headers):
            calls.append(sql)
            return {"success": True, "state": "SUCCEEDED"}

        with (
            patch.object(tool, "_authenticate", return_value=auth),
            patch.object(tool, "_execute_sql_sync", side_effect=mock_exec),
        ):
            tool._run(
                yaml_specs_json=json.dumps({"fact_sales": SAMPLE_YAML}),
                warehouse_id="wh-123",
                catalog="mycat",
                schema_name="mysch",
                dry_run=False,
            )

        # First call should be schema creation
        assert len(calls) >= 1
        assert "CREATE SCHEMA IF NOT EXISTS" in calls[0]
        assert "mycat" in calls[0]
        assert "mysch" in calls[0]

    def test_ssrf_check_blocks_invalid_host(self):
        """Non-Databricks workspace URL → error status for each view."""
        tool = MetricViewDeployerTool()
        auth = _mock_auth(workspace_url="https://evil.example.com")

        with patch.object(tool, "_authenticate", return_value=auth):
            result = tool._run(
                yaml_specs_json=json.dumps({"fact_sales": SAMPLE_YAML}),
                warehouse_id="wh-123",
                dry_run=False,
            )

        data = json.loads(result)
        assert data["deployment_results"]["fact_sales"]["status"] == "error"
        assert "Untrusted" in data["deployment_results"]["fact_sales"]["message"]

    def test_output_contains_yaml_passthrough(self):
        """Output JSON always contains 'yaml' key for downstream flow injection."""
        tool = MetricViewDeployerTool()
        result = tool._run(ucmv_output=SAMPLE_UCMV_OUTPUT, dry_run=True)
        data = json.loads(result)
        assert "yaml" in data
        assert isinstance(data["yaml"], dict)


# ---------------------------------------------------------------------------
# Summary stats
# ---------------------------------------------------------------------------


class TestSummaryStats:
    def test_summary_total_matches_input(self):
        tool = MetricViewDeployerTool()
        result = tool._run(ucmv_output=SAMPLE_UCMV_OUTPUT, dry_run=True)
        data = json.loads(result)
        assert data["summary"]["total"] == 2

    def test_summary_errors_counted(self):
        tool = MetricViewDeployerTool()
        result = tool._run(
            ucmv_output=SAMPLE_UCMV_OUTPUT, dry_run=False, warehouse_id=None
        )
        data = json.loads(result)
        assert data["summary"]["errors"] == 2
        assert data["summary"]["deployed"] == 0


# ---------------------------------------------------------------------------
# Post-deploy schema guard rail (DESCRIBE vs YAML)
# ---------------------------------------------------------------------------


class TestDeclaredNamesFromYaml:
    def test_extracts_measures_and_dimensions_lowercased(self):
        m, d = MetricViewDeployerTool._declared_names_from_yaml(SAMPLE_YAML)
        assert m == {"total_revenue"}
        assert d == {"region"}

    def test_unparseable_yaml_returns_empty_sets(self):
        m, d = MetricViewDeployerTool._declared_names_from_yaml(":\n  bad: [unclosed")
        assert m == set() and d == set()

    def test_non_mapping_yaml_returns_empty_sets(self):
        m, d = MetricViewDeployerTool._declared_names_from_yaml("- just\n- a\n- list")
        assert m == set() and d == set()


class TestParseDescribeJsonColumns:
    def test_splits_measures_from_dimensions(self):
        data = _describe_result(
            measures=["Total_Revenue"], dimensions=["Region", "product"]
        )["data"]
        parsed = MetricViewDeployerTool._parse_describe_json_columns(data)
        assert parsed is not None
        measures, dims = parsed
        assert measures == {"total_revenue"}
        assert dims == {"region", "product"}

    def test_missing_columns_key_returns_none(self):
        assert (
            MetricViewDeployerTool._parse_describe_json_columns(
                {"result": {"data_array": [["{}"]]}}
            )
            is None
        )

    def test_empty_result_returns_none(self):
        assert (
            MetricViewDeployerTool._parse_describe_json_columns(
                {"result": {"data_array": []}}
            )
            is None
        )


class TestCompareSchema:
    def test_full_match_is_verified(self):
        out = MetricViewDeployerTool._compare_schema(
            {"a", "b"}, {"x"}, {"a", "b"}, {"x"}
        )
        assert out["schema_verified"] is True
        assert "truncated" not in out

    def test_missing_measure_flagged_not_truncated(self):
        out = MetricViewDeployerTool._compare_schema({"a", "b"}, {"x"}, {"a"}, {"x"})
        assert out["schema_verified"] is False
        assert out["missing_measures"] == ["b"]
        assert out.get("truncated") is not True

    def test_all_measures_missing_is_truncated(self):
        out = MetricViewDeployerTool._compare_schema({"a", "b"}, {"x"}, set(), {"x"})
        assert out["truncated"] is True
        assert out["deployed_measures"] == 0


class TestPostDeployGuardRail:
    def _auth(self):
        return _mock_auth()

    def _deploy(self, tool, describe_ret):
        with (
            patch.object(tool, "_authenticate", return_value=self._auth()),
            patch(
                "src.services.tools.metric_view_utils.yaml_emitter._check_dangerous_sql",
                return_value=True,
            ),
            patch.object(
                tool,
                "_execute_sql_sync",
                side_effect=[
                    {"success": True, "state": "SUCCEEDED"},  # schema
                    {"success": True, "state": "SUCCEEDED"},  # DDL
                    describe_ret,  # DESCRIBE
                ],
            ),
        ):
            return json.loads(
                tool._run(
                    yaml_specs_json=json.dumps({"fact_sales": SAMPLE_YAML}),
                    warehouse_id="wh-123",
                    dry_run=False,
                )
            )

    def test_truncated_view_flagged_deployed_incomplete(self):
        """DDL succeeds but DESCRIBE shows zero of the declared measures →
        the view is measureless; status must be deployed_incomplete."""
        tool = MetricViewDeployerTool()
        data = self._deploy(tool, _describe_result(measures=[], dimensions=["region"]))
        entry = data["deployment_results"]["fact_sales"]
        assert entry["status"] == "deployed_incomplete"
        assert entry["schema_check"]["truncated"] is True
        assert "truncated" in entry["message"].lower()
        assert data["summary"]["deployed_incomplete"] == 1
        assert data["summary"]["deployed"] == 0

    def test_complete_view_verified(self):
        tool = MetricViewDeployerTool()
        data = self._deploy(
            tool, _describe_result(measures=["total_revenue"], dimensions=["region"])
        )
        entry = data["deployment_results"]["fact_sales"]
        assert entry["status"] == "deployed"
        assert entry["schema_check"]["schema_verified"] is True

    def test_describe_failure_does_not_break_deploy(self):
        """A failed/garbled DESCRIBE must leave the successful deploy intact."""
        tool = MetricViewDeployerTool()
        data = self._deploy(tool, {"success": False, "state": "FAILED"})
        entry = data["deployment_results"]["fact_sales"]
        assert entry["status"] == "deployed"
        assert entry["schema_check"]["schema_verified"] is None
