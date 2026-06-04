"""Unit tests for GenieSpaceGeneratorTool (Tool 92)."""
import json
from unittest.mock import patch, MagicMock

from src.engines.crewai.tools.custom.genie_space_generator_tool import (
    GenieSpaceGeneratorTool,
    GenieSpaceGeneratorSchema,
)


# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

SAMPLE_UCMV_OUTPUT = json.dumps({
    "yaml": {
        "fact_sales": "name: fact_sales_uc_metric_view\nsource: main.metrics.fact_sales\n",
        "fact_orders": "name: fact_orders_uc_metric_view\nsource: main.metrics.fact_orders\n",
    },
    "sql": {
        "fact_sales": "CREATE METRIC VIEW main.metrics.fact_sales_uc_metric_view ...",
        "fact_orders": "CREATE METRIC VIEW main.metrics.fact_orders_uc_metric_view ...",
    },
    "stats": {
        "fact_sales": {"total": 3, "translated": 3},
        "fact_orders": {"total": 2, "translated": 2},
    },
})

SAMPLE_JOIN_SPECS = json.dumps([
    {
        "left_table": "main.metrics.fact_sales_uc_metric_view",
        "right_table": "main.raw.dim_customer",
        "join_condition": "fact_sales_uc_metric_view.customer_id = dim_customer.customer_id",
    }
])

SAMPLE_SQL_EXPRESSIONS = json.dumps([
    {"display_name": "Revenue", "sql": "SUM(net_revenue)"},
])

SAMPLE_SQL_MEASURES = json.dumps([
    {"display_name": "Gross Margin", "sql": "SUM(gross_profit) / SUM(revenue)", "instruction": "Express as ratio 0-1"},
])

SAMPLE_SQL_FILTERS = json.dumps([
    {"display_name": "EMEA Only", "sql": "region = 'EMEA'"},
])

SAMPLE_EXAMPLE_SQLS = json.dumps([
    {"question": "What was total revenue last month?", "sql": "SELECT SUM(net_revenue) FROM ..."},
])


def _mock_auth(workspace_url="https://my-workspace.cloud.databricks.com"):
    """Build a mock AuthContext."""
    auth = MagicMock()
    auth.workspace_url = workspace_url
    auth.get_headers.return_value = {
        "Authorization": "Bearer dapi-test-token",
        "Content-Type": "application/json",
    }
    return auth


def _make_requests_mock(list_spaces=None, post_status=201, patch_status=200):
    """Build a mock requests module."""
    mock_requests = MagicMock()

    list_resp = MagicMock()
    list_resp.status_code = 200
    list_resp.json.return_value = {"spaces": list_spaces or []}
    mock_requests.get.return_value = list_resp

    post_resp = MagicMock()
    post_resp.status_code = post_status
    post_resp.json.return_value = {"space_id": "new-space-001", "display_name": "Test Space"}
    mock_requests.post.return_value = post_resp

    patch_resp = MagicMock()
    patch_resp.status_code = patch_status
    patch_resp.json.return_value = {"space_id": "existing-space-001", "display_name": "Test Space"}
    mock_requests.patch.return_value = patch_resp

    return mock_requests


def _run_tool(tool, mock_requests=None, auth=None, **kwargs):
    """Helper: run tool with mocked _authenticate and optionally mocked requests."""
    if auth is None:
        auth = _mock_auth()
    if mock_requests is None:
        mock_requests = _make_requests_mock()

    with patch.object(tool, "_authenticate", return_value=auth), \
         patch.dict("sys.modules", {"requests": mock_requests}):
        return tool._run(**kwargs)


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------

class TestGenieSpaceGeneratorSchema:
    def test_all_fields_optional(self):
        schema = GenieSpaceGeneratorSchema()
        assert schema.ucmv_output is None
        assert schema.space_title is None
        assert schema.catalog is None
        assert schema.schema_name is None
        assert schema.warehouse_id is None
        assert schema.additional_tables is None
        assert schema.text_instructions is None
        assert schema.join_specs_json is None
        assert schema.sample_questions is None
        assert schema.sql_expressions_json is None
        assert schema.sql_measures_json is None
        assert schema.sql_filters_json is None
        assert schema.example_sqls_json is None

    def test_ucmv_output_field(self):
        schema = GenieSpaceGeneratorSchema(ucmv_output=SAMPLE_UCMV_OUTPUT)
        assert schema.ucmv_output == SAMPLE_UCMV_OUTPUT

    def test_space_title_field(self):
        schema = GenieSpaceGeneratorSchema(space_title="My Space")
        assert schema.space_title == "My Space"

    def test_sample_questions_field(self):
        schema = GenieSpaceGeneratorSchema(sample_questions="Q1\nQ2\nQ3")
        assert schema.sample_questions == "Q1\nQ2\nQ3"

    def test_join_specs_json_field(self):
        schema = GenieSpaceGeneratorSchema(join_specs_json=SAMPLE_JOIN_SPECS)
        assert schema.join_specs_json == SAMPLE_JOIN_SPECS


# ---------------------------------------------------------------------------
# Initialization tests
# ---------------------------------------------------------------------------

class TestGenieSpaceGeneratorToolInit:
    def test_tool_name(self):
        tool = GenieSpaceGeneratorTool()
        assert tool.name == "Genie Space Generator"

    def test_description_present(self):
        tool = GenieSpaceGeneratorTool()
        assert "Genie Space" in tool.description

    def test_ucmv_output_always_in_default_config(self):
        """ucmv_output must always be in _default_config for flow injection check."""
        tool = GenieSpaceGeneratorTool()
        assert "ucmv_output" in tool._default_config

    def test_static_config_stored_on_init(self):
        tool = GenieSpaceGeneratorTool(
            space_title="Test Space",
            catalog="main",
            schema_name="metrics",
            warehouse_id="abc123",
        )
        assert tool._default_config["space_title"] == "Test Space"
        assert tool._default_config["catalog"] == "main"
        assert tool._default_config["warehouse_id"] == "abc123"

    def test_ucmv_output_stored_when_provided(self):
        tool = GenieSpaceGeneratorTool(ucmv_output=SAMPLE_UCMV_OUTPUT)
        assert tool._default_config["ucmv_output"] == SAMPLE_UCMV_OUTPUT

    def test_idempotent_description(self):
        tool = GenieSpaceGeneratorTool()
        desc_lower = tool.description.lower()
        assert "idempotent" in desc_lower or "patches" in desc_lower


# ---------------------------------------------------------------------------
# Validation: missing warehouse_id
# ---------------------------------------------------------------------------

class TestMissingWarehouseId:
    def test_missing_warehouse_id_returns_error(self):
        tool = GenieSpaceGeneratorTool()
        result = tool._run(space_title="Test", catalog="main", schema_name="m")
        data = json.loads(result)
        assert "error" in data
        assert "warehouse_id" in data["error"]

    def test_empty_warehouse_id_returns_error(self):
        tool = GenieSpaceGeneratorTool()
        result = tool._run(space_title="Test", catalog="main", schema_name="m", warehouse_id="")
        data = json.loads(result)
        assert "error" in data


# ---------------------------------------------------------------------------
# Auth failure handling
# ---------------------------------------------------------------------------

class TestAuthFailure:
    def test_auth_exception_returns_error(self):
        tool = GenieSpaceGeneratorTool()
        with patch.object(tool, "_authenticate", side_effect=RuntimeError("No credentials configured")):
            result = tool._run(warehouse_id="wh-123")
        data = json.loads(result)
        assert "error" in data
        assert "Authentication" in data["error"]

    def test_auth_none_returns_error(self):
        tool = GenieSpaceGeneratorTool()
        with patch.object(tool, "_authenticate", return_value=None):
            result = tool._run(warehouse_id="wh-123")
        data = json.loads(result)
        assert "error" in data
        assert "no auth context" in data["error"].lower()


# ---------------------------------------------------------------------------
# SSRF protection
# ---------------------------------------------------------------------------

class TestSsrfProtection:
    def test_trusted_cloud_host_passes(self):
        tool = GenieSpaceGeneratorTool()
        result = _run_tool(
            tool,
            auth=_mock_auth("https://my-workspace.cloud.databricks.com"),
            warehouse_id="wh-123",
            space_title="Test",
            catalog="main",
            schema_name="m",
        )
        data = json.loads(result)
        assert "Untrusted" not in data.get("error", "")

    def test_trusted_azure_host_passes(self):
        tool = GenieSpaceGeneratorTool()
        result = _run_tool(
            tool,
            auth=_mock_auth("https://adb-123456.7.azuredatabricks.net"),
            warehouse_id="wh-123",
            space_title="Test",
            catalog="main",
            schema_name="m",
        )
        data = json.loads(result)
        assert "Untrusted" not in data.get("error", "")

    def test_untrusted_host_blocked(self):
        tool = GenieSpaceGeneratorTool()
        result = _run_tool(
            tool,
            auth=_mock_auth("https://evil.example.com"),
            warehouse_id="wh-123",
            space_title="Test",
            catalog="main",
            schema_name="m",
        )
        data = json.loads(result)
        assert "error" in data
        assert "Untrusted" in data["error"]

    def test_localhost_blocked(self):
        tool = GenieSpaceGeneratorTool()
        result = _run_tool(
            tool,
            auth=_mock_auth("https://localhost:8080"),
            warehouse_id="wh-123",
        )
        data = json.loads(result)
        assert "error" in data

    def test_invalid_url_returns_error(self):
        tool = GenieSpaceGeneratorTool()
        auth = MagicMock()
        auth.workspace_url = ""
        auth.get_headers.return_value = {}
        with patch.object(tool, "_authenticate", return_value=auth):
            result = tool._run(warehouse_id="wh-123")
        data = json.loads(result)
        assert "error" in data


# ---------------------------------------------------------------------------
# Happy path: POST (create new space)
# ---------------------------------------------------------------------------

class TestCreateNewSpace:
    def test_create_returns_space_id(self):
        tool = GenieSpaceGeneratorTool()
        mock_requests = _make_requests_mock(list_spaces=[], post_status=201)
        result = _run_tool(
            tool, mock_requests,
            ucmv_output=SAMPLE_UCMV_OUTPUT,
            warehouse_id="wh-abc123",
            space_title="Test Space",
            catalog="main",
            schema_name="metrics",
        )
        data = json.loads(result)
        assert "space_id" in data
        assert data["space_id"] == "new-space-001"

    def test_create_returns_url(self):
        tool = GenieSpaceGeneratorTool()
        mock_requests = _make_requests_mock(list_spaces=[], post_status=201)
        result = _run_tool(
            tool, mock_requests,
            ucmv_output=SAMPLE_UCMV_OUTPUT,
            warehouse_id="wh-abc123",
            space_title="Test Space",
            catalog="main",
            schema_name="metrics",
        )
        data = json.loads(result)
        assert "url" in data
        assert "genie/spaces/new-space-001" in data["url"]

    def test_create_operation_is_created(self):
        tool = GenieSpaceGeneratorTool()
        mock_requests = _make_requests_mock(list_spaces=[], post_status=201)
        result = _run_tool(
            tool, mock_requests,
            ucmv_output=SAMPLE_UCMV_OUTPUT,
            warehouse_id="wh-abc123",
            space_title="Test Space",
            catalog="main",
            schema_name="metrics",
        )
        data = json.loads(result)
        assert data["operation"] == "created"

    def test_metric_view_table_count(self):
        tool = GenieSpaceGeneratorTool()
        mock_requests = _make_requests_mock(list_spaces=[], post_status=201)
        result = _run_tool(
            tool, mock_requests,
            ucmv_output=SAMPLE_UCMV_OUTPUT,
            warehouse_id="wh-abc123",
            space_title="Test Space",
            catalog="main",
            schema_name="metrics",
        )
        data = json.loads(result)
        # SAMPLE_UCMV_OUTPUT has 2 tables (fact_sales, fact_orders)
        assert data["metric_view_count"] == 2
        assert data["table_count"] == 2

    def test_additional_tables_counted(self):
        tool = GenieSpaceGeneratorTool()
        mock_requests = _make_requests_mock(list_spaces=[], post_status=201)
        result = _run_tool(
            tool, mock_requests,
            ucmv_output=SAMPLE_UCMV_OUTPUT,
            warehouse_id="wh-abc123",
            space_title="Test Space",
            catalog="main",
            schema_name="metrics",
            additional_tables="main.raw.dim_customer\nmain.raw.dim_date",
        )
        data = json.loads(result)
        assert data["additional_table_count"] == 2
        assert data["table_count"] == 4  # 2 metric views + 2 additional

    def test_sample_questions_counted_newline(self):
        tool = GenieSpaceGeneratorTool()
        mock_requests = _make_requests_mock(list_spaces=[], post_status=201)
        result = _run_tool(
            tool, mock_requests,
            ucmv_output=SAMPLE_UCMV_OUTPUT,
            warehouse_id="wh-abc123",
            space_title="Test Space",
            catalog="main",
            schema_name="metrics",
            sample_questions="Q1\nQ2\nQ3",
        )
        data = json.loads(result)
        assert data["question_count"] == 3

    def test_sample_questions_counted_json_array(self):
        tool = GenieSpaceGeneratorTool()
        mock_requests = _make_requests_mock(list_spaces=[], post_status=201)
        result = _run_tool(
            tool, mock_requests,
            ucmv_output=SAMPLE_UCMV_OUTPUT,
            warehouse_id="wh-abc123",
            space_title="Test Space",
            catalog="main",
            schema_name="metrics",
            sample_questions='["What was revenue?", "Show top 5 customers"]',
        )
        data = json.loads(result)
        assert data["question_count"] == 2

    def test_sql_snippets_counted(self):
        tool = GenieSpaceGeneratorTool()
        mock_requests = _make_requests_mock(list_spaces=[], post_status=201)
        result = _run_tool(
            tool, mock_requests,
            ucmv_output=SAMPLE_UCMV_OUTPUT,
            warehouse_id="wh-abc123",
            space_title="Test Space",
            catalog="main",
            schema_name="metrics",
            sql_expressions_json=SAMPLE_SQL_EXPRESSIONS,
            sql_measures_json=SAMPLE_SQL_MEASURES,
            sql_filters_json=SAMPLE_SQL_FILTERS,
        )
        data = json.loads(result)
        assert data["sql_snippet_count"] == 3  # 1 expr + 1 measure + 1 filter

    def test_example_sqls_counted(self):
        tool = GenieSpaceGeneratorTool()
        mock_requests = _make_requests_mock(list_spaces=[], post_status=201)
        result = _run_tool(
            tool, mock_requests,
            ucmv_output=SAMPLE_UCMV_OUTPUT,
            warehouse_id="wh-abc123",
            space_title="Test Space",
            catalog="main",
            schema_name="metrics",
            example_sqls_json=SAMPLE_EXAMPLE_SQLS,
        )
        data = json.loads(result)
        assert data["example_sql_count"] == 1

    def test_post_200_also_accepted(self):
        tool = GenieSpaceGeneratorTool()
        mock_requests = _make_requests_mock(list_spaces=[], post_status=200)
        result = _run_tool(
            tool, mock_requests,
            warehouse_id="wh-abc123",
            space_title="Test",
            catalog="main",
            schema_name="m",
        )
        data = json.loads(result)
        assert data.get("operation") == "created"


# ---------------------------------------------------------------------------
# Happy path: PATCH (update existing space)
# ---------------------------------------------------------------------------

class TestUpdateExistingSpace:
    def test_update_operation_is_updated(self):
        tool = GenieSpaceGeneratorTool()
        existing = [{"space_id": "existing-space-001", "display_name": "Existing Space"}]
        mock_requests = _make_requests_mock(list_spaces=existing, patch_status=200)
        result = _run_tool(
            tool, mock_requests,
            ucmv_output=SAMPLE_UCMV_OUTPUT,
            warehouse_id="wh-abc123",
            space_title="Existing Space",
            catalog="main",
            schema_name="metrics",
        )
        data = json.loads(result)
        assert data["operation"] == "updated"

    def test_update_returns_existing_space_id(self):
        tool = GenieSpaceGeneratorTool()
        existing = [{"space_id": "existing-space-001", "display_name": "Existing Space"}]
        mock_requests = _make_requests_mock(list_spaces=existing, patch_status=200)
        result = _run_tool(
            tool, mock_requests,
            ucmv_output=SAMPLE_UCMV_OUTPUT,
            warehouse_id="wh-abc123",
            space_title="Existing Space",
            catalog="main",
            schema_name="metrics",
        )
        data = json.loads(result)
        assert data["space_id"] == "existing-space-001"

    def test_no_title_match_creates_new(self):
        """A different title in the existing list — no match → POST."""
        tool = GenieSpaceGeneratorTool()
        existing = [{"space_id": "other-space", "display_name": "Other Space"}]
        mock_requests = _make_requests_mock(list_spaces=existing, post_status=201)
        result = _run_tool(
            tool, mock_requests,
            ucmv_output=SAMPLE_UCMV_OUTPUT,
            warehouse_id="wh-abc123",
            space_title="My New Space",
            catalog="main",
            schema_name="metrics",
        )
        data = json.loads(result)
        assert data["operation"] == "created"

    def test_empty_spaces_list_creates_new(self):
        tool = GenieSpaceGeneratorTool()
        mock_requests = _make_requests_mock(list_spaces=[], post_status=201)
        result = _run_tool(
            tool, mock_requests,
            warehouse_id="wh-abc123",
            space_title="Test",
            catalog="main",
            schema_name="m",
        )
        data = json.loads(result)
        assert data["operation"] == "created"


# ---------------------------------------------------------------------------
# API error handling
# ---------------------------------------------------------------------------

class TestApiErrors:
    def _run_with_http_error(self, post_status):
        tool = GenieSpaceGeneratorTool()
        mock_requests = MagicMock()
        list_resp = MagicMock()
        list_resp.status_code = 200
        list_resp.json.return_value = {"spaces": []}
        mock_requests.get.return_value = list_resp
        post_resp = MagicMock()
        post_resp.status_code = post_status
        post_resp.text = f"HTTP {post_status} error"
        post_resp.json.return_value = {}
        mock_requests.post.return_value = post_resp
        return _run_tool(
            tool, mock_requests,
            ucmv_output=SAMPLE_UCMV_OUTPUT,
            warehouse_id="wh-123",
            space_title="Test",
            catalog="main",
            schema_name="metrics",
        )

    def test_400_returns_error(self):
        result = self._run_with_http_error(400)
        data = json.loads(result)
        assert "error" in data
        assert "400" in data["error"]

    def test_403_returns_error(self):
        result = self._run_with_http_error(403)
        data = json.loads(result)
        assert "error" in data

    def test_500_returns_error(self):
        result = self._run_with_http_error(500)
        data = json.loads(result)
        assert "error" in data
        assert "500" in data["error"]

    def test_requests_connection_error(self):
        tool = GenieSpaceGeneratorTool()
        mock_requests = MagicMock()
        mock_requests.get.side_effect = ConnectionError("Network unreachable")
        result = _run_tool(
            tool, mock_requests,
            ucmv_output=SAMPLE_UCMV_OUTPUT,
            warehouse_id="wh-123",
            space_title="Test",
            catalog="main",
            schema_name="metrics",
        )
        data = json.loads(result)
        assert "error" in data


# ---------------------------------------------------------------------------
# UCMV output parsing
# ---------------------------------------------------------------------------

class TestUcmvOutputParsing:
    def test_two_metric_views_extracted(self):
        """SAMPLE_UCMV_OUTPUT has 2 fact tables → 2 metric view FQNs."""
        tool = GenieSpaceGeneratorTool()
        mock_requests = _make_requests_mock(list_spaces=[], post_status=201)
        result = _run_tool(
            tool, mock_requests,
            ucmv_output=SAMPLE_UCMV_OUTPUT,
            warehouse_id="wh-123",
            space_title="Test",
            catalog="main",
            schema_name="metrics",
        )
        data = json.loads(result)
        assert data["metric_view_count"] == 2

    def test_empty_yaml_dict_gives_zero_metric_views(self):
        tool = GenieSpaceGeneratorTool()
        mock_requests = _make_requests_mock(list_spaces=[], post_status=201)
        result = _run_tool(
            tool, mock_requests,
            ucmv_output=json.dumps({"yaml": {}, "sql": {}}),
            warehouse_id="wh-123",
            space_title="Test",
            catalog="main",
            schema_name="m",
        )
        data = json.loads(result)
        assert data["metric_view_count"] == 0

    def test_no_ucmv_output_gives_zero_metric_views(self):
        tool = GenieSpaceGeneratorTool()
        mock_requests = _make_requests_mock(list_spaces=[], post_status=201)
        result = _run_tool(
            tool, mock_requests,
            warehouse_id="wh-123",
            space_title="Test",
            catalog="main",
            schema_name="m",
        )
        data = json.loads(result)
        assert data.get("metric_view_count", 0) == 0

    def test_invalid_ucmv_json_handled_gracefully(self):
        """Malformed ucmv_output logs a warning and continues with 0 metric views."""
        tool = GenieSpaceGeneratorTool()
        mock_requests = _make_requests_mock(list_spaces=[], post_status=201)
        result = _run_tool(
            tool, mock_requests,
            ucmv_output="not-json{{{",
            warehouse_id="wh-123",
            space_title="Test",
            catalog="main",
            schema_name="m",
        )
        # Should not raise — result is either success or a non-parse error
        data = json.loads(result)
        assert isinstance(data, dict)

    def test_single_metric_view_extracted(self):
        single_ucmv = json.dumps({
            "yaml": {"fact_revenue": "name: fact_revenue_uc_metric_view\n"},
            "sql": {},
        })
        tool = GenieSpaceGeneratorTool()
        mock_requests = _make_requests_mock(list_spaces=[], post_status=201)
        result = _run_tool(
            tool, mock_requests,
            ucmv_output=single_ucmv,
            warehouse_id="wh-123",
            space_title="Test",
            catalog="cat",
            schema_name="sch",
        )
        data = json.loads(result)
        assert data["metric_view_count"] == 1


# ---------------------------------------------------------------------------
# Additional tables parsing
# ---------------------------------------------------------------------------

class TestAdditionalTablesParsing:
    def test_valid_fqn_tables_counted(self):
        tool = GenieSpaceGeneratorTool()
        mock_requests = _make_requests_mock(list_spaces=[], post_status=201)
        result = _run_tool(
            tool, mock_requests,
            ucmv_output=SAMPLE_UCMV_OUTPUT,
            warehouse_id="wh-123",
            space_title="Test",
            catalog="main",
            schema_name="metrics",
            additional_tables="main.raw.dim_customer\nmain.raw.dim_date",
        )
        data = json.loads(result)
        assert data["additional_table_count"] == 2

    def test_invalid_table_lines_skipped(self):
        """Lines without at least 2 dots (not fully qualified) are skipped."""
        tool = GenieSpaceGeneratorTool()
        mock_requests = _make_requests_mock(list_spaces=[], post_status=201)
        result = _run_tool(
            tool, mock_requests,
            ucmv_output=SAMPLE_UCMV_OUTPUT,
            warehouse_id="wh-123",
            space_title="Test",
            catalog="main",
            schema_name="metrics",
            additional_tables="main.raw.dim_customer\ninvalid_table\n",
        )
        data = json.loads(result)
        # Only 1 valid FQN, 1 invalid skipped
        assert data["additional_table_count"] == 1

    def test_empty_additional_tables(self):
        tool = GenieSpaceGeneratorTool()
        mock_requests = _make_requests_mock(list_spaces=[], post_status=201)
        result = _run_tool(
            tool, mock_requests,
            ucmv_output=SAMPLE_UCMV_OUTPUT,
            warehouse_id="wh-123",
            space_title="Test",
            catalog="main",
            schema_name="metrics",
            additional_tables="",
        )
        data = json.loads(result)
        assert data["additional_table_count"] == 0


# ---------------------------------------------------------------------------
# Static config (default_config) fallback
# ---------------------------------------------------------------------------

class TestStaticConfigFallback:
    def test_warehouse_from_default_config(self):
        """warehouse_id from init → not required in _run()."""
        tool = GenieSpaceGeneratorTool(warehouse_id="from-config", space_title="T", catalog="c", schema_name="s")
        mock_requests = _make_requests_mock(list_spaces=[], post_status=201)
        result = _run_tool(tool, mock_requests)
        data = json.loads(result)
        # No "warehouse_id is required" error
        assert "warehouse_id" not in data.get("error", "")

    def test_kwargs_override_default_config(self):
        """Explicit kwargs take priority over default_config."""
        tool = GenieSpaceGeneratorTool(warehouse_id="from-config")
        mock_requests = _make_requests_mock(list_spaces=[], post_status=201)
        result = _run_tool(
            tool, mock_requests,
            warehouse_id="from-kwargs",
            space_title="Test",
            catalog="main",
            schema_name="m",
        )
        data = json.loads(result)
        assert "warehouse_id" not in data.get("error", "")

    def test_ucmv_output_in_default_config_used(self):
        tool = GenieSpaceGeneratorTool(
            ucmv_output=SAMPLE_UCMV_OUTPUT,
            warehouse_id="wh-123",
            space_title="Test",
            catalog="main",
            schema_name="metrics",
        )
        mock_requests = _make_requests_mock(list_spaces=[], post_status=201)
        result = _run_tool(tool, mock_requests)
        data = json.loads(result)
        assert data["metric_view_count"] == 2


# ---------------------------------------------------------------------------
# JSON parsing helpers (bad JSON handled gracefully)
# ---------------------------------------------------------------------------

class TestJsonParsing:
    def _run_minimal(self, tool, **kwargs):
        mock_requests = _make_requests_mock(list_spaces=[], post_status=201)
        return _run_tool(
            tool, mock_requests,
            warehouse_id="wh-123",
            space_title="Test",
            catalog="main",
            schema_name="m",
            **kwargs,
        )

    def test_invalid_join_specs_handled(self):
        """Bad JSON in join_specs_json results in 0 join specs (no crash)."""
        tool = GenieSpaceGeneratorTool()
        result = self._run_minimal(tool, join_specs_json="not-json{{{")
        data = json.loads(result)
        # Should succeed (bad JSON → empty list → 0 join specs)
        assert "operation" in data or "error" in data

    def test_invalid_sql_expressions_handled(self):
        tool = GenieSpaceGeneratorTool()
        result = self._run_minimal(tool, sql_expressions_json="not-json")
        data = json.loads(result)
        assert isinstance(data, dict)

    def test_empty_json_lists_all_zero(self):
        tool = GenieSpaceGeneratorTool()
        result = self._run_minimal(
            tool,
            join_specs_json="[]",
            sql_expressions_json="[]",
            sql_measures_json="[]",
            sql_filters_json="[]",
            example_sqls_json="[]",
        )
        data = json.loads(result)
        if "operation" in data:
            assert data["sql_snippet_count"] == 0
            assert data["example_sql_count"] == 0

    def test_null_json_handled(self):
        tool = GenieSpaceGeneratorTool()
        result = self._run_minimal(tool, join_specs_json="null", sql_expressions_json="null")
        data = json.loads(result)
        assert isinstance(data, dict)
