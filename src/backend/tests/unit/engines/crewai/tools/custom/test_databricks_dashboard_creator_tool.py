"""Unit tests for DatabricksDashboardCreatorTool (Tool 95)."""
import json
from unittest.mock import MagicMock, patch

import pytest

from src.engines.crewai.tools.custom.databricks_dashboard_creator_tool import (
    DatabricksDashboardCreatorTool,
    DatabricksDashboardCreatorSchema,
)


# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

SAMPLE_VISUAL_MAPPINGS = [
    {
        "visual_id": "v001",
        "page_name": "Sales Overview",
        "visual_type": "barChart",
        "chart_title": "Revenue by Region",
        "ucmv_view": "main.metrics.fact_sales",
        "dimensions": ["region"],
        "measures": ["total_revenue"],
        "sql": "SELECT region, MEASURE(total_revenue) FROM main.metrics.fact_sales GROUP BY region",
    },
    {
        "visual_id": "v002",
        "page_name": "Sales Overview",
        "visual_type": "tableEx",
        "chart_title": "Sales Table",
        "ucmv_view": "main.metrics.fact_sales",
        "dimensions": ["region", "product"],
        "measures": ["total_revenue"],
        "sql": "SELECT region, product, MEASURE(total_revenue) FROM main.metrics.fact_sales GROUP BY region, product",
    },
    {
        "visual_id": "v003",
        "page_name": "KPIs",
        "visual_type": "card",
        "chart_title": "Total Revenue KPI",
        "ucmv_view": "main.metrics.fact_orders",
        "dimensions": [],
        "measures": ["total_revenue"],
        "sql": "SELECT MEASURE(total_revenue) FROM main.metrics.fact_orders",
    },
]

SAMPLE_TOOL94_OUTPUT = json.dumps({
    "visual_mappings": SAMPLE_VISUAL_MAPPINGS,
    "dashboard_title": "My PBI Dashboard",
    "catalog": "main",
    "schema_name": "metrics",
    "summary": {
        "total_visuals": 3,
        "mapped": 3,
        "unmapped": 0,
    },
})

SAMPLE_TOOL94_OUTPUT_TWO_VIEWS = json.dumps({
    "visual_mappings": [
        {
            "visual_id": "v001",
            "page_name": "Page1",
            "visual_type": "barChart",
            "chart_title": "Chart A",
            "ucmv_view": "main.metrics.view_a",
            "dimensions": ["dim1"],
            "measures": ["m1"],
            "sql": "SELECT dim1, MEASURE(m1) FROM main.metrics.view_a GROUP BY dim1",
        },
        {
            "visual_id": "v002",
            "page_name": "Page1",
            "visual_type": "tableEx",
            "chart_title": "Table B",
            "ucmv_view": "main.metrics.view_b",
            "dimensions": ["dim2"],
            "measures": ["m2"],
            "sql": "SELECT dim2, MEASURE(m2) FROM main.metrics.view_b GROUP BY dim2",
        },
        {
            "visual_id": "v003",
            "page_name": "Page1",
            "visual_type": "lineChart",
            "chart_title": "Line C",
            "ucmv_view": "main.metrics.view_a",  # same view as v001
            "dimensions": ["dim3"],
            "measures": ["m3"],
            "sql": "SELECT dim3, MEASURE(m3) FROM main.metrics.view_a GROUP BY dim3",
        },
    ],
    "dashboard_title": "Two Views Dashboard",
    "catalog": "main",
    "schema_name": "metrics",
})


def _mock_auth(workspace_url="https://test.azuredatabricks.net"):
    auth = MagicMock()
    auth.workspace_url = workspace_url
    auth.get_headers.return_value = {"Authorization": "Bearer test-token"}
    return auth


def _make_requests_mock(list_status=200, post_status=201, dashboard_id="dash-001"):
    """Build a mock requests module."""
    mock_req = MagicMock()

    list_resp = MagicMock()
    list_resp.status_code = list_status
    list_resp.json.return_value = {"dashboards": []}
    list_resp.text = ""

    create_resp = MagicMock()
    create_resp.status_code = post_status
    create_resp.json.return_value = {"dashboard_id": dashboard_id}
    create_resp.text = ""

    pub_resp = MagicMock()
    pub_resp.status_code = 200
    pub_resp.json.return_value = {}
    pub_resp.text = ""

    mock_req.request.side_effect = [list_resp, create_resp, pub_resp]
    return mock_req


def _run_tool(tool, auth=None, mock_requests=None, **kwargs):
    """Helper to run tool with mocked _authenticate and optionally mocked requests."""
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

class TestDatabricksDashboardCreatorSchema:
    def test_all_fields_optional(self):
        schema = DatabricksDashboardCreatorSchema()
        assert schema.visual_mappings_json is None
        assert schema.dashboard_title is None
        assert schema.catalog is None
        assert schema.schema_name is None
        assert schema.warehouse_id is None
        assert schema.databricks_host is None
        assert schema.parent_path is None
        assert schema.publish_dashboard is None

    def test_fields_accepted(self):
        schema = DatabricksDashboardCreatorSchema(
            dashboard_title="Test Dashboard",
            warehouse_id="wh-123",
            publish_dashboard=False,
        )
        assert schema.dashboard_title == "Test Dashboard"
        assert schema.warehouse_id == "wh-123"
        assert schema.publish_dashboard is False


# ---------------------------------------------------------------------------
# Tool initialization
# ---------------------------------------------------------------------------

class TestToolInit:
    def test_tool_name(self):
        tool = DatabricksDashboardCreatorTool()
        assert tool.name == "Databricks Dashboard Creator"

    def test_description_present(self):
        tool = DatabricksDashboardCreatorTool()
        assert "dashboard" in tool.description.lower() or "lakeview" in tool.description.lower()

    def test_visual_mappings_json_in_default_config(self):
        tool = DatabricksDashboardCreatorTool()
        assert "visual_mappings_json" in tool._default_config

    def test_static_config_stored_on_init(self):
        tool = DatabricksDashboardCreatorTool(
            dashboard_title="My Dashboard",
            warehouse_id="wh-abc",
            parent_path="/Workspace/Users/me",
        )
        assert tool._default_config["dashboard_title"] == "My Dashboard"
        assert tool._default_config["warehouse_id"] == "wh-abc"
        assert tool._default_config["parent_path"] == "/Workspace/Users/me"


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------

class TestErrorCases:
    def test_no_visual_mappings_returns_error(self):
        """Empty/None visual_mappings → error."""
        tool = DatabricksDashboardCreatorTool()
        result = tool._run()
        data = json.loads(result)
        assert "error" in data
        assert "visual_mappings" in data["error"]

    def test_no_mapped_visuals_with_sql_returns_error(self):
        """visual_mappings with no ucmv_view/sql → error."""
        unmapped = json.dumps([
            {
                "visual_id": "v1",
                "page_name": "Page1",
                "visual_type": "barChart",
                "ucmv_view": None,
                "sql": None,
            }
        ])
        tool = DatabricksDashboardCreatorTool()
        auth = _mock_auth()
        with patch.object(tool, "_authenticate", return_value=auth):
            result = tool._run(visual_mappings_json=unmapped)
        data = json.loads(result)
        assert "error" in data

    def test_auth_failure_returns_error(self):
        tool = DatabricksDashboardCreatorTool()
        with patch.object(tool, "_authenticate", side_effect=RuntimeError("No auth")):
            result = tool._run(
                visual_mappings_json=SAMPLE_TOOL94_OUTPUT,
                warehouse_id="wh-123",
            )
        data = json.loads(result)
        assert "error" in data


# ---------------------------------------------------------------------------
# Visual mappings parsing
# ---------------------------------------------------------------------------

class TestParseVisualMappings:
    def test_parses_visual_mappings_from_dict_output(self):
        """Input has {'visual_mappings': [...]} wrapper → extracted."""
        tool = DatabricksDashboardCreatorTool()
        mappings, title, catalog, schema = tool._parse_visual_mappings(SAMPLE_TOOL94_OUTPUT)
        assert len(mappings) == 3
        assert title == "My PBI Dashboard"
        assert catalog == "main"
        assert schema == "metrics"

    def test_parses_visual_mappings_from_list(self):
        """Input is raw list → used directly."""
        tool = DatabricksDashboardCreatorTool()
        raw_list = json.dumps(SAMPLE_VISUAL_MAPPINGS)
        mappings, title, catalog, schema = tool._parse_visual_mappings(raw_list)
        assert len(mappings) == 3
        # No title/catalog/schema when raw list
        assert title == ""
        assert catalog == ""

    def test_parse_none_returns_empty(self):
        tool = DatabricksDashboardCreatorTool()
        mappings, title, catalog, schema = tool._parse_visual_mappings(None)
        assert mappings == []

    def test_parse_invalid_json_returns_empty(self):
        tool = DatabricksDashboardCreatorTool()
        mappings, title, catalog, schema = tool._parse_visual_mappings("not-json{{{")
        assert mappings == []


# ---------------------------------------------------------------------------
# Widget type mapping
# ---------------------------------------------------------------------------

class TestWidgetTypeMapping:
    def test_widget_type_bar_chart(self):
        """visual_type 'barChart' → widgetType 'bar', version 3."""
        tool = DatabricksDashboardCreatorTool()
        widget_type, version = tool._widget_type_for_visual("barChart")
        assert widget_type == "bar"
        assert version == 3

    def test_widget_type_table(self):
        """visual_type 'tableEx' → widgetType 'table', version 2."""
        tool = DatabricksDashboardCreatorTool()
        widget_type, version = tool._widget_type_for_visual("tableEx")
        assert widget_type == "table"
        assert version == 2

    def test_widget_type_counter(self):
        """visual_type 'card' → widgetType 'counter', version 2."""
        tool = DatabricksDashboardCreatorTool()
        widget_type, version = tool._widget_type_for_visual("card")
        assert widget_type == "counter"
        assert version == 2

    def test_widget_type_line(self):
        """visual_type 'lineChart' → widgetType 'line', version 3."""
        tool = DatabricksDashboardCreatorTool()
        widget_type, version = tool._widget_type_for_visual("lineChart")
        assert widget_type == "line"
        assert version == 3

    def test_widget_type_kpi(self):
        """visual_type 'kpiVisual' → widgetType 'counter', version 2."""
        tool = DatabricksDashboardCreatorTool()
        widget_type, version = tool._widget_type_for_visual("kpiVisual")
        assert widget_type == "counter"
        assert version == 2

    def test_widget_type_matrix(self):
        """visual_type 'matrix' → widgetType 'table', version 2."""
        tool = DatabricksDashboardCreatorTool()
        widget_type, version = tool._widget_type_for_visual("matrix")
        assert widget_type == "table"
        assert version == 2

    def test_widget_type_unknown_defaults_to_table(self):
        """Unknown visual_type → ('table', 2)."""
        tool = DatabricksDashboardCreatorTool()
        widget_type, version = tool._widget_type_for_visual("unknownType")
        assert widget_type == "table"
        assert version == 2


# ---------------------------------------------------------------------------
# Lakeview JSON building
# ---------------------------------------------------------------------------

class TestLakeviewJsonBuilding:
    def test_lakeview_json_has_datasets(self):
        """_build_lakeview_json() → serialized JSON has 'datasets' key."""
        tool = DatabricksDashboardCreatorTool()
        result = tool._build_lakeview_json(SAMPLE_VISUAL_MAPPINGS, "wh-123")
        assert "datasets" in result
        assert isinstance(result["datasets"], list)

    def test_lakeview_json_has_pages(self):
        """_build_lakeview_json() → JSON has 'pages' key."""
        tool = DatabricksDashboardCreatorTool()
        result = tool._build_lakeview_json(SAMPLE_VISUAL_MAPPINGS, "wh-123")
        assert "pages" in result
        assert isinstance(result["pages"], list)
        assert len(result["pages"]) >= 1

    def test_one_dataset_per_unique_view(self):
        """3 visuals using 2 views → 2 datasets."""
        tool = DatabricksDashboardCreatorTool()
        raw_data = json.loads(SAMPLE_TOOL94_OUTPUT_TWO_VIEWS)
        mappings = raw_data["visual_mappings"]
        result = tool._build_lakeview_json(mappings, "wh-123")
        # view_a appears twice, view_b once → 2 unique datasets
        assert len(result["datasets"]) == 2

    def test_datasets_have_required_keys(self):
        tool = DatabricksDashboardCreatorTool()
        result = tool._build_lakeview_json(SAMPLE_VISUAL_MAPPINGS, "wh-123")
        for ds in result["datasets"]:
            assert "name" in ds
            assert "query" in ds

    def test_pages_grouped_by_page_name(self):
        """Visuals from same page_name are on the same page."""
        tool = DatabricksDashboardCreatorTool()
        result = tool._build_lakeview_json(SAMPLE_VISUAL_MAPPINGS, "wh-123")
        page_names = {p["display_name"] for p in result["pages"]}
        assert "Sales Overview" in page_names
        assert "KPIs" in page_names

    def test_serialized_json_is_valid(self):
        """_build_lakeview_json output can be serialized to valid JSON string."""
        tool = DatabricksDashboardCreatorTool()
        result = tool._build_lakeview_json(SAMPLE_VISUAL_MAPPINGS, "wh-123")
        serialized = json.dumps(result)
        parsed_back = json.loads(serialized)
        assert "datasets" in parsed_back
        assert "pages" in parsed_back


# ---------------------------------------------------------------------------
# Dashboard creation via API
# ---------------------------------------------------------------------------

class TestDashboardCreation:
    def test_dashboard_created_via_api(self):
        """mock requests → dashboard_id returned in output."""
        tool = DatabricksDashboardCreatorTool()
        auth = _mock_auth()
        mock_req = _make_requests_mock(dashboard_id="dash-abc-123")

        with patch.object(tool, "_authenticate", return_value=auth), \
             patch("src.engines.crewai.tools.custom.databricks_dashboard_creator_tool._requests", mock_req, create=True):
            # Patch _api_request directly to avoid import complexity
            with patch.object(
                tool, "_api_request",
                side_effect=[
                    (200, {"dashboards": []}),           # list existing
                    (201, {"dashboard_id": "dash-abc-123"}),  # create
                    (200, {}),                            # publish
                ]
            ):
                result = tool._run(
                    visual_mappings_json=SAMPLE_TOOL94_OUTPUT,
                    warehouse_id="wh-123",
                )

        data = json.loads(result)
        assert "dashboard_id" in data
        assert data["dashboard_id"] == "dash-abc-123"

    def test_dashboard_created_returns_url(self):
        """Dashboard URL contains the dashboard_id."""
        tool = DatabricksDashboardCreatorTool()
        auth = _mock_auth()

        with patch.object(tool, "_authenticate", return_value=auth), \
             patch.object(
                 tool, "_api_request",
                 side_effect=[
                     (200, {"dashboards": []}),
                     (201, {"dashboard_id": "dash-xyz"}),
                     (200, {}),
                 ]
             ):
            result = tool._run(
                visual_mappings_json=SAMPLE_TOOL94_OUTPUT,
                warehouse_id="wh-123",
            )

        data = json.loads(result)
        assert "dashboard_url" in data
        assert "dash-xyz" in data["dashboard_url"]

    def test_dashboard_update_when_exists(self):
        """If dashboard with same name exists, PUT is used (updated status)."""
        tool = DatabricksDashboardCreatorTool()
        auth = _mock_auth()
        existing = [{"display_name": "My PBI Dashboard", "dashboard_id": "existing-001"}]

        with patch.object(tool, "_authenticate", return_value=auth), \
             patch.object(
                 tool, "_api_request",
                 side_effect=[
                     (200, {"dashboards": existing}),
                     (200, {"dashboard_id": "existing-001"}),
                     (200, {}),
                 ]
             ):
            result = tool._run(
                visual_mappings_json=SAMPLE_TOOL94_OUTPUT,
                warehouse_id="wh-123",
            )

        data = json.loads(result)
        assert data["status"] == "updated"

    def test_api_error_returns_error(self):
        """Dashboard API returns 500 → error in output."""
        tool = DatabricksDashboardCreatorTool()
        auth = _mock_auth()

        with patch.object(tool, "_authenticate", return_value=auth), \
             patch.object(
                 tool, "_api_request",
                 side_effect=[
                     (200, {"dashboards": []}),
                     (500, {"message": "Internal Server Error"}),
                 ]
             ):
            result = tool._run(
                visual_mappings_json=SAMPLE_TOOL94_OUTPUT,
                warehouse_id="wh-123",
            )

        data = json.loads(result)
        assert "error" in data

    def test_ssrf_check_blocks_invalid_host(self):
        """Non-Databricks host → error."""
        tool = DatabricksDashboardCreatorTool()
        auth = _mock_auth(workspace_url="https://evil.example.com")

        with patch.object(tool, "_authenticate", return_value=auth):
            result = tool._run(
                visual_mappings_json=SAMPLE_TOOL94_OUTPUT,
                warehouse_id="wh-123",
            )

        data = json.loads(result)
        assert "error" in data
        assert "Untrusted" in data["error"]

    def test_publish_called_when_publish_true(self):
        """publish_dashboard=True → _publish_dashboard is called."""
        tool = DatabricksDashboardCreatorTool()
        auth = _mock_auth()

        with patch.object(tool, "_authenticate", return_value=auth), \
             patch.object(
                 tool, "_api_request",
                 side_effect=[
                     (200, {"dashboards": []}),
                     (201, {"dashboard_id": "dash-001"}),
                     (200, {}),
                 ]
             ), \
             patch.object(tool, "_publish_dashboard", return_value=True) as mock_pub:
                result = tool._run(
                    visual_mappings_json=SAMPLE_TOOL94_OUTPUT,
                    warehouse_id="wh-123",
                    publish_dashboard=True,
                )

        mock_pub.assert_called_once()

    def test_no_publish_when_publish_false(self):
        """publish_dashboard=False → _publish_dashboard not called."""
        tool = DatabricksDashboardCreatorTool()
        auth = _mock_auth()

        with patch.object(tool, "_authenticate", return_value=auth), \
             patch.object(
                 tool, "_api_request",
                 side_effect=[
                     (200, {"dashboards": []}),
                     (201, {"dashboard_id": "dash-001"}),
                 ]
             ), \
             patch.object(tool, "_publish_dashboard", return_value=True) as mock_pub:
                result = tool._run(
                    visual_mappings_json=SAMPLE_TOOL94_OUTPUT,
                    warehouse_id="wh-123",
                    publish_dashboard=False,
                )

        mock_pub.assert_not_called()


# ---------------------------------------------------------------------------
# Output structure
# ---------------------------------------------------------------------------

class TestOutputStructure:
    def test_output_has_required_keys(self):
        """Successful output has dashboard_id, dashboard_url, widget_count, etc."""
        tool = DatabricksDashboardCreatorTool()
        auth = _mock_auth()

        with patch.object(tool, "_authenticate", return_value=auth), \
             patch.object(
                 tool, "_api_request",
                 side_effect=[
                     (200, {"dashboards": []}),
                     (201, {"dashboard_id": "dash-123"}),
                     (200, {}),
                 ]
             ):
            result = tool._run(
                visual_mappings_json=SAMPLE_TOOL94_OUTPUT,
                warehouse_id="wh-123",
            )

        data = json.loads(result)
        assert "dashboard_id" in data
        assert "dashboard_url" in data
        assert "widget_count" in data
        assert "page_count" in data
        assert "dataset_count" in data
        assert "status" in data

    def test_widget_count_matches_mapped_visuals(self):
        """widget_count reflects visuals with ucmv_view."""
        tool = DatabricksDashboardCreatorTool()
        auth = _mock_auth()

        with patch.object(tool, "_authenticate", return_value=auth), \
             patch.object(
                 tool, "_api_request",
                 side_effect=[
                     (200, {"dashboards": []}),
                     (201, {"dashboard_id": "dash-123"}),
                     (200, {}),
                 ]
             ):
            result = tool._run(
                visual_mappings_json=SAMPLE_TOOL94_OUTPUT,
                warehouse_id="wh-123",
            )

        data = json.loads(result)
        # SAMPLE_TOOL94_OUTPUT has 3 visuals, all mapped
        assert data["widget_count"] == 3


# ---------------------------------------------------------------------------
# Safe name helper
# ---------------------------------------------------------------------------

class TestSafeName:
    def test_safe_name_alphanumeric_ok(self):
        tool = DatabricksDashboardCreatorTool()
        assert tool._safe_name("hello_world_123") == "hello_world_123"

    def test_safe_name_replaces_special_chars(self):
        tool = DatabricksDashboardCreatorTool()
        result = tool._safe_name("hello world! (test)")
        assert " " not in result
        assert "!" not in result
        assert "(" not in result

    def test_safe_name_empty_returns_unnamed(self):
        tool = DatabricksDashboardCreatorTool()
        result = tool._safe_name("")
        assert result == "unnamed"

    def test_safe_name_max_60_chars(self):
        tool = DatabricksDashboardCreatorTool()
        long_name = "a" * 100
        result = tool._safe_name(long_name)
        assert len(result) <= 60
