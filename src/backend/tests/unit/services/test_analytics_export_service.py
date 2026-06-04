"""
Unit tests for analytics_export_service.py

Covers:
  - _safe_folder_name()
  - _genie_space_to_files()   — all 4 YAML files, edge cases
  - _dashboard_to_files()     — all 3 YAML files, all widget types
  - AnalyticsExportService.export_genie_space() — with mocked repo
  - AnalyticsExportService.export_dashboard()   — with mocked repo
  - AnalyticsExportService.list_dashboards()    — with mocked repo
"""
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.analytics_export_service import (
    _safe_folder_name,
    _genie_space_to_files,
    _dashboard_to_files,
    _widget_layout_to_yaml_dict,
    AnalyticsExportService,
)
import yaml


# ─────────────────────────────────────────────────────────────────────────────
# _safe_folder_name
# ─────────────────────────────────────────────────────────────────────────────

class TestSafeFolderName:
    def test_simple_ascii(self):
        assert _safe_folder_name("Supply Chain Analytics") == "supply_chain_analytics"

    def test_special_chars_replaced(self):
        result = _safe_folder_name("CCHBC Order Analytics – IOM004")
        assert " " not in result
        assert "–" not in result
        assert result.startswith("cchbc")

    def test_already_safe(self):
        assert _safe_folder_name("my_dashboard") == "my_dashboard"

    def test_empty_string_fallback(self):
        assert _safe_folder_name("") == "export"

    def test_truncated_at_80_chars(self):
        long = "a" * 100
        assert len(_safe_folder_name(long)) <= 80

    def test_leading_trailing_underscores_stripped(self):
        result = _safe_folder_name("---name---")
        assert not result.startswith("_")
        assert not result.endswith("_")


# ─────────────────────────────────────────────────────────────────────────────
# Helper fixtures
# ─────────────────────────────────────────────────────────────────────────────

def _make_genie_space(**overrides) -> dict:
    """Minimal valid Genie space API response."""
    base = {
        "space_id": "space_abc123",
        "title": "Supply Chain Analytics",
        "warehouse_id": "wh_xyz",
        "description": "Test space",
        "serialized_space": json.dumps({
            "version": 2,
            "config": {
                "sample_questions": [
                    {"id": "q1", "question": ["What is total revenue?"]},
                    {"id": "q2", "question": ["Top 10 customers?"]},
                ]
            },
            "data_sources": {
                "metric_views": [{"identifier": "main.sc.orders_mv"}],
                "tables": [{"identifier": "main.sc.dim_customer"}],
            },
            "instructions": {
                "text_instructions": [
                    {"id": "t1", "content": ["Use fiscal periods for all time comparisons."]}
                ],
                "join_specs": [{
                    "id": "j1",
                    "left": {"identifier": "main.sc.orders_mv", "alias": "orders_mv"},
                    "right": {"identifier": "main.sc.dim_customer", "alias": "dim_customer"},
                    "sql": [
                        "orders_mv.customer_id = dim_customer.id",
                        "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--",
                    ],
                }],
                "sql_snippets": {
                    "expressions": [
                        {"id": "e1", "display_name": "Total Revenue", "sql": ["SUM(revenue_eur)"]}
                    ],
                    "measures": [
                        {
                            "id": "m1",
                            "display_name": "Service Level %",
                            "sql": ["100.0 * SUM(confirmed) / NULLIF(SUM(ordered), 0)"],
                            "instruction": ["Order fill rate percentage"],
                        }
                    ],
                    "filters": [
                        {"id": "f1", "display_name": "Active Orders", "sql": ["status = 'ACTIVE'"]}
                    ],
                },
                "example_question_sqls": [{
                    "id": "ex1",
                    "question": ["Top 10 customers by PhC"],
                    "sql": ["SELECT customer_name, SUM(phc) FROM orders GROUP BY 1 ORDER BY 2 DESC LIMIT 10"],
                }],
            },
        }),
    }
    base.update(overrides)
    return base


def _make_dashboard(**overrides) -> dict:
    """Minimal valid Lakeview dashboard API response."""
    base = {
        "dashboard_id": "dash_abc123",
        "display_name": "CCHBC Order Analytics",
        "warehouse_id": "wh_xyz",
        "parent_path": "/Workspace/Shared/Demo",
        "serialized_dashboard": json.dumps({
            "datasets": [
                {
                    "name": "ds_overview",
                    "displayName": "KPI Overview",
                    "query": "SELECT SUM(revenue) AS total_rev FROM orders",
                },
                {
                    "name": "ds_trend",
                    "displayName": "Monthly Trend",
                    "query": "SELECT period, SUM(revenue) AS rev\nFROM orders\nGROUP BY period",
                },
            ],
            "pages": [
                {
                    "name": "page_overview",
                    "displayName": "Executive Overview",
                    "layout": [
                        {
                            "widget": {
                                "name": "title_text",
                                "multilineTextboxSpec": {"lines": ["## CCHBC Analytics\n"]},
                            },
                            "position": {"x": 0, "y": 0, "width": 6, "height": 1},
                        },
                        {
                            "widget": {
                                "name": "kpi_rev",
                                "queries": [{"name": "main_query", "query": {
                                    "datasetName": "ds_overview",
                                    "fields": [{"name": "sum(revenue)", "expression": "SUM(`revenue`)"}],
                                    "disaggregated": False,
                                }}],
                                "spec": {
                                    "version": 2,
                                    "widgetType": "counter",
                                    "encodings": {"value": {"fieldName": "sum(revenue)"}},
                                    "frame": {"title": "Total Revenue", "showTitle": True},
                                },
                            },
                            "position": {"x": 0, "y": 1, "width": 2, "height": 3},
                        },
                    ],
                }
            ],
        }),
    }
    base.update(overrides)
    return base


# ─────────────────────────────────────────────────────────────────────────────
# _genie_space_to_files
# ─────────────────────────────────────────────────────────────────────────────

class TestGenieSpaceToFiles:
    """Tests for the new reference-implementation YAML structure.

    Output files: header.yaml, config.yaml, data_sources.yaml, instructions.yaml
    (replaces the old config/tables/instructions/questions layout)
    """

    def test_returns_at_least_four_files(self):
        files = _genie_space_to_files(_make_genie_space())
        # header + config + data_sources + instructions = 4 minimum
        assert len(files) >= 4
        paths = {f.path for f in files}
        assert "header.yaml" in paths
        assert "config.yaml" in paths
        assert "data_sources.yaml" in paths
        assert "instructions.yaml" in paths

    def test_header_yaml_has_space_metadata(self):
        files = {f.path: f for f in _genie_space_to_files(_make_genie_space())}
        data = yaml.safe_load(files["header.yaml"].content)
        assert data["space_id"] == "space_abc123"
        assert data["title"] == "Supply Chain Analytics"
        assert data["warehouse_id"] == "wh_xyz"

    def test_header_yaml_has_api_comment(self):
        files = {f.path: f for f in _genie_space_to_files(_make_genie_space())}
        assert "/api/2.0/genie/spaces" in files["header.yaml"].content

    def test_data_sources_yaml_has_metric_views_and_tables(self):
        files = {f.path: f for f in _genie_space_to_files(_make_genie_space())}
        data = yaml.safe_load(files["data_sources.yaml"].content)
        mv_ids = [t.get("identifier", "") for t in data.get("metric_views", [])]
        tbl_ids = [t.get("identifier", "") for t in data.get("tables", [])]
        assert "main.sc.orders_mv" in mv_ids
        assert "main.sc.dim_customer" in tbl_ids

    def test_instructions_yaml_has_text_instructions(self):
        files = {f.path: f for f in _genie_space_to_files(_make_genie_space())}
        content = files["instructions.yaml"].content
        assert "fiscal periods" in content

    def test_instructions_yaml_has_join_specs(self):
        files = {f.path: f for f in _genie_space_to_files(_make_genie_space())}
        content = files["instructions.yaml"].content
        assert "orders_mv" in content
        assert "dim_customer" in content

    def test_instructions_yaml_has_sql_snippets(self):
        files = {f.path: f for f in _genie_space_to_files(_make_genie_space())}
        content = files["instructions.yaml"].content
        assert "Total Revenue" in content
        assert "Service Level" in content
        assert "Active Orders" in content

    def test_config_yaml_has_sample_questions(self):
        files = {f.path: f for f in _genie_space_to_files(_make_genie_space())}
        content = files["config.yaml"].content
        assert "total revenue" in content.lower() or "revenue" in content.lower()

    def test_empty_serialized_space_still_produces_files(self):
        """Minimal space with no serialized_space → still valid YAML files."""
        space = {"space_id": "x", "title": "Empty Space", "warehouse_id": "wh1"}
        files = _genie_space_to_files(space)
        assert len(files) >= 1
        # header.yaml always present
        paths = {f.path for f in files}
        assert "header.yaml" in paths
        for f in files:
            yaml.safe_load(f.content)  # must be valid YAML

    def test_malformed_serialized_space_falls_back_gracefully(self):
        """Corrupt JSON in serialized_space → no crash, still produces header."""
        space = _make_genie_space(serialized_space="{corrupt json")
        files = _genie_space_to_files(space)
        assert len(files) >= 1
        paths = {f.path for f in files}
        assert "header.yaml" in paths

    def test_only_metric_views_no_plain_tables(self):
        space = _make_genie_space()
        spec = json.loads(space["serialized_space"])
        del spec["data_sources"]["tables"]
        space["serialized_space"] = json.dumps(spec)
        files = {f.path: f for f in _genie_space_to_files(space)}
        data = yaml.safe_load(files["data_sources.yaml"].content)
        mv_ids = [t.get("identifier", "") for t in data.get("metric_views", [])]
        assert "main.sc.orders_mv" in mv_ids


# ─────────────────────────────────────────────────────────────────────────────
# _dashboard_to_files
# ─────────────────────────────────────────────────────────────────────────────

class TestDashboardToFiles:
    def test_returns_three_files(self):
        files = _dashboard_to_files(_make_dashboard())
        assert len(files) == 3
        paths = {f.path for f in files}
        assert paths == {"config.yaml", "datasets.yaml", "pages.yaml"}

    def test_config_yaml_is_valid(self):
        files = {f.path: f for f in _dashboard_to_files(_make_dashboard())}
        data = yaml.safe_load(files["config.yaml"].content)
        assert data["apiVersion"] == "lakeview/v1"
        assert data["kind"] == "Dashboard"
        assert data["metadata"]["dashboard_id"] == "dash_abc123"
        assert data["spec"]["title"] == "CCHBC Order Analytics"
        assert data["spec"]["warehouse_id"] == "wh_xyz"
        assert data["spec"]["parent_path"] == "/Workspace/Shared/Demo"
        assert data["spec"]["publish"] is True

    def test_config_yaml_has_api_comments(self):
        files = {f.path: f for f in _dashboard_to_files(_make_dashboard())}
        content = files["config.yaml"].content
        assert "POST /api/2.0/lakeview/dashboards" in content

    def test_datasets_yaml_contains_both_datasets(self):
        files = {f.path: f for f in _dashboard_to_files(_make_dashboard())}
        content = files["datasets.yaml"].content
        assert "ds_overview" in content
        assert "ds_trend" in content
        assert "KPI Overview" in content

    def test_datasets_multiline_sql_uses_literal_block(self):
        """Multi-line SQL should be indented (literal block scalar), not inline."""
        files = {f.path: f for f in _dashboard_to_files(_make_dashboard())}
        content = files["datasets.yaml"].content
        # Multi-line query for ds_trend should be rendered with |
        assert "query: |" in content

    def test_datasets_single_line_sql_stays_inline(self):
        files = {f.path: f for f in _dashboard_to_files(_make_dashboard())}
        content = files["datasets.yaml"].content
        # Single-line SELECT for ds_overview
        assert '"SELECT SUM(revenue) AS total_rev FROM orders"' in content

    def test_pages_yaml_has_page(self):
        files = {f.path: f for f in _dashboard_to_files(_make_dashboard())}
        data = yaml.safe_load(files["pages.yaml"].content)
        assert len(data["pages"]) == 1
        assert data["pages"][0]["name"] == "page_overview"
        assert data["pages"][0]["displayName"] == "Executive Overview"

    def test_pages_yaml_has_widgets(self):
        files = {f.path: f for f in _dashboard_to_files(_make_dashboard())}
        data = yaml.safe_load(files["pages.yaml"].content)
        widgets = data["pages"][0]["widgets"]
        assert len(widgets) == 2

    def test_text_widget_converted_correctly(self):
        files = {f.path: f for f in _dashboard_to_files(_make_dashboard())}
        data = yaml.safe_load(files["pages.yaml"].content)
        text_w = next(w for w in data["pages"][0]["widgets"] if w["type"] == "text")
        assert "CCHBC Analytics" in text_w["content"]
        assert text_w["position"]["width"] == 6

    def test_counter_widget_converted_correctly(self):
        files = {f.path: f for f in _dashboard_to_files(_make_dashboard())}
        data = yaml.safe_load(files["pages.yaml"].content)
        counter = next(w for w in data["pages"][0]["widgets"] if w["type"] == "counter")
        assert counter["name"] == "kpi_rev"
        assert counter["dataset"] == "ds_overview"
        assert counter["value_field"] == "sum(revenue)"
        assert counter["position"]["width"] == 2

    def test_empty_serialized_dashboard_still_produces_files(self):
        dashboard = {"dashboard_id": "x", "display_name": "Empty"}
        files = _dashboard_to_files(dashboard)
        assert len(files) == 3
        for f in files:
            yaml.safe_load(f.content)  # valid YAML

    def test_malformed_serialized_dashboard_falls_back(self):
        dashboard = _make_dashboard(serialized_dashboard="{bad json")
        files = _dashboard_to_files(dashboard)
        assert len(files) == 3


# ─────────────────────────────────────────────────────────────────────────────
# _widget_layout_to_yaml_dict  (per widget type)
# ─────────────────────────────────────────────────────────────────────────────

class TestWidgetLayoutToYamlDict:
    def _counter_layout(self, name="kpi"):
        return {
            "widget": {
                "name": name,
                "queries": [{"name": "main_query", "query": {
                    "datasetName": "ds_x",
                    "fields": [{"name": "sum(revenue)", "expression": "SUM(`revenue`)"}],
                    "disaggregated": False,
                }}],
                "spec": {
                    "version": 2,
                    "widgetType": "counter",
                    "encodings": {"value": {"fieldName": "sum(revenue)"}},
                    "frame": {"title": "Revenue", "showTitle": True},
                },
            },
            "position": {"x": 0, "y": 0, "width": 2, "height": 3},
        }

    def _bar_layout(self, name="chart"):
        return {
            "widget": {
                "name": name,
                "queries": [{"name": "main_query", "query": {
                    "datasetName": "ds_x",
                    "fields": [
                        {"name": "pack_type", "expression": "`pack_type`"},
                        {"name": "sum(phc)", "expression": "SUM(`phc`)"},
                    ],
                    "disaggregated": False,
                }}],
                "spec": {
                    "version": 3,
                    "widgetType": "bar",
                    "encodings": {
                        "x": {"fieldName": "pack_type", "scale": {"type": "categorical"}},
                        "y": {"fieldName": "sum(phc)", "scale": {"type": "quantitative"}},
                        "color": {"fieldName": "region"},
                    },
                    "frame": {"title": "PhC by Pack Type", "showTitle": True},
                },
            },
            "position": {"x": 0, "y": 4, "width": 3, "height": 5},
        }

    def _filter_layout(self, name="filter_org"):
        return {
            "widget": {
                "name": name,
                "queries": [{"name": "q", "query": {
                    "datasetName": "ds_x",
                    "fields": [],
                    "disaggregated": False,
                }}],
                "spec": {
                    "version": 2,
                    "widgetType": "filter-multi-select",
                    "encodings": {"fields": [
                        {"fieldName": "salesorg_name", "displayName": "Sales Org", "queryName": "q"}
                    ]},
                    "frame": {"title": "Sales Organization", "showTitle": True},
                },
            },
            "position": {"x": 4, "y": 0, "width": 2, "height": 1},
        }

    def test_text_widget(self):
        layout = {
            "widget": {
                "name": "header",
                "multilineTextboxSpec": {"lines": ["## Title\n", "Subtitle text"]},
            },
            "position": {"x": 0, "y": 0, "width": 6, "height": 1},
        }
        result = _widget_layout_to_yaml_dict(layout)
        assert result["type"] == "text"
        assert "Title" in result["content"]
        assert result["position"]["width"] == 6

    def test_counter_widget(self):
        result = _widget_layout_to_yaml_dict(self._counter_layout())
        assert result["type"] == "counter"
        assert result["dataset"] == "ds_x"
        assert result["value_field"] == "sum(revenue)"
        assert len(result["fields"]) == 1

    def test_bar_widget(self):
        result = _widget_layout_to_yaml_dict(self._bar_layout())
        assert result["type"] == "bar"
        assert result["x"]["field"] == "pack_type"
        assert result["x"]["type"] == "categorical"
        assert result["y"]["field"] == "sum(phc)"
        assert result["color"]["field"] == "region"

    def test_line_widget(self):
        layout = self._bar_layout()
        layout["widget"]["spec"]["widgetType"] = "line"
        result = _widget_layout_to_yaml_dict(layout)
        assert result["type"] == "line"

    def test_table_widget(self):
        layout = {
            "widget": {
                "name": "tbl",
                "queries": [{"name": "main_query", "query": {
                    "datasetName": "ds_customer",
                    "fields": [
                        {"name": "customer_name", "expression": "`customer_name`"},
                        {"name": "sum(rev)", "expression": "SUM(`rev`)"},
                    ],
                    "disaggregated": False,
                }}],
                "spec": {
                    "version": 2,
                    "widgetType": "table",
                    "encodings": {"columns": [
                        {"fieldName": "customer_name", "displayName": "Customer"},
                        {"fieldName": "sum(rev)", "displayName": "Revenue"},
                    ]},
                    "frame": {"title": "Top Customers", "showTitle": True},
                },
            },
            "position": {"x": 0, "y": 10, "width": 6, "height": 5},
        }
        result = _widget_layout_to_yaml_dict(layout)
        assert result["type"] == "table"
        assert result["columns"][0]["field"] == "customer_name"
        assert result["columns"][1]["displayName"] == "Revenue"

    def test_filter_multi_select(self):
        result = _widget_layout_to_yaml_dict(self._filter_layout())
        assert result["type"] == "filter-multi-select"
        assert result["field"] == "salesorg_name"
        assert result["displayName"] == "Sales Org"

    def test_filter_date_range(self):
        layout = self._filter_layout()
        layout["widget"]["spec"]["widgetType"] = "filter-date-range-picker"
        result = _widget_layout_to_yaml_dict(layout)
        assert result["type"] == "filter-date-range-picker"

    def test_position_preserved(self):
        result = _widget_layout_to_yaml_dict(self._counter_layout())
        assert result["position"] == {"x": 0, "y": 0, "width": 2, "height": 3}


# ─────────────────────────────────────────────────────────────────────────────
# AnalyticsExportService — async tests with mocked dependencies
# ─────────────────────────────────────────────────────────────────────────────

class TestAnalyticsExportService:
    """Tests for the async service layer with mocked repositories."""

    # ── export_genie_space ────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_export_genie_space_returns_bundle(self):
        """export_genie_space returns space_id, space_name, folder_name, files."""
        service = AnalyticsExportService(user_token="tok")
        space = _make_genie_space()

        # The service now calls _fetch_raw_genie_space directly (no GenieRepository)
        with patch.object(
            service, "_fetch_raw_genie_space", new=AsyncMock(return_value=space)
        ):
            result = await service.export_genie_space("space_abc123")

        assert result["space_id"] == "space_abc123"
        assert result["space_name"] == "Supply Chain Analytics"
        assert result["folder_name"] == "supply_chain_analytics"
        assert len(result["files"]) >= 4

    @pytest.mark.asyncio
    async def test_export_genie_space_not_found_raises(self):
        """export_genie_space raises ValueError when _fetch_raw_genie_space raises."""
        service = AnalyticsExportService()

        with patch.object(
            service, "_fetch_raw_genie_space",
            new=AsyncMock(side_effect=Exception("HTTP 404"))
        ):
            with pytest.raises(Exception):
                await service.export_genie_space("missing_id")

    @pytest.mark.asyncio
    async def test_export_genie_space_with_serialized_space_passed_directly(self):
        """When serialized_space is passed, metadata fetch is attempted but optional."""
        service = AnalyticsExportService(user_token="tok")
        space = _make_genie_space()

        # Pass serialized_space directly — fetch is only for metadata enrichment
        with patch.object(
            service, "_fetch_raw_genie_space",
            new=AsyncMock(return_value={"title": "Supply Chain Analytics", "warehouse_id": "wh_xyz"})
        ):
            result = await service.export_genie_space(
                "space_abc123",
                serialized_space=space["serialized_space"]
            )

        assert result["space_id"] == "space_abc123"
        assert len(result["files"]) >= 4

    # ── export_dashboard ──────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_export_dashboard_returns_bundle(self):
        """export_dashboard returns dashboard_id, dashboard_name, folder_name, files."""
        service = AnalyticsExportService()
        dashboard = _make_dashboard()

        with patch(
            "src.services.analytics_export_service.DashboardRepository"
        ) as MockRepo:
            instance = MockRepo.return_value
            instance.get_dashboard = AsyncMock(return_value=dashboard)
            instance.close = AsyncMock()

            result = await service.export_dashboard("dash_abc123")

        assert result["dashboard_id"] == "dash_abc123"
        assert result["dashboard_name"] == "CCHBC Order Analytics"
        assert result["folder_name"] == "cchbc_order_analytics"
        assert len(result["files"]) == 3

    @pytest.mark.asyncio
    async def test_export_dashboard_not_found_raises(self):
        """export_dashboard raises ValueError when dashboard is not found."""
        service = AnalyticsExportService()

        with patch(
            "src.services.analytics_export_service.DashboardRepository"
        ) as MockRepo:
            instance = MockRepo.return_value
            instance.get_dashboard = AsyncMock(return_value=None)
            instance.close = AsyncMock()

            with pytest.raises(ValueError, match="not found"):
                await service.export_dashboard("missing_id")

    @pytest.mark.asyncio
    async def test_export_dashboard_closes_repo(self):
        """Repository.close() is always called after export_dashboard."""
        service = AnalyticsExportService()
        dashboard = _make_dashboard()

        with patch(
            "src.services.analytics_export_service.DashboardRepository"
        ) as MockRepo:
            instance = MockRepo.return_value
            instance.get_dashboard = AsyncMock(return_value=dashboard)
            instance.close = AsyncMock()

            await service.export_dashboard("dash_abc123")

        instance.close.assert_awaited_once()

    # ── list_dashboards ───────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_list_dashboards_returns_list(self):
        """list_dashboards returns the raw list from the repository."""
        service = AnalyticsExportService()
        raw = [
            {"dashboard_id": "d1", "display_name": "Sales"},
            {"dashboard_id": "d2", "display_name": "Marketing"},
        ]

        with patch(
            "src.services.analytics_export_service.DashboardRepository"
        ) as MockRepo:
            instance = MockRepo.return_value
            instance.list_dashboards = AsyncMock(return_value=raw)
            instance.close = AsyncMock()

            result = await service.list_dashboards(page_size=20)

        assert len(result) == 2
        assert result[0]["dashboard_id"] == "d1"

    @pytest.mark.asyncio
    async def test_list_dashboards_passes_page_size(self):
        """list_dashboards forwards the page_size to the repository."""
        service = AnalyticsExportService()

        with patch(
            "src.services.analytics_export_service.DashboardRepository"
        ) as MockRepo:
            instance = MockRepo.return_value
            instance.list_dashboards = AsyncMock(return_value=[])
            instance.close = AsyncMock()

            await service.list_dashboards(page_size=75)

        instance.list_dashboards.assert_awaited_once_with(page_size=75)

    # ── init / user_token ─────────────────────────────────────────────────────

    def test_service_stores_user_token(self):
        service = AnalyticsExportService(user_token="my-tok")
        assert service._user_token == "my-tok"

    def test_service_no_token_defaults_none(self):
        service = AnalyticsExportService()
        assert service._user_token is None

    @pytest.mark.asyncio
    async def test_user_token_passed_to_dashboard_repository(self):
        """The user_token is forwarded to DashboardRepository on init."""
        service = AnalyticsExportService(user_token="obo-token")
        dashboard = _make_dashboard()

        with patch(
            "src.services.analytics_export_service.DashboardRepository"
        ) as MockRepo:
            instance = MockRepo.return_value
            instance.get_dashboard = AsyncMock(return_value=dashboard)
            instance.close = AsyncMock()

            await service.export_dashboard("dash_abc123")

        MockRepo.assert_called_once_with(user_token="obo-token")
