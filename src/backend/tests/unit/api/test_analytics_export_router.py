"""
Unit tests for analytics_export_router.py

Tests all five endpoints by calling handler functions directly with mocked
AnalyticsExportService — same pattern as test_genie_router.py.

Endpoints under test:
  GET /api/analytics-export/genie-spaces/{space_id}/download
  GET /api/analytics-export/genie-spaces/{space_id}/preview
  GET /api/analytics-export/dashboards
  GET /api/analytics-export/dashboards/{dashboard_id}/download
  GET /api/analytics-export/dashboards/{dashboard_id}/preview
"""
import io
import json
import zipfile
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.schemas.analytics_export import ExportFile


# ─────────────────────────────────────────────────────────────────────────────
# Test helpers
# ─────────────────────────────────────────────────────────────────────────────

class FakeGroupCtx:
    """Minimal GroupContext stand-in."""
    def __init__(self):
        self.group_ids = ["grp-1"]
        self.group_email = "test@example.com"


def _make_request():
    return MagicMock()


def _make_genie_files(folder="supply_chain"):
    return [
        ExportFile(path="config.yaml", content="apiVersion: genie/v1\nkind: GenieSpace\n"),
        ExportFile(path="tables.yaml", content="metric_views:\n- main.sc.orders_mv\n"),
        ExportFile(path="instructions.yaml", content="text_instructions: Use fiscal periods.\n"),
        ExportFile(path="questions.yaml", content="sample_questions:\n- What is revenue?\n"),
    ]


def _make_dashboard_files(folder="example_analytics"):
    return [
        ExportFile(path="config.yaml", content="apiVersion: lakeview/v1\nkind: Dashboard\n"),
        ExportFile(path="datasets.yaml", content="datasets:\n  - name: ds_1\n    query: SELECT 1\n"),
        ExportFile(path="pages.yaml", content="pages:\n  - name: page_1\n    widgets: []\n"),
    ]


def _genie_export_result(space_id="sp123", name="Supply Chain", folder="supply_chain"):
    return {
        "space_id": space_id,
        "space_name": name,
        "folder_name": folder,
        "files": _make_genie_files(folder),
    }


def _dashboard_export_result(dash_id="d123", name="Example Analytics", folder="example_analytics"):
    return {
        "dashboard_id": dash_id,
        "dashboard_name": name,
        "folder_name": folder,
        "files": _make_dashboard_files(folder),
    }


def _patch_service(mock_instance):
    return patch(
        "src.api.analytics_export_router.AnalyticsExportService",
        return_value=mock_instance,
    )


def _patch_token(token=None):
    return patch(
        "src.api.analytics_export_router.extract_user_token_from_request",
        return_value=token,
    )


def _patch_user_ctx():
    return patch("src.api.analytics_export_router.UserContext.set_group_context")


# ─────────────────────────────────────────────────────────────────────────────
# Genie Space — download
# ─────────────────────────────────────────────────────────────────────────────

class TestDownloadGenieSpaceExport:
    @pytest.mark.asyncio
    async def test_returns_streaming_response(self):
        from src.api.analytics_export_router import download_genie_space_export
        from fastapi.responses import StreamingResponse

        mock_svc = AsyncMock()
        mock_svc.export_genie_space = AsyncMock(return_value=_genie_export_result())

        with _patch_service(mock_svc), _patch_token("tok"), _patch_user_ctx():
            response = await download_genie_space_export(
                space_id="sp123",
                request=_make_request(),
                group_context=FakeGroupCtx(),
            )

        assert isinstance(response, StreamingResponse)
        assert response.media_type == "application/zip"

    @pytest.mark.asyncio
    async def test_zip_contains_correct_files(self):
        from src.api.analytics_export_router import download_genie_space_export

        mock_svc = AsyncMock()
        mock_svc.export_genie_space = AsyncMock(return_value=_genie_export_result())

        with _patch_service(mock_svc), _patch_token(), _patch_user_ctx():
            response = await download_genie_space_export(
                space_id="sp123",
                request=_make_request(),
                group_context=None,
            )

        # Read the streaming response body
        chunks = []
        async for chunk in response.body_iterator:
            chunks.append(chunk)
        zip_bytes = b"".join(chunks)

        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = zf.namelist()

        assert "supply_chain/config.yaml" in names
        assert "supply_chain/tables.yaml" in names
        assert "supply_chain/instructions.yaml" in names
        assert "supply_chain/questions.yaml" in names

    @pytest.mark.asyncio
    async def test_zip_filename_in_content_disposition(self):
        from src.api.analytics_export_router import download_genie_space_export

        mock_svc = AsyncMock()
        mock_svc.export_genie_space = AsyncMock(return_value=_genie_export_result())

        with _patch_service(mock_svc), _patch_token(), _patch_user_ctx():
            response = await download_genie_space_export(
                space_id="sp123",
                request=_make_request(),
                group_context=None,
            )

        cd = response.headers["Content-Disposition"]
        assert "supply_chain_genie_space.zip" in cd

    @pytest.mark.asyncio
    async def test_space_not_found_raises_404(self):
        from src.api.analytics_export_router import download_genie_space_export
        from src.core.exceptions import NotFoundError

        mock_svc = AsyncMock()
        mock_svc.export_genie_space = AsyncMock(side_effect=ValueError("not found"))

        with _patch_service(mock_svc), _patch_token(), _patch_user_ctx():
            with pytest.raises(NotFoundError):
                await download_genie_space_export(
                    space_id="bad_id",
                    request=_make_request(),
                    group_context=None,
                )

    @pytest.mark.asyncio
    async def test_service_called_with_correct_space_id(self):
        from src.api.analytics_export_router import download_genie_space_export

        mock_svc = AsyncMock()
        mock_svc.export_genie_space = AsyncMock(return_value=_genie_export_result())

        with _patch_service(mock_svc), _patch_token("tok"), _patch_user_ctx():
            await download_genie_space_export(
                space_id="my_space_id",
                request=_make_request(),
                group_context=None,
            )

        mock_svc.export_genie_space.assert_awaited_once_with("my_space_id")


# ─────────────────────────────────────────────────────────────────────────────
# Genie Space — preview
# ─────────────────────────────────────────────────────────────────────────────

class TestPreviewGenieSpaceExport:
    @pytest.mark.asyncio
    async def test_returns_json_with_files(self):
        from src.api.analytics_export_router import preview_genie_space_export
        from fastapi.responses import JSONResponse

        mock_svc = AsyncMock()
        mock_svc.export_genie_space = AsyncMock(return_value=_genie_export_result())

        with _patch_service(mock_svc), _patch_token(), _patch_user_ctx():
            response = await preview_genie_space_export(
                space_id="sp123",
                request=_make_request(),
                group_context=None,
            )

        assert isinstance(response, JSONResponse)
        body = json.loads(response.body)
        assert body["space_id"] == "sp123"
        assert body["file_count"] == 4
        assert len(body["files"]) == 4

    @pytest.mark.asyncio
    async def test_preview_paths_include_folder_prefix(self):
        from src.api.analytics_export_router import preview_genie_space_export

        mock_svc = AsyncMock()
        mock_svc.export_genie_space = AsyncMock(return_value=_genie_export_result())

        with _patch_service(mock_svc), _patch_token(), _patch_user_ctx():
            response = await preview_genie_space_export(
                space_id="sp123",
                request=_make_request(),
                group_context=None,
            )

        body = json.loads(response.body)
        paths = [f["path"] for f in body["files"]]
        assert "supply_chain/config.yaml" in paths
        assert "supply_chain/tables.yaml" in paths

    @pytest.mark.asyncio
    async def test_preview_not_found_raises_404(self):
        from src.api.analytics_export_router import preview_genie_space_export
        from src.core.exceptions import NotFoundError

        mock_svc = AsyncMock()
        mock_svc.export_genie_space = AsyncMock(side_effect=ValueError("not found"))

        with _patch_service(mock_svc), _patch_token(), _patch_user_ctx():
            with pytest.raises(NotFoundError):
                await preview_genie_space_export(
                    space_id="bad_id",
                    request=_make_request(),
                    group_context=None,
                )


# ─────────────────────────────────────────────────────────────────────────────
# Dashboard — list
# ─────────────────────────────────────────────────────────────────────────────

class TestListDashboards:
    @pytest.mark.asyncio
    async def test_returns_list_of_summaries(self):
        from src.api.analytics_export_router import list_dashboards

        raw = [
            {"dashboard_id": "d1", "display_name": "Sales", "warehouse_id": "wh1", "lifecycle_state": "ACTIVE"},
            {"dashboard_id": "d2", "display_name": "Marketing", "warehouse_id": "wh2"},
        ]

        mock_svc = AsyncMock()
        mock_svc.list_dashboards = AsyncMock(return_value=raw)

        with _patch_service(mock_svc), _patch_token(), _patch_user_ctx():
            result = await list_dashboards(
                request=_make_request(),
                page_size=50,
                group_context=None,
            )

        assert len(result) == 2
        assert result[0].dashboard_id == "d1"
        assert result[0].display_name == "Sales"
        assert result[1].dashboard_id == "d2"

    @pytest.mark.asyncio
    async def test_page_size_forwarded_to_service(self):
        from src.api.analytics_export_router import list_dashboards

        mock_svc = AsyncMock()
        mock_svc.list_dashboards = AsyncMock(return_value=[])

        with _patch_service(mock_svc), _patch_token(), _patch_user_ctx():
            await list_dashboards(
                request=_make_request(),
                page_size=100,
                group_context=None,
            )

        mock_svc.list_dashboards.assert_awaited_once_with(page_size=100)

    @pytest.mark.asyncio
    async def test_empty_list_returns_empty(self):
        from src.api.analytics_export_router import list_dashboards

        mock_svc = AsyncMock()
        mock_svc.list_dashboards = AsyncMock(return_value=[])

        with _patch_service(mock_svc), _patch_token(), _patch_user_ctx():
            result = await list_dashboards(
                request=_make_request(),
                page_size=50,
                group_context=None,
            )

        assert result == []

    @pytest.mark.asyncio
    async def test_missing_optional_fields_dont_crash(self):
        """Dashboards with only dashboard_id + display_name should still parse."""
        from src.api.analytics_export_router import list_dashboards

        raw = [{"dashboard_id": "d1", "display_name": "Minimal"}]
        mock_svc = AsyncMock()
        mock_svc.list_dashboards = AsyncMock(return_value=raw)

        with _patch_service(mock_svc), _patch_token(), _patch_user_ctx():
            result = await list_dashboards(
                request=_make_request(),
                page_size=50,
                group_context=None,
            )

        assert result[0].warehouse_id is None
        assert result[0].parent_path is None


# ─────────────────────────────────────────────────────────────────────────────
# Dashboard — download
# ─────────────────────────────────────────────────────────────────────────────

class TestDownloadDashboardExport:
    @pytest.mark.asyncio
    async def test_returns_streaming_response(self):
        from src.api.analytics_export_router import download_dashboard_export
        from fastapi.responses import StreamingResponse

        mock_svc = AsyncMock()
        mock_svc.export_dashboard = AsyncMock(return_value=_dashboard_export_result())

        with _patch_service(mock_svc), _patch_token("tok"), _patch_user_ctx():
            response = await download_dashboard_export(
                dashboard_id="d123",
                request=_make_request(),
                group_context=FakeGroupCtx(),
            )

        assert isinstance(response, StreamingResponse)
        assert response.media_type == "application/zip"

    @pytest.mark.asyncio
    async def test_zip_contains_three_yaml_files(self):
        from src.api.analytics_export_router import download_dashboard_export

        mock_svc = AsyncMock()
        mock_svc.export_dashboard = AsyncMock(return_value=_dashboard_export_result())

        with _patch_service(mock_svc), _patch_token(), _patch_user_ctx():
            response = await download_dashboard_export(
                dashboard_id="d123",
                request=_make_request(),
                group_context=None,
            )

        chunks = []
        async for chunk in response.body_iterator:
            chunks.append(chunk)
        zip_bytes = b"".join(chunks)

        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = zf.namelist()

        assert "example_analytics/config.yaml" in names
        assert "example_analytics/datasets.yaml" in names
        assert "example_analytics/pages.yaml" in names

    @pytest.mark.asyncio
    async def test_zip_content_is_readable_yaml(self):
        """The YAML files inside the ZIP should parse without errors."""
        import yaml as _yaml
        from src.api.analytics_export_router import download_dashboard_export

        mock_svc = AsyncMock()
        mock_svc.export_dashboard = AsyncMock(return_value=_dashboard_export_result())

        with _patch_service(mock_svc), _patch_token(), _patch_user_ctx():
            response = await download_dashboard_export(
                dashboard_id="d123",
                request=_make_request(),
                group_context=None,
            )

        chunks = []
        async for chunk in response.body_iterator:
            chunks.append(chunk)
        zip_bytes = b"".join(chunks)

        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            for name in zf.namelist():
                content = zf.read(name).decode("utf-8")
                _yaml.safe_load(content)  # should not raise

    @pytest.mark.asyncio
    async def test_content_disposition_filename(self):
        from src.api.analytics_export_router import download_dashboard_export

        mock_svc = AsyncMock()
        mock_svc.export_dashboard = AsyncMock(return_value=_dashboard_export_result())

        with _patch_service(mock_svc), _patch_token(), _patch_user_ctx():
            response = await download_dashboard_export(
                dashboard_id="d123",
                request=_make_request(),
                group_context=None,
            )

        cd = response.headers["Content-Disposition"]
        assert "example_analytics_dashboard.zip" in cd

    @pytest.mark.asyncio
    async def test_dashboard_not_found_raises_404(self):
        from src.api.analytics_export_router import download_dashboard_export
        from src.core.exceptions import NotFoundError

        mock_svc = AsyncMock()
        mock_svc.export_dashboard = AsyncMock(side_effect=ValueError("not found"))

        with _patch_service(mock_svc), _patch_token(), _patch_user_ctx():
            with pytest.raises(NotFoundError):
                await download_dashboard_export(
                    dashboard_id="bad_id",
                    request=_make_request(),
                    group_context=None,
                )

    @pytest.mark.asyncio
    async def test_service_called_with_correct_dashboard_id(self):
        from src.api.analytics_export_router import download_dashboard_export

        mock_svc = AsyncMock()
        mock_svc.export_dashboard = AsyncMock(return_value=_dashboard_export_result())

        with _patch_service(mock_svc), _patch_token("tok"), _patch_user_ctx():
            await download_dashboard_export(
                dashboard_id="my_dashboard_id",
                request=_make_request(),
                group_context=None,
            )

        mock_svc.export_dashboard.assert_awaited_once_with("my_dashboard_id")


# ─────────────────────────────────────────────────────────────────────────────
# Dashboard — preview
# ─────────────────────────────────────────────────────────────────────────────

class TestPreviewDashboardExport:
    @pytest.mark.asyncio
    async def test_returns_json_with_files(self):
        from src.api.analytics_export_router import preview_dashboard_export
        from fastapi.responses import JSONResponse

        mock_svc = AsyncMock()
        mock_svc.export_dashboard = AsyncMock(return_value=_dashboard_export_result())

        with _patch_service(mock_svc), _patch_token(), _patch_user_ctx():
            response = await preview_dashboard_export(
                dashboard_id="d123",
                request=_make_request(),
                group_context=None,
            )

        assert isinstance(response, JSONResponse)
        body = json.loads(response.body)
        assert body["dashboard_id"] == "d123"
        assert body["file_count"] == 3
        assert len(body["files"]) == 3

    @pytest.mark.asyncio
    async def test_preview_paths_include_folder_prefix(self):
        from src.api.analytics_export_router import preview_dashboard_export

        mock_svc = AsyncMock()
        mock_svc.export_dashboard = AsyncMock(return_value=_dashboard_export_result())

        with _patch_service(mock_svc), _patch_token(), _patch_user_ctx():
            response = await preview_dashboard_export(
                dashboard_id="d123",
                request=_make_request(),
                group_context=None,
            )

        body = json.loads(response.body)
        paths = [f["path"] for f in body["files"]]
        assert "example_analytics/config.yaml" in paths
        assert "example_analytics/datasets.yaml" in paths
        assert "example_analytics/pages.yaml" in paths

    @pytest.mark.asyncio
    async def test_preview_not_found_raises_404(self):
        from src.api.analytics_export_router import preview_dashboard_export
        from src.core.exceptions import NotFoundError

        mock_svc = AsyncMock()
        mock_svc.export_dashboard = AsyncMock(side_effect=ValueError("not found"))

        with _patch_service(mock_svc), _patch_token(), _patch_user_ctx():
            with pytest.raises(NotFoundError):
                await preview_dashboard_export(
                    dashboard_id="bad_id",
                    request=_make_request(),
                    group_context=None,
                )

    @pytest.mark.asyncio
    async def test_yaml_content_present_in_preview(self):
        """Each file in preview response should contain YAML text."""
        from src.api.analytics_export_router import preview_dashboard_export

        mock_svc = AsyncMock()
        mock_svc.export_dashboard = AsyncMock(return_value=_dashboard_export_result())

        with _patch_service(mock_svc), _patch_token(), _patch_user_ctx():
            response = await preview_dashboard_export(
                dashboard_id="d123",
                request=_make_request(),
                group_context=None,
            )

        body = json.loads(response.body)
        config_file = next(f for f in body["files"] if f["path"].endswith("config.yaml"))
        assert "lakeview/v1" in config_file["content"]
