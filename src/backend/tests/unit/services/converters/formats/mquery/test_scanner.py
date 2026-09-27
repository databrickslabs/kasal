"""
Unit tests for the PowerBIAdminScanner class.

Tests scanner initialization, API URL building, scan initiation,
scan status polling, result parsing, and relationship enrichment.
All httpx calls are mocked to avoid real network dependencies.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.converters.formats.mquery.models import (
    ExpressionType,
    MQueryConversionConfig,
    MQueryExpression,
    PowerBITable,
    ScanStatus,
    SemanticModel,
    StorageMode,
)
from src.services.converters.formats.mquery.scanner import PowerBIAdminScanner

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(**kwargs):
    defaults = dict(
        tenant_id="ten-1",
        client_id="cli-1",
        client_secret="sec-1",
        workspace_id="ws-1",
    )
    defaults.update(kwargs)
    return MQueryConversionConfig(**defaults)


def _make_scanner(access_token: str = "tok", **config_kwargs):
    cfg = _make_config(**config_kwargs)
    return PowerBIAdminScanner(access_token=access_token, config=cfg)


def _make_httpx_response(status_code: int = 200, json_data: dict = None):
    """Create a mock httpx response."""
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.json.return_value = json_data or {}
    mock_resp.raise_for_status = MagicMock()
    return mock_resp


def _make_semantic_model_data(
    ws_id: str = "ws-1",
    ws_name: str = "MyWorkspace",
    dataset_id: str = "ds-1",
    dataset_name: str = "SalesModel",
    tables: list = None,
):
    return {
        "workspaces": [
            {
                "id": ws_id,
                "name": ws_name,
                "datasets": [
                    {
                        "id": dataset_id,
                        "name": dataset_name,
                        "tables": tables or [],
                        "relationships": [],
                    }
                ],
            }
        ]
    }


# ---------------------------------------------------------------------------
# Initialization tests
# ---------------------------------------------------------------------------


def test_scanner_init_with_config():
    """Scanner stores access_token and config correctly."""
    cfg = _make_config(workspace_id="ws-abc")
    scanner = PowerBIAdminScanner(access_token="mytoken", config=cfg)
    assert scanner.access_token == "mytoken"
    assert scanner.config.workspace_id == "ws-abc"
    assert scanner._client is None


def test_scanner_init_without_config_uses_defaults():
    """Scanner without explicit config creates a default MQueryConversionConfig."""
    scanner = PowerBIAdminScanner(access_token="tok")
    assert scanner.config is not None
    assert scanner.access_token == "tok"


# ---------------------------------------------------------------------------
# _get_headers tests
# ---------------------------------------------------------------------------


def test_get_headers_includes_bearer_token():
    """_get_headers returns Authorization header with Bearer token."""
    scanner = _make_scanner(access_token="my-token-123")
    headers = scanner._get_headers()
    assert headers["Authorization"] == "Bearer my-token-123"
    assert headers["Content-Type"] == "application/json"


# ---------------------------------------------------------------------------
# _build_scan_url tests
# ---------------------------------------------------------------------------


def test_build_scan_url_all_options_enabled():
    """_build_scan_url includes all query params when all options are True."""
    cfg = _make_config(
        include_lineage=True,
        include_datasource_details=True,
        include_dataset_schema=True,
        include_dataset_expressions=True,
        include_artifact_users=True,
    )
    scanner = PowerBIAdminScanner(access_token="tok", config=cfg)
    url = scanner._build_scan_url()
    assert "lineage=True" in url
    assert "datasourceDetails=True" in url
    assert "datasetSchema=True" in url
    assert "datasetExpressions=True" in url
    assert "getArtifactUsers=True" in url


def test_build_scan_url_no_options():
    """_build_scan_url omits params when options are False."""
    cfg = _make_config(
        include_lineage=False,
        include_datasource_details=False,
        include_dataset_schema=False,
        include_dataset_expressions=False,
        include_artifact_users=False,
    )
    scanner = PowerBIAdminScanner(access_token="tok", config=cfg)
    url = scanner._build_scan_url()
    assert "lineage" not in url
    assert "datasourceDetails" not in url
    assert "getArtifactUsers" not in url


# ---------------------------------------------------------------------------
# Async context manager tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_context_manager_creates_and_closes_client():
    """Async context manager creates and closes httpx.AsyncClient."""
    scanner = _make_scanner()
    mock_client = AsyncMock()
    mock_client.aclose = AsyncMock()

    with patch(
        "src.services.converters.formats.mquery.scanner.httpx.AsyncClient"
    ) as mock_cls:
        mock_instance = AsyncMock()
        mock_instance.aclose = AsyncMock()
        mock_cls.return_value = mock_instance
        async with scanner:
            assert scanner._client is mock_instance
        mock_instance.aclose.assert_called_once()


# ---------------------------------------------------------------------------
# initiate_scan tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_initiate_scan_returns_scan_status():
    """initiate_scan posts to Power BI API and returns ScanStatus."""
    scanner = _make_scanner()

    response_data = {"id": "scan-abc-123", "createdDateTime": "2024-01-01T00:00:00Z"}
    mock_resp = _make_httpx_response(json_data=response_data)
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_resp)

    with patch("httpx.AsyncClient") as mock_cls:
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)
        status = await scanner.initiate_scan(["ws-1", "ws-2"])

    assert isinstance(status, ScanStatus)
    assert status.scan_id == "scan-abc-123"
    assert status.status == "Running"


@pytest.mark.asyncio
async def test_initiate_scan_uses_existing_client():
    """initiate_scan uses _client if already set (context manager mode)."""
    scanner = _make_scanner()
    mock_client = AsyncMock()
    response_data = {"id": "scan-xyz", "createdDateTime": None}
    mock_resp = _make_httpx_response(json_data=response_data)
    mock_client.post = AsyncMock(return_value=mock_resp)
    scanner._client = mock_client

    status = await scanner.initiate_scan(["ws-1"])
    assert status.scan_id == "scan-xyz"
    mock_client.post.assert_called_once()


# ---------------------------------------------------------------------------
# check_scan_status tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_scan_status_succeeded():
    """check_scan_status returns Succeeded status."""
    scanner = _make_scanner()

    response_data = {"status": "Succeeded"}
    mock_resp = _make_httpx_response(json_data=response_data)
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_resp)

    with patch("httpx.AsyncClient") as mock_cls:
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)
        status = await scanner.check_scan_status("scan-abc")

    assert status.status == "Succeeded"
    assert status.scan_id == "scan-abc"


@pytest.mark.asyncio
async def test_check_scan_status_uses_existing_client():
    """check_scan_status uses _client if already set."""
    scanner = _make_scanner()
    mock_client = AsyncMock()
    response_data = {"status": "Running"}
    mock_resp = _make_httpx_response(json_data=response_data)
    mock_client.get = AsyncMock(return_value=mock_resp)
    scanner._client = mock_client

    status = await scanner.check_scan_status("scan-123")
    assert status.status == "Running"


@pytest.mark.asyncio
async def test_check_scan_status_with_error():
    """check_scan_status returns error info when scan has failed."""
    scanner = _make_scanner()

    response_data = {"status": "Failed", "error": "Timeout"}
    mock_resp = _make_httpx_response(json_data=response_data)
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_resp)
    scanner._client = mock_client

    status = await scanner.check_scan_status("scan-err")
    assert status.status == "Failed"
    assert status.error == "Timeout"


# ---------------------------------------------------------------------------
# get_scan_result tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_scan_result_returns_raw_data():
    """get_scan_result returns raw JSON data from API."""
    scanner = _make_scanner()

    raw_data = {"workspaces": [{"id": "ws-1", "datasets": []}]}
    mock_resp = _make_httpx_response(json_data=raw_data)
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_resp)
    scanner._client = mock_client

    result = await scanner.get_scan_result("scan-done")
    assert result == raw_data


@pytest.mark.asyncio
async def test_get_scan_result_uses_httpx_when_no_client():
    """get_scan_result creates a new httpx.AsyncClient when no client set."""
    scanner = _make_scanner()

    raw_data = {"workspaces": []}
    mock_resp = _make_httpx_response(json_data=raw_data)
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_resp)

    with patch("httpx.AsyncClient") as mock_cls:
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)
        result = await scanner.get_scan_result("scan-123")

    assert result == raw_data


# ---------------------------------------------------------------------------
# wait_for_scan tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_wait_for_scan_succeeds_immediately():
    """wait_for_scan returns immediately when scan is Succeeded."""
    scanner = _make_scanner()

    async def mock_check(scan_id):
        return ScanStatus(scan_id=scan_id, status="Succeeded")

    scanner.check_scan_status = mock_check

    status = await scanner.wait_for_scan("scan-1", timeout_seconds=10, poll_interval=1)
    assert status.status == "Succeeded"


@pytest.mark.asyncio
async def test_wait_for_scan_fails():
    """wait_for_scan returns Failed status without polling further."""
    scanner = _make_scanner()
    call_count = 0

    async def mock_check(scan_id):
        nonlocal call_count
        call_count += 1
        return ScanStatus(scan_id=scan_id, status="Failed", error="Dataset not found")

    scanner.check_scan_status = mock_check

    status = await scanner.wait_for_scan(
        "scan-fail", timeout_seconds=10, poll_interval=1
    )
    assert status.status == "Failed"
    assert call_count == 1  # Should stop after first Failed status


@pytest.mark.asyncio
async def test_wait_for_scan_times_out():
    """wait_for_scan raises TimeoutError when scan doesn't complete in time."""
    scanner = _make_scanner()

    async def mock_check(scan_id):
        return ScanStatus(scan_id=scan_id, status="Running")

    scanner.check_scan_status = mock_check

    with pytest.raises(TimeoutError):
        await scanner.wait_for_scan("scan-slow", timeout_seconds=0, poll_interval=1)


@pytest.mark.asyncio
async def test_wait_for_scan_polls_until_done():
    """wait_for_scan polls multiple times before succeeding."""
    scanner = _make_scanner()
    call_count = 0

    async def mock_check(scan_id):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            return ScanStatus(scan_id=scan_id, status="Running")
        return ScanStatus(scan_id=scan_id, status="Succeeded")

    with patch("asyncio.sleep", new_callable=AsyncMock):
        scanner.check_scan_status = mock_check
        status = await scanner.wait_for_scan(
            "scan-poll", timeout_seconds=300, poll_interval=1
        )

    assert status.status == "Succeeded"
    assert call_count == 3


# ---------------------------------------------------------------------------
# scan_workspace tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scan_workspace_returns_semantic_models():
    """scan_workspace returns parsed SemanticModels from raw API data."""
    scanner = _make_scanner()

    raw_data = _make_semantic_model_data(
        ws_id="ws-1",
        ws_name="Workspace1",
        dataset_id="ds-1",
        dataset_name="SalesModel",
    )

    # Mock initiate_scan and wait_for_scan and get_scan_result
    async def mock_initiate(workspace_ids):
        return ScanStatus(scan_id="scan-1", status="Running")

    async def mock_wait(scan_id, **kwargs):
        return ScanStatus(scan_id=scan_id, status="Succeeded")

    async def mock_get_result(scan_id):
        return raw_data

    scanner.initiate_scan = mock_initiate
    scanner.wait_for_scan = mock_wait
    scanner.get_scan_result = mock_get_result

    models, raw = await scanner.scan_workspace("ws-1")

    assert len(models) == 1
    assert models[0].name == "SalesModel"
    assert models[0].workspace_id == "ws-1"
    assert raw == raw_data


@pytest.mark.asyncio
async def test_scan_workspace_filters_by_dataset_id():
    """scan_workspace filters results to the specified dataset_id."""
    scanner = _make_scanner()

    raw_data = {
        "workspaces": [
            {
                "id": "ws-1",
                "name": "WS1",
                "datasets": [
                    {"id": "ds-1", "name": "Model1", "tables": [], "relationships": []},
                    {"id": "ds-2", "name": "Model2", "tables": [], "relationships": []},
                ],
            }
        ]
    }

    async def mock_initiate(workspace_ids):
        return ScanStatus(scan_id="scan-1", status="Running")

    async def mock_wait(scan_id, **kwargs):
        return ScanStatus(scan_id=scan_id, status="Succeeded")

    async def mock_get_result(scan_id):
        return raw_data

    scanner.initiate_scan = mock_initiate
    scanner.wait_for_scan = mock_wait
    scanner.get_scan_result = mock_get_result

    # Only request ds-2
    models, _ = await scanner.scan_workspace("ws-1", dataset_id="ds-2")
    assert len(models) == 1
    assert models[0].name == "Model2"


@pytest.mark.asyncio
async def test_scan_workspace_raises_on_failed_scan():
    """scan_workspace raises RuntimeError when scan fails."""
    scanner = _make_scanner()

    async def mock_initiate(workspace_ids):
        return ScanStatus(scan_id="scan-fail", status="Running")

    async def mock_wait(scan_id, **kwargs):
        return ScanStatus(scan_id=scan_id, status="Failed", error="Access denied")

    scanner.initiate_scan = mock_initiate
    scanner.wait_for_scan = mock_wait

    with pytest.raises(RuntimeError, match="Scan failed"):
        await scanner.scan_workspace("ws-1")


# ---------------------------------------------------------------------------
# scan_multiple_workspaces tests
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# extract_tables_with_mquery tests
# ---------------------------------------------------------------------------


def _make_table_obj(
    name: str, is_hidden: bool = False, with_expr: bool = True
) -> PowerBITable:
    exprs = (
        [
            MQueryExpression(
                raw_expression='Sql.Database("s", "d")',
                expression_type=ExpressionType.SQL_DATABASE,
            )
        ]
        if with_expr
        else []
    )
    return PowerBITable(
        name=name,
        is_hidden=is_hidden,
        storage_mode=StorageMode.IMPORT,
        columns=[],
        measures=[],
        source_expressions=exprs,
    )


def _make_model(tables: list) -> SemanticModel:
    return SemanticModel(
        id="ds-1",
        name="Model",
        tables=tables,
        relationships=[],
        workspace_id="ws-1",
    )


def test_extract_tables_with_mquery_excludes_hidden_by_default():
    """extract_tables_with_mquery skips hidden tables unless include_hidden=True."""
    scanner = _make_scanner()
    model = _make_model(
        [
            _make_table_obj("Visible", is_hidden=False),
            _make_table_obj("Hidden", is_hidden=True),
        ]
    )
    tables = scanner.extract_tables_with_mquery(model, include_hidden=False)
    names = [t.name for t in tables]
    assert "Visible" in names
    assert "Hidden" not in names


def test_extract_tables_with_mquery_includes_hidden_when_requested():
    """extract_tables_with_mquery includes hidden tables when include_hidden=True."""
    scanner = _make_scanner()
    model = _make_model(
        [
            _make_table_obj("Visible", is_hidden=False),
            _make_table_obj("Hidden", is_hidden=True),
        ]
    )
    tables = scanner.extract_tables_with_mquery(model, include_hidden=True)
    names = [t.name for t in tables]
    assert "Visible" in names
    assert "Hidden" in names


def test_extract_tables_with_mquery_excludes_tables_without_expressions():
    """extract_tables_with_mquery excludes tables with no source expressions."""
    scanner = _make_scanner()
    model = _make_model(
        [
            _make_table_obj("HasExpr", with_expr=True),
            _make_table_obj("NoExpr", with_expr=False),
        ]
    )
    tables = scanner.extract_tables_with_mquery(model)
    names = [t.name for t in tables]
    assert "HasExpr" in names
    assert "NoExpr" not in names


# ---------------------------------------------------------------------------
# fetch_relationships_via_execute_queries tests
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# enrich_model_with_relationships tests
# ---------------------------------------------------------------------------
