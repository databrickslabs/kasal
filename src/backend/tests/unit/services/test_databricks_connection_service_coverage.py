"""
Coverage tests for services/databricks_connection_service.py
Covers: inner exception handler (105-124), get_databricks_auth_token (146-155), endpoint_status exception (188-190)
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any


def make_service():
    from src.services.databricks_connection_service import DatabricksConnectionService
    return DatabricksConnectionService(session=None)


def make_config(
    workspace_url="https://example.com",
    endpoint_name="ep1",
    short_term_index="cat.sch.idx",
    long_term_index=None,
    entity_index=None,
):
    cfg = MagicMock()
    cfg.workspace_url = workspace_url
    cfg.endpoint_name = endpoint_name
    cfg.short_term_index = short_term_index
    cfg.long_term_index = long_term_index
    cfg.entity_index = entity_index
    return cfg


# ---- test_databricks_connection — inner exception ----

@pytest.mark.asyncio
async def test_connection_inner_exception():
    """Test inner exception handler (lines 105-113)."""
    svc = make_service()
    cfg = make_config()

    with patch('src.services.databricks_connection_service.DatabricksVectorEndpointRepository') as MockEndpointRepo, \
         patch('src.services.databricks_connection_service.DatabricksVectorIndexRepository'):
        mock_endpoint = AsyncMock()
        mock_endpoint.get_endpoint_status = AsyncMock(side_effect=Exception("network error"))
        MockEndpointRepo.return_value = mock_endpoint

        result = await svc.test_databricks_connection(cfg)

    assert result["success"] is False
    assert "Failed to get endpoint info" in result["message"]


@pytest.mark.asyncio
async def test_connection_endpoint_failure():
    """Test when endpoint returns not success."""
    svc = make_service()
    cfg = make_config()

    with patch('src.services.databricks_connection_service.DatabricksVectorEndpointRepository') as MockEndpointRepo, \
         patch('src.services.databricks_connection_service.DatabricksVectorIndexRepository'):
        mock_endpoint = AsyncMock()
        mock_endpoint.get_endpoint_status = AsyncMock(return_value={
            "success": False,
            "message": "Endpoint not found",
            "error": "NOT_FOUND"
        })
        MockEndpointRepo.return_value = mock_endpoint

        result = await svc.test_databricks_connection(cfg)

    assert result["success"] is False


@pytest.mark.asyncio
async def test_connection_import_error():
    """Test ImportError handler (lines 115-122)."""
    svc = make_service()
    cfg = make_config()

    with patch('src.services.databricks_connection_service.DatabricksVectorEndpointRepository',
               side_effect=ImportError("package not installed")):
        result = await svc.test_databricks_connection(cfg)

    assert result["success"] is False
    assert "not installed" in result["message"]


@pytest.mark.asyncio
async def test_connection_outer_exception():
    """Test outer exception handler."""
    svc = make_service()
    cfg = make_config()

    with patch('src.services.databricks_connection_service.DatabricksVectorEndpointRepository',
               side_effect=RuntimeError("unexpected error")):
        result = await svc.test_databricks_connection(cfg)

    assert result["success"] is False


# ---- get_databricks_auth_token ----

@pytest.mark.asyncio
async def test_get_auth_token_success():
    """Test successful auth token retrieval."""
    svc = make_service()

    mock_auth = MagicMock()
    mock_auth.token = "tok_123"
    mock_auth.auth_method = "oauth"

    with patch('src.services.databricks_connection_service.get_auth_context', new_callable=AsyncMock, return_value=mock_auth):
        token, method = await svc.get_databricks_auth_token("https://example.com")

    assert token == "tok_123"
    assert method == "oauth"


@pytest.mark.asyncio
async def test_get_auth_token_no_auth_raises():
    """Test ValueError when no auth available."""
    svc = make_service()

    with patch('src.services.databricks_connection_service.get_auth_context', new_callable=AsyncMock, return_value=None):
        with pytest.raises(ValueError, match="No authentication token"):
            await svc.get_databricks_auth_token("https://example.com")


@pytest.mark.asyncio
async def test_get_auth_token_exception_raises():
    """Test ValueError on exception."""
    svc = make_service()

    with patch('src.services.databricks_connection_service.get_auth_context',
               new_callable=AsyncMock,
               side_effect=Exception("auth error")):
        with pytest.raises(ValueError, match="All authentication methods failed"):
            await svc.get_databricks_auth_token("https://example.com")


# ---- get_databricks_endpoint_status — exception ----

@pytest.mark.asyncio
async def test_get_endpoint_status_exception():
    """Test exception handler in get_databricks_endpoint_status."""
    svc = make_service()

    with patch('src.services.databricks_connection_service.DatabricksVectorEndpointRepository',
               side_effect=Exception("connection refused")):
        result = await svc.get_databricks_endpoint_status("https://example.com", "ep1")

    assert result["success"] is False
    assert "Failed to get endpoint status" in result["message"]


@pytest.mark.asyncio
async def test_get_endpoint_status_success():
    """Test successful endpoint status retrieval."""
    svc = make_service()

    with patch('src.services.databricks_connection_service.DatabricksVectorEndpointRepository') as MockRepo:
        mock_repo_instance = AsyncMock()
        mock_repo_instance.get_endpoint_status = AsyncMock(return_value={
            "success": True,
            "status": "ONLINE"
        })
        MockRepo.return_value = mock_repo_instance

        result = await svc.get_databricks_endpoint_status("https://example.com", "ep1")

    assert result["success"] is True
