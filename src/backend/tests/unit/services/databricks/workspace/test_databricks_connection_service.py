"""
Unit tests for DatabricksConnectionService.

Tests connection testing and authentication for Databricks Vector Search.
"""

from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from aiohttp import ClientSession

from src.core.unit_of_work import UnitOfWork
from src.schemas.memory_backend import DatabricksMemoryConfig
from src.services.databricks.workspace.connection import DatabricksConnectionService


@pytest.fixture
def mock_uow():
    """Create a mock Unit of Work."""
    uow = AsyncMock(spec=UnitOfWork)
    return uow


@pytest.fixture
def service(mock_uow):
    """Create a DatabricksConnectionService instance."""
    return DatabricksConnectionService(mock_uow)


@pytest.fixture
def databricks_config():
    """Create a sample Databricks configuration."""
    return DatabricksMemoryConfig(
        memory_index="catalog.schema.memory_index",
        endpoint_name="test-endpoint",
        document_index="ml.agents.document",
        workspace_url="https://test.databricks.com",
        embedding_dimension=768,
        auth_type="pat",
        personal_access_token="test-token",
    )


class TestDatabricksConnectionService:
    """Test cases for DatabricksConnectionService."""

    @pytest.mark.asyncio
    @patch(
        "src.services.databricks.workspace.connection.DatabricksVectorIndexRepository"
    )
    @patch(
        "src.services.databricks.workspace.connection.DatabricksVectorEndpointRepository"
    )
    async def test_test_connection_repository_success(
        self,
        mock_endpoint_repo_class,
        mock_index_repo_class,
        service,
        databricks_config,
    ):
        """Test successful connection using repository pattern."""
        # Arrange
        user_token = "user-token-123"

        # Mock endpoint repository
        mock_endpoint_repo = AsyncMock()
        mock_endpoint_repo.get_endpoint_status.return_value = {
            "success": True,
            "endpoint": {"endpoint_status": {"state": "ONLINE"}},
            "status": "ONLINE",
        }
        mock_endpoint_repo_class.return_value = mock_endpoint_repo

        # Mock index repository
        mock_index_repo = AsyncMock()
        from src.schemas.databricks_vector_index import (
            IndexInfo,
            IndexResponse,
            IndexState,
        )

        # Create mock responses for each index
        short_term_response = IndexResponse(
            success=True,
            index=IndexInfo(
                name="ml.agents.short_term",
                endpoint_name="test-endpoint",
                state=IndexState.READY,
                ready=True,
            ),
        )

        long_term_response = IndexResponse(
            success=True,
            index=IndexInfo(
                name="ml.agents.long_term",
                endpoint_name="test-endpoint",
                state=IndexState.READY,
                ready=True,
            ),
        )

        entity_response = IndexResponse(success=False, error="Index not found")

        mock_index_repo.get_index.side_effect = [
            short_term_response,  # memory_index → found (source checks the unified memory_index only)
        ]
        mock_index_repo_class.return_value = mock_index_repo

        # Act
        result = await service.test_databricks_connection(databricks_config, user_token)

        # Assert (source verifies the unified memory_index only → 1 found, 0 missing)
        assert result["success"] is True
        assert "Successfully connected" in result["message"]
        assert len(result["details"]["indexes_found"]) == 1
        assert len(result["details"]["indexes_missing"]) == 0

        # Verify repository methods were called
        mock_endpoint_repo.get_endpoint_status.assert_called_once_with(
            "test-endpoint", user_token
        )
        assert mock_index_repo.get_index.call_count == 1  # unified memory_index only

    @pytest.mark.asyncio
    @patch(
        "src.services.databricks.workspace.connection.DatabricksVectorEndpointRepository"
    )
    async def test_test_connection_repository_failure(
        self, mock_endpoint_repo_class, service, databricks_config
    ):
        """Test connection failure when repository operations fail."""
        # Arrange
        # Mock endpoint repository to return failure
        mock_endpoint_repo = AsyncMock()
        mock_endpoint_repo.get_endpoint_status.return_value = {
            "success": False,
            "message": "Endpoint not found",
            "error": "404: Endpoint does not exist",
        }
        mock_endpoint_repo_class.return_value = mock_endpoint_repo

        # Act
        result = await service.test_databricks_connection(databricks_config, None)

        # Assert
        assert result["success"] is False
        assert "Endpoint not found" in result["message"]
        assert "404: Endpoint does not exist" in result["details"]["error"]

    @pytest.mark.asyncio
    @patch(
        "src.services.databricks.workspace.connection.DatabricksVectorEndpointRepository"
    )
    async def test_get_endpoint_status_success(self, mock_endpoint_repo_class, service):
        """Test getting endpoint status successfully using repository."""
        # Arrange
        workspace_url = "https://test.databricks.com"
        endpoint_name = "test-endpoint"
        user_token = "user-token"

        # Mock repository instance and its method
        mock_repo = AsyncMock()
        mock_endpoint_repo_class.return_value = mock_repo
        mock_repo.get_endpoint_status = AsyncMock(
            return_value={
                "success": True,
                "endpoint": {
                    "endpoint_status": {
                        "state": "ONLINE",
                        "message": "Endpoint is ready",
                    },
                    "endpoint_type": "STANDARD",
                },
                "status": "ONLINE",
            }
        )

        # Act
        result = await service.get_databricks_endpoint_status(
            workspace_url, endpoint_name, user_token
        )

        # Assert
        assert result["success"] is True
        assert result["endpoint"]["endpoint_status"]["state"] == "ONLINE"
        assert result["status"] == "ONLINE"
        # Verify repository was called correctly
        mock_endpoint_repo_class.assert_called_once_with(workspace_url)
        mock_repo.get_endpoint_status.assert_called_once_with(endpoint_name, user_token)

    @pytest.mark.asyncio
    @patch(
        "src.services.databricks.workspace.connection.DatabricksVectorEndpointRepository"
    )
    async def test_get_endpoint_status_not_found(
        self, mock_endpoint_repo_class, service
    ):
        """Test getting endpoint status when endpoint not found using repository."""
        # Arrange
        workspace_url = "https://test.databricks.com"
        endpoint_name = "missing-endpoint"

        # Mock repository instance and its method
        mock_repo = AsyncMock()
        mock_endpoint_repo_class.return_value = mock_repo
        mock_repo.get_endpoint_status = AsyncMock(
            return_value={
                "success": False,
                "message": f"Endpoint {endpoint_name} not found",
                "status": "not_found",
            }
        )

        # Act
        result = await service.get_databricks_endpoint_status(
            workspace_url, endpoint_name, None
        )

        # Assert
        assert result["success"] is False
        assert result["message"] == f"Endpoint {endpoint_name} not found"
        assert result["status"] == "not_found"
        # Verify repository was called correctly
        mock_endpoint_repo_class.assert_called_once_with(workspace_url)
        mock_repo.get_endpoint_status.assert_called_once_with(endpoint_name, None)


# ---------------------------------------------------------------------------
# Additional branch coverage: inner exception handlers, auth token retrieval,
# and endpoint-status exception path (merged from
# test_databricks_connection_service_coverage.py)
# ---------------------------------------------------------------------------


def _make_bare_service():
    from src.services.databricks.workspace.connection import DatabricksConnectionService

    return DatabricksConnectionService(session=None)


def _make_bare_config(
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


@pytest.mark.asyncio
async def test_connection_inner_exception():
    """Test inner exception handler (lines 105-113)."""
    svc = _make_bare_service()
    cfg = _make_bare_config()

    with (
        patch(
            "src.services.databricks.workspace.connection.DatabricksVectorEndpointRepository"
        ) as MockEndpointRepo,
        patch(
            "src.services.databricks.workspace.connection.DatabricksVectorIndexRepository"
        ),
    ):
        mock_endpoint = AsyncMock()
        mock_endpoint.get_endpoint_status = AsyncMock(
            side_effect=Exception("network error")
        )
        MockEndpointRepo.return_value = mock_endpoint

        result = await svc.test_databricks_connection(cfg)

    assert result["success"] is False
    assert "Failed to get endpoint info" in result["message"]


@pytest.mark.asyncio
async def test_connection_endpoint_failure():
    """Test when endpoint returns not success."""
    svc = _make_bare_service()
    cfg = _make_bare_config()

    with (
        patch(
            "src.services.databricks.workspace.connection.DatabricksVectorEndpointRepository"
        ) as MockEndpointRepo,
        patch(
            "src.services.databricks.workspace.connection.DatabricksVectorIndexRepository"
        ),
    ):
        mock_endpoint = AsyncMock()
        mock_endpoint.get_endpoint_status = AsyncMock(
            return_value={
                "success": False,
                "message": "Endpoint not found",
                "error": "NOT_FOUND",
            }
        )
        MockEndpointRepo.return_value = mock_endpoint

        result = await svc.test_databricks_connection(cfg)

    assert result["success"] is False


@pytest.mark.asyncio
async def test_connection_import_error():
    """Test ImportError handler (lines 115-122)."""
    svc = _make_bare_service()
    cfg = _make_bare_config()

    with patch(
        "src.services.databricks.workspace.connection.DatabricksVectorEndpointRepository",
        side_effect=ImportError("package not installed"),
    ):
        result = await svc.test_databricks_connection(cfg)

    assert result["success"] is False
    assert "not installed" in result["message"]


@pytest.mark.asyncio
async def test_connection_outer_exception():
    """Test outer exception handler."""
    svc = _make_bare_service()
    cfg = _make_bare_config()

    with patch(
        "src.services.databricks.workspace.connection.DatabricksVectorEndpointRepository",
        side_effect=RuntimeError("unexpected error"),
    ):
        result = await svc.test_databricks_connection(cfg)

    assert result["success"] is False


@pytest.mark.asyncio
async def test_get_auth_token_success():
    """Test successful auth token retrieval."""
    svc = _make_bare_service()

    mock_auth = MagicMock()
    mock_auth.token = "tok_123"
    mock_auth.auth_method = "oauth"

    with patch(
        "src.services.databricks.workspace.connection.get_auth_context",
        new_callable=AsyncMock,
        return_value=mock_auth,
    ):
        token, method = await svc.get_databricks_auth_token("https://example.com")

    assert token == "tok_123"
    assert method == "oauth"


@pytest.mark.asyncio
async def test_get_auth_token_no_auth_raises():
    """Test ValueError when no auth available."""
    svc = _make_bare_service()

    with patch(
        "src.services.databricks.workspace.connection.get_auth_context",
        new_callable=AsyncMock,
        return_value=None,
    ):
        with pytest.raises(ValueError, match="No authentication token"):
            await svc.get_databricks_auth_token("https://example.com")


@pytest.mark.asyncio
async def test_get_auth_token_exception_raises():
    """Test ValueError on exception."""
    svc = _make_bare_service()

    with patch(
        "src.services.databricks.workspace.connection.get_auth_context",
        new_callable=AsyncMock,
        side_effect=Exception("auth error"),
    ):
        with pytest.raises(ValueError, match="All authentication methods failed"):
            await svc.get_databricks_auth_token("https://example.com")


@pytest.mark.asyncio
async def test_get_endpoint_status_exception():
    """Test exception handler in get_databricks_endpoint_status."""
    svc = _make_bare_service()

    with patch(
        "src.services.databricks.workspace.connection.DatabricksVectorEndpointRepository",
        side_effect=Exception("connection refused"),
    ):
        result = await svc.get_databricks_endpoint_status("https://example.com", "ep1")

    assert result["success"] is False
    assert "Failed to get endpoint status" in result["message"]


@pytest.mark.asyncio
async def test_get_endpoint_status_success_bare_service():
    """Test successful endpoint status retrieval (bare service, no repository mocks on class)."""
    svc = _make_bare_service()

    with patch(
        "src.services.databricks.workspace.connection.DatabricksVectorEndpointRepository"
    ) as MockRepo:
        mock_repo_instance = AsyncMock()
        mock_repo_instance.get_endpoint_status = AsyncMock(
            return_value={"success": True, "status": "ONLINE"}
        )
        MockRepo.return_value = mock_repo_instance

        result = await svc.get_databricks_endpoint_status("https://example.com", "ep1")

    assert result["success"] is True
