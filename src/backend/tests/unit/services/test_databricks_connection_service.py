"""
Unit tests for DatabricksConnectionService.

Tests connection testing and authentication for Databricks Vector Search.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, Mock
from aiohttp import ClientSession

from src.services.databricks_connection_service import DatabricksConnectionService
from src.schemas.memory_backend import DatabricksMemoryConfig
from src.core.unit_of_work import UnitOfWork


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
        endpoint_name="test-endpoint",
        short_term_index="ml.agents.short_term",
        long_term_index="ml.agents.long_term",
        entity_index="ml.agents.entity",
        workspace_url="https://test.databricks.com",
        embedding_dimension=768,
        auth_type="pat",
        personal_access_token="test-token"
    )


class TestDatabricksConnectionService:
    """Test cases for DatabricksConnectionService."""
    
    @pytest.mark.asyncio
    @patch('src.services.databricks_connection_service.DatabricksVectorIndexRepository')
    @patch('src.services.databricks_connection_service.DatabricksVectorEndpointRepository')
    async def test_test_connection_repository_success(
        self, mock_endpoint_repo_class, mock_index_repo_class, service, databricks_config
    ):
        """Test successful connection using repository pattern."""
        # Arrange
        user_token = "user-token-123"
        
        # Mock endpoint repository
        mock_endpoint_repo = AsyncMock()
        mock_endpoint_repo.get_endpoint_status.return_value = {
            "success": True,
            "endpoint": {"endpoint_status": {"state": "ONLINE"}},
            "status": "ONLINE"
        }
        mock_endpoint_repo_class.return_value = mock_endpoint_repo
        
        # Mock index repository
        mock_index_repo = AsyncMock()
        from src.schemas.databricks_vector_index import IndexResponse, IndexInfo, IndexState
        
        # Create mock responses for each index
        short_term_response = IndexResponse(
            success=True,
            index=IndexInfo(
                name="ml.agents.short_term",
                endpoint_name="test-endpoint",
                state=IndexState.READY,
                ready=True
            )
        )
        
        long_term_response = IndexResponse(
            success=True,
            index=IndexInfo(
                name="ml.agents.long_term",
                endpoint_name="test-endpoint",
                state=IndexState.READY,
                ready=True
            )
        )
        
        entity_response = IndexResponse(
            success=False,
            error="Index not found"
        )
        
        mock_index_repo.get_index.side_effect = [
            short_term_response,  # short_term_index
            long_term_response,   # long_term_index
            entity_response       # entity_index
        ]
        mock_index_repo_class.return_value = mock_index_repo
        
        # Act
        result = await service.test_databricks_connection(databricks_config, user_token)
        
        # Assert
        assert result["success"] is True
        assert "Successfully connected" in result["message"]
        assert len(result["details"]["indexes_found"]) == 2
        assert len(result["details"]["indexes_missing"]) == 1
        
        # Verify repository methods were called
        mock_endpoint_repo.get_endpoint_status.assert_called_once_with("test-endpoint", user_token)
        assert mock_index_repo.get_index.call_count == 3
    
    @pytest.mark.asyncio
    @patch('src.services.databricks_connection_service.DatabricksVectorEndpointRepository')
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
            "error": "404: Endpoint does not exist"
        }
        mock_endpoint_repo_class.return_value = mock_endpoint_repo
        
        # Act
        result = await service.test_databricks_connection(databricks_config, None)
        
        # Assert
        assert result["success"] is False
        assert "Endpoint not found" in result["message"]
        assert "404: Endpoint does not exist" in result["details"]["error"]
    
    
    
    @pytest.mark.asyncio
    @patch('src.services.databricks_connection_service.DatabricksVectorEndpointRepository')
    async def test_get_endpoint_status_success(
        self, mock_endpoint_repo_class, service
    ):
        """Test getting endpoint status successfully using repository."""
        # Arrange
        workspace_url = "https://test.databricks.com"
        endpoint_name = "test-endpoint"
        user_token = "user-token"
        
        # Mock repository instance and its method
        mock_repo = AsyncMock()
        mock_endpoint_repo_class.return_value = mock_repo
        mock_repo.get_endpoint_status = AsyncMock(return_value={
            "success": True,
            "endpoint": {
                "endpoint_status": {
                    "state": "ONLINE",
                    "message": "Endpoint is ready"
                },
                "endpoint_type": "STANDARD"
            },
            "status": "ONLINE"
        })
        
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
    @patch('src.services.databricks_connection_service.DatabricksVectorEndpointRepository')
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
        mock_repo.get_endpoint_status = AsyncMock(return_value={
            "success": False,
            "message": f"Endpoint {endpoint_name} not found",
            "status": "not_found"
        })
        
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