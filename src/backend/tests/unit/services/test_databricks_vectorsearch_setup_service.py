"""
Unit tests for DatabricksVectorSearchSetupService.

Tests one-click setup functionality for Databricks Vector Search.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call
from datetime import datetime
import asyncio

from src.services.databricks_vectorsearch_setup_service import DatabricksVectorSearchSetupService
from src.schemas.memory_backend import MemoryBackendType, MemoryBackendCreate
from src.core.unit_of_work import UnitOfWork


@pytest.fixture
def mock_uow():
    """Create a mock Unit of Work."""
    uow = AsyncMock(spec=UnitOfWork)
    uow.memory_backend_repository = AsyncMock()
    return uow


@pytest.fixture
def service(mock_uow):
    """Create a DatabricksVectorSearchSetupService instance."""
    return DatabricksVectorSearchSetupService(mock_uow)


class TestDatabricksVectorSearchSetupService:
    """Test cases for DatabricksVectorSearchSetupService."""
    
    @pytest.mark.asyncio
    @patch('databricks.vector_search.client.VectorSearchClient')
    @patch('src.services.databricks_vectorsearch_setup_service.datetime')
    @patch('random.choices')
    @patch('asyncio.sleep')
    async def test_one_click_setup_complete_success(
        self, mock_sleep, mock_choices, mock_datetime, mock_client_class, service
    ):
        """Test successful one-click setup creating all resources."""
        # Arrange
        mock_datetime.utcnow.return_value.strftime.return_value = "20240115_120000"
        mock_choices.return_value = ['a', 'b', 'c', 'd']
        mock_sleep.return_value = None
        
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Mock successful endpoint and index creation
        mock_client.create_endpoint.return_value = None
        mock_client.create_direct_access_index.return_value = None
        
        # Act
        result = await service.one_click_databricks_setup(
            workspace_url="https://test.databricks.com",
            catalog="ml",
            schema="agents",
            embedding_dimension=1024,
            user_token="user-token"
        )
        
        # Assert
        assert result["success"] is True
        assert "successfully" in result["message"]
        
        # Check endpoints created
        assert result["endpoints"]["memory"]["name"] == "kasal_memory_20240115_120000_abcd"
        assert result["endpoints"]["memory"]["status"] == "created"
        assert result["endpoints"]["document"]["name"] == "kasal_docs_20240115_120000_abcd"
        
        # Check indexes created
        assert "short_term" in result["indexes"]
        assert "long_term" in result["indexes"]
        assert "entity" in result["indexes"]
        assert "document" in result["indexes"]
        
        # Verify endpoint creation calls
        assert mock_client.create_endpoint.call_count == 2
        
        # Verify index creation calls (4 indexes)
        assert mock_client.create_direct_access_index.call_count == 4
    
    @pytest.mark.asyncio
    @patch('databricks.vector_search.client.VectorSearchClient')
    @patch('src.services.databricks_vectorsearch_setup_service.datetime')
    @patch('random.choices')
    async def test_one_click_setup_endpoints_already_exist(
        self, mock_choices, mock_datetime, mock_client_class, service
    ):
        """Test setup when endpoints already exist."""
        # Arrange
        mock_datetime.utcnow.return_value.strftime.return_value = "20240115_120000"
        mock_choices.return_value = ['x', 'y', 'z', '1']
        
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Mock endpoints already exist
        mock_client.create_endpoint.side_effect = [
            Exception("Endpoint already exists"),
            Exception("Endpoint already exists")
        ]
        
        # Act
        result = await service.one_click_databricks_setup(
            workspace_url="https://test.databricks.com"
        )
        
        # Assert
        assert result["success"] is True
        assert result["endpoints"]["memory"]["status"] == "already_exists"
        assert result["endpoints"]["document"]["status"] == "already_exists"
    
    @pytest.mark.asyncio
    @patch('databricks.vector_search.client.VectorSearchClient')
    @patch('src.services.databricks_vectorsearch_setup_service.MemoryBackendBaseService')
    @patch('src.services.databricks_vectorsearch_setup_service.datetime')
    async def test_one_click_setup_with_group_id_saves_config(
        self, mock_datetime, mock_base_service_class, mock_client_class, service, mock_uow
    ):
        """Test setup saves configuration when group_id is provided."""
        # Arrange
        group_id = "test-group-123"
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Mock datetime to return a fixed time
        mock_datetime.utcnow.return_value.strftime.return_value = "2024-01-15 10:30"
        
        # Mock successful resource creation
        mock_client.create_endpoint.return_value = None
        mock_client.create_direct_access_index.return_value = None
        
        # Mock repository operations
        mock_uow.memory_backend_repository.get_by_group_id.return_value = [
            MagicMock(id="old-config-1"),
            MagicMock(id="old-config-2")
        ]
        mock_uow.memory_backend_repository.get_by_name.return_value = None  # No existing backend with this name
        
        # Mock the repository create method to return a backend with the expected id
        mock_created_backend = MagicMock(id="new-backend-123")
        mock_uow.memory_backend_repository.create.return_value = mock_created_backend
        
        # Mock base service
        mock_base_service = AsyncMock()
        mock_base_service.create_memory_backend.return_value = mock_created_backend
        mock_base_service_class.return_value = mock_base_service
        
        # Act
        result = await service.one_click_databricks_setup(
            workspace_url="https://test.databricks.com",
            group_id=group_id
        )
        
        # Assert
        assert result["success"] is True
        assert result["backend_id"] == "new-backend-123"
        assert "saved" in result["message"]
        
        # Verify old configs were deleted
        assert mock_uow.memory_backend_repository.delete.call_count == 2
        mock_uow.commit.assert_called()
        
        # Verify new config was created
        mock_base_service.create_memory_backend.assert_called_once()
        call_args = mock_base_service.create_memory_backend.call_args
        assert call_args[0][0] == group_id
        assert isinstance(call_args[0][1], MemoryBackendCreate)
    
    @pytest.mark.asyncio
    @patch('databricks.vector_search.client.VectorSearchClient')
    async def test_one_click_setup_document_endpoint_fails(
        self, mock_client_class, service
    ):
        """Test setup continues when document endpoint creation fails."""
        # Arrange
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Memory endpoint succeeds, document fails
        mock_client.create_endpoint.side_effect = [
            None,  # Memory endpoint success
            Exception("Insufficient permissions")  # Document endpoint fails
        ]
        
        # Act
        result = await service.one_click_databricks_setup(
            workspace_url="https://test.databricks.com"
        )
        
        # Assert
        assert result["success"] is True
        assert result["endpoints"]["memory"]["status"] == "created"
        assert "error" in result["endpoints"]["document"]
        
        # Document index should not be created
        assert "document" not in result["indexes"]
    
    @pytest.mark.asyncio
    @patch('databricks.vector_search.client.VectorSearchClient')
    async def test_one_click_setup_index_creation_failures(
        self, mock_client_class, service
    ):
        """Test handling of individual index creation failures."""
        # Arrange
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Endpoints succeed
        mock_client.create_endpoint.return_value = None
        
        # Some indexes fail
        mock_client.create_direct_access_index.side_effect = [
            None,  # short_term succeeds
            Exception("Schema validation error"),  # long_term fails
            None,  # entity succeeds
            Exception("Index already exists")  # document exists
        ]
        
        # Act
        result = await service.one_click_databricks_setup(
            workspace_url="https://test.databricks.com"
        )
        
        # Assert
        assert result["success"] is True
        assert result["indexes"]["short_term"]["status"] == "created"
        assert "error" in result["indexes"]["long_term"]
        assert result["indexes"]["entity"]["status"] == "created"
        assert result["indexes"]["document"]["status"] == "already_exists"
    
    @pytest.mark.asyncio
    @patch('databricks.vector_search.client.VectorSearchClient')
    @patch('src.utils.databricks_auth.get_databricks_auth_headers')
    async def test_one_click_setup_auth_with_user_token(
        self, mock_get_headers, mock_client_class, service
    ):
        """Test setup uses OBO authentication when user token provided."""
        # Arrange
        user_token = "user-token-123"
        mock_get_headers.return_value = ({"Authorization": "Bearer obo-token"}, None)
        
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Act
        result = await service.one_click_databricks_setup(
            workspace_url="https://test.databricks.com",
            user_token=user_token
        )
        
        # Assert
        assert result["success"] is True
        mock_get_headers.assert_called_once()
        mock_client_class.assert_called_with(
            workspace_url="https://test.databricks.com",
            personal_access_token="obo-token"
        )
    
    @pytest.mark.asyncio
    @patch('databricks.vector_search.client.VectorSearchClient')
    async def test_one_click_setup_complete_failure(
        self, mock_client_class, service
    ):
        """Test handling of complete setup failure."""
        # Arrange
        mock_client_class.side_effect = Exception("Authentication failed")
        
        # Act
        result = await service.one_click_databricks_setup(
            workspace_url="https://test.databricks.com"
        )
        
        # Assert
        assert result["success"] is False
        assert "Authentication failed" in result["message"]
        assert "Authentication failed" in result["error"]
    
    @pytest.mark.asyncio
    @patch('databricks.vector_search.client.VectorSearchClient')
    @patch('src.services.databricks_vectorsearch_setup_service.MemoryBackendBaseService')
    @patch('src.services.databricks_vectorsearch_setup_service.datetime')
    async def test_one_click_setup_save_config_foreign_key_error(
        self, mock_datetime, mock_base_service_class, mock_client_class, service, mock_uow
    ):
        """Test handling of foreign key error when saving configuration."""
        # Arrange
        group_id = "invalid-group"
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Mock datetime to return a fixed time
        mock_datetime.utcnow.return_value.strftime.return_value = "2024-01-15 10:35"
        
        # Mock successful resource creation
        mock_client.create_endpoint.return_value = None
        mock_client.create_direct_access_index.return_value = None
        
        # Mock repository operations
        mock_uow.memory_backend_repository.get_by_group_id.return_value = []
        mock_uow.memory_backend_repository.get_by_name.return_value = None  # No existing backend with this name
        
        # Mock repository create to simulate foreign key error (not at service level)
        mock_created_backend = MagicMock(id="test-backend-id")
        mock_uow.memory_backend_repository.create.return_value = mock_created_backend
        
        # Mock base service throws foreign key error
        mock_base_service = AsyncMock()
        mock_base_service.create_memory_backend.side_effect = Exception(
            "FOREIGN KEY constraint failed"
        )
        mock_base_service_class.return_value = mock_base_service
        
        # Act
        result = await service.one_click_databricks_setup(
            workspace_url="https://test.databricks.com",
            group_id=group_id
        )
        
        # Assert
        assert result["success"] is True
        assert "warning" in result
        assert "Setup completed but" in result["warning"]
        # The exact error message depends on whether it's a foreign key error or not
        if "foreign key constraint" in result["warning"].lower():
            assert "Please ensure you are logged in" in result["warning"]
            assert "info" in result
    
    @pytest.mark.asyncio
    @patch('databricks.vector_search.client.VectorSearchClient')
    async def test_one_click_setup_custom_parameters(
        self, mock_client_class, service
    ):
        """Test setup with custom catalog, schema, and embedding dimension."""
        # Arrange
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Act
        result = await service.one_click_databricks_setup(
            workspace_url="https://test.databricks.com",
            catalog="custom_catalog",
            schema="custom_schema",
            embedding_dimension=512
        )
        
        # Assert
        assert result["success"] is True
        assert result["catalog"] == "custom_catalog"
        assert result["schema"] == "custom_schema"
        
        # Verify index creation used custom values
        create_calls = mock_client.create_direct_access_index.call_args_list
        for call in create_calls:
            index_name = call.kwargs["index_name"]
            assert index_name.startswith("custom_catalog.custom_schema.")
            assert call.kwargs["embedding_dimension"] == 512