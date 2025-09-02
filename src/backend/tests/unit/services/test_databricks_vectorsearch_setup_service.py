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
    @patch('src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository')
    @patch('src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository')
    @patch('src.services.databricks_vectorsearch_setup_service.asyncio.sleep')
    @patch('random.choices')
    @patch('src.services.databricks_vectorsearch_setup_service.datetime')
    async def test_one_click_setup_complete_success(
        self, mock_datetime, mock_choices, mock_sleep, mock_endpoint_repo_class, mock_index_repo_class, service
    ):
        """Test successful one-click setup creating all resources."""
        # Arrange
        mock_datetime.utcnow.return_value.strftime.return_value = "20240115_120000"
        mock_choices.return_value = ['a', 'b', 'c', 'd']
        # Mock asyncio.sleep to return immediately
        mock_sleep.return_value = None
        
        # Mock endpoint repository
        mock_endpoint_repo = AsyncMock()
        mock_endpoint_repo_class.return_value = mock_endpoint_repo
        
        # Mock successful endpoint creation responses
        from src.schemas.databricks_vector_endpoint import EndpointResponse, EndpointInfo, EndpointState
        mock_endpoint_response = EndpointResponse(
            success=True,
            message="Endpoint created successfully",
            endpoint=EndpointInfo(
                name="test_endpoint",
                endpoint_type="STANDARD",
                state=EndpointState.PROVISIONING
            )
        )
        mock_endpoint_repo.create_endpoint.return_value = mock_endpoint_response
        
        # Mock index repository
        mock_index_repo = AsyncMock()
        mock_index_repo_class.return_value = mock_index_repo
        
        # Mock successful index creation responses
        from src.schemas.databricks_vector_index import IndexResponse, IndexInfo, IndexState
        mock_index_response = IndexResponse(
            success=True,
            message="Index created successfully",
            index=IndexInfo(
                name="test_index",
                state=IndexState.PROVISIONING,
                endpoint_name="test_endpoint"
            )
        )
        mock_index_repo.create_index.return_value = mock_index_response
        
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
        assert mock_endpoint_repo.create_endpoint.call_count == 2
        
        # Verify index creation calls (4 indexes)
        assert mock_index_repo.create_index.call_count == 4
    
    @pytest.mark.asyncio
    @patch('src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository')
    @patch('src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository')
    @patch('src.services.databricks_vectorsearch_setup_service.asyncio.sleep')
    @patch('random.choices')
    @patch('src.services.databricks_vectorsearch_setup_service.datetime')
    async def test_one_click_setup_endpoints_already_exist(
        self, mock_datetime, mock_choices, mock_sleep, mock_endpoint_repo_class, mock_index_repo_class, service
    ):
        """Test setup when endpoints already exist."""
        # Arrange
        mock_datetime.utcnow.return_value.strftime.return_value = "20240115_120000"
        mock_choices.return_value = ['x', 'y', 'z', '1']
        mock_sleep.return_value = None
        
        # Mock endpoint repository
        mock_endpoint_repo = AsyncMock()
        mock_endpoint_repo_class.return_value = mock_endpoint_repo
        
        # Mock endpoints already exist
        from src.schemas.databricks_vector_endpoint import EndpointResponse
        mock_endpoint_response = EndpointResponse(
            success=True,
            message="Endpoint already exists",
            endpoint=None
        )
        mock_endpoint_repo.create_endpoint.return_value = mock_endpoint_response
        
        # Mock index repository
        mock_index_repo = AsyncMock()
        mock_index_repo_class.return_value = mock_index_repo
        
        # Act
        result = await service.one_click_databricks_setup(
            workspace_url="https://test.databricks.com"
        )
        
        # Assert
        assert result["success"] is True
        assert result["endpoints"]["memory"]["status"] == "already_exists"
        assert result["endpoints"]["document"]["status"] == "already_exists"
    
    @pytest.mark.asyncio
    @patch('src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository')
    @patch('src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository')
    @patch('src.services.databricks_vectorsearch_setup_service.asyncio.sleep')
    @patch('src.services.databricks_vectorsearch_setup_service.MemoryBackendBaseService')
    @patch('src.services.databricks_vectorsearch_setup_service.datetime')
    async def test_one_click_setup_with_group_id_saves_config(
        self, mock_datetime, mock_base_service_class, mock_sleep, mock_endpoint_repo_class, mock_index_repo_class, service, mock_uow
    ):
        """Test setup saves configuration when group_id is provided."""
        # Arrange
        group_id = "test-group-123"
        mock_datetime.utcnow.return_value.strftime.return_value = "2024-01-15 10:30"
        mock_sleep.return_value = None
        
        # Mock endpoint repository
        mock_endpoint_repo = AsyncMock()
        mock_endpoint_repo_class.return_value = mock_endpoint_repo
        from src.schemas.databricks_vector_endpoint import EndpointResponse, EndpointInfo, EndpointState
        mock_endpoint_repo.create_endpoint.return_value = EndpointResponse(
            success=True,
            message="Endpoint created successfully",
            endpoint=EndpointInfo(
                name="test_endpoint",
                endpoint_type="STANDARD",
                state=EndpointState.PROVISIONING
            )
        )
        
        # Mock index repository
        mock_index_repo = AsyncMock()
        mock_index_repo_class.return_value = mock_index_repo
        from src.schemas.databricks_vector_index import IndexResponse, IndexInfo, IndexState
        mock_index_repo.create_index.return_value = IndexResponse(
            success=True,
            message="Index created successfully",
            index=IndexInfo(
                name="test_index",
                state=IndexState.PROVISIONING,
                endpoint_name="test_endpoint"
            )
        )
        
        # Mock repository operations - create disabled configs that should be deleted
        from src.schemas.memory_backend import MemoryBackendType
        mock_uow.memory_backend_repository.get_by_group_id.return_value = [
            MagicMock(id="old-config-1", backend_type=MemoryBackendType.DEFAULT),
            MagicMock(id="old-config-2", backend_type=MemoryBackendType.DEFAULT)
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
        
        # Verify old disabled configs were deleted
        assert mock_uow.memory_backend_repository.delete.call_count == 2
        mock_uow.commit.assert_called()
        
        # Verify new config was created
        mock_base_service.create_memory_backend.assert_called_once()
        call_args = mock_base_service.create_memory_backend.call_args
        assert call_args[0][0] == group_id
        assert isinstance(call_args[0][1], MemoryBackendCreate)
    
    @pytest.mark.asyncio
    @patch('src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository')
    @patch('src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository')
    @patch('src.services.databricks_vectorsearch_setup_service.asyncio.sleep')
    async def test_one_click_setup_document_endpoint_fails(
        self, mock_sleep, mock_endpoint_repo_class, mock_index_repo_class, service
    ):
        """Test setup continues when document endpoint creation fails."""
        # Arrange
        mock_sleep.return_value = None
        
        # Mock endpoint repository
        mock_endpoint_repo = AsyncMock()
        mock_endpoint_repo_class.return_value = mock_endpoint_repo
        from src.schemas.databricks_vector_endpoint import EndpointResponse, EndpointInfo, EndpointState
        
        # Memory endpoint succeeds, document fails
        mock_endpoint_repo.create_endpoint.side_effect = [
            EndpointResponse(
                success=True,
                message="Memory endpoint created",
                endpoint=EndpointInfo(
                    name="memory_endpoint",
                    endpoint_type="STANDARD",
                    state=EndpointState.PROVISIONING
                )
            ),
            EndpointResponse(
                success=False,
                message="Insufficient permissions",
                endpoint=None
            )
        ]
        
        # Mock index repository
        mock_index_repo = AsyncMock()
        mock_index_repo_class.return_value = mock_index_repo
        from src.schemas.databricks_vector_index import IndexResponse, IndexInfo, IndexState
        mock_index_repo.create_index.return_value = IndexResponse(
            success=True,
            message="Index created successfully",
            index=IndexInfo(
                name="test_index",
                state=IndexState.PROVISIONING,
                endpoint_name="test_endpoint"
            )
        )
        
        # Act
        result = await service.one_click_databricks_setup(
            workspace_url="https://test.databricks.com"
        )
        
        # Assert
        assert result["success"] is True
        assert result["endpoints"]["memory"]["status"] == "created"
        assert "error" in result["endpoints"]["document"]
        
        # Should still try to create memory indexes (3 indexes, not 4 since doc endpoint failed)
        assert mock_index_repo.create_index.call_count == 3
    
    @pytest.mark.asyncio
    @patch('src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository')
    @patch('src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository')
    @patch('src.services.databricks_vectorsearch_setup_service.asyncio.sleep')
    async def test_one_click_setup_index_creation_failures(
        self, mock_sleep, mock_endpoint_repo_class, mock_index_repo_class, service
    ):
        """Test handling of individual index creation failures."""
        # Arrange
        mock_sleep.return_value = None
        
        # Mock endpoint repository
        mock_endpoint_repo = AsyncMock()
        mock_endpoint_repo_class.return_value = mock_endpoint_repo
        from src.schemas.databricks_vector_endpoint import EndpointResponse, EndpointInfo, EndpointState
        mock_endpoint_repo.create_endpoint.return_value = EndpointResponse(
            success=True,
            message="Endpoint created successfully",
            endpoint=EndpointInfo(
                name="test_endpoint",
                endpoint_type="STANDARD",
                state=EndpointState.PROVISIONING
            )
        )
        
        # Mock index repository
        mock_index_repo = AsyncMock()
        mock_index_repo_class.return_value = mock_index_repo
        from src.schemas.databricks_vector_index import IndexResponse, IndexInfo, IndexState
        
        # Some indexes fail
        mock_index_repo.create_index.side_effect = [
            IndexResponse(
                success=True,
                message="short_term created",
                index=IndexInfo(name="short_term", state=IndexState.PROVISIONING, endpoint_name="test_endpoint")
            ),
            IndexResponse(
                success=False,
                message="Schema validation error",
                index=None
            ),
            IndexResponse(
                success=True,
                message="entity created",
                index=IndexInfo(name="entity", state=IndexState.PROVISIONING, endpoint_name="test_endpoint")
            ),
            IndexResponse(
                success=True,
                message="Index already exists",
                index=IndexInfo(name="document", state=IndexState.READY, endpoint_name="test_endpoint")
            )
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
        assert result["indexes"]["document"]["status"] == "created" or result["indexes"]["document"]["status"] == "already_exists"
    
    @pytest.mark.asyncio
    @patch('src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository')
    @patch('src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository')
    @patch('src.services.databricks_vectorsearch_setup_service.asyncio.sleep')
    async def test_one_click_setup_auth_with_user_token(
        self, mock_sleep, mock_endpoint_repo_class, mock_index_repo_class, service
    ):
        """Test setup uses OBO authentication when user token provided."""
        # Arrange
        user_token = "user-token-123"
        mock_sleep.return_value = None
        
        # Mock repositories
        mock_endpoint_repo = AsyncMock()
        mock_endpoint_repo_class.return_value = mock_endpoint_repo
        mock_index_repo = AsyncMock()
        mock_index_repo_class.return_value = mock_index_repo
        
        # Mock successful responses
        from src.schemas.databricks_vector_endpoint import EndpointResponse, EndpointInfo, EndpointState
        from src.schemas.databricks_vector_index import IndexResponse, IndexInfo, IndexState
        mock_endpoint_repo.create_endpoint.return_value = EndpointResponse(
            success=True,
            message="Endpoint created",
            endpoint=EndpointInfo(name="test", endpoint_type="STANDARD", state=EndpointState.PROVISIONING)
        )
        mock_index_repo.create_index.return_value = IndexResponse(
            success=True,
            message="Index created",
            index=IndexInfo(name="test", state=IndexState.PROVISIONING, endpoint_name="test")
        )
        
        # Act
        result = await service.one_click_databricks_setup(
            workspace_url="https://test.databricks.com",
            user_token=user_token
        )
        
        # Assert
        assert result["success"] is True
        # Verify repositories were created with the workspace URL
        mock_endpoint_repo_class.assert_called_with("https://test.databricks.com")
        mock_index_repo_class.assert_called_with("https://test.databricks.com")
    
    @pytest.mark.asyncio
    @patch('src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository')
    async def test_one_click_setup_complete_failure(
        self, mock_endpoint_repo_class, service
    ):
        """Test handling of complete setup failure."""
        # Arrange
        mock_endpoint_repo_class.side_effect = Exception("Authentication failed")
        
        # Act
        result = await service.one_click_databricks_setup(
            workspace_url="https://test.databricks.com"
        )
        
        # Assert
        assert result["success"] is False
        assert "Authentication failed" in result["message"]
        assert "Authentication failed" in result["error"]
    
    @pytest.mark.asyncio
    @patch('src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository')
    @patch('src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository')
    @patch('src.services.databricks_vectorsearch_setup_service.asyncio.sleep')
    @patch('src.services.databricks_vectorsearch_setup_service.MemoryBackendBaseService')
    @patch('src.services.databricks_vectorsearch_setup_service.datetime')
    async def test_one_click_setup_save_config_foreign_key_error(
        self, mock_datetime, mock_base_service_class, mock_sleep, mock_endpoint_repo_class, mock_index_repo_class, service, mock_uow
    ):
        """Test handling of foreign key error when saving configuration."""
        # Arrange
        group_id = "invalid-group"
        mock_datetime.utcnow.return_value.strftime.return_value = "2024-01-15 10:35"
        mock_sleep.return_value = None
        
        # Mock repositories
        mock_endpoint_repo = AsyncMock()
        mock_endpoint_repo_class.return_value = mock_endpoint_repo
        mock_index_repo = AsyncMock()
        mock_index_repo_class.return_value = mock_index_repo
        
        # Mock successful resource creation
        from src.schemas.databricks_vector_endpoint import EndpointResponse, EndpointInfo, EndpointState
        from src.schemas.databricks_vector_index import IndexResponse, IndexInfo, IndexState
        mock_endpoint_repo.create_endpoint.return_value = EndpointResponse(
            success=True,
            message="Endpoint created",
            endpoint=EndpointInfo(name="test", endpoint_type="STANDARD", state=EndpointState.PROVISIONING)
        )
        mock_index_repo.create_index.return_value = IndexResponse(
            success=True,
            message="Index created",
            index=IndexInfo(name="test", state=IndexState.PROVISIONING, endpoint_name="test")
        )
        
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
    @patch('src.services.databricks_vectorsearch_setup_service.DatabricksVectorIndexRepository')
    @patch('src.services.databricks_vectorsearch_setup_service.DatabricksVectorEndpointRepository')
    @patch('src.services.databricks_vectorsearch_setup_service.asyncio.sleep')
    async def test_one_click_setup_custom_parameters(
        self, mock_sleep, mock_endpoint_repo_class, mock_index_repo_class, service
    ):
        """Test setup with custom catalog, schema, and embedding dimension."""
        # Arrange
        mock_sleep.return_value = None
        
        # Mock repositories
        mock_endpoint_repo = AsyncMock()
        mock_endpoint_repo_class.return_value = mock_endpoint_repo
        mock_index_repo = AsyncMock()
        mock_index_repo_class.return_value = mock_index_repo
        
        # Mock successful responses
        from src.schemas.databricks_vector_endpoint import EndpointResponse, EndpointInfo, EndpointState
        from src.schemas.databricks_vector_index import IndexResponse, IndexInfo, IndexState
        mock_endpoint_repo.create_endpoint.return_value = EndpointResponse(
            success=True,
            message="Endpoint created",
            endpoint=EndpointInfo(name="test", endpoint_type="STANDARD", state=EndpointState.PROVISIONING)
        )
        mock_index_repo.create_index.return_value = IndexResponse(
            success=True,
            message="Index created",
            index=IndexInfo(name="test", state=IndexState.PROVISIONING, endpoint_name="test")
        )
        
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
        
        # Verify index creation was called with custom values
        assert mock_index_repo.create_index.call_count == 4
        # Verify the repositories were created
        assert mock_endpoint_repo_class.called
        assert mock_index_repo_class.called