"""
Unit tests for MemoryBackendService facade.

Tests that the facade properly delegates to sub-services.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Tuple, Optional, Any

from src.services.memory_backend_service import MemoryBackendService
from src.models.memory_backend import MemoryBackend
from src.schemas.memory_backend import (
    MemoryBackendCreate,
    MemoryBackendUpdate,
    MemoryBackendConfig,
    DatabricksMemoryConfig,
    MemoryBackendType
)
from src.core.unit_of_work import UnitOfWork


@pytest.fixture
def mock_uow():
    """Create a mock Unit of Work."""
    uow = AsyncMock(spec=UnitOfWork)
    return uow


@pytest.fixture
def mock_sub_services():
    """Create mocks for all sub-services."""
    return {
        'base': AsyncMock(),
        'config': AsyncMock(),
        'connection': AsyncMock(),
        'index': AsyncMock(),
        'setup': AsyncMock(),
        'verification': AsyncMock()
    }


@pytest.fixture
def service(mock_uow, mock_sub_services):
    """Create a MemoryBackendService with mocked sub-services."""
    service = MemoryBackendService(mock_uow)
    
    # Replace sub-services with mocks
    service._base_service = mock_sub_services['base']
    service._config_service = mock_sub_services['config']
    service._connection_service = mock_sub_services['connection']
    service._index_service = mock_sub_services['index']
    service._setup_service = mock_sub_services['setup']
    service._verification_service = mock_sub_services['verification']
    
    return service


class TestMemoryBackendServiceFacade:
    """Test cases for MemoryBackendService facade pattern."""
    
    @pytest.mark.asyncio
    async def test_create_memory_backend_delegates_to_base(self, service, mock_sub_services):
        """Test create_memory_backend delegates to base service."""
        # Arrange
        group_id = "test-group"
        config = MagicMock(spec=MemoryBackendCreate)
        expected_result = MagicMock(spec=MemoryBackend)
        mock_sub_services['base'].create_memory_backend.return_value = expected_result
        
        # Act
        result = await service.create_memory_backend(group_id, config)
        
        # Assert
        assert result == expected_result
        mock_sub_services['base'].create_memory_backend.assert_called_once_with(group_id, config)
    
    @pytest.mark.asyncio
    async def test_get_memory_backends_delegates_to_base(self, service, mock_sub_services):
        """Test get_memory_backends delegates to base service."""
        # Arrange
        group_id = "test-group"
        expected_result = [MagicMock(spec=MemoryBackend)]
        mock_sub_services['base'].get_memory_backends.return_value = expected_result
        
        # Act
        result = await service.get_memory_backends(group_id)
        
        # Assert
        assert result == expected_result
        mock_sub_services['base'].get_memory_backends.assert_called_once_with(group_id)
    
    @pytest.mark.asyncio
    async def test_get_active_config_delegates_to_config(self, service, mock_sub_services):
        """Test get_active_config delegates to config service."""
        # Arrange
        group_id = "test-group"
        expected_result = MagicMock(spec=MemoryBackendConfig)
        mock_sub_services['config'].get_active_config.return_value = expected_result
        
        # Act
        result = await service.get_active_config(group_id)
        
        # Assert
        assert result == expected_result
        mock_sub_services['config'].get_active_config.assert_called_once_with(group_id)
    
    @pytest.mark.asyncio
    async def test_test_databricks_connection_delegates_to_connection(self, service, mock_sub_services):
        """Test test_databricks_connection delegates to connection service."""
        # Arrange
        config = MagicMock(spec=DatabricksMemoryConfig)
        user_token = "token"
        expected_result = {"success": True}
        mock_sub_services['connection'].test_databricks_connection.return_value = expected_result
        
        # Act
        result = await service.test_databricks_connection(config, user_token)
        
        # Assert
        assert result == expected_result
        mock_sub_services['connection'].test_databricks_connection.assert_called_once_with(
            config, user_token
        )
    
    @pytest.mark.asyncio
    async def test_create_databricks_index_delegates_to_index(self, service, mock_sub_services):
        """Test create_databricks_index delegates to index service."""
        # Arrange
        config = MagicMock(spec=DatabricksMemoryConfig)
        params = {
            "index_type": "short_term",
            "catalog": "ml",
            "schema": "agents",
            "table_name": "test",
            "primary_key": "id",
            "user_token": "token"
        }
        expected_result = {"success": True}
        mock_sub_services['index'].create_databricks_index.return_value = expected_result
        
        # Act
        result = await service.create_databricks_index(config, **params)
        
        # Assert
        assert result == expected_result
        mock_sub_services['index'].create_databricks_index.assert_called_once_with(
            config, 
            params["index_type"],
            params["catalog"],
            params["schema"],
            params["table_name"],
            params["primary_key"],
            params["user_token"]
        )
    
    @pytest.mark.asyncio
    async def test_one_click_databricks_setup_delegates_to_setup(self, service, mock_sub_services):
        """Test one_click_databricks_setup delegates to setup service."""
        # Arrange
        params = {
            "workspace_url": "https://test.databricks.com",
            "catalog": "ml",
            "schema": "agents",
            "embedding_dimension": 1024,
            "user_token": "token",
            "group_id": "group-123"
        }
        expected_result = {"success": True}
        mock_sub_services['setup'].one_click_databricks_setup.return_value = expected_result
        
        # Act
        result = await service.one_click_databricks_setup(**params)
        
        # Assert
        assert result == expected_result
        mock_sub_services['setup'].one_click_databricks_setup.assert_called_once_with(
            params["workspace_url"],
            params["catalog"],
            params["schema"],
            params["embedding_dimension"],
            params["user_token"],
            params["group_id"]
        )
    
    @pytest.mark.asyncio
    async def test_verify_databricks_resources_delegates_to_verification(self, service, mock_sub_services):
        """Test verify_databricks_resources delegates to verification service."""
        # Arrange
        workspace_url = "https://test.databricks.com"
        user_token = "token"
        config = MagicMock(spec=MemoryBackend)
        config.databricks_config = {"endpoint_name": "test"}
        
        expected_result = {"success": True, "resources": {}}
        mock_sub_services['verification'].verify_databricks_resources.return_value = expected_result
        
        # Act
        result = await service.verify_databricks_resources(workspace_url, user_token, config)
        
        # Assert
        assert result == expected_result
        mock_sub_services['verification'].verify_databricks_resources.assert_called_once()
        
        # Check that config was converted to dict
        call_args = mock_sub_services['verification'].verify_databricks_resources.call_args
        assert call_args[0][2] == {'databricks_config': {"endpoint_name": "test"}}
    
    @pytest.mark.asyncio
    async def test_all_crud_methods_delegate_to_base(self, service, mock_sub_services):
        """Test all CRUD methods delegate to base service."""
        # Test update
        await service.update_memory_backend("group", "id", MagicMock())
        mock_sub_services['base'].update_memory_backend.assert_called_once()
        
        # Test delete
        await service.delete_memory_backend("group", "id")
        mock_sub_services['base'].delete_memory_backend.assert_called_once()
        
        # Test set default
        await service.set_default_backend("group", "id")
        mock_sub_services['base'].set_default_backend.assert_called_once()
        
        # Test get stats
        await service.get_memory_stats("group", "crew")
        mock_sub_services['base'].get_memory_stats.assert_called_once()
        
        # Test delete all and create disabled
        await service.delete_all_and_create_disabled("group")
        mock_sub_services['base'].delete_all_and_create_disabled.assert_called_once()
        
        # Test delete disabled
        await service.delete_disabled_configurations("group")
        mock_sub_services['base'].delete_disabled_configurations.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_all_connection_methods_delegate(self, service, mock_sub_services):
        """Test all connection methods delegate to connection service."""
        # Test get endpoint status
        await service.get_databricks_endpoint_status("url", "endpoint", "token")
        mock_sub_services['connection'].get_databricks_endpoint_status.assert_called_once()
        
        # Test get auth token
        mock_sub_services['connection'].get_databricks_auth_token.return_value = (
            "token", "OBO"
        )
        result = await service._get_databricks_auth_token("url", "token")
        assert isinstance(result, tuple)
        mock_sub_services['connection'].get_databricks_auth_token.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_all_index_methods_delegate(self, service, mock_sub_services):
        """Test all index methods delegate to index service."""
        # Test get indexes
        config = MagicMock(spec=DatabricksMemoryConfig)
        await service.get_databricks_indexes(config, "token")
        mock_sub_services['index'].get_databricks_indexes.assert_called_once()
        
        # Test delete index
        await service.delete_databricks_index("url", "index", "endpoint", "token")
        mock_sub_services['index'].delete_databricks_index.assert_called_once()
        
        # Test delete endpoint
        await service.delete_databricks_endpoint("url", "endpoint", "token")
        mock_sub_services['index'].delete_databricks_endpoint.assert_called_once()
        
        # Test get index info
        await service.get_index_info("url", "index", "endpoint", "token")
        mock_sub_services['index'].get_index_info.assert_called_once()
        
        # Test empty index
        await service.empty_index("url", "index", "endpoint", "short_term", 768, "token")
        mock_sub_services['index'].empty_index.assert_called_once()
        
        # Test get index documents
        await service.get_index_documents("url", "endpoint", "index", "short_term", 1024, 30, "token")
        mock_sub_services['index'].get_index_documents.assert_called_once()
        
        # Test search vectors
        await service.search_vectors("url", "index", "endpoint", [0.1, 0.2], "short_term", 5, None, "token")
        mock_sub_services['index'].search_vectors.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_facade_initialization(self, mock_uow):
        """Test facade properly initializes all sub-services."""
        # Act
        service = MemoryBackendService(mock_uow)
        
        # Assert
        assert service.uow == mock_uow
        assert hasattr(service, '_base_service')
        assert hasattr(service, '_config_service')
        assert hasattr(service, '_connection_service')
        assert hasattr(service, '_index_service')
        assert hasattr(service, '_setup_service')
        assert hasattr(service, '_verification_service')
    
    @pytest.mark.asyncio
    async def test_get_workspace_url(self, service):
        """Test get_workspace_url method."""
        with patch.dict('os.environ', {}, clear=True):
            # Test when no environment variables are set
            result = await service.get_workspace_url()
            assert result['workspace_url'] is None
            assert result['source'] is None
            assert result['detected'] is False
        
        with patch.dict('os.environ', {'DATABRICKS_HOST': 'test.databricks.com'}, clear=True):
            # Test with DATABRICKS_HOST
            result = await service.get_workspace_url()
            assert result['workspace_url'] == 'https://test.databricks.com'
            assert result['source'] == 'DATABRICKS_HOST'
            assert result['detected'] is True
        
        with patch.dict('os.environ', {'DATABRICKS_WORKSPACE_URL': 'workspace.databricks.com'}, clear=True):
            # Test with DATABRICKS_WORKSPACE_URL
            result = await service.get_workspace_url()
            assert result['workspace_url'] == 'https://workspace.databricks.com'
            assert result['source'] == 'DATABRICKS_WORKSPACE_URL'
            assert result['detected'] is True
    
    def test_facade_preserves_api_compatibility(self):
        """Test that facade has all the expected public methods."""
        # List of expected public methods
        expected_methods = [
            # Base CRUD
            'create_memory_backend',
            'get_memory_backends',
            'get_memory_backend',
            'get_default_memory_backend',
            'update_memory_backend',
            'delete_memory_backend',
            'set_default_backend',
            'get_memory_stats',
            'delete_all_and_create_disabled',
            'delete_disabled_configurations',
            
            # Config
            'get_active_config',
            
            # Connection
            'test_databricks_connection',
            'get_databricks_endpoint_status',
            '_get_databricks_auth_token',
            
            # Index
            'create_databricks_index',
            'get_databricks_indexes',
            'delete_databricks_index',
            'delete_databricks_endpoint',
            'get_index_info',
            'empty_index',
            'get_index_documents',
            'search_vectors',
            
            # Setup
            'one_click_databricks_setup',
            
            # Verification
            'verify_databricks_resources',
            
            # Environment
            'get_workspace_url'
        ]
        
        # Check all methods exist
        for method_name in expected_methods:
            assert hasattr(MemoryBackendService, method_name), f"Missing method: {method_name}"