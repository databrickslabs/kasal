"""
Unit tests for DatabricksIndexService.

Tests index creation, deletion, and management operations.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call
import random

from src.services.databricks_index_service import DatabricksIndexService
from src.schemas.memory_backend import DatabricksMemoryConfig


@pytest.fixture
def service():
    """Create a DatabricksIndexService instance."""
    return DatabricksIndexService()


@pytest.fixture
def databricks_config():
    """Create a sample Databricks configuration."""
    return DatabricksMemoryConfig(
        endpoint_name="test-endpoint",
        short_term_index="ml.agents.short_term",
        long_term_index="ml.agents.long_term",
        entity_index="ml.agents.entity",
        workspace_url="https://test.databricks.com",
        embedding_dimension=768
    )


class TestDatabricksIndexService:
    """Test cases for DatabricksIndexService."""
    
    @pytest.mark.asyncio
    @patch('databricks.vector_search.client.VectorSearchClient')
    @patch('src.utils.databricks_auth.get_databricks_auth_headers')
    async def test_create_index_short_term_success(
        self, mock_get_headers, mock_client_class, service, databricks_config
    ):
        """Test successful creation of short-term memory index."""
        # Arrange
        user_token = "user-token"
        mock_get_headers.return_value = ({"Authorization": "Bearer token"}, None)
        
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Act
        result = await service.create_databricks_index(
            config=databricks_config,
            index_type="short_term",
            catalog="ml",
            schema="agents",
            table_name="short_term_test",
            user_token=user_token
        )
        
        # Assert
        assert result["success"] is True
        assert "Successfully created short_term index" in result["message"]
        assert result["details"]["index_type"] == "short_term"
        assert result["details"]["embedding_dimension"] == 768
        
        # Verify schema includes required fields
        mock_client.create_direct_access_index.assert_called_once()
        call_args = mock_client.create_direct_access_index.call_args
        schema = call_args.kwargs["schema"]
        assert "crew_id" in schema
        assert "agent_id" in schema
        assert "content" in schema
        assert "score" in schema
    
    @pytest.mark.asyncio
    @patch('databricks.vector_search.client.VectorSearchClient')
    async def test_create_index_long_term_schema(
        self, mock_client_class, service, databricks_config
    ):
        """Test long-term index creation with correct schema."""
        # Arrange
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Act
        await service.create_databricks_index(
            config=databricks_config,
            index_type="long_term",
            catalog="ml",
            schema="agents",
            table_name="long_term_test"
        )
        
        # Assert
        call_args = mock_client.create_direct_access_index.call_args
        schema = call_args.kwargs["schema"]
        assert "importance" in schema
        assert "score" not in schema
    
    @pytest.mark.asyncio
    @patch('databricks.vector_search.client.VectorSearchClient')
    async def test_create_index_entity_schema(
        self, mock_client_class, service, databricks_config
    ):
        """Test entity index creation with correct schema."""
        # Arrange
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Act
        await service.create_databricks_index(
            config=databricks_config,
            index_type="entity",
            catalog="ml",
            schema="agents",
            table_name="entity_test"
        )
        
        # Assert
        call_args = mock_client.create_direct_access_index.call_args
        schema = call_args.kwargs["schema"]
        assert "entity_type" in schema
        assert "entity_name" in schema
        assert "attributes" in schema
        assert "relationships" in schema
    
    @pytest.mark.asyncio
    @patch('databricks.vector_search.client.VectorSearchClient')
    async def test_create_index_document_with_endpoint(
        self, mock_client_class, service
    ):
        """Test document index creation uses document endpoint if available."""
        # Arrange
        config = DatabricksMemoryConfig(
            endpoint_name="memory-endpoint",
            document_endpoint_name="document-endpoint",
            short_term_index="ml.agents.short_term",
            workspace_url="https://test.databricks.com"
        )
        
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Act
        await service.create_databricks_index(
            config=config,
            index_type="document",
            catalog="ml",
            schema="docs",
            table_name="embeddings"
        )
        
        # Assert
        call_args = mock_client.create_direct_access_index.call_args
        assert call_args.kwargs["endpoint_name"] == "document-endpoint"
        schema = call_args.kwargs["schema"]
        assert "source" in schema
        assert "title" in schema
        assert "doc_metadata" in schema
    
    @pytest.mark.asyncio
    @patch('databricks.vector_search.client.VectorSearchClient')
    async def test_create_index_already_exists(
        self, mock_client_class, service, databricks_config
    ):
        """Test handling when index already exists."""
        # Arrange
        mock_client = MagicMock()
        mock_client.create_direct_access_index.side_effect = Exception("Index already exists")
        mock_client_class.return_value = mock_client
        
        # Act
        result = await service.create_databricks_index(
            config=databricks_config,
            index_type="short_term",
            catalog="ml",
            schema="agents",
            table_name="existing"
        )
        
        # Assert
        assert result["success"] is False
        assert "already exists" in result["message"]
    
    @pytest.mark.asyncio
    @patch('databricks.vector_search.client.VectorSearchClient')
    async def test_get_indexes_success(
        self, mock_client_class, service, databricks_config
    ):
        """Test getting list of indexes for an endpoint."""
        # Arrange
        mock_client = MagicMock()
        mock_client.list_indexes.return_value = {
            "indexes": [
                {
                    "name": "ml.agents.short_term",
                    "status": {"state": "READY"},
                    "embedding_dimension": 768,
                    "primary_key": "id",
                    "doc_count": 1000
                },
                {
                    "name": "ml.agents.long_term",
                    "status": {"state": "PROVISIONING"},
                    "embedding_dimension": 768,
                    "primary_key": "id",
                    "doc_count": 500
                }
            ]
        }
        mock_client_class.return_value = mock_client
        
        # Act
        result = await service.get_databricks_indexes(databricks_config)
        
        # Assert
        assert result["success"] is True
        assert len(result["indexes"]) == 2
        assert result["indexes"][0]["name"] == "ml.agents.short_term"
        assert result["indexes"][0]["status"] == "READY"
        assert result["indexes"][1]["doc_count"] == 500
    
    @pytest.mark.asyncio
    @patch('databricks.vector_search.client.VectorSearchClient')
    async def test_delete_index_success(
        self, mock_client_class, service
    ):
        """Test successful index deletion."""
        # Arrange
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Act
        result = await service.delete_databricks_index(
            workspace_url="https://test.databricks.com",
            index_name="ml.agents.old_index",
            endpoint_name="test-endpoint",
            user_token="token"
        )
        
        # Assert
        assert result["success"] is True
        assert "Successfully deleted" in result["message"]
        mock_client.delete_index.assert_called_once_with(
            endpoint_name="test-endpoint",
            index_name="ml.agents.old_index"
        )
    
    @pytest.mark.asyncio
    @patch('databricks.vector_search.client.VectorSearchClient')
    async def test_delete_index_not_found(
        self, mock_client_class, service
    ):
        """Test deleting non-existent index."""
        # Arrange
        mock_client = MagicMock()
        mock_client.delete_index.side_effect = Exception("Index not found")
        mock_client_class.return_value = mock_client
        
        # Act
        result = await service.delete_databricks_index(
            workspace_url="https://test.databricks.com",
            index_name="ml.agents.missing",
            endpoint_name="test-endpoint"
        )
        
        # Assert
        assert result["success"] is False
        assert "not found" in result["message"]
    
    @pytest.mark.asyncio
    @patch('databricks.vector_search.client.VectorSearchClient')
    async def test_delete_endpoint_with_indexes_fails(
        self, mock_client_class, service
    ):
        """Test endpoint deletion fails when it has indexes."""
        # Arrange
        mock_client = MagicMock()
        mock_client.list_indexes.return_value = {"indexes": [{"name": "index1"}]}
        mock_client_class.return_value = mock_client
        
        # Act
        result = await service.delete_databricks_endpoint(
            workspace_url="https://test.databricks.com",
            endpoint_name="test-endpoint"
        )
        
        # Assert
        assert result["success"] is False
        assert "has active indexes" in result["message"]
        mock_client.delete_endpoint.assert_not_called()
    
    @pytest.mark.asyncio
    @patch('databricks.vector_search.client.VectorSearchClient')
    async def test_get_index_info_success(
        self, mock_client_class, service
    ):
        """Test getting detailed index information."""
        # Arrange
        mock_client = MagicMock()
        mock_index = MagicMock()
        mock_index.describe.return_value = {
            "direct_access_index_spec": {
                "embedding_dimension": 768,
                "num_rows": 5000
            },
            "primary_key": "id",
            "status": {
                "indexed_row_count": 5000
            }
        }
        mock_client.get_index.return_value = mock_index
        mock_client_class.return_value = mock_client
        
        # Act
        result = await service.get_index_info(
            workspace_url="https://test.databricks.com",
            index_name="ml.agents.short_term",
            endpoint_name="test-endpoint"
        )
        
        # Assert
        assert result["success"] is True
        assert result["doc_count"] == 5000
        assert result["dimension"] == 768
        assert result["index_type"] == "Direct Access"
    
    @pytest.mark.asyncio
    @patch('src.services.databricks_connection_service.DatabricksConnectionService')
    @patch('random.random')
    async def test_empty_index_memory_type(self, mock_random, mock_conn_service_class, service):
        """Test emptying a memory index with batch deletion."""
        # Arrange
        mock_conn_service = AsyncMock()
        mock_client = MagicMock()
        mock_conn_service.get_databricks_client_with_auth.return_value = (mock_client, "OBO")
        service.connection_service = mock_conn_service
        
        # Mock index
        mock_index = MagicMock()
        mock_client.get_index.return_value = mock_index
        
        # Mock search results - first batch has results, second is empty
        mock_index.similarity_search.side_effect = [
            {
                "result": {
                    "data_array": [["id1"], ["id2"], ["id3"]]
                }
            },
            {
                "result": {
                    "data_array": []
                }
            }
        ]
        
        # Mock random vectors
        mock_random.return_value = 0.5
        
        # Act
        result = await service.empty_index(
            workspace_url="https://test.databricks.com",
            index_name="ml.agents.short_term",
            endpoint_name="test-endpoint",
            index_type="short_term",
            embedding_dimension=768
        )
        
        # Assert
        assert result["success"] is True
        assert result["num_deleted"] == 3
        mock_index.delete.assert_called_once_with(primary_keys=["id1", "id2", "id3"])
    
    @pytest.mark.asyncio
    @patch('src.services.databricks_connection_service.DatabricksConnectionService')
    @patch('random.random')
    async def test_empty_index_document_type(self, mock_random, mock_conn_service_class, service):
        """Test document indexes are emptied using batch deletion."""
        # Arrange
        mock_conn_service = AsyncMock()
        mock_client = MagicMock()
        mock_conn_service.get_databricks_client_with_auth.return_value = (mock_client, "OBO")
        service.connection_service = mock_conn_service
        
        mock_index = MagicMock()
        mock_client.get_index.return_value = mock_index
        
        # Mock search results - first batch has results, second is empty
        mock_index.similarity_search.side_effect = [
            {
                "result": {
                    "data_array": [["doc1"], ["doc2"]]
                }
            },
            {
                "result": {
                    "data_array": []
                }
            }
        ]
        
        # Mock random vectors
        mock_random.return_value = 0.5
        
        # Act
        result = await service.empty_index(
            workspace_url="https://test.databricks.com",
            index_name="ml.docs.embeddings",
            endpoint_name="doc-endpoint",
            index_type="document",
            embedding_dimension=768
        )
        
        # Assert
        assert result["success"] is True
        assert result["num_deleted"] == 2
        assert "Successfully emptied index" in result["message"]
        mock_index.delete.assert_called_once_with(primary_keys=["doc1", "doc2"])
    
    @pytest.mark.asyncio
    @patch('src.services.databricks_connection_service.DatabricksConnectionService')
    async def test_empty_index_batch_failure(self, mock_conn_service_class, service):
        """Test handling batch deletion failure."""
        # Arrange
        mock_conn_service = AsyncMock()
        mock_client = MagicMock()
        mock_conn_service.get_databricks_client_with_auth.return_value = (mock_client, "API_KEY")
        service.connection_service = mock_conn_service
        
        mock_index = MagicMock()
        mock_client.get_index.return_value = mock_index
        
        # Mock search to throw error
        mock_index.similarity_search.side_effect = Exception("Search failed")
        
        # Act
        result = await service.empty_index(
            workspace_url="https://test.databricks.com",
            index_name="ml.agents.entity",
            endpoint_name="test-endpoint",
            index_type="entity",
            embedding_dimension=768
        )
        
        # Assert
        assert result["success"] is False
        assert "Cannot empty index" in result["message"]
        assert "Search failed" in result["error_details"]
    
    @pytest.mark.asyncio
    @patch('src.core.unit_of_work.UnitOfWork')
    @patch('src.services.api_keys_service.ApiKeysService')
    @patch('databricks.vector_search.client.VectorSearchClient')
    async def test_create_index_auth_fallback_sequence(
        self, mock_client_class, mock_api_service_class, mock_uow_class, service
    ):
        """Test authentication fallback sequence for index creation."""
        # Arrange
        # Create config with PAT auth
        pat_config = DatabricksMemoryConfig(
            endpoint_name="test-endpoint",
            short_term_index="ml.agents.short_term",
            workspace_url="https://test.databricks.com",
            auth_type="pat",
            personal_access_token="test-token",
            embedding_dimension=768
        )
        
        # Mock successful client
        mock_client = MagicMock()
        mock_client.create_direct_access_index = MagicMock()
        
        # Mock API service to fail, forcing fallback to PAT auth
        mock_api_service_class.from_unit_of_work.side_effect = Exception("No API service")
        
        # Mock UnitOfWork
        mock_uow_instance = AsyncMock()
        mock_uow_class.return_value.__aenter__.return_value = mock_uow_instance
        
        # Return the successful client when using PAT auth
        mock_client_class.return_value = mock_client
        
        # Act
        with patch('src.utils.databricks_auth.get_databricks_auth_headers', 
                   return_value=(None, "Error")):
            result = await service.create_databricks_index(
                config=pat_config,
                index_type="short_term",
                catalog="ml",
                schema="agents",
                table_name="test"
            )
        
        # Assert
        assert result["success"] is True
        assert result["details"]["auth_method"] == "PAT"
        # Verify the client was created with PAT auth
        mock_client_class.assert_called_with(
            workspace_url="https://test.databricks.com",
            personal_access_token="test-token"
        )