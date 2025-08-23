"""
Unit tests for DatabricksIndexService.

Tests index creation, deletion, and management operations.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call
import random
import numpy as np

from src.services.databricks_index_service import DatabricksIndexService
from src.schemas.memory_backend import DatabricksMemoryConfig


@pytest.fixture
def service():
    """Create a DatabricksIndexService instance."""
    return DatabricksIndexService()


@pytest.fixture
def mock_repo():
    """Create a mock repository."""
    return AsyncMock()


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
    async def test_create_index_short_term_success(self, service, databricks_config):
        """Test successful creation of short-term memory index."""
        # Arrange
        from src.schemas.databricks_vector_index import IndexResponse
        
        user_token = "user-token"
        mock_repo = AsyncMock()
        
        # Mock repository response
        mock_repo.create_index.return_value = IndexResponse(
            success=True,
            message="Successfully created short_term index: ml.agents.short_term_test"
        )
        
        # Patch the _get_index_repository method to return our mock
        with patch.object(service, '_get_index_repository', return_value=mock_repo):
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
        assert "Successfully created" in result["message"]
        assert result["details"]["index_type"] == "short_term"
        assert result["details"]["embedding_dimension"] == 768
        
        # Verify the repository was called with correct parameters
        mock_repo.create_index.assert_called_once()
        call_args = mock_repo.create_index.call_args
        index_request = call_args[0][0]
        assert index_request.name == "ml.agents.short_term_test"
        assert index_request.endpoint_name == "test-endpoint"
        assert index_request.embedding_dimension == 768
    
    @pytest.mark.asyncio
    async def test_create_index_long_term_schema(self, service, databricks_config, mock_repo):
        """Test long-term index creation with correct schema."""
        # Arrange
        from src.schemas.databricks_vector_index import IndexResponse
        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas
        
        # Mock repository response
        mock_repo.create_index.return_value = IndexResponse(
            success=True,
            message="Successfully created long_term index"
        )
        
        # Patch the _get_index_repository method to return our mock
        with patch.object(service, '_get_index_repository', return_value=mock_repo):
            # Act
            await service.create_databricks_index(
                config=databricks_config,
                index_type="long_term",
                catalog="ml",
                schema="agents",
                table_name="long_term_test"
            )
        
        # Assert
        mock_repo.create_index.assert_called_once()
        call_args = mock_repo.create_index.call_args
        index_request = call_args[0][0]
        
        # Verify schema has correct fields for long_term
        schema = DatabricksIndexSchemas.get_schema("long_term")
        assert "importance" in schema
        assert "score" not in schema
    
    @pytest.mark.asyncio
    async def test_create_index_entity_schema(self, service, databricks_config, mock_repo):
        """Test entity index creation with correct schema."""
        # Arrange
        from src.schemas.databricks_vector_index import IndexResponse
        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas
        
        # Mock repository response
        mock_repo.create_index.return_value = IndexResponse(
            success=True,
            message="Successfully created entity index"
        )
        
        # Patch the _get_index_repository method to return our mock
        with patch.object(service, '_get_index_repository', return_value=mock_repo):
            # Act
            await service.create_databricks_index(
                config=databricks_config,
                index_type="entity",
                catalog="ml",
                schema="agents",
                table_name="entity_test"
            )
        
        # Assert
        mock_repo.create_index.assert_called_once()
        
        # Verify schema has correct fields for entity
        schema = DatabricksIndexSchemas.get_schema("entity")
        assert "entity_type" in schema
        assert "entity_name" in schema
        assert "description" in schema  # Changed from attributes
        assert "relationships" in schema
    
    @pytest.mark.asyncio
    async def test_create_index_document_with_endpoint(self, service, mock_repo):
        """Test document index creation uses document endpoint if available."""
        # Arrange
        from src.schemas.databricks_vector_index import IndexResponse
        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas
        
        config = DatabricksMemoryConfig(
            endpoint_name="memory-endpoint",
            document_endpoint_name="document-endpoint",
            short_term_index="ml.agents.short_term",
            workspace_url="https://test.databricks.com"
        )
        
        # Mock repository response
        mock_repo.create_index.return_value = IndexResponse(
            success=True,
            message="Successfully created document index"
        )
        
        # Patch the _get_index_repository method to return our mock
        with patch.object(service, '_get_index_repository', return_value=mock_repo):
            # Act
            await service.create_databricks_index(
            config=config,
            index_type="document",
            catalog="ml",
            schema="docs",
                table_name="embeddings"
            )
        
        # Assert
        mock_repo.create_index.assert_called_once()
        call_args = mock_repo.create_index.call_args
        index_request = call_args[0][0]
        assert index_request.endpoint_name == "document-endpoint"
        
        # Verify schema has correct fields for document
        schema = DatabricksIndexSchemas.get_schema("document")
        assert "source" in schema
        assert "title" in schema
        assert "doc_metadata" in schema
    
    @pytest.mark.asyncio
    async def test_create_index_already_exists(self, service, databricks_config, mock_repo):
        """Test handling when index already exists."""
        # Arrange
        from src.schemas.databricks_vector_index import IndexResponse
        
        # Mock repository response for already existing index
        mock_repo.create_index.return_value = IndexResponse(
            success=False,
            message="Failed to create index",
            error="Index already exists"
        )
        
        # Patch the _get_index_repository method to return our mock
        with patch.object(service, '_get_index_repository', return_value=mock_repo):
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
    async def test_get_indexes_success(self, service, databricks_config, mock_repo):
        """Test getting list of indexes for an endpoint."""
        # Arrange
        from src.schemas.databricks_vector_index import IndexInfo, IndexListResponse, IndexState
        
        # Create proper IndexInfo objects
        index1 = IndexInfo(
            name="ml.agents.short_term",
            endpoint_name="test-endpoint",
            state=IndexState.READY,
            ready=True,
            embedding_dimension=768,
            primary_key="id",
            row_count=1000
        )
        index2 = IndexInfo(
            name="ml.agents.long_term",
            endpoint_name="test-endpoint",
            state=IndexState.PROVISIONING,
            ready=False,
            embedding_dimension=768,
            primary_key="id",
            row_count=500
        )
        
        # Mock repository response
        mock_repo.list_indexes.return_value = IndexListResponse(
            success=True,
            indexes=[index1, index2],
            message="Success"
        )
        
        # Patch the _get_index_repository method to return our mock
        with patch.object(service, '_get_index_repository', return_value=mock_repo):
            # Act
            result = await service.get_databricks_indexes(databricks_config)
        
        # Assert
        assert result["success"] is True
        assert len(result["indexes"]) == 2
        assert result["indexes"][0]["name"] == "ml.agents.short_term"
        assert result["indexes"][0]["status"] == "READY"
        assert result["indexes"][1]["doc_count"] == 500
    
    @pytest.mark.asyncio
    async def test_delete_index_success(self, service, mock_repo):
        """Test successful index deletion."""
        # Arrange
        from src.schemas.databricks_vector_index import IndexResponse
        
        # Mock repository response
        mock_repo.delete_index.return_value = IndexResponse(
            success=True,
            message="Successfully deleted index"
        )
        
        # Patch the _get_index_repository method to return our mock
        with patch.object(service, '_get_index_repository', return_value=mock_repo):
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
        mock_repo.delete_index.assert_called_once_with(
            "ml.agents.old_index",
            "test-endpoint",
            "token"
        )
    
    @pytest.mark.asyncio
    async def test_delete_index_not_found(self, service, mock_repo):
        """Test deleting non-existent index."""
        # Arrange
        from src.schemas.databricks_vector_index import IndexResponse
        
        # Mock repository response
        mock_repo.delete_index.return_value = IndexResponse(
            success=False,
            message="Failed to delete index: Index not found",
            error="Index not found"
        )
        
        # Patch the _get_index_repository method to return our mock
        with patch.object(service, '_get_index_repository', return_value=mock_repo):
            # Act
            result = await service.delete_databricks_index(
                workspace_url="https://test.databricks.com",
                index_name="ml.agents.missing",
                endpoint_name="test-endpoint"
            )
        
        # Assert
        assert result["success"] is False
        assert "Failed to delete" in result["message"]
    
    @pytest.mark.asyncio
    async def test_delete_endpoint_with_indexes_succeeds(self, service, mock_repo):
        """Test endpoint deletion when no checks for existing indexes."""
        # Arrange
        from src.schemas.databricks_vector_endpoint import EndpointResponse
        
        mock_endpoint_repo = AsyncMock()
        
        # Mock successful deletion
        mock_endpoint_repo.delete_endpoint.return_value = EndpointResponse(
            success=True,
            message="Successfully deleted endpoint"
        )
        
        # Patch the endpoint repository method
        with patch.object(service, '_get_endpoint_repository', return_value=mock_endpoint_repo):
            # Act
            result = await service.delete_databricks_endpoint(
                workspace_url="https://test.databricks.com",
                endpoint_name="test-endpoint"
            )
        
        # Assert
        assert result["success"] is True
        assert "Successfully deleted" in result["message"]
        mock_endpoint_repo.delete_endpoint.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_index_info_success(self, service, mock_repo):
        """Test getting detailed index information."""
        # Arrange
        from src.schemas.databricks_vector_index import IndexInfo, IndexResponse, IndexType, IndexState
        
        # Create proper IndexInfo object
        index_info = IndexInfo(
            name="ml.agents.short_term",
            endpoint_name="test-endpoint",
            index_type=IndexType.DIRECT_ACCESS,
            state=IndexState.READY,
            ready=True,
            row_count=5000,
            indexed_row_count=5000,
            embedding_dimension=768,
            primary_key="id"
        )
        
        # Mock repository response
        mock_repo.get_index.return_value = IndexResponse(
            success=True,
            index=index_info,
            message="Success"
        )
        
        # Patch the _get_index_repository method to return our mock
        with patch.object(service, '_get_index_repository', return_value=mock_repo):
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
    async def test_empty_index_memory_type(self, service, mock_repo):
        """Test emptying a memory index with batch deletion."""
        # Arrange
        # Mock the empty_index method to return success
        mock_repo.empty_index.return_value = {
            "success": True,
            "num_deleted": 3,
            "message": "Successfully emptied index"
        }
        
        # Patch the _get_index_repository method to return our mock
        with patch.object(service, '_get_index_repository', return_value=mock_repo):
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
        mock_repo.empty_index.assert_called_once_with(
            "ml.agents.short_term",
            "test-endpoint",
            768,
            None
        )
    
    @pytest.mark.asyncio
    async def test_empty_index_document_type(self, service, mock_repo):
        """Test document indexes are emptied using batch deletion."""
        # Arrange
        # Mock the empty_index method to return success
        mock_repo.empty_index.return_value = {
            "success": True,
            "num_deleted": 2,
            "message": "Successfully emptied index"
        }
        
        # Patch the _get_index_repository method to return our mock
        with patch.object(service, '_get_index_repository', return_value=mock_repo):
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
        mock_repo.empty_index.assert_called_once_with(
            "ml.docs.embeddings",
            "doc-endpoint",
            768,
            None
        )
    
    @pytest.mark.asyncio
    async def test_empty_index_batch_failure(self, service, mock_repo):
        """Test handling batch deletion failure."""
        # Arrange
        # Mock the empty_index method to return failure
        mock_repo.empty_index.return_value = {
            "success": False,
            "message": "Failed to empty index: Search failed",
            "error": "Search failed"
        }
        
        # Patch the _get_index_repository method to return our mock
        with patch.object(service, '_get_index_repository', return_value=mock_repo):
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
        assert "Failed to empty index" in result["message"]
        mock_repo.empty_index.assert_called_once_with(
            "ml.agents.entity",
            "test-endpoint",
            768,
            None
        )
    
    @pytest.mark.asyncio
    async def test_get_index_documents_with_repository(self, service, mock_repo):
        """Test get_index_documents uses repository pattern for similarity search."""
        # Arrange
        # Mock similarity search result
        mock_search_result = {
            "success": True,
            "results": {
                "result": {
                    "data_array": [
                        ["doc1", "Test content 1", "query1", "session1", 1, "2024-01-01", "2024-01-01", 24, '{"meta": "data"}', "crew1", "agent1", "group1", "gpt-4", "[]", "model1", 1],
                        ["doc2", "Test content 2", "query2", "session1", 2, "2024-01-02", "2024-01-02", 24, '{"meta": "data2"}', "crew1", "agent2", "group1", "gpt-4", "[]", "model1", 1]
                    ]
                }
            }
        }
        mock_repo.similarity_search.return_value = mock_search_result
        
        # Patch the _get_index_repository method to return our mock
        with patch.object(service, '_get_index_repository', return_value=mock_repo):
            # Act
            result = await service.get_index_documents(
                workspace_url="https://test.databricks.com",
                endpoint_name="test-endpoint",
                index_name="ml.agents.short_term",
                index_type="short_term",
                limit=10
            )
        
        # Assert
        assert result["success"] is True
        assert result["count"] == 2
        assert len(result["documents"]) == 2
        assert result["documents"][0]["id"] == "doc1"
        assert result["documents"][0]["text"] == "Test content 1"
        
        # Verify repository method was called
        mock_repo.similarity_search.assert_called_once()
        call_args = mock_repo.similarity_search.call_args
        assert call_args.kwargs["index_name"] == "ml.agents.short_term"
        assert call_args.kwargs["endpoint_name"] == "test-endpoint"
        assert call_args.kwargs["num_results"] == 10
    
    @pytest.mark.asyncio
    async def test_get_index_documents_no_search_query(
        self, service, mock_repo
    ):
        """Test get_index_documents without search query uses random vector."""
        # Arrange
        
        # Mock similarity search result with all documents
        mock_search_result = {
            "success": True,
            "results": {
                "result": {
                    "data_array": [
                        ["doc1", "Content 1", "query1", "session1", 1, "2024-01-01", "2024-01-01", 24, '{}', "crew1", "agent1", "group1", "gpt-4", "[]", "model1", 1],
                        ["doc2", "Content 2", "query2", "session1", 2, "2024-01-02", "2024-01-02", 24, '{}', "crew1", "agent2", "group1", "gpt-4", "[]", "model1", 1],
                        ["doc3", "Content 3", "query3", "session1", 3, "2024-01-03", "2024-01-03", 24, '{}', "crew1", "agent3", "group1", "gpt-4", "[]", "model1", 1]
                    ]
                }
            }
        }
        mock_repo.similarity_search.return_value = mock_search_result
        
        # Act
        with patch.object(service, '_get_index_repository', return_value=mock_repo):
            with patch('numpy.random.randn') as mock_randn:
                # Mock randn to return a fixed value array
                mock_randn.return_value = np.full(1024, 0.5)
                result = await service.get_index_documents(
                workspace_url="https://test.databricks.com",
                endpoint_name="test-endpoint",
                index_name="ml.agents.long_term",
                index_type="long_term",
                limit=20
            )
        
        # Assert
        assert result["success"] is True
        assert result["count"] == 3
        
        # Verify the mocked vector was used
        call_args = mock_repo.similarity_search.call_args
        query_vector = call_args.kwargs["query_vector"]
        assert len(query_vector) == 1024  # Default dimension
    
    @pytest.mark.asyncio
    async def test_get_index_documents_repository_failure(
        self, service, mock_repo
    ):
        """Test get_index_documents handles repository failures gracefully."""
        # Arrange
        
        # Mock repository failure
        mock_repo.similarity_search.return_value = {
            "success": False,
            "error": "Index not found",
            "results": None
        }
        
        # Act
        with patch.object(service, '_get_index_repository', return_value=mock_repo):
            result = await service.get_index_documents(
            workspace_url="https://test.databricks.com",
            endpoint_name="test-endpoint",
            index_name="ml.agents.nonexistent",
            limit=10
        )
        
        # Assert
        assert result["success"] is False
        assert "Search failed" in result["message"] or "Failed to retrieve" in result["message"]
        assert result["documents"] == []
    
    @pytest.mark.asyncio
    async def test_query_entity_data_with_repository(
        self, service, mock_repo
    ):
        """Test query_entity_data uses repository pattern for similarity search."""
        # Arrange
        
        # Mock similarity search result for entity data - match the actual schema columns
        mock_search_result = {
            "success": True,
            "results": {
                "result": {
                    "data_array": [
                        ["entity1", "John Doe", "Person", "A person named John", '["rel1", "rel2"]', "2024-01-01", "crew1", "agent1", "group1", "gpt-4", "[]", "model1"],
                        ["entity2", "Acme Corp", "Company", "A large company", '["rel3"]', "2024-01-02", "crew1", "agent2", "group1", "gpt-4", "[]", "model1"]
                    ]
                }
            }
        }
        mock_repo.similarity_search.return_value = mock_search_result
        
        # Act
        with patch.object(service, '_get_index_repository', return_value=mock_repo):
            result = await service.query_entity_data(
            workspace_url="https://test.databricks.com",
            endpoint_name="test-endpoint",
            index_name="ml.agents.entity",
            embedding_dimension=768,
            limit=5
        )
        
        # Assert
        assert result["success"] is True
        # The service creates additional entities from relationships, so we expect more than 2
        assert len(result["entities"]) >= 2
        
        # Find the specific entities we're looking for
        entities_by_id = {e["id"]: e for e in result["entities"]}
        assert "entity1" in entities_by_id
        entity1 = entities_by_id["entity1"]
        assert entity1["type"] == "Person"
        assert entity1["name"] == "John Doe"
        
        # Verify repository method was called with correct parameters
        mock_repo.similarity_search.assert_called_once()
        call_args = mock_repo.similarity_search.call_args
        assert call_args.kwargs["index_name"] == "ml.agents.entity"
        assert call_args.kwargs["endpoint_name"] == "test-endpoint"
        assert len(call_args.kwargs["query_vector"]) == 768
        assert call_args.kwargs["num_results"] == 5
    
    @pytest.mark.asyncio
    async def test_query_entity_data_without_search_query(
        self, service, mock_repo
    ):
        """Test query_entity_data without search query returns all entities."""
        # Arrange
        
        # Mock similarity search result
        mock_search_result = {
            "success": True,
            "results": {
                "result": {
                    "data_array": [
                        ["e1", "Name1", "Type1", "Description1", "[]", "2024-01-01", "crew1", "agent1", "group1", "gpt-4", "[]", "model1"],
                        ["e2", "Name2", "Type2", "Description2", "[]", "2024-01-02", "crew1", "agent2", "group1", "gpt-4", "[]", "model1"]
                    ]
                }
            }
        }
        mock_repo.similarity_search.return_value = mock_search_result
        
        # Act
        with patch.object(service, '_get_index_repository', return_value=mock_repo):
            with patch('random.random', return_value=0.5):
                result = await service.query_entity_data(
                workspace_url="https://test.databricks.com",
                endpoint_name="test-endpoint",
                index_name="ml.agents.entity",
                embedding_dimension=768,
                limit=100
            )
        
        # Assert
        assert result["success"] is True
        assert len(result["entities"]) == 2
        
        # Verify random vector was used
        call_args = mock_repo.similarity_search.call_args
        query_vector = call_args.kwargs["query_vector"]
        assert len(query_vector) == 768
        assert all(v == 0.5 for v in query_vector)
    
    @pytest.mark.asyncio
    async def test_query_entity_data_repository_error(
        self, service, mock_repo
    ):
        """Test query_entity_data handles repository errors gracefully."""
        # Arrange
        
        # Mock repository error
        mock_repo.similarity_search.return_value = {
            "success": False,
            "error": "Authentication failed",
            "results": None
        }
        
        # Act
        with patch.object(service, '_get_index_repository', return_value=mock_repo):
            result = await service.query_entity_data(
            workspace_url="https://test.databricks.com",
            endpoint_name="test-endpoint",
            index_name="ml.agents.entity",
            limit=10
        )
        
        # Assert
        assert result["success"] is False
        assert "Search failed" in result["message"] or "Failed to query" in result["message"]
        assert result["entities"] == []