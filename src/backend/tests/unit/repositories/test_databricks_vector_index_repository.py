"""
Unit tests for DatabricksVectorIndexRepository.

Tests the REST API implementation for Vector Search operations.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call
from typing import List, Dict, Any
import json
import aiohttp

from src.repositories.databricks_vector_index_repository import DatabricksVectorIndexRepository
from src.schemas.databricks_vector_index import (
    IndexCreate,
    IndexInfo,
    IndexResponse,
    IndexListResponse,
    IndexState,
    IndexType
)


@pytest.fixture
def mock_auth_token():
    """Mock authentication token."""
    return "test-auth-token"


@pytest.fixture
def repository():
    """Create a DatabricksVectorIndexRepository instance."""
    return DatabricksVectorIndexRepository("https://example.databricks.com")


class TestDatabricksVectorIndexRepository:
    """Test suite for DatabricksVectorIndexRepository."""

    @pytest.mark.asyncio
    async def test_similarity_search_success(self, repository, mock_auth_token):
        """Test successful similarity search using REST API."""
        # Arrange
        index_name = "catalog.schema.test_index"
        endpoint_name = "test_endpoint"
        query_vector = [0.1, 0.2, 0.3, 0.4, 0.5]
        columns = ["id", "content", "metadata"]
        num_results = 10
        
        # Mock the search results
        expected_results = {
            "result": {
                "data_array": [
                    ["doc1", "Test content 1", {"key": "value1"}],
                    ["doc2", "Test content 2", {"key": "value2"}]
                ],
                "row_count": 2
            }
        }
        
        # Mock the auth token retrieval
        with patch.object(repository, '_get_auth_token', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth_token
            
            # Mock aiohttp session - patch the entire async with context
            with patch('src.repositories.databricks_vector_index_repository.aiohttp') as mock_aiohttp:
                # Create the response mock
                mock_response = AsyncMock()
                mock_response.status = 200
                mock_response.json = AsyncMock(return_value=expected_results)
                
                # Create the session mock
                mock_session = MagicMock()
                
                # Mock the post method - return a context manager
                mock_post_cm = MagicMock()
                mock_post_cm.__aenter__ = AsyncMock(return_value=mock_response)
                mock_post_cm.__aexit__ = AsyncMock(return_value=None)
                mock_session.post = MagicMock(return_value=mock_post_cm)
                
                # Mock ClientSession to return a context manager
                mock_session_cm = MagicMock()
                mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
                mock_session_cm.__aexit__ = AsyncMock(return_value=None)
                mock_aiohttp.ClientSession = MagicMock(return_value=mock_session_cm)
                
                # Act
                result = await repository.similarity_search(
                    index_name=index_name,
                    endpoint_name=endpoint_name,
                    query_vector=query_vector,
                    columns=columns,
                    num_results=num_results
                )
                
                # Assert
                assert result["success"] is True
                assert result["message"] == "Search completed successfully"
                assert result["results"] == expected_results
                assert "error" not in result or result.get("error") is None
                
                # Verify the auth token was retrieved
                mock_get_auth.assert_called_once_with(None)
                
                # Verify the correct URL was called
                expected_url = f"https://example.databricks.com/api/2.0/vector-search/indexes/catalog.schema.test_index/query"
                mock_session.post.assert_called_once()
                actual_url = mock_session.post.call_args[0][0]
                assert actual_url == expected_url

    @pytest.mark.asyncio
    async def test_similarity_search_with_filters(self, repository, mock_auth_token):
        """Test similarity search with filters using REST API."""
        # Arrange
        index_name = "catalog.schema.test_index"
        endpoint_name = "test_endpoint"
        query_vector = [0.1, 0.2, 0.3, 0.4, 0.5]
        columns = ["id", "content"]
        num_results = 5
        filters = {"metadata.category": "test"}
        
        # Mock the search results
        expected_results = {
            "result": {
                "data_array": [
                    ["doc1", "Filtered content"]
                ],
                "row_count": 1
            }
        }
        
        # Mock the auth token retrieval
        with patch.object(repository, '_get_auth_token', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth_token
            
            # Mock aiohttp session
            with patch('src.repositories.databricks_vector_index_repository.aiohttp') as mock_aiohttp:
                # Create the response mock
                mock_response = AsyncMock()
                mock_response.status = 200
                mock_response.json = AsyncMock(return_value=expected_results)
                
                # Create the session mock
                mock_session = MagicMock()
                
                # Mock the post method - return a context manager
                mock_post_cm = MagicMock()
                mock_post_cm.__aenter__ = AsyncMock(return_value=mock_response)
                mock_post_cm.__aexit__ = AsyncMock(return_value=None)
                mock_session.post = MagicMock(return_value=mock_post_cm)
                
                # Mock ClientSession to return a context manager
                mock_session_cm = MagicMock()
                mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
                mock_session_cm.__aexit__ = AsyncMock(return_value=None)
                mock_aiohttp.ClientSession = MagicMock(return_value=mock_session_cm)
                
                # Act
                result = await repository.similarity_search(
                    index_name=index_name,
                    endpoint_name=endpoint_name,
                    query_vector=query_vector,
                    columns=columns,
                    num_results=num_results,
                    filters=filters
                )
                
                # Assert
                assert result["success"] is True
                assert result["results"]["result"]["data_array"] == [["doc1", "Filtered content"]]
                
                # Verify filters were passed in the request
                mock_session.post.assert_called()
                call_kwargs = mock_session.post.call_args[1]
                assert "json" in call_kwargs
                assert "filters" in call_kwargs["json"]
                assert call_kwargs["json"]["filters"] == filters

    @pytest.mark.asyncio
    async def test_similarity_search_failure(self, repository, mock_auth_token):
        """Test failed similarity search using REST API."""
        # Arrange
        index_name = "catalog.schema.test_index"
        endpoint_name = "test_endpoint"
        query_vector = [0.1, 0.2, 0.3, 0.4, 0.5]
        columns = ["id", "content"]
        
        # Mock the auth token retrieval
        with patch.object(repository, '_get_auth_token', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth_token
            
            # Mock aiohttp session with error
            with patch('src.repositories.databricks_vector_index_repository.aiohttp') as mock_aiohttp:
                # Create the response mock with error
                mock_response = AsyncMock()
                mock_response.status = 500
                mock_response.text = AsyncMock(return_value="Internal Server Error")
                
                # Create the session mock
                mock_session = MagicMock()
                
                # Mock the post method - return a context manager
                mock_post_cm = MagicMock()
                mock_post_cm.__aenter__ = AsyncMock(return_value=mock_response)
                mock_post_cm.__aexit__ = AsyncMock(return_value=None)
                mock_session.post = MagicMock(return_value=mock_post_cm)
                
                # Mock ClientSession to return a context manager
                mock_session_cm = MagicMock()
                mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
                mock_session_cm.__aexit__ = AsyncMock(return_value=None)
                mock_aiohttp.ClientSession = MagicMock(return_value=mock_session_cm)
                
                # Act
                result = await repository.similarity_search(
                    index_name=index_name,
                    endpoint_name=endpoint_name,
                    query_vector=query_vector,
                    columns=columns
                )
                
                # Assert
                assert result["success"] is False
                assert "error" in result
                assert "Failed to perform search" in result["message"]
                assert result["results"] is None

    @pytest.mark.asyncio
    async def test_describe_index_success(self, repository, mock_auth_token):
        """Test successful describe_index using REST API."""
        # Arrange
        index_name = "catalog.schema.test_index"
        endpoint_name = "test_endpoint"
        
        # Mock the get_index call
        mock_index_response = IndexResponse(
            success=True,
            index=IndexInfo(
                name=index_name,
                endpoint_name=endpoint_name,
                index_type=IndexType.DIRECT_ACCESS,
                state=IndexState.READY,
                ready=True,
                row_count=1000,
                indexed_row_count=1000,
                embedding_dimension=768,
                primary_key="id"
            ),
            message="Index retrieved successfully"
        )
        
        with patch.object(repository, 'get_index', new_callable=AsyncMock) as mock_get_index:
            mock_get_index.return_value = mock_index_response
            
            # Act
            result = await repository.describe_index(
                index_name=index_name,
                endpoint_name=endpoint_name
            )
            
            # Assert
            assert result["success"] is True
            assert result["message"] == "Index description retrieved successfully"
            assert result["description"] is not None
            assert result["description"]["name"] == index_name
            assert result["description"]["num_rows"] == 1000
            assert result["description"]["status"]["ready"] is True

    @pytest.mark.asyncio
    async def test_upsert_success(self, repository, mock_auth_token):
        """Test successful upsert using REST API."""
        # Arrange
        index_name = "catalog.schema.test_index"
        endpoint_name = "test_endpoint"
        records = [
            {"id": "doc1", "content": "Test content 1", "embedding": [0.1, 0.2, 0.3]},
            {"id": "doc2", "content": "Test content 2", "embedding": [0.4, 0.5, 0.6]}
        ]
        
        # Mock the auth token retrieval
        with patch.object(repository, '_get_auth_token', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth_token
            
            # Mock aiohttp session
            with patch('src.repositories.databricks_vector_index_repository.aiohttp') as mock_aiohttp:
                # Create the response mock
                mock_response = AsyncMock()
                mock_response.status = 200
                mock_response.text = AsyncMock(return_value="Success")
                
                # Create the session mock
                mock_session = MagicMock()
                
                # Mock the post method - return a context manager
                mock_post_cm = MagicMock()
                mock_post_cm.__aenter__ = AsyncMock(return_value=mock_response)
                mock_post_cm.__aexit__ = AsyncMock(return_value=None)
                mock_session.post = MagicMock(return_value=mock_post_cm)
                
                # Mock ClientSession to return a context manager
                mock_session_cm = MagicMock()
                mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
                mock_session_cm.__aexit__ = AsyncMock(return_value=None)
                mock_aiohttp.ClientSession = MagicMock(return_value=mock_session_cm)
                
                # Act
                result = await repository.upsert(
                    index_name=index_name,
                    endpoint_name=endpoint_name,
                    records=records
                )
                
                # Assert
                assert result["success"] is True
                assert result["upserted_count"] == 2
                assert "Successfully upserted" in result["message"]
                
                # Verify the payload
                call_kwargs = mock_session.post.call_args[1]
                assert "json" in call_kwargs
                assert "inputs_json" in call_kwargs["json"]
                # inputs_json should be a JSON string
                assert isinstance(call_kwargs["json"]["inputs_json"], str)
                # Parse it back to verify content
                parsed_inputs = json.loads(call_kwargs["json"]["inputs_json"])
                assert parsed_inputs == records

    @pytest.mark.asyncio
    async def test_delete_records_success(self, repository, mock_auth_token):
        """Test successful delete_records using REST API."""
        # Arrange
        index_name = "catalog.schema.test_index"
        endpoint_name = "test_endpoint"
        primary_keys = ["doc1", "doc2", "doc3"]
        
        # Mock the auth token retrieval
        with patch.object(repository, '_get_auth_token', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth_token
            
            # Mock aiohttp session
            with patch('src.repositories.databricks_vector_index_repository.aiohttp') as mock_aiohttp:
                # Create the response mock
                mock_response = AsyncMock()
                mock_response.status = 204
                
                # Create the session mock
                mock_session = MagicMock()
                
                # Mock the post method - return a context manager
                mock_post_cm = MagicMock()
                mock_post_cm.__aenter__ = AsyncMock(return_value=mock_response)
                mock_post_cm.__aexit__ = AsyncMock(return_value=None)
                mock_session.post = MagicMock(return_value=mock_post_cm)
                
                # Mock ClientSession to return a context manager
                mock_session_cm = MagicMock()
                mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
                mock_session_cm.__aexit__ = AsyncMock(return_value=None)
                mock_aiohttp.ClientSession = MagicMock(return_value=mock_session_cm)
                
                # Act
                result = await repository.delete_records(
                    index_name=index_name,
                    endpoint_name=endpoint_name,
                    primary_keys=primary_keys
                )
                
                # Assert
                assert result["success"] is True
                assert result["deleted_count"] == 3
                assert "Successfully deleted" in result["message"]
                
                # Verify the payload
                call_kwargs = mock_session.post.call_args[1]
                assert "json" in call_kwargs
                assert "primary_keys" in call_kwargs["json"]
                assert call_kwargs["json"]["primary_keys"] == primary_keys

    @pytest.mark.asyncio
    async def test_count_documents_without_filters(self, repository, mock_auth_token):
        """Test counting documents without filters using REST API."""
        # Arrange
        index_name = "catalog.schema.test_index"
        endpoint_name = "test_endpoint"
        
        # Mock the describe_index response
        describe_response = {
            "success": True,
            "description": {
                "status": {
                    "indexed_row_count": 5000
                }
            }
        }
        
        with patch.object(repository, 'describe_index', new_callable=AsyncMock) as mock_describe:
            mock_describe.return_value = describe_response
            
            # Act
            count = await repository.count_documents(
                index_name=index_name,
                endpoint_name=endpoint_name
            )
            
            # Assert
            assert count == 5000
            mock_describe.assert_called_once_with(index_name, endpoint_name, None)

    @pytest.mark.asyncio
    async def test_count_documents_with_filters(self, repository, mock_auth_token):
        """Test counting documents with filters using REST API."""
        # Arrange
        index_name = "catalog.schema.test_index"
        endpoint_name = "test_endpoint"
        filters = {"category": "test"}
        
        # Mock similarity_search to return filtered results
        search_response = {
            "success": True,
            "results": {
                "result": {
                    "data_array": [["id1"], ["id2"], ["id3"]]
                }
            }
        }
        
        with patch.object(repository, 'similarity_search', new_callable=AsyncMock) as mock_search:
            mock_search.return_value = search_response
            
            # Act
            count = await repository.count_documents(
                index_name=index_name,
                endpoint_name=endpoint_name,
                filters=filters
            )
            
            # Assert
            assert count == 3
            mock_search.assert_called_once()
            # Check that filters were passed to similarity_search
            call_args = mock_search.call_args
            assert call_args[1]["filters"] == filters