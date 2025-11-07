"""
Comprehensive unit tests for Databricks Vector Search schemas.

Tests all Pydantic models for validation, serialization, and edge cases.
"""
import pytest
from typing import Dict, Any, List, Union
from pydantic import ValidationError

from src.schemas.databricks_vector_search import (
    VectorSearchRequest,
    VectorUpsertRequest,
    VectorDeleteRequest,
    SearchResult,
    VectorSearchResponse,
    VectorUpsertResponse,
    VectorDeleteResponse
)


class TestVectorSearchRequest:
    """Test VectorSearchRequest schema."""

    def test_vector_search_request_minimal(self):
        """Test VectorSearchRequest with minimal data."""
        request = VectorSearchRequest()
        
        assert request.query_vector is None
        assert request.query_text is None
        assert request.k == 10  # Default value
        assert request.filters is None
        assert request.columns is None

    def test_vector_search_request_with_query_vector(self):
        """Test VectorSearchRequest with query vector."""
        query_vector = [0.1, 0.2, 0.3, 0.4, 0.5]
        request = VectorSearchRequest(query_vector=query_vector)
        
        assert request.query_vector == query_vector
        assert request.query_text is None
        assert request.k == 10

    def test_vector_search_request_with_query_text(self):
        """Test VectorSearchRequest with query text."""
        query_text = "test search query"
        request = VectorSearchRequest(query_text=query_text)
        
        assert request.query_text == query_text
        assert request.query_vector is None
        assert request.k == 10

    def test_vector_search_request_with_custom_k(self):
        """Test VectorSearchRequest with custom k value."""
        k = 20
        request = VectorSearchRequest(k=k)
        
        assert request.k == k

    def test_vector_search_request_with_filters(self):
        """Test VectorSearchRequest with filters."""
        filters = {"category": "test", "score": {"$gt": 0.5}}
        request = VectorSearchRequest(filters=filters)
        
        assert request.filters == filters

    def test_vector_search_request_with_columns(self):
        """Test VectorSearchRequest with columns."""
        columns = ["id", "content", "metadata"]
        request = VectorSearchRequest(columns=columns)
        
        assert request.columns == columns

    def test_vector_search_request_full(self):
        """Test VectorSearchRequest with all fields."""
        data = {
            "query_vector": [0.1, 0.2, 0.3],
            "query_text": "test query",
            "k": 15,
            "filters": {"type": "document"},
            "columns": ["id", "content"]
        }
        request = VectorSearchRequest(**data)
        
        assert request.query_vector == data["query_vector"]
        assert request.query_text == data["query_text"]
        assert request.k == data["k"]
        assert request.filters == data["filters"]
        assert request.columns == data["columns"]


class TestVectorUpsertRequest:
    """Test VectorUpsertRequest schema."""

    def test_vector_upsert_request_minimal(self):
        """Test VectorUpsertRequest with minimal data."""
        vectors = [{"id": "1", "vector": [0.1, 0.2, 0.3]}]
        request = VectorUpsertRequest(vectors=vectors)
        
        assert request.vectors == vectors
        assert request.primary_keys is None

    def test_vector_upsert_request_with_primary_keys(self):
        """Test VectorUpsertRequest with primary keys."""
        vectors = [{"id": "1", "vector": [0.1, 0.2, 0.3]}]
        primary_keys = ["1"]
        request = VectorUpsertRequest(vectors=vectors, primary_keys=primary_keys)
        
        assert request.vectors == vectors
        assert request.primary_keys == primary_keys

    def test_vector_upsert_request_multiple_vectors(self):
        """Test VectorUpsertRequest with multiple vectors."""
        vectors = [
            {"id": "1", "vector": [0.1, 0.2, 0.3]},
            {"id": "2", "vector": [0.4, 0.5, 0.6]}
        ]
        request = VectorUpsertRequest(vectors=vectors)
        
        assert request.vectors == vectors
        assert len(request.vectors) == 2

    def test_vector_upsert_request_missing_vectors(self):
        """Test VectorUpsertRequest validation error when vectors missing."""
        with pytest.raises(ValidationError) as exc_info:
            VectorUpsertRequest()
        
        assert "vectors" in str(exc_info.value)


class TestVectorDeleteRequest:
    """Test VectorDeleteRequest schema."""

    def test_vector_delete_request_string_keys(self):
        """Test VectorDeleteRequest with string primary keys."""
        primary_keys = ["key1", "key2", "key3"]
        request = VectorDeleteRequest(primary_keys=primary_keys)
        
        assert request.primary_keys == primary_keys

    def test_vector_delete_request_int_keys(self):
        """Test VectorDeleteRequest with integer primary keys."""
        primary_keys = [1, 2, 3]
        request = VectorDeleteRequest(primary_keys=primary_keys)
        
        assert request.primary_keys == primary_keys

    def test_vector_delete_request_mixed_keys(self):
        """Test VectorDeleteRequest with mixed primary key types."""
        primary_keys = ["key1", 2, "key3"]
        request = VectorDeleteRequest(primary_keys=primary_keys)
        
        assert request.primary_keys == primary_keys

    def test_vector_delete_request_missing_keys(self):
        """Test VectorDeleteRequest validation error when primary_keys missing."""
        with pytest.raises(ValidationError) as exc_info:
            VectorDeleteRequest()
        
        assert "primary_keys" in str(exc_info.value)


class TestSearchResult:
    """Test SearchResult schema."""

    def test_search_result_minimal(self):
        """Test SearchResult with minimal data."""
        result = SearchResult(id="test-id", score=0.95)
        
        assert result.id == "test-id"
        assert result.score == 0.95
        assert result.metadata == {}

    def test_search_result_with_metadata(self):
        """Test SearchResult with metadata."""
        metadata = {"title": "Test Document", "category": "test"}
        result = SearchResult(id="test-id", score=0.95, metadata=metadata)
        
        assert result.id == "test-id"
        assert result.score == 0.95
        assert result.metadata == metadata

    def test_search_result_int_id(self):
        """Test SearchResult with integer ID."""
        result = SearchResult(id=123, score=0.85)
        
        assert result.id == 123
        assert result.score == 0.85

    def test_search_result_missing_required_fields(self):
        """Test SearchResult validation error when required fields missing."""
        with pytest.raises(ValidationError) as exc_info:
            SearchResult()
        
        error_str = str(exc_info.value)
        assert "id" in error_str
        assert "score" in error_str


class TestVectorSearchResponse:
    """Test VectorSearchResponse schema."""

    def test_vector_search_response_minimal(self):
        """Test VectorSearchResponse with minimal data."""
        response = VectorSearchResponse(success=True)
        
        assert response.success is True
        assert response.results == []
        assert response.message is None
        assert response.error is None

    def test_vector_search_response_with_results(self):
        """Test VectorSearchResponse with search results."""
        results = [
            SearchResult(id="1", score=0.95),
            SearchResult(id="2", score=0.85)
        ]
        response = VectorSearchResponse(success=True, results=results)
        
        assert response.success is True
        assert len(response.results) == 2
        assert response.results[0].id == "1"
        assert response.results[1].id == "2"

    def test_vector_search_response_with_message(self):
        """Test VectorSearchResponse with success message."""
        message = "Search completed successfully"
        response = VectorSearchResponse(success=True, message=message)
        
        assert response.success is True
        assert response.message == message

    def test_vector_search_response_with_error(self):
        """Test VectorSearchResponse with error."""
        error = "Search failed due to invalid query"
        response = VectorSearchResponse(success=False, error=error)
        
        assert response.success is False
        assert response.error == error

    def test_vector_search_response_missing_success(self):
        """Test VectorSearchResponse validation error when success missing."""
        with pytest.raises(ValidationError) as exc_info:
            VectorSearchResponse()
        
        assert "success" in str(exc_info.value)


class TestVectorUpsertResponse:
    """Test VectorUpsertResponse schema."""

    def test_vector_upsert_response_minimal(self):
        """Test VectorUpsertResponse with minimal data."""
        response = VectorUpsertResponse(success=True)
        
        assert response.success is True
        assert response.upserted_count == 0  # Default value
        assert response.message is None
        assert response.error is None

    def test_vector_upsert_response_with_count(self):
        """Test VectorUpsertResponse with upserted count."""
        response = VectorUpsertResponse(success=True, upserted_count=5)
        
        assert response.success is True
        assert response.upserted_count == 5

    def test_vector_upsert_response_with_message(self):
        """Test VectorUpsertResponse with success message."""
        message = "5 vectors upserted successfully"
        response = VectorUpsertResponse(success=True, message=message)
        
        assert response.success is True
        assert response.message == message

    def test_vector_upsert_response_with_error(self):
        """Test VectorUpsertResponse with error."""
        error = "Upsert failed due to invalid vector format"
        response = VectorUpsertResponse(success=False, error=error)
        
        assert response.success is False
        assert response.error == error


class TestVectorDeleteResponse:
    """Test VectorDeleteResponse schema."""

    def test_vector_delete_response_minimal(self):
        """Test VectorDeleteResponse with minimal data."""
        response = VectorDeleteResponse(success=True)
        
        assert response.success is True
        assert response.deleted_count == 0  # Default value
        assert response.message is None
        assert response.error is None

    def test_vector_delete_response_with_count(self):
        """Test VectorDeleteResponse with deleted count."""
        response = VectorDeleteResponse(success=True, deleted_count=3)
        
        assert response.success is True
        assert response.deleted_count == 3

    def test_vector_delete_response_with_message(self):
        """Test VectorDeleteResponse with success message."""
        message = "3 vectors deleted successfully"
        response = VectorDeleteResponse(success=True, message=message)
        
        assert response.success is True
        assert response.message == message

    def test_vector_delete_response_with_error(self):
        """Test VectorDeleteResponse with error."""
        error = "Delete failed due to invalid primary keys"
        response = VectorDeleteResponse(success=False, error=error)
        
        assert response.success is False
        assert response.error == error
