"""
Pydantic schemas for Databricks Vector Search operations.

This module defines the request/response schemas for search and vector operations.
"""
from typing import Optional, Dict, Any, List, Union
from pydantic import BaseModel, Field


class VectorSearchRequest(BaseModel):
    """Request schema for vector search."""
    query_vector: Optional[List[float]] = Field(None, description="Query vector for similarity search")
    query_text: Optional[str] = Field(None, description="Text to be embedded for search")
    k: int = Field(10, description="Number of results to return")
    filters: Optional[Dict[str, Any]] = Field(None, description="Filters to apply to search")
    columns: Optional[List[str]] = Field(None, description="Columns to return in results")


class VectorUpsertRequest(BaseModel):
    """Request schema for upserting vectors."""
    vectors: List[Dict[str, Any]] = Field(..., description="Vectors to upsert")
    primary_keys: Optional[List[str]] = Field(None, description="Primary keys for the vectors")


class VectorDeleteRequest(BaseModel):
    """Request schema for deleting vectors."""
    primary_keys: List[Union[str, int]] = Field(..., description="Primary keys of vectors to delete")


class SearchResult(BaseModel):
    """Schema for a single search result."""
    id: Union[str, int] = Field(..., description="Primary key of the result")
    score: float = Field(..., description="Similarity score")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")


class VectorSearchResponse(BaseModel):
    """Response schema for vector search."""
    success: bool = Field(..., description="Whether the operation was successful")
    results: List[SearchResult] = Field(default_factory=list, description="Search results")
    message: Optional[str] = Field(None, description="Success or error message")
    error: Optional[str] = Field(None, description="Error details if failed")


class VectorUpsertResponse(BaseModel):
    """Response schema for vector upsert."""
    success: bool = Field(..., description="Whether the operation was successful")
    upserted_count: int = Field(0, description="Number of vectors upserted")
    message: Optional[str] = Field(None, description="Success or error message")
    error: Optional[str] = Field(None, description="Error details if failed")


class VectorDeleteResponse(BaseModel):
    """Response schema for vector deletion."""
    success: bool = Field(..., description="Whether the operation was successful")
    deleted_count: int = Field(0, description="Number of vectors deleted")
    message: Optional[str] = Field(None, description="Success or error message")
    error: Optional[str] = Field(None, description="Error details if failed")