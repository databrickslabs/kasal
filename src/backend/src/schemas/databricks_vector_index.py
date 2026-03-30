"""
Pydantic schemas for Databricks Vector Search Indexes.

This module defines the request/response schemas for index operations.
"""
from typing import Optional, Dict, Any, List
from enum import Enum
from pydantic import BaseModel, Field
from datetime import datetime


class IndexType(str, Enum):
    """Index type enumeration."""
    DIRECT_ACCESS = "DIRECT_ACCESS"
    DELTA_SYNC = "DELTA_SYNC"
    MANAGED_EMBEDDING = "MANAGED_EMBEDDING"


class IndexState(str, Enum):
    """Index state enumeration."""
    READY = "READY"
    PROVISIONING = "PROVISIONING"
    OFFLINE = "OFFLINE"
    FAILED = "FAILED"
    NOT_FOUND = "NOT_FOUND"
    UNKNOWN = "UNKNOWN"


class VectorIndexColumn(BaseModel):
    """Schema for vector index column definition."""
    name: str = Field(..., description="Column name")
    type: str = Field(..., description="Column data type")
    is_primary_key: bool = Field(False, description="Whether this is the primary key")
    is_embedding: bool = Field(False, description="Whether this is the embedding column")


class IndexSchema(BaseModel):
    """Schema definition for a vector index."""
    columns: List[VectorIndexColumn] = Field(..., description="Index columns")
    primary_key: str = Field(..., description="Primary key column name")
    embedding_vector_column: str = Field(..., description="Embedding vector column name")
    embedding_dimension: int = Field(..., description="Dimension of embedding vectors")


class IndexCreate(BaseModel):
    """Schema for creating a vector search index."""
    name: str = Field(..., description="Full index name (catalog.schema.table)")
    endpoint_name: str = Field(..., description="Endpoint to host the index")
    primary_key: str = Field("id", description="Primary key column")
    embedding_dimension: int = Field(..., description="Dimension of embedding vectors")
    embedding_vector_column: str = Field("embedding", description="Embedding column name")
    schema_definition: Dict[str, Any] = Field(..., description="Index schema definition", alias="schema")
    
    model_config = {
        "use_enum_values": True,
        "populate_by_name": True
    }


class IndexInfo(BaseModel):
    """Schema for index information."""
    name: str = Field(..., description="Full index name")
    endpoint_name: str = Field(..., description="Endpoint hosting the index")
    index_type: Optional[IndexType] = Field(None, description="Type of index")
    state: IndexState = Field(..., description="Current state of the index")
    ready: bool = Field(False, description="Whether the index is ready for use")
    row_count: int = Field(0, description="Number of rows in the index")
    indexed_row_count: int = Field(0, description="Number of indexed rows")
    embedding_dimension: Optional[int] = Field(None, description="Dimension of embeddings")
    primary_key: Optional[str] = Field(None, description="Primary key column")
    creation_timestamp: Optional[datetime] = Field(None, description="When the index was created")
    last_updated_timestamp: Optional[datetime] = Field(None, description="Last update time")
    
    model_config = {
        "use_enum_values": True,
        "populate_by_name": True
    }


class IndexResponse(BaseModel):
    """Response schema for index operations."""
    success: bool = Field(..., description="Whether the operation was successful")
    index: Optional[IndexInfo] = Field(None, description="Index information")
    message: Optional[str] = Field(None, description="Success or error message")
    error: Optional[str] = Field(None, description="Error details if failed")


class IndexListResponse(BaseModel):
    """Response schema for listing indexes."""
    success: bool = Field(..., description="Whether the operation was successful")
    indexes: List[IndexInfo] = Field(default_factory=list, description="List of indexes")
    message: Optional[str] = Field(None, description="Success or error message")


class IndexDeleteRequest(BaseModel):
    """Request schema for deleting an index."""
    index_name: str = Field(..., description="Full index name to delete")
    endpoint_name: str = Field(..., description="Endpoint hosting the index")
    force: bool = Field(False, description="Force deletion even if index has data")


class IndexEmptyRequest(BaseModel):
    """Request schema for emptying an index."""
    index_name: str = Field(..., description="Full index name to empty")
    endpoint_name: str = Field(..., description="Endpoint hosting the index")
    workspace_url: str = Field(..., description="Databricks workspace URL")