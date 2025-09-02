"""
Memory backend configuration schemas.

This module defines schemas for configuring different memory storage backends.
"""
from enum import Enum
from typing import Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field


class MemoryBackendType(str, Enum):
    """Supported memory backend types."""
    DEFAULT = "default"  # CrewAI's default (ChromaDB + SQLite)
    DATABRICKS = "databricks"  # Databricks Vector Search
    # Future backends can be added here
    # PINECONE = "pinecone"
    # QDRANT = "qdrant"
    # WEAVIATE = "weaviate"


class DatabricksMemoryConfig(BaseModel):
    """Configuration for Databricks Vector Search memory backend."""
    
    # Memory endpoint configuration (Direct Access for dynamic data)
    endpoint_name: str = Field(..., description="Name of the Vector Search endpoint for memory (Direct Access)")
    
    # Document endpoint configuration (Storage Optimized for static data)
    document_endpoint_name: Optional[str] = Field(None, description="Name of the Vector Search endpoint for documents (Storage Optimized)")
    
    # Index names for different memory types
    short_term_index: str = Field(..., description="Index name for short-term memory (catalog.schema.index)")
    long_term_index: Optional[str] = Field(None, description="Index name for long-term memory")
    entity_index: Optional[str] = Field(None, description="Index name for entity memory")
    
    # Document embeddings index (for storage optimized endpoint)
    document_index: Optional[str] = Field(None, description="Index name for document embeddings")
    
    # Authentication (optional - can use environment variables)
    workspace_url: Optional[str] = Field(None, description="Databricks workspace URL")
    auth_type: Optional[str] = Field("default", description="Authentication type: default, pat, service_principal")
    
    # For PAT authentication
    personal_access_token: Optional[str] = Field(None, description="Personal Access Token")
    
    # For Service Principal authentication
    service_principal_client_id: Optional[str] = Field(None, description="Service Principal Client ID")
    service_principal_client_secret: Optional[str] = Field(None, description="Service Principal Client Secret")
    
    # Vector configuration
    embedding_dimension: int = Field(1024, description="Dimension of embedding vectors (1024 for databricks-gte-large-en)")
    
    # Catalog and schema information
    catalog: Optional[str] = Field(None, description="Unity Catalog name where indexes are created")
    schema_name: Optional[str] = Field(None, description="Schema name within catalog where indexes are created", alias="schema")
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "endpoint_name": "vector_search_endpoint",
                "short_term_index": "ml.agents.short_term_memory",
                "long_term_index": "ml.agents.long_term_memory",
                "entity_index": "ml.agents.entity_memory",
                "embedding_dimension": 1024
            }
        }
    }


class MemoryBackendCreate(BaseModel):
    """Schema for creating a memory backend configuration."""
    
    name: str = Field(..., description="Name for this configuration")
    description: Optional[str] = Field(None, description="Description of the configuration")
    backend_type: MemoryBackendType = Field(..., description="Type of memory backend")
    
    # Backend-specific configuration
    databricks_config: Optional[DatabricksMemoryConfig] = Field(None)
    
    # Common configuration
    enable_short_term: bool = Field(True, description="Enable short-term memory")
    enable_long_term: bool = Field(True, description="Enable long-term memory")
    enable_entity: bool = Field(True, description="Enable entity memory")
    
    # Advanced configuration
    enable_relationship_retrieval: bool = Field(False, description="Enable relationship-based entity retrieval (experimental)")
    
    # Advanced options
    custom_config: Optional[Dict[str, Any]] = Field(None)


class MemoryBackendUpdate(BaseModel):
    """Schema for updating a memory backend configuration."""
    
    name: Optional[str] = Field(None)
    description: Optional[str] = Field(None)
    backend_type: Optional[MemoryBackendType] = Field(None)
    
    # Backend-specific configuration
    databricks_config: Optional[DatabricksMemoryConfig] = Field(None)
    
    # Common configuration
    enable_short_term: Optional[bool] = Field(None)
    enable_long_term: Optional[bool] = Field(None)
    enable_entity: Optional[bool] = Field(None)
    
    # Advanced configuration
    enable_relationship_retrieval: Optional[bool] = Field(None)
    
    # Advanced options
    custom_config: Optional[Dict[str, Any]] = Field(None)
    is_active: Optional[bool] = Field(None)


class MemoryBackendResponse(BaseModel):
    """Schema for memory backend response."""
    
    id: str
    group_id: str
    name: str
    description: Optional[str]
    backend_type: MemoryBackendType
    
    # Configuration
    databricks_config: Optional[DatabricksMemoryConfig]
    enable_short_term: bool
    enable_long_term: bool
    enable_entity: bool
    enable_relationship_retrieval: bool
    custom_config: Optional[Dict[str, Any]]
    
    # Metadata
    is_active: bool
    is_default: bool
    created_at: datetime
    updated_at: datetime
    
    model_config = {
        "from_attributes": True,
        "arbitrary_types_allowed": True
    }


class MemoryBackendConfig(BaseModel):
    """Configuration for memory storage backend."""
    
    backend_type: MemoryBackendType = Field(
        MemoryBackendType.DEFAULT,
        description="Type of memory backend to use"
    )
    
    # Backend-specific configuration
    databricks_config: Optional[DatabricksMemoryConfig] = Field(
        None,
        description="Configuration for Databricks backend (required if backend_type is 'databricks')"
    )
    
    # Common configuration
    enable_short_term: bool = Field(True, description="Enable short-term memory")
    enable_long_term: bool = Field(True, description="Enable long-term memory")
    enable_entity: bool = Field(True, description="Enable entity memory")
    
    # Advanced configuration
    enable_relationship_retrieval: bool = Field(False, description="Enable relationship-based entity retrieval (experimental)")
    
    # Advanced options
    custom_config: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional backend-specific configuration"
    )
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "backend_type": "databricks",
                "databricks_config": {
                    "endpoint_name": "vector_search_endpoint",
                    "short_term_index": "ml.agents.short_term_memory",
                    "embedding_dimension": 1024
                },
                "enable_short_term": True,
                "enable_long_term": True,
                "enable_entity": True,
                "enable_relationship_retrieval": False
            }
        }
    }