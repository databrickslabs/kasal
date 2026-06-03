"""
Memory backend configuration schemas.

Defines the Pydantic models for configuring Kasal's cognitive memory backends.
CrewAI 1.10+ uses a single unified ``Memory`` class over one storage, so these
schemas no longer split memory into short-term / long-term / entity tiers.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class MemoryBackendType(str, Enum):
    """Supported memory backend types."""

    DEFAULT = "default"  # CrewAI unified Memory over its built-in LanceDB
    DATABRICKS = "databricks"  # Databricks Vector Search
    LAKEBASE = "lakebase"  # Lakebase pgvector


class CognitiveMemoryConfig(BaseModel):
    """Tuning knobs for CrewAI 1.10+ unified cognitive memory.

    These map 1:1 to ``crewai.memory.Memory`` constructor parameters. Defaults
    mirror upstream. Any value left ``None`` is omitted so CrewAI's own
    defaults apply.
    """

    # Composite score weights (should roughly sum to 1.0).
    semantic_weight: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Weight for semantic similarity in recall (default 0.5).",
    )
    recency_weight: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Weight for recency decay in recall (default 0.3).",
    )
    importance_weight: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Weight for explicit importance in recall (default 0.2).",
    )
    recency_half_life_days: Optional[int] = Field(
        None,
        ge=1,
        description="Days for recency score to halve (default 30).",
    )

    # Consolidation.
    consolidation_threshold: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description=(
            "Similarity above which consolidation is triggered on save "
            "(default 0.85). Set 1.0 to disable consolidation."
        ),
    )
    consolidation_limit: Optional[int] = Field(
        None,
        ge=1,
        description="Max existing records compared during consolidation (default 5).",
    )
    default_importance: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Importance used when LLM analysis is skipped (default 0.5).",
    )

    # Recall depth control.
    confidence_threshold_high: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Confidence at/above which recall returns directly (default 0.8).",
    )
    confidence_threshold_low: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Confidence below which deeper exploration triggers (default 0.5).",
    )
    complex_query_threshold: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Complex-query threshold for exploration (default 0.7).",
    )
    exploration_budget: Optional[int] = Field(
        None,
        ge=0,
        description="Number of LLM-driven exploration rounds during deep recall (default 1).",
    )
    query_analysis_threshold: Optional[int] = Field(
        None,
        ge=0,
        description=(
            "Character count below which deep recall skips LLM query analysis "
            "(default 200). Set 0 to always run LLM analysis."
        ),
    )

    # LLM override for memory analysis.
    memory_llm_model: Optional[str] = Field(
        None,
        description=(
            "Override the LLM used for memory analysis (scope, importance, "
            "consolidation). Defaults to the crew's LLM so OPENAI_API_KEY "
            "is not implicitly required."
        ),
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "semantic_weight": 0.5,
                "recency_weight": 0.3,
                "importance_weight": 0.2,
                "consolidation_threshold": 0.85,
                "exploration_budget": 1,
            }
        }
    }


class DatabricksMemoryConfig(BaseModel):
    """Configuration for Databricks Vector Search as the unified memory backend."""

    # Memory endpoint (Direct Access for dynamic record-level writes).
    endpoint_name: str = Field(
        ...,
        description="Name of the Vector Search endpoint for memory (Direct Access)",
    )

    # Unified cognitive memory index — one index stores every MemoryRecord.
    memory_index: str = Field(
        ...,
        description=(
            "Index for CrewAI unified cognitive memory "
            "(catalog.schema.index). Must use UNIFIED_SCHEMA."
        ),
    )

    # Document search endpoint (Storage Optimized for static corpora; unrelated to memory).
    document_endpoint_name: Optional[str] = Field(
        None,
        description=(
            "Name of the Vector Search endpoint for documents (Storage Optimized). "
            "Unrelated to memory; used by the knowledge-search feature."
        ),
    )
    document_index: Optional[str] = Field(
        None, description="Index name for document embeddings"
    )

    # Authentication (optional — environment variables / OBO also supported).
    workspace_url: Optional[str] = Field(None, description="Databricks workspace URL")
    auth_type: Optional[str] = Field(
        "default",
        description="Authentication type: default, pat, service_principal",
    )
    personal_access_token: Optional[str] = Field(
        None, description="Personal Access Token"
    )
    service_principal_client_id: Optional[str] = Field(
        None, description="Service Principal Client ID"
    )
    service_principal_client_secret: Optional[str] = Field(
        None, description="Service Principal Client Secret"
    )

    embedding_dimension: int = Field(
        1024,
        description="Dimension of embedding vectors (1024 for databricks-gte-large-en)",
    )

    # Catalog / schema metadata, used by the one-click setup flow.
    catalog: Optional[str] = Field(
        None, description="Unity Catalog name where the index is created"
    )
    schema_name: Optional[str] = Field(
        None,
        description="Schema name within catalog where the index is created",
        alias="schema",
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "endpoint_name": "vector_search_endpoint",
                "memory_index": "ml.agents.crew_memory",
                "embedding_dimension": 1024,
            }
        }
    }


class LakebaseMemoryConfig(BaseModel):
    """Configuration for Lakebase pgvector as the unified memory backend."""

    instance_name: Optional[str] = Field(
        None,
        description="Lakebase instance name (uses configured default if not set)",
    )
    embedding_dimension: int = Field(1024, description="Dimension of embedding vectors")

    # Unified cognitive memory table — one table stores every MemoryRecord.
    memory_table: str = Field(
        "crew_memory",
        description="Table for CrewAI unified cognitive memory records.",
    )

    tables_initialized: bool = Field(
        False,
        description="Whether the memory table has been initialized on the Lakebase instance",
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "embedding_dimension": 1024,
                "memory_table": "crew_memory",
                "tables_initialized": False,
            }
        }
    }


# ---------------------------------------------------------------------------
# CRUD schemas
# ---------------------------------------------------------------------------


class MemoryBackendCreate(BaseModel):
    """Schema for creating a memory backend configuration."""

    name: str = Field(..., description="Name for this configuration")
    description: Optional[str] = Field(
        None, description="Description of the configuration"
    )
    backend_type: MemoryBackendType = Field(..., description="Type of memory backend")

    databricks_config: Optional[DatabricksMemoryConfig] = Field(None)
    lakebase_config: Optional[LakebaseMemoryConfig] = Field(None)

    cognitive_config: Optional[CognitiveMemoryConfig] = Field(
        None,
        description="Optional tuning parameters for the unified cognitive memory.",
    )

    # Escape hatch for experimental backend-specific options that don't yet
    # have first-class schema fields.
    custom_config: Optional[Dict[str, Any]] = Field(None)


class MemoryBackendUpdate(BaseModel):
    """Schema for updating a memory backend configuration."""

    name: Optional[str] = Field(None)
    description: Optional[str] = Field(None)
    backend_type: Optional[MemoryBackendType] = Field(None)

    databricks_config: Optional[DatabricksMemoryConfig] = Field(None)
    lakebase_config: Optional[LakebaseMemoryConfig] = Field(None)

    cognitive_config: Optional[CognitiveMemoryConfig] = Field(None)

    custom_config: Optional[Dict[str, Any]] = Field(None)
    is_active: Optional[bool] = Field(None)


class MemoryBackendResponse(BaseModel):
    """Schema for memory backend response."""

    id: str
    group_id: str
    name: str
    description: Optional[str]
    backend_type: MemoryBackendType

    databricks_config: Optional[DatabricksMemoryConfig]
    lakebase_config: Optional[LakebaseMemoryConfig]
    cognitive_config: Optional[CognitiveMemoryConfig]
    custom_config: Optional[Dict[str, Any]]

    is_active: bool
    is_default: bool
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True, "arbitrary_types_allowed": True}


class MemoryBackendConfig(BaseModel):
    """Runtime configuration passed to the factory when building memory."""

    backend_type: MemoryBackendType = Field(
        MemoryBackendType.DEFAULT, description="Type of memory backend to use"
    )

    databricks_config: Optional[DatabricksMemoryConfig] = Field(
        None,
        description="Configuration for Databricks backend (required if backend_type='databricks')",
    )
    lakebase_config: Optional[LakebaseMemoryConfig] = Field(
        None,
        description="Configuration for Lakebase backend (required if backend_type='lakebase')",
    )

    cognitive_config: Optional[CognitiveMemoryConfig] = Field(
        None,
        description="Optional tuning parameters for the unified cognitive memory.",
    )

    custom_config: Optional[Dict[str, Any]] = Field(
        None, description="Additional backend-specific configuration"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "backend_type": "databricks",
                "databricks_config": {
                    "endpoint_name": "vector_search_endpoint",
                    "memory_index": "ml.agents.crew_memory",
                    "embedding_dimension": 1024,
                },
                "cognitive_config": {
                    "semantic_weight": 0.5,
                    "recency_weight": 0.3,
                    "importance_weight": 0.2,
                },
            }
        }
    }
