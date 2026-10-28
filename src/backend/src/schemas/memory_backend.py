"""
Memory backend configuration schemas.

Defines the Pydantic models for configuring Kasal's memory backends.
CrewAI 1.10+ uses a single unified ``Memory`` class over one storage, so these
schemas no longer split memory into short-term / long-term / entity tiers.
"""

import re as _re
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator

# SECURITY: memory_table is interpolated into raw Lakebase SQL — restrict to a
# strict SQL identifier so it cannot inject SQL or comment out tenant filters.
_SAFE_TABLE_NAME = _re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class MemoryBackendType(str, Enum):
    """Supported memory backend types."""

    DEFAULT = "default"  # Local SQLite store (services/memory/local_storage_backend.py)
    DATABRICKS = "databricks"  # Databricks Vector Search
    LAKEBASE = "lakebase"  # Lakebase pgvector


class MemoryTuningConfig(BaseModel):
    """Memory Tuning knobs for a teamspace's memory (Configuration > Memory).

    The scoring weights, half-life and relevance threshold reach the storage
    backend (``MemoryBackendFactory._scoring_kwargs``); everything else is a
    declared field of the engine's ``Memory`` (``CrewMemoryService``
    forwards them), so no value set here is ever silently dropped. Any value
    left ``None`` is omitted and the code default applies.
    """

    # Composite score weights (semantic + keyword + recency + importance —
    # should roughly sum to 1.0).
    semantic_weight: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Weight for semantic similarity in recall (default 0.6).",
    )
    keyword_weight: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Weight for query-term overlap in recall (default 0.15).",
    )
    recency_weight: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Weight for recency decay in recall (default 0.15).",
    )
    importance_weight: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Weight for explicit importance in recall (default 0.1).",
    )
    recency_half_life_days: Optional[int] = Field(
        None,
        ge=1,
        description="Days for recency score to halve (default 30).",
    )
    relevance_threshold: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description=(
            "Minimum semantic similarity for a memory to be recalled at all "
            "(default 0.35). Applied before recency/importance blending, so "
            "unrelated memories are never pulled into the context."
        ),
    )
    recall_min_score: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description=(
            "Similarity (cosine) floor below which a memory is not recalled. "
            "Recency and importance only rank what passes it, so an old "
            "memory is never dropped for its age. Left unset, the floor "
            "follows the embedder in use: 0.60 with the Databricks embedder, "
            "0.64 with the local Ollama fallback."
        ),
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

    # Hygiene and retention (were KASAL_MEMORY_* environment variables).
    recall_max_drop: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description=(
            "Recall drops candidates scoring more than this below the best one "
            "(default 0.12). Keeps 'least unrelated filler' out of the context."
        ),
    )
    write_screening: Optional[Literal["quarantine", "annotate", "off"]] = Field(
        None,
        description=(
            "Prompt-injection screening before a memory is saved: quarantine "
            "(default) drops high-severity content, annotate only records it, "
            "off screens nothing."
        ),
    )
    forgetting_enabled: Optional[bool] = Field(
        None,
        description=(
            "Delete memories past their retention rule (default off: it is the "
            "only pass that removes something a user might still want)."
        ),
    )
    superseded_retention_days: Optional[float] = Field(
        None,
        ge=1,
        description="Days a replaced fact is kept as history (default 90).",
    )
    episodic_ttl_days: Optional[float] = Field(
        None,
        ge=1,
        description="Days an unimportant run/chat memory is kept (default 180).",
    )
    importance_floor: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Memories at or above this importance are never forgotten (default 0.4).",
    )
    supersession_enabled: Optional[bool] = Field(
        None,
        description="Retire facts a newer one contradicts (default on).",
    )
    llm_consolidation_enabled: Optional[bool] = Field(
        None,
        description="Merge clusters of near-duplicate memories between runs (default on).",
    )

    graph_memory: Optional[bool] = Field(
        None,
        description=(
            "Store extracted facts in an entity graph instead of whole records "
            "(default off). A write extracts durable facts and a newer value "
            "replaces the one it contradicts; recall is one search plus the "
            "facts about the matched entities."
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

    # Memory index — one index stores every MemoryRecord.
    memory_index: str = Field(
        ...,
        description=(
            "Index for Kasal memory (catalog.schema.index). Must use UNIFIED_SCHEMA."
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
    embedding_dimension: int = Field(
        default=1024, description="Dimension of embedding vectors"
    )

    # Memory table — one table stores every MemoryRecord.
    memory_table: str = Field(
        default="crew_memory",
        description="Table for CrewAI memory records.",
    )

    @field_validator("memory_table")
    @classmethod
    def _validate_memory_table(cls, v: str) -> str:
        if not v or not _SAFE_TABLE_NAME.match(str(v)):
            raise ValueError(f"Invalid memory_table identifier: {v!r}")
        return v

    tables_initialized: bool = Field(
        False,
        description="Whether the memory table has been initialized on the Lakebase instance",
    )

    #: Role knowledge sessions assume (SET ROLE); it replaced the
    #: LAKEBASE_KNOWLEDGE_ROLE env var. None = databricks_superuser, "" = none.
    db_role: Optional[str] = Field(
        default=None,
        description=(
            "Role assumed for knowledge search on this instance "
            "(default databricks_superuser; empty string assumes none)."
        ),
    )

    @field_validator("db_role")
    @classmethod
    def _validate_db_role(cls, v: Optional[str]) -> Optional[str]:
        if v is None or v == "":
            return v
        if not _SAFE_TABLE_NAME.match(v):
            raise ValueError(f"Invalid db_role identifier: {v!r}")
        return v

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

    cognitive_config: Optional[MemoryTuningConfig] = Field(
        None,
        description="Optional tuning parameters for the memory.",
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

    cognitive_config: Optional[MemoryTuningConfig] = Field(None)

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
    cognitive_config: Optional[MemoryTuningConfig]
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
        default=None,
        description="Configuration for Databricks backend (required if backend_type='databricks')",
    )
    lakebase_config: Optional[LakebaseMemoryConfig] = Field(
        None,
        description="Configuration for Lakebase backend (required if backend_type='lakebase')",
    )

    cognitive_config: Optional[MemoryTuningConfig] = Field(
        default=None,
        description="Optional tuning parameters for the memory.",
    )

    custom_config: Optional[Dict[str, Any]] = Field(
        default=None, description="Additional backend-specific configuration"
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


class MemoryGraphEntity(BaseModel):
    """One entity in a teamspace's graph memory."""

    id: str
    name: str
    type: str


class MemoryGraphFact(BaseModel):
    """One fact: ``subject --relation--> object`` and the sentence stating it.
    ``valid_to`` set means a newer fact (``superseded_by``) replaced it."""

    id: str
    subject_id: str
    subject: str
    relation: str
    object_id: Optional[str] = None
    object: str
    statement: str
    source: Optional[str] = None
    created_at: Optional[datetime] = None
    valid_to: Optional[datetime] = None
    superseded_by: Optional[str] = None
    execution_id: Optional[str] = None
    qualifiers: Dict[str, str] = Field(
        default_factory=dict,
        description="What qualifies the relation: a role, a period, a field.",
    )
    kind: str = Field(
        "semantic",
        description=(
            "procedural (a standing preference, always recalled), semantic (a "
            "fact the user stated), finding (a fact a run established) or "
            "episodic (a run note)."
        ),
    )


class MemoryGraphResponse(BaseModel):
    """The memory browser's ontology view."""

    backend: str
    enabled: bool = Field(
        description="Whether graph memory is switched on for this teamspace."
    )
    entities: List[MemoryGraphEntity] = Field(default_factory=list)
    facts: List[MemoryGraphFact] = Field(default_factory=list)


class MemoryGraphForgetResponse(BaseModel):
    """Result of asking memory to stop recalling one fact."""

    forgotten: bool = Field(
        description="False when the fact was not found or was already retired."
    )
