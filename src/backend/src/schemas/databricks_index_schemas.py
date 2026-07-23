"""
Centralized schema definitions for Databricks Vector Search indexes.

Two index kinds live in Databricks Vector Search:

1. ``unified`` — CrewAI 1.10+ unified cognitive memory. One index stores every
   :class:`crewai.memory.types.MemoryRecord` written by a crew's unified Memory
   instance. See :class:`UNIFIED_SCHEMA` below.

2. ``document`` — the separate knowledge-search feature (document embeddings,
   RAG over Kasal's uploaded documents). Unrelated to crew memory.
"""
from typing import Any, Dict, List


class DatabricksIndexSchemas:
    """Column definitions for Kasal's Databricks Vector Search indexes."""

    # ------------------------------------------------------------------
    # Unified cognitive memory (CrewAI 1.10+)
    # ------------------------------------------------------------------
    # Maps directly to crewai.memory.types.MemoryRecord fields, plus the
    # Kasal-specific columns needed for tenant isolation and provenance.
    UNIFIED_SCHEMA: Dict[str, str] = {
        # Core MemoryRecord fields
        "id": "string",
        "content": "string",
        "scope": "string",                # Hierarchical path e.g. "/crew/123/research"
        "categories": "string",           # JSON array of category tags
        "importance": "float",            # 0.0 - 1.0, inferred by LLM or explicit
        "source": "string",               # Provenance tag e.g. "agent:researcher"
        "private": "boolean",             # Visibility flag for source-based filtering
        "metadata": "string",             # JSON dict for arbitrary metadata

        # Temporal
        "created_at": "timestamp",
        "last_accessed": "timestamp",

        # Kasal tenant isolation (also duplicated into metadata for portability)
        "crew_id": "string",
        "agent_id": "string",
        "group_id": "string",
        "session_id": "string",           # Maps to job_id for run-scoped queries

        # Provenance
        "llm_model": "string",            # Model that generated/analyzed this memory
        "tools_used": "string",           # JSON array of tool names

        # Technical
        "embedding": "array<float>",
        "embedding_model": "string",
        "version": "int",                 # Schema version for future migrations
    }

    UNIFIED_SEARCH_COLUMNS: List[str] = [
        "id", "content", "scope", "categories", "importance",
        "source", "private", "metadata",
        "created_at", "last_accessed",
        "crew_id", "agent_id", "group_id", "session_id",
        "llm_model", "tools_used",
        "embedding_model", "version",
    ]

    # ------------------------------------------------------------------
    # Document search (separate knowledge-search feature, not crew memory)
    # ------------------------------------------------------------------
    DOCUMENT_SCHEMA: Dict[str, str] = {
        "id": "string",
        "title": "string",
        "content": "string",
        "source": "string",
        "document_type": "string",

        "section": "string",
        "chunk_index": "int",
        "chunk_size": "int",
        "parent_document_id": "string",
        "document_summary": "string",

        "agent_ids": "string",

        "created_at": "string",
        "updated_at": "string",

        "doc_metadata": "string",
        "group_id": "string",

        "embedding": "array<float>",
        "embedding_model": "string",
        "version": "int",
    }

    DOCUMENT_SEARCH_COLUMNS: List[str] = [
        "id", "title", "content", "source", "document_type",
        "section", "chunk_index", "chunk_size", "parent_document_id",
        # ``document_summary`` is stored but not queried back for backward compat.
        "agent_ids", "created_at", "updated_at", "doc_metadata", "group_id",
        "embedding_model", "version",
    ]

    # ------------------------------------------------------------------
    # Lookups
    # ------------------------------------------------------------------

    @classmethod
    def get_schema(cls, index_type: str) -> Dict[str, str]:
        """Return column definitions for ``"unified"`` or ``"document"``."""
        schemas: Dict[str, Dict[str, str]] = {
            "unified": cls.UNIFIED_SCHEMA,
            "document": cls.DOCUMENT_SCHEMA,
        }
        # "memory" is an accepted synonym for "unified".
        if index_type == "memory":
            return cls.UNIFIED_SCHEMA
        return schemas.get(index_type, {})

    @classmethod
    def get_search_columns(cls, index_type: str) -> List[str]:
        """Return the columns requested in a similarity-search call."""
        columns: Dict[str, List[str]] = {
            "unified": cls.UNIFIED_SEARCH_COLUMNS,
            "document": cls.DOCUMENT_SEARCH_COLUMNS,
        }
        if index_type == "memory":
            return cls.UNIFIED_SEARCH_COLUMNS
        return columns.get(index_type, ["id"])

    @classmethod
    def get_column_positions(cls, index_type: str) -> Dict[str, int]:
        """Return a mapping of column name to position for result parsing."""
        columns = cls.get_search_columns(index_type)
        return {col: idx for idx, col in enumerate(columns)}

    @classmethod
    def parse_search_result(cls, index_type: str, result: List[Any]) -> Dict[str, Any]:
        """Zip a raw search-result row with its expected column names."""
        columns = cls.get_search_columns(index_type)
        parsed: Dict[str, Any] = {}
        for idx, value in enumerate(result):
            if idx < len(columns):
                parsed[columns[idx]] = value
        return parsed
