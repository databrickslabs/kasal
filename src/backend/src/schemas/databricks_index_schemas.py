"""
Centralized schema definitions for Databricks Vector Search indexes.

This module defines the schema for all memory types in one place to ensure consistency
across index creation, data saving, and search operations.
"""
from typing import Dict, List, Any
from enum import Enum


class MemoryType(Enum):
    """Memory type enumeration."""
    SHORT_TERM = "short_term"
    LONG_TERM = "long_term"
    ENTITY = "entity"
    DOCUMENT = "document"


class DatabricksIndexSchemas:
    """Centralized schema definitions for Databricks Vector Search indexes."""
    
    # Short-term memory schema
    SHORT_TERM_SCHEMA = {
        # Primary content (most important)
        "id": "string",
        "content": "string",
        "query_text": "string",  # Original query text (for hybrid search)
        "session_id": "string",  # Session tracking for short-term memory
        "interaction_sequence": "bigint",  # Sequence number within session
        
        # Temporal tracking
        "timestamp": "string",
        "created_at": "timestamp",  # When the memory was created
        "ttl_hours": "int",  # Time-to-live in hours (for sliding window)
        
        # Context and metadata
        "metadata": "string",  # JSON string
        "crew_id": "string", 
        "agent_id": "string",
        "group_id": "string",  # For proper tenant isolation
        
        # Provenance tracking
        "llm_model": "string",  # Model that generated this memory (e.g., gpt-4o, claude-3-5-sonnet)
        "tools_used": "string",  # JSON array of tools used (e.g., ["web_search", "database_query"])
        
        # Technical fields
        "embedding": "array<float>",
        "embedding_model": "string",  # Model used for embedding
        "version": "int"  # Schema version for future migrations
    }
    
    # Columns to request when searching short-term memory
    SHORT_TERM_SEARCH_COLUMNS = [
        "id", "content", "query_text", "session_id", "interaction_sequence",
        "timestamp", "created_at", "ttl_hours",
        "metadata", "crew_id", "agent_id", "group_id",
        "llm_model", "tools_used",
        "embedding_model", "version"
    ]
    
    # Long-term memory schema
    LONG_TERM_SCHEMA = {
        # Primary content (most important)
        "id": "string",
        "content": "string",
        "task_description": "string",  # Full task description
        "task_hash": "string",  # Hash for exact task matching
        "quality": "float",  # Quality score for the memory
        "importance": "float",
        
        # Temporal tracking
        "timestamp": "string",
        "last_accessed": "timestamp",  # For retrieval tracking
        
        # Context and metadata
        "metadata": "string",  # JSON string
        "crew_id": "string",
        "agent_id": "string", 
        "group_id": "string",  # For proper tenant isolation
        
        # Provenance tracking
        "llm_model": "string",  # Model that completed this task
        "tools_used": "string",  # JSON array of tools used during task
        
        # Technical fields
        "embedding": "array<float>",
        "embedding_model": "string",  # Model used for embedding
        "version": "int"  # Schema version for future migrations
    }
    
    # Columns to request when searching long-term memory
    LONG_TERM_SEARCH_COLUMNS = [
        "id", "content", "task_description", "task_hash",
        "quality", "importance", "timestamp", "last_accessed",
        "metadata", "crew_id", "agent_id", "group_id",
        "llm_model", "tools_used",
        "embedding_model", "version"
    ]
    
    # Entity memory schema (enhanced with provenance tracking)
    ENTITY_SCHEMA = {
        # Primary identifiers (required)
        "id": "string",
        "entity_name": "string",
        "entity_type": "string",
        
        # Core entity data
        "description": "string",  # Entity description
        "relationships": "string",  # JSON string of related entities
        
        # Temporal tracking
        "timestamp": "string",  # When entity was created/updated
        
        # Context and metadata (required for isolation)
        "crew_id": "string",
        "agent_id": "string",
        "group_id": "string",  # Required for proper tenant isolation
        
        # Provenance tracking
        "llm_model": "string",  # Model that extracted/generated this entity
        "tools_used": "string",  # JSON array of tools used for this entity
        
        # Technical fields (required)
        "embedding": "array<float>",
        "embedding_model": "string"  # Model used for embedding (e.g., databricks-gte-large-en)
    }
    
    # Columns to request when searching entity memory
    ENTITY_SEARCH_COLUMNS = [
        "id", "entity_name", "entity_type", 
        "description", "relationships",
        "timestamp", 
        "crew_id", "agent_id", "group_id",
        "llm_model", "tools_used",
        "embedding_model"
    ]
    
    # Document memory schema
    DOCUMENT_SCHEMA = {
        # Primary content (most important)
        "id": "string",
        "title": "string",
        "content": "string",
        "source": "string",
        "document_type": "string",  # Type of document (md, pdf, txt, etc.)
        
        # Document structure
        "section": "string",  # Section within document
        "chunk_index": "int",  # Index of chunk within document
        "chunk_size": "int",  # Size of the chunk in characters
        "parent_document_id": "string",  # ID of parent document if chunked
        
        # Temporal tracking
        "created_at": "string",
        "updated_at": "string",
        
        # Metadata
        "doc_metadata": "string",  # JSON string
        "group_id": "string",  # For proper tenant isolation
        
        # Technical fields
        "embedding": "array<float>",
        "embedding_model": "string",  # Model used for embedding
        "version": "int"  # Schema version for future migrations
    }
    
    # Columns to request when searching document memory
    DOCUMENT_SEARCH_COLUMNS = [
        "id", "title", "content", "source", "document_type",
        "section", "chunk_index", "chunk_size", "parent_document_id",
        "created_at", "updated_at", "doc_metadata", "group_id",
        "embedding_model", "version"
    ]
    
    @classmethod
    def get_schema(cls, memory_type: str) -> Dict[str, str]:
        """
        Get the schema definition for a specific memory type.
        
        Args:
            memory_type: The type of memory (short_term, long_term, entity, document)
            
        Returns:
            Schema definition dictionary
        """
        schemas = {
            "short_term": cls.SHORT_TERM_SCHEMA,
            "long_term": cls.LONG_TERM_SCHEMA,
            "entity": cls.ENTITY_SCHEMA,
            "document": cls.DOCUMENT_SCHEMA
        }
        return schemas.get(memory_type, {})
    
    @classmethod
    def get_search_columns(cls, memory_type: str) -> List[str]:
        """
        Get the columns to request when searching a specific memory type.
        
        Args:
            memory_type: The type of memory (short_term, long_term, entity, document)
            
        Returns:
            List of column names to request
        """
        columns = {
            "short_term": cls.SHORT_TERM_SEARCH_COLUMNS,
            "long_term": cls.LONG_TERM_SEARCH_COLUMNS,
            "entity": cls.ENTITY_SEARCH_COLUMNS,
            "document": cls.DOCUMENT_SEARCH_COLUMNS
        }
        return columns.get(memory_type, ["id"])
    
    @classmethod
    def get_column_positions(cls, memory_type: str) -> Dict[str, int]:
        """
        Get the position mapping for columns in search results.
        
        Args:
            memory_type: The type of memory
            
        Returns:
            Dictionary mapping column names to their positions in result arrays
        """
        columns = cls.get_search_columns(memory_type)
        return {col: idx for idx, col in enumerate(columns)}
    
    @classmethod
    def parse_search_result(cls, memory_type: str, result: List[Any]) -> Dict[str, Any]:
        """
        Parse a search result array into a dictionary based on the schema.
        
        Args:
            memory_type: The type of memory
            result: List of values from search result
            
        Returns:
            Dictionary with parsed values
        """
        columns = cls.get_search_columns(memory_type)
        parsed = {}
        
        # Map each value to its column name
        for idx, value in enumerate(result):
            if idx < len(columns):
                column_name = columns[idx]
                parsed[column_name] = value
        
        return parsed