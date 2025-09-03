"""
Databricks Vector Search storage implementation for CrewAI memory.

This module provides a custom storage backend that uses Databricks Vector Search
for storing and retrieving short-term memory in CrewAI agents.
"""
import os

# CRITICAL: Set USE_NULLPOOL immediately at import time to prevent asyncpg connection pool issues
# This must be done before any database connections are created
if not os.environ.get("USE_NULLPOOL"):
    os.environ["USE_NULLPOOL"] = "true"

import uuid
import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import hashlib
import base64
import time
import random
from src.schemas.databricks_index_schemas import DatabricksIndexSchemas
from src.repositories.databricks_auth_helper import DatabricksAuthHelper
from src.repositories.databricks_vector_index_repository import DatabricksVectorIndexRepository
from src.core.logger import LoggerManager
from src.utils.databricks_auth import get_databricks_auth_headers, is_databricks_apps_environment
import asyncio

logger = LoggerManager.get_instance().crew
# Get all memory-type specific loggers
memory_logger = LoggerManager.get_instance().databricks_vector_search
short_term_logger = LoggerManager.get_instance().databricks_short_term
long_term_logger = LoggerManager.get_instance().databricks_long_term
entity_logger = LoggerManager.get_instance().databricks_entity


class DatabricksVectorStorage:
    """
    Custom storage implementation using Databricks Vector Search for CrewAI memory.
    
    This class implements the storage interface required by CrewAI for short-term memory,
    using pre-configured Databricks Vector Search indexes.
    """
    
    def __init__(
        self,
        endpoint_name: str,
        index_name: str,
        crew_id: str,
        agent_id: Optional[str] = None,
        memory_type: str = "short_term",
        workspace_url: Optional[str] = None,
        personal_access_token: Optional[str] = None,
        service_principal_client_id: Optional[str] = None,
        service_principal_client_secret: Optional[str] = None,
        embedding_dimension: int = 1024,
        user_token: Optional[str] = None,
        wait_for_index: bool = False,
        max_wait_seconds: int = 300
    ):
        """
        Initialize Databricks Vector Storage.
        
        Args:
            endpoint_name: Name of the Databricks Vector Search endpoint
            index_name: Full name of the index (catalog.schema.table format)
            crew_id: Unique identifier for the crew
            agent_id: Optional identifier for the specific agent
            memory_type: Type of memory (short_term, long_term, entity)
            workspace_url: Databricks workspace URL
            personal_access_token: Optional PAT for authentication
            service_principal_client_id: Optional service principal client ID
            service_principal_client_secret: Optional service principal client secret
            embedding_dimension: Dimension of embeddings (default 1024 for databricks-gte-large-en)
            user_token: Optional user token for OBO authentication
            wait_for_index: Whether to wait for index to be ready
            max_wait_seconds: Maximum time to wait for index
        """
        self.endpoint_name = endpoint_name
        self.index_name = index_name
        self.crew_id = crew_id
        self.agent_id = agent_id or "default_agent"
        self.memory_type = memory_type
        self.embedding_dimension = embedding_dimension
        
        # Initialize memory logger based on type
        if memory_type == "short_term":
            self.memory_logger = short_term_logger
        elif memory_type == "long_term":
            self.memory_logger = long_term_logger
        elif memory_type == "entity":
            self.memory_logger = entity_logger
        else:
            self.memory_logger = memory_logger
            
        self.memory_logger.info(f"Initializing Databricks Vector Storage for {memory_type} memory")
        self.memory_logger.info(f"Endpoint: {endpoint_name}, Index: {index_name}")
        self.memory_logger.info(f"Crew ID: {crew_id}, Agent ID: {agent_id}")
        
        # Store workspace URL and user token for repository
        self.workspace_url = workspace_url
        self.user_token = user_token
        
        # Initialize repository for clean architecture - this handles all operations
        self.repository = DatabricksVectorIndexRepository(workspace_url or os.getenv('DATABRICKS_HOST', ''))
        
        # IMPORTANT: Set USE_NULLPOOL environment variable to prevent asyncpg connection pool issues
        # This is needed because Databricks memory operations run async code in different event loops
        # which can conflict with SQLAlchemy's connection pooling
        if not os.environ.get("USE_NULLPOOL"):
            os.environ["USE_NULLPOOL"] = "true"
            self.memory_logger.info("Set USE_NULLPOOL=true to prevent asyncpg connection pool issues")
        
        # Note: We no longer need direct client access since we use the repository pattern
        # The repository handles all authentication, client creation, and operations
    
    async def save(self, data: Dict[str, Any]) -> None:
        """
        Save memory data to Databricks Vector Search.
        
        Args:
            data: Dictionary containing memory data to save
        """
        try:
            # Get schema for the specific memory type
            schema = DatabricksIndexSchemas.get_schema(self.memory_type)
            
            # If no schema found, raise error
            if not schema:
                raise ValueError(f"Unsupported memory type: {self.memory_type}")
            
            # Extract embedding from data
            embedding = data.get("embedding", None)
            if embedding is None:
                # Generate a random embedding if none provided (for testing)
                embedding = [random.random() for _ in range(self.embedding_dimension)]
            
            # Initialize record with only fields that exist in schema
            record = {}
            
            # Build record based on memory type, only including fields defined in schema
            if self.memory_type == "short_term":
                # Only add fields that exist in SHORT_TERM_SCHEMA
                if "id" in schema:
                    record["id"] = str(uuid.uuid4())
                if "content" in schema:
                    record["content"] = data.get("content", "")
                if "embedding" in schema:
                    record["embedding"] = embedding
                if "query_text" in schema:
                    record["query_text"] = data.get("context", {}).get("query_text", "")
                if "session_id" in schema:
                    record["session_id"] = data.get("context", {}).get("session_id", str(uuid.uuid4()))
                if "interaction_sequence" in schema:
                    record["interaction_sequence"] = data.get("context", {}).get("interaction_sequence", 0)
                if "timestamp" in schema:
                    record["timestamp"] = datetime.utcnow().isoformat()
                if "created_at" in schema:
                    # Use ISO format string for timestamp field
                    record["created_at"] = datetime.utcnow().isoformat()
                if "ttl_hours" in schema:
                    record["ttl_hours"] = data.get("ttl_hours", 24)  # Default 24 hour TTL
                if "crew_id" in schema:
                    record["crew_id"] = self.crew_id
                if "agent_id" in schema:
                    record["agent_id"] = data.get("agent_id", self.agent_id)
                if "group_id" in schema:
                    # Extract group_id from crew_id or use default
                    if self.crew_id and "user_" in self.crew_id:
                        parts = self.crew_id.split("_crew_")
                        record["group_id"] = parts[0] if parts else "default"
                    else:
                        record["group_id"] = data.get("group_id", "default")
                if "metadata" in schema:
                    record["metadata"] = json.dumps(data.get("metadata", {}))
                if "llm_model" in schema:
                    record["llm_model"] = data.get("llm_model", data.get("metadata", {}).get("llm_model", "unknown"))
                if "tools_used" in schema:
                    tools = data.get("tools_used", data.get("metadata", {}).get("tools_used", []))
                    record["tools_used"] = json.dumps(tools) if isinstance(tools, list) else tools
                if "embedding_model" in schema:
                    record["embedding_model"] = data.get("embedding_model", "databricks-gte-large-en")
                if "version" in schema:
                    record["version"] = 1
                    
            elif self.memory_type == "long_term":
                if "id" in schema:
                    record["id"] = str(uuid.uuid4())
                if "content" in schema:
                    record["content"] = data.get("content", "")
                if "embedding" in schema:
                    record["embedding"] = embedding
                if "task_description" in schema:
                    record["task_description"] = data.get("task_description", "")
                if "task_hash" in schema:
                    record["task_hash"] = hashlib.md5(data.get("task_description", "").encode()).hexdigest()
                if "quality" in schema:
                    record["quality"] = data.get("quality", 0.8)
                if "importance" in schema:
                    record["importance"] = data.get("importance", 0.5)
                if "timestamp" in schema:
                    record["timestamp"] = datetime.utcnow().isoformat()
                if "last_accessed" in schema:
                    record["last_accessed"] = datetime.utcnow().isoformat()
                if "crew_id" in schema:
                    record["crew_id"] = self.crew_id
                if "agent_id" in schema:
                    record["agent_id"] = data.get("agent_id", self.agent_id)
                if "group_id" in schema:
                    # Extract group_id from crew_id or use default
                    if self.crew_id and "user_" in self.crew_id:
                        parts = self.crew_id.split("_crew_")
                        record["group_id"] = parts[0] if parts else "default"
                    else:
                        record["group_id"] = data.get("group_id", "default")
                if "metadata" in schema:
                    record["metadata"] = json.dumps(data.get("metadata", {}))
                if "embedding_model" in schema:
                    record["embedding_model"] = data.get("embedding_model", "databricks-gte-large-en")
                if "version" in schema:
                    record["version"] = 1
                if "llm_model" in schema:
                    record["llm_model"] = data.get("llm_model", data.get("metadata", {}).get("llm_model", "unknown"))
                if "tools_used" in schema:
                    tools = data.get("tools_used", data.get("metadata", {}).get("tools_used", []))
                    record["tools_used"] = json.dumps(tools) if isinstance(tools, list) else tools
                    
            elif self.memory_type == "entity":
                # Entity memory - use simplified schema
                if "id" in schema:
                    record["id"] = str(uuid.uuid4())
                if "entity_name" in schema:
                    record["entity_name"] = data.get("entity_name", "")
                if "entity_type" in schema:
                    record["entity_type"] = data.get("entity_type", "unknown")
                if "description" in schema:
                    record["description"] = data.get("description", "")
                if "relationships" in schema:
                    record["relationships"] = json.dumps(data.get("relationships", []))
                if "timestamp" in schema:
                    record["timestamp"] = datetime.utcnow().isoformat()
                if "crew_id" in schema:
                    record["crew_id"] = self.crew_id
                if "agent_id" in schema:
                    # Extract agent_id from agent object or metadata
                    agent_id = data.get("agent_id", self.agent_id)
                    if not agent_id and data.get("agent"):
                        agent = data.get("agent")
                        if hasattr(agent, "role"):
                            agent_id = agent.role
                        elif hasattr(agent, "id"):
                            agent_id = agent.id
                    record["agent_id"] = agent_id or "unknown"
                if "group_id" in schema:
                    # Extract group_id from crew_id (format: user_X_Y_crew_Z)
                    if self.crew_id and "user_" in self.crew_id:
                        # Extract the user part before _crew_
                        parts = self.crew_id.split("_crew_")
                        if parts:
                            record["group_id"] = parts[0]
                    else:
                        record["group_id"] = data.get("group_id", "default")
                if "embedding" in schema:
                    record["embedding"] = embedding
                if "embedding_model" in schema:
                    # Use the configured embedding model
                    record["embedding_model"] = data.get("embedding_model", "databricks-gte-large-en")
                if "llm_model" in schema:
                    record["llm_model"] = data.get("llm_model", data.get("metadata", {}).get("llm_model", "unknown"))
                if "tools_used" in schema:
                    tools = data.get("tools_used", data.get("metadata", {}).get("tools_used", []))
                    record["tools_used"] = json.dumps(tools) if isinstance(tools, list) else tools
                    
            elif self.memory_type == "document":
                # Document memory type for documentation embeddings
                if "id" in schema:
                    record["id"] = str(uuid.uuid4())
                if "title" in schema:
                    record["title"] = data.get("context", {}).get("query_text", "")
                if "content" in schema:
                    record["content"] = data.get("content", "")
                if "source" in schema:
                    record["source"] = data.get("metadata", {}).get("source", "")
                if "document_type" in schema:
                    record["document_type"] = data.get("metadata", {}).get("type", "documentation")
                if "section" in schema:
                    record["section"] = data.get("metadata", {}).get("section", "")
                if "chunk_index" in schema:
                    record["chunk_index"] = data.get("metadata", {}).get("chunk_index", 0)
                if "chunk_size" in schema:
                    record["chunk_size"] = len(data.get("content", ""))
                if "parent_document_id" in schema:
                    record["parent_document_id"] = data.get("metadata", {}).get("parent_document_id", "")
                if "created_at" in schema:
                    record["created_at"] = datetime.utcnow().isoformat()
                if "updated_at" in schema:
                    record["updated_at"] = datetime.utcnow().isoformat()
                if "doc_metadata" in schema:
                    record["doc_metadata"] = json.dumps(data.get("metadata", {}))
                if "group_id" in schema:
                    record["group_id"] = self.crew_id
                if "embedding" in schema:
                    record["embedding"] = embedding
                if "embedding_model" in schema:
                    record["embedding_model"] = data.get("metadata", {}).get("embedding_model", "databricks-gte-large-en")
                if "version" in schema:
                    record["version"] = 1
            else:
                # This should not happen since we check for schema above
                raise ValueError(f"Unsupported memory type: {self.memory_type}")
            
            # Ensure embedding is a list, not numpy array
            if "embedding" in record:
                import numpy as np
                if isinstance(record["embedding"], np.ndarray):
                    record["embedding"] = record["embedding"].tolist()
                elif not isinstance(record["embedding"], list):
                    # Try to convert to list if it's some other iterable
                    try:
                        record["embedding"] = list(record["embedding"])
                    except:
                        self.memory_logger.error(f"Could not convert embedding to list: {type(record['embedding'])}")
            
            # Validate record is not empty
            if not record:
                self.memory_logger.error(f"Record is empty after building for memory type {self.memory_type}")
                self.memory_logger.error(f"Original data keys: {list(data.keys())}")
                self.memory_logger.error(f"Schema fields: {list(schema.keys())}")
                raise ValueError(f"Empty record built for memory type {self.memory_type}")
            
            # Log the record before upsert for debugging
            self.memory_logger.info(f"Upserting {self.memory_type} record with {len(record)} fields: {list(record.keys())}")
            self.memory_logger.debug(f"Record ID: {record.get('id', 'NO_ID')}")
            if "embedding" in record:
                self.memory_logger.debug(f"Embedding type: {type(record['embedding'])}, length: {len(record['embedding']) if hasattr(record['embedding'], '__len__') else 'N/A'}")
            
            # Additional validation - ensure essential fields are present
            if "id" not in record:
                self.memory_logger.warning("Record missing 'id' field, adding one")
                record["id"] = str(uuid.uuid4())
            
            if "embedding" not in record:
                self.memory_logger.error("Record missing 'embedding' field!")
                self.memory_logger.error(f"Available fields: {list(record.keys())}")
                raise ValueError("Record must have an embedding field")
            
            # Check index status before upsert
            try:
                index_info = await self.repository.get_index(
                    self.index_name,
                    self.endpoint_name,
                    self.user_token
                )
                if index_info.success and index_info.index:
                    if not index_info.index.ready:
                        self.memory_logger.warning(f"Index {self.index_name} is not ready yet (state: {index_info.index.state})")
                        # Still try to upsert as it might work
            except Exception as check_error:
                self.memory_logger.warning(f"Could not check index status: {check_error}")
            
            # Upsert to Databricks Vector Search using repository
            result = await self.repository.upsert(
                self.index_name,
                self.endpoint_name,
                [record],
                self.user_token
            )
            
            if not result.get("success"):
                error_msg = result.get("message", "Upsert failed")
                self.memory_logger.error(f"Upsert failed: {error_msg}")
                self.memory_logger.error(f"Record that failed: {json.dumps({k: v for k, v in record.items() if k != 'embedding'}, indent=2)}")
                raise Exception(error_msg)
                
            self.memory_logger.debug(f"Saved {self.memory_type} memory record to index {self.index_name}")
            
        except Exception as e:
            self.memory_logger.error(f"Failed to save to Databricks Vector Search: {e}")
            raise
    
    async def search(
        self, 
        query_embedding: List[float], 
        k: int = 5,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for similar memories in Databricks Vector Search.
        
        Args:
            query_embedding: Query vector for similarity search
            k: Number of results to return
            filters: Optional filters to apply
            
        Returns:
            List of similar memory records
        """
        try:
            # Get search columns for the specific memory type
            search_columns = DatabricksIndexSchemas.get_search_columns(self.memory_type)
            
            # Build filters based on memory type
            search_filters = {}
            if self.crew_id:
                # Different memory types use different fields for filtering
                if self.memory_type == "document":
                    search_filters["group_id"] = self.crew_id
                else:
                    search_filters["crew_id"] = self.crew_id
                    self.memory_logger.info(f"Filtering by crew_id: '{self.crew_id}'")
                
            if filters:
                search_filters.update(filters)
            
            # Log comprehensive search details
            self.memory_logger.info(f"Performing embedding search on index: {self.index_name}")
            self.memory_logger.info(f"Final search filters: {search_filters}")
            self.memory_logger.info(f"Requesting {k} results")
            
            # Perform similarity search using repository
            result = await self.repository.similarity_search(
                self.index_name,
                self.endpoint_name,
                query_embedding,
                search_columns,
                k,
                search_filters,
                self.user_token
            )
            
            if not result.get("success"):
                self.memory_logger.error(f"Search failed: {result.get('message')}")
                return []
                
            results = result.get("results")
            
            # Log search results
            if results and 'result' in results:
                data_array = results['result'].get('data_array', [])
                self.memory_logger.info(f"Embedding search returned {len(data_array)} results")
            else:
                self.memory_logger.info(f"Embedding search returned no results (empty result structure)")
            
            # Process results based on memory type
            processed_results = []
            if results and 'result' in results:
                data_array = results['result'].get('data_array', [])
                
                # Get column positions for mapping
                column_positions = DatabricksIndexSchemas.get_column_positions(self.memory_type)
                
                for row in data_array:
                    # Map row data to column names
                    result_dict = {}
                    for col_name, col_idx in column_positions.items():
                        if col_idx < len(row):
                            # Parse JSON fields if needed
                            if col_name in ['metadata', 'attributes', 'relationships', 'relationship_data']:
                                try:
                                    result_dict[col_name] = json.loads(row[col_idx]) if row[col_idx] else {}
                                except (json.JSONDecodeError, TypeError):
                                    result_dict[col_name] = row[col_idx]
                            else:
                                result_dict[col_name] = row[col_idx]
                    
                    processed_results.append(result_dict)
            
            self.memory_logger.debug(f"Found {len(processed_results)} similar {self.memory_type} memories")
            return processed_results
            
        except Exception as e:
            self.memory_logger.error(f"Failed to search Databricks Vector Search: {e}")
            return []
    
    async def delete(self, memory_id: str) -> bool:
        """
        Delete a memory record from Databricks Vector Search.
        
        Args:
            memory_id: ID of the memory to delete
            
        Returns:
            True if deletion was successful, False otherwise
        """
        try:
            result = await self.repository.delete_records(
                self.index_name,
                self.endpoint_name,
                [memory_id],
                self.user_token
            )
            
            if result.get("success"):
                self.memory_logger.debug(f"Deleted {self.memory_type} memory {memory_id} from index {self.index_name}")
                return True
            else:
                self.memory_logger.error(f"Delete failed: {result.get('message')}")
                return False
        except Exception as e:
            self.memory_logger.error(f"Failed to delete from Databricks Vector Search: {e}")
            return False
    
    async def clear(self) -> bool:
        """
        Clear all memories for this crew from the index.
        
        Returns:
            True if clearing was successful, False otherwise
        """
        try:
            # First, search for all records belonging to this crew
            # Use a dummy embedding for the search
            dummy_embedding = [0.0] * self.embedding_dimension
            
            # Search for up to 10000 records (maximum allowed) using repository
            # Use the correct filter field based on memory type
            filters = {}
            if self.crew_id:
                if self.memory_type == "document":
                    filters["group_id"] = self.crew_id
                else:
                    filters["crew_id"] = self.crew_id
                    
            result = await self.repository.similarity_search(
                self.index_name,
                self.endpoint_name,
                dummy_embedding,
                ["id"],
                10000,
                filters,
                self.user_token
            )
            
            # Extract IDs from results
            ids_to_delete = []
            if result.get("success") and result.get("results"):
                results = result["results"]
                if results and 'result' in results:
                    data_array = results['result'].get('data_array', [])
                    ids_to_delete = [row[0] for row in data_array if row and len(row) > 0]
            
            # Delete all found records using repository
            if ids_to_delete:
                delete_result = await self.repository.delete_records(
                    self.index_name,
                    self.endpoint_name,
                    ids_to_delete,
                    self.user_token
                )
                
                if delete_result.get("success"):
                    self.memory_logger.info(f"Cleared {len(ids_to_delete)} {self.memory_type} memories for crew {self.crew_id}")
                else:
                    self.memory_logger.error(f"Failed to delete records: {delete_result.get('message')}")
                    return False
            else:
                self.memory_logger.info(f"No {self.memory_type} memories found to clear for crew {self.crew_id}")
            
            return True
            
        except Exception as e:
            self.memory_logger.error(f"Failed to clear memories from Databricks Vector Search: {e}")
            return False
    
    async def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the current memory storage.
        
        Returns:
            Dictionary with storage statistics
        """
        try:
            # Get index description using repository
            result = await self.repository.describe_index(
                self.index_name,
                self.endpoint_name,
                self.user_token
            )
            
            # Extract relevant stats
            stats = {
                "index_name": self.index_name,
                "endpoint_name": self.endpoint_name,
                "memory_type": self.memory_type,
                "crew_id": self.crew_id,
                "agent_id": self.agent_id
            }
            
            # Add index stats if available
            if result.get("success") and result.get("description"):
                index_info = result["description"]
                if isinstance(index_info, dict):
                    if 'status' in index_info:
                        status = index_info['status']
                        stats['indexed_row_count'] = status.get('indexed_row_count', 0)
                        stats['ready'] = status.get('ready', False)
                        stats['state'] = status.get('detailed_state', 'unknown')
                    
                    stats['num_rows'] = index_info.get('num_rows', 0)
            
            return stats
            
        except Exception as e:
            self.memory_logger.error(f"Failed to get stats from Databricks Vector Search: {e}")
            return {
                "index_name": self.index_name,
                "endpoint_name": self.endpoint_name,
                "memory_type": self.memory_type,
                "crew_id": self.crew_id,
                "agent_id": self.agent_id,
                "error": str(e)
            }
    
    async def count_documents(self) -> int:
        """
        Count the number of documents in the index for this crew.
        Uses the repository pattern for clean architecture.
        
        Returns:
            Number of documents in the index
        """
        try:
            # Use the correct filter field based on memory type
            filters = None
            if self.crew_id:
                if self.memory_type == "document":
                    filters = {"group_id": self.crew_id}
                else:
                    filters = {"crew_id": self.crew_id}
                    
            count = await self.repository.count_documents(
                self.index_name,
                self.endpoint_name,
                filters,
                self.user_token
            )
            
            self.memory_logger.debug(f"Counted {count} documents for crew {self.crew_id}")
            return count
            
        except Exception as e:
            self.memory_logger.error(f"Failed to count documents: {e}")
            return 0