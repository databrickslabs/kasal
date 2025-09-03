"""
Databricks index service for managing Vector Search indexes.

This module provides business logic for managing Databricks Vector Search indexes,
delegating actual API operations to the repository layer.
"""
from typing import Dict, Any, Optional, List
import random
import json
import numpy as np

from src.schemas.memory_backend import DatabricksMemoryConfig
from src.schemas.databricks_index_schemas import DatabricksIndexSchemas
from src.schemas.databricks_vector_index import IndexCreate, IndexResponse, IndexListResponse, IndexType
from src.repositories.databricks_vector_index_repository import DatabricksVectorIndexRepository
from src.repositories.databricks_vector_endpoint_repository import DatabricksVectorEndpointRepository
from src.core.logger import LoggerManager

# Use the system logger for general operations
logger = LoggerManager.get_instance().system
# Use the databricks_vector_search logger for vector search specific operations
databricks_logger = LoggerManager.get_instance().databricks_vector_search
# Memory-type specific loggers
short_term_logger = LoggerManager.get_instance().databricks_short_term
long_term_logger = LoggerManager.get_instance().databricks_long_term
entity_logger = LoggerManager.get_instance().databricks_entity


class DatabricksIndexService:
    """Service for managing Databricks Vector Search indexes."""
    
    def __init__(self, workspace_url: Optional[str] = None):
        """
        Initialize the service.
        
        Args:
            workspace_url: Optional workspace URL, will use from config if not provided
        """
        self.workspace_url = workspace_url
        self._index_repo: Optional[DatabricksVectorIndexRepository] = None
        self._endpoint_repo = None  # Will be lazy loaded when needed
        self.memory_loggers = {
            "short_term": short_term_logger,
            "long_term": long_term_logger,
            "entity": entity_logger
        }
    
    def _get_index_repository(self, workspace_url: str) -> DatabricksVectorIndexRepository:
        """
        Get or create index repository.
        
        Args:
            workspace_url: Databricks workspace URL
            
        Returns:
            DatabricksVectorIndexRepository instance
        """
        if not self._index_repo or self._index_repo.workspace_url != workspace_url:
            self._index_repo = DatabricksVectorIndexRepository(workspace_url)
        return self._index_repo
    
    def _get_endpoint_repository(self, workspace_url: str) -> DatabricksVectorEndpointRepository:
        """
        Get or create endpoint repository.
        
        Args:
            workspace_url: Databricks workspace URL
            
        Returns:
            DatabricksVectorEndpointRepository instance
        """
        if not self._endpoint_repo or self._endpoint_repo.workspace_url != workspace_url:
            self._endpoint_repo = DatabricksVectorEndpointRepository(workspace_url)
        return self._endpoint_repo
    
    async def create_databricks_index(
        self,
        config: DatabricksMemoryConfig,
        index_type: str,  # "short_term", "long_term", or "entity"
        catalog: str,
        schema: str,
        table_name: str,
        primary_key: str = "id",
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a Databricks Vector Search index.
        
        Args:
            config: Databricks configuration
            index_type: Type of index to create
            catalog: Catalog name
            schema: Schema name
            table_name: Table name for the index
            primary_key: Primary key column (default: "id")
            user_token: Optional user access token for OBO authentication
            
        Returns:
            Creation result
        """
        try:
            # Get repository
            repo = self._get_index_repository(config.workspace_url)
            
            # Get the appropriate logger for this memory type
            memory_logger = self.memory_loggers.get(index_type, databricks_logger)
            
            # Construct index name
            index_name = f"{catalog}.{schema}.{table_name}"
            memory_logger.info(f"Creating {index_type} index: {index_name}")
            
            # Define index configuration based on type
            # Default to 1024 for databricks-gte-large-en model
            embedding_dimension = config.embedding_dimension or 1024
            memory_logger.info(f"Using embedding dimension {embedding_dimension} for {index_type} index")
            
            # Determine which endpoint to use based on index type
            # For document embeddings, use storage optimized endpoint if available
            use_document_endpoint = (index_type == "document" and config.document_endpoint_name)
            target_endpoint = config.document_endpoint_name if use_document_endpoint else config.endpoint_name
            
            # Get schema from centralized definition
            schema_def = DatabricksIndexSchemas.get_schema(index_type)
            if not schema_def:
                raise ValueError(f"Unknown index type: {index_type}")
            
            # Create index request using Pydantic schema
            index_request = IndexCreate(
                name=index_name,
                endpoint_name=target_endpoint,
                primary_key=primary_key,
                embedding_dimension=embedding_dimension,
                embedding_vector_column="embedding",
                schema=schema_def
            )
            
            # Use repository to create the index
            response = await repo.create_index(index_request, user_token)
            
            if response.success:
                # Update the config to include the new index
                if index_type == "short_term":
                    config.short_term_index = index_name
                elif index_type == "long_term":
                    config.long_term_index = index_name
                elif index_type == "entity":
                    config.entity_index = index_name
                elif index_type == "document":
                    config.document_index = index_name
                
                memory_logger.info(f"Successfully created {index_type} index: {index_name}")
                
                return {
                    "success": True,
                    "message": response.message,
                    "details": {
                        "index_name": index_name,
                        "index_type": index_type,
                        "embedding_dimension": embedding_dimension
                    }
                }
            else:
                if "already exists" in str(response.error).lower():
                    memory_logger.warning(f"Index {index_name} already exists for {index_type}")
                    return {
                        "success": False,
                        "message": f"Index {index_name} already exists",
                        "details": {
                            "index_name": index_name,
                            "error": "Index already exists"
                        }
                    }
                else:
                    memory_logger.error(f"Failed to create {index_type} index: {response.error}")
                    return {
                        "success": False,
                        "message": response.message,
                        "details": {
                            "error": response.error,
                            "index_type": index_type
                        }
                    }
                    
        except Exception as e:
            memory_logger.error(f"Failed to create {index_type} index: {str(e)}")
            return {
                "success": False,
                "message": f"Failed to create index: {str(e)}",
                "details": {
                    "error": str(e),
                    "index_type": index_type
                }
            }
    
    async def get_databricks_indexes(
        self,
        config: DatabricksMemoryConfig,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get available Databricks Vector Search indexes for an endpoint.
        
        Args:
            config: Databricks configuration
            user_token: Optional user access token for OBO authentication
            
        Returns:
            List of indexes
        """
        try:
            # Get repository
            repo = self._get_index_repository(config.workspace_url)
            
            # List indexes using repository
            response = await repo.list_indexes(config.endpoint_name, user_token)
            
            if response.success:
                # Format the response
                formatted_indexes = []
                for index in response.indexes:
                    formatted_indexes.append({
                        "name": index.name,
                        "status": index.state if index.state else "UNKNOWN",
                        "dimension": index.embedding_dimension,
                        "primary_key": index.primary_key,
                        "doc_count": index.row_count
                    })
                
                return {
                    "success": True,
                    "indexes": formatted_indexes,
                    "endpoint_name": config.endpoint_name
                }
            else:
                return {
                    "success": False,
                    "message": response.message,
                    "indexes": []
                }
                
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to get indexes: {str(e)}",
                "indexes": []
            }
    
    async def delete_databricks_index(
        self,
        workspace_url: str,
        index_name: str,
        endpoint_name: str,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Delete a Databricks Vector Search index.
        
        Args:
            workspace_url: Databricks workspace URL
            index_name: Full index name (catalog.schema.table)
            endpoint_name: Endpoint name that hosts the index
            user_token: Optional user access token for OBO authentication
            
        Returns:
            Deletion result
        """
        try:
            # Get repository
            repo = self._get_index_repository(workspace_url)
            
            # Delete index using repository
            response = await repo.delete_index(index_name, endpoint_name, user_token)
            
            return {
                "success": response.success,
                "message": response.message
            }
                    
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to delete index: {str(e)}"
            }
    
    async def delete_databricks_endpoint(
        self,
        workspace_url: str,
        endpoint_name: str,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Delete a Databricks Vector Search endpoint.
        
        Args:
            workspace_url: Databricks workspace URL
            endpoint_name: Endpoint name to delete
            user_token: Optional user access token for OBO authentication
            
        Returns:
            Deletion result
        """
        try:
            # Get repository
            repo = self._get_endpoint_repository(workspace_url)
            
            # Delete endpoint using repository
            response = await repo.delete_endpoint(endpoint_name, user_token)
            
            return {
                "success": response.success,
                "message": response.message
            }
                    
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to delete endpoint: {str(e)}"
            }
    
    async def wait_for_index_ready(
        self,
        workspace_url: str,
        index_name: str,
        endpoint_name: str,
        max_wait_seconds: int = 300,
        check_interval_seconds: int = 10,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Wait for a Databricks Vector Search index to be ready with retries and delays.
        
        Args:
            workspace_url: Databricks workspace URL
            index_name: Full index name (catalog.schema.table)
            endpoint_name: Endpoint name that hosts the index
            max_wait_seconds: Maximum time to wait for index readiness (default: 300 seconds)
            check_interval_seconds: Seconds to wait between readiness checks (default: 10 seconds)
            user_token: Optional user access token for OBO authentication
            
        Returns:
            Dictionary with readiness status and index information
        """
        try:
            import asyncio
            
            # Get repository
            repo = self._get_index_repository(workspace_url)
            
            logger.info(f"Waiting for index {index_name} to be ready (max {max_wait_seconds}s, check every {check_interval_seconds}s)")
            
            start_time = asyncio.get_event_loop().time()
            attempt = 0
            
            while True:
                attempt += 1
                elapsed_time = asyncio.get_event_loop().time() - start_time
                
                # Check if we've exceeded the maximum wait time
                if elapsed_time >= max_wait_seconds:
                    logger.warning(f"Index {index_name} did not become ready within {max_wait_seconds} seconds")
                    return {
                        "success": False,
                        "ready": False,
                        "message": f"Index not ready after {max_wait_seconds} seconds",
                        "elapsed_time": elapsed_time,
                        "attempts": attempt - 1
                    }
                
                logger.info(f"Attempt {attempt}: Checking index {index_name} readiness...")
                
                try:
                    # Get index info using repository
                    response = await repo.get_index(index_name, endpoint_name, user_token)
                    
                    if response.success and response.index:
                        index_info = response.index
                        state = index_info.state if index_info.state else "UNKNOWN"
                        ready = index_info.ready
                        
                        logger.info(f"Index {index_name} state: {state}, ready: {ready}")
                        
                        # Also try describe_index method for additional verification
                        try:
                            describe_response = await repo.describe_index(index_name, endpoint_name, user_token)
                            if describe_response.get("success"):
                                description = describe_response.get("description", {})
                                status = description.get("status", {})
                                describe_state = status.get("state", "UNKNOWN")
                                describe_ready = status.get("ready", False)
                                
                                logger.info(f"Describe method - state: {describe_state}, ready: {describe_ready}")
                                
                                # Use describe results if they differ and seem more reliable
                                if describe_ready and not ready:
                                    logger.info("Describe method indicates ready=True, using that value")
                                    ready = True
                                    state = describe_state
                        except Exception as e:
                            logger.debug(f"Describe method failed, using get_index results: {e}")
                        
                        if ready:
                            logger.info(f"✅ Index {index_name} is ready after {elapsed_time:.1f}s ({attempt} attempts)")
                            return {
                                "success": True,
                                "ready": True,
                                "index_name": index_name,
                                "endpoint_name": endpoint_name,
                                "state": state,
                                "elapsed_time": elapsed_time,
                                "attempts": attempt
                            }
                        else:
                            logger.info(f"Index {index_name} not ready yet (state: {state})")
                    else:
                        logger.warning(f"Failed to get index info: {response.message}")
                        
                except Exception as e:
                    logger.warning(f"Error checking index readiness (attempt {attempt}): {e}")
                
                # Wait before next check (unless we're about to exceed max time)
                remaining_time = max_wait_seconds - elapsed_time
                if remaining_time > check_interval_seconds:
                    logger.info(f"Waiting {check_interval_seconds}s before next check...")
                    await asyncio.sleep(check_interval_seconds)
                else:
                    # If less than check_interval_seconds remaining, wait the remaining time
                    if remaining_time > 0:
                        logger.info(f"Waiting {remaining_time:.1f}s (final check)...")
                        await asyncio.sleep(remaining_time)
                    break
                    
        except Exception as e:
            logger.error(f"Error waiting for index readiness: {e}")
            return {
                "success": False,
                "ready": False,
                "message": f"Error checking index readiness: {str(e)}",
                "error": str(e)
            }
    
    async def get_index_info(
        self,
        workspace_url: str,
        index_name: str,
        endpoint_name: str,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get information about a Databricks Vector Search index including document count.
        
        Args:
            workspace_url: Databricks workspace URL
            index_name: Full index name (catalog.schema.table)
            endpoint_name: Endpoint name that hosts the index
            user_token: Optional user access token for OBO authentication
            
        Returns:
            Index information including document count
        """
        try:
            # Get repository
            repo = self._get_index_repository(workspace_url)
            
            # Get index info using repository
            response = await repo.get_index(index_name, endpoint_name, user_token)
            
            if response.success and response.index:
                index_info = response.index
                
                # Extract information from the response
                doc_count = index_info.row_count or index_info.indexed_row_count or 0
                index_type = "Direct Access" if index_info.index_type == IndexType.DIRECT_ACCESS else "Delta Sync"
                
                return {
                    "success": True,
                    "index_name": index_name,
                    "endpoint_name": endpoint_name,
                    "doc_count": doc_count,
                    "index_type": index_type,
                    "dimension": index_info.embedding_dimension or 0,
                    "primary_key": index_info.primary_key or "id",
                    "last_sync_time": None,  # Direct Access indexes don't have sync time
                    "state": str(index_info.state) if index_info.state else "UNKNOWN",
                    "ready": index_info.ready
                }
            else:
                return {
                    "success": False,
                    "index_name": index_name,
                    "message": response.message or "Failed to get index info",
                    "doc_count": 0
                }
                
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to get index info: {str(e)}",
                "doc_count": 0
            }
    
    async def empty_index(
        self,
        workspace_url: str,
        index_name: str,
        endpoint_name: str,
        index_type: str,  # "short_term", "long_term", "entity", or "document"
        embedding_dimension: int,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Empty a Databricks Vector Search index by deleting all vectors without dropping the index.
        
        Args:
            workspace_url: Databricks workspace URL
            index_name: Full index name (catalog.schema.table)
            endpoint_name: Endpoint name that hosts the index
            index_type: Type of index to empty
            embedding_dimension: Dimension of the index
            user_token: Optional user access token for OBO authentication
            
        Returns:
            Result of the operation
        """
        try:
            # Get repository
            repo = self._get_index_repository(workspace_url)
            
            # Use repository to empty the index
            result = await repo.empty_index(index_name, endpoint_name, embedding_dimension, user_token)
            
            if result.get("success"):
                logger.info(f"Successfully emptied {index_type} index {index_name}")
            else:
                logger.error(f"Failed to empty index {index_name}: {result.get('message')}")
            
            return result
                    
        except Exception as e:
            logger.error(f"Failed to empty index {index_name}: {str(e)}")
            return {
                "success": False,
                "message": f"Failed to empty index: {str(e)}"
            }
    
    async def get_index_documents(
        self,
        workspace_url: str,
        endpoint_name: str,
        index_name: str,
        index_type: Optional[str] = None,
        embedding_dimension: int = 1024,
        limit: int = 30,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get documents from a Databricks Vector Search index.
        
        Args:
            workspace_url: Databricks workspace URL
            endpoint_name: Vector Search endpoint name
            index_name: Full index name (catalog.schema.index)
            limit: Maximum number of documents to return
            user_token: Optional user access token
            
        Returns:
            Dictionary containing documents and metadata
        """
        try:
            # Get repository
            repo = self._get_index_repository(workspace_url)
            
            # Determine the memory type from the index type parameter or index name
            if index_type:
                memory_type = index_type
            else:
                index_name_lower = index_name.lower()
                if 'short_term' in index_name_lower:
                    memory_type = 'short_term'
                elif 'long_term' in index_name_lower:
                    memory_type = 'long_term'
                elif 'entity' in index_name_lower:
                    memory_type = 'entity'
                elif 'document' in index_name_lower or 'doc' in index_name_lower:
                    memory_type = 'document'
                else:
                    # Default to short_term schema
                    memory_type = 'short_term'
            
            # Get the appropriate columns for this memory type
            columns = DatabricksIndexSchemas.get_search_columns(memory_type)
            
            # Query for documents - use a random vector to get any documents
            try:
                # Create a random query vector - we're not doing similarity search
                # We just want to retrieve documents
                
                # Use the provided embedding dimension
                databricks_logger.info(f"Using embedding dimension {embedding_dimension} for index {index_name}")
                
                # Create a random vector to ensure we get some results
                query_vector = np.random.randn(embedding_dimension).tolist()
                
                logger.info(f"Querying index {index_name} with {len(columns)} columns")
                logger.info(f"Requested columns: {columns[:5]}...")  # Log first 5 columns
                
                # Use repository to perform similarity search
                search_response = await repo.similarity_search(
                    index_name=index_name,
                    endpoint_name=endpoint_name,
                    query_vector=query_vector,
                    columns=columns,
                    num_results=limit,
                    user_token=user_token
                )
                
                if not search_response.get("success"):
                    return {
                        "success": False,
                        "message": search_response.get("message", "Search failed"),
                        "documents": []
                    }
                
                results = search_response.get("results", {})
                
                logger.info(f"Search results type: {type(results)}")
                
                # Format the results based on memory type
                documents = []
                
                # Handle different result formats from Databricks Vector Search
                if results:
                    data_array = []
                    
                    # Check different possible result structures
                    if isinstance(results, dict):
                        # Most common: results['result']['data_array']
                        if 'result' in results and isinstance(results['result'], dict):
                            data_array = results['result'].get('data_array', [])
                            logger.info(f"Found {len(data_array)} documents in results['result']['data_array']")
                        # Alternative: results['data_array']
                        elif 'data_array' in results:
                            data_array = results.get('data_array', [])
                            logger.info(f"Found {len(data_array)} documents in results['data_array']")
                        # Alternative: results['data']
                        elif 'data' in results:
                            data_array = results.get('data', [])
                            logger.info(f"Found {len(data_array)} documents in results['data']")
                    elif isinstance(results, list):
                        data_array = results
                        logger.info(f"Results is a list with {len(data_array)} items")
                    
                    # Process the data array
                    for idx, item in enumerate(data_array):
                        # Check if item is a list (row format) or dict
                        if isinstance(item, list):
                            # Convert list to dict using column positions
                            column_positions = DatabricksIndexSchemas.get_column_positions(memory_type)
                            item_dict = {}
                            for col_name, col_idx in column_positions.items():
                                if col_idx < len(item):
                                    item_dict[col_name] = item[col_idx]
                            item = item_dict
                        elif not isinstance(item, dict):
                            logger.warning(f"Unexpected item type at index {idx}: {type(item)}")
                            continue
                        # Parse based on memory type
                        if memory_type == 'short_term':
                            doc = {
                                "id": item.get("id", ""),
                                "text": item.get("content", ""),
                                "metadata": {
                                    "query_text": item.get("query_text", ""),
                                    "session_id": item.get("session_id", ""),
                                    "interaction_sequence": item.get("interaction_sequence", ""),
                                    "timestamp": item.get("timestamp", ""),
                                    "crew_id": item.get("crew_id", ""),
                                    "agent_id": item.get("agent_id", ""),
                                    "metadata": item.get("metadata", "")
                                }
                            }
                        elif memory_type == 'long_term':
                            doc = {
                                "id": item.get("id", ""),
                                "text": item.get("content", ""),
                                "metadata": {
                                    "task_description": item.get("task_description", ""),
                                    "task_hash": item.get("task_hash", ""),
                                    "quality": item.get("quality", ""),
                                    "importance": item.get("importance", ""),
                                    "timestamp": item.get("timestamp", ""),
                                    "crew_id": item.get("crew_id", ""),
                                    "agent_id": item.get("agent_id", ""),
                                    "metadata": item.get("metadata", "")
                                }
                            }
                        elif memory_type == 'entity':
                            doc = {
                                "id": item.get("id", ""),
                                "text": f"{item.get('entity_name', '')}: {item.get('description', '')}",
                                "metadata": {
                                    "entity_type": item.get("entity_type", ""),
                                    "entity_name": item.get("entity_name", ""),
                                    "relationships": item.get("relationships", ""),
                                    "attributes": item.get("attributes", ""),
                                    "confidence_score": item.get("confidence_score", ""),
                                    "timestamp": item.get("timestamp", ""),
                                    "crew_id": item.get("crew_id", ""),
                                    "agent_id": item.get("agent_id", "")
                                }
                            }
                        elif memory_type == 'document':
                            doc = {
                                "id": item.get("id", ""),
                                "text": item.get("content", ""),
                                "metadata": {
                                    "title": item.get("title", ""),
                                    "source": item.get("source", ""),
                                    "document_type": item.get("document_type", ""),
                                    "section": item.get("section", ""),
                                    "chunk_index": item.get("chunk_index", ""),
                                    "created_at": item.get("created_at", ""),
                                    "doc_metadata": item.get("doc_metadata", "")
                                }
                            }
                        else:
                            # Fallback format
                            doc = {
                                "id": item.get("id", ""),
                                "text": item.get("content", item.get("description", "")),
                                "metadata": item
                            }
                        
                        documents.append(doc)
                
                logger.info(f"Total documents parsed: {len(documents)}")
                
                return {
                    "success": True,
                    "documents": documents,
                    "count": len(documents),
                    "index_name": index_name,
                    "endpoint_name": endpoint_name,
                    "memory_type": memory_type
                }
                
            except Exception as e:
                logger.error(f"Failed to query documents from index: {e}")
                return {
                    "success": False,
                    "message": f"Failed to retrieve documents: {str(e)}",
                    "documents": [],
                    "error_details": str(e)
                }
                    
        except Exception as e:
            logger.error(f"Error in get_index_documents: {e}")
            return {
                "success": False,
                "message": f"An error occurred: {str(e)}",
                "documents": []
            }
    
    async def query_entity_data(
        self,
        workspace_url: str,
        endpoint_name: str,
        index_name: str,
        embedding_dimension: int = 1024,
        limit: int = 100,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Query entity data from a Databricks Vector Search index.
        
        This method retrieves all entities and attempts to identify relationships
        from the entity memory index for visualization purposes.
        
        Args:
            workspace_url: Databricks workspace URL
            endpoint_name: Vector Search endpoint name
            index_name: Full index name (catalog.schema.index)
            embedding_dimension: Dimension of embedding vectors
            limit: Maximum number of entities to return
            user_token: Optional user access token for OBO authentication
            
        Returns:
            Dictionary containing entities and relationships
        """
        try:
            # Get repository
            repo = self._get_index_repository(workspace_url)
            
            entity_logger.info(f"Using repository for query_entity_data operation")
            entity_logger.info(f"Querying index: {index_name} on endpoint: {endpoint_name}")
            
            # Query for entities using a random vector (to get all data)
            # In entity memory, each row typically contains entity information
            random_vector = [random.random() for _ in range(embedding_dimension)]
            
            # Use the correct columns for entity memory from the schema
            entity_columns = DatabricksIndexSchemas.ENTITY_SEARCH_COLUMNS
            entity_logger.info(f"Requesting columns: {entity_columns}")
            
            # Use repository to perform similarity search
            search_response = await repo.similarity_search(
                index_name=index_name,
                endpoint_name=endpoint_name,
                query_vector=random_vector,
                columns=entity_columns,
                num_results=limit,
                filters={},
                user_token=user_token
            )
            
            if not search_response.get("success"):
                entity_logger.error(f"Search failed: {search_response.get('message')}")
                return {
                    "success": False,
                    "entities": [],
                    "relationships": [],
                    "message": search_response.get("message", "Search failed"),
                    "error": search_response.get("error")
                }
            
            search_results = search_response.get("results", {})
            
            entity_logger.info(f"Search completed, processing results...")
            entity_logger.info(f"Raw search_results type: {type(search_results)}")
            entity_logger.info(f"Raw search_results keys: {list(search_results.keys()) if isinstance(search_results, dict) else 'Not a dict'}")
            if isinstance(search_results, dict) and 'result' in search_results:
                entity_logger.info(f"Result keys: {list(search_results['result'].keys()) if isinstance(search_results['result'], dict) else 'Not a dict'}")
            
            entities = []
            relationships = []
            entity_map = {}  # Map entity names to IDs for relationship creation
            
            # Handle different response formats from Databricks
            if search_results:
                # Try to get data from different possible structures
                if isinstance(search_results, dict):
                    if 'result' in search_results:
                        data_array = search_results['result'].get('data_array', [])
                        column_names = search_results['result'].get('column_names', [])
                    elif 'data_array' in search_results:
                        # Direct data_array format
                        data_array = search_results.get('data_array', [])
                        column_names = search_results.get('column_names', [])
                    else:
                        # Try manifest format
                        manifest = search_results.get('manifest', {})
                        column_names = [col['name'] for col in manifest.get('columns', [])]
                        result = search_results.get('result', {})
                        data_array = result.get('data_array', [])
                elif isinstance(search_results, list):
                    # Direct list of results
                    data_array = search_results
                    column_names = []
                else:
                    data_array = []
                    column_names = []
                
                # If column_names is empty but we have data, use the entity schema columns
                # This happens when Databricks doesn't return column names in the response
                if not column_names and data_array:
                    # Use the entity schema columns we requested
                    column_names = entity_columns
                    entity_logger.info(f"No column names in response, using schema columns")
                
                # Log the column names we're receiving
                entity_logger.info(f"Column names from query: {column_names}")
                entity_logger.info(f"Number of rows returned: {len(data_array)}")
                if data_array and len(data_array) > 0:
                    entity_logger.info(f"First row sample (first 5 values): {data_array[0][:5] if len(data_array[0]) > 5 else data_array[0]}")
                
                # Process each row
                for idx, row in enumerate(data_array):
                    if not row:
                        continue
                    
                    # Create a dictionary from column names and values
                    entity_data = {}
                    for col_idx, col_name in enumerate(column_names):
                        if col_idx < len(row):
                            entity_data[col_name] = row[col_idx]
                    
                    # Extract entity information using the proper schema columns
                    # Generate a unique ID if not present
                    entity_id = entity_data.get('id')
                    if not entity_id or entity_id == 'null' or entity_id == 'None':
                        # Use entity_name as ID if available, otherwise generate one
                        if entity_data.get('entity_name') and entity_data['entity_name'] not in ['null', 'None', '']:
                            entity_id = f"entity_{entity_data['entity_name'].replace(' ', '_').lower()}"
                        else:
                            entity_id = f"entity_{idx}"
                    entity_id = str(entity_id)
                    
                    # Get entity name, handling null/None values
                    entity_name = entity_data.get('entity_name', '')
                    if entity_name in ['null', 'None', None]:
                        entity_name = ''
                    entity_name = str(entity_name) if entity_name else ''
                    
                    # Get entity type, handling null/None values
                    entity_type = entity_data.get('entity_type', 'unknown')
                    if entity_type in ['null', 'None', None, '']:
                        entity_type = 'unknown'
                    entity_type = str(entity_type) if entity_type else 'unknown'
                    
                    # Log first few entities for debugging
                    if idx < 5:
                        entity_logger.info(f" Entity {idx}: id={entity_id}, name='{entity_name}', type='{entity_type}'")
                        entity_logger.info(f" Raw data keys: {list(entity_data.keys())}")
                        entity_logger.info(f" Raw entity_name value: {repr(entity_data.get('entity_name'))}")
                        entity_logger.info(f" Raw entity_type value: {repr(entity_data.get('entity_type'))}")
                    
                    # Parse attributes from JSON string
                    attributes = {}
                    if 'attributes' in entity_data and entity_data['attributes']:
                        try:
                            import json
                            parsed_attrs = json.loads(entity_data['attributes'])
                            if isinstance(parsed_attrs, dict):
                                attributes = parsed_attrs
                            else:
                                attributes = {'raw': entity_data['attributes']}
                        except (json.JSONDecodeError, TypeError):
                            attributes = {'raw': entity_data['attributes']}
                    
                    # Get description - try from entity_data first, then from attributes
                    description = entity_data.get('description', '')
                    if not description and attributes and 'description' in attributes:
                        description = attributes['description']
                    
                    # Parse relationships from JSON string
                    entity_relationships = []
                    if 'relationships' in entity_data and entity_data['relationships']:
                        try:
                            import json
                            parsed_relationships = json.loads(entity_data['relationships'])
                            # Ensure it's always a list
                            if isinstance(parsed_relationships, list):
                                entity_relationships = parsed_relationships
                            elif isinstance(parsed_relationships, str):
                                # If it's a string, treat it as a single relationship
                                entity_relationships = [parsed_relationships]
                            else:
                                entity_relationships = []
                        except (json.JSONDecodeError, TypeError):
                            entity_relationships = []
                    
                    # Parse relationship_data for more detailed relationships
                    if 'relationship_data' in entity_data and entity_data['relationship_data']:
                        try:
                            import json
                            rel_data = json.loads(entity_data['relationship_data'])
                            if isinstance(rel_data, list):
                                entity_relationships.extend(rel_data)
                            elif isinstance(rel_data, dict):
                                entity_relationships.append(rel_data)
                        except (json.JSONDecodeError, TypeError):
                            pass
                    
                    # Add metadata to attributes
                    if 'confidence_score' in entity_data:
                        attributes['confidence'] = entity_data['confidence_score']
                    if 'source_context' in entity_data:
                        attributes['source'] = entity_data['source_context']
                    if 'timestamp' in entity_data:
                        attributes['last_seen'] = entity_data['timestamp']
                    
                    # Use description as name if entity_name is empty
                    if not entity_name and description:
                        entity_name = description[:50]  # First 50 chars of description
                    elif not entity_name:
                        entity_name = f"Entity {idx}"
                    
                    # Add to entities list
                    entities.append({
                        "id": entity_id,
                        "name": entity_name or f"Entity {idx}",
                        "type": entity_type,
                        "attributes": attributes
                    })
                    
                    # Store in map for relationship detection
                    entity_map[entity_name.lower()] = entity_id
                    
                    # Process parsed relationships for this entity
                    if entity_relationships:
                        # Log first few relationships for debugging
                        if idx < 3 and entity_relationships:
                            entity_logger.info(f" Entity {idx} relationships: {entity_relationships[:2]}")
                        
                        for rel in entity_relationships:
                            if isinstance(rel, dict):
                                # Handle relationship dict format
                                target = rel.get('target', rel.get('entity', ''))
                                rel_type = rel.get('type', rel.get('relationship', 'related_to'))
                                
                                if target:
                                    # Create relationship (will link by name later)
                                    # Extract strength, ensuring it's a float
                                    strength = rel.get('strength', 0.8)
                                    if isinstance(strength, str):
                                        try:
                                            strength = float(strength)
                                        except ValueError:
                                            strength = 0.8
                                    
                                    relationships.append({
                                        "source": entity_id,
                                        "target": target,  # This might be a name, will resolve later
                                        "type": rel_type,
                                        "label": rel.get('description', rel_type.replace('_', ' ').title()),
                                        "strength": strength,
                                        "pending_resolution": True  # Mark for later resolution
                                    })
                            elif isinstance(rel, str):
                                # Handle simple string format (just entity name)
                                relationships.append({
                                    "source": entity_id,
                                    "target": rel,
                                    "type": "related_to",
                                    "label": "Related To",
                                    "strength": 0.7,
                                    "pending_resolution": True
                                })
                
                # After processing all entities, resolve relationship targets
                # Also create nodes for targets that don't exist as primary entities
                targets_to_create = set()
                for rel in relationships:
                    if rel.get('pending_resolution'):
                        # Try to find the target entity by name
                        target_name = rel['target']
                        target_name_lower = target_name.lower()
                        
                        if target_name_lower in entity_map:
                            # Target exists, use its ID
                            rel['target'] = entity_map[target_name_lower]
                            del rel['pending_resolution']
                        else:
                            # Target doesn't exist as an entity, create it
                            targets_to_create.add(target_name)
                            # Use the target name as the ID for now
                            target_id = f"target_{target_name.replace(' ', '_').lower()}"
                            rel['target'] = target_id
                            del rel['pending_resolution']
                
                entity_logger.info(f"Creating {len(targets_to_create)} additional nodes for relationship targets")
                entity_logger.info(f"Primary entities: {len(entities)}, Relationships found: {len(relationships)}")
                
                # Create nodes for relationship targets that don't exist as primary entities
                for target_name in targets_to_create:
                    target_id = f"target_{target_name.replace(' ', '_').lower()}"
                    entity_map[target_name.lower()] = target_id
                    
                    # Determine entity type from name (heuristic)
                    entity_type = 'unknown'
                    target_lower = target_name.lower()
                    if any(word in target_lower for word in ['researchers', 'team', 'group', 'people', 'users', 'developers', 'scientist']):
                        entity_type = 'person'
                    elif any(word in target_lower for word in ['system', 'api', 'database', 'service', 'cuda', 'gpu', 'server']):
                        entity_type = 'system'
                    elif any(word in target_lower for word in ['data', 'dataset', 'model', 'algorithm', 'big data', 'ai']):
                        entity_type = 'concept'
                    elif any(word in target_lower for word in ['company', 'organization', 'department']):
                        entity_type = 'organization'
                    elif any(word in target_lower for word in ['event', 'meetup', 'gathering', 'summit']):
                        entity_type = 'event'
                    elif any(word in target_lower for word in ['conference', 'symposium', 'workshop']):
                        entity_type = 'conference'
                    elif any(word in target_lower for word in ['tool', 'software', 'application']):
                        entity_type = 'tool'
                    elif any(word in target_lower for word in ['community', 'forum', 'group']):
                        entity_type = 'organization'
                    
                    entities.append({
                        "id": target_id,
                        "name": target_name,
                        "type": entity_type,
                        "attributes": {
                            "inferred": True,
                            "source": "relationship_target"
                        }
                    })
                
                # Log summary before returning
                entity_logger.info(f" Query entity data summary: {len(entities)} entities, {len(relationships)} relationships")
                if entities:
                    entity_logger.info(f" First 3 entities:")
                    for i in range(min(3, len(entities))):
                        entity_logger.info(f"   Entity {i}: name='{entities[i].get('name')}', type='{entities[i].get('type')}', id='{entities[i].get('id')}'")
                if relationships:
                    entity_logger.info(f" First 3 relationships:")
                    for i in range(min(3, len(relationships))):
                        entity_logger.info(f"   Relationship {i}: {relationships[i]}")
                
                # Return the structured data
                return {
                    "success": True,
                    "entities": entities,
                    "relationships": relationships,
                    "stats": {
                        "total_entities": len(entities),
                        "total_relationships": len(relationships),
                        "entity_types": {
                            entity_type: sum(1 for e in entities if e["type"] == entity_type)
                            for entity_type in set(e["type"] for e in entities)
                        },
                        "index_name": index_name,
                        "endpoint_name": endpoint_name
                    },
                    "message": f"Successfully retrieved {len(entities)} entities from {index_name}"
                }
                
        except Exception as e:
            if "not found" in str(e).lower():
                entity_logger.error(f" Index not found: {index_name} on endpoint {endpoint_name}")
                return {
                    "success": False,
                    "entities": [],
                    "relationships": [],
                    "message": f"Index {index_name} not found on endpoint {endpoint_name}",
                    "error": str(e)
                }
            else:
                entity_logger.error(f" Error querying index: {e}")
                entity_logger.error(f" Error details: {type(e).__name__}: {str(e)}")
                return {
                    "success": False,
                    "entities": [],
                    "relationships": [],
                    "message": f"Failed to query index: {str(e)}",
                    "error": str(e)
                }
    
    async def search_vectors(
        self,
        workspace_url: str,
        index_name: str,
        endpoint_name: str,
        query_embedding: List[float],
        memory_type: str,
        k: int = 5,
        filters: Optional[Dict[str, Any]] = None,
        user_token: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for similar vectors in a Databricks Vector Search index.
        
        Args:
            workspace_url: Databricks workspace URL
            index_name: Full index name (catalog.schema.table)
            endpoint_name: Endpoint hosting the index
            query_embedding: Query vector for similarity search
            memory_type: Type of memory ("short_term", "long_term", "entity", "document")
            k: Number of results to return
            filters: Optional filters to apply
            user_token: Optional user access token for OBO authentication
            
        Returns:
            List of search results
        """
        try:
            # Get repository
            repo = self._get_index_repository(workspace_url)
            
            # Get search columns for the specific memory type
            from src.schemas.databricks_index_schemas import DatabricksIndexSchemas
            search_columns = DatabricksIndexSchemas.get_search_columns(memory_type)
            
            # Perform similarity search using repository
            result = await repo.similarity_search(
                index_name,
                endpoint_name,
                query_embedding,
                search_columns,
                k,
                filters,
                user_token
            )
            
            if result.get("success") and result.get("results"):
                # Process results based on memory type
                return self._process_search_results(result["results"], memory_type)
            else:
                logger.error(f"Search failed: {result.get('message')}")
                return []
                
        except Exception as e:
            logger.error(f"Failed to search vectors in {index_name}: {e}")
            return []
    
    def _process_search_results(self, raw_results: Dict[str, Any], memory_type: str) -> List[Dict[str, Any]]:
        """
        Process raw search results based on memory type.
        
        Args:
            raw_results: Raw results from Databricks Vector Search
            memory_type: Type of memory for result processing
            
        Returns:
            Processed search results
        """
        try:
            processed_results = []
            
            if raw_results and 'result' in raw_results:
                data_array = raw_results['result'].get('data_array', [])
                
                # Get memory-specific logger
                memory_logger = self.memory_loggers.get(memory_type, logger)
                memory_logger.info(f"[_process_search_results] Processing {len(data_array)} raw results for {memory_type}")
                
                from src.schemas.databricks_index_schemas import DatabricksIndexSchemas
                columns = DatabricksIndexSchemas.get_search_columns(memory_type)
                memory_logger.debug(f"[_process_search_results] Expected columns: {len(columns)}")
                
                for idx, row in enumerate(data_array):
                    if row:
                        # Databricks Vector Search may add a score column at the end
                        # We only process the expected columns and ignore any extra columns
                        memory_logger.debug(f"[_process_search_results] Row {idx}: length={len(row)}, expected={len(columns)}")
                        
                        # Map row data to column names (only process expected columns)
                        result_dict = {}
                        for i, column in enumerate(columns):
                            if i < len(row):
                                value = row[i]
                                # Parse JSON strings for metadata and relationship fields
                                if column in ['metadata', 'relationships', 'doc_metadata', 'attributes', 'relationship_data'] and isinstance(value, str):
                                    try:
                                        import json
                                        value = json.loads(value)
                                    except (json.JSONDecodeError, TypeError):
                                        # Keep as string if not valid JSON
                                        pass
                                result_dict[column] = value
                            else:
                                result_dict[column] = None
                        
                        # Ignore any extra columns (like score) - we don't need them
                        
                        processed_results.append(result_dict)
                    else:
                        memory_logger.warning(f"[_process_search_results] Empty row at index {idx}")
            
            memory_logger.info(f"[_process_search_results] Processed {len(processed_results)} results for {memory_type}")
            return processed_results
            
        except Exception as e:
            logger.error(f"Failed to process search results: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return []