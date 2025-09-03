"""
Repository for Databricks Vector Search Index operations.

This repository handles all interactions with Databricks Vector Search indexes,
following the clean architecture pattern.
"""
from typing import Optional, List, Dict, Any
import aiohttp
import asyncio
import json
import os
# No longer using VectorSearchClient - using REST API directly
from src.core.logger import LoggerManager
from src.schemas.databricks_vector_index import (
    IndexCreate,
    IndexInfo,
    IndexResponse,
    IndexListResponse,
    IndexState,
    IndexType
)
from src.repositories.databricks_auth_helper import DatabricksAuthHelper

logger = LoggerManager.get_instance().system
vector_search_logger = LoggerManager.get_instance().databricks_vector_search


class DatabricksVectorIndexRepository:
    """Repository for managing Databricks Vector Search indexes."""
    
    def __init__(self, workspace_url: str):
        """
        Initialize the repository.
        
        Args:
            workspace_url: Databricks workspace URL
        """
        # Clean up workspace URL and validate
        if workspace_url:
            self.workspace_url = workspace_url.rstrip('/')
        else:
            # Try to get from environment variable
            env_url = os.getenv('DATABRICKS_HOST', '').rstrip('/')
            if env_url:
                self.workspace_url = env_url
                logger.info(f"Using DATABRICKS_HOST from environment: {self.workspace_url}")
            else:
                self.workspace_url = ""
                logger.warning("No Databricks workspace URL configured. Set DATABRICKS_HOST environment variable.")
    
    async def _get_auth_token(self, user_token: Optional[str] = None) -> str:
        """
        Get authentication token for REST API calls.
        
        Follows authentication priority:
        1. OBO (On-Behalf-Of) with user token
        2. PAT from database (encrypted storage)
        3. PAT from environment variables
        
        Args:
            user_token: Optional user token for OBO authentication
            
        Returns:
            Authentication token
            
        Raises:
            Exception: If no authentication token can be obtained
        """
        return await DatabricksAuthHelper.get_auth_token(
            workspace_url=self.workspace_url,
            user_token=user_token
        )
    
    
    async def create_index(
        self,
        index_data: IndexCreate,
        user_token: Optional[str] = None
    ) -> IndexResponse:
        """
        Create a new vector search index using REST API.
        
        Args:
            index_data: Index creation parameters
            user_token: Optional user token for OBO authentication
            
        Returns:
            IndexResponse with creation result
        """
        try:
            # Get authentication token
            auth_token = await self._get_auth_token(user_token)
            
            # Prepare the REST API endpoint
            url = f"{self.workspace_url}/api/2.0/vector-search/indexes"
            
            # Prepare headers
            headers = {
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json"
            }
            
            # Prepare the payload for direct access index
            payload = {
                "name": index_data.name,
                "endpoint_name": index_data.endpoint_name,
                "index_type": "DIRECT_ACCESS",
                "primary_key": index_data.primary_key,
                "direct_access_index_spec": {
                    "embedding_vector_columns": [
                        {
                            "name": index_data.embedding_vector_column,
                            "embedding_dimension": index_data.embedding_dimension
                        }
                    ],
                    "schema_json": json.dumps(index_data.schema_definition)
                }
            }
            
            logger.info(f"Creating index {index_data.name} via REST API at {url}")
            
            # Make the REST API call
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=payload) as response:
                    response_text = await response.text()
                    
                    if response.status in [200, 201]:
                        logger.info(f"Successfully created index: {index_data.name}")
                        
                        # Get index info to return
                        index_info = await self.get_index(
                            index_data.name,
                            index_data.endpoint_name,
                            user_token
                        )
                        
                        return IndexResponse(
                            success=True,
                            index=index_info.index,
                            message=f"Index {index_data.name} created successfully"
                        )
                    else:
                        error_msg = f"Failed to create index. Status: {response.status}, Response: {response_text}"
                        logger.error(error_msg)
                        return IndexResponse(
                            success=False,
                            error=error_msg,
                            message=f"Failed to create index: {error_msg}"
                        )
            
        except Exception as e:
            logger.error(f"Failed to create index {index_data.name}: {e}")
            return IndexResponse(
                success=False,
                error=str(e),
                message=f"Failed to create index: {str(e)}"
            )
    
    async def get_index(
        self,
        index_name: str,
        endpoint_name: Optional[str] = None,
        user_token: Optional[str] = None
    ) -> IndexResponse:
        """
        Get information about a specific index using REST API.
        
        Args:
            index_name: Full index name (catalog.schema.table)
            endpoint_name: Optional endpoint hosting the index (not used for direct access indexes)
            user_token: Optional user token for OBO authentication
            
        Returns:
            IndexResponse with index information
        """
        try:
            # Get authentication token
            auth_token = await self._get_auth_token(user_token)
            
            # Prepare the REST API endpoint
            from urllib.parse import quote
            encoded_index_name = quote(index_name, safe='')
            url = f"{self.workspace_url}/api/2.0/vector-search/indexes/{encoded_index_name}"
            
            # Prepare headers
            headers = {
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json"
            }
            
            logger.info(f"Getting index {index_name} via REST API at {url}")
            
            # Make the REST API call
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Extract index info from response
                        index_status = data.get("status", {})
                        
                        # Debug logging to see exact values from Databricks
                        logger.info(f"Raw Databricks index data keys: {list(data.keys())}")
                        logger.info(f"Raw index status: {index_status}")
                        logger.info(f"Raw state value: {repr(index_status.get('state'))}")
                        logger.info(f"Raw detailed_state value: {repr(index_status.get('detailed_state'))}")
                        logger.info(f"Raw ready value: {repr(index_status.get('ready'))}")
                        
                        # Parse state with proper handling
                        raw_state = index_status.get("state")
                        if raw_state is None:
                            raw_detailed_state = index_status.get("detailed_state")
                            if raw_detailed_state:
                                if raw_detailed_state == "ONLINE_DIRECT_ACCESS":
                                    raw_state = "READY"
                                elif raw_detailed_state in ["PROVISIONING", "INITIALIZING"]:
                                    raw_state = "PROVISIONING"
                                elif raw_detailed_state in ["OFFLINE", "STOPPING", "STOPPED"]:
                                    raw_state = "OFFLINE"
                                elif raw_detailed_state in ["FAILED", "ERROR"]:
                                    raw_state = "FAILED"
                                else:
                                    logger.info(f"Unknown detailed_state '{raw_detailed_state}', will determine from ready flag")
                                    raw_state = "READY" if index_status.get("ready", False) else "UNKNOWN"
                            else:
                                raw_state = "READY" if index_status.get("ready", False) else "UNKNOWN"
                        
                        try:
                            state = IndexState(raw_state)
                        except ValueError:
                            logger.warning(f"Unknown index state '{raw_state}', defaulting to UNKNOWN")
                            state = IndexState.UNKNOWN
                        
                        # Parse ready flag
                        raw_ready = index_status.get("ready", False)
                        ready = bool(raw_ready) if raw_ready is not None else False
                        
                        logger.info(f"Parsed state: {state}, ready: {ready}")
                        
                        # Determine index type
                        # ALWAYS use DIRECT_ACCESS - no DELTA_SYNC allowed
                        index_type = IndexType.DIRECT_ACCESS
                        if "direct_access_index_spec" in data:
                            index_type = IndexType.DIRECT_ACCESS
                        
                        index_info = IndexInfo(
                            name=index_name,
                            endpoint_name=endpoint_name,
                            index_type=index_type,
                            state=state,
                            ready=ready,
                            row_count=data.get("num_rows", 0) or index_status.get("indexed_row_count", 0),
                            indexed_row_count=index_status.get("indexed_row_count", 0),
                            embedding_dimension=data.get("direct_access_index_spec", {}).get("embedding_dimension"),
                            primary_key=data.get("primary_key")
                        )
                        
                        return IndexResponse(
                            success=True,
                            index=index_info,
                            message=f"Index {index_name} retrieved successfully"
                        )
                    
                    elif response.status == 404:
                        return IndexResponse(
                            success=False,
                            index=IndexInfo(
                                name=index_name,
                                endpoint_name=endpoint_name,
                                state=IndexState.NOT_FOUND,
                                ready=False
                            ),
                            error="Index not found",
                            message=f"Index {index_name} not found"
                        )
                    
                    else:
                        error_text = await response.text()
                        error_msg = f"API returned status {response.status}: {error_text}"
                        logger.error(error_msg)
                        return IndexResponse(
                            success=False,
                            error=error_msg,
                            message=f"Failed to get index: {error_msg}"
                        )
            
        except Exception as e:
            logger.error(f"Failed to get index {index_name}: {e}")
            return IndexResponse(
                success=False,
                error=str(e),
                message=f"Failed to get index: {str(e)}"
            )
    
    async def list_indexes(
        self,
        endpoint_name: str,
        user_token: Optional[str] = None
    ) -> IndexListResponse:
        """
        List all indexes on an endpoint using REST API.
        
        Args:
            endpoint_name: Endpoint to list indexes for
            user_token: Optional user token for OBO authentication
            
        Returns:
            IndexListResponse with list of indexes
        """
        try:
            # Get authentication token
            auth_token = await self._get_auth_token(user_token)
            
            # Prepare the REST API endpoint
            url = f"{self.workspace_url}/api/2.0/vector-search/indexes"
            
            # Prepare headers
            headers = {
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json"
            }
            
            # Add endpoint filter as query parameter
            params = {"endpoint_name": endpoint_name}
            
            logger.info(f"Listing indexes for endpoint {endpoint_name} via REST API")
            
            # Make the REST API call
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        indexes_data = data.get("indexes", [])
                        
                        # Convert to IndexInfo objects
                        indexes = []
                        for idx_data in indexes_data:
                            index_status = idx_data.get("status", {})
                            
                            # Parse state with same logic as get_index method
                            raw_state = index_status.get("state")
                            if raw_state is None:
                                raw_detailed_state = index_status.get("detailed_state")
                                if raw_detailed_state:
                                    if raw_detailed_state == "ONLINE_DIRECT_ACCESS":
                                        raw_state = "READY"
                                    elif raw_detailed_state in ["PROVISIONING", "INITIALIZING"]:
                                        raw_state = "PROVISIONING"
                                    elif raw_detailed_state in ["OFFLINE", "STOPPING", "STOPPED"]:
                                        raw_state = "OFFLINE"
                                    elif raw_detailed_state in ["FAILED", "ERROR"]:
                                        raw_state = "FAILED"
                                    else:
                                        raw_state = "READY" if index_status.get("ready", False) else "UNKNOWN"
                                else:
                                    raw_state = "READY" if index_status.get("ready", False) else "UNKNOWN"
                            
                            try:
                                state = IndexState(raw_state)
                            except ValueError:
                                state = IndexState.UNKNOWN
                            
                            indexes.append(IndexInfo(
                                name=idx_data.get("name", ""),
                                endpoint_name=endpoint_name,
                                # ALWAYS use DIRECT_ACCESS - no DELTA_SYNC allowed
                                index_type=IndexType.DIRECT_ACCESS,
                                state=state,
                                ready=index_status.get("ready", False),
                                row_count=idx_data.get("num_rows", 0) or index_status.get("indexed_row_count", 0),
                                indexed_row_count=index_status.get("indexed_row_count", 0)
                            ))
                        
                        return IndexListResponse(
                            success=True,
                            indexes=indexes,
                            message=f"Found {len(indexes)} indexes on endpoint {endpoint_name}"
                        )
                    
                    else:
                        error_text = await response.text()
                        error_msg = f"API returned status {response.status}: {error_text}"
                        logger.error(error_msg)
                        return IndexListResponse(
                            success=False,
                            indexes=[],
                            message=f"Failed to list indexes: {error_msg}"
                        )
            
        except Exception as e:
            logger.error(f"Failed to list indexes for endpoint {endpoint_name}: {e}")
            return IndexListResponse(
                success=False,
                indexes=[],
                message=f"Failed to list indexes: {str(e)}"
            )
    
    async def delete_index(
        self,
        index_name: str,
        endpoint_name: str,
        user_token: Optional[str] = None
    ) -> IndexResponse:
        """
        Delete a vector search index using REST API.
        
        Args:
            index_name: Full index name to delete
            endpoint_name: Endpoint hosting the index
            user_token: Optional user token for OBO authentication
            
        Returns:
            IndexResponse with deletion result
        """
        try:
            # Get authentication token
            auth_token = await self._get_auth_token(user_token)
            
            # Prepare the REST API endpoint
            # URL encode the index name to handle special characters
            from urllib.parse import quote
            encoded_index_name = quote(index_name, safe='')
            url = f"{self.workspace_url}/api/2.0/vector-search/indexes/{encoded_index_name}"
            
            # Prepare headers
            headers = {
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json"
            }
            
            logger.info(f"Deleting index {index_name} via REST API at {url}")
            
            # Make the REST API call
            async with aiohttp.ClientSession() as session:
                async with session.delete(url, headers=headers) as response:
                    response_text = await response.text()
                    
                    if response.status in [200, 204]:
                        logger.info(f"Successfully deleted index: {index_name}")
                        return IndexResponse(
                            success=True,
                            message=f"Index {index_name} deleted successfully"
                        )
                    elif response.status == 404:
                        logger.warning(f"Index {index_name} not found")
                        return IndexResponse(
                            success=False,
                            error="Index not found",
                            message=f"Index {index_name} not found"
                        )
                    else:
                        error_msg = f"Failed to delete index. Status: {response.status}, Response: {response_text}"
                        logger.error(error_msg)
                        return IndexResponse(
                            success=False,
                            error=error_msg,
                            message=f"Failed to delete index: {error_msg}"
                        )
            
        except Exception as e:
            logger.error(f"Failed to delete index {index_name}: {e}")
            return IndexResponse(
                success=False,
                error=str(e),
                message=f"Failed to delete index: {str(e)}"
            )
    
    async def empty_index(
        self,
        index_name: str,
        endpoint_name: str,
        embedding_dimension: int,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Empty all vectors from a Direct Access index by deleting and recreating it.
        
        Since Direct Access indexes don't support bulk delete via the API,
        this method deletes the entire index and recreates it with the same configuration.
        
        Args:
            index_name: Full index name to empty
            endpoint_name: Endpoint hosting the index  
            embedding_dimension: Dimension of the index embeddings
            user_token: Optional user token for OBO authentication
            
        Returns:
            Dict with operation result
        """
        try:
            logger.info(f"Attempting to empty Direct Access index {index_name} via delete/recreate")
            
            # Get authentication token
            auth_token = await self._get_auth_token(user_token)
            
            # Prepare headers
            headers = {
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json"
            }
            
            from urllib.parse import quote
            encoded_index_name = quote(index_name, safe='')
            
            # Step 1: Get current index configuration
            describe_url = f"{self.workspace_url}/api/2.0/vector-search/indexes/{encoded_index_name}"
            
            async with aiohttp.ClientSession() as session:
                # Get index info
                async with session.get(describe_url, headers=headers) as response:
                    if response.status != 200:
                        return {
                            "success": False,
                            "deleted_count": 0,
                            "message": f"Index {index_name} not found or not accessible",
                            "error": "Index not found"
                        }
                    
                    index_info = await response.json()
                    original_doc_count = index_info.get('status', {}).get('indexed_row_count', 0)
                    logger.info(f"Index {index_name} has {original_doc_count} documents, type: {index_info.get('index_type', 'unknown')}")
                
                # Extract configuration for recreation
                primary_key = index_info.get("primary_key", "id")
                direct_access_spec = index_info.get("direct_access_index_spec", {})
                embedding_columns = direct_access_spec.get("embedding_vector_columns", [])
                schema_json = direct_access_spec.get("schema_json", "{}")
                
                # If no embedding columns found, create default based on provided dimension
                if not embedding_columns:
                    embedding_columns = [{
                        "name": "embedding",
                        "embedding_dimension": embedding_dimension
                    }]
                
                # Step 2: Delete the index
                logger.info(f"Deleting index {index_name}...")
                delete_url = f"{self.workspace_url}/api/2.0/vector-search/indexes/{encoded_index_name}"
                
                async with session.delete(delete_url, headers=headers) as response:
                    if response.status not in [200, 204]:
                        error_text = await response.text()
                        return {
                            "success": False,
                            "deleted_count": 0,
                            "message": f"Failed to delete index: {error_text[:200]}",
                            "error": "Delete failed"
                        }
                
                logger.info(f"Successfully deleted index {index_name}")
                
                # Wait a bit for deletion to propagate
                await asyncio.sleep(3)
                
                # Step 3: Recreate the index with same configuration
                logger.info(f"Recreating index {index_name} with same configuration...")
                
                create_payload = {
                    "name": index_name,
                    "endpoint_name": endpoint_name,
                    "primary_key": primary_key,
                    "index_type": "DIRECT_ACCESS",
                    "direct_access_index_spec": {
                        "embedding_vector_columns": embedding_columns,
                        "schema_json": schema_json
                    }
                }
                
                create_url = f"{self.workspace_url}/api/2.0/vector-search/indexes"
                
                async with session.post(create_url, headers=headers, json=create_payload) as response:
                    response_text = await response.text()
                    if response.status not in [200, 201]:
                        return {
                            "success": False,
                            "deleted_count": 0,
                            "message": f"Failed to recreate index: {response_text[:200]}",
                            "error": "Recreation failed"
                        }
                
                logger.info(f"Successfully recreated index {index_name}")
                
                # Step 4: Wait for index to be ready (with timeout)
                max_attempts = 12  # 60 seconds total
                for attempt in range(max_attempts):
                    await asyncio.sleep(5)
                    
                    async with session.get(describe_url, headers=headers) as response:
                        if response.status == 200:
                            new_info = await response.json()
                            status = new_info.get("status", {})
                            if status.get("ready"):
                                logger.info(f"Index {index_name} is ready after {(attempt + 1) * 5} seconds")
                                return {
                                    "success": True,
                                    "deleted_count": original_doc_count,
                                    "message": f"Successfully emptied index by delete/recreate. Removed {original_doc_count} documents."
                                }
                            else:
                                state = status.get("detailed_state", "UNKNOWN")
                                logger.info(f"Index state: {state}, attempt {attempt + 1}/{max_attempts}")
                
                # If we get here, index was created but may not be fully ready
                logger.warning(f"Index {index_name} recreated but may not be fully ready yet")
                return {
                    "success": True,
                    "deleted_count": original_doc_count,
                    "message": f"Index recreated (removed {original_doc_count} documents). It may take a moment to be fully ready."
                }
        
        except Exception as e:
            logger.error(f"Failed to empty index {index_name}: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to empty index: {str(e)}"
            }
    
    async def similarity_search(
        self,
        index_name: str,
        endpoint_name: str,
        query_vector: List[float],
        columns: List[str],
        num_results: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Perform similarity search on an index using REST API.
        
        Args:
            index_name: Full index name (catalog.schema.table)
            endpoint_name: Endpoint hosting the index
            query_vector: Query embedding vector
            columns: Columns to return in results
            num_results: Number of results to return
            filters: Optional filters to apply
            user_token: Optional user token for OBO authentication
            
        Returns:
            Search results dictionary
        """
        try:
            # Get authentication token
            auth_token = await self._get_auth_token(user_token)
            
            # Prepare the REST API endpoint
            from urllib.parse import quote
            encoded_index_name = quote(index_name, safe='')
            url = f"{self.workspace_url}/api/2.0/vector-search/indexes/{encoded_index_name}/query"
            
            # Prepare headers
            headers = {
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json"
            }
            
            # Log search parameters for debugging
            vector_search_logger.info(f"[similarity_search] Index: {index_name}")
            vector_search_logger.info(f"[similarity_search] Query vector dimension: {len(query_vector)}")
            vector_search_logger.info(f"[similarity_search] Requested columns: {columns[:5]}..." if len(columns) > 5 else f"[similarity_search] Requested columns: {columns}")
            vector_search_logger.info(f"[similarity_search] Num results requested: {num_results}")
            vector_search_logger.info(f"[similarity_search] Filters: {filters}")
            
            # Prepare the payload
            payload = {
                "query_vector": query_vector,
                "columns": columns,
                "num_results": num_results
            }
            if filters:
                payload["filters"] = filters
            
            # Make the REST API call
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=payload) as response:
                    if response.status == 200:
                        results = await response.json()
                        
                        # Log detailed results
                        if results and 'result' in results:
                            data_array = results.get('result', {}).get('data_array', [])
                            vector_search_logger.info(f"[similarity_search] Returned {len(data_array)} results")
                            if len(data_array) == 0 and filters:
                                vector_search_logger.warning(f"[similarity_search] No results found with filters: {filters}")
                                # Try without filters to debug
                                vector_search_logger.info("[similarity_search] Trying search without filters for debugging...")
                                debug_payload = {
                                    "query_vector": query_vector,
                                    "columns": columns,
                                    "num_results": num_results
                                }
                                async with session.post(url, headers=headers, json=debug_payload) as debug_response:
                                    if debug_response.status == 200:
                                        debug_results = await debug_response.json()
                                        debug_data = debug_results.get('result', {}).get('data_array', [])
                                        vector_search_logger.info(f"[similarity_search] Debug search without filters returned {len(debug_data)} results")
                        else:
                            vector_search_logger.info(f"[similarity_search] Returned {len(results.get('result', {}).get('data_array', []))} results")
                        
                        return {
                            "success": True,
                            "results": results,
                            "message": "Search completed successfully"
                        }
                    else:
                        error_text = await response.text()
                        logger.error(f"Search failed with status {response.status}: {error_text}")
                        return {
                            "success": False,
                            "error": error_text,
                            "message": f"Failed to perform search: {error_text}",
                            "results": None
                        }
            
        except Exception as e:
            logger.error(f"Failed to perform similarity search on {index_name}: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to perform search: {str(e)}",
                "results": None
            }
    
    async def describe_index(
        self,
        index_name: str,
        endpoint_name: str,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get detailed description of an index using REST API.
        
        Args:
            index_name: Full index name (catalog.schema.table)
            endpoint_name: Endpoint hosting the index
            user_token: Optional user token for OBO authentication
            
        Returns:
            Index description dictionary
        """
        try:
            # Use get_index REST API which returns full description
            response = await self.get_index(index_name, endpoint_name, user_token)
            
            if response.success:
                # Convert IndexInfo back to description format
                # Note: Pydantic with use_enum_values=True means enum fields are already strings
                description = {
                    "name": index_name,
                    "endpoint_name": endpoint_name,
                    "index_type": response.index.index_type if response.index else None,
                    "primary_key": response.index.primary_key if response.index else None,
                    "status": {
                        "state": response.index.state if response.index else None,
                        "ready": response.index.ready if response.index else False,
                        "indexed_row_count": response.index.indexed_row_count if response.index else 0
                    },
                    "num_rows": response.index.row_count if response.index else 0
                }
                
                if response.index and response.index.embedding_dimension:
                    description["direct_access_index_spec"] = {
                        "embedding_dimension": response.index.embedding_dimension
                    }
                
                return {
                    "success": True,
                    "description": description,
                    "message": "Index description retrieved successfully"
                }
            else:
                return {
                    "success": False,
                    "error": response.error,
                    "message": response.message,
                    "description": None
                }
            
        except Exception as e:
            logger.error(f"Failed to describe index {index_name}: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to describe index: {str(e)}",
                "description": None
            }
    
    async def upsert(
        self,
        index_name: str,
        endpoint_name: str,
        records: List[Dict[str, Any]],
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Upsert records into a vector search index using REST API.
        
        Args:
            index_name: Full index name (catalog.schema.table)
            endpoint_name: Endpoint hosting the index
            records: List of records to upsert
            user_token: Optional user token for OBO authentication
            
        Returns:
            Operation result dictionary
        """
        try:
            # Validate workspace URL
            if not self.workspace_url or self.workspace_url == "/api/2.0/vector-search":
                error_msg = "Databricks workspace URL is not configured. Please set DATABRICKS_HOST environment variable or configure it in the application."
                logger.error(error_msg)
                return {
                    "success": False,
                    "error": "Missing workspace URL",
                    "message": error_msg,
                    "suggestion": "Set DATABRICKS_HOST environment variable to your Databricks workspace URL (e.g., https://your-workspace.databricks.com)"
                }
            
            # Get authentication token
            auth_token = await self._get_auth_token(user_token)
            
            # Prepare the REST API endpoint
            from urllib.parse import quote
            encoded_index_name = quote(index_name, safe='')
            url = f"{self.workspace_url}/api/2.0/vector-search/indexes/{encoded_index_name}/upsert-data"
            
            # Prepare headers
            headers = {
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json"
            }
            
            # Prepare the payload - ensure records is a list
            if not isinstance(records, list):
                records = [records]
            
            # Validate records are not empty
            if not records:
                logger.error("Empty records list provided to upsert")
                return {
                    "success": False,
                    "error": "No records provided",
                    "message": "Failed to upsert records: No records provided"
                }
            
            # Log record structure for debugging
            if records:
                sample_record = records[0]
                logger.debug(f"Sample record keys: {list(sample_record.keys()) if isinstance(sample_record, dict) else 'Not a dict'}")
                
                # Additional validation - check if records have required fields
                if isinstance(sample_record, dict):
                    if not sample_record:
                        logger.error("First record is an empty dictionary")
                        return {
                            "success": False,
                            "error": "Empty record",
                            "message": "Failed to upsert records: Records cannot be empty"
                        }
                    # Log first few fields of the sample record (without embedding for brevity)
                    sample_fields = {k: v for k, v in list(sample_record.items())[:5] if k != 'embedding'}
                    logger.debug(f"Sample record content (first 5 fields): {sample_fields}")
                    
                    # Check if embedding field exists and is valid
                    if 'embedding' in sample_record:
                        embedding = sample_record['embedding']
                        if isinstance(embedding, list):
                            logger.debug(f"Embedding is a list with {len(embedding)} dimensions")
                        else:
                            logger.warning(f"Embedding is not a list: {type(embedding)}")
                    else:
                        logger.warning("Sample record does not contain 'embedding' field")
            
            # IMPORTANT: Databricks expects "inputs_json" as a JSON STRING, not "inputs" as an object
            # Convert records to JSON string for the inputs_json field
            try:
                import json
                inputs_json_str = json.dumps(records)
                logger.debug(f"Serialized {len(records)} records to JSON string, size: {len(inputs_json_str)} bytes")
            except Exception as json_error:
                logger.error(f"Records are not JSON serializable: {json_error}")
                # Try to identify the problematic field
                for i, record in enumerate(records):
                    for key, value in record.items():
                        try:
                            json.dumps({key: value})
                        except:
                            logger.error(f"Record {i}, field '{key}' is not JSON serializable: {type(value)}")
                return {
                    "success": False,
                    "error": f"JSON serialization failed: {json_error}",
                    "message": f"Failed to upsert records: Records are not JSON serializable"
                }
            
            # Create payload with inputs_json as a string
            payload = {"inputs_json": inputs_json_str}
            
            logger.info(f"Upserting {len(records)} records to {index_name}")
            logger.debug(f"Payload has 'inputs_json' key with JSON string of {len(records)} records")
            
            # Make the REST API call
            async with aiohttp.ClientSession() as session:
                # Log the complete structure for debugging
                logger.info(f"Sending upsert request to: {url}")
                logger.info(f"Payload keys: {list(payload.keys())}")
                logger.info(f"Using inputs_json field with {len(records)} records as JSON string")
                
                # Log sample record structure for debugging
                if records:
                    first_record = records[0]
                    logger.info(f"First record keys: {list(first_record.keys())}")
                    # Check embedding specifically
                    if 'embedding' in first_record:
                        emb = first_record['embedding']
                        logger.info(f"Embedding type: {type(emb)}, length: {len(emb) if isinstance(emb, (list, tuple)) else 'N/A'}")
                        # Log first few values to verify it's numeric
                        if isinstance(emb, list) and len(emb) > 0:
                            logger.info(f"First 3 embedding values: {emb[:3]}")
                
                # Use json parameter for proper serialization
                logger.info("Sending request with json parameter for inputs_json string...")
                
                # IMPORTANT: Use json= parameter, not data= parameter
                # The json= parameter properly serializes the data and sets Content-Type
                async with session.post(url, headers=headers, json=payload) as response:
                    response_text = await response.text()
                    
                    if response.status in [200, 201, 202]:
                        logger.info(f"Successfully upserted {len(records)} records to {index_name}")
                        return {
                            "success": True,
                            "upserted_count": len(records),
                            "message": f"Successfully upserted {len(records)} records"
                        }
                    elif response.status == 400 and "INVALID_PARAMETER_VALUE" in response_text:
                        # Parse the error message for better diagnostics
                        error_msg = f"Invalid parameter value: {response_text}"
                        logger.error(f"Upsert failed with invalid parameter: {error_msg}")
                        
                        # Check if it's an empty payload error
                        if "is empty" in response_text:
                            logger.error("The upsert payload appears to be empty or malformed")
                            logger.error(f"Records provided: {len(records)}, First record keys: {list(records[0].keys()) if records else 'None'}")
                            
                            # Log the actual payload structure for debugging
                            if records:
                                sample = records[0]
                                logger.error(f"Sample record structure: {json.dumps({k: type(v).__name__ for k, v in sample.items()}, indent=2)}")
                                if 'embedding' in sample:
                                    logger.error(f"Embedding dimensions: {len(sample['embedding']) if isinstance(sample['embedding'], list) else 'Not a list'}")
                            
                            return {
                                "success": False,
                                "error": "Empty or malformed payload",
                                "message": f"The upsert payload is empty or malformed. Check that records contain valid data.",
                                "details": response_text
                            }
                        else:
                            return {
                                "success": False,
                                "error": "Invalid parameter value",
                                "message": f"Invalid parameter in upsert request: {response_text}",
                                "details": response_text
                            }
                    else:
                        error_msg = f"Failed to upsert. Status: {response.status}, Response: {response_text}"
                        logger.error(error_msg)
                        
                        # Log more details about the request for debugging
                        logger.debug(f"Request URL: {url}")
                        logger.debug(f"Request headers: {headers}")
                        logger.debug(f"Number of records in payload: {len(records)}")
                        
                        return {
                            "success": False,
                            "error": error_msg,
                            "message": f"Failed to upsert records: {error_msg}"
                        }
            
        except Exception as e:
            logger.error(f"Failed to upsert to index {index_name}: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to upsert records: {str(e)}"
            }
    
    async def delete_records(
        self,
        index_name: str,
        endpoint_name: str,
        primary_keys: List[str],
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Delete specific records from a vector search index using REST API.
        
        Args:
            index_name: Full index name (catalog.schema.table)
            endpoint_name: Endpoint hosting the index
            primary_keys: List of primary keys to delete
            user_token: Optional user token for OBO authentication
            
        Returns:
            Operation result dictionary
        """
        try:
            # Get authentication token
            auth_token = await self._get_auth_token(user_token)
            
            # Prepare the REST API endpoint
            from urllib.parse import quote
            encoded_index_name = quote(index_name, safe='')
            url = f"{self.workspace_url}/api/2.0/vector-search/indexes/{encoded_index_name}/delete-data"
            
            # Prepare headers
            headers = {
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json"
            }
            
            # Prepare the payload
            payload = {"primary_keys": primary_keys}
            
            logger.info(f"Deleting {len(primary_keys)} records from {index_name}")
            
            # Make the REST API call
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=payload) as response:
                    if response.status in [200, 204]:
                        logger.info(f"Successfully deleted {len(primary_keys)} records from {index_name}")
                        return {
                            "success": True,
                            "deleted_count": len(primary_keys),
                            "message": f"Successfully deleted {len(primary_keys)} records"
                        }
                    else:
                        error_text = await response.text()
                        error_msg = f"Failed to delete. Status: {response.status}, Response: {error_text}"
                        logger.error(error_msg)
                        return {
                            "success": False,
                            "error": error_msg,
                            "message": f"Failed to delete records: {error_msg}"
                        }
            
        except Exception as e:
            logger.error(f"Failed to delete from index {index_name}: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to delete records: {str(e)}"
            }
    
    async def count_documents(
        self,
        index_name: str,
        endpoint_name: str,
        filters: Optional[Dict[str, Any]] = None,
        user_token: Optional[str] = None
    ) -> int:
        """
        Count documents in an index with optional filters.
        
        Args:
            index_name: Full index name (catalog.schema.table)
            endpoint_name: Endpoint hosting the index
            filters: Optional filters to apply
            user_token: Optional user token for OBO authentication
            
        Returns:
            Number of documents matching the filters
        """
        try:
            # First try to get count from index stats if no filters
            if not filters:
                description = await self.describe_index(index_name, endpoint_name, user_token)
                if description.get("success") and description.get("description"):
                    desc = description["description"]
                    if isinstance(desc, dict):
                        # Check for indexed_row_count in status
                        if "status" in desc:
                            status = desc["status"]
                            if "indexed_row_count" in status:
                                return status["indexed_row_count"]
                        # Check for num_rows
                        if "num_rows" in desc:
                            return desc["num_rows"]
            
            # If we have filters or couldn't get count from stats, do a search
            # Use a dummy vector for counting
            dummy_vector = [0.0] * 768  # Default dimension, will be ignored for count
            
            # Search with filters to count matching documents
            search_result = await self.similarity_search(
                index_name=index_name,
                endpoint_name=endpoint_name,
                query_vector=dummy_vector,
                columns=["id"],
                num_results=10000,  # Maximum allowed
                filters=filters,
                user_token=user_token
            )
            
            count = 0
            if search_result.get("success") and search_result.get("results"):
                results = search_result["results"]
                if "result" in results:
                    data_array = results["result"].get("data_array", [])
                    count = len(data_array)
            
            logger.info(f"Counted {count} documents in {index_name} with filters: {filters}")
            return count
            
        except Exception as e:
            logger.error(f"Failed to count documents in {index_name}: {e}")
            return 0