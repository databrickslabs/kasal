"""
Databricks index service for managing Vector Search indexes.

This module handles creation, deletion, and management of Databricks Vector Search indexes.
"""
from typing import Dict, Any, Optional
import random

from src.schemas.memory_backend import DatabricksMemoryConfig
from src.core.logger import LoggerManager
from src.services.databricks_connection_service import DatabricksConnectionService

logger = LoggerManager.get_instance().system


class DatabricksIndexService:
    """Service for managing Databricks Vector Search indexes."""
    
    def __init__(self):
        """Initialize the service."""
        self.connection_service = DatabricksConnectionService()
    
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
            from databricks.vector_search.client import VectorSearchClient
            from src.utils.databricks_auth import get_databricks_auth_headers, is_databricks_apps_environment
            
            # Get authenticated client using the same pattern as test_databricks_connection
            client = None
            auth_method = None
            
            # 1. Try OBO authentication if user token is provided
            if user_token:
                try:
                    headers, error = await get_databricks_auth_headers(user_token=user_token)
                    if headers and not error:
                        auth_header = headers.get("Authorization", "")
                        if auth_header.startswith("Bearer "):
                            token = auth_header[7:]
                            client = VectorSearchClient(
                                workspace_url=config.workspace_url,
                                personal_access_token=token
                            )
                            auth_method = "OBO"
                except Exception as obo_error:
                    logger.warning(f"OBO authentication failed: {obo_error}")
            
            # 2. Try API key from service
            if not client:
                try:
                    from src.services.api_keys_service import ApiKeysService
                    from src.core.unit_of_work import UnitOfWork
                    
                    async with UnitOfWork() as uow:
                        api_service = await ApiKeysService.from_unit_of_work(uow)
                        
                        for key_name in ["DATABRICKS_TOKEN", "DATABRICKS_API_KEY"]:
                            api_key = await api_service.find_by_name(key_name)
                            if api_key and api_key.encrypted_value:
                                from src.utils.encryption_utils import EncryptionUtils
                                token = EncryptionUtils.decrypt_value(api_key.encrypted_value)
                                client = VectorSearchClient(
                                    workspace_url=config.workspace_url,
                                    personal_access_token=token
                                )
                                auth_method = "API Key Service"
                                break
                except Exception:
                    pass
            
            # 3. Try provided authentication config
            if not client:
                client_kwargs = {"workspace_url": config.workspace_url}
                
                if config.auth_type == "pat" and config.personal_access_token:
                    client_kwargs["personal_access_token"] = config.personal_access_token
                    auth_method = "PAT"
                elif config.auth_type == "service_principal":
                    if config.service_principal_client_id and config.service_principal_client_secret:
                        client_kwargs["service_principal_client_id"] = config.service_principal_client_id
                        client_kwargs["service_principal_client_secret"] = config.service_principal_client_secret
                        auth_method = "Service Principal"
                
                client = VectorSearchClient(**client_kwargs)
            
            # 4. Default auth
            if not client:
                client = VectorSearchClient(workspace_url=config.workspace_url)
                auth_method = "Default"
            
            # Construct index name
            index_name = f"{catalog}.{schema}.{table_name}"
            
            # Define index configuration based on type
            # Default to 1024 for databricks-gte-large-en model
            embedding_dimension = config.embedding_dimension or 1024
            
            # Determine which endpoint to use based on index type
            # For document embeddings, use storage optimized endpoint if available
            use_document_endpoint = (index_type == "document" and config.document_endpoint_name)
            target_endpoint = config.document_endpoint_name if use_document_endpoint else config.endpoint_name
            
            # Create the index
            try:
                # Define schema based on index type
                schema_def = {}
                if index_type == "short_term":
                    schema_def = {
                        "id": "string",
                        "crew_id": "string", 
                        "agent_id": "string",
                        "content": "string",
                        "embedding": "array<float>",
                        "metadata": "string",  # JSON string
                        "timestamp": "string",
                        "score": "float"
                    }
                elif index_type == "long_term":
                    schema_def = {
                        "id": "string",
                        "crew_id": "string",
                        "agent_id": "string", 
                        "content": "string",
                        "embedding": "array<float>",
                        "metadata": "string",  # JSON string
                        "timestamp": "string",
                        "importance": "float"
                    }
                elif index_type == "entity":
                    schema_def = {
                        "id": "string",
                        "crew_id": "string",
                        "agent_id": "string",
                        "entity_type": "string",
                        "entity_name": "string",
                        "embedding": "array<float>",
                        "attributes": "string",  # JSON string
                        "relationships": "string",  # JSON string
                        "timestamp": "string"
                    }
                elif index_type == "document":
                    schema_def = {
                        "id": "string",
                        "source": "string",
                        "title": "string",
                        "content": "string",
                        "embedding": "array<float>",
                        "doc_metadata": "string",  # JSON string
                        "created_at": "string",
                        "updated_at": "string"
                    }
                
                # Create direct access index
                client.create_direct_access_index(
                    endpoint_name=target_endpoint,
                    index_name=index_name,
                    primary_key=primary_key,
                    embedding_dimension=embedding_dimension,
                    embedding_vector_column="embedding",
                    schema=schema_def
                )
                
                # Update the config to include the new index
                if index_type == "short_term":
                    config.short_term_index = index_name
                elif index_type == "long_term":
                    config.long_term_index = index_name
                elif index_type == "entity":
                    config.entity_index = index_name
                elif index_type == "document":
                    config.document_index = index_name
                
                return {
                    "success": True,
                    "message": f"Successfully created {index_type} index: {index_name}",
                    "details": {
                        "index_name": index_name,
                        "index_type": index_type,
                        "auth_method": auth_method,
                        "embedding_dimension": embedding_dimension
                    }
                }
                
            except Exception as e:
                if "already exists" in str(e).lower():
                    return {
                        "success": False,
                        "message": f"Index {index_name} already exists",
                        "details": {
                            "index_name": index_name,
                            "error": "Index already exists"
                        }
                    }
                else:
                    raise
                    
        except Exception as e:
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
            from databricks.vector_search.client import VectorSearchClient
            from src.utils.databricks_auth import get_databricks_auth_headers, is_databricks_apps_environment
            
            # Get authenticated client (same auth pattern)
            client = None
            
            if user_token:
                try:
                    headers, error = await get_databricks_auth_headers(user_token=user_token)
                    if headers and not error:
                        auth_header = headers.get("Authorization", "")
                        if auth_header.startswith("Bearer "):
                            token = auth_header[7:]
                            client = VectorSearchClient(
                                workspace_url=config.workspace_url,
                                personal_access_token=token
                            )
                except Exception:
                    pass
            
            if not client:
                client = VectorSearchClient(workspace_url=config.workspace_url)
            
            # List indexes for the endpoint
            try:
                indexes = client.list_indexes(endpoint_name=config.endpoint_name)
                
                # Format the response
                formatted_indexes = []
                for index in indexes.get("indexes", []):
                    formatted_indexes.append({
                        "name": index.get("name"),
                        "status": index.get("status", {}).get("state"),
                        "dimension": index.get("embedding_dimension"),
                        "primary_key": index.get("primary_key"),
                        "doc_count": index.get("doc_count", 0)
                    })
                
                return {
                    "success": True,
                    "indexes": formatted_indexes,
                    "endpoint_name": config.endpoint_name
                }
                
            except Exception as e:
                return {
                    "success": False,
                    "message": f"Failed to list indexes: {str(e)}",
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
            from databricks.vector_search.client import VectorSearchClient
            
            # Get authenticated client
            client = None
            if user_token:
                try:
                    from src.utils.databricks_auth import get_databricks_auth_headers
                    headers, error = await get_databricks_auth_headers(user_token=user_token)
                    if headers and not error:
                        auth_header = headers.get("Authorization", "")
                        if auth_header.startswith("Bearer "):
                            token = auth_header[7:]
                            client = VectorSearchClient(
                                workspace_url=workspace_url,
                                personal_access_token=token
                            )
                except Exception:
                    pass
            
            if not client:
                client = VectorSearchClient(workspace_url=workspace_url)
            
            # Delete the index
            try:
                client.delete_index(
                    endpoint_name=endpoint_name,
                    index_name=index_name
                )
                
                return {
                    "success": True,
                    "message": f"Successfully deleted index: {index_name}"
                }
                
            except Exception as e:
                if "not found" in str(e).lower():
                    return {
                        "success": False,
                        "message": f"Index {index_name} not found"
                    }
                else:
                    raise
                    
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
            from databricks.vector_search.client import VectorSearchClient
            
            # Get authenticated client
            client = None
            if user_token:
                try:
                    from src.utils.databricks_auth import get_databricks_auth_headers
                    headers, error = await get_databricks_auth_headers(user_token=user_token)
                    if headers and not error:
                        auth_header = headers.get("Authorization", "")
                        if auth_header.startswith("Bearer "):
                            token = auth_header[7:]
                            client = VectorSearchClient(
                                workspace_url=workspace_url,
                                personal_access_token=token
                            )
                except Exception:
                    pass
            
            if not client:
                client = VectorSearchClient(workspace_url=workspace_url)
            
            # Check if endpoint has any indexes
            try:
                indexes = client.list_indexes(endpoint_name=endpoint_name)
                if indexes.get("indexes", []):
                    return {
                        "success": False,
                        "message": f"Cannot delete endpoint {endpoint_name} while it has active indexes"
                    }
            except Exception:
                # If we can't list indexes, try to delete anyway
                pass
            
            # Delete the endpoint
            try:
                client.delete_endpoint(name=endpoint_name)
                
                return {
                    "success": True,
                    "message": f"Successfully deleted endpoint: {endpoint_name}"
                }
                
            except Exception as e:
                if "not found" in str(e).lower():
                    return {
                        "success": False,
                        "message": f"Endpoint {endpoint_name} not found"
                    }
                else:
                    raise
                    
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to delete endpoint: {str(e)}"
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
            from databricks.vector_search.client import VectorSearchClient
            
            # Get authenticated client
            client = None
            if user_token:
                try:
                    from src.utils.databricks_auth import get_databricks_auth_headers
                    headers, error = await get_databricks_auth_headers(user_token=user_token)
                    if headers and not error:
                        auth_header = headers.get("Authorization", "")
                        if auth_header.startswith("Bearer "):
                            token = auth_header[7:]
                            client = VectorSearchClient(
                                workspace_url=workspace_url,
                                personal_access_token=token
                            )
                except Exception:
                    pass
            
            if not client:
                client = VectorSearchClient(workspace_url=workspace_url)
            
            # Get index and describe it
            try:
                index = client.get_index(
                    endpoint_name=endpoint_name,
                    index_name=index_name
                )
                
                # Get index description which includes metadata and stats
                description = index.describe()
                logger.info(f"Full index description for {index_name}: {description}")
                
                # Log the type and keys to help debug
                logger.info(f"Description type: {type(description)}")
                if isinstance(description, dict):
                    logger.info(f"Description keys: {list(description.keys())}")
                    # Log nested structures
                    for key, value in description.items():
                        if isinstance(value, dict):
                            logger.info(f"Description['{key}'] keys: {list(value.keys())}")
                
                # Extract relevant information from the description
                # Get document count - check various possible field names
                doc_count = 0
                
                # First check top-level fields
                doc_count = (
                    description.get("num_rows", 0) or 
                    description.get("row_count", 0) or 
                    description.get("num_indexed_rows", 0) or
                    description.get("indexed_row_count", 0) or
                    0
                )
                
                # Check in status object
                if doc_count == 0 and "status" in description:
                    status = description.get("status", {})
                    doc_count = (
                        status.get("indexed_row_count", 0) or
                        status.get("num_indexed_rows", 0) or
                        status.get("row_count", 0) or
                        0
                    )
                
                # For Direct Access indexes, check in direct_access_index_spec
                if doc_count == 0 and "direct_access_index_spec" in description:
                    direct_spec = description.get("direct_access_index_spec", {})
                    doc_count = (
                        direct_spec.get("num_rows", 0) or
                        direct_spec.get("row_count", 0) or
                        direct_spec.get("indexed_row_count", 0) or
                        0
                    )
                
                logger.info(f"Extracted doc_count: {doc_count} from description")
                
                # Get index type - all our indexes are Direct Access
                index_type = "Direct Access" if "direct_access_index_spec" in description else "UNKNOWN"
                
                # Get last update time if available
                last_sync_time = None
                # Direct Access indexes don't have automatic sync, so no last_sync_time
                
                # If we still don't have doc_count, try alternative approach
                if doc_count == 0:
                    logger.warning(f"Could not extract doc_count from description, will try REST API")
                    # Try using REST API to get index info
                    try:
                        import aiohttp
                        import urllib.parse
                        
                        # Get auth headers
                        headers = {}
                        if user_token:
                            from src.utils.databricks_auth import get_databricks_auth_headers
                            auth_headers, error = await get_databricks_auth_headers(user_token=user_token)
                            if auth_headers and not error:
                                headers = auth_headers
                        
                        # Parse workspace URL
                        parsed_url = urllib.parse.urlparse(workspace_url)
                        host = parsed_url.netloc or parsed_url.path.strip('/')
                        
                        # Try the REST API endpoint
                        api_url = f"https://{host}/api/2.0/vector-search/indexes/{index_name}"
                        async with aiohttp.ClientSession() as session:
                            async with session.get(api_url, headers=headers) as response:
                                if response.status == 200:
                                    rest_data = await response.json()
                                    logger.info(f"REST API response: {rest_data}")
                                    # Check for document count in REST response
                                    doc_count = (
                                        rest_data.get("num_indexed_rows", 0) or
                                        rest_data.get("indexed_row_count", 0) or
                                        rest_data.get("status", {}).get("indexed_row_count", 0) or
                                        0
                                    )
                    except Exception as e:
                        logger.error(f"Failed to get index info via REST API: {e}")
                
                return {
                    "success": True,
                    "index_name": index_name,
                    "endpoint_name": endpoint_name,
                    "doc_count": doc_count,
                    "index_type": index_type,
                    "dimension": description.get("direct_access_index_spec", {}).get("embedding_dimension", 0),
                    "primary_key": description.get("primary_key", "id"),
                    "last_sync_time": last_sync_time,
                    "description": description  # Full description for debugging
                }
                
            except Exception as e:
                return {
                    "success": False,
                    "index_name": index_name,
                    "message": f"Failed to get index info: {str(e)}",
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
            # Get authenticated client using fallback hierarchy
            client, auth_method = await self.connection_service.get_databricks_client_with_auth(workspace_url, user_token)
            logger.info(f"Using {auth_method} authentication for empty_index operation")
            
            # Get the index object
            try:
                index = client.get_index(
                    endpoint_name=endpoint_name,
                    index_name=index_name
                )
                
                # Try to delete all vectors in batches for all index types
                logger.info(f"Attempting to empty {index_type} index: {index_name}")
                try:
                    # Use scan to get all document IDs in smaller batches
                    batch_size = 1000
                    total_deleted = 0
                    
                    while True:
                        # Get a batch of documents
                        # Note: Databricks Vector Search doesn't have a direct scan API
                        # We'll use similarity search with a random vector
                        random_vector = [random.random() for _ in range(embedding_dimension)]
                        
                        search_results = index.similarity_search(
                            query_vector=random_vector,
                            columns=["id"],  # Only need the primary key
                            num_results=batch_size,
                            filters={}
                        )
                        
                        # Extract primary keys
                        primary_keys = []
                        if search_results and 'result' in search_results:
                            data_array = search_results['result'].get('data_array', [])
                            for row in data_array:
                                if row and len(row) > 0:
                                    primary_keys.append(row[0])
                        
                        if not primary_keys:
                            logger.info(f"No more vectors found. Total deleted: {total_deleted}")
                            break
                        
                        # Delete this batch
                        try:
                            index.delete(primary_keys=primary_keys)
                            total_deleted += len(primary_keys)
                            logger.info(f"Deleted batch of {len(primary_keys)} vectors. Total so far: {total_deleted}")
                            
                            # If we got fewer results than batch_size, we're probably done
                            if len(primary_keys) < batch_size:
                                break
                                
                        except Exception as batch_error:
                            logger.warning(f"Failed to delete batch: {batch_error}")
                            # Continue trying other batches
                    
                    return {
                        "success": True,
                        "message": f"Successfully emptied index {index_name}",
                        "num_deleted": total_deleted
                    }
                    
                except Exception as batch_error:
                    logger.error(f"Batch deletion failed: {batch_error}")
                    # Fall back to recreate approach for memory indexes too
                    return {
                        "success": False,
                        "message": f"Cannot empty index {index_name}. The index may be too large or have constraints. Consider manually deleting and recreating it.",
                        "error_details": str(batch_error)
                    }
                
            except Exception as e:
                if "not found" in str(e).lower():
                    return {
                        "success": False,
                        "message": f"Index {index_name} not found on endpoint {endpoint_name}"
                    }
                else:
                    raise
                    
        except Exception as e:
            logger.error(f"Failed to empty index {index_name}: {str(e)}")
            return {
                "success": False,
                "message": f"Failed to empty index: {str(e)}"
            }