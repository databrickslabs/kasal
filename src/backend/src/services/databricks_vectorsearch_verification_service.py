"""
Databricks Vector Search verification service.

This module handles verification of Databricks Vector Search resources and configurations.
"""
from typing import Dict, Any, Optional
import os

from src.repositories.databricks_vector_endpoint_repository import DatabricksVectorEndpointRepository
from src.repositories.databricks_vector_index_repository import DatabricksVectorIndexRepository
from src.schemas.databricks_vector_endpoint import EndpointState
from src.schemas.databricks_vector_index import IndexState
from src.core.logger import LoggerManager

logger = LoggerManager.get_instance().system


class DatabricksVectorSearchVerificationService:
    """Service for verifying Databricks Vector Search resources."""
    
    async def verify_databricks_resources(
        self,
        workspace_url: str,
        user_token: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Verify which Databricks resources actually exist.
        
        Uses repository pattern for clean architecture compliance.
        
        Args:
            workspace_url: Databricks workspace URL
            user_token: Optional user access token for OBO authentication
            config: Optional memory backend configuration to check specific resources
            
        Returns:
            Dict with existing endpoints and indexes
        """
        try:
            logger.info(f"Verifying Databricks resources - workspace_url: {workspace_url}, user_token provided: {bool(user_token)}")
            
            # Initialize repositories - they handle all authentication internally
            endpoint_repo = DatabricksVectorEndpointRepository(workspace_url)
            index_repo = DatabricksVectorIndexRepository(workspace_url)
            
            result = {
                "endpoints": {},
                "indexes": {}
            }
            
            # Get configured endpoints and indexes to check
            endpoints_to_check = set()
            indexes_to_check = set()
            
            if config and config.get('databricks_config'):
                db_config = config['databricks_config']
                
                # Add configured endpoints
                if db_config.get('endpoint_name'):
                    endpoints_to_check.add(db_config['endpoint_name'])
                if db_config.get('document_endpoint_name'):
                    endpoints_to_check.add(db_config['document_endpoint_name'])
                
                # Add configured indexes
                for index_key in ['short_term_index', 'long_term_index', 'entity_index', 'document_index']:
                    if db_config.get(index_key):
                        indexes_to_check.add(db_config[index_key])
            
            logger.info(f"Checking configured endpoints: {endpoints_to_check}")
            logger.info(f"Checking configured indexes: {indexes_to_check}")
            
            if not endpoints_to_check and not indexes_to_check:
                logger.info("No configured resources to check")
                return {
                    "success": True,
                    "resources": result
                }
            
            # Check only the configured endpoints using repository
            for endpoint_name in endpoints_to_check:
                try:
                    # Use repository to get endpoint info
                    endpoint_response = await endpoint_repo.get_endpoint(endpoint_name, user_token)
                    
                    if endpoint_response.success and endpoint_response.endpoint:
                        endpoint = endpoint_response.endpoint
                        logger.info(f"Endpoint {endpoint_name} exists with state {endpoint.state}")
                        
                        result["endpoints"][endpoint_name] = {
                            "exists": True,
                            "state": endpoint.state if isinstance(endpoint.state, str) else endpoint.state.value,
                            "ready": endpoint.ready,
                            "type": endpoint.endpoint_type if isinstance(endpoint.endpoint_type, str) else endpoint.endpoint_type.value
                        }
                    else:
                        # Check if it's specifically not found or another error
                        if endpoint_response.endpoint and endpoint_response.endpoint.state == EndpointState.NOT_FOUND:
                            logger.info(f"Endpoint {endpoint_name} not found")
                            result["endpoints"][endpoint_name] = {
                                "exists": False,
                                "state": "NOT_FOUND",
                                "ready": False,
                                "type": "UNKNOWN"
                            }
                        else:
                            logger.warning(f"Error checking endpoint {endpoint_name}: {endpoint_response.message}")
                            result["endpoints"][endpoint_name] = {
                                "exists": False,
                                "state": "ERROR",
                                "ready": False,
                                "type": "UNKNOWN"
                            }
                except Exception as e:
                    logger.error(f"Error checking endpoint {endpoint_name}: {e}")
                    result["endpoints"][endpoint_name] = {
                        "exists": False,
                        "state": "ERROR",
                        "ready": False,
                        "type": "UNKNOWN"
                    }
            
            # Check only the configured indexes
            # We need to check each index individually through their endpoints
            for index_name in indexes_to_check:
                found = False
                # Check each endpoint for this index
                for endpoint_name in endpoints_to_check:
                    if found:
                        break
                    try:
                        # Use repository to list indexes for this endpoint
                        indexes_response = await index_repo.list_indexes(endpoint_name, user_token)
                        
                        if indexes_response.success:
                            # Check if our index is in the list
                            for index in indexes_response.indexes:
                                if index.name == index_name:
                                    logger.info(f"Index {index_name} found on endpoint {endpoint_name}")
                                    
                                    result["indexes"][index_name] = {
                                        "exists": True,
                                        "endpoint": endpoint_name,
                                        "state": index.state if isinstance(index.state, str) else index.state.value,
                                        "ready": index.ready
                                    }
                                    found = True
                                    break
                        else:
                            logger.warning(f"Failed to get indexes for endpoint {endpoint_name}: {indexes_response.message}")
                    except Exception as e:
                        logger.error(f"Error getting indexes for endpoint {endpoint_name}: {e}")
                
                # If index not found on any endpoint, try to get it directly
                if not found:
                    # Try to get the index directly (it might exist on a different endpoint)
                    for endpoint_name in endpoints_to_check:
                        try:
                            index_response = await index_repo.get_index(
                                index_name=index_name,
                                endpoint_name=endpoint_name,
                                user_token=user_token
                            )
                            
                            if index_response.success and index_response.index:
                                if index_response.index.state != IndexState.NOT_FOUND:
                                    logger.info(f"Index {index_name} found on endpoint {endpoint_name}")
                                    result["indexes"][index_name] = {
                                        "exists": True,
                                        "endpoint": endpoint_name,
                                        "state": index_response.index.state if isinstance(index_response.index.state, str) else index_response.index.state.value,
                                        "ready": index_response.index.ready
                                    }
                                    found = True
                                    break
                        except Exception as e:
                            logger.debug(f"Index {index_name} not found on endpoint {endpoint_name}: {e}")
                    
                    # If still not found
                    if not found:
                        logger.info(f"Index {index_name} not found on any configured endpoint")
                        result["indexes"][index_name] = {
                            "exists": False,
                            "endpoint": None,
                            "state": "NOT_FOUND",
                            "ready": False
                        }
            
            return {
                "success": True,
                "resources": result
            }
                        
        except Exception as e:
            logger.error(f"Error verifying Databricks resources: {e}")
            return {
                "success": False,
                "message": str(e),
                "resources": {
                    "endpoints": {},
                    "indexes": {}
                }
            }