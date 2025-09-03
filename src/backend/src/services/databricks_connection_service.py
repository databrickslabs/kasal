"""
Databricks connection service for testing and managing connections.

This module handles authentication and connection testing for Databricks Vector Search.
"""
from typing import Dict, Any, Optional, Tuple
import os

from src.schemas.memory_backend import DatabricksMemoryConfig
from src.repositories.databricks_auth_helper import DatabricksAuthHelper
from src.repositories.databricks_vector_endpoint_repository import DatabricksVectorEndpointRepository
from src.repositories.databricks_vector_index_repository import DatabricksVectorIndexRepository
from src.core.logger import LoggerManager
from src.core.unit_of_work import UnitOfWork

logger = LoggerManager.get_instance().system


class DatabricksConnectionService:
    """Service for managing Databricks connections and authentication."""
    
    def __init__(self, uow: UnitOfWork = None):
        """
        Initialize the service.
        
        Args:
            uow: Unit of Work instance (optional for connection testing)
        """
        self.uow = uow
    
    async def test_databricks_connection(
        self,
        config: DatabricksMemoryConfig,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Test connection to Databricks Vector Search.
        
        Args:
            config: Databricks configuration
            user_token: Optional user access token for OBO authentication
            
        Returns:
            Test result
        """
        try:
            # Create repositories
            endpoint_repo = DatabricksVectorEndpointRepository(config.workspace_url)
            index_repo = DatabricksVectorIndexRepository(config.workspace_url)
            
            # Test by getting endpoint info using repository
            try:
                endpoint_response = await endpoint_repo.get_endpoint_status(config.endpoint_name, user_token)
                
                if not endpoint_response.get("success"):
                    return {
                        "success": False,
                        "message": endpoint_response.get("message", "Failed to get endpoint"),
                        "details": {
                            "error": endpoint_response.get("error")
                        }
                    }
                
                endpoint_status = endpoint_response.get("status", "unknown")
                
                # Check if indexes exist using repository
                indexes_found = []
                indexes_missing = []
                
                for index_name, index_type in [
                    (config.short_term_index, "short_term"),
                    (config.long_term_index, "long_term"),
                    (config.entity_index, "entity")
                ]:
                    if index_name:
                        # Use repository to get index info
                        index_response = await index_repo.get_index(
                            index_name=index_name,
                            endpoint_name=config.endpoint_name,
                            user_token=user_token
                        )
                        
                        if index_response.success and index_response.index:
                            indexes_found.append({
                                "name": index_name,
                                "type": index_type,
                                "status": index_response.index.state if index_response.index.state else "unknown"
                            })
                        else:
                            indexes_missing.append({
                                "name": index_name,
                                "type": index_type
                            })
                
                return {
                    "success": True,
                    "message": f"Successfully connected to endpoint: {config.endpoint_name}",
                    "details": {
                        "endpoint_status": endpoint_status,
                        "auth_method": "Repository Pattern",
                        "indexes_found": indexes_found,
                        "indexes_missing": indexes_missing
                    }
                }
                
            except Exception as e:
                return {
                    "success": False,
                    "message": f"Failed to get endpoint info: {str(e)}",
                    "details": {
                        "error": str(e),
                        "auth_method": "Repository Pattern"
                    }
                }
            
        except ImportError:
            return {
                "success": False,
                "message": "databricks-vectorsearch package not installed",
                "details": {
                    "error": "Please install databricks-vectorsearch package"
                }
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Connection test failed: {str(e)}",
                "details": {
                    "error": str(e)
                }
            }
    
    async def get_databricks_auth_token(
        self,
        workspace_url: str,
        user_token: Optional[str] = None
    ) -> Tuple[str, str]:
        """
        Get Databricks authentication token with proper fallback.
        
        Uses the centralized auth helper for consistent authentication.
        
        Returns:
            Tuple of (auth_token, auth_method_used)
        """
        # Use auth helper to get token with proper authentication hierarchy
        try:
            token = await DatabricksAuthHelper.get_auth_token(
                workspace_url,
                user_token
            )
            
            if user_token:
                return token, "OBO Authentication"
            elif token:
                # Could be from DB or environment
                return token, "PAT Authentication"
            else:
                raise ValueError("No authentication token available")
        except Exception as e:
            logger.error(f"Failed to get authentication token: {e}")
            raise ValueError(f"All authentication methods failed: {e}")
    
    async def get_databricks_endpoint_status(
        self,
        workspace_url: str,
        endpoint_name: str,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get the status of a Databricks Vector Search endpoint.
        
        Uses the repository pattern for consistency with clean architecture.
        
        Args:
            workspace_url: Databricks workspace URL
            endpoint_name: Name of the endpoint
            user_token: Optional user access token for OBO authentication
            
        Returns:
            Dict with endpoint status information
        """
        logger.info(f"Getting endpoint status for {endpoint_name} at {workspace_url}")
        try:
            # Use repository for clean architecture compliance
            # The repository handles all authentication logic internally
            endpoint_repo = DatabricksVectorEndpointRepository(workspace_url)
            
            # Use the repository method which handles all authentication and API calls
            result = await endpoint_repo.get_endpoint_status(endpoint_name, user_token)
            
            # The repository returns the result in the expected format
            return result
                        
        except Exception as e:
            logger.error(f"Failed to get endpoint status: {e}")
            return {
                "success": False,
                "message": f"Failed to get endpoint status: {str(e)}",
                "details": {
                    "error": str(e)
                }
            }