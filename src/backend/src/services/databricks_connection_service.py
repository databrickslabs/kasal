"""
Databricks connection service for testing and managing connections.

This module handles authentication and connection testing for Databricks Vector Search.
"""
from typing import Dict, Any, Optional, Tuple
import aiohttp
import os

from src.schemas.memory_backend import DatabricksMemoryConfig
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
            from databricks.vector_search.client import VectorSearchClient
            from src.utils.databricks_auth import get_databricks_auth_headers, is_databricks_apps_environment
            
            # Try authentication methods in order (following genie_tool.py pattern)
            client = None
            auth_method = None
            
            # 1. Try OBO authentication if user token is provided
            if user_token:
                try:
                    headers, error = await get_databricks_auth_headers(user_token=user_token)
                    if headers and not error:
                        # Extract token from headers
                        auth_header = headers.get("Authorization", "")
                        if auth_header.startswith("Bearer "):
                            token = auth_header[7:]
                            client = VectorSearchClient(
                                workspace_url=config.workspace_url,
                                personal_access_token=token
                            )
                            auth_method = "OBO"
                            logger.info("Using OBO authentication for Vector Search")
                except Exception as obo_error:
                    logger.warning(f"OBO authentication failed: {obo_error}")
            
            # 2. Try API key from service if not authenticated yet
            if not client:
                try:
                    from src.services.api_keys_service import ApiKeysService
                    from src.core.unit_of_work import UnitOfWork
                    
                    async with UnitOfWork() as uow:
                        api_service = await ApiKeysService.from_unit_of_work(uow)
                        
                        # Try to get DATABRICKS_TOKEN or DATABRICKS_API_KEY
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
                                logger.info(f"Using {key_name} from API service for Vector Search")
                                break
                except Exception as api_error:
                    logger.warning(f"API key service authentication failed: {api_error}")
            
            # 3. Try provided authentication config
            if not client:
                client_kwargs = {}
                if config.workspace_url:
                    client_kwargs["workspace_url"] = config.workspace_url
                
                if config.auth_type == "pat" and config.personal_access_token:
                    client_kwargs["personal_access_token"] = config.personal_access_token
                    auth_method = "PAT (from config)"
                elif config.auth_type == "service_principal":
                    if config.service_principal_client_id and config.service_principal_client_secret:
                        client_kwargs["service_principal_client_id"] = config.service_principal_client_id
                        client_kwargs["service_principal_client_secret"] = config.service_principal_client_secret
                        auth_method = "Service Principal"
                
                if client_kwargs and auth_method:
                    client = VectorSearchClient(**client_kwargs)
                    logger.info(f"Using {auth_method} for Vector Search")
            
            # 4. Try environment variables or default auth
            if not client:
                if is_databricks_apps_environment():
                    client = VectorSearchClient()
                    auth_method = "Databricks Apps Environment"
                    logger.info("Using Databricks Apps environment authentication")
                else:
                    # Try with just workspace URL, let SDK figure out auth
                    client = VectorSearchClient(workspace_url=config.workspace_url)
                    auth_method = "Default SDK Authentication"
                    logger.info("Using default SDK authentication")
            
            # Test by getting endpoint info
            try:
                endpoint = client.get_endpoint(config.endpoint_name)
                endpoint_status = endpoint.get("endpoint_status", {}).get("state", "unknown")
                
                # Check if indexes exist
                indexes_found = []
                indexes_missing = []
                
                for index_name, index_type in [
                    (config.short_term_index, "short_term"),
                    (config.long_term_index, "long_term"),
                    (config.entity_index, "entity")
                ]:
                    if index_name:
                        try:
                            index = client.get_index(
                                endpoint_name=config.endpoint_name,
                                index_name=index_name
                            )
                            indexes_found.append({
                                "name": index_name,
                                "type": index_type,
                                "status": index.get("status", {}).get("state", "unknown")
                            })
                        except Exception:
                            indexes_missing.append({
                                "name": index_name,
                                "type": index_type
                            })
                
                return {
                    "success": True,
                    "message": f"Successfully connected to endpoint: {config.endpoint_name}",
                    "details": {
                        "endpoint_status": endpoint_status,
                        "auth_method": auth_method,
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
                        "auth_method": auth_method
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
    
    async def get_databricks_client_with_auth(
        self,
        workspace_url: str,
        user_token: Optional[str] = None
    ) -> Tuple[Optional[Any], str]:
        """
        Get Databricks VectorSearchClient with proper authentication fallback.
        
        Tries authentication methods in order:
        1. OBO (On-Behalf-Of) using user token
        2. Databricks client credentials (OAuth)
        3. API key from service (DATABRICKS_TOKEN/DATABRICKS_API_KEY)
        4. Environment variables as last resort
        
        Returns:
            Tuple of (client, auth_method_used)
        """
        from databricks.vector_search.client import VectorSearchClient
        from src.services.api_keys_service import ApiKeysService
        from src.core.unit_of_work import UnitOfWork
        from src.utils.encryption_utils import EncryptionUtils
        
        # 1. First try: OBO authentication with user token
        if user_token:
            try:
                logger.info("Attempting OBO authentication for empty_index")
                client = VectorSearchClient(
                    workspace_url=workspace_url,
                    personal_access_token=user_token
                )
                # Quick validation
                client.list_endpoints()
                logger.info("Successfully authenticated using OBO (user token)")
                return client, "OBO"
            except Exception as e:
                logger.warning(f"OBO authentication failed: {e}")
        
        # 2. Second try: Databricks client credentials (OAuth)
        try:
            logger.info("Attempting OAuth client credentials authentication")
            async with UnitOfWork() as uow:
                api_service = await ApiKeysService.from_unit_of_work(uow)
                
                # Look for client ID and secret
                client_id_key = await api_service.find_by_name("DATABRICKS_CLIENT_ID")
                client_secret_key = await api_service.find_by_name("DATABRICKS_CLIENT_SECRET")
                
                if client_id_key and client_secret_key:
                    client_id = EncryptionUtils.decrypt_value(client_id_key.encrypted_value)
                    client_secret = EncryptionUtils.decrypt_value(client_secret_key.encrypted_value)
                    
                    # Try OAuth authentication
                    from databricks.sdk import WorkspaceClient
                    from databricks.sdk.config import Config
                    
                    config = Config(
                        client_id=client_id,
                        client_secret=client_secret,
                        host=workspace_url
                    )
                    
                    # Get OAuth token
                    auth_result = config.authenticate()
                    if hasattr(auth_result, 'access_token'):
                        client = VectorSearchClient(
                            workspace_url=workspace_url,
                            personal_access_token=auth_result.access_token
                        )
                        # Quick validation
                        client.list_endpoints()
                        logger.info("Successfully authenticated using OAuth client credentials")
                        return client, "OAuth"
        except Exception as e:
            logger.warning(f"OAuth client credentials authentication failed: {e}")
        
        # 3. Third try: API key from service
        try:
            logger.info("Attempting API key authentication from service")
            async with UnitOfWork() as uow:
                api_service = await ApiKeysService.from_unit_of_work(uow)
                
                # Try DATABRICKS_TOKEN first, then DATABRICKS_API_KEY
                for key_name in ["DATABRICKS_TOKEN", "DATABRICKS_API_KEY"]:
                    api_key = await api_service.find_by_name(key_name)
                    if api_key and api_key.encrypted_value:
                        token = EncryptionUtils.decrypt_value(api_key.encrypted_value)
                        client = VectorSearchClient(
                            workspace_url=workspace_url,
                            personal_access_token=token
                        )
                        # Quick validation
                        client.list_endpoints()
                        logger.info(f"Successfully authenticated using API key from service: {key_name}")
                        return client, f"API_KEY_{key_name}"
        except Exception as e:
            logger.warning(f"API key authentication from service failed: {e}")
        
        # 4. Fourth try: Environment variables
        try:
            logger.info("Attempting environment variable authentication")
            token = os.environ.get("DATABRICKS_TOKEN") or os.environ.get("DATABRICKS_API_KEY")
            if token:
                client = VectorSearchClient(
                    workspace_url=workspace_url,
                    personal_access_token=token
                )
                # Quick validation
                client.list_endpoints()
                logger.info("Successfully authenticated using environment variables")
                return client, "ENV_VAR"
        except Exception as e:
            logger.warning(f"Environment variable authentication failed: {e}")
        
        # All methods failed
        raise ValueError("All authentication methods failed. Please check your Databricks credentials.")
    
    async def get_databricks_endpoint_status(
        self,
        workspace_url: str,
        endpoint_name: str,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get the status of a Databricks Vector Search endpoint.
        
        Args:
            workspace_url: Databricks workspace URL
            endpoint_name: Name of the endpoint
            user_token: Optional user access token for OBO authentication
            
        Returns:
            Dict with endpoint status information
        """
        logger.info(f"Getting endpoint status for {endpoint_name} at {workspace_url}")
        try:
            # Import here to avoid circular dependencies
            from src.utils.databricks_auth import get_databricks_auth_headers
            
            # Get authentication headers with user token for OBO auth
            headers, error = await get_databricks_auth_headers(user_token=user_token)
            if error or not headers:
                logger.info(f"OBO auth failed, trying PAT from database")
                # If OBO fails, try with API key from service
                try:
                    from src.services.api_keys_service import ApiKeysService
                    from src.core.unit_of_work import UnitOfWork
                    
                    async with UnitOfWork() as uow:
                        api_service = await ApiKeysService.from_unit_of_work(uow)
                        # Try to get DATABRICKS_TOKEN or DATABRICKS_API_KEY
                        for key_name in ["DATABRICKS_TOKEN", "DATABRICKS_API_KEY"]:
                            databricks_key = await api_service.find_by_name(key_name)
                            if databricks_key and databricks_key.encrypted_value:
                                try:
                                    from src.utils.encryption_utils import EncryptionUtils
                                    decrypted_value = EncryptionUtils.decrypt_value(databricks_key.encrypted_value)
                                    if decrypted_value:
                                        logger.info(f"Found Databricks API key in database: {key_name}")
                                        headers = {
                                            "Authorization": f"Bearer {decrypted_value}",
                                            "Content-Type": "application/json"
                                        }
                                        error = None
                                        break
                                except Exception as decrypt_error:
                                    logger.warning(f"Failed to decrypt API key {key_name}: {decrypt_error}")
                        else:
                            logger.info("No Databricks API key found in database")
                except Exception as api_key_error:
                    logger.warning(f"Failed to get API key from database: {api_key_error}")
                
                # If still no auth, try environment variable
                if not headers:
                    import os
                    env_token = os.getenv("DATABRICKS_API_KEY") or os.getenv("DATABRICKS_TOKEN")
                    if env_token:
                        logger.info("Using Databricks token from environment variable")
                        headers = {
                            "Authorization": f"Bearer {env_token}",
                            "Content-Type": "application/json"
                        }
                        error = None
                    else:
                        raise ValueError(f"Unable to authenticate with Databricks: No authentication method succeeded")
            
            # Extract workspace host from URL
            import urllib.parse
            parsed_url = urllib.parse.urlparse(workspace_url)
            host = parsed_url.netloc or parsed_url.path.strip('/')
            
            # Build API URL
            api_url = f"https://{host}/api/2.0/vector-search/endpoints/{endpoint_name}"
            logger.info(f"Making request to {api_url}")
            
            async with aiohttp.ClientSession() as session:
                async with session.get(api_url, headers=headers) as response:
                    logger.info(f"Response status: {response.status}")
                    if response.status == 200:
                        data = await response.json()
                        endpoint_status = data.get('endpoint_status', {})
                        state = endpoint_status.get('state', 'UNKNOWN')
                        
                        return {
                            "success": True,
                            "endpoint_name": endpoint_name,
                            "state": state,
                            "message": endpoint_status.get('message', ''),
                            "ready": state == "ONLINE",
                            "provisioning": state == "PROVISIONING",
                            "can_delete_indexes": state == "ONLINE"  # Can only delete indexes when endpoint is online
                        }
                    elif response.status == 404:
                        return {
                            "success": False,
                            "endpoint_name": endpoint_name,
                            "state": "NOT_FOUND",
                            "message": "Endpoint not found",
                            "ready": False,
                            "provisioning": False,
                            "can_delete_indexes": False
                        }
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to get endpoint status. Status: {response.status}, Error: {error_text}")
                        return {
                            "success": False,
                            "endpoint_name": endpoint_name,
                            "state": "ERROR",
                            "message": f"Failed to get endpoint status: {error_text}",
                            "ready": False,
                            "provisioning": False,
                            "can_delete_indexes": False
                        }
                        
        except Exception as e:
            logger.error(f"Error getting endpoint status: {e}")
            return {
                "success": False,
                "endpoint_name": endpoint_name,
                "state": "ERROR",
                "message": str(e),
                "ready": False,
                "provisioning": False,
                "can_delete_indexes": False
            }