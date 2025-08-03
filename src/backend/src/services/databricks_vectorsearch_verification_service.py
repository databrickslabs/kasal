"""
Databricks Vector Search verification service.

This module handles verification of Databricks Vector Search resources and configurations.
"""
from typing import Dict, Any, Optional
import aiohttp

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
        
        Args:
            workspace_url: Databricks workspace URL
            user_token: Optional user access token for OBO authentication
            config: Optional memory backend configuration to check specific resources
            
        Returns:
            Dict with existing endpoints and indexes
        """
        try:
            # Import here to avoid circular dependencies
            from src.utils.databricks_auth import get_databricks_auth_headers
            
            logger.info(f"Verifying Databricks resources - workspace_url: {workspace_url}, user_token provided: {bool(user_token)}")
            
            # Get authentication headers with user token for OBO auth
            headers, error = await get_databricks_auth_headers(user_token=user_token)
            logger.info(f"Auth headers result - headers: {bool(headers)}, error: {error}")
            
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
            
            async with aiohttp.ClientSession() as session:
                # Check only the configured endpoints
                for endpoint_name in endpoints_to_check:
                    api_url = f"https://{host}/api/2.0/vector-search/endpoints/{endpoint_name}"
                    try:
                        async with session.get(api_url, headers=headers) as response:
                            if response.status == 200:
                                data = await response.json()
                                endpoint_status = data.get('endpoint_status', {})
                                state = endpoint_status.get('state', 'UNKNOWN')
                                
                                logger.info(f"Endpoint {endpoint_name} exists with state {state}")
                                
                                result["endpoints"][endpoint_name] = {
                                    "exists": True,
                                    "state": state,
                                    "ready": state == "ONLINE",
                                    "type": data.get('endpoint_type', 'UNKNOWN')
                                }
                            elif response.status == 404:
                                logger.info(f"Endpoint {endpoint_name} not found")
                                result["endpoints"][endpoint_name] = {
                                    "exists": False,
                                    "state": "NOT_FOUND",
                                    "ready": False,
                                    "type": "UNKNOWN"
                                }
                            else:
                                logger.warning(f"Unexpected status {response.status} for endpoint {endpoint_name}")
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
                            # Get indexes for this endpoint
                            index_api_url = f"https://{host}/api/2.0/vector-search/indexes?endpoint_name={endpoint_name}"
                            async with session.get(index_api_url, headers=headers) as index_response:
                                if index_response.status == 200:
                                    index_data = await index_response.json()
                                    indexes = index_data.get('vector_indexes', [])
                                    
                                    for index in indexes:
                                        if index.get('name') == index_name:
                                            index_status = index.get('status', {})
                                            logger.info(f"Index {index_name} found on endpoint {endpoint_name}")
                                            
                                            result["indexes"][index_name] = {
                                                "exists": True,
                                                "endpoint": endpoint_name,
                                                "state": index_status.get('state', 'UNKNOWN'),
                                                "ready": index_status.get('ready', False)
                                            }
                                            found = True
                                            break
                                else:
                                    logger.warning(f"Failed to get indexes for endpoint {endpoint_name}: {index_response.status}")
                        except Exception as e:
                            logger.error(f"Error getting indexes for endpoint {endpoint_name}: {e}")
                    
                    # If index not found on any endpoint
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