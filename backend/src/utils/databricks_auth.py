"""
Databricks Authentication Utilities

Simple programmatic authentication for backend services.
Uses the databricks_service and api_keys_service to get configuration and tokens.
"""

import os
import logging
import requests
import json
from typing import Optional, Tuple, Dict

logger = logging.getLogger(__name__)


class DatabricksAuth:
    """Simple Databricks authentication class for backend services."""
    
    def __init__(self):
        self._api_token: Optional[str] = None
        self._workspace_host: Optional[str] = None
        self._config_loaded = False

    async def _load_config(self) -> bool:
        """Load configuration from services if not already loaded."""
        if self._config_loaded:
            return True
            
        try:
            # Get databricks configuration
            from src.services.databricks_service import DatabricksService
            from src.core.unit_of_work import UnitOfWork
            
            async with UnitOfWork() as uow:
                service = await DatabricksService.from_unit_of_work(uow)
                
                # Get workspace config
                try:
                    config = await service.get_databricks_config()
                    if config and config.workspace_url:
                        self._workspace_host = config.workspace_url.rstrip('/')
                        if not self._workspace_host.startswith('https://'):
                            self._workspace_host = f"https://{self._workspace_host}"
                        logger.info(f"Loaded workspace host: {self._workspace_host}")
                    else:
                        logger.error("No workspace URL found in configuration")
                        return False
                except Exception as e:
                    logger.error(f"Failed to get databricks config: {e}")
                    return False
                
                # Get API token
                try:
                    from src.services.api_keys_service import ApiKeysService
                    api_service = await ApiKeysService.from_unit_of_work(uow)
                    
                    # Try to get DATABRICKS_TOKEN or DATABRICKS_API_KEY
                    for key_name in ["DATABRICKS_TOKEN", "DATABRICKS_API_KEY"]:
                        api_key = await api_service.find_by_name(key_name)
                        if api_key and api_key.encrypted_value:
                            from src.utils.encryption_utils import EncryptionUtils
                            self._api_token = EncryptionUtils.decrypt_value(api_key.encrypted_value)
                            logger.info(f"Loaded API token from {key_name}")
                            break
                    
                    if not self._api_token:
                        # Try environment variables as fallback
                        self._api_token = os.environ.get("DATABRICKS_TOKEN") or os.environ.get("DATABRICKS_API_KEY")
                        if self._api_token:
                            logger.info("Using API token from environment variables")
                        else:
                            logger.error("No Databricks API token found")
                            return False
                            
                except Exception as e:
                    logger.error(f"Failed to get API token: {e}")
                    return False
            
            self._config_loaded = True
            return True
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            return False

    async def get_auth_headers(self, mcp_server_url: str = None) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
        """
        Get authentication headers for Databricks API calls.
        
        Args:
            mcp_server_url: Optional MCP server URL (for future extension)
            
        Returns:
            Tuple[Optional[Dict[str, str]], Optional[str]]: Headers dict and error message if any
        """
        try:
            # Load config if needed
            if not await self._load_config():
                return None, "Failed to load Databricks configuration"
            
            # Validate token with a simple API call
            if not await self._validate_token():
                return None, "Invalid or expired Databricks token"
            
            # Return simple Bearer token headers
            headers = {
                "Authorization": f"Bearer {self._api_token}",
                "Content-Type": "application/json"
            }
            
            # Add SSE headers if it's an MCP server request
            if mcp_server_url:
                headers.update({
                    "Accept": "text/event-stream",
                    "Cache-Control": "no-cache"
                })
            
            return headers, None
            
        except Exception as e:
            logger.error(f"Error getting auth headers: {e}")
            return None, str(e)

    async def _validate_token(self) -> bool:
        """Validate the API token by making a simple API call."""
        try:
            if not self._api_token or not self._workspace_host:
                return False
            
            # Simple validation call to get current user
            url = f"{self._workspace_host}/api/2.0/preview/scim/v2/Me"
            headers = {
                "Authorization": f"Bearer {self._api_token}",
                "Content-Type": "application/json"
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                user_data = response.json()
                username = user_data.get("userName", "Unknown")
                logger.info(f"Token validated for user: {username}")
                return True
            else:
                logger.error(f"Token validation failed with status {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error validating token: {e}")
            return False

    def get_workspace_host(self) -> Optional[str]:
        """Get the workspace host."""
        return self._workspace_host

    def get_api_token(self) -> Optional[str]:
        """Get the API token."""
        return self._api_token


# Global instance for easy access
_databricks_auth = DatabricksAuth()


async def get_databricks_auth_headers(host: str = None, mcp_server_url: str = None) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
    """
    Get authentication headers for Databricks API calls.
    
    Args:
        host: Optional host (for compatibility, ignored since we get it from config)
        mcp_server_url: Optional MCP server URL
        
    Returns:
        Tuple[Optional[Dict[str, str]], Optional[str]]: Headers dict and error message if any
    """
    return await _databricks_auth.get_auth_headers(mcp_server_url)


def get_databricks_auth_headers_sync(host: str = None, mcp_server_url: str = None) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
    """
    Synchronous version of get_databricks_auth_headers.
    
    Args:
        host: Optional host (for compatibility, ignored since we get it from config)
        mcp_server_url: Optional MCP server URL
        
    Returns:
        Tuple[Optional[Dict[str, str]], Optional[str]]: Headers dict and error message if any
    """
    try:
        import asyncio
        return asyncio.run(get_databricks_auth_headers(host, mcp_server_url))
    except Exception as e:
        logger.error(f"Error in sync auth headers: {e}")
        return None, str(e)


async def validate_databricks_connection() -> Tuple[bool, Optional[str]]:
    """
    Validate the Databricks connection.
    
    Returns:
        Tuple[bool, Optional[str]]: (is_valid, error_message)
    """
    try:
        if not await _databricks_auth._load_config():
            return False, "Failed to load configuration"
        
        is_valid = await _databricks_auth._validate_token()
        if is_valid:
            return True, None
        else:
            return False, "Token validation failed"
            
    except Exception as e:
        logger.error(f"Error validating connection: {e}")
        return False, str(e)


def setup_environment_variables() -> bool:
    """
    Set up Databricks environment variables for compatibility with other libraries.
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        import asyncio
        
        async def _setup():
            if not await _databricks_auth._load_config():
                return False
            
            # Set environment variables
            if _databricks_auth._api_token:
                os.environ["DATABRICKS_TOKEN"] = _databricks_auth._api_token
                os.environ["DATABRICKS_API_KEY"] = _databricks_auth._api_token
                
            if _databricks_auth._workspace_host:
                os.environ["DATABRICKS_HOST"] = _databricks_auth._workspace_host
                # Also set API_BASE for LiteLLM compatibility
                os.environ["DATABRICKS_API_BASE"] = _databricks_auth._workspace_host
                
            return True
        
        return asyncio.run(_setup())
        
    except Exception as e:
        logger.error(f"Error setting up environment variables: {e}")
        return False


async def get_mcp_access_token() -> Tuple[Optional[str], Optional[str]]:
    """
    Get an MCP access token by calling the Databricks CLI directly.
    This is the most reliable approach since we know 'databricks auth token -p mcp' works.
    
    Returns:
        Tuple[Optional[str], Optional[str]]: (access_token, error_message)
    """
    try:
        import subprocess
        import json
        
        logger.info("Getting MCP token using Databricks CLI")
        
        # Call the CLI command that we know works
        result = subprocess.run(
            ["databricks", "auth", "token", "-p", "mcp"],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Parse the JSON output
        token_data = json.loads(result.stdout)
        access_token = token_data.get("access_token")
        
        if not access_token:
            return None, "No access token found in CLI response"
        
        # Verify this is a JWT token (should start with eyJ)
        if access_token.startswith("eyJ"):
            logger.info("Successfully obtained JWT token from CLI for MCP")
            return access_token, None
        else:
            logger.warning(f"Token doesn't look like JWT: {access_token[:20]}...")
            return access_token, None
            
    except subprocess.CalledProcessError as e:
        return None, f"CLI command failed: {e.stderr}"
    except json.JSONDecodeError as e:
        return None, f"Failed to parse CLI output: {e}"
    except Exception as e:
        logger.error(f"Error getting MCP token from CLI: {e}")
        return None, str(e)


async def get_mcp_auth_headers(mcp_server_url: str) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
    """
    Get authentication headers for MCP server calls.
    This follows the exact same approach as the CLI test.
    
    Args:
        mcp_server_url: MCP server URL
        
    Returns:
        Tuple[Optional[Dict[str, str]], Optional[str]]: Headers dict and error message if any
    """
    try:
        # Get the access token (same as CLI)
        access_token, error = await get_mcp_access_token()
        if error:
            return None, error
            
        # Return headers exactly as the CLI test does
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive"
        }
        
        return headers, None
        
    except Exception as e:
        logger.error(f"Error getting MCP auth headers: {e}")
        return None, str(e)