"""
Databricks Authentication Utilities

Enhanced authentication for backend services supporting both:
1. Traditional PAT-based authentication
2. Databricks Apps OAuth with OBO (On-Behalf-Of) authentication

Uses the databricks_service and api_keys_service to get configuration and tokens.
"""

import os
import logging
import requests
import json
from typing import Optional, Tuple, Dict
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config

logger = logging.getLogger(__name__)


class DatabricksAuth:
    """Enhanced Databricks authentication class supporting PAT and OAuth OBO."""
    
    def __init__(self):
        self._api_token: Optional[str] = None
        self._workspace_host: Optional[str] = None
        self._config_loaded = False
        self._use_databricks_apps = False
        self._user_access_token: Optional[str] = None
        self._client_id: Optional[str] = None
        self._client_secret: Optional[str] = None

    async def _load_config(self) -> bool:
        """Load configuration from services if not already loaded."""
        if self._config_loaded:
            return True
            
        try:
            # First, check for Databricks Apps environment
            self._check_databricks_apps_environment()
            
            # If we're in Databricks Apps environment and have what we need, skip database config
            if self._use_databricks_apps and self._workspace_host:
                logger.info("Using Databricks Apps environment with host from environment variables")
                self._config_loaded = True
                return True
            
            # Only try database configuration if not fully configured from environment
            if not self._use_databricks_apps or not self._workspace_host:
                try:
                    # Get databricks configuration from database
                    from src.services.databricks_service import DatabricksService
                    from src.core.unit_of_work import UnitOfWork
                    
                    async with UnitOfWork() as uow:
                        service = await DatabricksService.from_unit_of_work(uow)
                        
                        # Get workspace config
                        try:
                            config = await service.get_databricks_config()
                            if config:
                                if config.workspace_url and not self._workspace_host:
                                    self._workspace_host = config.workspace_url.rstrip('/')
                                    if not self._workspace_host.startswith('https://'):
                                        self._workspace_host = f"https://{self._workspace_host}"
                                    logger.info(f"Loaded workspace host from database: {self._workspace_host}")
                                
                                # Check if Databricks Apps is enabled in config
                                if hasattr(config, 'apps_enabled') and config.apps_enabled and not self._use_databricks_apps:
                                    self._use_databricks_apps = True
                                    logger.info("Databricks Apps authentication enabled via database config")
                            
                        except Exception as e:
                            logger.warning(f"Failed to get databricks config from database: {e}")
                            
                except Exception as e:
                    logger.warning(f"Could not load database configuration: {e}")
            
            # If using Databricks Apps and we have what we need, we're done
            if self._use_databricks_apps and (self._workspace_host or (self._client_id and self._client_secret)):
                logger.info("Using Databricks Apps OAuth authentication")
                self._config_loaded = True
                return True
            
            # Try SDK auto-detection as fallback if not using apps
            if not self._workspace_host and not self._use_databricks_apps:
                try:
                    sdk_config = Config()
                    if sdk_config.host:
                        self._workspace_host = sdk_config.host
                        logger.info(f"Auto-detected workspace host: {self._workspace_host}")
                except Exception as e:
                    logger.debug(f"SDK auto-detection failed: {e}")
                
                # Get API token (only if not using Databricks Apps)
                if not self._use_databricks_apps:
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

    def _check_databricks_apps_environment(self):
        """Check if running in Databricks Apps environment and load credentials."""
        try:
            # Check for Databricks Apps environment variables
            client_id = os.environ.get('DATABRICKS_CLIENT_ID')
            client_secret = os.environ.get('DATABRICKS_CLIENT_SECRET')
            databricks_host = os.environ.get('DATABRICKS_HOST')
            app_name = os.environ.get('DATABRICKS_APP_NAME')
            workspace_id = os.environ.get('DATABRICKS_WORKSPACE_ID')
            
            # If we have any Databricks Apps indicators, we're in Apps environment
            if client_id and client_secret:
                self._client_id = client_id
                self._client_secret = client_secret
                self._use_databricks_apps = True
                logger.info("Detected Databricks Apps environment with OAuth credentials")
            elif app_name or workspace_id:
                self._use_databricks_apps = True
                logger.info("Detected Databricks Apps environment based on app variables")
            
            # Set workspace host from environment if available
            if databricks_host:
                # Ensure proper format
                if not databricks_host.startswith('https://'):
                    databricks_host = f"https://{databricks_host}"
                self._workspace_host = databricks_host.rstrip('/')
                logger.info(f"Using workspace host from environment: {self._workspace_host}")
            elif self._use_databricks_apps and not self._workspace_host:
                # Try to get workspace host from SDK config as fallback
                try:
                    sdk_config = Config()
                    if sdk_config.host:
                        self._workspace_host = sdk_config.host
                        logger.info(f"Using SDK host: {self._workspace_host}")
                except Exception as e:
                    logger.debug(f"Could not get host from SDK: {e}")
                    
            if not self._use_databricks_apps:
                logger.debug("No Databricks Apps environment detected")
                
        except Exception as e:
            logger.debug(f"Error checking Databricks Apps environment: {e}")

    def set_user_access_token(self, user_token: str):
        """Set user access token for OBO authentication."""
        self._user_access_token = user_token
        logger.info("User access token set for OBO authentication")

    async def get_auth_headers(self, mcp_server_url: str = None, user_token: str = None) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
        """
        Get authentication headers for Databricks API calls.
        
        Args:
            mcp_server_url: Optional MCP server URL (for MCP-specific headers)
            user_token: Optional user access token for OBO authentication
            
        Returns:
            Tuple[Optional[Dict[str, str]], Optional[str]]: Headers dict and error message if any
        """
        try:
            # Load config if needed
            if not await self._load_config():
                return None, "Failed to load Databricks configuration"
            
            # Set user token if provided
            if user_token:
                self.set_user_access_token(user_token)
            
            # Determine which authentication method to use
            if self._use_databricks_apps:
                return await self._get_oauth_headers(mcp_server_url)
            else:
                return await self._get_pat_headers(mcp_server_url)
            
        except Exception as e:
            logger.error(f"Error getting auth headers: {e}")
            return None, str(e)

    async def _get_oauth_headers(self, mcp_server_url: str = None) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
        """Get OAuth-based authentication headers for Databricks Apps."""
        try:
            # Use user access token if available (OBO)
            if self._user_access_token:
                logger.info("Using user access token for OBO authentication")
                token = self._user_access_token
            else:
                # Use service principal OAuth token
                logger.info("Using service principal OAuth token")
                token = await self._get_service_principal_token()
                if not token:
                    return None, "Failed to obtain service principal OAuth token"
            
            # Create headers
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            # Add SSE headers if it's an MCP server request
            if mcp_server_url:
                headers.update({
                    "Accept": "text/event-stream",
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive"
                })
            
            return headers, None
            
        except Exception as e:
            logger.error(f"Error getting OAuth headers: {e}")
            return None, str(e)

    async def _get_pat_headers(self, mcp_server_url: str = None) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
        """Get PAT-based authentication headers (legacy)."""
        try:
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
            logger.error(f"Error getting PAT headers: {e}")
            return None, str(e)

    async def _get_service_principal_token(self) -> Optional[str]:
        """Get OAuth token for service principal using client credentials flow."""
        try:
            if not self._client_id or not self._client_secret:
                logger.error("Missing client credentials for service principal authentication")
                return None
            
            # Use Databricks SDK for OAuth token
            try:
                # Create config with client credentials
                config = Config(
                    client_id=self._client_id,
                    client_secret=self._client_secret,
                    host=self._workspace_host
                )
                
                # Get access token through OAuth flow
                auth_result = config.authenticate()
                if hasattr(auth_result, 'access_token'):
                    logger.info("Successfully obtained service principal OAuth token")
                    return auth_result.access_token
                else:
                    logger.error("No access token in authentication result")
                    return None
                    
            except Exception as sdk_error:
                logger.error(f"SDK OAuth failed, trying manual approach: {sdk_error}")
                
                # Manual OAuth client credentials flow as fallback
                return await self._manual_oauth_flow()
                
        except Exception as e:
            logger.error(f"Error getting service principal token: {e}")
            return None

    async def _manual_oauth_flow(self) -> Optional[str]:
        """Manual OAuth client credentials flow for service principal."""
        try:
            if not self._workspace_host:
                logger.error("No workspace host available for OAuth")
                return None
            
            # OAuth token endpoint
            token_url = f"{self._workspace_host}/oidc/v1/token"
            
            # Prepare the request
            auth = (self._client_id, self._client_secret)
            data = {
                'grant_type': 'client_credentials',
                'scope': 'all-apis'
            }
            
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            
            # Make the token request
            response = requests.post(
                token_url,
                auth=auth,
                data=data,
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                token_data = response.json()
                access_token = token_data.get('access_token')
                if access_token:
                    logger.info("Successfully obtained OAuth token via manual flow")
                    return access_token
                else:
                    logger.error("No access token in OAuth response")
                    return None
            else:
                logger.error(f"OAuth request failed with status {response.status_code}: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error in manual OAuth flow: {e}")
            return None

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


async def get_databricks_auth_headers(host: str = None, mcp_server_url: str = None, user_token: str = None) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
    """
    Get authentication headers for Databricks API calls.
    
    Args:
        host: Optional host (for compatibility, ignored since we get it from config)
        mcp_server_url: Optional MCP server URL
        user_token: Optional user access token for OBO authentication
        
    Returns:
        Tuple[Optional[Dict[str, str]], Optional[str]]: Headers dict and error message if any
    """
    return await _databricks_auth.get_auth_headers(mcp_server_url, user_token)


def get_databricks_auth_headers_sync(host: str = None, mcp_server_url: str = None, user_token: str = None) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
    """
    Synchronous version of get_databricks_auth_headers.
    
    Args:
        host: Optional host (for compatibility, ignored since we get it from config)
        mcp_server_url: Optional MCP server URL
        user_token: Optional user access token for OBO authentication
        
    Returns:
        Tuple[Optional[Dict[str, str]], Optional[str]]: Headers dict and error message if any
    """
    try:
        import asyncio
        return asyncio.run(get_databricks_auth_headers(host, mcp_server_url, user_token))
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
        
        # Check if we're already in an event loop
        try:
            asyncio.get_running_loop()
            # We're in an event loop, create a task instead of using asyncio.run
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, _setup())
                return future.result()
        except RuntimeError:
            # No event loop running, safe to use asyncio.run
            return asyncio.run(_setup())
        
    except Exception as e:
        logger.error(f"Error setting up environment variables: {e}")
        return False


def extract_user_token_from_request(request) -> Optional[str]:
    """
    Extract user access token from request headers for OBO authentication.
    
    Args:
        request: FastAPI request object or similar with headers
        
    Returns:
        Optional[str]: User access token if found
    """
    try:
        # Check for X-Forwarded-Access-Token header (Databricks Apps standard)
        if hasattr(request, 'headers'):
            token = request.headers.get('X-Forwarded-Access-Token')
            if token:
                logger.debug("Found user access token in X-Forwarded-Access-Token header")
                return token
        
        # Fallback to Authorization header if no forwarded token
        if hasattr(request, 'headers'):
            auth_header = request.headers.get('Authorization')
            if auth_header and auth_header.startswith('Bearer '):
                token = auth_header[7:]  # Remove 'Bearer ' prefix
                logger.debug("Found user access token in Authorization header")
                return token
        
        return None
        
    except Exception as e:
        logger.debug(f"Error extracting user token: {e}")
        return None


def is_databricks_apps_environment() -> bool:
    """
    Check if running in a Databricks Apps environment.
    
    Returns:
        bool: True if Databricks Apps environment detected
    """
    # Check for multiple Databricks Apps environment indicators
    client_id = os.environ.get('DATABRICKS_CLIENT_ID')
    client_secret = os.environ.get('DATABRICKS_CLIENT_SECRET')
    app_name = os.environ.get('DATABRICKS_APP_NAME')
    workspace_id = os.environ.get('DATABRICKS_WORKSPACE_ID')
    
    # If we have client credentials OR app-specific variables, we're in Databricks Apps
    has_oauth_creds = bool(client_id and client_secret)
    has_app_vars = bool(app_name or workspace_id)
    
    return has_oauth_creds or has_app_vars


async def get_workspace_client(user_token: str = None) -> Optional[WorkspaceClient]:
    """
    Get a Databricks WorkspaceClient with appropriate authentication.
    
    Args:
        user_token: Optional user access token for OBO authentication
        
    Returns:
        Optional[WorkspaceClient]: Configured workspace client
    """
    try:
        if not await _databricks_auth._load_config():
            logger.error("Failed to load Databricks configuration")
            return None
        
        if _databricks_auth._use_databricks_apps:
            if user_token:
                # Use user token for OBO
                return WorkspaceClient(
                    host=_databricks_auth._workspace_host,
                    token=user_token
                )
            else:
                # Use service principal OAuth
                return WorkspaceClient(
                    host=_databricks_auth._workspace_host,
                    client_id=_databricks_auth._client_id,
                    client_secret=_databricks_auth._client_secret
                )
        else:
            # Use traditional PAT
            return WorkspaceClient(
                host=_databricks_auth._workspace_host,
                token=_databricks_auth._api_token
            )
            
    except Exception as e:
        logger.error(f"Error creating workspace client: {e}")
        return None


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