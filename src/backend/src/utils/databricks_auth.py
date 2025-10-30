"""
Databricks Authentication Utilities

This module provides a unified authentication layer for Databricks API interactions,
supporting multiple authentication methods with intelligent fallback mechanisms.

## Supported Authentication Methods

1. **OBO (On-Behalf-Of) Authentication**
   - Uses user access tokens from X-Forwarded-Access-Token header
   - Enables requests to execute with the authenticated user's identity
   - Best for user-specific operations (e.g., knowledge search, file access)

2. **PAT (Personal Access Token) Authentication**
   - Traditional token-based authentication
   - Tokens stored encrypted in database via API Keys Service
   - Scoped to workspace (group_id) for multi-tenant isolation
   - Fallback when OBO is not available
   - Works for both service-level and user-level operations

3. **Service Principal OAuth (Client Credentials Flow)**
   - OAuth 2.0 client credentials flow using client_id and client_secret
   - Automatic token refresh with expiration tracking
   - Requires DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET environment variables

## Unified Authentication Priority Chain

**ALL services use the same priority chain:**

1. **OBO** (user_token parameter from request headers)
   - If user_token is provided, tries OBO first
   - If user_token is None, skips OBO

2. **PAT** (from API Keys Service with group_id)
   - Loaded from database using API Keys Service
   - Scoped to workspace (group_id) for multi-tenant isolation
   - Looks for keys: DATABRICKS_TOKEN or DATABRICKS_API_KEY

3. **SPN** (Service Principal OAuth)
   - Uses DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET from environment
   - Automatic token refresh with expiration tracking

**SECURITY NOTE**: PAT tokens are ONLY retrieved from database with group_id filtering for multi-tenant isolation.
Environment variables are NOT used for PAT authentication.

## Key Classes

### DatabricksAuth
Main authentication class that manages credentials and provides authentication headers.

Key features:
- Lazy configuration loading from database and environment
- Token validation and automatic refresh
- Environment variable management for SDK compatibility
- Support for multiple authentication methods with fallback

### Key Functions (Unified API)

**PRIMARY FUNCTIONS (Use These)**:
- `get_auth_context()`: **UNIFIED** authentication - returns AuthContext with token, headers, environment
- `get_workspace_client()`: Get configured WorkspaceClient (wrapper around get_auth_context())

**LEGACY FUNCTIONS (Being Deprecated)**:
- `get_databricks_auth_headers()`: Get auth headers for API calls (use get_auth_context().get_headers() instead)
- `setup_environment_variables()`: **DEPRECATED** - causes race conditions, use get_auth_context() instead
- `get_mcp_auth_headers()`: Get auth headers for MCP server calls (use get_auth_context().get_mcp_headers() instead)

**UTILITY FUNCTIONS**:
- `validate_databricks_connection()`: Validate current authentication setup
- `extract_user_token_from_request()`: Extract OBO token from request headers
- `get_current_databricks_user()`: Get current user identity

## Usage Examples

### RECOMMENDED: Unified API (get_auth_context)

#### Basic REST API Calls
```python
from src.utils.databricks_auth import get_auth_context

# Get authentication context (OBO → PAT → SPN priority)
auth = await get_auth_context(user_token=user_token)
if not auth:
    raise AuthenticationError("No authentication available")

# Use headers for REST API calls
headers = auth.get_headers()
response = requests.get(f"{auth.workspace_url}/api/2.0/clusters/list", headers=headers)

# Access token directly if needed
token = auth.token
```

#### WorkspaceClient Usage
```python
from src.utils.databricks_auth import get_workspace_client

# With OBO (user-specific operations)
client = await get_workspace_client(user_token=user_token)

# Service-level (no OBO, uses PAT/SPN)
# Example: MLflow, background jobs, system operations
client = await get_workspace_client(user_token=None)
```

#### liteLLM Integration (Thread-Safe, No Race Conditions)
```python
from src.utils.databricks_auth import get_auth_context
import litellm

# Get auth context
auth = await get_auth_context(user_token=user_token)
params = auth.get_litellm_params()

# Pass parameters directly (thread-safe)
response = litellm.completion(
    model="databricks/model-name",
    messages=[...],
    api_key=params["api_key"],
    api_base=params["api_base"]
)
```

#### MCP Server Integration
```python
from src.utils.databricks_auth import get_auth_context

# Get auth context
auth = await get_auth_context(user_token=user_token)

# Get headers with SSE support for MCP servers
headers = auth.get_mcp_headers(include_sse=True)

# Use headers for MCP server calls
response = requests.post(mcp_url, headers=headers, json=payload)
```

### DEPRECATED: Legacy Functions (Migration Guide)

#### OLD: get_databricks_auth_headers() → NEW: get_auth_context()
```python
# OLD (still works but deprecated)
headers, error = await get_databricks_auth_headers(user_token=user_token)

# NEW (recommended)
auth = await get_auth_context(user_token=user_token)
headers = auth.get_headers()
```

#### OLD: setup_environment_variables() → NEW: get_auth_context()
```python
# OLD (causes race conditions - DO NOT USE)
setup_environment_variables(user_token=user_token)
response = litellm.completion(model="databricks/model", messages=[...])

# NEW (thread-safe, no race conditions)
auth = await get_auth_context(user_token=user_token)
params = auth.get_litellm_params()
response = litellm.completion(
    model="databricks/model",
    messages=[...],
    **params
)
```

## Token Management

### Service Principal Token Caching
- Tokens are cached with expiration tracking
- Automatic refresh 5 minutes before expiration (configurable buffer)
- Manual refresh available via `_refresh_service_token()`

### Environment Variable Cleanup
- Temporary cleanup during WorkspaceClient creation to avoid SDK conflicts
- Automatic restoration after client creation
- Critical for preventing "more than one authorization method" errors

## Configuration Sources

1. **Database** (via DatabricksService and ApiKeysService)
   - Workspace URL from databricks_config table
   - Encrypted PAT tokens from api_keys table (REQUIRED: scoped to group_id)

2. **Environment Variables** (Service Principal ONLY)
   - DATABRICKS_HOST: Workspace URL
   - DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET: OAuth credentials for Service Principal
   - **SECURITY**: DATABRICKS_TOKEN and DATABRICKS_API_KEY environment variables are NOT used

3. **Databricks SDK Auto-detection** (Workspace URL only)
   - ~/.databrickscfg profiles
   - Azure CLI authentication
   - Environment-based discovery

## Error Handling

All authentication methods return tuples of (result, error_message):
- Success: (result, None)
- Failure: (None, error_message)

This pattern allows graceful fallback and clear error reporting.

## Dependencies

- databricks.sdk: Official Databricks SDK for Python
- requests: HTTP client for manual OAuth flows
- Database services: DatabricksService, ApiKeysService
- Encryption: EncryptionUtils for secure token storage
"""

import os
import logging
import requests
import json
import contextlib
from typing import Optional, Tuple, Dict
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config

logger = logging.getLogger(__name__)


class AuthContext:
    """
    Authentication context containing all auth artifacts for a request.
    Provides token, headers, and environment management in a thread-safe manner.
    """

    def __init__(
        self,
        token: str,
        workspace_url: str,
        auth_method: str,  # "obo", "pat", "service_principal"
        user_identity: Optional[str] = None
    ):
        """
        Initialize authentication context.

        Args:
            token: Authentication token (PAT or OAuth token)
            workspace_url: Databricks workspace URL
            auth_method: Authentication method used ("obo", "pat", "service_principal")
            user_identity: Optional user identity (email or service principal ID)
        """
        self.token = token
        self.workspace_url = workspace_url.rstrip('/')
        if not self.workspace_url.startswith('https://'):
            self.workspace_url = f"https://{self.workspace_url}"
        self.auth_method = auth_method
        self.user_identity = user_identity

    def get_headers(self) -> Dict[str, str]:
        """
        Get authentication headers for REST API calls.

        Returns:
            Dictionary with Authorization and Content-Type headers
        """
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def get_mcp_headers(self, include_sse: bool = False) -> Dict[str, str]:
        """
        Get headers for MCP server calls with optional SSE headers.

        Args:
            include_sse: Whether to include Server-Sent Events headers

        Returns:
            Dictionary with MCP-appropriate headers
        """
        headers = self.get_headers()
        if include_sse:
            headers.update({
                "Accept": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive"
            })
        return headers

    def get_litellm_params(self) -> Dict[str, str]:
        """
        Get parameters for liteLLM completion calls.
        Use these parameters instead of environment variables to avoid race conditions.

        Returns:
            Dictionary with api_key and api_base for litellm.completion()
        """
        return {
            "api_key": self.token,
            "api_base": f"{self.workspace_url}/serving-endpoints"
        }

    def get_workspace_client(self) -> WorkspaceClient:
        """
        Get WorkspaceClient using this auth context.

        Returns:
            Configured WorkspaceClient instance
        """
        # Clean environment to prevent SDK conflicts
        with _clean_environment():
            return WorkspaceClient(
                host=self.workspace_url,
                token=self.token
            )

    def __repr__(self) -> str:
        return f"AuthContext(method={self.auth_method}, user={self.user_identity or 'service'})"


@contextlib.contextmanager
def _clean_environment():
    """
    Context manager to temporarily clean Databricks environment variables.
    This prevents SDK conflicts when creating clients with explicit credentials.
    """
    old_env = {}
    env_vars_to_clean = [
        "DATABRICKS_TOKEN",
        "DATABRICKS_API_KEY",
        "DATABRICKS_CLIENT_ID",
        "DATABRICKS_CLIENT_SECRET",
        "DATABRICKS_CONFIG_FILE",
        "DATABRICKS_CONFIG_PROFILE"
    ]

    for var in env_vars_to_clean:
        if var in os.environ:
            old_env[var] = os.environ.pop(var)

    try:
        yield
    finally:
        # Restore environment variables
        os.environ.update(old_env)


class DatabricksAuth:
    """Enhanced Databricks authentication class supporting PAT and OAuth OBO."""

    def __init__(self):
        self._api_token: Optional[str] = None
        self._workspace_host: Optional[str] = None
        self._config_loaded = False
        self._user_access_token: Optional[str] = None
        self._client_id: Optional[str] = None
        self._client_secret: Optional[str] = None
        # Token expiration tracking
        self._service_token: Optional[str] = None
        self._service_token_fetched_at: Optional[float] = None
        self._service_token_expires_in: int = 3600  # Default 1 hour
        self._token_refresh_buffer: int = 300  # Refresh 5 minutes before expiration

    async def _load_config(self) -> bool:
        """Load configuration from services if not already loaded."""
        if self._config_loaded:
            return True

        try:
            # First, check for OAuth environment variables (for service principal auth)
            self._check_oauth_environment()

            # Try to load workspace host from database configuration
            try:
                from src.services.databricks_service import DatabricksService
                from src.db.session import async_session_factory

                async with async_session_factory() as session:
                    service = DatabricksService(session)

                    try:
                        config = await service.get_databricks_config()
                        if config and config.workspace_url and not self._workspace_host:
                            self._workspace_host = config.workspace_url.rstrip('/')
                            if not self._workspace_host.startswith('https://'):
                                self._workspace_host = f"https://{self._workspace_host}"
                            logger.info(f"Loaded workspace host from database: {self._workspace_host}")

                    except Exception as e:
                        logger.warning(f"Failed to get databricks config from database: {e}")

            except Exception as e:
                logger.warning(f"Could not load database configuration: {e}")

            # Try SDK auto-detection for workspace host if not set
            if not self._workspace_host:
                try:
                    sdk_config = Config()
                    if sdk_config.host:
                        self._workspace_host = sdk_config.host
                        logger.info(f"Auto-detected workspace host: {self._workspace_host}")
                except Exception as e:
                    logger.debug(f"SDK auto-detection failed: {e}")

            # NOTE: PAT token loading is now done per-request in get_auth_context()
            # This is because we need the group_id from UserContext, which is only
            # available during request processing, not during initialization

            self._config_loaded = True
            return True

        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            return False

    def _check_oauth_environment(self):
        """Check for OAuth credentials in environment variables (for service principal auth)."""
        try:
            # Check for OAuth environment variables
            client_id = os.environ.get('DATABRICKS_CLIENT_ID')
            client_secret = os.environ.get('DATABRICKS_CLIENT_SECRET')
            databricks_host = os.environ.get('DATABRICKS_HOST')

            # If we have OAuth credentials, store them for SPN auth
            if client_id and client_secret:
                self._client_id = client_id
                self._client_secret = client_secret
                logger.info("Found OAuth credentials for service principal authentication")

            # Set workspace host from environment if available
            if databricks_host:
                if not databricks_host.startswith('https://'):
                    databricks_host = f"https://{databricks_host}"
                self._workspace_host = databricks_host.rstrip('/')
                logger.info(f"Using workspace host from environment: {self._workspace_host}")

        except Exception as e:
            logger.debug(f"Error checking OAuth environment: {e}")

    def set_user_access_token(self, user_token: str):
        """Set user access token for OBO authentication."""
        self._user_access_token = user_token
        logger.info("User access token set for OBO authentication")

    def _is_service_token_expired(self) -> bool:
        """Check if the cached service principal token is expired or about to expire."""
        if not self._service_token or not self._service_token_fetched_at:
            return True

        import time
        elapsed = time.time() - self._service_token_fetched_at
        # Consider token expired if it's within the refresh buffer of actual expiration
        return elapsed >= (self._service_token_expires_in - self._token_refresh_buffer)

    async def _refresh_service_token(self) -> Optional[str]:
        """Refresh the service principal OAuth token."""
        logger.info("Refreshing service principal OAuth token")
        try:
            # Get a new token using the service principal credentials
            new_token = await self._get_service_principal_token()
            if new_token:
                self._service_token = new_token
                import time
                self._service_token_fetched_at = time.time()
                logger.info("Service principal token refreshed successfully")
                return new_token
            else:
                logger.error("Failed to refresh service principal token")
                return None
        except Exception as e:
            logger.error(f"Error refreshing service principal token: {e}")
            return None

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

            # Use unified authentication fallback chain: OBO → PAT → SPN
            return await self._get_unified_auth_headers(mcp_server_url)

        except Exception as e:
            logger.error(f"Error getting auth headers: {e}")
            return None, str(e)

    async def _get_unified_auth_headers(self, mcp_server_url: str = None) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
        """
        Get authentication headers using unified fallback chain.
        Tries in order: OBO (user token) → PAT → SPN (service principal)
        """
        try:
            # Method 1: Try OBO (On-Behalf-Of) with user access token
            if self._user_access_token:
                logger.info("Using OBO authentication with user access token")
                return self._create_bearer_headers(self._user_access_token, mcp_server_url)

            # Method 2: Try PAT (Personal Access Token)
            if self._api_token:
                logger.info("Using PAT authentication")
                # Validate token before using
                if await self._validate_token():
                    return self._create_bearer_headers(self._api_token, mcp_server_url)
                else:
                    logger.warning("PAT validation failed, trying next auth method")

            # Method 3: Try SPN (Service Principal) with OAuth
            if self._client_id and self._client_secret:
                logger.info("Using service principal OAuth authentication")
                # Check if cached service token is expired
                if self._is_service_token_expired():
                    logger.info("Service token expired or not cached, refreshing...")
                    token = await self._refresh_service_token()
                    if token:
                        return self._create_bearer_headers(token, mcp_server_url)
                    else:
                        logger.warning("Failed to obtain service principal OAuth token")
                else:
                    logger.info("Using cached service principal OAuth token")
                    return self._create_bearer_headers(self._service_token, mcp_server_url)

            # No authentication method available
            logger.error("No authentication method available (tried OBO, PAT, SPN)")
            return None, "No Databricks authentication method available"

        except Exception as e:
            logger.error(f"Error in unified auth: {e}")
            return None, str(e)

    def _create_bearer_headers(self, token: str, mcp_server_url: str = None) -> Tuple[Dict[str, str], None]:
        """Create Bearer token headers with optional MCP server headers."""
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
                    import time
                    self._service_token = auth_result.access_token
                    self._service_token_fetched_at = time.time()
                    # Try to get expires_in from response, default to 3600 (1 hour)
                    if hasattr(auth_result, 'expires_in'):
                        self._service_token_expires_in = auth_result.expires_in
                    logger.info(f"Successfully obtained service principal OAuth token (expires in {self._service_token_expires_in}s)")
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
            # Request comprehensive scopes matching deploy.py OAuth configuration
            # This ensures Service Principal has access to all required APIs including Unity Catalog
            scopes = [
                'sql',
                'sql.alerts',
                'sql.alerts-legacy',
                'sql.dashboards',
                'sql.data-sources',
                'sql.dbsql-permissions',
                'sql.queries',
                'sql.queries-legacy',
                'sql.query-history',
                'sql.statement-execution',
                'sql.warehouses',
                'vectorsearch.vector-search-endpoints',
                'vectorsearch.vector-search-indexes',
                'serving.serving-endpoints',
                'files.files',
                'dashboards.genie',
                'catalog.connections',
                'catalog.catalogs:read',
                'catalog.tables:read',
                'catalog.schemas:read',
            ]
            data = {
                'grant_type': 'client_credentials',
                'scope': ' '.join(scopes)
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
                    import time
                    self._service_token = access_token
                    self._service_token_fetched_at = time.time()
                    # Get expires_in from response, default to 3600 (1 hour)
                    self._service_token_expires_in = token_data.get('expires_in', 3600)
                    logger.info(f"Successfully obtained OAuth token via manual flow (expires in {self._service_token_expires_in}s)")
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
    
    async def get_workspace_url(self) -> Optional[str]:
        """Get the workspace URL (async version of get_workspace_host)."""
        if not self._config_loaded:
            await self._load_config()
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
        
        # Check if there's already a running event loop
        try:
            loop = asyncio.get_running_loop()
            # If we're already in an async context, we can't use asyncio.run()
            # This shouldn't happen in a sync function, but let's handle it gracefully
            logger.warning("Sync function called from async context - returning error")
            return None, "Cannot call sync version from async context"
        except RuntimeError:
            # No running loop, safe to create one
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


def setup_environment_variables(user_token: Optional[str] = None) -> bool:
    """
    DEPRECATED: Set up Databricks environment variables for compatibility with other libraries.

    **WARNING**: This function causes race conditions in multi-threaded environments.
    Process-wide environment variables (os.environ) are NOT thread-safe.

    **Use get_auth_context() instead**:
    - For liteLLM: Use auth.get_litellm_params() and pass api_key/api_base directly
    - For WorkspaceClient: Use auth.get_workspace_client()
    - For REST APIs: Use auth.get_headers()

    **MLflow**: Configure ONCE at application startup with service-level credentials,
    not per-request with user tokens.

    Args:
        user_token: Optional user access token for OBO authentication

    Returns:
        bool: True if successful, False otherwise

    Migration Example:
        # OLD (race condition):
        setup_environment_variables(user_token=user_token)
        response = litellm.completion(model="databricks/model", messages=[...])

        # NEW (thread-safe):
        auth = await get_auth_context(user_token=user_token)
        params = auth.get_litellm_params()
        response = litellm.completion(model="databricks/model", messages=[...], **params)
    """
    import warnings
    warnings.warn(
        "setup_environment_variables() is deprecated and causes race conditions. "
        "Use get_auth_context() instead. "
        "See module docstring for migration examples.",
        DeprecationWarning,
        stacklevel=2
    )

    try:
        import asyncio

        async def _setup():
            if not await _databricks_auth._load_config():
                return False

            # Set environment variables for host first
            if _databricks_auth._workspace_host:
                os.environ["DATABRICKS_HOST"] = _databricks_auth._workspace_host
                # Also set API_BASE for LiteLLM compatibility - must include /serving-endpoints
                os.environ["DATABRICKS_API_BASE"] = f"{_databricks_auth._workspace_host}/serving-endpoints"

            # Prefer OBO user token if provided
            if user_token:
                try:
                    _databricks_auth.set_user_access_token(user_token)
                except Exception:
                    # Best-effort; still export token to env for SDKs
                    pass
                os.environ["DATABRICKS_TOKEN"] = user_token
                os.environ["DATABRICKS_API_KEY"] = user_token
            elif _databricks_auth._api_token:
                # Fall back to service PAT
                os.environ["DATABRICKS_TOKEN"] = _databricks_auth._api_token
                os.environ["DATABRICKS_API_KEY"] = _databricks_auth._api_token

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




async def get_auth_context(
    user_token: Optional[str] = None
) -> Optional[AuthContext]:
    """
    Get authentication context with token, headers, and environment.
    This is the UNIFIED authentication function - use this for all Databricks authentication.

    Authentication Priority Chain (applies to ALL services):
    1. OBO (On-Behalf-Of) - User token from request headers (if provided)
    2. PAT (Personal Access Token) - From API Keys Service with group_id
    3. SPN (Service Principal) - OAuth with client credentials

    Args:
        user_token: Optional user access token for OBO authentication.
                   If None, skips OBO and uses PAT/SPN (service-level auth).

    Returns:
        AuthContext with all authentication artifacts, or None if auth failed

    Examples:
        # With OBO (user-specific operations)
        auth = await get_auth_context(user_token=user_token)
        headers = auth.get_headers()

        # Service-level (no OBO, uses PAT/SPN)
        auth = await get_auth_context(user_token=None)
        client = auth.get_workspace_client()

        # liteLLM integration (thread-safe)
        auth = await get_auth_context(user_token=user_token)
        params = auth.get_litellm_params()
        response = litellm.completion(
            model="databricks/model",
            messages=[...],
            **params
        )
    """
    try:
        logger.debug(f"get_auth_context called with user_token={'SET' if user_token else 'None'}")

        # Load configuration
        if not await _databricks_auth._load_config():
            logger.error("Failed to load Databricks configuration")
            return None

        # Ensure workspace host is set
        if not _databricks_auth._workspace_host:
            logger.error("No workspace host available after config load")
            return None

        workspace_url = _databricks_auth._workspace_host

        # Unified authentication chain: OBO → PAT → SPN (applies to ALL services)
        logger.info("[AUTH] Starting unified authentication chain: OBO → PAT → SPN")

        # Priority 1: Try OBO (On-Behalf-Of) if user token provided
        if user_token:
            logger.info("[AUTH] Priority 1: Attempting OBO authentication with user token")
            user_identity = None
            try:
                with _clean_environment():
                    client = WorkspaceClient(host=workspace_url, token=user_token)
                    current_user = client.current_user.me()
                    user_identity = getattr(current_user, 'user_name', None) or \
                                  getattr(current_user, 'application_id', None)
                logger.info(f"[AUTH] Priority 1: ✓ OBO authentication successful for user: {user_identity}")
                return AuthContext(
                    token=user_token,
                    workspace_url=workspace_url,
                    auth_method="obo",
                    user_identity=user_identity
                )
            except Exception as e:
                logger.warning(f"[AUTH] Priority 1: ✗ OBO authentication failed: {e}")
                logger.info("[AUTH] Falling back to PAT/SPN authentication")
        else:
            logger.info("[AUTH] Priority 1: No user token provided, skipping OBO")

        # Priority 2: Try PAT from ApiKeysService (per-request lookup with group_id)
        # SECURITY: PAT tokens must be loaded with group_id for multi-tenant isolation
        logger.info("[AUTH] Priority 2: Attempting PAT authentication from API Keys Service")
        pat_token = None
        try:
            from src.services.api_keys_service import ApiKeysService
            from src.db.session import async_session_factory
            from src.utils.user_context import UserContext

            # Get group_id from UserContext for multi-tenant isolation
            group_id = None
            try:
                group_context = UserContext.get_group_context()
                logger.info(f"[AUTH PAT] Retrieved group_context: {group_context}")
                if group_context and hasattr(group_context, 'primary_group_id'):
                    group_id = group_context.primary_group_id
                    logger.info(f"[AUTH PAT] ✓ Using group_id from UserContext: {group_id}")
                else:
                    logger.warning(f"[AUTH PAT] ✗ No group_context or primary_group_id. group_context={group_context}")
            except Exception as e:
                logger.warning(f"[AUTH PAT] ✗ Could not get group_id from UserContext: {e}")

            if group_id:
                logger.info(f"[AUTH PAT] Attempting to load PAT from database with group_id={group_id}")
                logger.info("[AUTH PAT] ABOUT TO CREATE async_session_factory() - if hang occurs, it's here")
                async with async_session_factory() as session:
                    logger.info("[AUTH PAT] async_session_factory() created successfully")
                    api_service = ApiKeysService(session, group_id=group_id)

                    for key_name in ["DATABRICKS_TOKEN", "DATABRICKS_API_KEY"]:
                        try:
                            logger.info(f"[AUTH PAT] Looking for key: {key_name}")
                            api_key = await api_service.find_by_name(key_name)
                            logger.info(f"[AUTH PAT] Key {key_name} found: {api_key is not None}")
                            if api_key and api_key.encrypted_value:
                                from src.utils.encryption_utils import EncryptionUtils
                                pat_token = EncryptionUtils.decrypt_value(api_key.encrypted_value)
                                logger.info(f"[AUTH] Priority 2: ✓ PAT loaded from database ({key_name}) with group_id={group_id}")
                                break
                        except Exception as e:
                            logger.warning(f"[AUTH PAT] Error loading {key_name}: {e}")

                if not pat_token:
                    logger.warning(f"[AUTH] Priority 2: ✗ No PAT found in database with group_id={group_id}")
            else:
                logger.warning("[AUTH] Priority 2: ✗ No group_id available for PAT lookup")

        except Exception as e:
            logger.error(f"[AUTH PAT] Error during PAT lookup: {e}")

        if pat_token:
            return AuthContext(
                token=pat_token,
                workspace_url=workspace_url,
                auth_method="pat",
                user_identity=None
            )
        else:
            logger.warning("[AUTH] Priority 2: ✗ No PAT token available")

        # Priority 3: Try Service Principal OAuth as last resort
        if _databricks_auth._client_id and _databricks_auth._client_secret:
            logger.info("[AUTH] Priority 3: Attempting Service Principal OAuth authentication")
            if _databricks_auth._is_service_token_expired():
                token = await _databricks_auth._refresh_service_token()
            else:
                token = _databricks_auth._service_token

            if token:
                logger.info("[AUTH] Priority 3: ✓ Service Principal authentication successful")
                return AuthContext(
                    token=token,
                    workspace_url=workspace_url,
                    auth_method="service_principal",
                    user_identity=None
                )
            else:
                logger.error("[AUTH] Priority 3: ✗ Service Principal authentication failed")
        else:
            logger.info("[AUTH] Priority 3: Service Principal credentials not configured, skipping SPN")

        # No authentication method available
        logger.error("No authentication method available")
        logger.error("Please configure at least one authentication method:")
        logger.error("  1. OBO: User token from request headers (automatic when authenticated)")
        logger.error("  2. PAT: Configure via API Keys Service (Configuration → API Keys)")
        logger.error("  3. SPN: Configure via Databricks authentication settings")
        return None

    except Exception as e:
        logger.error(f"Error getting auth context: {e}")
        return None


def is_scope_error(error: Exception) -> bool:
    """
    Check if an error is due to missing OAuth scopes.

    OBO tokens may lack required scopes for certain operations (e.g., volume creation).
    PAT tokens inherit full user permissions and don't have scope limitations.

    Args:
        error: Exception to check

    Returns:
        True if error is due to missing OAuth scopes
    """
    error_str = str(error).lower()
    return any(phrase in error_str for phrase in [
        "does not have required scopes",
        "required scopes",
        "insufficient scopes",
        "missing scopes"
    ])


async def get_workspace_client(user_token: Optional[str] = None) -> Optional[WorkspaceClient]:
    """
    Get a Databricks WorkspaceClient with appropriate authentication.

    This is a convenience wrapper around get_auth_context().get_workspace_client()

    Unified Authentication Priority Chain (applies to ALL services):
    1. OBO (On-Behalf-Of) - User token from request headers (if provided)
    2. PAT (Personal Access Token) - From API Keys Service with group_id
    3. SPN (Service Principal) - OAuth with client credentials

    Args:
        user_token: Optional user access token for OBO authentication.
                   If None, skips OBO and uses PAT/SPN (service-level auth).

    Returns:
        Optional[WorkspaceClient]: Configured workspace client

    Examples:
        # With OBO (user-specific operations)
        client = await get_workspace_client(user_token=user_token)

        # Service-level (no OBO, uses PAT/SPN)
        client = await get_workspace_client(user_token=None)
    """
    try:
        # Get authentication context using unified chain
        auth = await get_auth_context(user_token=user_token)
        if not auth:
            logger.error("Failed to get authentication context")
            return None

        # Return WorkspaceClient from auth context
        return auth.get_workspace_client()

    except Exception as e:
        logger.error(f"Error creating workspace client: {e}")
        return None


async def get_workspace_client_with_fallback(
    user_token: Optional[str] = None,
    operation_name: str = "operation"
) -> tuple[Optional[WorkspaceClient], Optional[str]]:
    """
    Get a Databricks WorkspaceClient with automatic PAT fallback capability.

    Returns both the client and the user_token to use. If OBO is attempted,
    the caller should catch scope errors and call this again with force_pat=True.

    This is used by operations that may require scopes not available in OBO tokens
    (e.g., volume operations, catalog management).

    Args:
        user_token: Optional user access token for OBO authentication
        operation_name: Name of operation for logging

    Returns:
        Tuple of (WorkspaceClient, effective_user_token) where effective_user_token
        is the token that should be passed to retry if scope errors occur
    """
    try:
        auth = await get_auth_context(user_token=user_token)
        if not auth:
            logger.error(f"[{operation_name}] Failed to get authentication context")
            return None, None

        client = auth.get_workspace_client()

        # Return the client and the user_token for potential retry
        # If auth is OBO, return the user_token so caller knows to retry with None
        # If auth is PAT/SPN, return None to indicate no retry needed
        effective_token = user_token if auth.auth_method == "obo" else None

        return client, effective_token

    except Exception as e:
        logger.error(f"[{operation_name}] Error creating workspace client: {e}")
        return None, None


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


async def get_current_databricks_user(user_token: Optional[str] = None) -> Tuple[Optional[str], Optional[str]]:
    """
    Get the current Databricks user identity (email or service principal ID).
    This centralizes the logic for determining the authenticated user.

    Args:
        user_token: Optional user access token for OBO authentication

    Returns:
        Tuple[Optional[str], Optional[str]]: (user_identity, error_message)
        The user_identity will be the email for users or application_id for service principals
    """
    try:
        # Get or create a workspace client with appropriate authentication
        workspace_client = await get_workspace_client(user_token=user_token)
        if not workspace_client:
            return None, "Failed to create workspace client"

        # Get current user identity
        try:
            current_user = workspace_client.current_user.me()
            if current_user:
                # For regular users, use user_name (email)
                if hasattr(current_user, 'user_name') and current_user.user_name:
                    logger.info(f"Identified Databricks user: {current_user.user_name}")
                    return current_user.user_name, None
                # For service principals, use application_id
                elif hasattr(current_user, 'applicationId') and getattr(current_user, 'applicationId'):
                    app_id = getattr(current_user, 'applicationId')
                    logger.info(f"Identified Databricks service principal: {app_id}")
                    return app_id, None
                # For display name fallback
                elif hasattr(current_user, 'display_name') and current_user.display_name:
                    logger.info(f"Using display name as identity: {current_user.display_name}")
                    return current_user.display_name, None
                else:
                    return None, "Could not determine user identity from current_user object"
            else:
                return None, "current_user.me() returned None"

        except Exception as e:
            logger.error(f"Error getting current user identity: {e}")
            return None, f"Failed to get current user: {str(e)}"

    except Exception as e:
        logger.error(f"Error in get_current_databricks_user: {e}")
        return None, str(e)


async def get_mcp_auth_headers(
    mcp_server_url: str, 
    user_token: Optional[str] = None,
    api_key: Optional[str] = None,
    include_sse_headers: bool = False
) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
    """
    Get authentication headers for MCP server calls.
    Tries authentication methods in order:
    1. OBO (On-Behalf-Of) using user token
    2. API key from service
    3. Databricks CLI token
    
    Args:
        mcp_server_url: MCP server URL
        user_token: Optional user access token for OBO
        api_key: Optional API key from the service
        include_sse_headers: Whether to include SSE-specific headers (for SSE endpoints only)
        
    Returns:
        Tuple[Optional[Dict[str, str]], Optional[str]]: Headers dict and error message if any
    """
    try:
        access_token = None
        
        # First try: OBO authentication if user token is provided
        if user_token:
            logger.info("Attempting OBO authentication for MCP")
            try:
                # Initialize DatabricksAuth instance for OBO
                auth = DatabricksAuth()
                auth.set_user_access_token(user_token)
                headers, auth_error = await auth.get_auth_headers(mcp_server_url=mcp_server_url)
                
                # If OBO authentication succeeded, use those headers
                if headers and not auth_error:
                    # Only add SSE headers if specifically requested (for SSE endpoints)
                    if include_sse_headers:
                        if "Content-Type" not in headers:
                            headers["Content-Type"] = "application/json"
                        if "Accept" not in headers:
                            headers["Accept"] = "text/event-stream"
                        if "Cache-Control" not in headers:
                            headers["Cache-Control"] = "no-cache"
                        if "Connection" not in headers:
                            headers["Connection"] = "keep-alive"
                    
                    logger.info("Successfully using OBO authentication for MCP")
                    return headers, None
                else:
                    logger.warning(f"OBO authentication failed: {auth_error}")
                    # Continue to next authentication method
            except Exception as obo_error:
                logger.warning(f"Error in OBO authentication: {obo_error}")
                # Continue to next authentication method
        
        # Second try: API key from service if provided
        if api_key:
            logger.info("Using API key authentication for MCP")
            headers = {"Authorization": f"Bearer {api_key}"}
            
            # Only add SSE headers if specifically requested
            if include_sse_headers:
                headers.update({
                    "Content-Type": "application/json",
                    "Accept": "text/event-stream",
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive"
                })
            
            return headers, None
        
        # Third try: Get token from Databricks CLI
        logger.info("Falling back to Databricks CLI authentication for MCP")
        access_token, error = await get_mcp_access_token()
        if error:
            return None, error
            
        # Return clean headers by default, add SSE headers only if requested
        headers = {"Authorization": f"Bearer {access_token}"}
        
        # Only add SSE headers if specifically requested (for SSE endpoints)
        if include_sse_headers:
            headers.update({
                "Content-Type": "application/json",
                "Accept": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive"
            })
        
        return headers, None
        
    except Exception as e:
        logger.error(f"Error getting MCP auth headers: {e}")
        return None, str(e)