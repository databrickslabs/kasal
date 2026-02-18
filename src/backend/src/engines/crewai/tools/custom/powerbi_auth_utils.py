"""
Shared Power BI Authentication Utilities for CrewAI Tools

Provides centralized authentication using AadService from the converter module.
All Power BI tools should use these utilities for consistent authentication.

Supports three authentication methods:
1. Pre-obtained access_token (User OAuth)
2. Service Principal (client_id + client_secret + tenant_id)
3. Service Account (username + password + client_id + tenant_id)
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Thread pool for running sync operations in async context
_AUTH_EXECUTOR = ThreadPoolExecutor(max_workers=3)


def _run_sync_in_thread(func, *args, **kwargs):
    """Run a synchronous function in a thread pool."""
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(_AUTH_EXECUTOR, lambda: func(*args, **kwargs))


async def get_powerbi_access_token(
    tenant_id: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    access_token: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    username_env: Optional[str] = None,
    password_env: Optional[str] = None,
    auth_method: Optional[str] = None,
) -> str:
    """
    Get Power BI access token using centralized AadService.

    This is the primary entry point for all Power BI tools to get authentication tokens.
    Uses the same authentication logic as the converter module.

    Args:
        tenant_id: Azure AD tenant ID
        client_id: Azure AD application (client) ID
        client_secret: Client secret for Service Principal auth
        access_token: Pre-obtained access token (bypasses all other auth)
        username: Service account username/UPN
        password: Service account password
        username_env: Environment variable name for username
        password_env: Environment variable name for password
        auth_method: Explicit auth method: 'service_principal', 'service_account', or auto-detect

    Returns:
        str: Access token for Power BI API

    Raises:
        ValueError: If credentials are missing or invalid
        RuntimeError: If azure-identity library not available
        Exception: If token acquisition fails
    """
    # Import here to avoid circular imports
    from src.converters.services.powerbi.authentication import AadService

    # Create AadService with all parameters
    aad_service = AadService(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        access_token=access_token,
        username=username,
        password=password,
        username_env=username_env,
        password_env=password_env,
        auth_method=auth_method,
        logger=logger,
    )

    # Run the synchronous get_access_token in a thread pool
    # This prevents blocking the event loop
    token = await _run_sync_in_thread(aad_service.get_access_token)
    return token


async def get_powerbi_access_token_from_config(config: Dict[str, Any]) -> str:
    """
    Get Power BI access token from a config dictionary.

    Convenience wrapper that extracts parameters from a config dict.
    Useful for tools that receive configuration as a dictionary.

    Args:
        config: Dictionary containing authentication parameters

    Returns:
        str: Access token for Power BI API
    """
    return await get_powerbi_access_token(
        tenant_id=config.get("tenant_id"),
        client_id=config.get("client_id"),
        client_secret=config.get("client_secret"),
        access_token=config.get("access_token"),
        username=config.get("username"),
        password=config.get("password"),
        username_env=config.get("username_env"),
        password_env=config.get("password_env"),
        auth_method=config.get("auth_method"),
    )


async def get_fabric_access_token(
    tenant_id: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    auth_method: Optional[str] = None,
) -> str:
    """
    Get Fabric API access token for TMDL/definition access.

    Uses the Fabric API scope instead of Power BI API scope.
    Supports both Service Principal and Service Account authentication.

    Args:
        tenant_id: Azure AD tenant ID
        client_id: Azure AD application (client) ID
        client_secret: Client secret for Service Principal auth
        username: Service account username/UPN
        password: Service account password
        auth_method: Explicit auth method: 'service_principal', 'service_account', or auto-detect

    Returns:
        str: Access token for Fabric API
    """
    import httpx

    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    # Determine authentication method
    if auth_method == "service_account" or (username and password and not client_secret):
        # Service Account (ROPC flow)
        data = {
            "grant_type": "password",
            "client_id": client_id,
            "username": username,
            "password": password,
            "scope": "https://api.fabric.microsoft.com/.default"
        }
    else:
        # Service Principal (client credentials)
        data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://api.fabric.microsoft.com/.default"
        }

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(url, data=data)
        response.raise_for_status()
        return response.json()["access_token"]


async def get_fabric_access_token_from_config(config: Dict[str, Any]) -> str:
    """
    Get Fabric API access token from a config dictionary.

    Args:
        config: Dictionary containing authentication parameters

    Returns:
        str: Access token for Fabric API
    """
    return await get_fabric_access_token(
        tenant_id=config.get("tenant_id"),
        client_id=config.get("client_id"),
        client_secret=config.get("client_secret"),
        username=config.get("username"),
        password=config.get("password"),
        auth_method=config.get("auth_method"),
    )


def validate_auth_config(config: Dict[str, Any]) -> tuple[bool, str]:
    """
    Validate that a config dictionary has sufficient authentication credentials.

    Args:
        config: Dictionary containing authentication parameters

    Returns:
        Tuple of (is_valid, error_message)
    """
    has_access_token = bool(config.get("access_token"))
    has_service_principal = all([
        config.get("tenant_id"),
        config.get("client_id"),
        config.get("client_secret")
    ])
    has_service_account = all([
        config.get("tenant_id"),
        config.get("client_id"),
        config.get("username"),
        config.get("password")
    ])

    if has_access_token or has_service_principal or has_service_account:
        return True, ""

    return False, (
        "Authentication required. Provide one of:\n"
        "- access_token (User OAuth)\n"
        "- tenant_id, client_id, client_secret (Service Principal)\n"
        "- tenant_id, client_id, username, password (Service Account)"
    )
