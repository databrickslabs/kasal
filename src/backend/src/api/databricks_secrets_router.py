"""
Router for handling Databricks secrets.

This module provides a router for Databricks secrets CRUD operations.
"""

import logging
import os
from typing import Annotated, Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Path, status

from src.core.exceptions import BadRequestError, KasalError, NotFoundError

from src.core.dependencies import GroupContextDep, SessionDep
from src.schemas.databricks_secret import (
    DatabricksTokenRequest,
    SecretCreate,
    SecretResponse,
    SecretUpdate,
)
from src.services.databricks_secrets_service import DatabricksSecretsService

router = APIRouter(
    prefix="/databricks-secrets",
    tags=["databricks-secrets"],
    responses={404: {"description": "Not found"}},
)

# Set up logging
logger = logging.getLogger(__name__)


def get_databricks_secrets_service(session: SessionDep) -> DatabricksSecretsService:
    """
    Dependency provider for DatabricksSecretsService.

    Creates service with session following the pattern:
    Router → Service → Repository → DB

    Args:
        session: Database session from FastAPI DI

    Returns:
        DatabricksSecretsService instance with session
    """
    return DatabricksSecretsService(session)


# Type alias for cleaner function signatures
DatabricksSecretsServiceDep = Annotated[
    DatabricksSecretsService, Depends(get_databricks_secrets_service)
]


@router.get("", response_model=List[SecretResponse])
async def get_databricks_secrets(
    group_context: GroupContextDep,
    service: DatabricksSecretsServiceDep,
):
    """
    Get all secrets from Databricks secret store.

    Args:
        service: Secret service injected by dependency

    Returns:
        List of secrets
    """
    # Get secrets from Databricks if configured
    databricks_secrets = []
    try:
        config = await service.databricks_service.get_databricks_config()
        if (
            config
            and config.is_enabled
            and config.workspace_url
            and config.secret_scope
        ):
            # Verify token is available via unified auth
            try:
                from src.utils.databricks_auth import get_auth_context

                auth = await get_auth_context()
                if auth and auth.token:
                    # Get secrets list from Databricks
                    databricks_results = await service.get_databricks_secrets(
                        config.secret_scope
                    )
                    # Return the results directly
                    return databricks_results
            except Exception as e:
                logger.warning(f"Failed to get unified auth for secrets: {e}")
    except Exception as e:
        logger.warning(f"Error getting Databricks secrets: {str(e)}")

    return []


@router.post("", response_model=SecretResponse, status_code=status.HTTP_201_CREATED)
async def create_databricks_secret(
    secret_data: SecretCreate,
    group_context: GroupContextDep,
    service: DatabricksSecretsServiceDep,
):
    """
    Create a new secret in Databricks.

    Args:
        secret_data: Secret data for creation
        service: Secret service injected by dependency

    Returns:
        Created secret
    """
    # Try to store in Databricks
    config = await service.databricks_service.get_databricks_config()
    if config and config.is_enabled and config.workspace_url and config.secret_scope:
        # Set secret in Databricks
        success = await service.set_databricks_secret_value(
            config.secret_scope, secret_data.name, secret_data.value
        )

        if success:
            # Create a response object
            return {
                "id": 1000,  # Use a high ID to avoid conflicts
                "name": secret_data.name,
                "value": secret_data.value,
                "description": secret_data.description or "",
                "scope": config.secret_scope,
                "source": "databricks",
            }
        else:
            raise KasalError("Failed to create secret in Databricks")
    else:
        # Databricks not configured
        raise BadRequestError("Databricks not properly configured for secret storage")


@router.put("/{secret_name}", response_model=SecretResponse)
async def update_databricks_secret(
    secret_name: str,
    secret_data: SecretUpdate,
    group_context: GroupContextDep,
    service: DatabricksSecretsServiceDep,
):
    """
    Update an existing secret in Databricks.

    Args:
        secret_name: Name of the secret to update
        secret_data: Secret data for update
        service: Secret service injected by dependency

    Returns:
        Updated secret
    """
    # Log the request for debugging
    logger.info(f"Attempting to update Databricks secret: {secret_name}")

    # Try to update in Databricks
    config = await service.databricks_service.get_databricks_config()
    if config and config.is_enabled and config.workspace_url and config.secret_scope:
        success = await service.set_databricks_secret_value(
            config.secret_scope, secret_name, secret_data.value
        )

        if success:
            # Return updated secret
            logger.info(f"Secret updated in Databricks: {secret_name}")
            return {
                "id": 1000,  # Use a high ID to avoid conflicts
                "name": secret_name,
                "value": secret_data.value,
                "description": secret_data.description or "",
                "scope": config.secret_scope,
                "source": "databricks",
            }
        else:
            error_msg = f"Failed to update secret '{secret_name}' in Databricks"
            logger.error(error_msg)
            raise KasalError(error_msg)
    else:
        # Databricks not configured
        error_msg = "Databricks not properly configured for secret storage"
        logger.error(error_msg)
        raise BadRequestError(error_msg)


@router.delete("/{secret_name}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_databricks_secret(
    secret_name: str,
    group_context: GroupContextDep,
    service: DatabricksSecretsServiceDep,
):
    """
    Delete a secret from Databricks.

    Args:
        secret_name: Name of the secret to delete
        service: Secret service injected by dependency
    """
    # Try to delete from Databricks
    config = await service.databricks_service.get_databricks_config()
    if config and config.is_enabled and config.workspace_url and config.secret_scope:
        success = await service.delete_databricks_secret(
            config.secret_scope, secret_name
        )

        if not success:
            raise NotFoundError(f"Secret '{secret_name}' not found in Databricks")
    else:
        # Databricks not configured
        raise BadRequestError("Databricks not properly configured for secret storage")


@router.post("/scopes", status_code=status.HTTP_200_OK)
async def create_databricks_secret_scope(
    group_context: GroupContextDep,
    service: DatabricksSecretsServiceDep,
):
    """
    Create a secret scope in Databricks if it doesn't exist.

    Args:
        service: Secret service injected by dependency

    Returns:
        Success status
    """
    config = await service.databricks_service.get_databricks_config()
    if (
        not config
        or not config.is_enabled
        or not config.workspace_url
        or not config.secret_scope
    ):
        raise BadRequestError("Databricks not properly configured")

    # Get token from unified auth
    from src.utils.databricks_auth import get_auth_context

    auth = await get_auth_context()
    if not auth or not auth.token:
        raise BadRequestError("No Databricks authentication available")

    success = await service.create_databricks_secret_scope(
        config.workspace_url, auth.token, config.secret_scope
    )

    if success:
        return {
            "status": "success",
            "message": f"Scope '{config.secret_scope}' created or already exists",
        }
    else:
        raise KasalError(f"Failed to create scope '{config.secret_scope}'")


# Legacy routes for backward compatibility (matching old routing paths)
@router.get("/secrets", response_model=List[Dict])
async def get_secrets(
    group_context: GroupContextDep,
    service: DatabricksSecretsServiceDep,
):
    """Legacy endpoint for getting all secrets from a specific Databricks scope."""
    try:
        workspace_url, scope = await service.validate_databricks_config()
        secrets_list = await service.get_databricks_secrets(scope)
        if secrets_list is None:
            return []
        return secrets_list
    except Exception as e:
        logger.error(f"Error getting secrets: {str(e)}")
        return []


@router.put("/secrets/{key}", status_code=status.HTTP_200_OK)
async def set_secret(
    key: str,
    secret_data: SecretUpdate,
    group_context: GroupContextDep,
    service: DatabricksSecretsServiceDep,
):
    """Legacy endpoint for setting a secret value in Databricks."""
    workspace_url, scope = await service.validate_databricks_config()
    success = await service.set_databricks_secret_value(scope, key, secret_data.value)
    if success:
        return {
            "status": "success",
            "message": f"Secret '{key}' set in scope '{scope}'",
        }
    else:
        raise KasalError(f"Failed to set secret '{key}'")


@router.delete("/secrets/{key}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_secret_endpoint(
    key: str,
    group_context: GroupContextDep,
    service: DatabricksSecretsServiceDep,
):
    """Legacy endpoint for deleting a secret from Databricks."""
    workspace_url, scope = await service.validate_databricks_config()
    success = await service.delete_databricks_secret(scope, key)
    if not success:
        raise NotFoundError(f"Secret '{key}' not found in scope '{scope}'")


@router.post("/secret-scopes", status_code=status.HTTP_200_OK)
async def create_secret_scope_endpoint(
    group_context: GroupContextDep,
    service: DatabricksSecretsServiceDep,
):
    """Legacy endpoint for creating a secret scope if it doesn't exist."""
    workspace_url, scope = await service.validate_databricks_config()

    # Get token from unified auth
    from src.utils.databricks_auth import get_auth_context

    auth = await get_auth_context()
    if not auth or not auth.token:
        raise BadRequestError("No Databricks authentication available")

    success = await service.create_databricks_secret_scope(
        workspace_url, auth.token, scope
    )
    if success:
        return {
            "status": "success",
            "message": f"Scope '{scope}' created or already exists",
        }
    else:
        raise KasalError(f"Failed to create scope '{scope}'")


@router.post(
    "/databricks/token", status_code=status.HTTP_200_OK, response_model=Dict[str, str]
)
async def set_databricks_token(
    request: DatabricksTokenRequest,
    group_context: GroupContextDep,
    service: DatabricksSecretsServiceDep,
):
    """Set Databricks token in the configuration."""
    try:
        # Validate that Databricks is configured and enabled
        config = await service.databricks_service.get_databricks_config()
        if (
            not config
            or not config.is_enabled
            or not config.workspace_url
            or not config.secret_scope
        ):
            raise BadRequestError("Databricks not properly configured")

        # Store the token in Databricks scopes for later use
        # NOTE: Do NOT set os.environ["DATABRICKS_TOKEN"] here as it causes race conditions
        # Authentication should use get_auth_context() from databricks_auth.py instead
        success = await service.set_databricks_token(config.secret_scope, request.token)

        if success:
            return {
                "status": "success",
                "message": f"Token set for scope '{config.secret_scope}'",
            }
        else:
            raise KasalError("Failed to set Databricks token")
    except KasalError:
        raise  # Re-raise domain exceptions as-is


# Legacy API key endpoints - preserved for backward compatibility
# These are identical to the routes in the old file but now use properly separation
# of concerns between API keys and Databricks secrets
@router.get("/api-keys", response_model=List[SecretResponse])
async def get_legacy_api_keys(
    group_context: GroupContextDep,
    service: DatabricksSecretsServiceDep,
    source: Optional[str] = None,
):
    """Legacy endpoint for getting all API keys."""
    logger.info(
        "Legacy API keys GET endpoint called - redirecting to Databricks secrets"
    )
    return await get_databricks_secrets(group_context=group_context, service=service)


@router.post("/api-key", response_model=SecretResponse)
async def create_legacy_api_key(
    secret_data: SecretCreate,
    group_context: GroupContextDep,
    service: DatabricksSecretsServiceDep,
):
    """Legacy endpoint for creating a new API key."""
    logger.info(
        f"Legacy API key CREATE endpoint called for key '{secret_data.name}' - redirecting to Databricks secrets"
    )
    return await create_databricks_secret(secret_data, group_context, service)


@router.put("/api-keys/{secret_name}", response_model=SecretResponse)
async def update_legacy_api_key(
    secret_name: str,
    secret_data: SecretUpdate,
    group_context: GroupContextDep,
    service: DatabricksSecretsServiceDep,
):
    """Legacy endpoint for updating an API key."""
    logger.info(
        f"Legacy API key UPDATE endpoint called for key '{secret_name}' - redirecting to Databricks secrets"
    )
    return await update_databricks_secret(
        secret_name, secret_data, group_context, service
    )


@router.delete("/api-key/{secret_name}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_legacy_api_key(
    secret_name: str,
    group_context: GroupContextDep,
    service: DatabricksSecretsServiceDep,
):
    """Legacy endpoint for deleting an API key."""
    logger.info(
        f"Legacy API key DELETE endpoint called for key '{secret_name}' - redirecting to Databricks secrets"
    )
    await delete_databricks_secret(secret_name, group_context, service)
