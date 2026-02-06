import os
from typing import Annotated, Dict

from fastapi import APIRouter, Depends

from src.core.exceptions import ForbiddenError

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.permissions import check_role_in_context, is_workspace_admin
from src.schemas.databricks_config import (
    DatabricksConfigCreate,
    DatabricksConfigResponse,
)
from src.services.api_keys_service import ApiKeysService
from src.services.databricks_service import DatabricksService

router = APIRouter(
    prefix="/databricks",
    tags=["databricks"],
    responses={404: {"description": "Not found"}},
)


# Dependency to get ApiKeysService
def get_api_keys_service(
    session: SessionDep, group_context: GroupContextDep
) -> ApiKeysService:
    """Get ApiKeysService instance with group context."""
    group_id = group_context.primary_group_id if group_context else None
    return ApiKeysService(session, group_id=group_id)


# Dependency to get DatabricksService
def get_databricks_service(
    session: SessionDep,
    group_context: GroupContextDep,
    api_keys_service: Annotated[ApiKeysService, Depends(get_api_keys_service)],
) -> DatabricksService:
    """
    Get a properly initialized DatabricksService instance with group context.

    Args:
        session: Database session from dependency injection
        group_context: Group context for multi-tenant filtering
        api_keys_service: ApiKeysService instance

    Returns:
        Initialized DatabricksService with all dependencies
    """
    # Get group_id from context
    group_id = group_context.primary_group_id if group_context else None

    # Create service with session and group context
    service = DatabricksService(session, group_id=group_id)

    # Set the API keys service
    service.secrets_service.set_api_keys_service(api_keys_service)

    return service


# Type alias for cleaner function signatures
DatabricksServiceDep = Annotated[DatabricksService, Depends(get_databricks_service)]
ApiKeysServiceDep = Annotated[ApiKeysService, Depends(get_api_keys_service)]


@router.post("/config", response_model=Dict)
async def set_databricks_config(
    request: DatabricksConfigCreate,
    group_context: GroupContextDep,
    service: DatabricksServiceDep,
):
    """
    Set Databricks configuration.
    Only workspace admins can set Databricks configuration for their workspace.

    Args:
        request: Configuration data
        group_context: Group context for multi-tenant operations
        service: Databricks service

    Returns:
        Success response with configuration
    """
    # Check permissions - only workspace admins can set Databricks configuration
    if not is_workspace_admin(group_context):
        raise ForbiddenError("Only workspace admins can set Databricks configuration")

    # Get user email from group context
    created_by_email = group_context.group_email if group_context else None
    return await service.set_databricks_config(
        request, created_by_email=created_by_email
    )


@router.get("/config", response_model=DatabricksConfigResponse)
async def get_databricks_config(
    group_context: GroupContextDep,
    service: DatabricksServiceDep,
):
    """
    Get current Databricks configuration.
    Only workspace admins can view Databricks configuration.

    Args:
        group_context: Group context for multi-tenant operations
        service: Databricks service

    Returns:
        Current Databricks configuration
    """
    # Check permissions - only workspace admins can view Databricks configuration
    if not is_workspace_admin(group_context):
        raise ForbiddenError("Only workspace admins can view Databricks configuration")

    config = await service.get_databricks_config()
    if not config:
        # Return a default empty configuration instead of 404
        from src.schemas.databricks_config import DatabricksConfigResponse

        return DatabricksConfigResponse(
            workspace_url="",
            warehouse_id="",
            catalog="",
            schema="",
            enabled=False,
            # MLflow configuration defaults
            mlflow_enabled=False,
            evaluation_enabled=False,
            # Volume configuration defaults
            volume_enabled=False,
            volume_path=None,
            volume_file_format="json",
            volume_create_date_dirs=True,
            # Knowledge source volume configuration defaults
            knowledge_volume_enabled=False,
            knowledge_volume_path=None,
            knowledge_chunk_size=1000,
            knowledge_chunk_overlap=200,
        )
    return config


@router.get("/status/personal-token-required", response_model=Dict)
async def check_personal_token_required(
    group_context: GroupContextDep,
    service: DatabricksServiceDep,
):
    """
    Check if personal access token is required for Databricks.
    Only workspace admins can check personal token requirements.

    Args:
        group_context: Group context for multi-tenant operations
        service: Databricks service

    Returns:
        Status indicating if personal token is required
    """
    # Check permissions - only workspace admins can check token requirements
    if not is_workspace_admin(group_context):
        raise ForbiddenError("Only workspace admins can check personal token requirements")

    return await service.check_personal_token_required()


@router.get("/connection", response_model=Dict)
async def check_databricks_connection(
    group_context: GroupContextDep,
    service: DatabricksServiceDep,
):
    """
    Check connection to Databricks.
    Only workspace admins can check Databricks connection status.

    Args:
        group_context: Group context for multi-tenant operations
        service: Databricks service

    Returns:
        Connection status
    """
    # Check permissions - only workspace admins can check connection status
    if not is_workspace_admin(group_context):
        raise ForbiddenError("Only workspace admins can check Databricks connection status")

    return await service.check_databricks_connection()


@router.get("/environment", response_model=Dict)
async def get_databricks_environment(
    group_context: GroupContextDep,
):
    """
    Get information about the Databricks environment.
    Only workspace admins can view Databricks environment information.

    Args:
        group_context: Group context for multi-tenant operations

    Returns:
        Dictionary containing environment information including workspace URL and authentication status
    """
    # Check permissions - only workspace admins can view environment information
    if not is_workspace_admin(group_context):
        raise ForbiddenError("Only workspace admins can view Databricks environment information")

    # Get workspace URL directly from DatabricksAuth config
    # This works even if full authentication isn't available
    from src.utils.databricks_auth import _databricks_auth, get_auth_context

    # Load config to get workspace URL from environment/database
    await _databricks_auth._load_config()
    databricks_host = _databricks_auth._workspace_host

    # Try to get full auth context for additional info
    auth = await get_auth_context()
    auth_method = auth.auth_method if auth else None
    user_identity = auth.user_identity if auth else None

    return {
        "databricks_host": databricks_host,
        "auth_method": auth_method,
        "user_identity": user_identity,
        "authenticated": bool(auth),
    }
