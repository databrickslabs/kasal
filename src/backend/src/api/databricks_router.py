from typing import Dict, Annotated
import logging
import os

from fastapi import APIRouter, Depends, HTTPException

from src.schemas.databricks_config import DatabricksConfigCreate, DatabricksConfigResponse
from src.services.databricks_service import DatabricksService
from src.services.api_keys_service import ApiKeysService
from src.core.dependencies import SessionDep, GroupContextDep
from src.core.permissions import check_role_in_context, is_workspace_admin
from src.utils.databricks_auth import is_databricks_apps_environment

router = APIRouter(
    prefix="/databricks",
    tags=["databricks"],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger(__name__)

# Dependency to get ApiKeysService
def get_api_keys_service(
    session: SessionDep,
    group_context: GroupContextDep
) -> ApiKeysService:
    """Get ApiKeysService instance with group context."""
    group_id = group_context.primary_group_id if group_context else None
    return ApiKeysService(session, group_id=group_id)

# Dependency to get DatabricksService
def get_databricks_service(
    session: SessionDep,
    group_context: GroupContextDep,
    api_keys_service: Annotated[ApiKeysService, Depends(get_api_keys_service)]
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
        raise HTTPException(
            status_code=403,
            detail="Only workspace admins can set Databricks configuration"
        )

    try:
        # Get user email from group context
        created_by_email = group_context.group_email if group_context else None
        return await service.set_databricks_config(request, created_by_email=created_by_email)
    except Exception as e:
        logger.error(f"Error setting Databricks configuration: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error setting Databricks configuration: {str(e)}")


@router.get("/config", response_model=DatabricksConfigResponse)
async def get_databricks_config(
    group_context: GroupContextDep,
    service: DatabricksServiceDep,
):
    """
    Get current Databricks configuration.
    
    Args:
        group_context: Group context for multi-tenant operations
        service: Databricks service
        
    Returns:
        Current Databricks configuration
    """
    try:
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
                apps_enabled=False,
                volume_enabled=False,
                volume_path=None,
                volume_file_format="json",
                volume_create_date_dirs=True,
                knowledge_volume_enabled=False,
                knowledge_volume_path=None,
                knowledge_chunk_size=1000,
                knowledge_chunk_overlap=200
            )
        return config
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting Databricks configuration: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting Databricks configuration: {str(e)}")


@router.get("/status/personal-token-required", response_model=Dict)
async def check_personal_token_required(
    group_context: GroupContextDep,
    service: DatabricksServiceDep,
):
    """
    Check if personal access token is required for Databricks.
    
    Args:
        group_context: Group context for multi-tenant operations
        service: Databricks service
        
    Returns:
        Status indicating if personal token is required
    """
    try:
        return await service.check_personal_token_required()
    except Exception as e:
        logger.error(f"Error checking personal token requirement: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error checking personal token requirement: {str(e)}")


@router.get("/connection", response_model=Dict)
async def check_databricks_connection(
    group_context: GroupContextDep,
    service: DatabricksServiceDep,
):
    """
    Check connection to Databricks.
    
    Args:
        group_context: Group context for multi-tenant operations
        service: Databricks service
        
    Returns:
        Connection status
    """
    try:
        return await service.check_databricks_connection()
    except Exception as e:
        logger.error(f"Error checking Databricks connection: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error checking Databricks connection: {str(e)}")


@router.get("/environment", response_model=Dict)
async def get_databricks_environment():
    """
    Get information about the Databricks environment.
    
    Returns:
        Dictionary containing environment information including whether we're in Databricks Apps
    """
    try:
        is_apps = is_databricks_apps_environment()
        return {
            "is_databricks_apps": is_apps,
            "databricks_app_name": os.getenv("DATABRICKS_APP_NAME"),
            "databricks_host": os.getenv("DATABRICKS_HOST"),
            "workspace_id": os.getenv("DATABRICKS_WORKSPACE_ID"),
            "has_oauth_credentials": bool(os.getenv("DATABRICKS_CLIENT_ID") and os.getenv("DATABRICKS_CLIENT_SECRET")),
            "message": "Running in Databricks Apps environment" if is_apps else "Not running in Databricks Apps"
        }
    except Exception as e:
        logger.error(f"Error getting Databricks environment info: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting Databricks environment info: {str(e)}") 