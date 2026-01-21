from typing import Dict, Annotated
import logging

from fastapi import APIRouter, Depends, HTTPException

from src.schemas.powerbi_config import (
    PowerBIConfigCreate,
    PowerBIConfigResponse,
    DAXQueryRequest,
    DAXQueryResponse
)
from src.services.powerbi_service import PowerBIService
from src.core.dependencies import SessionDep, GroupContextDep
from src.core.permissions import is_workspace_admin

router = APIRouter(
    prefix="/powerbi",
    tags=["powerbi"],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger(__name__)


# Dependency to get PowerBIService
def get_powerbi_service(
    session: SessionDep,
    group_context: GroupContextDep
) -> PowerBIService:
    """
    Get a properly initialized PowerBIService instance with group context.

    Args:
        session: Database session from dependency injection
        group_context: Group context for multi-tenant filtering

    Returns:
        Initialized PowerBIService with all dependencies
    """
    # Get group_id from context
    group_id = group_context.primary_group_id if group_context else None

    # Create service with session and group context
    service = PowerBIService(session, group_id=group_id)

    return service


# Type alias for cleaner function signatures
PowerBIServiceDep = Annotated[PowerBIService, Depends(get_powerbi_service)]


@router.post("/config", response_model=Dict)
async def set_powerbi_config(
    request: PowerBIConfigCreate,
    group_context: GroupContextDep,
    service: PowerBIServiceDep,
):
    """
    Set Power BI configuration.
    Only workspace admins can set Power BI configuration for their workspace.

    Args:
        request: Configuration data
        group_context: Group context for multi-tenant operations
        service: Power BI service

    Returns:
        Success response with configuration
    """
    # Check permissions - only workspace admins can set Power BI configuration
    if not is_workspace_admin(group_context):
        raise HTTPException(
            status_code=403,
            detail="Only workspace admins can set Power BI configuration"
        )

    try:
        # Get user email from group context
        created_by_email = group_context.group_email if group_context else None

        # Get group ID
        group_id = group_context.primary_group_id if group_context else None

        # Create configuration data
        config_data = request.model_dump()
        config_data['group_id'] = group_id
        config_data['created_by_email'] = created_by_email

        # Create configuration using repository
        config = await service.repository.create_config(config_data)

        return {
            "message": "Power BI configuration saved successfully",
            "config": {
                "tenant_id": config.tenant_id,
                "client_id": config.client_id,
                "workspace_id": config.workspace_id,
                "semantic_model_id": config.semantic_model_id,
                "is_enabled": config.is_enabled,
                "is_active": config.is_active,
            }
        }
    except Exception as e:
        logger.error(f"Error setting Power BI configuration: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error setting Power BI configuration: {str(e)}")


@router.get("/config", response_model=PowerBIConfigResponse)
async def get_powerbi_config(
    group_context: GroupContextDep,
    service: PowerBIServiceDep,
):
    """
    Get current Power BI configuration.

    Args:
        group_context: Group context for multi-tenant operations
        service: Power BI service

    Returns:
        Current Power BI configuration
    """
    try:
        config = await service.repository.get_active_config(group_id=service.group_id)
        if not config:
            # Return a default empty configuration
            return PowerBIConfigResponse(
                tenant_id="",
                client_id="",
                workspace_id=None,
                semantic_model_id=None,
                enabled=False
            )

        return PowerBIConfigResponse(
            tenant_id=config.tenant_id,
            client_id=config.client_id,
            workspace_id=config.workspace_id,
            semantic_model_id=config.semantic_model_id,
            enabled=config.is_enabled
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting Power BI configuration: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error getting Power BI configuration: {str(e)}")


@router.post("/query", response_model=DAXQueryResponse)
async def execute_dax_query(
    request: DAXQueryRequest,
    group_context: GroupContextDep,
    service: PowerBIServiceDep,
):
    """
    Execute a DAX query against a Power BI semantic model.

    Args:
        request: DAX query request with query and optional semantic model ID
        group_context: Group context for multi-tenant operations
        service: Power BI service

    Returns:
        Query execution results
    """
    try:
        response = await service.execute_dax_query(request)
        return response
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error executing DAX query: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error executing DAX query: {str(e)}")


@router.get("/status", response_model=Dict)
async def check_powerbi_status(
    group_context: GroupContextDep,
    service: PowerBIServiceDep,
):
    """
    Check Power BI integration status.

    Args:
        group_context: Group context for multi-tenant operations
        service: Power BI service

    Returns:
        Status information about Power BI integration
    """
    try:
        config = await service.repository.get_active_config(group_id=service.group_id)

        if not config:
            return {
                "configured": False,
                "enabled": False,
                "message": "Power BI is not configured. Please configure connection settings."
            }

        return {
            "configured": True,
            "enabled": config.is_enabled,
            "workspace_id": config.workspace_id,
            "semantic_model_id": config.semantic_model_id,
            "message": "Power BI is configured and ready" if config.is_enabled else "Power BI is configured but disabled"
        }
    except Exception as e:
        logger.error(f"Error checking Power BI status: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error checking Power BI status: {str(e)}")
