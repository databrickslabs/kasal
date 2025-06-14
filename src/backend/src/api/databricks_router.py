from typing import Dict, Annotated
import logging

from fastapi import APIRouter, Depends, HTTPException

from src.schemas.databricks_config import DatabricksConfigCreate, DatabricksConfigResponse
from src.services.databricks_service import DatabricksService
from src.services.api_keys_service import ApiKeysService
from src.core.dependencies import SessionDep

router = APIRouter(
    prefix="/databricks",
    tags=["databricks"],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger(__name__)

# Dependency to get ApiKeysService
def get_api_keys_service(session: SessionDep) -> ApiKeysService:
    """Get ApiKeysService instance."""
    return ApiKeysService(session)

# Dependency to get DatabricksService
def get_databricks_service(
    session: SessionDep,
    api_keys_service: Annotated[ApiKeysService, Depends(get_api_keys_service)]
) -> DatabricksService:
    """
    Get a properly initialized DatabricksService instance.
    
    Args:
        session: Database session from dependency injection
        api_keys_service: ApiKeysService instance
        
    Returns:
        Initialized DatabricksService with all dependencies
    """
    return DatabricksService.from_session(session, api_keys_service)


@router.post("/config", response_model=Dict)
async def set_databricks_config(
    request: DatabricksConfigCreate,
    service: Annotated[DatabricksService, Depends(get_databricks_service)],
):
    """
    Set Databricks configuration.
    
    Args:
        request: Configuration data
        service: Databricks service
        
    Returns:
        Success response with configuration
    """
    try:
        return await service.set_databricks_config(request)
    except Exception as e:
        logger.error(f"Error setting Databricks configuration: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error setting Databricks configuration: {str(e)}")


@router.get("/config", response_model=DatabricksConfigResponse)
async def get_databricks_config(
    service: Annotated[DatabricksService, Depends(get_databricks_service)],
):
    """
    Get current Databricks configuration.
    
    Args:
        service: Databricks service
        
    Returns:
        Current Databricks configuration
    """
    try:
        return await service.get_databricks_config()
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting Databricks configuration: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting Databricks configuration: {str(e)}")


@router.get("/status/personal-token-required", response_model=Dict)
async def check_personal_token_required(
    service: Annotated[DatabricksService, Depends(get_databricks_service)],
):
    """
    Check if personal access token is required for Databricks.
    
    Args:
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
    service: Annotated[DatabricksService, Depends(get_databricks_service)],
):
    """
    Check connection to Databricks.
    
    Args:
        service: Databricks service
        
    Returns:
        Connection status
    """
    try:
        return await service.check_databricks_connection()
    except Exception as e:
        logger.error(f"Error checking Databricks connection: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error checking Databricks connection: {str(e)}") 