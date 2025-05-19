from typing import Dict
import logging

from fastapi import APIRouter, Depends, HTTPException

from src.schemas.databricks_config import DatabricksConfigCreate, DatabricksConfigResponse
from src.services.databricks_service import DatabricksService
from src.core.unit_of_work import UnitOfWork

router = APIRouter(
    prefix="/databricks",
    tags=["databricks"],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger(__name__)

# Dependency to get DatabricksService using UnitOfWork pattern
async def get_databricks_service():
    """
    Get a properly initialized DatabricksService instance using UnitOfWork.
    
    Returns:
        Initialized DatabricksService
    """
    async with UnitOfWork() as uow:
        service = await DatabricksService.from_unit_of_work(uow)
        yield service


@router.post("/config", response_model=Dict)
async def set_databricks_config(
    request: DatabricksConfigCreate,
    service: DatabricksService = Depends(get_databricks_service),
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
    service: DatabricksService = Depends(get_databricks_service),
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
    service: DatabricksService = Depends(get_databricks_service),
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
    service: DatabricksService = Depends(get_databricks_service),
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