"""
Router for execution history operations.

This module provides API endpoints for retrieving, managing, and deleting
execution history records and related data.
"""


from fastapi import APIRouter, Depends, HTTPException, Query, status, Response

from src.core.logger import LoggerManager
from src.services.execution_history_service import ExecutionHistoryService, get_execution_history_service
from src.schemas.execution_history import (
    ExecutionHistoryList,
    ExecutionHistoryItem,
    ExecutionOutputList,
    ExecutionOutputDebugList,
    DeleteResponse
)

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().system

router = APIRouter(
    prefix="/executions",
    tags=["Execution History"]
)

@router.get("/history", response_model=ExecutionHistoryList)
async def get_execution_history(
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    service: ExecutionHistoryService = Depends(get_execution_history_service)
):
    """
    Get a paginated list of execution history.
    
    Args:
        limit: Maximum number of executions to return (1-100)
        offset: Pagination offset
        service: ExecutionHistoryService instance
    
    Returns:
        ExecutionHistoryList with paginated execution history
    """
    try:
        return await service.get_execution_history(limit, offset)
    except Exception as e:
        logger.error(f"Error getting execution history: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve execution history: {str(e)}"
        )

@router.head("/history/{execution_id}")
async def check_execution_exists(
    execution_id: int, 
    service: ExecutionHistoryService = Depends(get_execution_history_service), 
    response: Response = None
):
    """
    Check if an execution exists by ID. This is a lightweight HEAD request
    that returns only status code without a response body.
    
    Args:
        execution_id: Database ID of the execution
        service: ExecutionHistoryService instance
        response: FastAPI Response object
        
    Returns:
        HTTP 200 OK if the execution exists, HTTP 404 Not Found otherwise
    """
    try:
        exists = await service.check_execution_exists(execution_id)
        if not exists:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Execution with ID {execution_id} not found"
            )
        # Just return an empty response with 200 status
        return Response(status_code=status.HTTP_200_OK)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error checking if execution {execution_id} exists: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to check execution existence: {str(e)}"
        )

@router.get("/history/{execution_id}", response_model=ExecutionHistoryItem)
async def get_execution_by_id(
    execution_id: int, 
    service: ExecutionHistoryService = Depends(get_execution_history_service)
):
    """
    Get execution details by ID.
    
    Args:
        execution_id: Database ID of the execution
        service: ExecutionHistoryService instance
    
    Returns:
        ExecutionHistoryItem with execution details
    """
    try:
        execution = await service.get_execution_by_id(execution_id)
        if not execution:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Execution with ID {execution_id} not found"
            )
        return execution
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting execution {execution_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve execution: {str(e)}"
        )

@router.get("/{execution_id}/outputs", response_model=ExecutionOutputList)
async def get_execution_outputs(
    execution_id: str,
    limit: int = Query(1000, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    service: ExecutionHistoryService = Depends(get_execution_history_service)
):
    """
    Get outputs for an execution.
    
    Args:
        execution_id: String ID of the execution
        limit: Maximum number of outputs to return (1-5000)
        offset: Pagination offset
        service: ExecutionHistoryService instance
    
    Returns:
        ExecutionOutputList with paginated execution outputs
    """
    try:
        return await service.get_execution_outputs(execution_id, limit, offset)
    except Exception as e:
        logger.error(f"Error getting outputs for execution {execution_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve execution outputs: {str(e)}"
        )

@router.get("/{execution_id}/outputs/debug", response_model=ExecutionOutputDebugList)
async def get_execution_debug_outputs(
    execution_id: str, 
    service: ExecutionHistoryService = Depends(get_execution_history_service)
):
    """
    Get debug information about outputs for an execution.
    
    Args:
        execution_id: String ID of the execution
        service: ExecutionHistoryService instance
    
    Returns:
        ExecutionOutputDebugList with debug information
    """
    try:
        debug_info = await service.get_debug_outputs(execution_id)
        if not debug_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Execution with ID {execution_id} not found"
            )
        return debug_info
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting debug outputs for execution {execution_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve debug outputs: {str(e)}"
        )

@router.delete("/history", response_model=DeleteResponse)
async def delete_all_executions(
    service: ExecutionHistoryService = Depends(get_execution_history_service)
):
    """
    Delete all executions and their associated data.
    
    Returns:
        DeleteResponse with information about the deleted data
    """
    try:
        return await service.delete_all_executions()
    except Exception as e:
        logger.error(f"Error deleting all executions: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete executions: {str(e)}"
        )

@router.delete("/history/{execution_id}", response_model=DeleteResponse)
async def delete_execution(
    execution_id: int, 
    service: ExecutionHistoryService = Depends(get_execution_history_service)
):
    """
    Delete a specific execution and its associated data.
    
    Args:
        execution_id: Database ID of the execution
        service: ExecutionHistoryService instance
    
    Returns:
        DeleteResponse with information about the deleted data
    """
    try:
        result = await service.delete_execution(execution_id)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Execution with ID {execution_id} not found"
            )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting execution {execution_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete execution: {str(e)}"
        )

@router.delete("/{job_id}", response_model=DeleteResponse)
async def delete_execution_by_job_id(
    job_id: str, 
    service: ExecutionHistoryService = Depends(get_execution_history_service)
):
    """
    Delete an execution by its job_id.
    
    Args:
        job_id: String ID (UUID) of the execution
        service: ExecutionHistoryService instance
    
    Returns:
        DeleteResponse with information about the deleted data
    """
    try:
        result = await service.delete_execution_by_job_id(job_id)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Execution with job_id {job_id} not found"
            )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting execution with job_id {job_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete execution: {str(e)}"
        ) 