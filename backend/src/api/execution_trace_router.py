"""
Router for execution trace operations.

This module provides API endpoints for retrieving, creating, and managing
execution traces.
"""

import logging
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query, status

from src.core.logger import LoggerManager
from src.services.execution_trace_service import ExecutionTraceService
from src.schemas.execution_trace import (
    ExecutionTraceItem,
    ExecutionTraceList,
    ExecutionTraceResponseByRunId,
    ExecutionTraceResponseByJobId,
    DeleteTraceResponse
)

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().system

router = APIRouter(
    prefix="/traces",
    tags=["Execution Traces"]
)

@router.get("/", response_model=ExecutionTraceList)
async def get_all_traces(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0)
):
    """
    Get a paginated list of all execution traces.
    
    Args:
        limit: Maximum number of traces to return (1-500)
        offset: Pagination offset
    
    Returns:
        ExecutionTraceList with paginated execution traces
    """
    try:
        return await ExecutionTraceService.get_all_traces(limit, offset)
    except Exception as e:
        logger.error(f"Error getting all traces: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve traces: {str(e)}"
        )

@router.get("/execution/{run_id}", response_model=ExecutionTraceResponseByRunId)
async def get_traces_by_run_id(
    run_id: int, 
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0)
):
    """
    Get traces for an execution by run_id.
    
    Args:
        run_id: Database ID of the execution
        limit: Maximum number of traces to return (1-500)
        offset: Pagination offset
    
    Returns:
        ExecutionTraceResponseByRunId with traces for the execution
    """
    try:
        result = await ExecutionTraceService.get_traces_by_run_id(None, run_id, limit, offset)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Execution with ID {run_id} not found"
            )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting traces for execution {run_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve traces: {str(e)}"
        )

@router.get("/job/{job_id}", response_model=ExecutionTraceResponseByJobId)
async def get_traces_by_job_id(
    job_id: str, 
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0)
):
    """
    Get traces for an execution by job_id.
    
    Args:
        job_id: String ID of the execution (job_id)
        limit: Maximum number of traces to return (1-500)
        offset: Pagination offset
    
    Returns:
        ExecutionTraceResponseByJobId with traces for the execution
    """
    try:
        result = await ExecutionTraceService.get_traces_by_job_id(None, job_id, limit, offset)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Execution with job_id {job_id} not found"
            )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting traces for execution with job_id {job_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve traces: {str(e)}"
        )

@router.get("/{trace_id}", response_model=ExecutionTraceItem)
async def get_trace_by_id(trace_id: int):
    """
    Get a specific trace by ID.
    
    Args:
        trace_id: ID of the trace to retrieve
    
    Returns:
        ExecutionTraceItem with trace details
    """
    try:
        trace = await ExecutionTraceService.get_trace_by_id(trace_id)
        if not trace:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Trace with ID {trace_id} not found"
            )
        return trace
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting trace {trace_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve trace: {str(e)}"
        )

@router.post("/", response_model=ExecutionTraceItem, status_code=status.HTTP_201_CREATED)
async def create_trace(trace_data: dict):
    """
    Create a new execution trace.
    
    Args:
        trace_data: Dictionary with trace data
    
    Returns:
        Created ExecutionTraceItem
    """
    try:
        return await ExecutionTraceService.create_trace(trace_data)
    except Exception as e:
        logger.error(f"Error creating trace: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create trace: {str(e)}"
        )

@router.delete("/execution/{run_id}", response_model=DeleteTraceResponse)
async def delete_traces_by_run_id(run_id: int):
    """
    Delete all traces for a specific execution.
    
    Args:
        run_id: Database ID of the execution
    
    Returns:
        DeleteTraceResponse with information about deleted traces
    """
    try:
        return await ExecutionTraceService.delete_traces_by_run_id(run_id)
    except Exception as e:
        logger.error(f"Error deleting traces for execution {run_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete traces: {str(e)}"
        )

@router.delete("/job/{job_id}", response_model=DeleteTraceResponse)
async def delete_traces_by_job_id(job_id: str):
    """
    Delete all traces for a specific job.
    
    Args:
        job_id: String ID of the execution (job_id)
    
    Returns:
        DeleteTraceResponse with information about deleted traces
    """
    try:
        return await ExecutionTraceService.delete_traces_by_job_id(job_id)
    except Exception as e:
        logger.error(f"Error deleting traces for job_id {job_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete traces: {str(e)}"
        )

@router.delete("/{trace_id}", response_model=DeleteTraceResponse)
async def delete_trace(trace_id: int):
    """
    Delete a specific trace by ID.
    
    Args:
        trace_id: ID of the trace to delete
    
    Returns:
        DeleteTraceResponse with information about the deleted trace
    """
    try:
        result = await ExecutionTraceService.delete_trace(trace_id)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Trace with ID {trace_id} not found"
            )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting trace {trace_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete trace: {str(e)}"
        )

@router.delete("/", response_model=DeleteTraceResponse)
async def delete_all_traces():
    """
    Delete all execution traces.
    
    Returns:
        DeleteTraceResponse with information about deleted traces
    """
    try:
        return await ExecutionTraceService.delete_all_traces()
    except Exception as e:
        logger.error(f"Error deleting all traces: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete traces: {str(e)}"
        ) 