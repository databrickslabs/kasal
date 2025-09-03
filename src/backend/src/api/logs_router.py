from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Query, Depends
import logging

from src.schemas.log import LLMLogResponse
from src.services.log_service import LLMLogService
from src.core.dependencies import get_log_service, GroupContextDep

router = APIRouter(
    prefix="/llm-logs",
    tags=["logs"],
    responses={404: {"description": "Not found"}},
)

# Set up logging
logger = logging.getLogger(__name__)

@router.get("", response_model=List[LLMLogResponse])
async def get_llm_logs(
    group_context: GroupContextDep,
    page: int = Query(0, ge=0, description="Page number, starting from 0"),
    per_page: int = Query(10, ge=1, le=100, description="Items per page, between 1 and 100"),
    endpoint: Optional[str] = Query(None, description="Filter by endpoint, 'all' or None for all endpoints"),
    log_service: LLMLogService = Depends(get_log_service)
):
    """
    Get LLM logs with pagination and optional endpoint filtering for the current group.
    
    Args:
        page: Page number, starting from 0
        per_page: Items per page, between 1 and 100
        endpoint: Optional endpoint to filter by
        log_service: Injected log service
        group_context: Group context from headers
        
    Returns:
        List of LLM logs for the specified page and group
    """
    try:
        logs = await log_service.get_logs_paginated_by_group(page, per_page, endpoint, group_context)
        return [LLMLogResponse.model_validate(log) for log in logs]
    except Exception as e:
        logger.error(f"Error getting LLM logs: {str(e)}")
        raise

@router.get("/count", response_model=int)
async def count_llm_logs(
    group_context: GroupContextDep,
    endpoint: Optional[str] = Query(None, description="Filter by endpoint, 'all' or None for all endpoints"),
    log_service: LLMLogService = Depends(get_log_service)
):
    """
    Count LLM logs with optional endpoint filtering for the current group.
    
    Args:
        endpoint: Optional endpoint to filter by
        log_service: Injected log service
        group_context: Group context from headers
        
    Returns:
        Total count of matching logs for group
    """
    try:
        return await log_service.count_logs_by_group(endpoint, group_context)
    except Exception as e:
        logger.error(f"Error counting LLM logs: {str(e)}")
        raise

@router.get("/endpoints", response_model=List[str])
async def get_unique_endpoints(
    group_context: GroupContextDep,
    log_service: LLMLogService = Depends(get_log_service)
):
    """
    Get list of unique endpoints in the logs for the current group.
    
    Args:
        log_service: Injected log service
        group_context: Group context from headers
    
    Returns:
        List of unique endpoint strings for group
    """
    try:
        return await log_service.get_unique_endpoints_by_group(group_context)
    except Exception as e:
        logger.error(f"Error getting unique endpoints: {str(e)}")
        raise

@router.get("/stats", response_model=Dict[str, Any])
async def get_log_stats(
    group_context: GroupContextDep,
    days: int = Query(30, ge=1, le=365, description="Number of days to include in stats"),
    log_service: LLMLogService = Depends(get_log_service)
):
    """
    Get statistics about LLM usage for the current group.
    
    Args:
        days: Number of days to include in stats
        log_service: Injected log service
        group_context: Group context from headers
        
    Returns:
        Dictionary with usage statistics for group
    """
    try:
        return await log_service.get_log_stats_by_group(days, group_context)
    except Exception as e:
        logger.error(f"Error getting log stats: {str(e)}")
        raise 