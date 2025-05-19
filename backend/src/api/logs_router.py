from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Query, Depends
import logging

from src.schemas.log import LLMLogResponse
from src.services.log_service import LLMLogService
from src.core.dependencies import get_log_service

router = APIRouter(
    prefix="/llm-logs",
    tags=["logs"],
    responses={404: {"description": "Not found"}},
)

# Set up logging
logger = logging.getLogger(__name__)

@router.get("", response_model=List[LLMLogResponse])
async def get_llm_logs(
    page: int = Query(0, ge=0, description="Page number, starting from 0"),
    per_page: int = Query(10, ge=1, le=100, description="Items per page, between 1 and 100"),
    endpoint: Optional[str] = Query(None, description="Filter by endpoint, 'all' or None for all endpoints"),
    log_service: LLMLogService = Depends(get_log_service)
):
    """
    Get LLM logs with pagination and optional endpoint filtering.
    
    Args:
        page: Page number, starting from 0
        per_page: Items per page, between 1 and 100
        endpoint: Optional endpoint to filter by
        log_service: Injected log service
        
    Returns:
        List of LLM logs for the specified page
    """
    try:
        logs = await log_service.get_logs_paginated(page, per_page, endpoint)
        return [LLMLogResponse.model_validate(log) for log in logs]
    except Exception as e:
        logger.error(f"Error getting LLM logs: {str(e)}")
        raise

@router.get("/count", response_model=int)
async def count_llm_logs(
    endpoint: Optional[str] = Query(None, description="Filter by endpoint, 'all' or None for all endpoints"),
    log_service: LLMLogService = Depends(get_log_service)
):
    """
    Count LLM logs with optional endpoint filtering.
    
    Args:
        endpoint: Optional endpoint to filter by
        log_service: Injected log service
        
    Returns:
        Total count of matching logs
    """
    try:
        return await log_service.count_logs(endpoint)
    except Exception as e:
        logger.error(f"Error counting LLM logs: {str(e)}")
        raise

@router.get("/endpoints", response_model=List[str])
async def get_unique_endpoints(
    log_service: LLMLogService = Depends(get_log_service)
):
    """
    Get list of unique endpoints in the logs.
    
    Args:
        log_service: Injected log service
    
    Returns:
        List of unique endpoint strings
    """
    try:
        return await log_service.get_unique_endpoints()
    except Exception as e:
        logger.error(f"Error getting unique endpoints: {str(e)}")
        raise

@router.get("/stats", response_model=Dict[str, Any])
async def get_log_stats(
    days: int = Query(30, ge=1, le=365, description="Number of days to include in stats"),
    log_service: LLMLogService = Depends(get_log_service)
):
    """
    Get statistics about LLM usage.
    
    Args:
        days: Number of days to include in stats
        log_service: Injected log service
        
    Returns:
        Dictionary with usage statistics
    """
    try:
        return await log_service.get_log_stats(days)
    except Exception as e:
        logger.error(f"Error getting log stats: {str(e)}")
        raise 