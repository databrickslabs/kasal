"""
Converter API Router
FastAPI routes for converter management (history, jobs, saved configurations)
"""

import logging
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import get_db
from src.services.converter_service import ConverterService
from src.schemas.conversion import (
    # History
    ConversionHistoryCreate,
    ConversionHistoryUpdate,
    ConversionHistoryResponse,
    ConversionHistoryListResponse,
    ConversionHistoryFilter,
    ConversionStatistics,
    # Jobs
    ConversionJobCreate,
    ConversionJobUpdate,
    ConversionJobResponse,
    ConversionJobListResponse,
    ConversionJobStatusUpdate,
    # Saved Configs
    SavedConfigurationCreate,
    SavedConfigurationUpdate,
    SavedConfigurationResponse,
    SavedConfigurationListResponse,
    SavedConfigurationFilter,
)
from src.core.dependencies import GroupContextDep

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/converters", tags=["converters"])


def get_converter_service(
    session: Annotated[AsyncSession, Depends(get_db)],
    group_context: GroupContextDep = None,
) -> ConverterService:
    """
    Dependency to get converter service with session and group context.

    Returns:
        ConverterService instance
    """
    return ConverterService(session, group_context=group_context)


# ===== CONVERSION HISTORY ENDPOINTS =====

@router.post(
    "/history",
    response_model=ConversionHistoryResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create Conversion History Entry",
    description="Create a new conversion history entry for audit trail and analytics",
)
async def create_history(
    history_data: ConversionHistoryCreate,
    service: Annotated[ConverterService, Depends(get_converter_service)],
) -> ConversionHistoryResponse:
    """
    Create conversion history entry.

    This is typically called automatically after a conversion completes,
    but can also be called manually for tracking purposes.
    """
    return await service.create_history(history_data)


@router.get(
    "/history/statistics",
    response_model=ConversionStatistics,
    summary="Get Conversion Statistics",
    description="Get analytics on conversion success rate, execution time, and popular conversion paths",
)
async def get_statistics(
    service: Annotated[ConverterService, Depends(get_converter_service)],
    days: int = Query(30, ge=1, le=365, description="Number of days to analyze"),
) -> ConversionStatistics:
    """
    Get conversion statistics for analytics.

    Returns:
    - Total conversions
    - Success/failure counts and rates
    - Average execution time
    - Most popular conversion paths
    """
    return await service.get_statistics(days)


@router.get(
    "/history/{history_id}",
    response_model=ConversionHistoryResponse,
    summary="Get Conversion History",
    description="Retrieve a specific conversion history entry by ID",
)
async def get_history(
    history_id: int,
    service: Annotated[ConverterService, Depends(get_converter_service)],
) -> ConversionHistoryResponse:
    """Get conversion history entry by ID."""
    return await service.get_history(history_id)


@router.patch(
    "/history/{history_id}",
    response_model=ConversionHistoryResponse,
    summary="Update Conversion History",
    description="Update conversion history entry (typically to add results or error messages)",
)
async def update_history(
    history_id: int,
    update_data: ConversionHistoryUpdate,
    service: Annotated[ConverterService, Depends(get_converter_service)],
) -> ConversionHistoryResponse:
    """Update conversion history entry."""
    return await service.update_history(history_id, update_data)


@router.get(
    "/history",
    response_model=ConversionHistoryListResponse,
    summary="List Conversion History",
    description="List conversion history with optional filters for audit trail and debugging",
)
async def list_history(
    service: Annotated[ConverterService, Depends(get_converter_service)],
    source_format: Optional[str] = Query(None, description="Filter by source format"),
    target_format: Optional[str] = Query(None, description="Filter by target format"),
    status: Optional[str] = Query(None, description="Filter by status (pending, success, failed)"),
    execution_id: Optional[str] = Query(None, description="Filter by execution ID"),
    limit: int = Query(100, ge=1, le=1000, description="Number of results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
) -> ConversionHistoryListResponse:
    """
    List conversion history with filters.

    Useful for:
    - Audit trail
    - Debugging failed conversions
    - Analytics on conversion patterns
    """
    filter_params = ConversionHistoryFilter(
        source_format=source_format,
        target_format=target_format,
        status=status,
        execution_id=execution_id,
        limit=limit,
        offset=offset,
    )
    return await service.list_history(filter_params)


# ===== CONVERSION JOB ENDPOINTS =====

@router.post(
    "/jobs",
    response_model=ConversionJobResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create Conversion Job",
    description="Create an async conversion job for long-running conversions",
)
async def create_job(
    job_data: ConversionJobCreate,
    service: Annotated[ConverterService, Depends(get_converter_service)],
) -> ConversionJobResponse:
    """
    Create async conversion job.

    For large conversions that may take time, create a job that can be
    monitored and retrieved later.
    """
    return await service.create_job(job_data)


@router.get(
    "/jobs/{job_id}",
    response_model=ConversionJobResponse,
    summary="Get Conversion Job",
    description="Get conversion job status and results by job ID",
)
async def get_job(
    job_id: str,
    service: Annotated[ConverterService, Depends(get_converter_service)],
) -> ConversionJobResponse:
    """Get conversion job by ID."""
    return await service.get_job(job_id)


@router.patch(
    "/jobs/{job_id}",
    response_model=ConversionJobResponse,
    summary="Update Conversion Job",
    description="Update conversion job details",
)
async def update_job(
    job_id: str,
    update_data: ConversionJobUpdate,
    service: Annotated[ConverterService, Depends(get_converter_service)],
) -> ConversionJobResponse:
    """Update conversion job."""
    return await service.update_job(job_id, update_data)


@router.patch(
    "/jobs/{job_id}/status",
    response_model=ConversionJobResponse,
    summary="Update Job Status",
    description="Update job status and progress (used by background workers)",
)
async def update_job_status(
    job_id: str,
    status_update: ConversionJobStatusUpdate,
    service: Annotated[ConverterService, Depends(get_converter_service)],
) -> ConversionJobResponse:
    """
    Update job status and progress.

    Typically called by background workers to report progress.
    """
    return await service.update_job_status(job_id, status_update)


@router.get(
    "/jobs",
    response_model=ConversionJobListResponse,
    summary="List Conversion Jobs",
    description="List conversion jobs with optional status filter",
)
async def list_jobs(
    service: Annotated[ConverterService, Depends(get_converter_service)],
    status: Optional[str] = Query(None, description="Filter by status (pending, running, completed, failed, cancelled)"),
    limit: int = Query(50, ge=1, le=500, description="Number of results"),
) -> ConversionJobListResponse:
    """
    List conversion jobs.

    By default, shows active jobs (pending/running).
    Use status filter to see completed/failed jobs.
    """
    return await service.list_jobs(status=status, limit=limit)


@router.post(
    "/jobs/{job_id}/cancel",
    response_model=ConversionJobResponse,
    summary="Cancel Conversion Job",
    description="Cancel a pending or running conversion job",
)
async def cancel_job(
    job_id: str,
    service: Annotated[ConverterService, Depends(get_converter_service)],
) -> ConversionJobResponse:
    """
    Cancel a conversion job.

    Only pending or running jobs can be cancelled.
    """
    return await service.cancel_job(job_id)


# ===== SAVED CONFIGURATION ENDPOINTS =====

@router.post(
    "/configs",
    response_model=SavedConfigurationResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Save Converter Configuration",
    description="Save a converter configuration for reuse",
)
async def create_config(
    config_data: SavedConfigurationCreate,
    service: Annotated[ConverterService, Depends(get_converter_service)],
) -> SavedConfigurationResponse:
    """
    Save converter configuration.

    Allows users to save frequently used converter configurations
    with custom names for quick access.
    """
    return await service.create_saved_config(config_data)


@router.get(
    "/configs/{config_id}",
    response_model=SavedConfigurationResponse,
    summary="Get Saved Configuration",
    description="Retrieve a saved converter configuration by ID",
)
async def get_config(
    config_id: int,
    service: Annotated[ConverterService, Depends(get_converter_service)],
) -> SavedConfigurationResponse:
    """Get saved configuration by ID."""
    return await service.get_saved_config(config_id)


@router.patch(
    "/configs/{config_id}",
    response_model=SavedConfigurationResponse,
    summary="Update Saved Configuration",
    description="Update a saved converter configuration",
)
async def update_config(
    config_id: int,
    update_data: SavedConfigurationUpdate,
    service: Annotated[ConverterService, Depends(get_converter_service)],
) -> SavedConfigurationResponse:
    """
    Update saved configuration.

    Only the owner can update their configurations.
    """
    return await service.update_saved_config(config_id, update_data)


@router.delete(
    "/configs/{config_id}",
    summary="Delete Saved Configuration",
    description="Delete a saved converter configuration",
)
async def delete_config(
    config_id: int,
    service: Annotated[ConverterService, Depends(get_converter_service)],
):
    """
    Delete saved configuration.

    Only the owner can delete their configurations.
    """
    return await service.delete_saved_config(config_id)


@router.get(
    "/configs",
    response_model=SavedConfigurationListResponse,
    summary="List Saved Configurations",
    description="List saved converter configurations with optional filters",
)
async def list_configs(
    service: Annotated[ConverterService, Depends(get_converter_service)],
    source_format: Optional[str] = Query(None, description="Filter by source format"),
    target_format: Optional[str] = Query(None, description="Filter by target format"),
    is_public: Optional[bool] = Query(None, description="Filter by public/shared status"),
    is_template: Optional[bool] = Query(None, description="Filter by template status"),
    search: Optional[str] = Query(None, description="Search in configuration name"),
    limit: int = Query(50, ge=1, le=200, description="Number of results"),
) -> SavedConfigurationListResponse:
    """
    List saved configurations.

    Shows:
    - User's own configurations
    - Public configurations shared by others
    - System templates
    """
    filter_params = SavedConfigurationFilter(
        source_format=source_format,
        target_format=target_format,
        is_public=is_public,
        is_template=is_template,
        search=search,
        limit=limit,
    )
    return await service.list_saved_configs(filter_params)


@router.post(
    "/configs/{config_id}/use",
    response_model=SavedConfigurationResponse,
    summary="Use Saved Configuration",
    description="Mark a configuration as used (increments usage counter)",
)
async def use_config(
    config_id: int,
    service: Annotated[ConverterService, Depends(get_converter_service)],
) -> SavedConfigurationResponse:
    """
    Mark configuration as used.

    Increments the use counter and updates last_used_at timestamp.
    Useful for tracking popular configurations.
    """
    return await service.use_saved_config(config_id)


# ===== HEALTH CHECK =====

@router.get(
    "/health",
    summary="Converter Health Check",
    description="Check if converter service is healthy",
)
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "converter",
        "version": "1.0.0",
    }
