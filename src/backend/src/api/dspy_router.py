"""
DSPy configuration API router.

Provides endpoints for managing DSPy prompt optimization settings.
Simplified to focus on enable/disable toggle and basic stats.
"""

from typing import Annotated, Optional
from fastapi import APIRouter, Depends, HTTPException
import logging

from pydantic import BaseModel
from src.services.dspy_settings_service import DSPySettingsService
from src.core.dependencies import SessionDep, GroupContextDep

router = APIRouter(
    prefix="/dspy",
    tags=["dspy"],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger(__name__)


# --- DSPy Models ---
class DSPyEnabledResponse(BaseModel):
    enabled: bool

class DSPyEnabledRequest(BaseModel):
    enabled: bool

class DSPyStatsResponse(BaseModel):
    enabled: bool
    total_optimizations: int
    active_signatures: int
    average_improvement: float
    last_optimization: Optional[str] = None
    examples_collected: int


# --- DSPy Endpoints ---

@router.get("/enabled", response_model=DSPyEnabledResponse)
async def get_dspy_enabled(session: SessionDep, group_context: GroupContextDep) -> DSPyEnabledResponse:
    """Get DSPy enabled status for the workspace."""
    try:
        svc = DSPySettingsService(session, group_id=group_context.primary_group_id if group_context else None)
        enabled = await svc.is_enabled()
        return DSPyEnabledResponse(enabled=enabled)
    except Exception as e:
        logger.error(f"Failed to get DSPy enabled status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/enabled", response_model=DSPyEnabledResponse)
async def set_dspy_enabled(payload: DSPyEnabledRequest, session: SessionDep, group_context: GroupContextDep) -> DSPyEnabledResponse:
    """Enable or disable DSPy for the workspace."""
    try:
        svc = DSPySettingsService(session, group_id=group_context.primary_group_id if group_context else None)
        await svc.set_enabled(payload.enabled)
        return DSPyEnabledResponse(enabled=payload.enabled)
    except Exception as e:
        logger.error(f"Failed to set DSPy enabled status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats", response_model=DSPyStatsResponse)
async def get_dspy_stats(session: SessionDep, group_context: GroupContextDep) -> DSPyStatsResponse:
    """Get DSPy optimization statistics for the workspace."""
    try:
        svc = DSPySettingsService(session, group_id=group_context.primary_group_id if group_context else None)
        enabled = await svc.is_enabled()

        # For now, return placeholder stats
        # TODO: Implement actual stats collection from dispatcher usage
        return DSPyStatsResponse(
            enabled=enabled,
            total_optimizations=0,
            active_signatures=0,
            average_improvement=0.0,
            last_optimization=None,
            examples_collected=0
        )
    except Exception as e:
        logger.error(f"Failed to get DSPy stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))