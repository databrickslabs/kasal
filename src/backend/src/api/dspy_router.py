"""
DSPy configuration API router.

Provides endpoints for managing DSPy prompt optimization settings.
Simplified to focus on enable/disable toggle and basic stats.
"""

from typing import Annotated, Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from src.core.dependencies import GroupContextDep, SessionDep
from src.services.dspy_settings_service import DSPySettingsService

router = APIRouter(
    prefix="/dspy",
    tags=["dspy"],
    responses={404: {"description": "Not found"}},
)


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
async def get_dspy_enabled(
    session: SessionDep, group_context: GroupContextDep
) -> DSPyEnabledResponse:
    """Get DSPy enabled status for the workspace."""
    svc = DSPySettingsService(
        session, group_id=group_context.primary_group_id if group_context else None
    )
    enabled = await svc.is_enabled()
    return DSPyEnabledResponse(enabled=enabled)


@router.post("/enabled", response_model=DSPyEnabledResponse)
async def set_dspy_enabled(
    payload: DSPyEnabledRequest, session: SessionDep, group_context: GroupContextDep
) -> DSPyEnabledResponse:
    """Enable or disable DSPy for the workspace."""
    svc = DSPySettingsService(
        session, group_id=group_context.primary_group_id if group_context else None
    )
    await svc.set_enabled(payload.enabled)
    return DSPyEnabledResponse(enabled=payload.enabled)


@router.get("/stats", response_model=DSPyStatsResponse)
async def get_dspy_stats(
    session: SessionDep, group_context: GroupContextDep
) -> DSPyStatsResponse:
    """Get DSPy optimization statistics for the workspace."""
    svc = DSPySettingsService(
        session, group_id=group_context.primary_group_id if group_context else None
    )
    enabled = await svc.is_enabled()

    # For now, return placeholder stats
    # TODO: Implement actual stats collection from dispatcher usage
    return DSPyStatsResponse(
        enabled=enabled,
        total_optimizations=0,
        active_signatures=0,
        average_improvement=0.0,
        last_optimization=None,
        examples_collected=0,
    )
