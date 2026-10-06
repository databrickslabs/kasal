"""UCMV correction-learning API.

Upload customer-corrected/deployed UC Metric View YAMLs → diff against our
original generated output (from conversion history) → distil a reusable
domain-context README. A direct analysis endpoint (no crew run), mirroring the
kpi-conversion router pattern.
"""

import logging
from typing import Annotated, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from src.dependencies.providers import GroupContextDep, SessionDep
from src.services.powerbi.correction_learning_service import (
    UCMVCorrectionLearningService,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/ucmv-learning", tags=["ucmv-learning"])


def get_learning_service(
    session: SessionDep, group_context: GroupContextDep = None
) -> UCMVCorrectionLearningService:
    return UCMVCorrectionLearningService(session, group_context=group_context)


LearningServiceDep = Annotated[
    UCMVCorrectionLearningService, Depends(get_learning_service)
]


class LearnRequest(BaseModel):
    """Corrected UCMV YAMLs keyed by view name, plus optional scoping."""

    corrected: Dict[str, str] = Field(
        ..., description="{view_name: corrected_UCMV_yaml} the customer deployed"
    )
    execution_id: Optional[str] = Field(
        None,
        description="Original Kasal run to diff against; omit to use the group's most recent conversion",
    )
    model_hint: str = Field("", description="Model/report name for prompt context")


class LearnResponse(BaseModel):
    readme: Optional[str] = None
    views_analyzed: int = 0
    views_with_changes: int = 0
    matched_views: List[str] = Field(default_factory=list)
    # view_name -> {matched_to, how} for name/measure-similarity matches.
    fuzzy_matched: Dict[str, Dict[str, str]] = Field(default_factory=dict)
    unmatched_views: List[str] = Field(default_factory=list)
    note: Optional[str] = None
    error: Optional[str] = None


@router.post("/learn", response_model=LearnResponse)
async def learn_from_corrections(
    request: LearnRequest,
    service: LearningServiceDep,
) -> LearnResponse:
    """Distil a domain-context README from customer-corrected UCMVs."""
    try:
        result = await service.learn(
            corrected=request.corrected,
            execution_id=request.execution_id,
            model_hint=request.model_hint,
        )
        return LearnResponse(**result)
    except Exception as e:  # noqa: BLE001
        logger.error(f"UCMV correction learning failed: {e}")
        raise HTTPException(
            status_code=500, detail=f"Correction learning failed: {str(e)}"
        )
