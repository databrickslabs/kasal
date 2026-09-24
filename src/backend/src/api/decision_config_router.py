"""The Jev switch applies only to the authenticated workspace."""

from fastapi import APIRouter

from src.core.exceptions import ForbiddenError
from src.core.permissions import is_workspace_admin
from src.dependencies.providers import GroupContextDep, SessionDep
from src.schemas.decision_config import (
    DecisionConfigResponse,
    DecisionConfigUpdate,
    DecisionRecommendationRequest,
    DecisionRecommendationResponse,
)
from src.services.decisions.settings import DecisionSettingsService

router = APIRouter(prefix="/decision-config", tags=["decision-config"])


@router.get("", response_model=DecisionConfigResponse)
async def get_config(session: SessionDep, group_context: GroupContextDep):
    return await DecisionSettingsService(session, group_context.primary_group_id).get()


@router.put("", response_model=DecisionConfigResponse)
async def update_config(
    config: DecisionConfigUpdate,
    session: SessionDep,
    group_context: GroupContextDep,
):
    if not is_workspace_admin(group_context):
        raise ForbiddenError("Only workspace admins can configure Jev")
    return await DecisionSettingsService(session, group_context.primary_group_id).save(
        config
    )


@router.post("/recommend", response_model=DecisionRecommendationResponse)
async def recommend_settings(
    body: DecisionRecommendationRequest,
    session: SessionDep,
    group_context: GroupContextDep,
):
    from src.services.decisions.recommendations import recommend

    return await recommend(session, group_context, body.prompt)
