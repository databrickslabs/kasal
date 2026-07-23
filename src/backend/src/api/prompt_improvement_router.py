"""
API router for prompt improvement operations.

Endpoint behind the form "Improve with AI" buttons: rewrites an agent's
or task's prompt fields using prompt-engineering best practices.
"""

import logging

from fastapi import APIRouter

from src.core.dependencies import GroupContextDep, SessionDep
from src.schemas.prompt_improvement import (
    PromptImprovementRequest,
    PromptImprovementResponse,
)
from src.services.prompt_improvement_service import PromptImprovementService

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/prompt-improvement",
    tags=["prompt improvement"],
    responses={404: {"description": "Not found"}},
)


@router.post("/improve", response_model=PromptImprovementResponse)
async def improve_prompt(
    request: PromptImprovementRequest, group_context: GroupContextDep, session: SessionDep
):
    """
    Improve prompt fields of an agent, task, or template as one coherent set.

    On-demand only — invoked from the agent/task forms' "Improve with AI"
    buttons. Never called during crew/task generation or execution.
    """
    # CRITICAL: publish the request's group + user token to UserContext so
    # LLMManager resolves auth the same way the dispatcher does. Without the
    # user token, get_auth_context can't do OBO, and OpenAI-protocol models
    # (e.g. gpt-5-3-codex via the Responses API) fail with "OPENAI_API_KEY is
    # required". Mirrors task_generation_router's suggest_guardrail.
    from src.utils.user_context import UserContext
    if group_context:
        UserContext.set_group_context(group_context)
        if group_context.access_token:
            UserContext.set_user_token(group_context.access_token)

    service = PromptImprovementService(session)
    improved = await service.improve_prompt(
        target=request.target,
        fields=request.fields,
        instructions=request.instructions,
        model=request.model,
        group_context=group_context,
    )
    return PromptImprovementResponse(fields=improved)
