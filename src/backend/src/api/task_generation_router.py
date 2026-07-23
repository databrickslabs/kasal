"""
API router for task generation operations.

This module provides API endpoints for generating tasks using LLMs
with proper validation and error handling.
"""

import json
import logging

from fastapi import APIRouter

from src.core.exceptions import KasalError

from src.core.dependencies import GroupContextDep, SessionDep
from src.schemas.task_generation import (
    TaskGenerationRequest,
    TaskGenerationResponse,
    GuardrailSuggestionRequest,
    GuardrailSuggestionResponse,
)
from src.services.task_generation_service import TaskGenerationService

# Configure logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(
    prefix="/task-generation",
    tags=["task generation"],
    responses={404: {"description": "Not found"}},
)


@router.post("/generate-task", response_model=TaskGenerationResponse)
async def generate_task(
    request: TaskGenerationRequest, group_context: GroupContextDep, session: SessionDep
):
    """
    Generate a task based on the provided prompt and context.

    This endpoint creates a task based on the provided text prompt,
    with optional agent context for tailoring the task to a specific agent.
    """
    try:
        # Create service with injected session
        task_generation_service = TaskGenerationService(session)

        # Generate task
        logger.info(f"Generating task from prompt: {request.text[:50]}...")
        task_response = await task_generation_service.generate_task(
            request, group_context
        )

        logger.info(f"Generated task: {task_response.name}")
        return task_response

    except json.JSONDecodeError:
        # Handle JSON parsing errors
        error_msg = "Failed to parse AI response as JSON"
        logger.error(error_msg)
        raise KasalError(error_msg)


@router.post("/suggest-guardrail", response_model=GuardrailSuggestionResponse)
async def suggest_guardrail(
    request: GuardrailSuggestionRequest, group_context: GroupContextDep, session: SessionDep
):
    """
    Generate a suggested LLM-guardrail validation criteria for a task.

    On-demand only — invoked from the task form's "Suggest" button. Reads the
    task's description and expected output and returns one concise validation
    criteria sentence for the guardrail's description field. Never called during
    crew/task generation.
    """
    # CRITICAL: publish the request's group + user token to UserContext so
    # LLMManager resolves auth the same way the dispatcher does. Without the
    # user token, get_auth_context can't do OBO, and OpenAI-protocol models
    # (e.g. gpt-5-3-codex via the Responses API) fail with "OPENAI_API_KEY is
    # required" — standard models survive via litellm's SDK fallback, codex
    # does not. Mirrors dispatcher_router.
    from src.utils.user_context import UserContext
    if group_context:
        UserContext.set_group_context(group_context)
        if group_context.access_token:
            UserContext.set_user_token(group_context.access_token)

    service = TaskGenerationService(session)
    criteria = await service.suggest_guardrail(
        description=request.description,
        expected_output=request.expected_output,
        model=request.model,
        group_context=group_context,
    )
    return GuardrailSuggestionResponse(description=criteria)
