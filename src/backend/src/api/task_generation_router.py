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
from src.schemas.task_generation import TaskGenerationRequest, TaskGenerationResponse
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
