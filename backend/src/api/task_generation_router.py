"""
API router for task generation operations.

This module provides API endpoints for generating tasks using LLMs
with proper validation and error handling.
"""

import logging
import json
from fastapi import APIRouter, HTTPException

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
    request: TaskGenerationRequest
):
    """
    Generate a task based on the provided prompt and context.
    
    This endpoint creates a task based on the provided text prompt,
    with optional agent context for tailoring the task to a specific agent.
    """
    try:
        # Create service using factory method
        task_generation_service = TaskGenerationService.create()
        
        # Generate task
        logger.info(f"Generating task from prompt: {request.text[:50]}...")
        task_response = await task_generation_service.generate_task(request)
        
        logger.info(f"Generated task: {task_response.name}")
        return task_response
        
    except ValueError as e:
        # Handle validation errors with a 400 response
        error_msg = f"Invalid request: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=400, detail=error_msg)
        
    except json.JSONDecodeError:
        # Handle JSON parsing errors
        error_msg = "Failed to parse AI response as JSON"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)
        
    except Exception as e:
        # Handle other errors with a 500 response
        error_msg = f"Error generating task: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg) 