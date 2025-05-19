"""
API router for template generation operations.

This module provides API endpoints for generating agent templates
with proper validation and error handling.
"""

import logging
import json
from fastapi import APIRouter, HTTPException

from src.schemas.template_generation import TemplateGenerationRequest, TemplateGenerationResponse
from src.services.template_generation_service import TemplateGenerationService

# Configure logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(
    prefix="/template-generation",
    tags=["template generation"],
    responses={404: {"description": "Not found"}},
)

@router.post("/generate-templates", response_model=TemplateGenerationResponse)
async def generate_templates(
    request: TemplateGenerationRequest
):
    """
    Generate templates for an agent based on role, goal, and backstory.
    
    This endpoint creates system, prompt, and response templates
    tailored to the agent's specifications using an LLM.
    """
    try:
        # Create service using factory method
        template_generation_service = TemplateGenerationService.create()
        
        # Generate templates
        logger.info(f"Generating templates for agent role: {request.role}")
        templates_response = await template_generation_service.generate_templates(request)
        
        logger.info(f"Successfully generated templates for agent role: {request.role}")
        return templates_response
        
    except ValueError as e:
        # Handle validation errors with a 400 response for client errors
        # or 500 for server-side issues like missing templates
        if "not found in database" in str(e):
            error_msg = f"Server configuration error: {str(e)}"
            logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)
        else:
            error_msg = f"Invalid request or response: {str(e)}"
            logger.error(error_msg)
            raise HTTPException(status_code=400, detail=error_msg)
        
    except json.JSONDecodeError:
        # Handle JSON parsing errors
        error_msg = "Failed to parse AI response as JSON"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)
        
    except Exception as e:
        # Handle other errors with a 500 response
        error_msg = f"Error generating templates: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg) 