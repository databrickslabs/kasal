"""
API router for agent generation operations.

This module defines the FastAPI router for generating agents
using LLM models to convert natural language descriptions into
CrewAI agent configurations.
"""

import logging
from typing import Dict, Any, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.services.agent_generation_service import AgentGenerationService

# Configure logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(
    prefix="/agent-generation",
    tags=["Agent Generation"],
    responses={404: {"description": "Not found"}},
)

class AgentPrompt(BaseModel):
    """Request model for agent generation."""
    prompt: str
    model: Optional[str] = "databricks-llama-4-maverick"
    tools: Optional[List[str]] = []


@router.post("/generate", response_model=Dict[str, Any])
async def generate_agent(
    prompt: AgentPrompt
):
    """
    Generate agent configuration from natural language description.
    
    This endpoint processes a natural language description of an agent
    and returns a structured configuration that can be used with CrewAI.
    
    Args:
        prompt: Request payload with prompt text, model, and optional tools
        
    Returns:
        Dict[str, Any]: Agent configuration in JSON format
    """
    try:
        # Create service instance
        service = AgentGenerationService.create()
        
        # Delegate to service layer
        return await service.generate_agent(
            prompt_text=prompt.prompt,
            model=prompt.model,
            tools=prompt.tools
        )
            
    except ValueError as e:
        # For validation errors
        logger.warning(f"Validation error in agent generation: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # For all other errors
        logger.error(f"Error generating agent: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to generate agent configuration") 