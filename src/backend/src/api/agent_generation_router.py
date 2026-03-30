"""
API router for agent generation operations.

This module defines the FastAPI router for generating agents
using LLM models to convert natural language descriptions into
CrewAI agent configurations.
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter
from pydantic import BaseModel

from src.core.dependencies import GroupContextDep, SessionDep
from src.services.agent_generation_service import AgentGenerationService

# Configure logging
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
    prompt: AgentPrompt, group_context: GroupContextDep, session: SessionDep
):
    """
    Generate agent configuration from natural language description.

    This endpoint processes a natural language description of an agent
    and returns a structured configuration that can be used with CrewAI.

    Args:
        prompt: Request payload with prompt text, model, and optional tools
        group_context: Group context for multi-group isolation
        session: Database session from dependency injection

    Returns:
        Dict[str, Any]: Agent configuration in JSON format
    """
    # Create service instance with injected session
    service = AgentGenerationService(session)

    # Delegate to service layer
    return await service.generate_agent(
        prompt_text=prompt.prompt,
        model=prompt.model,
        tools=prompt.tools,
        group_context=group_context,
    )
