"""
Pydantic schemas for template generation operations.

This module defines schemas used for validating and structuring data
in template generation API requests and responses.
"""

from typing import Dict, Any, Optional
from pydantic import BaseModel, Field


class TemplateGenerationRequest(BaseModel):
    """Request schema for generating agent templates."""
    role: str = Field(..., description="Role of the agent")
    goal: str = Field(..., description="Goal of the agent")
    backstory: str = Field(..., description="Backstory of the agent")
    model: str = Field("databricks-llama-4-maverick", description="LLM model to use for template generation")


class TemplateGenerationResponse(BaseModel):
    """Response schema for template generation."""
    system_template: str = Field(..., description="System template for the agent")
    prompt_template: str = Field(..., description="Prompt template for the agent")
    response_template: str = Field(..., description="Response template for the agent") 