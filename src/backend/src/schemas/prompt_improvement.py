"""
Pydantic schemas for prompt improvement operations.

This module defines schemas used for validating and structuring data
in prompt improvement API requests and responses (the form "Improve
with AI" buttons).
"""

from typing import Dict, Literal, Optional
from pydantic import BaseModel, Field


class PromptImprovementRequest(BaseModel):
    """Request to improve one or more prompt fields of an agent/task/template."""
    target: Literal["agent", "task", "template", "chat"] = Field(
        ..., description="What kind of configuration the fields belong to "
                         "('chat' improves a free-form chat request)"
    )
    fields: Dict[str, str] = Field(
        ...,
        description="Current prompt field texts keyed by field name, e.g. "
                    "{'role': ..., 'goal': ..., 'backstory': ...}",
        min_length=1,
    )
    instructions: Optional[str] = Field(
        None, description="Optional user guidance for the rewrite"
    )
    model: Optional[str] = Field(
        None, description="Optional LLM model for generating the improvement"
    )


class PromptImprovementResponse(BaseModel):
    """Improved prompt field texts, same keys as the request's fields."""
    fields: Dict[str, str] = Field(
        ..., description="Improved prompt field texts keyed by field name"
    )
