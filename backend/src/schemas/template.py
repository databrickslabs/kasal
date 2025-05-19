from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, Field


class PromptTemplateBase(BaseModel):
    """Base schema with common prompt template attributes."""
    name: str = Field(..., description="Name of the prompt template")
    description: Optional[str] = Field(None, description="Description of the prompt template")
    template: str = Field(..., description="The prompt template text")
    is_active: bool = Field(True, description="Whether the template is active")


class PromptTemplateCreate(PromptTemplateBase):
    """Schema for creating a new prompt template."""
    pass


class PromptTemplateUpdate(BaseModel):
    """Schema for updating an existing prompt template."""
    name: Optional[str] = Field(None, description="Name of the prompt template")
    description: Optional[str] = Field(None, description="Description of the prompt template")
    template: Optional[str] = Field(None, description="The prompt template text")
    is_active: Optional[bool] = Field(None, description="Whether the template is active")


class PromptTemplateResponse(PromptTemplateBase):
    """Schema for prompt template responses."""
    id: int
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class TemplateListResponse(BaseModel):
    """Schema for list of prompt templates."""
    templates: List[PromptTemplateResponse]
    count: int


class ResetResponse(BaseModel):
    """Schema for reset response."""
    message: str
    reset_count: int 