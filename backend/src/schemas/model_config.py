from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, Field


class ModelConfigBase(BaseModel):
    """Base schema with common model configuration attributes."""
    key: str = Field(..., description="Unique identifier for the model")
    name: str = Field(..., description="Display name of the model")
    provider: Optional[str] = Field(None, description="Provider of the model (e.g., 'openai', 'anthropic')")
    temperature: Optional[float] = Field(None, description="Temperature setting for generation")
    context_window: Optional[int] = Field(None, description="Maximum context window size in tokens")
    max_output_tokens: Optional[int] = Field(None, description="Maximum output tokens allowed")
    extended_thinking: Optional[bool] = Field(False, description="Whether extended thinking is enabled")
    enabled: Optional[bool] = Field(True, description="Whether the model is enabled")


class ModelConfigCreate(ModelConfigBase):
    """Schema for creating a new model configuration."""
    pass


class ModelConfigUpdate(ModelConfigBase):
    """Schema for updating an existing model configuration."""
    pass


class ModelConfigResponse(ModelConfigBase):
    """Schema for model configuration responses."""
    id: int
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class ModelToggleUpdate(BaseModel):
    """Schema for toggling model enabled status."""
    enabled: bool = Field(..., description="New enabled status")


class ModelListResponse(BaseModel):
    """Schema for list of model configurations."""
    models: List[ModelConfigResponse]
    count: int 