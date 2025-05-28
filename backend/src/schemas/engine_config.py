from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, Field


class EngineConfigBase(BaseModel):
    """Base schema with common engine configuration attributes."""
    engine_name: str = Field(..., description="Name of the engine (e.g., 'crewai')")
    engine_type: str = Field(..., description="Type of engine (e.g., 'workflow', 'ai', 'processing')")
    config_key: str = Field(..., description="Configuration key (e.g., 'flow_enabled')")
    config_value: str = Field(..., description="Configuration value (JSON string or simple value)")
    enabled: Optional[bool] = Field(True, description="Whether the configuration is enabled")
    description: Optional[str] = Field(None, description="Description of the configuration")


class EngineConfigCreate(EngineConfigBase):
    """Schema for creating a new engine configuration."""
    pass


class EngineConfigUpdate(BaseModel):
    """Schema for updating an existing engine configuration."""
    engine_type: Optional[str] = Field(None, description="Type of engine")
    config_key: Optional[str] = Field(None, description="Configuration key")
    config_value: Optional[str] = Field(None, description="Configuration value")
    enabled: Optional[bool] = Field(None, description="Whether the configuration is enabled")
    description: Optional[str] = Field(None, description="Description of the configuration")


class EngineConfigResponse(EngineConfigBase):
    """Schema for engine configuration responses."""
    id: int
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class EngineConfigToggleUpdate(BaseModel):
    """Schema for toggling engine configuration enabled status."""
    enabled: bool = Field(..., description="New enabled status")


class EngineConfigValueUpdate(BaseModel):
    """Schema for updating engine configuration value."""
    config_value: str = Field(..., description="New configuration value")


class EngineConfigListResponse(BaseModel):
    """Schema for list of engine configurations."""
    configs: List[EngineConfigResponse]
    count: int


class CrewAIFlowConfigUpdate(BaseModel):
    """Schema for updating CrewAI flow configuration."""
    flow_enabled: bool = Field(..., description="Whether flow feature is enabled") 