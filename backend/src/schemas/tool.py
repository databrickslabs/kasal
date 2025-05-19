from typing import Dict, Any, Optional, List, ClassVar
from datetime import datetime
from pydantic import BaseModel, Field


class ToolBase(BaseModel):
    """Base Tool schema with common attributes"""
    title: str = Field(..., description="Title of the tool")
    description: str = Field(..., description="Description of the tool's functionality")
    icon: str = Field(..., description="Icon identifier for the tool")
    config: Dict[str, Any] = Field(default_factory=dict, description="Configuration parameters for the tool")
    enabled: bool = Field(default=True, description="Whether the tool is enabled")


class ToolCreate(ToolBase):
    """Schema for creating a new tool"""
    pass


class ToolUpdate(BaseModel):
    """Schema for updating an existing tool"""
    title: Optional[str] = Field(default=None, description="Title of the tool")
    description: Optional[str] = Field(default=None, description="Description of the tool's functionality")
    icon: Optional[str] = Field(default=None, description="Icon identifier for the tool")
    config: Optional[Dict[str, Any]] = Field(default=None, description="Configuration parameters for the tool")
    enabled: Optional[bool] = Field(default=None, description="Whether the tool is enabled")


class ToolResponse(ToolBase):
    """Schema for tool responses"""
    id: int = Field(..., description="Unique identifier for the tool")
    created_at: datetime = Field(..., description="Timestamp when the tool was created")
    updated_at: datetime = Field(..., description="Timestamp when the tool was last updated")

    model_config: ClassVar[Dict[str, Any]] = {
        "from_attributes": True
    }


class ToolListResponse(BaseModel):
    """Schema for list of tools response"""
    tools: List[ToolResponse] = Field(..., description="List of tools")
    count: int = Field(..., description="Total number of tools")


class ToggleResponse(BaseModel):
    """Schema for toggle enabled response"""
    message: str = Field(..., description="Success message")
    enabled: bool = Field(..., description="Current enabled state") 