from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional

from pydantic import BaseModel, ConfigDict, Field

from src.utils.sensitive_data_utils import mask_sensitive_strings


class ToolBase(BaseModel):
    """Base Tool schema with common attributes"""

    title: str = Field(..., description="Title of the tool")
    description: str = Field(..., description="Description of the tool's functionality")
    icon: str = Field(..., description="Icon identifier for the tool")
    config: Dict[str, Any] = Field(
        default_factory=dict, description="Configuration parameters for the tool"
    )
    enabled: bool = Field(default=True, description="Whether the tool is enabled")


class ToolCreate(ToolBase):
    """Schema for creating a new tool"""

    pass


class ToolUpdate(BaseModel):
    """Schema for updating an existing tool"""

    title: Optional[str] = Field(default=None, description="Title of the tool")
    description: Optional[str] = Field(
        default=None, description="Description of the tool's functionality"
    )
    icon: Optional[str] = Field(
        default=None, description="Icon identifier for the tool"
    )
    config: Optional[Dict[str, Any]] = Field(
        default=None, description="Configuration parameters for the tool"
    )
    enabled: Optional[bool] = Field(
        default=None, description="Whether the tool is enabled"
    )


class ToolResponse(ToolBase):
    """Schema for tool responses.

    ``config`` holds credentials as stored (client secrets, passwords, tokens).
    Read endpoints send members without builder rights ``masked_tool(...)``
    instead; see ``services/catalog/secret_view``.
    """

    id: int = Field(..., description="Unique identifier for the tool")
    group_id: Optional[str] = Field(
        default=None, description="Group ID if this is a workspace-specific override"
    )

    created_at: datetime = Field(..., description="Timestamp when the tool was created")
    updated_at: datetime = Field(
        ..., description="Timestamp when the tool was last updated"
    )

    model_config = ConfigDict(from_attributes=True)


def masked_config(config: Optional[Mapping[str, object]]) -> Dict[str, object]:
    """A copy of a tool config with every credential value masked."""
    masked = mask_sensitive_strings(dict(config or {}))
    return masked if isinstance(masked, dict) else {}


def masked_tool(tool: ToolResponse) -> ToolResponse:
    """A copy of ``tool`` with the credentials in its config masked."""
    return tool.model_copy(update={"config": masked_config(tool.config)})


class ToolListResponse(BaseModel):
    """Schema for list of tools response"""

    tools: List[ToolResponse] = Field(..., description="List of tools")
    count: int = Field(..., description="Total number of tools")


class ToggleResponse(BaseModel):
    """Schema for toggle enabled response"""

    message: str = Field(..., description="Success message")
    enabled: bool = Field(..., description="Current enabled state")
