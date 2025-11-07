from datetime import datetime
from typing import Dict, Any, Optional, List, ClassVar

from pydantic import BaseModel, Field


class GroupToolBase(BaseModel):
    tool_id: int = Field(..., description="ID of the global tool")
    enabled: bool = Field(default=False, description="Enabled for this group")
    config: Dict[str, Any] = Field(default_factory=dict, description="Group-scoped configuration")


class GroupToolCreate(GroupToolBase):
    pass


class GroupToolUpdate(BaseModel):
    enabled: Optional[bool] = Field(default=None)
    config: Optional[Dict[str, Any]] = Field(default=None)


class GroupToolResponse(GroupToolBase):
    id: int = Field(...)
    group_id: str = Field(..., description="Group ID")
    credentials_status: str = Field(..., description="Credential validation status")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)

    model_config: ClassVar[Dict[str, Any]] = {"from_attributes": True}


class GroupToolListResponse(BaseModel):
    items: List[GroupToolResponse] = Field(...)
    count: int = Field(...)

