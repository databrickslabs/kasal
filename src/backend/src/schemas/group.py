"""
Pydantic schemas for group management API.

These schemas define the request and response models for group-related endpoints.
"""

import os
from datetime import datetime
from typing import List, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    TypeAdapter,
    field_validator,
)

from src.models.enums import GroupStatus, GroupUserRole, GroupUserStatus


def _is_local_dev() -> bool:
    """Local-dev environments issue synthetic emails (e.g. dev@localhost) that
    have no TLD. Mirror the ENVIRONMENT convention used in admin_auth."""
    return os.getenv("ENVIRONMENT", "development").lower() in (
        "development",
        "dev",
        "local",
    )


class GroupBase(BaseModel):
    """Base group schema with common fields."""

    name: str = Field(
        ..., description="Human-readable group name", min_length=1, max_length=255
    )
    description: Optional[str] = Field(
        None, description="Optional group description", max_length=1000
    )


class GroupCreateRequest(GroupBase):
    """Schema for creating a new group."""

    pass


class GroupUpdateRequest(BaseModel):
    """Schema for updating an existing group."""

    name: Optional[str] = Field(
        None, description="Human-readable group name", min_length=1, max_length=255
    )
    description: Optional[str] = Field(
        None, description="Optional group description", max_length=1000
    )
    status: Optional[GroupStatus] = Field(None, description="Group status")


# Backward compatibility aliases
GroupCreate = GroupCreateRequest
GroupUpdate = GroupUpdateRequest


class GroupResponse(GroupBase):
    """Schema for group responses."""

    id: str = Field(..., description="Unique group identifier")
    status: GroupStatus = Field(..., description="Group status")
    auto_created: bool = Field(..., description="Whether group was auto-created")
    created_by_email: Optional[str] = Field(
        None, description="Email of user who created the group"
    )
    created_at: datetime = Field(..., description="Group creation timestamp")
    updated_at: datetime = Field(..., description="Group last update timestamp")
    user_count: int = Field(..., description="Number of users in the group")

    model_config = ConfigDict(from_attributes=True)


class GroupWithRoleResponse(GroupResponse):
    """Schema for group responses that include the current user's role."""

    user_role: Optional[GroupUserRole] = Field(
        None, description="Current user's role in this group"
    )
    # EFFECTIVE surface capabilities for the current user in this group —
    # membership override when set, otherwise derived from the role
    # (operator -> False, editor/admin -> True). The frontend permission
    # store gates the Agent/Flow Builder surfaces on these.
    allow_agent_builder: Optional[bool] = Field(
        None, description="Whether this user may use the Agent Builder here"
    )
    allow_flow_builder: Optional[bool] = Field(
        None, description="Whether this user may use the Flow Builder here"
    )


class GroupUserBase(BaseModel):
    """Base group user schema with common fields."""

    role: GroupUserRole = Field(
        GroupUserRole.OPERATOR, description="User role in the group"
    )
    status: GroupUserStatus = Field(
        GroupUserStatus.ACTIVE, description="User status in the group"
    )


class GroupUserCreateRequest(BaseModel):
    """Schema for assigning a user to a group."""

    user_email: str = Field(..., description="Email of user to assign to group")
    role: GroupUserRole = Field(
        GroupUserRole.OPERATOR, description="Role to assign to user"
    )

    @field_validator("user_email")
    @classmethod
    def validate_user_email(cls, v: str) -> str:
        """Strict RFC email validation in production / Databricks Apps; in local
        dev also accept synthetic no-TLD emails (e.g. dev@localhost) that the app
        itself issues, so locally-created users can be assigned to workspaces."""
        v = (v or "").strip()
        if _is_local_dev():
            local, sep, domain = v.partition("@")
            if not sep or not local or not domain:
                raise ValueError("value is not a valid email address")
            return v
        return str(TypeAdapter(EmailStr).validate_python(v))


class GroupUserUpdateRequest(BaseModel):
    """Schema for updating a group user."""

    role: Optional[GroupUserRole] = Field(None, description="User role in the group")
    status: Optional[GroupUserStatus] = Field(
        None, description="User status in the group"
    )
    # Capability overrides. Omitted = untouched; explicit true/false = set.
    # (Reverting to role-derived requires clearing the column — deliberately
    # not expressible here yet; changing the role re-establishes intent.)
    allow_agent_builder: Optional[bool] = Field(
        None, description="Override: may use the Agent Builder in this group"
    )
    allow_flow_builder: Optional[bool] = Field(
        None, description="Override: may use the Flow Builder in this group"
    )


class GroupUserResponse(GroupUserBase):
    """Schema for group user responses."""

    id: str = Field(..., description="Unique group user identifier")
    group_id: str = Field(..., description="Group identifier")
    user_id: str = Field(..., description="User identifier")
    email: str = Field(..., description="User email address")
    allow_agent_builder: Optional[bool] = Field(
        None, description="Override: may use the Agent Builder (None = from role)"
    )
    allow_flow_builder: Optional[bool] = Field(
        None, description="Override: may use the Flow Builder (None = from role)"
    )
    joined_at: datetime = Field(..., description="When user joined the group")
    auto_created: bool = Field(..., description="Whether association was auto-created")
    created_at: datetime = Field(..., description="Association creation timestamp")
    updated_at: datetime = Field(..., description="Association last update timestamp")

    @field_validator("role", mode="before")
    @classmethod
    def migrate_legacy_roles(cls, v):
        """Automatically migrate legacy role values to new 3-tier system."""
        if isinstance(v, str):
            # Map old roles to new roles
            role_mapping = {
                "manager": "editor",
                "user": "operator",
                "viewer": "operator",
            }
            return role_mapping.get(v, v)
        return v

    model_config = ConfigDict(from_attributes=True)


class GroupStatsResponse(BaseModel):
    """Schema for group statistics."""

    total_groups: int = Field(..., description="Total number of groups")
    active_groups: int = Field(..., description="Number of active groups")
    auto_created_groups: int = Field(..., description="Number of auto-created groups")
    manual_groups: int = Field(..., description="Number of manually created groups")
    total_users: int = Field(..., description="Total number of group users")
    active_users: int = Field(..., description="Number of active group users")

    model_config = ConfigDict(from_attributes=True)
