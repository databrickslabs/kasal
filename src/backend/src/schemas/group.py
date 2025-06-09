"""
Pydantic schemas for group management API.

These schemas define the request and response models for group-related endpoints.
"""
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field, EmailStr
from src.models.enums import GroupStatus, GroupUserRole, GroupUserStatus


class GroupBase(BaseModel):
    """Base group schema with common fields."""
    name: str = Field(..., description="Human-readable group name", min_length=1, max_length=255)
    email_domain: str = Field(..., description="Email domain for the group", min_length=1, max_length=255)
    description: Optional[str] = Field(None, description="Optional group description", max_length=1000)


class GroupCreateRequest(GroupBase):
    """Schema for creating a new group."""
    pass


class GroupUpdateRequest(BaseModel):
    """Schema for updating an existing group."""
    name: Optional[str] = Field(None, description="Human-readable group name", min_length=1, max_length=255)
    email_domain: Optional[str] = Field(None, description="Email domain for the group", min_length=1, max_length=255)
    description: Optional[str] = Field(None, description="Optional group description", max_length=1000)
    status: Optional[GroupStatus] = Field(None, description="Group status")


class GroupResponse(GroupBase):
    """Schema for group responses."""
    id: str = Field(..., description="Unique group identifier")
    status: GroupStatus = Field(..., description="Group status")
    auto_created: bool = Field(..., description="Whether group was auto-created")
    created_by_email: Optional[str] = Field(None, description="Email of user who created the group")
    created_at: datetime = Field(..., description="Group creation timestamp")
    updated_at: datetime = Field(..., description="Group last update timestamp")
    user_count: int = Field(..., description="Number of users in the group")

    class Config:
        from_attributes = True


class GroupUserBase(BaseModel):
    """Base group user schema with common fields."""
    role: GroupUserRole = Field(GroupUserRole.USER, description="User role in the group")
    status: GroupUserStatus = Field(GroupUserStatus.ACTIVE, description="User status in the group")


class GroupUserCreateRequest(BaseModel):
    """Schema for assigning a user to a group."""
    user_email: EmailStr = Field(..., description="Email of user to assign to group")
    role: GroupUserRole = Field(GroupUserRole.USER, description="Role to assign to user")


class GroupUserUpdateRequest(BaseModel):
    """Schema for updating a group user."""
    role: Optional[GroupUserRole] = Field(None, description="User role in the group")
    status: Optional[GroupUserStatus] = Field(None, description="User status in the group")


class GroupUserResponse(GroupUserBase):
    """Schema for group user responses."""
    id: str = Field(..., description="Unique group user identifier")
    group_id: str = Field(..., description="Group identifier")
    user_id: str = Field(..., description="User identifier")
    email: str = Field(..., description="User email address")
    joined_at: datetime = Field(..., description="When user joined the group")
    auto_created: bool = Field(..., description="Whether association was auto-created")
    created_at: datetime = Field(..., description="Association creation timestamp")
    updated_at: datetime = Field(..., description="Association last update timestamp")

    class Config:
        from_attributes = True


class GroupStatsResponse(BaseModel):
    """Schema for group statistics."""
    total_groups: int = Field(..., description="Total number of groups")
    active_groups: int = Field(..., description="Number of active groups")
    auto_created_groups: int = Field(..., description="Number of auto-created groups")
    manual_groups: int = Field(..., description="Number of manually created groups")
    total_users: int = Field(..., description="Total number of group users")
    active_users: int = Field(..., description="Number of active group users")

    class Config:
        from_attributes = True