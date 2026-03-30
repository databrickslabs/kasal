"""
Simple multi-group models for basic group-based isolation.

This foundation can be incrementally enhanced with Unity Catalog and SCIM integration.
"""

from datetime import datetime, timezone
from sqlalchemy import Column, String, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import relationship, Mapped, mapped_column
from sqlalchemy import Enum as SQLAlchemyEnum
from src.db.base import Base
from uuid import uuid4

def generate_uuid():
    return str(uuid4())

# Simple enums for group management
from enum import Enum

class GroupStatus(str, Enum):
    ACTIVE = "active"
    SUSPENDED = "suspended"
    ARCHIVED = "archived"

class GroupUserRole(str, Enum):
    ADMIN = "admin"           # Full control within group
    EDITOR = "editor"         # Can build and modify workflows
    OPERATOR = "operator"     # Can execute workflows and monitor

class GroupUserStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    SUSPENDED = "suspended"


class Group(Base):
    """
    Simple group model for basic multi-group isolation.
    
    Each group represents an organization identified by email domain.
    Groups are automatically created from user email domains.
    """
    __tablename__ = "groups"
    __table_args__ = {'extend_existing': True}
    
    # Primary identification
    id: Mapped[str] = mapped_column(String(100), primary_key=True)  # e.g., "acme_corp"
    name: Mapped[str] = mapped_column(String(255), nullable=False)  # e.g., "Acme Corporation"
    
    # Status and metadata
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="ACTIVE")
    description: Mapped[str] = mapped_column(String(500), nullable=True)
    
    # Auto-creation tracking
    auto_created: Mapped[bool] = mapped_column(Boolean, default=False)  # Was this group auto-created?
    created_by_email: Mapped[str] = mapped_column(String(255), nullable=True)  # Email of first user
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))
    
    # Relationships
    group_users = relationship("GroupUser", back_populates="group", cascade="all, delete-orphan")
    
    @classmethod
    def generate_group_id(cls, group_name: str) -> str:
        """
        Generate unique group ID from group name.

        Examples:
        - "Engineering Team" -> engineering_team_<uuid>
        - "Sales" -> sales_<uuid>
        """
        # Clean group name for ID use
        name_part = group_name.replace(" ", "_").replace("-", "_").lower()
        # Remove special characters
        name_part = "".join(c for c in name_part if c.isalnum() or c == "_")

        # Add short UUID to ensure uniqueness
        short_uuid = str(uuid4())[:8]
        return f"{name_part}_{short_uuid}"


class GroupUser(Base):
    """
    Simple group user membership model.
    
    Links existing users to groups with role-based access control.
    """
    __tablename__ = "group_users"
    __table_args__ = {'extend_existing': True}
    
    id: Mapped[str] = mapped_column(String(100), primary_key=True, default=generate_uuid)
    group_id: Mapped[str] = mapped_column(String(100), ForeignKey("groups.id"), nullable=False)
    user_id: Mapped[str] = mapped_column(String(255), ForeignKey("users.id"), nullable=False)
    
    # Role and status within group
    role: Mapped[str] = mapped_column(String(50), nullable=False, default="USER")
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="ACTIVE")
    
    # Membership tracking
    joined_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    auto_created: Mapped[bool] = mapped_column(Boolean, default=False)  # Was this membership auto-created?
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))
    
    # Relationships
    group = relationship("Group", back_populates="group_users")
    user = relationship("User", foreign_keys=[user_id])
    
    # Unique constraint: one user can only have one membership per group
    __table_args__ = (
        {'mysql_engine': 'InnoDB', 'extend_existing': True},
    )

    def __repr__(self):
        return f"<GroupUser(group_id='{self.group_id}', user_id='{self.user_id}', role='{self.role}')>"


# Simplified role-based access - permissions handled by decorators in src.core.permissions
# Role hierarchy: admin > editor > operator

def get_role_hierarchy(role: GroupUserRole) -> int:
    """
    Get the hierarchy level for a role.
    Higher numbers = more permissions.

    Returns:
        int: Hierarchy level (3=admin, 2=editor, 1=operator)
    """
    hierarchy = {
        GroupUserRole.ADMIN: 3,     # Full access including user/group management
        GroupUserRole.EDITOR: 2,    # Can create/edit workflows, execute
        GroupUserRole.OPERATOR: 1   # Can execute and monitor only
    }
    return hierarchy.get(role, 0)

def role_has_access(user_role: GroupUserRole, required_role: GroupUserRole) -> bool:
    """
    Check if user role has sufficient access level for the required role.

    Args:
        user_role: The user's current role
        required_role: The minimum role required for the action

    Returns:
        bool: True if user has sufficient access
    """
    return get_role_hierarchy(user_role) >= get_role_hierarchy(required_role)

# Legacy compatibility aliases removed - migration complete