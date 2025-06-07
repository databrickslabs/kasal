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
    MANAGER = "manager"       # Can manage users and workflows
    USER = "user"            # Can execute workflows
    VIEWER = "viewer"        # Read-only access

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
    email_domain: Mapped[str] = mapped_column(String(255), nullable=False)  # e.g., "acme-corp.com"
    
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
    def generate_group_id(cls, email_domain: str, group_name: str = None) -> str:
        """
        Generate unique group ID from email domain and group name.
        
        Examples:
        - acme-corp.com, "Engineering Team" -> acme_corp_engineering_team_<uuid>
        - tech.startup.io, "Sales" -> tech_startup_io_sales_<uuid>
        """
        domain_part = email_domain.replace(".", "_").replace("-", "_").lower()
        
        if group_name:
            # Clean group name for ID use
            name_part = group_name.replace(" ", "_").replace("-", "_").lower()
            # Remove special characters
            name_part = "".join(c for c in name_part if c.isalnum() or c == "_")
            base_id = f"{domain_part}_{name_part}"
        else:
            base_id = domain_part
        
        # Add short UUID to ensure uniqueness
        short_uuid = str(uuid4())[:8]
        return f"{base_id}_{short_uuid}"
    
    @classmethod
    def generate_group_name(cls, email_domain: str) -> str:
        """
        Generate friendly group name from email domain.
        
        Examples:
        - acme-corp.com -> Acme Corp
        - tech.startup.io -> Tech Startup IO
        """
        # Remove .com, .io, etc. and replace separators with spaces
        name_part = email_domain.split('.')[0] if '.' in email_domain else email_domain
        return name_part.replace('-', ' ').replace('_', ' ').title()


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


# Import privilege constants
from src.models.privileges import Privileges

# Simple permission mapping for roles
GROUP_PERMISSIONS = {
    GroupUserRole.ADMIN: [
        # Group management (within group context)
        Privileges.GROUP_MANAGE_USERS,
        Privileges.GROUP_MANAGE_ROLES,
        # User management
        Privileges.USER_INVITE,
        Privileges.USER_REMOVE,
        Privileges.USER_UPDATE_ROLE,
        # Full resource access
        Privileges.AGENT_CREATE,
        Privileges.AGENT_READ,
        Privileges.AGENT_UPDATE,
        Privileges.AGENT_DELETE,
        Privileges.TASK_CREATE,
        Privileges.TASK_READ,
        Privileges.TASK_UPDATE,
        Privileges.TASK_DELETE,
        Privileges.CREW_CREATE,
        Privileges.CREW_READ,
        Privileges.CREW_UPDATE,
        Privileges.CREW_DELETE,
        Privileges.EXECUTION_READ,
        Privileges.EXECUTION_MANAGE,
        # Full configuration access for group admins
        Privileges.SETTINGS_READ,
        Privileges.SETTINGS_UPDATE,
        Privileges.TOOL_CREATE,
        Privileges.TOOL_READ,
        Privileges.TOOL_UPDATE,
        Privileges.TOOL_DELETE,
        Privileges.TOOL_CONFIGURE,
        Privileges.MODEL_CREATE,
        Privileges.MODEL_READ,
        Privileges.MODEL_UPDATE,
        Privileges.MODEL_DELETE,
        Privileges.MODEL_CONFIGURE,
        Privileges.MCP_CREATE,
        Privileges.MCP_READ,
        Privileges.MCP_UPDATE,
        Privileges.MCP_DELETE,
        Privileges.MCP_CONFIGURE,
        Privileges.API_KEY_CREATE,
        Privileges.API_KEY_READ,
        Privileges.API_KEY_UPDATE,
        Privileges.API_KEY_DELETE,
        Privileges.API_KEY_MANAGE
    ],
    GroupUserRole.MANAGER: [
        # User management
        Privileges.USER_INVITE,
        Privileges.USER_UPDATE_ROLE,
        # Resource management
        Privileges.AGENT_CREATE,
        Privileges.AGENT_READ,
        Privileges.AGENT_UPDATE,
        Privileges.AGENT_DELETE,
        Privileges.TASK_CREATE,
        Privileges.TASK_READ,
        Privileges.TASK_UPDATE,
        Privileges.TASK_DELETE,
        Privileges.CREW_CREATE,
        Privileges.CREW_READ,
        Privileges.CREW_UPDATE,
        Privileges.CREW_DELETE,
        Privileges.EXECUTION_READ,
        # Limited configuration access (read + configure existing)
        Privileges.TOOL_READ,
        Privileges.TOOL_CONFIGURE,
        Privileges.MODEL_READ,
        Privileges.MODEL_CONFIGURE,
        Privileges.MCP_READ,
        Privileges.SETTINGS_READ,
        Privileges.API_KEY_READ
    ],
    GroupUserRole.USER: [
        # Read and execute access
        Privileges.AGENT_READ,
        Privileges.TASK_READ,
        Privileges.TASK_EXECUTE,
        Privileges.CREW_READ,
        Privileges.CREW_EXECUTE,
        Privileges.EXECUTION_CREATE,
        Privileges.EXECUTION_READ,
        # Basic configuration read access
        Privileges.TOOL_READ,
        Privileges.MODEL_READ,
        Privileges.SETTINGS_READ
    ],
    GroupUserRole.VIEWER: [
        # Read-only access
        Privileges.AGENT_READ,
        Privileges.TASK_READ,
        Privileges.CREW_READ,
        Privileges.EXECUTION_READ,
        # Basic read access to configuration
        Privileges.TOOL_READ,
        Privileges.MODEL_READ,
        Privileges.SETTINGS_READ
    ]
}

# Legacy compatibility aliases removed - migration complete