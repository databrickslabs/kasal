"""
Simple multi-tenant models for basic tenant isolation.

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

# Simple enums for tenant management
from enum import Enum

class TenantStatus(str, Enum):
    ACTIVE = "active"
    SUSPENDED = "suspended"
    ARCHIVED = "archived"

class TenantUserRole(str, Enum):
    ADMIN = "admin"           # Full control within tenant
    MANAGER = "manager"       # Can manage users and workflows
    USER = "user"            # Can execute workflows
    VIEWER = "viewer"        # Read-only access

class TenantUserStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    SUSPENDED = "suspended"


class Tenant(Base):
    """
    Simple tenant model for basic multi-tenant isolation.
    
    Each tenant represents an organization identified by email domain.
    Tenants are automatically created from user email domains.
    """
    __tablename__ = "tenants"
    
    # Primary identification
    id: Mapped[str] = mapped_column(String(100), primary_key=True)  # e.g., "acme_corp"
    name: Mapped[str] = mapped_column(String(255), nullable=False)  # e.g., "Acme Corporation"
    email_domain: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)  # e.g., "acme-corp.com"
    
    # Status and metadata
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="ACTIVE")
    description: Mapped[str] = mapped_column(String(500), nullable=True)
    
    # Auto-creation tracking
    auto_created: Mapped[bool] = mapped_column(Boolean, default=False)  # Was this tenant auto-created?
    created_by_email: Mapped[str] = mapped_column(String(255), nullable=True)  # Email of first user
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))
    
    # Relationships
    tenant_users = relationship("TenantUser", back_populates="tenant", cascade="all, delete-orphan")
    
    @classmethod
    def generate_tenant_id(cls, email_domain: str) -> str:
        """
        Generate tenant ID from email domain.
        
        Examples:
        - acme-corp.com -> acme_corp
        - tech.startup.io -> tech_startup_io
        """
        return email_domain.replace(".", "_").replace("-", "_").lower()
    
    @classmethod
    def generate_tenant_name(cls, email_domain: str) -> str:
        """
        Generate friendly tenant name from email domain.
        
        Examples:
        - acme-corp.com -> Acme Corp
        - tech.startup.io -> Tech Startup IO
        """
        # Remove .com, .io, etc. and replace separators with spaces
        name_part = email_domain.split('.')[0] if '.' in email_domain else email_domain
        return name_part.replace('-', ' ').replace('_', ' ').title()


class TenantUser(Base):
    """
    Simple tenant user membership model.
    
    Links existing users to tenants with role-based access control.
    """
    __tablename__ = "tenant_users"
    
    id: Mapped[str] = mapped_column(String(100), primary_key=True, default=generate_uuid)
    tenant_id: Mapped[str] = mapped_column(String(100), ForeignKey("tenants.id"), nullable=False)
    user_id: Mapped[str] = mapped_column(String(255), ForeignKey("users.id"), nullable=False)
    
    # Role and status within tenant
    role: Mapped[str] = mapped_column(String(50), nullable=False, default="USER")
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="ACTIVE")
    
    # Membership tracking
    joined_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    auto_created: Mapped[bool] = mapped_column(Boolean, default=False)  # Was this membership auto-created?
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))
    
    # Relationships
    tenant = relationship("Tenant", back_populates="tenant_users")
    user = relationship("User")
    
    # Unique constraint: one user can only have one membership per tenant
    __table_args__ = (
        {'mysql_engine': 'InnoDB'},
    )

    def __repr__(self):
        return f"<TenantUser(tenant_id='{self.tenant_id}', user_id='{self.user_id}', role='{self.role}')>"


# Simple permission mapping for roles
TENANT_PERMISSIONS = {
    TenantUserRole.ADMIN: [
        "tenant:manage",
        "user:invite", "user:remove", "user:update_role",
        "agent:create", "agent:update", "agent:delete", "agent:read",
        "task:create", "task:update", "task:delete", "task:read", 
        "crew:create", "crew:update", "crew:delete", "crew:read",
        "execution:read", "execution:manage",
        "settings:update"
    ],
    TenantUserRole.MANAGER: [
        "user:invite", "user:update_role",
        "agent:create", "agent:update", "agent:delete", "agent:read",
        "task:create", "task:update", "task:delete", "task:read",
        "crew:create", "crew:update", "crew:delete", "crew:read", 
        "execution:read"
    ],
    TenantUserRole.USER: [
        "agent:read",
        "task:read", "task:execute",
        "crew:read", "crew:execute",
        "execution:create", "execution:read"
    ],
    TenantUserRole.VIEWER: [
        "agent:read",
        "task:read", 
        "crew:read",
        "execution:read"
    ]
}