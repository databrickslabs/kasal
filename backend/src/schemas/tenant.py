"""
Pydantic schemas for tenant management.

Defines request/response models for the manual tenant management API.
"""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, EmailStr, Field, validator

from src.models.enums import TenantStatus, TenantUserRole, TenantUserStatus


class TenantBase(BaseModel):
    """Base tenant schema with common fields."""
    name: str = Field(..., min_length=1, max_length=255, description="Human-readable tenant name")
    email_domain: str = Field(..., min_length=1, max_length=255, description="Email domain or identifier for tenant")
    description: Optional[str] = Field(None, max_length=500, description="Optional description")


class TenantCreateRequest(TenantBase):
    """Request schema for creating a new tenant."""
    
    @validator('email_domain')
    def validate_email_domain(cls, v):
        """Validate email domain format."""
        if not v or len(v.strip()) == 0:
            raise ValueError('Email domain cannot be empty')
        
        # Allow both actual domains and virtual identifiers
        # e.g., "acme-corp.com", "team-alpha", "workspace-123"
        v = v.strip().lower()
        
        # Basic validation - no spaces, some reasonable characters
        if ' ' in v or any(char in v for char in ['<', '>', '"', "'"]):
            raise ValueError('Email domain contains invalid characters')
            
        return v
    
    @validator('name')
    def validate_name(cls, v):
        """Validate tenant name."""
        if not v or len(v.strip()) == 0:
            raise ValueError('Tenant name cannot be empty')
        return v.strip()


class TenantUpdateRequest(BaseModel):
    """Request schema for updating a tenant."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=500)
    status: Optional[TenantStatus] = None


class TenantResponse(TenantBase):
    """Response schema for tenant information."""
    id: str
    status: TenantStatus
    auto_created: bool
    created_by_email: Optional[str]
    created_at: datetime
    updated_at: datetime
    user_count: int = Field(0, description="Number of users in this tenant")
    
    class Config:
        from_attributes = True


class TenantUserBase(BaseModel):
    """Base tenant user schema."""
    role: TenantUserRole = Field(default=TenantUserRole.USER, description="User role in tenant")
    status: TenantUserStatus = Field(default=TenantUserStatus.ACTIVE, description="User status in tenant")


class TenantUserCreateRequest(BaseModel):
    """Request schema for assigning a user to a tenant."""
    user_email: EmailStr = Field(..., description="Email of user to assign")
    role: TenantUserRole = Field(default=TenantUserRole.USER, description="Role to assign")
    
    @validator('user_email')
    def validate_user_email(cls, v):
        """Validate user email."""
        if not v or len(v.strip()) == 0:
            raise ValueError('User email cannot be empty')
        return v.strip().lower()


class TenantUserUpdateRequest(BaseModel):
    """Request schema for updating a tenant user."""
    role: Optional[TenantUserRole] = None
    status: Optional[TenantUserStatus] = None


class TenantUserResponse(TenantUserBase):
    """Response schema for tenant user information."""
    id: str
    tenant_id: str
    user_id: str
    email: str = Field(..., description="User email address")
    joined_at: datetime
    auto_created: bool
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class TenantStatsResponse(BaseModel):
    """Response schema for tenant statistics."""
    total_tenants: int
    active_tenants: int
    total_users: int
    tenants_by_status: dict[str, int]
    recent_activity: list[dict]


class TenantListResponse(BaseModel):
    """Response schema for paginated tenant list."""
    tenants: list[TenantResponse]
    total: int
    skip: int
    limit: int


class TenantUserListResponse(BaseModel):
    """Response schema for paginated tenant user list."""
    users: list[TenantUserResponse]
    total: int
    skip: int
    limit: int