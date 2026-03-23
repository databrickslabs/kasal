from datetime import datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, EmailStr, field_validator
import re

# Import enums from models to ensure consistency
from src.models.enums import UserRole, UserStatus

class IdentityProviderType(str, Enum):
    LOCAL = "local"
    OAUTH = "oauth"
    OIDC = "oidc"
    SAML = "saml"
    CUSTOM = "custom"

# Base schemas
class UserBase(BaseModel):
    username: str
    email: str  # Changed from EmailStr to str to allow localhost domains in development

    @field_validator('username', mode='before')
    def username_validator(cls, v):
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('Username can only contain letters, numbers, underscores, and hyphens')
        if len(v) < 3 or len(v) > 50:
            raise ValueError('Username must be between 3 and 50 characters')
        return v
    
    @field_validator('email', mode='before')
    def email_validator(cls, v):
        # Accept any stored value on read — partial emails may exist in the DB
        # from incremental header processing. Write-path validation is handled
        # by UserUpdate (EmailStr) and get_or_create_user_by_email.
        return v if v else ''

# User update
class UserUpdate(BaseModel):
    username: Optional[str] = None
    email: Optional[EmailStr] = None
    status: Optional[UserStatus] = None

    @field_validator('username', mode='before')
    def username_validator(cls, v):
        if v is None:
            return v
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('Username can only contain letters, numbers, underscores, and hyphens')
        if len(v) < 3 or len(v) > 50:
            raise ValueError('Username must be between 3 and 50 characters')
        return v


# User permission update (for system admins only)
class UserPermissionUpdate(BaseModel):
    is_system_admin: Optional[bool] = None
    is_personal_workspace_manager: Optional[bool] = None

# User with complete info
class UserInDB(UserBase):
    id: str
    display_name: Optional[str] = None  # Moved from UserProfile
    role: UserRole
    status: UserStatus
    is_system_admin: bool = False
    is_personal_workspace_manager: bool = False
    created_at: datetime
    updated_at: datetime
    last_login: Optional[datetime] = None

    model_config = {
        "from_attributes": True,
        "use_enum_values": True
    }

# UserWithProfile removed - display_name is now part of UserInDB

# Complex user auth schemas removed - using simplified auth

# Response schemas
class UserResponse(UserInDB):
    """User response schema for API endpoints."""
    pass

# Group schemas (for backward compatibility)
class GroupCreate(BaseModel):
    name: str
    description: Optional[str] = None

class GroupUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None

# Complex RBAC and Databricks role schemas removed - using simplified group-based roles