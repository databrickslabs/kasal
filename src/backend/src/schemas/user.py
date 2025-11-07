from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any, Union
from pydantic import BaseModel, EmailStr, Field, field_validator, root_validator
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
        # Allow localhost domains for development
        if '@localhost' in v:
            return v
        # For other domains, use basic email regex validation
        if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', v):
            raise ValueError('Invalid email format')
        return v

# User registration and creation
class UserCreate(UserBase):
    password: str
    
    @field_validator('password', mode='before')
    def password_validator(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters')
        if not any(char.isdigit() for char in v):
            raise ValueError('Password must contain at least one digit')
        if not any(char.isupper() for char in v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not any(char.islower() for char in v):
            raise ValueError('Password must contain at least one lowercase letter')
        # Optional: check for special characters if required
        return v

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

# Password change
class PasswordChange(BaseModel):
    current_password: str
    new_password: str
    
    @field_validator('new_password', mode='before')
    def password_validator(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters')
        if not any(char.isdigit() for char in v):
            raise ValueError('Password must contain at least one digit')
        if not any(char.isupper() for char in v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not any(char.islower() for char in v):
            raise ValueError('Password must contain at least one lowercase letter')
        return v

# Password reset request
class PasswordResetRequest(BaseModel):
    email: EmailStr

# Password reset confirmation
class PasswordReset(BaseModel):
    token: str
    new_password: str
    
    @field_validator('new_password', mode='before')
    def password_validator(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters')
        if not any(char.isdigit() for char in v):
            raise ValueError('Password must contain at least one digit')
        if not any(char.isupper() for char in v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not any(char.islower() for char in v):
            raise ValueError('Password must contain at least one lowercase letter')
        return v

# Login
class UserLogin(BaseModel):
    username_or_email: str
    password: str

# Tokens
class Token(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"

# Token data
class TokenData(BaseModel):
    sub: str
    role: UserRole
    exp: int

# UserProfile schemas removed - display_name moved to User model

# Complex RBAC schemas removed - using simplified group-based roles

# Complex identity provider schemas removed - using simplified auth

# Complex external identity schemas removed - using simplified auth

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

# OAuth authorization
class OAuthAuthorize(BaseModel):
    provider: str
    redirect_uri: Optional[str] = None
    state: Optional[str] = None

# OAuth callback
class OAuthCallback(BaseModel):
    provider: str
    code: str
    state: Optional[str] = None
    redirect_uri: Optional[str] = None

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