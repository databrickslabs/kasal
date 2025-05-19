from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any, Union
from pydantic import BaseModel, EmailStr, Field, field_validator, root_validator
import re

# Enums
class UserRole(str, Enum):
    ADMIN = "admin"
    TECHNICAL = "technical"
    REGULAR = "regular"

class UserStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    SUSPENDED = "suspended"

class IdentityProviderType(str, Enum):
    LOCAL = "local"
    OAUTH = "oauth"
    OIDC = "oidc"
    SAML = "saml"
    CUSTOM = "custom"

# Base schemas
class UserBase(BaseModel):
    username: str
    email: EmailStr

    @field_validator('username', mode='before')
    def username_validator(cls, v):
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('Username can only contain letters, numbers, underscores, and hyphens')
        if len(v) < 3 or len(v) > 50:
            raise ValueError('Username must be between 3 and 50 characters')
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

# User profile
class UserProfileBase(BaseModel):
    display_name: Optional[str] = None
    avatar_url: Optional[str] = None
    preferences: Optional[Dict[str, Any]] = None

class UserProfileCreate(UserProfileBase):
    pass

class UserProfileUpdate(UserProfileBase):
    pass

class UserProfileInDB(UserProfileBase):
    id: str
    user_id: str
    
    model_config = {
        "from_attributes": True
    }

# Role and privilege schemas
class PrivilegeBase(BaseModel):
    name: str  # Format: "resource:action"
    description: Optional[str] = None

class PrivilegeCreate(PrivilegeBase):
    pass

class PrivilegeUpdate(BaseModel):
    description: Optional[str] = None

class PrivilegeInDB(PrivilegeBase):
    id: str
    created_at: datetime
    
    model_config = {
        "from_attributes": True
    }

class RoleBase(BaseModel):
    name: str
    description: Optional[str] = None

class RoleCreate(RoleBase):
    privileges: List[str]  # List of privilege names

class RoleUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    privileges: Optional[List[str]] = None  # List of privilege names

class RoleInDB(RoleBase):
    id: str
    created_at: datetime
    updated_at: datetime
    
    model_config = {
        "from_attributes": True
    }

class RoleWithPrivileges(RoleInDB):
    privileges: List[PrivilegeInDB]

# Identity provider schemas
class IdentityProviderBase(BaseModel):
    name: str
    type: IdentityProviderType
    enabled: bool = True
    is_default: bool = False

class IdentityProviderConfig(BaseModel):
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    tenant_id: Optional[str] = None
    authorization_endpoint: Optional[str] = None
    token_endpoint: Optional[str] = None
    user_info_endpoint: Optional[str] = None
    scope: Optional[str] = None
    redirect_uri: Optional[str] = None
    cert: Optional[str] = None
    issuer: Optional[str] = None
    entry_point: Optional[str] = None
    role_mapping: Optional[Dict[str, Any]] = None

class IdentityProviderCreate(IdentityProviderBase):
    config: IdentityProviderConfig

class IdentityProviderUpdate(BaseModel):
    name: Optional[str] = None
    type: Optional[IdentityProviderType] = None
    enabled: Optional[bool] = None
    is_default: Optional[bool] = None
    config: Optional[IdentityProviderConfig] = None

class IdentityProviderInDB(IdentityProviderBase):
    id: str
    config: IdentityProviderConfig
    created_at: datetime
    updated_at: datetime
    
    model_config = {
        "from_attributes": True
    }

class IdentityProviderResponse(IdentityProviderInDB):
    pass

class IdentityProviderListResponse(BaseModel):
    providers: List[IdentityProviderResponse]
    total: int

class IdentityProviderUsageStatsResponse(BaseModel):
    provider_id: str
    provider_name: str
    user_count: int
    login_count: int
    last_login: Optional[datetime] = None
    active_users: Optional[int] = None

# External identity schemas
class ExternalIdentityBase(BaseModel):
    provider: str
    provider_user_id: str
    email: Optional[EmailStr] = None

class ExternalIdentityCreate(ExternalIdentityBase):
    profile_data: Optional[Dict[str, Any]] = None

class ExternalIdentityInDB(ExternalIdentityBase):
    id: str
    user_id: str
    profile_data: Optional[Dict[str, Any]] = None
    created_at: datetime
    last_login: Optional[datetime] = None
    
    model_config = {
        "from_attributes": True
    }

# User with complete info
class UserInDB(UserBase):
    id: str
    role: UserRole
    status: UserStatus
    created_at: datetime
    updated_at: datetime
    last_login: Optional[datetime] = None
    
    model_config = {
        "from_attributes": True
    }

class UserWithProfile(UserInDB):
    profile: Optional[UserProfileInDB] = None

class UserWithExternalIdentities(UserInDB):
    external_identities: List[ExternalIdentityInDB] = []

class UserComplete(UserInDB):
    profile: Optional[UserProfileInDB] = None
    external_identities: List[ExternalIdentityInDB] = []

# Role assignment
class UserRoleAssign(BaseModel):
    role_id: str

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