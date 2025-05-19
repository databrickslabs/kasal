from enum import Enum

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