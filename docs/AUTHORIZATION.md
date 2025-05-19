# Kasal Platform Authorization Model

## Table of Contents
- [Overview](#overview)
- [User Model](#user-model)
- [Role-Based Access Control](#role-based-access-control)
- [Permission Model](#permission-model)
- [Authentication Flow](#authentication-flow)
- [Authorization Flow](#authorization-flow)
- [API Security](#api-security)
- [Frontend Implementation](#frontend-implementation)
- [Backend Implementation](#backend-implementation)
- [Identity Provider Integration](#identity-provider-integration)
- [Role and Privilege Configuration](#role-and-privilege-configuration)
- [Testing Strategy](#testing-strategy)

## Overview

The Kasal Platform implements a comprehensive Role-Based Access Control (RBAC) system to manage user permissions and secure access to resources. This document outlines the authorization model and serves as the reference for both frontend and backend implementation.

The system is designed with three primary user roles (Admin, Technical, Regular) with predefined permissions for each role. The authentication is implemented using JWT tokens with refresh token capabilities.

## User Model

### Core User Entity

```typescript
interface User {
  id: string;              // Unique identifier
  username: string;        // Username for login
  email: string;           // User email address
  password: string;        // Hashed password (never returned to client)
  role: UserRole;          // User role (enum)
  status: UserStatus;      // Account status
  created_at: string;      // Creation timestamp
  updated_at: string;      // Last update timestamp
  last_login: string;      // Last login timestamp
}

enum UserRole {
  ADMIN = 'admin',
  TECHNICAL = 'technical',
  REGULAR = 'regular'
}

enum UserStatus {
  ACTIVE = 'active',
  INACTIVE = 'inactive',
  SUSPENDED = 'suspended'
}
```

### User Profile

Additional user information stored separately from core auth data:

```typescript
interface UserProfile {
  user_id: string;         // Reference to User
  display_name: string;    // User's displayed name
  avatar_url: string;      // Profile picture URL
  preferences: object;     // User preferences (JSON)
}
```

### External Identity Support

The system now supports linking user accounts with external identity providers:

```typescript
interface ExternalIdentity {
  id: string;              // Unique identifier
  user_id: string;         // Reference to User
  provider: string;        // Provider name
  provider_user_id: string; // User ID in external system
  email: string;           // Email from provider
  profile_data: string;    // JSON profile data from provider
  created_at: string;      // Creation timestamp
  last_login: string;      // Last login timestamp
}
```

## Role-Based Access Control

### User Roles

1. **Admin**
   - Full access to all system features
   - Can manage users (create, update, delete)
   - Can assign roles to users
   - Can configure system settings
   - Can perform all actions of Technical and Regular users

2. **Technical**
   - Can create and manage agents and tasks
   - Can configure workflows
   - Can view system logs and metrics
   - Can perform all actions of Regular users
   - Cannot manage users or change system settings

3. **Regular**
   - Can run predefined agents and tasks
   - Can view results and logs of their executions
   - Cannot create new agents or tasks
   - Cannot access system configuration

### Custom Roles

The system now supports custom roles with specific privilege sets:

```typescript
interface Role {
  id: string;              // Unique identifier
  name: string;            // Role name
  description: string;     // Role description
  created_at: string;      // Creation timestamp
  updated_at: string;      // Last update timestamp
  privileges: string[];    // Array of privilege names
}
```

## Permission Model

### Permission Structure

Permissions are defined as action-resource pairs and grouped by role.

```typescript
interface Permission {
  resource: string;       // Resource type (e.g., "agent", "task", "user")
  action: string;         // Action (e.g., "create", "read", "update", "delete", "execute")
}

// Examples of permissions
const permissions = {
  "agent:create": ["admin", "technical"],
  "agent:read": ["admin", "technical", "regular"],
  "agent:update": ["admin", "technical"],
  "agent:delete": ["admin"],
  "agent:execute": ["admin", "technical", "regular"],
  
  "task:create": ["admin", "technical"],
  "task:read": ["admin", "technical", "regular"],
  "task:update": ["admin", "technical"],
  "task:delete": ["admin"],
  "task:execute": ["admin", "technical", "regular"],
  
  "user:create": ["admin"],
  "user:read": ["admin"],
  "user:update": ["admin"],
  "user:delete": ["admin"],
  
  "system:configure": ["admin"]
}
```

### Privileges Model

The system implements a flexible privilege system:

```typescript
interface Privilege {
  id: string;              // Unique identifier
  name: string;            // Privilege name (format: "resource:action")
  description: string;     // Privilege description
  created_at: string;      // Creation timestamp
}

interface RolePrivilege {
  id: string;              // Unique identifier
  role_id: string;         // Reference to Role
  privilege_id: string;    // Reference to Privilege
}
```

### Permission Checking

Permissions are checked using:
1. Role-based checks (simpler, default approach)
2. Granular permission checks via the new RoleService

## Authentication Flow

### Registration

1. User submits registration form with username, email, password
2. System validates input and checks for existing users
3. Password is hashed using bcrypt
4. New user is created with default "regular" role
5. Welcome email is sent
6. User is redirected to login page

### Login

1. User submits login form with username/email and password
2. System validates credentials
3. If valid, system generates:
   - Access token (short-lived JWT, 15 minutes)
   - Refresh token (longer-lived JWT, 7 days)
4. Tokens are returned to client
5. Client stores tokens (access token in memory, refresh token in httpOnly cookie)

### External Provider Authentication

1. User selects an identity provider (Google, GitHub, etc.)
2. User is redirected to the provider's authentication page
3. After successful authentication, provider redirects back with authorization code
4. Backend exchanges code for user information
5. If user with linked identity exists, they are logged in
6. If user with matching email exists, identity is linked to their account
7. Otherwise, a new user account is created and linked to the identity

### Token Refresh

1. When access token expires, client uses refresh token to get new access token
2. If refresh token is valid, new access token is issued
3. If refresh token is invalid or expired, user is logged out and redirected to login

### Logout

1. Client clears tokens from storage
2. Refresh token is invalidated on server
3. User is redirected to login page

## Authorization Flow

### API Request Authorization

1. Client includes access token in Authorization header
2. Backend middleware validates token signature and expiration
3. If token is valid, user information is extracted and attached to request
4. Route handlers check user role against required permissions
5. Request is processed or rejected based on permissions

### Frontend Authorization

1. Protected routes check user authentication status
2. UI components are conditionally rendered based on user permissions
3. Action buttons are disabled or hidden based on user permissions

## API Security

### Token Security

- Access tokens expire quickly (15 minutes)
- Refresh tokens have longer life but are securely stored
- All tokens are signed with a server secret
- Token rotation on suspicious activity

### API Endpoints Security

1. **Public Endpoints** (no auth required)
   - `/auth/login`
   - `/auth/register`
   - `/auth/forgot-password`
   - `/auth/reset-password`

2. **Protected Endpoints** (require authentication)
   - `/auth/refresh-token`
   - `/auth/logout`
   - `/users/me`

3. **Role-Restricted Endpoints** (require specific roles)
   - `/users` (admin only)
   - `/roles` (admin only)
   - `/identity-providers` (admin only)
   - `/agents/create` (admin, technical)
   - `/tasks/create` (admin, technical)

### Security Headers

- CORS configuration to restrict allowed origins
- Content-Security-Policy to prevent XSS
- X-Content-Type-Options to prevent MIME sniffing
- X-Frame-Options to prevent clickjacking

## Frontend Implementation

### Authentication Store (Zustand)

```typescript
// authStore.ts structure
interface AuthState {
  user: User | null;
  token: string | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  error: string | null;
  
  login: (email: string, password: string) => Promise<void>;
  logout: () => void;
  register: (userData: RegisterData) => Promise<void>;
  refreshToken: () => Promise<boolean>;
  clearError: () => void;
  
  hasPermission: (permission: string) => boolean;
  isAdmin: () => boolean;
  isTechnical: () => boolean;
}
```

### Protected Routes

```tsx
// Example of protected route implementation
const ProtectedRoute = ({ children, requiredRole }) => {
  const { isAuthenticated, hasPermission } = useAuth();
  
  if (!isAuthenticated) {
    return <Navigate to="/login" />;
  }
  
  if (requiredRole && !hasPermission(requiredRole)) {
    return <Navigate to="/unauthorized" />;
  }
  
  return children;
};
```

### UI Access Control

```tsx
// Example of conditional rendering based on permissions
const ActionsMenu = () => {
  const { isAdmin, isTechnical } = useAuth();
  
  return (
    <Menu>
      {/* Available to all authenticated users */}
      <MenuItem>View Dashboard</MenuItem>
      
      {/* Only for technical and admin users */}
      {isTechnical() && (
        <>
          <MenuItem>Create Agent</MenuItem>
          <MenuItem>Create Task</MenuItem>
        </>
      )}
      
      {/* Only for admin users */}
      {isAdmin() && (
        <>
          <MenuItem>Manage Users</MenuItem>
          <MenuItem>System Settings</MenuItem>
        </>
      )}
    </Menu>
  );
};
```

## Backend Implementation

### Database Models

User model with relationships to roles and permissions:

```python
# SQLAlchemy model example
class User(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True, default=generate_uuid)
    username = Column(String, unique=True, index=True, nullable=False)
    email = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    role = Column(Enum("admin", "technical", "regular"), default="regular")
    status = Column(Enum("active", "inactive", "suspended"), default="active")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_login = Column(DateTime, nullable=True)

    # Relationships
    profile = relationship("UserProfile", back_populates="user", uselist=False)
    refresh_tokens = relationship("RefreshToken", back_populates="user")
    external_identities = relationship("ExternalIdentity", back_populates="user")

class Role(Base):
    __tablename__ = "roles"

    id = Column(String, primary_key=True, default=generate_uuid)
    name = Column(String, nullable=False, unique=True)
    description = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    role_privileges = relationship("RolePrivilege", back_populates="role")

class Privilege(Base):
    __tablename__ = "privileges"

    id = Column(String, primary_key=True, default=generate_uuid)
    name = Column(String, nullable=False, unique=True)
    description = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    role_privileges = relationship("RolePrivilege", back_populates="privilege")

class RolePrivilege(Base):
    __tablename__ = "role_privileges"

    id = Column(String, primary_key=True, default=generate_uuid)
    role_id = Column(String, ForeignKey("roles.id", ondelete="CASCADE"))
    privilege_id = Column(String, ForeignKey("privileges.id", ondelete="CASCADE"))
    
    # Relationships
    role = relationship("Role", back_populates="role_privileges")
    privilege = relationship("Privilege", back_populates="role_privileges")
```

### Role Service

The new RoleService provides comprehensive role and privilege management:

```python
class RoleService:
    """Service for role management operations"""
    
    def __init__(self, session: AsyncSession):
        self.session = session
        self.role_repo = RoleRepository(Role, session)
        self.privilege_repo = PrivilegeRepository(Privilege, session)
        self.role_privilege_repo = RolePrivilegeRepository(RolePrivilege, session)
    
    async def get_role(self, role_id: str) -> Optional[Role]:
        """Get a role by ID"""
        return await self.role_repo.get(role_id)
    
    async def get_role_with_privileges(self, role_id: str) -> Optional[Dict[str, Any]]:
        """Get a role with its privileges"""
        # Implementation...
    
    async def create_role(self, role_data: RoleCreate) -> Dict[str, Any]:
        """Create a new role with privileges"""
        # Implementation...
    
    async def update_role(self, role_id: str, role_data: RoleUpdate) -> Optional[Dict[str, Any]]:
        """Update a role and its privileges"""
        # Implementation...
    
    async def delete_role(self, role_id: str) -> bool:
        """Delete a role"""
        # Implementation...
    
    async def check_role_has_privilege(self, role_id: str, privilege_name: str) -> bool:
        """Check if a role has a specific privilege"""
        # Implementation...
```

### Identity Provider Service

The new IdentityProviderService manages external authentication providers:

```python
class IdentityProviderService:
    """Service for identity provider management operations"""
    
    def __init__(self, session: AsyncSession):
        self.session = session
        self.provider_repo = IdentityProviderRepository(IdentityProvider, session)
        self.external_identity_repo = ExternalIdentityRepository(ExternalIdentity, session)
    
    async def get_provider(self, provider_id: str) -> Optional[IdentityProvider]:
        """Get a provider by ID"""
        # Implementation...
    
    async def get_providers(self, skip: int = 0, limit: int = 100, enabled_only: bool = False) -> List[IdentityProvider]:
        """Get a list of identity providers"""
        # Implementation...
    
    async def create_provider(self, provider_data: IdentityProviderCreate) -> IdentityProvider:
        """Create a new identity provider"""
        # Implementation...
    
    async def update_provider(self, provider_id: str, provider_data: IdentityProviderUpdate) -> Optional[IdentityProvider]:
        """Update an identity provider"""
        # Implementation...
    
    async def delete_provider(self, provider_id: str) -> bool:
        """Delete an identity provider"""
        # Implementation...
    
    async def toggle_provider_status(self, provider_id: str, enabled: bool) -> Optional[IdentityProvider]:
        """Enable or disable an identity provider"""
        # Implementation...
```

### Authentication Service

```python
# Updated authentication service
class AuthService:
    """Service for authentication operations"""
    
    def __init__(self, session: AsyncSession):
        self.session = session
        self.user_repo = UserRepository(User, session)
        self.refresh_token_repo = RefreshTokenRepository(RefreshToken, session)
        self.user_profile_repo = UserProfileRepository(UserProfile, session)
        self.external_identity_repo = ExternalIdentityRepository(ExternalIdentity, session)
        self.identity_provider_repo = IdentityProviderRepository(IdentityProvider, session)
    
    async def authenticate_user(self, username_or_email: str, password: str) -> Optional[User]:
        """Authenticate a user by username/email and password"""
        # Implementation...
    
    async def register_user(self, user_data: UserCreate) -> User:
        """Register a new user"""
        # Implementation...
    
    async def create_user_tokens(self, user: User) -> Dict[str, str]:
        """Create access and refresh tokens for a user"""
        # Implementation...
    
    async def authenticate_with_external_provider(
        self, provider: str, provider_user_id: str, email: str, profile_data: Dict[str, Any] = None
    ) -> Tuple[User, bool]:
        """Authenticate a user with an external identity provider"""
        # Implementation...
```

### Authorization Middleware

```python
# FastAPI dependency for authentication
async def get_current_user(
    token: str = Depends(oauth2_scheme),
    session: AsyncSession = Depends(get_db)
) -> User:
    """Get the current user from the token."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        # Decode JWT
        payload = jwt.decode(
            token, 
            settings.JWT_SECRET_KEY, 
            algorithms=[settings.JWT_ALGORITHM]
        )
        user_id: str = payload.get("sub")
        if user_id is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    
    # Get user from database
    user_service = UserService(session)
    user = await user_service.get_user(user_id)
    if user is None:
        raise credentials_exception
    
    return user

# Role-checking dependency
def check_user_role(allowed_roles: List[str]):
    """Check if the current user has one of the allowed roles."""
    async def _check_role(current_user: User = Depends(get_current_user)):
        if current_user.role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Not enough permissions"
            )
        return current_user
    
    return _check_role

# Permission-checking dependency
def check_permission(permission: str):
    """Check if the current user has the required permission."""
    async def _check_permission(
        current_user: User = Depends(get_current_user),
        session: AsyncSession = Depends(get_db)
    ) -> User:
        # Implementation...
    
    return _check_permission
```

## Identity Provider Integration

The platform supports multiple authentication methods including local username/password authentication and integration with external identity providers.

### Supported Identity Providers

1. **Local Authentication**
   - Username/password stored in the application database
   - Default method if no external providers are configured
   - Supports password hashing with bcrypt
   - Password reset flow with email confirmation

2. **OAuth 2.0 / OpenID Connect Providers**
   - **Google**
     - OAuth 2.0 and OpenID Connect compliant
     - Provides email verification automatically
     - Profile information access for user details
     - Requires Google Cloud Console project setup

   - **GitHub**
     - OAuth 2.0 compliant
     - User information through GitHub API
     - Team/organization membership for role mapping
     - Requires GitHub OAuth App registration

   - **Microsoft Entra ID** (formerly Azure AD)
     - OpenID Connect and OAuth 2.0 compliant
     - Enterprise user directory integration
     - Group membership for role mapping
     - Tenant-specific configuration
     - Requires application registration in Azure portal

   - **AWS Cognito**
     - OpenID Connect and OAuth 2.0 compliant
     - User pool management
     - Custom attributes for role assignments
     - Requires AWS Cognito user pool setup

   - **Auth0**
     - Identity-as-a-Service platform
     - Support for social logins (Google, Facebook, Twitter, etc.)
     - Rules and hooks for custom authentication logic
     - Role management through Auth0 dashboard
     - Requires Auth0 tenant setup

   - **Okta**
     - Enterprise identity provider
     - User directory synchronization
     - Group-based access control
     - Custom authorization servers
     - Requires Okta developer account

3. **SAML 2.0 Providers**
   - Support for enterprise identity providers using SAML
   - Attribute mapping for user information
   - Metadata exchange for configuration
   - IdP-initiated and SP-initiated flows

4. **Custom Identity Provider Integration**
   - Extensible architecture for custom provider integration
   - Pluggable authentication modules
   - Custom provider implementation guidelines

### Provider Configuration

The platform now includes a comprehensive API for managing identity providers:

```python
@router.get(
    "/",
    response_model=List[IdentityProviderResponse],
    summary="Get all identity providers"
)
async def get_identity_providers(
    skip: int = 0,
    limit: int = 100,
    enabled_only: bool = False,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    # Implementation...

@router.post(
    "/",
    response_model=IdentityProviderResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new identity provider",
    dependencies=[Depends(admin_only)],
)
async def create_identity_provider(
    provider_data: IdentityProviderCreate,
    session: AsyncSession = Depends(get_db),
):
    # Implementation...

@router.put(
    "/{provider_id}",
    response_model=IdentityProviderResponse,
    summary="Update an identity provider",
    dependencies=[Depends(admin_only)],
)
async def update_identity_provider(
    provider_id: str,
    provider_data: IdentityProviderUpdate,
    session: AsyncSession = Depends(get_db),
):
    # Implementation...

@router.patch(
    "/{provider_id}/toggle",
    response_model=IdentityProviderResponse,
    summary="Toggle an identity provider's status",
    dependencies=[Depends(admin_only)],
)
async def toggle_identity_provider(
    provider_id: str,
    enabled: bool,
    session: AsyncSession = Depends(get_db),
):
    # Implementation...
```

## Role and Privilege Configuration

The platform provides an administrative API for configuring roles and assigning granular privileges.

### Role Management API

```python
@router.get("", response_model=List[RoleInDB])
async def read_roles(
    skip: int = 0,
    limit: int = 100,
    session: AsyncSession = Depends(get_db),
):
    """Get list of roles"""
    # Implementation...

@router.post("", response_model=RoleWithPrivileges, status_code=status.HTTP_201_CREATED)
async def create_role(
    role_data: RoleCreate,
    session: AsyncSession = Depends(get_db),
):
    """Create a new role"""
    # Implementation...

@router.get("/{role_id}", response_model=RoleWithPrivileges)
async def read_role(
    role_id: str,
    session: AsyncSession = Depends(get_db),
):
    """Get a role by ID"""
    # Implementation...

@router.put("/{role_id}", response_model=RoleWithPrivileges)
async def update_role(
    role_id: str,
    role_data: RoleUpdate,
    session: AsyncSession = Depends(get_db),
):
    """Update a role"""
    # Implementation...

@router.delete("/{role_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_role(
    role_id: str,
    session: AsyncSession = Depends(get_db),
):
    """Delete a role"""
    # Implementation...
```

### Privilege Management API

```python
@privilege_router.get("", response_model=List[PrivilegeInDB])
async def read_privileges(
    skip: int = 0,
    limit: int = 100,
    session: AsyncSession = Depends(get_db),
):
    """Get list of privileges"""
    # Implementation...

@privilege_router.post("", response_model=PrivilegeInDB, status_code=status.HTTP_201_CREATED)
async def create_privilege(
    privilege_data: PrivilegeCreate,
    session: AsyncSession = Depends(get_db),
):
    """Create a new privilege"""
    # Implementation...

@privilege_router.get("/{privilege_id}", response_model=PrivilegeInDB)
async def read_privilege(
    privilege_id: str,
    session: AsyncSession = Depends(get_db),
):
    """Get a privilege by ID"""
    # Implementation...

@privilege_router.put("/{privilege_id}", response_model=PrivilegeInDB)
async def update_privilege(
    privilege_id: str,
    privilege_data: PrivilegeUpdate,
    session: AsyncSession = Depends(get_db),
):
    """Update a privilege"""
    # Implementation...

@privilege_router.delete("/{privilege_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_privilege(
    privilege_id: str,
    session: AsyncSession = Depends(get_db),
):
    """Delete a privilege"""
    # Implementation...
```

## Testing Strategy

Comprehensive testing is implemented for authentication and authorization components:

1. **Unit Tests**
   - Service layer tests for AuthService, RoleService, and IdentityProviderService
   - Repository layer tests for data access operations
   - Permission checking logic tests

2. **Integration Tests**
   - API endpoint tests with authentication
   - Role-based access control tests
   - Token generation and validation tests
   - Identity provider integration tests

3. **End-to-End Tests**
   - Complete authentication flows
   - Role assignment and permission checking
   - External identity provider authentication

4. **Security Tests**
   - JWT token validation and security
   - Password hashing effectiveness
   - CSRF protection verification
   - Rate limiting and brute force prevention 