"""
Unit tests for user schemas.

Tests the functionality of Pydantic schemas for user management
including validation, serialization, and field constraints.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError

from src.schemas.user import (
    UserBase, UserCreate, UserUpdate, PasswordChange, PasswordResetRequest,
    PasswordReset, UserLogin, Token, TokenData, UserProfileBase,
    UserProfileCreate, UserProfileUpdate, UserProfileInDB, PrivilegeBase,
    PrivilegeCreate, PrivilegeUpdate, PrivilegeInDB, RoleBase, RoleCreate,
    RoleUpdate, RoleInDB, RoleWithPrivileges, IdentityProviderType,
    IdentityProviderBase, IdentityProviderConfig, IdentityProviderCreate,
    IdentityProviderUpdate, IdentityProviderInDB, IdentityProviderResponse,
    IdentityProviderListResponse, IdentityProviderUsageStatsResponse,
    ExternalIdentityBase, ExternalIdentityCreate, ExternalIdentityInDB,
    UserInDB, UserWithProfile, UserWithExternalIdentities, UserComplete,
    UserRoleAssign, OAuthAuthorize, OAuthCallback
)
from src.models.enums import UserRole, UserStatus


class TestIdentityProviderType:
    """Test cases for IdentityProviderType enum."""
    
    def test_identity_provider_type_values(self):
        """Test IdentityProviderType enum values."""
        assert IdentityProviderType.LOCAL == "local"
        assert IdentityProviderType.OAUTH == "oauth"
        assert IdentityProviderType.OIDC == "oidc"
        assert IdentityProviderType.SAML == "saml"
        assert IdentityProviderType.CUSTOM == "custom"
    
    def test_identity_provider_type_all_values(self):
        """Test that all expected IdentityProviderType values are present."""
        expected_values = {"local", "oauth", "oidc", "saml", "custom"}
        actual_values = {provider.value for provider in IdentityProviderType}
        assert actual_values == expected_values


class TestUserBase:
    """Test cases for UserBase schema."""
    
    def test_valid_user_base(self):
        """Test valid UserBase creation."""
        user_data = {
            "username": "testuser",
            "email": "test@example.com"
        }
        
        user = UserBase(**user_data)
        
        assert user.username == "testuser"
        assert user.email == "test@example.com"
    
    def test_user_base_localhost_email(self):
        """Test UserBase with localhost email (allowed in development)."""
        user_data = {
            "username": "devuser",
            "email": "dev@localhost"
        }
        
        user = UserBase(**user_data)
        
        assert user.username == "devuser"
        assert user.email == "dev@localhost"
    
    def test_user_base_username_validation_valid(self):
        """Test UserBase username validation with valid usernames."""
        valid_usernames = ["user123", "test_user", "my-username", "abc", "a" * 50]
        
        for username in valid_usernames:
            user = UserBase(username=username, email="test@example.com")
            assert user.username == username
    
    def test_user_base_username_validation_invalid_characters(self):
        """Test UserBase username validation with invalid characters."""
        invalid_usernames = ["user@name", "user.name", "user name", "user#123"]
        
        for username in invalid_usernames:
            with pytest.raises(ValidationError) as exc_info:
                UserBase(username=username, email="test@example.com")
            
            assert "can only contain letters, numbers, underscores, and hyphens" in str(exc_info.value)
    
    def test_user_base_username_validation_length(self):
        """Test UserBase username validation for length constraints."""
        # Too short (2 characters)
        with pytest.raises(ValidationError) as exc_info:
            UserBase(username="ab", email="test@example.com")
        assert "must be between 3 and 50 characters" in str(exc_info.value)
        
        # Too short (1 character)
        with pytest.raises(ValidationError) as exc_info:
            UserBase(username="a", email="test@example.com")
        assert "must be between 3 and 50 characters" in str(exc_info.value)
        
        # Too long (51 characters)
        with pytest.raises(ValidationError) as exc_info:
            UserBase(username="a" * 51, email="test@example.com")
        assert "must be between 3 and 50 characters" in str(exc_info.value)
        
        # Too long (100 characters)
        with pytest.raises(ValidationError) as exc_info:
            UserBase(username="b" * 100, email="test@example.com")
        assert "must be between 3 and 50 characters" in str(exc_info.value)
        
        # Test specific upper bound
        very_long_username = "x" * 52
        with pytest.raises(ValidationError) as exc_info:
            UserBase(username=very_long_username, email="test@example.com")
        error_msg = str(exc_info.value)
        assert "must be between 3 and 50 characters" in error_msg
        
        # Edge cases - exactly at boundaries
        # Exactly 3 characters (should be valid)
        user = UserBase(username="abc", email="test@example.com")
        assert user.username == "abc"
        
        # Exactly 50 characters (should be valid)
        username_50 = "a" * 50
        user = UserBase(username=username_50, email="test@example.com")
        assert user.username == username_50
    
    def test_user_base_email_validation_valid(self):
        """Test UserBase email validation with valid emails."""
        valid_emails = [
            "test@example.com",
            "user@localhost",
            "complex.email+tag@domain.co.uk",
            "numbers123@test.org"
        ]
        
        for email in valid_emails:
            user = UserBase(username="testuser", email=email)
            assert user.email == email
    
    def test_user_base_email_validation_invalid(self):
        """Test UserBase email validation with invalid emails."""
        invalid_emails = [
            "notanemail",
            "@example.com",
            "test@",
            "test.example.com",
            "test@example"
        ]
        
        for email in invalid_emails:
            with pytest.raises(ValidationError) as exc_info:
                UserBase(username="testuser", email=email)
            
            assert "Invalid email format" in str(exc_info.value)


class TestUserCreate:
    """Test cases for UserCreate schema."""
    
    def test_valid_user_create(self):
        """Test valid UserCreate creation."""
        user_data = {
            "username": "newuser",
            "email": "new@example.com",
            "password": "SecurePass123"
        }
        
        user = UserCreate(**user_data)
        
        assert user.username == "newuser"
        assert user.email == "new@example.com"
        assert user.password == "SecurePass123"
    
    def test_user_create_password_validation_valid(self):
        """Test UserCreate password validation with valid passwords."""
        valid_passwords = [
            "SecurePass123",
            "ComplexP@ssw0rd",
            "MyPassword1",
            "Abcdefgh1"
        ]
        
        for password in valid_passwords:
            user = UserCreate(
                username="testuser",
                email="test@example.com",
                password=password
            )
            assert user.password == password
    
    def test_user_create_password_validation_too_short(self):
        """Test UserCreate password validation with too short passwords."""
        with pytest.raises(ValidationError) as exc_info:
            UserCreate(
                username="testuser",
                email="test@example.com",
                password="Short1"
            )
        
        assert "must be at least 8 characters" in str(exc_info.value)
    
    def test_user_create_password_validation_no_digit(self):
        """Test UserCreate password validation without digits."""
        with pytest.raises(ValidationError) as exc_info:
            UserCreate(
                username="testuser",
                email="test@example.com",
                password="NoDigitPassword"
            )
        
        assert "must contain at least one digit" in str(exc_info.value)
    
    def test_user_create_password_validation_no_uppercase(self):
        """Test UserCreate password validation without uppercase letters."""
        with pytest.raises(ValidationError) as exc_info:
            UserCreate(
                username="testuser",
                email="test@example.com",
                password="nouppercasepass1"
            )
        
        assert "must contain at least one uppercase letter" in str(exc_info.value)
    
    def test_user_create_password_validation_no_lowercase(self):
        """Test UserCreate password validation without lowercase letters."""
        with pytest.raises(ValidationError) as exc_info:
            UserCreate(
                username="testuser",
                email="test@example.com",
                password="NOLOWERCASEPASS1"
            )
        
        assert "must contain at least one lowercase letter" in str(exc_info.value)


class TestUserUpdate:
    """Test cases for UserUpdate schema."""
    
    def test_valid_user_update(self):
        """Test valid UserUpdate creation."""
        update_data = {
            "username": "updateduser",
            "email": "updated@example.com",
            "status": UserStatus.ACTIVE
        }
        
        user_update = UserUpdate(**update_data)
        
        assert user_update.username == "updateduser"
        assert user_update.email == "updated@example.com"
        assert user_update.status == UserStatus.ACTIVE
    
    def test_user_update_all_optional(self):
        """Test UserUpdate with all optional fields."""
        user_update = UserUpdate()
        
        assert user_update.username is None
        assert user_update.email is None
        assert user_update.status is None
    
    def test_user_update_partial(self):
        """Test UserUpdate with partial data."""
        update_data = {"username": "partialupdate"}
        
        user_update = UserUpdate(**update_data)
        
        assert user_update.username == "partialupdate"
        assert user_update.email is None
        assert user_update.status is None
    
    def test_user_update_username_validation(self):
        """Test UserUpdate username validation."""
        # Valid username
        user_update = UserUpdate(username="validuser123")
        assert user_update.username == "validuser123"
        
        # None username (should be allowed)
        user_update = UserUpdate(username=None)
        assert user_update.username is None
        
        # Invalid username (invalid characters)
        with pytest.raises(ValidationError):
            UserUpdate(username="invalid@user")
        
        # Invalid username (too short)
        with pytest.raises(ValidationError) as exc_info:
            UserUpdate(username="ab")
        assert "must be between 3 and 50 characters" in str(exc_info.value)
        
        # Invalid username (too long)
        with pytest.raises(ValidationError) as exc_info:
            UserUpdate(username="a" * 51)
        assert "must be between 3 and 50 characters" in str(exc_info.value)


class TestPasswordChange:
    """Test cases for PasswordChange schema."""
    
    def test_valid_password_change(self):
        """Test valid PasswordChange creation."""
        password_data = {
            "current_password": "OldPassword123",
            "new_password": "NewPassword456"
        }
        
        password_change = PasswordChange(**password_data)
        
        assert password_change.current_password == "OldPassword123"
        assert password_change.new_password == "NewPassword456"
    
    def test_password_change_new_password_validation(self):
        """Test PasswordChange new password validation."""
        # Valid new password
        password_change = PasswordChange(
            current_password="OldPass123",
            new_password="ValidNewPass1"
        )
        assert password_change.new_password == "ValidNewPass1"
        
        # Invalid new password (too short)
        with pytest.raises(ValidationError) as exc_info:
            PasswordChange(
                current_password="OldPass123",
                new_password="Short1"
            )
        assert "must be at least 8 characters" in str(exc_info.value)
        
        # Invalid new password (no digit)
        with pytest.raises(ValidationError) as exc_info:
            PasswordChange(
                current_password="OldPass123",
                new_password="NoDigitPass"
            )
        assert "must contain at least one digit" in str(exc_info.value)
        
        # Invalid new password (no uppercase)
        with pytest.raises(ValidationError) as exc_info:
            PasswordChange(
                current_password="OldPass123",
                new_password="nouppercase1"
            )
        assert "must contain at least one uppercase letter" in str(exc_info.value)
        
        # Invalid new password (no lowercase)
        with pytest.raises(ValidationError) as exc_info:
            PasswordChange(
                current_password="OldPass123",
                new_password="NOLOWERCASE1"
            )
        assert "must contain at least one lowercase letter" in str(exc_info.value)


class TestPasswordResetRequest:
    """Test cases for PasswordResetRequest schema."""
    
    def test_valid_password_reset_request(self):
        """Test valid PasswordResetRequest creation."""
        reset_request = PasswordResetRequest(email="reset@example.com")
        
        assert reset_request.email == "reset@example.com"
    
    def test_password_reset_request_invalid_email(self):
        """Test PasswordResetRequest with invalid email."""
        with pytest.raises(ValidationError):
            PasswordResetRequest(email="invalid-email")


class TestPasswordReset:
    """Test cases for PasswordReset schema."""
    
    def test_valid_password_reset(self):
        """Test valid PasswordReset creation."""
        reset_data = {
            "token": "reset-token-123",
            "new_password": "NewSecurePass1"
        }
        
        password_reset = PasswordReset(**reset_data)
        
        assert password_reset.token == "reset-token-123"
        assert password_reset.new_password == "NewSecurePass1"
    
    def test_password_reset_new_password_validation(self):
        """Test PasswordReset new password validation."""
        # Invalid new password (too short)
        with pytest.raises(ValidationError) as exc_info:
            PasswordReset(
                token="token123",
                new_password="weak"
            )
        assert "must be at least 8 characters" in str(exc_info.value)
        
        # Invalid new password (no digit)
        with pytest.raises(ValidationError) as exc_info:
            PasswordReset(
                token="token123",
                new_password="NoDigitPass"
            )
        assert "must contain at least one digit" in str(exc_info.value)
        
        # Invalid new password (no uppercase)
        with pytest.raises(ValidationError) as exc_info:
            PasswordReset(
                token="token123",
                new_password="nouppercase1"
            )
        assert "must contain at least one uppercase letter" in str(exc_info.value)
        
        # Invalid new password (no lowercase)
        with pytest.raises(ValidationError) as exc_info:
            PasswordReset(
                token="token123",
                new_password="NOLOWERCASE1"
            )
        assert "must contain at least one lowercase letter" in str(exc_info.value)


class TestUserLogin:
    """Test cases for UserLogin schema."""
    
    def test_valid_user_login(self):
        """Test valid UserLogin creation."""
        login_data = {
            "username_or_email": "testuser",
            "password": "LoginPass123"
        }
        
        login = UserLogin(**login_data)
        
        assert login.username_or_email == "testuser"
        assert login.password == "LoginPass123"
    
    def test_user_login_with_email(self):
        """Test UserLogin with email as username_or_email."""
        login = UserLogin(
            username_or_email="test@example.com",
            password="LoginPass123"
        )
        
        assert login.username_or_email == "test@example.com"


class TestToken:
    """Test cases for Token schema."""
    
    def test_valid_token(self):
        """Test valid Token creation."""
        token_data = {
            "access_token": "access_token_123",
            "refresh_token": "refresh_token_456"
        }
        
        token = Token(**token_data)
        
        assert token.access_token == "access_token_123"
        assert token.refresh_token == "refresh_token_456"
        assert token.token_type == "bearer"  # Default value
    
    def test_token_custom_type(self):
        """Test Token with custom token type."""
        token = Token(
            access_token="access_123",
            refresh_token="refresh_456",
            token_type="custom"
        )
        
        assert token.token_type == "custom"


class TestTokenData:
    """Test cases for TokenData schema."""
    
    def test_valid_token_data(self):
        """Test valid TokenData creation."""
        token_data = TokenData(
            sub="user123",
            role=UserRole.ADMIN,
            exp=1640995200
        )
        
        assert token_data.sub == "user123"
        assert token_data.role == UserRole.ADMIN
        assert token_data.exp == 1640995200


class TestUserProfile:
    """Test cases for user profile schemas."""
    
    def test_user_profile_base(self):
        """Test UserProfileBase creation."""
        profile_data = {
            "display_name": "Test User",
            "avatar_url": "https://example.com/avatar.jpg",
            "preferences": {"theme": "dark", "language": "en"}
        }
        
        profile = UserProfileBase(**profile_data)
        
        assert profile.display_name == "Test User"
        assert profile.avatar_url == "https://example.com/avatar.jpg"
        assert profile.preferences == {"theme": "dark", "language": "en"}
    
    def test_user_profile_base_defaults(self):
        """Test UserProfileBase with default values."""
        profile = UserProfileBase()
        
        assert profile.display_name is None
        assert profile.avatar_url is None
        assert profile.preferences is None
    
    def test_user_profile_create(self):
        """Test UserProfileCreate schema."""
        profile = UserProfileCreate(display_name="Created User")
        
        assert profile.display_name == "Created User"
    
    def test_user_profile_update(self):
        """Test UserProfileUpdate schema."""
        profile = UserProfileUpdate(preferences={"new_setting": "value"})
        
        assert profile.preferences == {"new_setting": "value"}
    
    def test_user_profile_in_db(self):
        """Test UserProfileInDB schema."""
        profile_data = {
            "id": "profile123",
            "user_id": "user456",
            "display_name": "DB User"
        }
        
        profile = UserProfileInDB(**profile_data)
        
        assert profile.id == "profile123"
        assert profile.user_id == "user456"
        assert profile.display_name == "DB User"


class TestPrivilege:
    """Test cases for privilege schemas."""
    
    def test_privilege_base(self):
        """Test PrivilegeBase creation."""
        privilege_data = {
            "name": "users:read",
            "description": "Read user information"
        }
        
        privilege = PrivilegeBase(**privilege_data)
        
        assert privilege.name == "users:read"
        assert privilege.description == "Read user information"
    
    def test_privilege_create(self):
        """Test PrivilegeCreate schema."""
        privilege = PrivilegeCreate(name="agents:write")
        
        assert privilege.name == "agents:write"
        assert privilege.description is None
    
    def test_privilege_update(self):
        """Test PrivilegeUpdate schema."""
        privilege = PrivilegeUpdate(description="Updated description")
        
        assert privilege.description == "Updated description"
    
    def test_privilege_in_db(self):
        """Test PrivilegeInDB schema."""
        privilege_data = {
            "id": "priv123",
            "name": "crews:execute",
            "created_at": datetime.now()
        }
        
        privilege = PrivilegeInDB(**privilege_data)
        
        assert privilege.id == "priv123"
        assert privilege.name == "crews:execute"
        assert isinstance(privilege.created_at, datetime)


class TestRole:
    """Test cases for role schemas."""
    
    def test_role_base(self):
        """Test RoleBase creation."""
        role_data = {
            "name": "administrator",
            "description": "Full system access"
        }
        
        role = RoleBase(**role_data)
        
        assert role.name == "administrator"
        assert role.description == "Full system access"
    
    def test_role_create(self):
        """Test RoleCreate schema."""
        role_data = {
            "name": "manager",
            "privileges": ["users:read", "users:write", "crews:read"]
        }
        
        role = RoleCreate(**role_data)
        
        assert role.name == "manager"
        assert role.privileges == ["users:read", "users:write", "crews:read"]
    
    def test_role_update(self):
        """Test RoleUpdate schema."""
        role_update = RoleUpdate(
            name="updated_role",
            privileges=["new:privilege"]
        )
        
        assert role_update.name == "updated_role"
        assert role_update.privileges == ["new:privilege"]
    
    def test_role_in_db(self):
        """Test RoleInDB schema."""
        role_data = {
            "id": "role123",
            "name": "analyst",
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }
        
        role = RoleInDB(**role_data)
        
        assert role.id == "role123"
        assert role.name == "analyst"
        assert isinstance(role.created_at, datetime)
    
    def test_role_with_privileges(self):
        """Test RoleWithPrivileges schema."""
        privilege = PrivilegeInDB(
            id="priv1",
            name="test:privilege",
            created_at=datetime.now()
        )
        
        role_data = {
            "id": "role123",
            "name": "test_role",
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "privileges": [privilege]
        }
        
        role = RoleWithPrivileges(**role_data)
        
        assert role.name == "test_role"
        assert len(role.privileges) == 1
        assert role.privileges[0].name == "test:privilege"


class TestIdentityProvider:
    """Test cases for identity provider schemas."""
    
    def test_identity_provider_base(self):
        """Test IdentityProviderBase creation."""
        provider_data = {
            "name": "Google OAuth",
            "type": IdentityProviderType.OAUTH,
            "enabled": True,
            "is_default": False
        }
        
        provider = IdentityProviderBase(**provider_data)
        
        assert provider.name == "Google OAuth"
        assert provider.type == IdentityProviderType.OAUTH
        assert provider.enabled is True
        assert provider.is_default is False
    
    def test_identity_provider_config(self):
        """Test IdentityProviderConfig creation."""
        config_data = {
            "client_id": "client123",
            "client_secret": "secret456",
            "authorization_endpoint": "https://oauth.example.com/auth",
            "token_endpoint": "https://oauth.example.com/token",
            "scope": "openid profile email"
        }
        
        config = IdentityProviderConfig(**config_data)
        
        assert config.client_id == "client123"
        assert config.client_secret == "secret456"
        assert config.scope == "openid profile email"
    
    def test_identity_provider_create(self):
        """Test IdentityProviderCreate schema."""
        config = IdentityProviderConfig(client_id="test123")
        
        provider_data = {
            "name": "Test Provider",
            "type": IdentityProviderType.OIDC,
            "config": config
        }
        
        provider = IdentityProviderCreate(**provider_data)
        
        assert provider.name == "Test Provider"
        assert provider.type == IdentityProviderType.OIDC
        assert provider.config.client_id == "test123"
    
    def test_identity_provider_response(self):
        """Test IdentityProviderResponse schema."""
        config = IdentityProviderConfig(client_id="response123")
        
        provider_data = {
            "id": "provider123",
            "name": "Response Provider",
            "type": IdentityProviderType.SAML,
            "enabled": True,
            "is_default": True,
            "config": config,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }
        
        provider = IdentityProviderResponse(**provider_data)
        
        assert provider.id == "provider123"
        assert provider.type == IdentityProviderType.SAML
        assert provider.is_default is True


class TestExternalIdentity:
    """Test cases for external identity schemas."""
    
    def test_external_identity_base(self):
        """Test ExternalIdentityBase creation."""
        identity_data = {
            "provider": "google",
            "provider_user_id": "google123",
            "email": "user@gmail.com"
        }
        
        identity = ExternalIdentityBase(**identity_data)
        
        assert identity.provider == "google"
        assert identity.provider_user_id == "google123"
        assert identity.email == "user@gmail.com"
    
    def test_external_identity_create(self):
        """Test ExternalIdentityCreate schema."""
        identity_data = {
            "provider": "github",
            "provider_user_id": "github456",
            "profile_data": {"login": "testuser", "avatar_url": "https://github.com/avatar.jpg"}
        }
        
        identity = ExternalIdentityCreate(**identity_data)
        
        assert identity.provider == "github"
        assert identity.profile_data["login"] == "testuser"
    
    def test_external_identity_in_db(self):
        """Test ExternalIdentityInDB schema."""
        identity_data = {
            "id": "ext123",
            "user_id": "user456",
            "provider": "microsoft",
            "provider_user_id": "ms789",
            "created_at": datetime.now()
        }
        
        identity = ExternalIdentityInDB(**identity_data)
        
        assert identity.id == "ext123"
        assert identity.user_id == "user456"
        assert identity.provider == "microsoft"


class TestUserComplexSchemas:
    """Test cases for complex user schemas."""
    
    def test_user_in_db(self):
        """Test UserInDB schema."""
        user_data = {
            "id": "user123",
            "username": "dbuser",
            "email": "db@example.com",
            "role": UserRole.TECHNICAL,
            "status": UserStatus.ACTIVE,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }
        
        user = UserInDB(**user_data)
        
        assert user.id == "user123"
        assert user.role == UserRole.TECHNICAL
        assert user.status == UserStatus.ACTIVE
    
    def test_user_with_profile(self):
        """Test UserWithProfile schema."""
        profile = UserProfileInDB(
            id="prof123",
            user_id="user456",
            display_name="Profile User"
        )
        
        user_data = {
            "id": "user456",
            "username": "profileuser",
            "email": "profile@example.com",
            "role": UserRole.REGULAR,
            "status": UserStatus.ACTIVE,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "profile": profile
        }
        
        user = UserWithProfile(**user_data)
        
        assert user.username == "profileuser"
        assert user.profile.display_name == "Profile User"
    
    def test_user_complete(self):
        """Test UserComplete schema."""
        profile = UserProfileInDB(
            id="prof123",
            user_id="user789",
            display_name="Complete User"
        )
        
        external_identity = ExternalIdentityInDB(
            id="ext123",
            user_id="user789",
            provider="google",
            provider_user_id="google789",
            created_at=datetime.now()
        )
        
        user_data = {
            "id": "user789",
            "username": "completeuser",
            "email": "complete@example.com",
            "role": UserRole.ADMIN,
            "status": UserStatus.ACTIVE,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "profile": profile,
            "external_identities": [external_identity]
        }
        
        user = UserComplete(**user_data)
        
        assert user.username == "completeuser"
        assert user.profile.display_name == "Complete User"
        assert len(user.external_identities) == 1
        assert user.external_identities[0].provider == "google"


class TestOAuth:
    """Test cases for OAuth schemas."""
    
    def test_oauth_authorize(self):
        """Test OAuthAuthorize schema."""
        oauth_data = {
            "provider": "google",
            "redirect_uri": "https://app.example.com/callback",
            "state": "random_state_123"
        }
        
        oauth = OAuthAuthorize(**oauth_data)
        
        assert oauth.provider == "google"
        assert oauth.redirect_uri == "https://app.example.com/callback"
        assert oauth.state == "random_state_123"
    
    def test_oauth_callback(self):
        """Test OAuthCallback schema."""
        callback_data = {
            "provider": "github",
            "code": "auth_code_456",
            "state": "callback_state_789"
        }
        
        callback = OAuthCallback(**callback_data)
        
        assert callback.provider == "github"
        assert callback.code == "auth_code_456"
        assert callback.state == "callback_state_789"


class TestUserRoleAssign:
    """Test cases for UserRoleAssign schema."""
    
    def test_user_role_assign(self):
        """Test UserRoleAssign schema."""
        role_assign = UserRoleAssign(role_id="role123")
        
        assert role_assign.role_id == "role123"


class TestSchemaInteraction:
    """Test cases for schema interactions and edge cases."""
    
    def test_model_config_from_attributes(self):
        """Test that schemas with from_attributes work correctly."""
        # Test UserProfileInDB
        assert 'from_attributes' in UserProfileInDB.model_config
        assert UserProfileInDB.model_config['from_attributes'] is True
        
        # Test PrivilegeInDB
        assert 'from_attributes' in PrivilegeInDB.model_config
        assert PrivilegeInDB.model_config['from_attributes'] is True
        
        # Test UserInDB
        assert 'from_attributes' in UserInDB.model_config
        assert UserInDB.model_config['from_attributes'] is True
        assert UserInDB.model_config['use_enum_values'] is True
    
    def test_schema_inheritance(self):
        """Test that schema inheritance works correctly."""
        # UserCreate should inherit from UserBase
        user_create = UserCreate(
            username="inherited",
            email="inherit@example.com",
            password="InheritPass1"
        )
        
        # Should have UserBase fields
        assert user_create.username == "inherited"
        assert user_create.email == "inherit@example.com"
        # And UserCreate specific fields
        assert user_create.password == "InheritPass1"
        
        # RoleWithPrivileges should inherit from RoleInDB
        privilege = PrivilegeInDB(
            id="priv1",
            name="inherit:test",
            created_at=datetime.now()
        )
        
        role = RoleWithPrivileges(
            id="role1",
            name="inherited_role",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            privileges=[privilege]
        )
        
        # Should have RoleInDB fields
        assert role.id == "role1"
        assert role.name == "inherited_role"
        # And RoleWithPrivileges specific fields
        assert len(role.privileges) == 1
    
    def test_enum_integration(self):
        """Test that enums are properly integrated in schemas."""
        # Test UserRole enum in schemas
        user = UserInDB(
            id="user1",
            username="enumuser",
            email="enum@example.com",
            role=UserRole.ADMIN,
            status=UserStatus.ACTIVE,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        assert user.role == UserRole.ADMIN
        assert user.status == UserStatus.ACTIVE
        
        # Test IdentityProviderType enum
        provider = IdentityProviderBase(
            name="Enum Provider",
            type=IdentityProviderType.OAUTH
        )
        
        assert provider.type == IdentityProviderType.OAUTH
    
    def test_optional_fields_behavior(self):
        """Test optional fields behavior across schemas."""
        # UserUpdate - all fields optional
        update = UserUpdate()
        assert update.username is None
        assert update.email is None
        assert update.status is None
        
        # UserProfileBase - all fields optional
        profile = UserProfileBase()
        assert profile.display_name is None
        assert profile.avatar_url is None
        assert profile.preferences is None
        
        # IdentityProviderConfig - all fields optional
        config = IdentityProviderConfig()
        assert config.client_id is None
        assert config.authorization_endpoint is None
    
    def test_complex_data_structures(self):
        """Test schemas with complex data structures."""
        # Test preferences in UserProfile
        complex_preferences = {
            "ui": {
                "theme": "dark",
                "language": "en",
                "sidebar_collapsed": True
            },
            "notifications": {
                "email": True,
                "push": False,
                "categories": ["alerts", "updates"]
            },
            "workflow": {
                "auto_save": True,
                "default_view": "grid"
            }
        }
        
        profile = UserProfileBase(preferences=complex_preferences)
        
        assert profile.preferences["ui"]["theme"] == "dark"
        assert profile.preferences["notifications"]["categories"] == ["alerts", "updates"]
        assert profile.preferences["workflow"]["auto_save"] is True
        
        # Test profile_data in ExternalIdentity
        complex_profile_data = {
            "user_info": {
                "name": "Test User",
                "picture": "https://example.com/avatar.jpg"
            },
            "oauth_details": {
                "scope": "openid profile email",
                "token_type": "Bearer"
            }
        }
        
        identity = ExternalIdentityCreate(
            provider="complex_provider",
            provider_user_id="complex123",
            profile_data=complex_profile_data
        )
        
        assert identity.profile_data["user_info"]["name"] == "Test User"
        assert identity.profile_data["oauth_details"]["scope"] == "openid profile email"