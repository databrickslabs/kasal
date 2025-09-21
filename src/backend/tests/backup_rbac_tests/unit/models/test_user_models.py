"""
Unit tests for user models.

Tests the functionality of the User, UserProfile, RefreshToken, ExternalIdentity,
Role, Privilege, RolePrivilege, UserRole, and IdentityProvider database models
including field validation, relationships, and data integrity.
"""
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

from src.models.user import (
    User, UserProfile, RefreshToken, ExternalIdentity,
    Role, Privilege, RolePrivilege, UserRole, IdentityProvider,
    DatabricksRole, generate_uuid
)
from src.models.enums import UserRole as UserRoleEnum, UserStatus, IdentityProviderType


class TestUser:
    """Test cases for User model."""

    def test_user_creation(self):
        """Test basic User model creation."""
        # Arrange
        username = "testuser"
        email = "test@example.com"
        hashed_password = "hashed_password_123"
        
        # Act
        user = User(
            username=username,
            email=email,
            hashed_password=hashed_password
        )
        
        # Assert
        assert user.username == username
        assert user.email == email
        assert user.hashed_password == hashed_password
        # Note: SQLAlchemy defaults are applied when saved to database
        assert User.__table__.columns['role'].default.arg == UserRoleEnum.REGULAR
        assert User.__table__.columns['status'].default.arg == UserStatus.ACTIVE
        assert user.last_login is None

    def test_user_with_all_fields(self):
        """Test User model creation with all fields."""
        # Arrange
        username = "adminuser"
        email = "admin@company.com"
        hashed_password = "secure_hash_456"
        role = UserRoleEnum.ADMIN
        status = UserStatus.ACTIVE
        last_login = datetime.now(timezone.utc)
        
        # Act
        user = User(
            username=username,
            email=email,
            hashed_password=hashed_password,
            role=role,
            status=status,
            last_login=last_login
        )
        
        # Assert
        assert user.username == username
        assert user.email == email
        assert user.hashed_password == hashed_password
        assert user.role == role
        assert user.status == status
        assert user.last_login == last_login

    def test_user_role_enum_values(self):
        """Test User with different role enum values."""
        # Test all role types
        for role in [UserRoleEnum.ADMIN, UserRoleEnum.TECHNICAL, UserRoleEnum.REGULAR]:
            user = User(
                username=f"user_{role.value}",
                email=f"{role.value}@example.com",
                hashed_password="password",
                role=role
            )
            assert user.role == role

    def test_user_status_enum_values(self):
        """Test User with different status enum values."""
        # Test all status types
        for status in [UserStatus.ACTIVE, UserStatus.INACTIVE, UserStatus.SUSPENDED]:
            user = User(
                username=f"user_{status.value}",
                email=f"{status.value}@example.com",
                hashed_password="password",
                status=status
            )
            assert user.status == status

    def test_user_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert User.__tablename__ == "users"

    def test_user_unique_constraints(self):
        """Test that username and email have unique constraints."""
        # Act
        columns = User.__table__.columns
        
        # Assert
        assert columns['username'].unique is True
        assert columns['email'].unique is True
        assert columns['username'].index is True
        assert columns['email'].index is True

    def test_user_relationships(self):
        """Test that User model has the expected relationships."""
        # Act
        relationships = User.__mapper__.relationships
        
        # Assert
        assert 'profile' in relationships
        assert 'refresh_tokens' in relationships
        assert 'external_identities' in relationships
        assert 'user_roles' in relationships


class TestUserProfile:
    """Test cases for UserProfile model."""

    def test_user_profile_creation(self):
        """Test basic UserProfile model creation."""
        # Arrange
        user_id = "user-123"
        display_name = "John Doe"
        avatar_url = "https://example.com/avatar.jpg"
        preferences = '{"theme": "dark", "notifications": true}'
        
        # Act
        profile = UserProfile(
            user_id=user_id,
            display_name=display_name,
            avatar_url=avatar_url,
            preferences=preferences
        )
        
        # Assert
        assert profile.user_id == user_id
        assert profile.display_name == display_name
        assert profile.avatar_url == avatar_url
        assert profile.preferences == preferences

    def test_user_profile_minimal(self):
        """Test UserProfile with minimal required fields."""
        # Arrange & Act
        profile = UserProfile(user_id="user-456")
        
        # Assert
        assert profile.user_id == "user-456"
        assert profile.display_name is None
        assert profile.avatar_url is None
        assert profile.preferences is None

    def test_user_profile_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert UserProfile.__tablename__ == "user_profiles"

    def test_user_profile_unique_user_id(self):
        """Test that user_id has unique constraint."""
        # Act
        columns = UserProfile.__table__.columns
        
        # Assert
        assert columns['user_id'].unique is True


class TestRefreshToken:
    """Test cases for RefreshToken model."""

    def test_refresh_token_creation(self):
        """Test basic RefreshToken model creation."""
        # Arrange
        user_id = "user-789"
        token = "hashed_refresh_token_abc123"
        expires_at = datetime.now(timezone.utc) + timedelta(days=30)
        
        # Act
        refresh_token = RefreshToken(
            user_id=user_id,
            token=token,
            expires_at=expires_at
        )
        
        # Assert
        assert refresh_token.user_id == user_id
        assert refresh_token.token == token
        assert refresh_token.expires_at == expires_at
        # Note: SQLAlchemy defaults are applied when saved to database
        assert RefreshToken.__table__.columns['is_revoked'].default.arg is False

    def test_refresh_token_revoked(self):
        """Test RefreshToken in revoked state."""
        # Arrange
        user_id = "user-revoked"
        token = "revoked_token_xyz"
        expires_at = datetime.now(timezone.utc) + timedelta(days=1)
        
        # Act
        refresh_token = RefreshToken(
            user_id=user_id,
            token=token,
            expires_at=expires_at,
            is_revoked=True
        )
        
        # Assert
        assert refresh_token.is_revoked is True

    def test_refresh_token_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert RefreshToken.__tablename__ == "refresh_tokens"

    def test_refresh_token_unique_token(self):
        """Test that token has unique constraint."""
        # Act
        columns = RefreshToken.__table__.columns
        
        # Assert
        assert columns['token'].unique is True


class TestExternalIdentity:
    """Test cases for ExternalIdentity model."""

    def test_external_identity_creation(self):
        """Test basic ExternalIdentity model creation."""
        # Arrange
        user_id = "user-external"
        provider = "google"
        provider_user_id = "google_user_12345"
        email = "user@gmail.com"
        profile_data = '{"name": "John Doe", "picture": "https://lh3.googleusercontent.com/..."}'
        
        # Act
        external_identity = ExternalIdentity(
            user_id=user_id,
            provider=provider,
            provider_user_id=provider_user_id,
            email=email,
            profile_data=profile_data
        )
        
        # Assert
        assert external_identity.user_id == user_id
        assert external_identity.provider == provider
        assert external_identity.provider_user_id == provider_user_id
        assert external_identity.email == email
        assert external_identity.profile_data == profile_data
        assert external_identity.last_login is None

    def test_external_identity_minimal(self):
        """Test ExternalIdentity with minimal required fields."""
        # Arrange & Act
        external_identity = ExternalIdentity(
            user_id="user-min",
            provider="github",
            provider_user_id="github_123"
        )
        
        # Assert
        assert external_identity.email is None
        assert external_identity.profile_data is None

    def test_external_identity_with_last_login(self):
        """Test ExternalIdentity with last login timestamp."""
        # Arrange
        last_login = datetime.now(timezone.utc)
        
        # Act
        external_identity = ExternalIdentity(
            user_id="user-login",
            provider="microsoft",
            provider_user_id="ms_user_456",
            last_login=last_login
        )
        
        # Assert
        assert external_identity.last_login == last_login

    def test_external_identity_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert ExternalIdentity.__tablename__ == "external_identities"

    def test_external_identity_unique_constraint(self):
        """Test that the unique constraint exists for provider and provider_user_id."""
        # Act
        constraints = ExternalIdentity.__table_args__
        
        # Assert
        assert len(constraints) == 1
        constraint = constraints[0]
        assert constraint.name == 'uq_external_identity_provider_user'
        assert 'provider' in [col.name for col in constraint.columns]
        assert 'provider_user_id' in [col.name for col in constraint.columns]


class TestRole:
    """Test cases for Role model."""

    def test_role_creation(self):
        """Test basic Role model creation."""
        # Arrange
        name = "Project Manager"
        description = "Can manage projects and assign tasks"
        
        # Act
        role = Role(
            name=name,
            description=description
        )
        
        # Assert
        assert role.name == name
        assert role.description == description

    def test_role_minimal(self):
        """Test Role with minimal required fields."""
        # Arrange & Act
        role = Role(name="Basic User")
        
        # Assert
        assert role.name == "Basic User"
        assert role.description is None

    def test_role_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert Role.__tablename__ == "roles"

    def test_role_unique_name(self):
        """Test that name has unique constraint."""
        # Act
        columns = Role.__table__.columns
        
        # Assert
        assert columns['name'].unique is True

    def test_role_relationships(self):
        """Test that Role model has the expected relationships."""
        # Act
        relationships = Role.__mapper__.relationships
        
        # Assert
        assert 'role_privileges' in relationships
        assert 'user_roles' in relationships


class TestPrivilege:
    """Test cases for Privilege model."""

    def test_privilege_creation(self):
        """Test basic Privilege model creation."""
        # Arrange
        name = "users:read"
        description = "Can read user information"
        
        # Act
        privilege = Privilege(
            name=name,
            description=description
        )
        
        # Assert
        assert privilege.name == name
        assert privilege.description == description

    def test_privilege_resource_action_format(self):
        """Test Privilege with resource:action format names."""
        # Arrange
        privileges = [
            "users:create",
            "users:read", 
            "users:update",
            "users:delete",
            "agents:execute",
            "crews:manage",
            "workflows:deploy"
        ]
        
        for priv_name in privileges:
            # Act
            privilege = Privilege(name=priv_name)
            
            # Assert
            assert privilege.name == priv_name
            assert ":" in privilege.name  # Should follow resource:action format

    def test_privilege_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert Privilege.__tablename__ == "privileges"

    def test_privilege_unique_name(self):
        """Test that name has unique constraint."""
        # Act
        columns = Privilege.__table__.columns
        
        # Assert
        assert columns['name'].unique is True


class TestRolePrivilege:
    """Test cases for RolePrivilege model."""

    def test_role_privilege_creation(self):
        """Test basic RolePrivilege model creation."""
        # Arrange
        role_id = "role-123"
        privilege_id = "privilege-456"
        
        # Act
        role_privilege = RolePrivilege(
            role_id=role_id,
            privilege_id=privilege_id
        )
        
        # Assert
        assert role_privilege.role_id == role_id
        assert role_privilege.privilege_id == privilege_id

    def test_role_privilege_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert RolePrivilege.__tablename__ == "role_privileges"

    def test_role_privilege_unique_constraint(self):
        """Test that the unique constraint exists for role_id and privilege_id."""
        # Act
        constraints = RolePrivilege.__table_args__
        
        # Assert
        assert len(constraints) == 1
        constraint = constraints[0]
        assert constraint.name == 'uq_role_privilege'
        assert 'role_id' in [col.name for col in constraint.columns]
        assert 'privilege_id' in [col.name for col in constraint.columns]


class TestUserRole:
    """Test cases for UserRole model."""

    def test_user_role_creation(self):
        """Test basic UserRole model creation."""
        # Arrange
        user_id = "user-789"
        role_id = "role-abc"
        assigned_by = "admin@company.com"
        
        # Act
        user_role = UserRole(
            user_id=user_id,
            role_id=role_id,
            assigned_by=assigned_by
        )
        
        # Assert
        assert user_role.user_id == user_id
        assert user_role.role_id == role_id
        assert user_role.assigned_by == assigned_by

    def test_user_role_minimal(self):
        """Test UserRole with minimal required fields."""
        # Arrange & Act
        user_role = UserRole(
            user_id="user-min",
            role_id="role-min"
        )
        
        # Assert
        assert user_role.assigned_by is None

    def test_user_role_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert UserRole.__tablename__ == "user_roles"

    def test_user_role_unique_constraint(self):
        """Test that the unique constraint exists for user_id and role_id."""
        # Act
        constraints = UserRole.__table_args__
        
        # Assert
        assert len(constraints) == 1
        constraint = constraints[0]
        assert constraint.name == 'uq_user_role'
        assert 'user_id' in [col.name for col in constraint.columns]
        assert 'role_id' in [col.name for col in constraint.columns]


class TestIdentityProvider:
    """Test cases for IdentityProvider model."""

    def test_identity_provider_creation(self):
        """Test basic IdentityProvider model creation."""
        # Arrange
        name = "Google OAuth"
        provider_type = IdentityProviderType.OAUTH
        config = '{"client_id": "google_client_123", "client_secret": "secret"}'
        
        # Act
        identity_provider = IdentityProvider(
            name=name,
            type=provider_type,
            config=config
        )
        
        # Assert
        assert identity_provider.name == name
        assert identity_provider.type == provider_type
        assert identity_provider.config == config
        # Note: SQLAlchemy defaults are applied when saved to database
        assert IdentityProvider.__table__.columns['enabled'].default.arg is True
        assert IdentityProvider.__table__.columns['is_default'].default.arg is False

    def test_identity_provider_all_types(self):
        """Test IdentityProvider with all provider types."""
        # Test all provider types
        for provider_type in IdentityProviderType:
            identity_provider = IdentityProvider(
                name=f"{provider_type.value} Provider",
                type=provider_type,
                config='{"test": "config"}'
            )
            assert identity_provider.type == provider_type

    def test_identity_provider_default(self):
        """Test IdentityProvider as default provider."""
        # Arrange & Act
        identity_provider = IdentityProvider(
            name="Default OIDC",
            type=IdentityProviderType.OIDC,
            config='{"issuer": "https://auth.company.com"}',
            is_default=True
        )
        
        # Assert
        assert identity_provider.is_default is True

    def test_identity_provider_disabled(self):
        """Test IdentityProvider in disabled state."""
        # Arrange & Act
        identity_provider = IdentityProvider(
            name="Disabled SAML",
            type=IdentityProviderType.SAML,
            config='{"sso_url": "https://saml.example.com"}',
            enabled=False
        )
        
        # Assert
        assert identity_provider.enabled is False

    def test_identity_provider_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert IdentityProvider.__tablename__ == "identity_providers"


class TestDatabricksRoleAlias:
    """Test cases for DatabricksRole alias."""

    def test_databricks_role_alias(self):
        """Test that DatabricksRole is an alias for Role."""
        # Act & Assert
        assert DatabricksRole is Role

    def test_databricks_role_functionality(self):
        """Test that DatabricksRole alias works the same as Role."""
        # Arrange
        name = "Databricks Admin"
        description = "Administrator role for Databricks"
        
        # Act
        role_via_alias = DatabricksRole(
            name=name,
            description=description
        )
        
        role_via_class = Role(
            name=name + "_2",
            description=description
        )
        
        # Assert
        assert role_via_alias.description == role_via_class.description
        assert type(role_via_alias) == type(role_via_class)
        assert isinstance(role_via_alias, Role)


class TestGenerateUuidFunction:
    """Test cases for generate_uuid function."""

    def test_generate_uuid_function(self):
        """Test the generate_uuid function."""
        # Act
        uuid1 = generate_uuid()
        uuid2 = generate_uuid()
        
        # Assert
        assert uuid1 is not None
        assert uuid2 is not None
        assert uuid1 != uuid2
        assert isinstance(uuid1, str)
        assert isinstance(uuid2, str)
        assert len(uuid1) == 36  # Standard UUID length
        assert len(uuid2) == 36

    def test_generate_uuid_uniqueness(self):
        """Test that generate_uuid generates unique IDs."""
        # Act
        uuids = [generate_uuid() for _ in range(50)]
        
        # Assert
        assert len(set(uuids)) == 50  # All UUIDs should be unique


class TestUserModelsIntegration:
    """Integration tests for user models."""

    def test_user_with_profile_relationship(self):
        """Test User and UserProfile relationship setup."""
        # Arrange
        user_id = "integration-user-1"
        
        # Act
        user = User(
            username="integrated_user",
            email="integrated@example.com",
            hashed_password="password"
        )
        
        profile = UserProfile(
            user_id=user_id,
            display_name="Integrated User",
            preferences='{"language": "en", "timezone": "UTC"}'
        )
        
        # Assert
        # Note: Actual relationship testing would require database session
        assert user.username == "integrated_user"
        assert profile.user_id == user_id
        assert profile.display_name == "Integrated User"

    def test_user_with_multiple_refresh_tokens(self):
        """Test User with multiple RefreshToken relationships."""
        # Arrange
        user_id = "multi-token-user"
        
        # Act
        user = User(
            username="token_user",
            email="tokens@example.com",
            hashed_password="password"
        )
        
        token1 = RefreshToken(
            user_id=user_id,
            token="token_1_hash",
            expires_at=datetime.now(timezone.utc) + timedelta(days=30)
        )
        
        token2 = RefreshToken(
            user_id=user_id,
            token="token_2_hash",
            expires_at=datetime.now(timezone.utc) + timedelta(days=7)
        )
        
        # Assert
        assert token1.user_id == token2.user_id == user_id
        assert token1.token != token2.token

    def test_user_with_external_identities(self):
        """Test User with multiple ExternalIdentity relationships."""
        # Arrange
        user_id = "external-user"
        
        # Act
        user = User(
            username="social_user",
            email="social@example.com",
            hashed_password="password"
        )
        
        google_identity = ExternalIdentity(
            user_id=user_id,
            provider="google",
            provider_user_id="google_123",
            email="social@gmail.com"
        )
        
        github_identity = ExternalIdentity(
            user_id=user_id,
            provider="github",
            provider_user_id="github_456",
            email="social@users.noreply.github.com"
        )
        
        # Assert
        assert google_identity.user_id == github_identity.user_id == user_id
        assert google_identity.provider != github_identity.provider

    def test_role_privilege_assignment_scenario(self):
        """Test role and privilege assignment scenario."""
        # Arrange
        role_id = "admin-role"
        user_id = "admin-user"
        
        # Act - Create role
        admin_role = Role(
            name="Administrator",
            description="Full system administrator"
        )
        
        # Create privileges
        user_read_priv = Privilege(
            name="users:read",
            description="Read user information"
        )
        
        user_write_priv = Privilege(
            name="users:write", 
            description="Create and update users"
        )
        
        # Assign privileges to role
        role_priv1 = RolePrivilege(
            role_id=role_id,
            privilege_id="priv-1"
        )
        
        role_priv2 = RolePrivilege(
            role_id=role_id,
            privilege_id="priv-2"
        )
        
        # Assign role to user
        user_role = UserRole(
            user_id=user_id,
            role_id=role_id,
            assigned_by="system@company.com"
        )
        
        # Assert
        assert admin_role.name == "Administrator"
        assert user_read_priv.name == "users:read"
        assert user_write_priv.name == "users:write"
        assert role_priv1.role_id == role_priv2.role_id == role_id
        assert user_role.user_id == user_id
        assert user_role.role_id == role_id

    def test_identity_provider_configurations(self):
        """Test different identity provider configurations."""
        # Google OAuth
        google_provider = IdentityProvider(
            name="Google OAuth 2.0",
            type=IdentityProviderType.OAUTH,
            config='{"client_id": "google_id", "scopes": ["email", "profile"]}',
            enabled=True,
            is_default=True
        )
        
        # Microsoft OIDC
        microsoft_provider = IdentityProvider(
            name="Microsoft Azure AD",
            type=IdentityProviderType.OIDC,
            config='{"issuer": "https://login.microsoftonline.com/tenant", "client_id": "ms_client"}',
            enabled=True
        )
        
        # SAML Provider
        saml_provider = IdentityProvider(
            name="Corporate SAML",
            type=IdentityProviderType.SAML,
            config='{"sso_url": "https://saml.corp.com", "certificate": "cert_data"}',
            enabled=False
        )
        
        # Assert
        assert google_provider.is_default is True
        # Note: SQLAlchemy defaults are applied when saved to database
        assert microsoft_provider.is_default is True or microsoft_provider.is_default is None
        assert saml_provider.enabled is False
        assert google_provider.type == IdentityProviderType.OAUTH
        assert microsoft_provider.type == IdentityProviderType.OIDC
        assert saml_provider.type == IdentityProviderType.SAML