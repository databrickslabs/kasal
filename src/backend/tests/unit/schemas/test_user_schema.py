"""
Unit tests for user schemas.

Tests the functionality of Pydantic schemas for user management
including validation, serialization, and field constraints.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError

from src.schemas.user import (
    UserBase, UserUpdate, UserPermissionUpdate, IdentityProviderType,
    UserInDB, UserResponse, GroupCreate, GroupUpdate
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


class TestUserPermissionUpdate:
    """Test cases for UserPermissionUpdate schema."""

    def test_valid_permission_update(self):
        """Test valid UserPermissionUpdate creation."""
        perm_update = UserPermissionUpdate(
            is_system_admin=True,
            is_personal_workspace_manager=False
        )

        assert perm_update.is_system_admin is True
        assert perm_update.is_personal_workspace_manager is False

    def test_permission_update_all_optional(self):
        """Test UserPermissionUpdate with all optional fields."""
        perm_update = UserPermissionUpdate()

        assert perm_update.is_system_admin is None
        assert perm_update.is_personal_workspace_manager is None

    def test_permission_update_partial(self):
        """Test UserPermissionUpdate with partial data."""
        perm_update = UserPermissionUpdate(is_system_admin=True)

        assert perm_update.is_system_admin is True
        assert perm_update.is_personal_workspace_manager is None


class TestUserInDB:
    """Test cases for UserInDB schema."""

    def test_valid_user_in_db(self):
        """Test valid UserInDB creation."""
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
        assert user.username == "dbuser"
        assert user.role == UserRole.TECHNICAL
        assert user.status == UserStatus.ACTIVE

    def test_user_in_db_defaults(self):
        """Test UserInDB default values."""
        user_data = {
            "id": "user456",
            "username": "defaultuser",
            "email": "default@example.com",
            "role": UserRole.REGULAR,
            "status": UserStatus.ACTIVE,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }

        user = UserInDB(**user_data)

        assert user.display_name is None
        assert user.is_system_admin is False
        assert user.is_personal_workspace_manager is False
        assert user.last_login is None

    def test_user_in_db_model_config(self):
        """Test UserInDB model configuration."""
        assert 'from_attributes' in UserInDB.model_config
        assert UserInDB.model_config['from_attributes'] is True
        assert UserInDB.model_config['use_enum_values'] is True

    def test_user_in_db_with_all_fields(self):
        """Test UserInDB with all fields populated."""
        now = datetime.now()
        user_data = {
            "id": "user789",
            "username": "fulluser",
            "email": "full@example.com",
            "display_name": "Full User",
            "role": UserRole.ADMIN,
            "status": UserStatus.ACTIVE,
            "is_system_admin": True,
            "is_personal_workspace_manager": True,
            "created_at": now,
            "updated_at": now,
            "last_login": now
        }

        user = UserInDB(**user_data)

        assert user.display_name == "Full User"
        assert user.is_system_admin is True
        assert user.is_personal_workspace_manager is True
        assert user.last_login == now


class TestUserResponse:
    """Test cases for UserResponse schema."""

    def test_user_response_inherits_user_in_db(self):
        """Test that UserResponse inherits from UserInDB."""
        assert issubclass(UserResponse, UserInDB)

    def test_valid_user_response(self):
        """Test valid UserResponse creation."""
        user_data = {
            "id": "user123",
            "username": "responseuser",
            "email": "response@example.com",
            "role": UserRole.REGULAR,
            "status": UserStatus.ACTIVE,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }

        response = UserResponse(**user_data)

        assert response.id == "user123"
        assert response.username == "responseuser"


class TestGroupSchemas:
    """Test cases for Group schemas."""

    def test_group_create(self):
        """Test GroupCreate schema."""
        group = GroupCreate(name="test-group", description="A test group")

        assert group.name == "test-group"
        assert group.description == "A test group"

    def test_group_create_minimal(self):
        """Test GroupCreate with minimal fields."""
        group = GroupCreate(name="minimal-group")

        assert group.name == "minimal-group"
        assert group.description is None

    def test_group_update(self):
        """Test GroupUpdate schema."""
        group_update = GroupUpdate(name="updated-group", description="Updated description")

        assert group_update.name == "updated-group"
        assert group_update.description == "Updated description"

    def test_group_update_all_optional(self):
        """Test GroupUpdate with all optional fields."""
        group_update = GroupUpdate()

        assert group_update.name is None
        assert group_update.description is None

    def test_group_update_partial(self):
        """Test GroupUpdate with partial data."""
        group_update = GroupUpdate(description="Only description updated")

        assert group_update.name is None
        assert group_update.description == "Only description updated"


class TestSchemaInteraction:
    """Test cases for schema interactions and edge cases."""

    def test_enum_integration(self):
        """Test that enums are properly integrated in schemas."""
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

    def test_optional_fields_behavior(self):
        """Test optional fields behavior across schemas."""
        # UserUpdate - all fields optional
        update = UserUpdate()
        assert update.username is None
        assert update.email is None
        assert update.status is None

        # UserPermissionUpdate - all fields optional
        perm_update = UserPermissionUpdate()
        assert perm_update.is_system_admin is None
        assert perm_update.is_personal_workspace_manager is None
