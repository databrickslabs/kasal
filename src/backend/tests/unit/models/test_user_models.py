"""
Unit tests for user models.

Tests the functionality of the User database model
including field validation, relationships, and data integrity.
"""
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

from src.models.user import User, generate_uuid
from src.models.enums import UserRole as UserRoleEnum, UserStatus


class TestUser:
    """Test cases for User model."""

    def test_user_creation(self):
        """Test basic User model creation."""
        # Arrange
        username = "testuser"
        email = "test@example.com"
        # Act
        user = User(
            username=username,
            email=email,
        )
        # Assert
        assert user.username == username
        assert user.email == email
        # Note: SQLAlchemy defaults are applied when saved to database
        assert User.__table__.columns['role'].default.arg == UserRoleEnum.REGULAR
        assert User.__table__.columns['status'].default.arg == UserStatus.ACTIVE
        assert user.last_login is None

    def test_user_with_all_fields(self):
        """Test User model creation with all fields."""
        # Arrange
        username = "adminuser"
        email = "admin@company.com"
        role = UserRoleEnum.ADMIN
        status = UserStatus.ACTIVE
        last_login = datetime.now(timezone.utc)
        display_name = "Admin User"
        # Act
        user = User(
            username=username,
            email=email,
            role=role,
            status=status,
            last_login=last_login,
            display_name=display_name,
        )
        # Assert
        assert user.username == username
        assert user.email == email
        assert user.role == role
        assert user.status == status
        assert user.last_login == last_login
        assert user.display_name == display_name

    def test_user_role_enum_values(self):
        """Test User with different role enum values."""
        for role in [UserRoleEnum.ADMIN, UserRoleEnum.TECHNICAL, UserRoleEnum.REGULAR]:
            user = User(
                username=f"user_{role.value}",
                email=f"{role.value}@example.com",
                role=role
            )
            assert user.role == role

    def test_user_status_enum_values(self):
        """Test User with different status enum values."""
        for status in [UserStatus.ACTIVE, UserStatus.INACTIVE, UserStatus.SUSPENDED]:
            user = User(
                username=f"user_{status.value}",
                email=f"{status.value}@example.com",
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

    def test_user_display_name(self):
        """Test User display_name field (moved from UserProfile)."""
        # Arrange & Act
        user = User(
            username="integrated_user",
            email="integrated@example.com",
            display_name="Integrated User",
        )

        # Assert
        assert user.username == "integrated_user"
        assert user.display_name == "Integrated User"

    def test_user_permission_fields(self):
        """Test User permission fields."""
        # Arrange & Act
        user = User(
            username="admin_user",
            email="admin@example.com",
            is_system_admin=True,
            is_personal_workspace_manager=True
        )

        # Assert
        assert user.is_system_admin is True
        assert user.is_personal_workspace_manager is True
