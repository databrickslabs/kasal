"""
Comprehensive unit tests for User and RefreshToken SQLAlchemy models.

Tests all models in user.py including table structure and utility functions.
"""
import pytest
from datetime import datetime, timezone
from sqlalchemy import Column, String, DateTime, Boolean, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from uuid import UUID

from src.models.user import generate_uuid, User, RefreshToken
from src.models.enums import UserRole, UserStatus, IdentityProviderType
from src.db.base import Base


class TestGenerateUuid:
    """Test generate_uuid utility function."""

    def test_generate_uuid_returns_string(self):
        """Test generate_uuid returns a string."""
        uuid_str = generate_uuid()
        
        assert isinstance(uuid_str, str)

    def test_generate_uuid_is_valid_uuid(self):
        """Test generate_uuid returns a valid UUID string."""
        uuid_str = generate_uuid()
        
        # Should be able to parse as UUID
        uuid_obj = UUID(uuid_str)
        assert str(uuid_obj) == uuid_str

    def test_generate_uuid_unique(self):
        """Test generate_uuid returns unique values."""
        uuid1 = generate_uuid()
        uuid2 = generate_uuid()
        
        assert uuid1 != uuid2

    def test_generate_uuid_format(self):
        """Test generate_uuid returns properly formatted UUID."""
        uuid_str = generate_uuid()
        
        # UUID4 format: 8-4-4-4-12 characters
        parts = uuid_str.split('-')
        assert len(parts) == 5
        assert len(parts[0]) == 8
        assert len(parts[1]) == 4
        assert len(parts[2]) == 4
        assert len(parts[3]) == 4
        assert len(parts[4]) == 12


class TestUser:
    """Test User model."""

    def test_user_inherits_base(self):
        """Test User inherits from Base."""
        assert issubclass(User, Base)

    def test_user_tablename(self):
        """Test User table name."""
        assert User.__tablename__ == "users"

    def test_user_columns_exist(self):
        """Test User has expected columns."""
        expected_columns = [
            'id', 'username', 'email', 'display_name', 'role', 'status',
            'is_system_admin', 'is_personal_workspace_manager', 'created_at',
            'updated_at', 'last_login'
        ]
        
        for column_name in expected_columns:
            assert hasattr(User, column_name)

    def test_user_id_column_properties(self):
        """Test id column properties."""
        id_column = User.id
        assert isinstance(id_column.property.columns[0], Column)
        assert isinstance(id_column.property.columns[0].type, String)
        assert id_column.property.columns[0].primary_key is True
        assert id_column.property.columns[0].default is not None

    def test_user_username_column_properties(self):
        """Test username column properties."""
        username_column = User.username
        column = username_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, String)
        assert column.unique is True
        assert column.index is True
        assert column.nullable is False

    def test_user_email_column_properties(self):
        """Test email column properties."""
        email_column = User.email
        column = email_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, String)
        assert column.unique is True
        assert column.index is True
        assert column.nullable is False

    def test_user_display_name_column_properties(self):
        """Test display_name column properties."""
        display_name_column = User.display_name
        column = display_name_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, String)
        assert column.nullable is True

    def test_user_role_column_properties(self):
        """Test role column properties."""
        role_column = User.role
        column = role_column.property.columns[0]
        assert isinstance(column, Column)
        # SQLAlchemy Enum column
        assert hasattr(column.type, 'enum_class')
        assert column.default is not None

    def test_user_status_column_properties(self):
        """Test status column properties."""
        status_column = User.status
        column = status_column.property.columns[0]
        assert isinstance(column, Column)
        # SQLAlchemy Enum column
        assert hasattr(column.type, 'enum_class')
        assert column.default is not None

    def test_user_boolean_columns_properties(self):
        """Test boolean column properties."""
        is_system_admin_column = User.is_system_admin
        column = is_system_admin_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, Boolean)
        assert column.default.arg is False
        assert column.nullable is False

        is_personal_workspace_manager_column = User.is_personal_workspace_manager
        column = is_personal_workspace_manager_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, Boolean)
        assert column.default.arg is False
        assert column.nullable is False

    def test_user_datetime_columns_properties(self):
        """Test datetime column properties."""
        created_at_column = User.created_at
        column = created_at_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, DateTime)
        assert column.type.timezone is True
        assert column.default is not None

        updated_at_column = User.updated_at
        column = updated_at_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, DateTime)
        assert column.type.timezone is True
        assert column.default is not None
        assert column.onupdate is not None

        last_login_column = User.last_login
        column = last_login_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, DateTime)
        assert column.type.timezone is True
        assert column.nullable is True

    def test_user_relationships(self):
        """Test User relationships."""
        assert hasattr(User, 'refresh_tokens')
        # Check if it's a relationship
        assert hasattr(User.refresh_tokens.property, 'mapper')

    def test_user_initialization(self):
        """Test User initialization."""
        user = User()
        
        assert isinstance(user, User)
        assert isinstance(user, Base)

    def test_user_initialization_with_values(self):
        """Test User initialization with values."""
        user = User(
            username="testuser",
            email="test@example.com",
            display_name="Test User",
            role=UserRole.ADMIN,
            status=UserStatus.ACTIVE,
            is_system_admin=True,
            is_personal_workspace_manager=True
        )
        
        assert user.username == "testuser"
        assert user.email == "test@example.com"
        assert user.display_name == "Test User"
        assert user.role == UserRole.ADMIN
        assert user.status == UserStatus.ACTIVE
        assert user.is_system_admin is True
        assert user.is_personal_workspace_manager is True

    def test_user_initialization_minimal(self):
        """Test User initialization with minimal required fields."""
        user = User(
            username="testuser",
            email="test@example.com"
        )
        
        assert user.username == "testuser"
        assert user.email == "test@example.com"
        assert user.display_name is None
        # Default values are applied at database level, so these will be None until saved
        assert user.role is None  # Will be UserRole.REGULAR when saved
        assert user.status is None  # Will be UserStatus.ACTIVE when saved
        assert user.is_system_admin is None  # Will be False when saved
        assert user.is_personal_workspace_manager is None  # Will be False when saved


class TestRefreshToken:
    """Test RefreshToken model."""

    def test_refresh_token_inherits_base(self):
        """Test RefreshToken inherits from Base."""
        assert issubclass(RefreshToken, Base)

    def test_refresh_token_tablename(self):
        """Test RefreshToken table name."""
        assert RefreshToken.__tablename__ == "refresh_tokens"

    def test_refresh_token_columns_exist(self):
        """Test RefreshToken has expected columns."""
        expected_columns = [
            'id', 'user_id', 'token', 'expires_at', 'is_revoked', 'created_at'
        ]
        
        for column_name in expected_columns:
            assert hasattr(RefreshToken, column_name)

    def test_refresh_token_id_column_properties(self):
        """Test id column properties."""
        id_column = RefreshToken.id
        assert isinstance(id_column.property.columns[0], Column)
        assert isinstance(id_column.property.columns[0].type, String)
        assert id_column.property.columns[0].primary_key is True
        assert id_column.property.columns[0].default is not None

    def test_refresh_token_user_id_column_properties(self):
        """Test user_id column properties."""
        user_id_column = RefreshToken.user_id
        column = user_id_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, String)
        assert len(column.foreign_keys) == 1
        # Check foreign key reference
        fk = list(column.foreign_keys)[0]
        assert str(fk.column) == "users.id"
        assert fk.ondelete == "CASCADE"

    def test_refresh_token_token_column_properties(self):
        """Test token column properties."""
        token_column = RefreshToken.token
        column = token_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, String)
        assert column.nullable is False
        assert column.unique is True

    def test_refresh_token_expires_at_column_properties(self):
        """Test expires_at column properties."""
        expires_at_column = RefreshToken.expires_at
        column = expires_at_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, DateTime)
        assert column.type.timezone is True
        assert column.nullable is False

    def test_refresh_token_is_revoked_column_properties(self):
        """Test is_revoked column properties."""
        is_revoked_column = RefreshToken.is_revoked
        column = is_revoked_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, Boolean)
        assert column.default.arg is False

    def test_refresh_token_created_at_column_properties(self):
        """Test created_at column properties."""
        created_at_column = RefreshToken.created_at
        column = created_at_column.property.columns[0]
        assert isinstance(column, Column)
        assert isinstance(column.type, DateTime)
        assert column.type.timezone is True
        assert column.default is not None

    def test_refresh_token_relationships(self):
        """Test RefreshToken relationships."""
        assert hasattr(RefreshToken, 'user')
        # Check if it's a relationship
        assert hasattr(RefreshToken.user.property, 'mapper')

    def test_refresh_token_initialization(self):
        """Test RefreshToken initialization."""
        token = RefreshToken()
        
        assert isinstance(token, RefreshToken)
        assert isinstance(token, Base)

    def test_refresh_token_initialization_with_values(self):
        """Test RefreshToken initialization with values."""
        expires_at = datetime.now(timezone.utc)
        
        token = RefreshToken(
            user_id="user-123",
            token="hashed_token_value",
            expires_at=expires_at,
            is_revoked=False
        )
        
        assert token.user_id == "user-123"
        assert token.token == "hashed_token_value"
        assert token.expires_at == expires_at
        assert token.is_revoked is False


class TestUserModelInteroperability:
    """Test User and RefreshToken model interoperability."""

    def test_all_models_inherit_base(self):
        """Test all models inherit from Base."""
        assert issubclass(User, Base)
        assert issubclass(RefreshToken, Base)

    def test_all_models_have_sqlalchemy_attributes(self):
        """Test all models have SQLAlchemy attributes."""
        models = [User, RefreshToken]
        
        for model in models:
            assert hasattr(model, '__table__')
            assert hasattr(model, '__mapper__')
            assert hasattr(model, 'metadata')

    def test_all_models_have_id_primary_key(self):
        """Test all models have id as primary key."""
        models = [User, RefreshToken]
        
        for model in models:
            table = model.__table__
            primary_key_columns = [col.name for col in table.primary_key.columns]
            assert primary_key_columns == ['id']

    def test_all_models_use_generate_uuid(self):
        """Test all models use generate_uuid for default id."""
        models = [User, RefreshToken]
        
        for model in models:
            id_column = model.id.property.columns[0]
            assert id_column.default is not None

    def test_user_refresh_token_relationship(self):
        """Test User-RefreshToken relationship configuration."""
        # Check User side of relationship
        user_relationship = User.refresh_tokens
        assert user_relationship.property.back_populates == "user"
        assert "delete-orphan" in str(user_relationship.property.cascade)
        
        # Check RefreshToken side of relationship
        token_relationship = RefreshToken.user
        assert token_relationship.property.back_populates == "refresh_tokens"

    def test_foreign_key_relationship(self):
        """Test foreign key relationship between User and RefreshToken."""
        user_id_column = RefreshToken.user_id.property.columns[0]
        foreign_keys = list(user_id_column.foreign_keys)
        
        assert len(foreign_keys) == 1
        fk = foreign_keys[0]
        assert str(fk.column) == "users.id"
        assert fk.ondelete == "CASCADE"

    def test_enum_usage(self):
        """Test enum usage in User model."""
        role_column = User.role.property.columns[0]
        status_column = User.status.property.columns[0]
        
        # Check that enum types are used
        assert hasattr(role_column.type, 'enum_class')
        assert hasattr(status_column.type, 'enum_class')

    def test_unique_constraints(self):
        """Test unique constraints on User model."""
        table = User.__table__
        
        # Check unique columns
        unique_columns = []
        for column in table.columns:
            if column.unique:
                unique_columns.append(column.name)
        
        assert 'username' in unique_columns
        assert 'email' in unique_columns

    def test_indexed_columns(self):
        """Test indexed columns."""
        user_table = User.__table__
        
        indexed_columns = []
        for column in user_table.columns:
            if column.index:
                indexed_columns.append(column.name)
        
        assert 'username' in indexed_columns
        assert 'email' in indexed_columns

    def test_timezone_aware_datetime_columns(self):
        """Test timezone-aware datetime columns."""
        user_datetime_columns = ['created_at', 'updated_at', 'last_login']
        token_datetime_columns = ['expires_at', 'created_at']
        
        for col_name in user_datetime_columns:
            column = User.__table__.columns[col_name]
            if isinstance(column.type, DateTime):
                assert column.type.timezone is True
        
        for col_name in token_datetime_columns:
            column = RefreshToken.__table__.columns[col_name]
            if isinstance(column.type, DateTime):
                assert column.type.timezone is True
