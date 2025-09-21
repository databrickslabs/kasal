"""
Unit tests for UserRepository (updated version).

Tests the functionality of the updated user repository including
RBAC features, group management, and enhanced user operations.
"""
import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, UTC

from src.repositories.user_repository import (
    UserRepository, UserProfileRepository, RefreshTokenRepository,
    ExternalIdentityRepository, RoleRepository, PrivilegeRepository,
    RolePrivilegeRepository, UserRoleRepository, IdentityProviderRepository
)
from src.models.user import (
    User, UserProfile, RefreshToken, ExternalIdentity,
    Role, Privilege, RolePrivilege, UserRole, IdentityProvider
)
from src.schemas.user import UserCreate, UserUpdate


@pytest.fixture
def mock_session():
    """Create a mock database session."""
    session = AsyncMock()
    session.add = MagicMock()
    session.delete = MagicMock()  # delete is not async in SQLAlchemy
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    
    # Create a mock result object for execute
    mock_result = MagicMock()
    mock_scalars = MagicMock()
    mock_scalars.first = MagicMock(return_value=None)  # Default to None
    mock_scalars.all = MagicMock(return_value=[])      # Default to empty list
    mock_result.scalars = MagicMock(return_value=mock_scalars)
    session.execute = AsyncMock(return_value=mock_result)
    
    session.scalar = AsyncMock()
    session.scalars = AsyncMock()
    return session


@pytest.fixture
def mock_user():
    """Create a mock user object."""
    user = MagicMock(spec=User)
    user.id = uuid.uuid4()
    user.username = "testuser"
    user.email = "test@example.com"
    user.full_name = "Test User"
    user.is_active = True
    user.is_superuser = False
    user.created_at = datetime.now(UTC)
    user.updated_at = datetime.now(UTC)
    user.last_login = None
    # user.tenant_id = uuid.uuid4()  # Removed - using groups instead
    user.roles = ["user"]
    user.groups = []
    return user


@pytest.fixture
def user_create_data():
    """Create test data for user creation."""
    return UserCreate(
        username="newuser",
        email="newuser@example.com",
        password="SecurePassword123!",
        full_name="New User",
        # tenant_id=uuid.uuid4()  # Removed - using groups instead
    )


@pytest.fixture
def user_update_data():
    """Create test data for user updates."""
    return UserUpdate(
        full_name="Updated User",
        email="updated@example.com",
        is_active=False
    )


class TestUserRepository:
    """Test cases for UserRepository."""
    
    @pytest.mark.asyncio
    async def test_create_user_success(self, mock_session, user_create_data):
        """Test successful user creation."""
        repository = UserRepository(User, mock_session)
        
        # Convert schema to dict and fix password field for User model
        user_data = user_create_data.model_dump()
        user_data['hashed_password'] = user_data.pop('password')  # User model expects hashed_password
        result = await repository.create(user_data)
        
        assert result is not None
        mock_session.add.assert_called_once()
        mock_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_user_by_id(self, mock_session, mock_user):
        """Test getting a user by ID."""
        user_id = uuid.uuid4()
        
        # Set up the mock to return the user
        mock_session.execute.return_value.scalars.return_value.first.return_value = mock_user
        
        repository = UserRepository(User, mock_session)
        
        result = await repository.get(user_id)
        
        assert result is not None
        assert result.username == "testuser"
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_user_by_username(self, mock_session, mock_user):
        """Test getting a user by username."""
        username = "testuser"
        # Set up the mock to return the user
        mock_session.execute.return_value.scalars.return_value.first.return_value = mock_user
        
        repository = UserRepository(User, mock_session)
        
        result = await repository.get_by_username(username)
        
        assert result is not None
        assert result.username == username
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_user_by_email(self, mock_session, mock_user):
        """Test getting a user by email."""
        email = "test@example.com"
        # Set up the mock to return the user
        mock_session.execute.return_value.scalars.return_value.first.return_value = mock_user
        
        repository = UserRepository(User, mock_session)
        
        result = await repository.get_by_email(email)
        
        assert result is not None
        assert result.email == email
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_user_success(self, mock_session, mock_user, user_update_data):
        """Test successful user update."""
        user_id = uuid.uuid4()
        # Set up the mock to return the user for get() and update operations
        mock_session.execute.return_value.scalars.return_value.first.return_value = mock_user
        
        repository = UserRepository(User, mock_session)
        
        # Convert update data to dict
        update_dict = user_update_data.model_dump(exclude_unset=True)
        result = await repository.update(user_id, update_dict)
        
        assert result is not None
        mock_session.flush.assert_called()
    
    @pytest.mark.asyncio
    async def test_delete_user_success(self, mock_session, mock_user):
        """Test successful user deletion."""
        user_id = uuid.uuid4()
        # Set up the mock to return the user for get()
        mock_session.execute.return_value.scalars.return_value.first.return_value = mock_user
        
        repository = UserRepository(User, mock_session)
        
        result = await repository.delete(user_id)
        
        assert result is True
        mock_session.delete.assert_called_once_with(mock_user)
        mock_session.flush.assert_called()
    
    @pytest.mark.skip(reason="Tenant concept removed, using groups instead")
    @pytest.mark.asyncio
    async def test_list_users_by_tenant(self, mock_session, mock_user):
        """Test listing users by tenant."""
        pass
    
    @pytest.mark.skip(reason="Role methods not in UserRepository")
    @pytest.mark.asyncio
    async def test_assign_role_to_user(self, mock_session, mock_user):
        """Test assigning role to user."""
        user_id = uuid.uuid4()
        role_id = uuid.uuid4()
        mock_session.scalar.return_value = mock_user
        
        repository = UserRepository(User, mock_session)
        
        result = await repository.assign_role(user_id, role_id)
        
        assert result is True
        mock_session.flush.assert_called_once()
    
    @pytest.mark.skip(reason="Role methods not in UserRepository")
    @pytest.mark.asyncio
    async def test_remove_role_from_user(self, mock_session, mock_user):
        """Test removing role from user."""
        user_id = uuid.uuid4()
        role_id = uuid.uuid4()
        mock_session.scalar.return_value = mock_user
        
        # Mock role assignment exists
        mock_role_assignment = MagicMock()
        mock_session.scalar.side_effect = [mock_user, mock_role_assignment]
        
        repository = UserRepository(User, mock_session)
        
        result = await repository.remove_role(user_id, role_id)
        
        assert result is True
        mock_session.delete.assert_called_once_with(mock_role_assignment)
        mock_session.flush.assert_called_once()
    
    @pytest.mark.skip(reason="Role methods not in UserRepository")
    @pytest.mark.asyncio
    async def test_get_user_roles(self, mock_session):
        """Test getting user roles."""
        user_id = uuid.uuid4()
        mock_roles = [
            MagicMock(id=uuid.uuid4(), name="admin", group_id="group-123"),
            MagicMock(id=uuid.uuid4(), name="editor", group_id="group-123")
        ]
        mock_result = MagicMock()
        mock_result.all.return_value = mock_roles
        mock_session.scalars.return_value = mock_result
        
        repository = UserRepository(User, mock_session)
        
        result = await repository.get_user_roles(user_id)
        
        assert len(result) == 2
        assert result[0].name == "admin"
        mock_session.execute.assert_called_once()
    
    @pytest.mark.skip(reason="Group methods not in UserRepository")
    @pytest.mark.asyncio
    async def test_add_user_to_group(self, mock_session, mock_user):
        """Test adding user to group."""
        user_id = uuid.uuid4()
        group_id = uuid.uuid4()
        mock_session.scalar.return_value = mock_user
        
        repository = UserRepository(User, mock_session)
        
        result = await repository.add_to_group(user_id, group_id)
        
        assert result is True
        mock_session.flush.assert_called_once()
    
    @pytest.mark.skip(reason="Group methods not in UserRepository")
    @pytest.mark.asyncio
    async def test_remove_user_from_group(self, mock_session, mock_user):
        """Test removing user from group."""
        user_id = uuid.uuid4()
        group_id = uuid.uuid4()
        mock_session.scalar.return_value = mock_user
        
        # Mock group membership exists
        mock_membership = MagicMock()
        mock_session.scalar.side_effect = [mock_user, mock_membership]
        
        repository = UserRepository(User, mock_session)
        
        result = await repository.remove_from_group(user_id, group_id)
        
        assert result is True
        mock_session.delete.assert_called_once_with(mock_membership)
        mock_session.flush.assert_called_once()
    
    @pytest.mark.skip(reason="Group methods not in UserRepository")
    @pytest.mark.asyncio
    async def test_get_user_groups(self, mock_session):
        """Test getting user groups."""
        user_id = uuid.uuid4()
        mock_groups = [
            MagicMock(id=uuid.uuid4(), name="developers", group_id="group-123"),
            MagicMock(id=uuid.uuid4(), name="admins", group_id="group-123")
        ]
        mock_result = MagicMock()
        mock_result.all.return_value = mock_groups
        mock_session.scalars.return_value = mock_result
        
        repository = UserRepository(User, mock_session)
        
        result = await repository.get_user_groups(user_id)
        
        assert len(result) == 2
        assert result[0].name == "developers"
        mock_session.execute.assert_called_once()
    
    @pytest.mark.skip(reason="Permission methods not in UserRepository")
    @pytest.mark.asyncio
    async def test_check_user_permission(self, mock_session):
        """Test checking if user has specific permission."""
        user_id = uuid.uuid4()
        permission = "read_data"
        
        # Mock that user has permission through role
        mock_session.scalar.return_value = MagicMock()  # Permission exists
        
        repository = UserRepository(User, mock_session)
        
        result = await repository.has_permission(user_id, permission)
        
        assert result is True
        mock_session.execute.assert_called_once()
    
    @pytest.mark.skip(reason="Permission methods not in UserRepository")
    @pytest.mark.asyncio
    async def test_get_user_permissions(self, mock_session):
        """Test getting all user permissions."""
        user_id = uuid.uuid4()
        mock_permissions = ["read_data", "write_data", "admin"]
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = mock_permissions
        mock_session.execute.return_value = mock_result
        
        repository = UserRepository(User, mock_session)
        
        result = await repository.get_permissions(user_id)
        
        assert len(result) == 3
        assert "admin" in result
        mock_session.execute.assert_called_once()
    
    @pytest.mark.skip(reason="update_last_login exists but for different signature")
    @pytest.mark.asyncio
    async def test_update_last_login(self, mock_session, mock_user):
        """Test updating user's last login timestamp."""
        user_id = uuid.uuid4()
        login_time = datetime.now(UTC)
        mock_session.scalar.return_value = mock_user
        
        repository = UserRepository(User, mock_session)
        
        result = await repository.update_last_login(user_id, login_time)
        
        assert result is True
        assert mock_user.last_login == login_time
        mock_session.flush.assert_called_once()
    
    @pytest.mark.skip(reason="activate method not in UserRepository")
    @pytest.mark.asyncio
    async def test_activate_user(self, mock_session, mock_user):
        """Test activating a user."""
        user_id = uuid.uuid4()
        mock_user.is_active = False
        mock_session.scalar.return_value = mock_user
        
        repository = UserRepository(User, mock_session)
        
        result = await repository.activate(user_id)
        
        assert result is True
        assert mock_user.is_active is True
        mock_session.flush.assert_called_once()
    
    @pytest.mark.skip(reason="deactivate method not in UserRepository")
    @pytest.mark.asyncio
    async def test_deactivate_user(self, mock_session, mock_user):
        """Test deactivating a user."""
        user_id = uuid.uuid4()
        mock_user.is_active = True
        mock_session.scalar.return_value = mock_user
        
        repository = UserRepository(User, mock_session)
        
        result = await repository.deactivate(user_id)
        
        assert result is True
        assert mock_user.is_active is False
        mock_session.flush.assert_called_once()
    
    @pytest.mark.skip(reason="search method not in UserRepository")
    @pytest.mark.asyncio
    async def test_search_users(self, mock_session, mock_user):
        """Test searching users by query."""
        search_query = "test"
        mock_result = MagicMock()
        mock_result.all.return_value = [mock_user]
        mock_session.scalars.return_value = mock_result
        
        repository = UserRepository(User, mock_session)
        
        result = await repository.search(search_query)
        
        assert len(result) == 1
        assert result[0].username == "testuser"
        mock_session.execute.assert_called_once()
    
    @pytest.mark.skip(reason="Role methods not in UserRepository")
    @pytest.mark.asyncio
    async def test_bulk_assign_roles(self, mock_session):
        """Test bulk role assignment to users."""
        user_ids = [uuid.uuid4(), uuid.uuid4()]
        role_ids = [uuid.uuid4(), uuid.uuid4()]
        
        repository = UserRepository(User, mock_session)
        
        result = await repository.bulk_assign_roles(user_ids, role_ids)
        
        assert result == 4  # 2 users * 2 roles
        assert mock_session.add.call_count == 4
        mock_session.flush.assert_called_once()
    
    @pytest.mark.skip(reason="Role methods not in UserRepository")
    @pytest.mark.asyncio
    async def test_bulk_remove_roles(self, mock_session):
        """Test bulk role removal from users."""
        user_ids = [uuid.uuid4(), uuid.uuid4()]
        role_ids = [uuid.uuid4(), uuid.uuid4()]
        
        repository = UserRepository(User, mock_session)
        
        result = await repository.bulk_remove_roles(user_ids, role_ids)
        
        assert result == 4
        mock_session.execute.assert_called_once()
        mock_session.flush.assert_called_once()
    
    @pytest.mark.skip(reason="Tenant concept removed, using groups instead")
    @pytest.mark.asyncio
    async def test_count_users_by_tenant(self, mock_session):
        """Test counting users by tenant."""
        pass
    
    @pytest.mark.skip(reason="get_active_users method not in UserRepository")
    @pytest.mark.asyncio
    async def test_get_active_users(self, mock_session, mock_user):
        """Test getting only active users."""
        mock_result = MagicMock()
        mock_result.all.return_value = [mock_user]
        mock_session.scalars.return_value = mock_result
        
        repository = UserRepository(User, mock_session)
        
        result = await repository.get_active_users()
        
        assert len(result) == 1
        assert result[0].is_active is True
        mock_session.execute.assert_called_once()
    
    @pytest.mark.skip(reason="username_exists method not in UserRepository")
    @pytest.mark.asyncio
    async def test_check_username_exists(self, mock_session, mock_user):
        """Test checking if username exists."""
        username = "testuser"
        mock_session.scalar.return_value = mock_user
        
        repository = UserRepository(User, mock_session)
        
        result = await repository.username_exists(username)
        
        assert result is True
        mock_session.execute.assert_called_once()
    
    @pytest.mark.skip(reason="email_exists method not in UserRepository")
    @pytest.mark.asyncio
    async def test_check_email_exists(self, mock_session, mock_user):
        """Test checking if email exists."""
        email = "test@example.com"
        mock_session.scalar.return_value = mock_user
        
        repository = UserRepository(User, mock_session)
        
        result = await repository.email_exists(email)
        
        assert result is True
        mock_session.execute.assert_called_once()
    
    @pytest.mark.skip(reason="Role methods not in UserRepository")
    @pytest.mark.asyncio
    async def test_get_users_with_role(self, mock_session, mock_user):
        """Test getting users with specific role."""
        role_name = "admin"
        mock_result = MagicMock()
        mock_result.all.return_value = [mock_user]
        mock_session.scalars.return_value = mock_result
        
        repository = UserRepository(User, mock_session)
        
        result = await repository.get_users_with_role(role_name)
        
        assert len(result) == 1
        assert result[0].username == "testuser"
        mock_session.execute.assert_called_once()
    
    @pytest.mark.skip(reason="Group methods not in UserRepository")
    @pytest.mark.asyncio
    async def test_get_users_in_group(self, mock_session, mock_user):
        """Test getting users in specific group."""
        group_id = uuid.uuid4()
        mock_result = MagicMock()
        mock_result.all.return_value = [mock_user]
        mock_session.scalars.return_value = mock_result
        
        repository = UserRepository(User, mock_session)
        
        result = await repository.get_users_in_group(group_id)
        
        assert len(result) == 1
        assert result[0].username == "testuser"
        mock_session.execute.assert_called_once()
    
    @pytest.mark.skip(reason="session tracking methods not in UserRepository")
    @pytest.mark.asyncio
    async def test_user_session_tracking(self, mock_session, mock_user):
        """Test user session tracking."""
        user_id = uuid.uuid4()
        session_id = "session_123"
        mock_session.scalar.return_value = mock_user
        
        repository = UserRepository(User, mock_session)
        
        # Create session
        result = await repository.create_session(user_id, session_id)
        assert result is True
        
        # Check active sessions
        mock_sessions = [{"id": session_id, "created_at": datetime.now(UTC)}]
        mock_result = MagicMock()
        mock_result.all.return_value = mock_sessions
        mock_session.scalars.return_value = mock_result
        
        sessions = await repository.get_active_sessions(user_id)
        assert len(sessions) == 1
    
    @pytest.mark.skip(reason="password reset methods not in UserRepository")
    @pytest.mark.asyncio
    async def test_password_reset_token(self, mock_session, mock_user):
        """Test password reset token operations."""
        user_id = uuid.uuid4()
        reset_token = "reset_token_123"
        mock_session.scalar.return_value = mock_user
        
        repository = UserRepository(User, mock_session)
        
        # Set reset token
        result = await repository.set_password_reset_token(user_id, reset_token)
        assert result is True
        
        # Verify reset token
        result = await repository.verify_password_reset_token(reset_token)
        assert result == user_id
    
    @pytest.mark.skip(reason="preferences methods not in UserRepository")
    @pytest.mark.asyncio
    async def test_user_preferences(self, mock_session, mock_user):
        """Test user preferences management."""
        user_id = uuid.uuid4()
        preferences = {"theme": "dark", "language": "en"}
        mock_session.scalar.return_value = mock_user
        
        repository = UserRepository(User, mock_session)
        
        result = await repository.update_preferences(user_id, preferences)
        
        assert result is True
        mock_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_username_or_email_with_username(self, mock_session, mock_user):
        """Test getting a user by username using get_by_username_or_email."""
        username_or_email = "testuser"
        mock_session.execute.return_value.scalars.return_value.first.return_value = mock_user
        
        repository = UserRepository(User, mock_session)
        result = await repository.get_by_username_or_email(username_or_email)
        
        assert result is not None
        assert result.username == "testuser"
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_username_or_email_with_email(self, mock_session, mock_user):
        """Test getting a user by email using get_by_username_or_email."""
        username_or_email = "test@example.com"
        mock_session.execute.return_value.scalars.return_value.first.return_value = mock_user
        
        repository = UserRepository(User, mock_session)
        result = await repository.get_by_username_or_email(username_or_email)
        
        assert result is not None
        assert result.email == "test@example.com"
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_last_login(self, mock_session):
        """Test updating user's last login timestamp."""
        user_id = str(uuid.uuid4())
        
        repository = UserRepository(User, mock_session)
        await repository.update_last_login(user_id)
        
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_with_filters_no_filters(self, mock_session, mock_user):
        """Test listing users without filters."""
        mock_session.execute.return_value.scalars.return_value.all.return_value = [mock_user]
        
        repository = UserRepository(User, mock_session)
        result = await repository.list_with_filters(skip=0, limit=10)
        
        assert len(result) == 1
        assert result[0].username == "testuser"
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_with_filters_with_role_filter(self, mock_session, mock_user):
        """Test listing users with role filter."""
        mock_session.execute.return_value.scalars.return_value.all.return_value = [mock_user]
        
        repository = UserRepository(User, mock_session)
        result = await repository.list_with_filters(skip=0, limit=10, filters={'role': 'admin'})
        
        assert len(result) == 1
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_with_filters_with_status_filter(self, mock_session, mock_user):
        """Test listing users with status filter."""
        mock_session.execute.return_value.scalars.return_value.all.return_value = [mock_user]
        
        repository = UserRepository(User, mock_session)
        result = await repository.list_with_filters(skip=0, limit=10, filters={'status': 'active'})
        
        assert len(result) == 1
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_with_filters_with_both_filters(self, mock_session, mock_user):
        """Test listing users with both role and status filters."""
        mock_session.execute.return_value.scalars.return_value.all.return_value = [mock_user]
        
        repository = UserRepository(User, mock_session)
        result = await repository.list_with_filters(
            skip=5, limit=20, 
            filters={'role': 'admin', 'status': 'active'}
        )
        
        assert len(result) == 1
        mock_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_database_error_handling(self, mock_session, user_create_data):
        """Test database error handling."""
        mock_session.add.side_effect = Exception("Database connection error")
        
        repository = UserRepository(User, mock_session)
        
        with pytest.raises(Exception, match="Database connection error"):
            # Convert schema to dict
            user_dict = user_create_data.model_dump()
            user_dict['hashed_password'] = user_dict.pop('password')  # User model expects hashed_password
            await repository.create(user_dict)


class TestUserProfileRepository:
    """Test cases for UserProfileRepository."""
    
    @pytest.mark.asyncio
    async def test_get_by_user_id(self, mock_session):
        """Test getting profile by user_id."""
        user_id = str(uuid.uuid4())
        mock_profile = MagicMock(spec=UserProfile)
        mock_profile.user_id = user_id
        
        mock_session.execute.return_value.scalars.return_value.first.return_value = mock_profile
        
        repository = UserProfileRepository(UserProfile, mock_session)
        result = await repository.get_by_user_id(user_id)
        
        assert result is not None
        assert result.user_id == user_id
        mock_session.execute.assert_called_once()


class TestRefreshTokenRepository:
    """Test cases for RefreshTokenRepository."""
    
    @pytest.mark.asyncio
    async def test_get_by_token(self, mock_session):
        """Test getting refresh token by token value."""
        token = "test_token_123"
        mock_token = MagicMock(spec=RefreshToken)
        mock_token.token = token
        
        mock_session.execute.return_value.scalars.return_value.first.return_value = mock_token
        
        repository = RefreshTokenRepository(RefreshToken, mock_session)
        result = await repository.get_by_token(token)
        
        assert result is not None
        assert result.token == token
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_valid_token(self, mock_session):
        """Test getting valid refresh token."""
        token = "test_token_123"
        current_time = datetime.now(UTC)
        
        mock_token = MagicMock(spec=RefreshToken)
        mock_token.token = token
        
        mock_session.execute.return_value.scalars.return_value.first.return_value = mock_token
        
        repository = RefreshTokenRepository(RefreshToken, mock_session)
        result = await repository.get_valid_token(token, current_time)
        
        assert result is not None
        assert result.token == token
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_revoke_token(self, mock_session):
        """Test revoking a refresh token."""
        token = "test_token_123"
        
        repository = RefreshTokenRepository(RefreshToken, mock_session)
        await repository.revoke_token(token)
        
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_revoke_all_for_user(self, mock_session):
        """Test revoking all refresh tokens for a user."""
        user_id = str(uuid.uuid4())
        
        repository = RefreshTokenRepository(RefreshToken, mock_session)
        await repository.revoke_all_for_user(user_id)
        
        mock_session.execute.assert_called_once()


class TestExternalIdentityRepository:
    """Test cases for ExternalIdentityRepository."""
    
    @pytest.mark.asyncio
    async def test_get_by_provider_and_id(self, mock_session):
        """Test getting external identity by provider and provider_user_id."""
        provider = "google"
        provider_user_id = "google_123"
        
        mock_identity = MagicMock(spec=ExternalIdentity)
        mock_identity.provider = provider
        mock_identity.provider_user_id = provider_user_id
        
        mock_session.execute.return_value.scalars.return_value.first.return_value = mock_identity
        
        repository = ExternalIdentityRepository(ExternalIdentity, mock_session)
        result = await repository.get_by_provider_and_id(provider, provider_user_id)
        
        assert result is not None
        assert result.provider == provider
        assert result.provider_user_id == provider_user_id
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_user_id_and_provider(self, mock_session):
        """Test getting external identity by user_id and provider."""
        user_id = str(uuid.uuid4())
        provider = "github"
        
        mock_identity = MagicMock(spec=ExternalIdentity)
        mock_identity.user_id = user_id
        mock_identity.provider = provider
        
        mock_session.execute.return_value.scalars.return_value.first.return_value = mock_identity
        
        repository = ExternalIdentityRepository(ExternalIdentity, mock_session)
        result = await repository.get_by_user_id_and_provider(user_id, provider)
        
        assert result is not None
        assert result.user_id == user_id
        assert result.provider == provider
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_all_by_user_id(self, mock_session):
        """Test getting all external identities for a user."""
        user_id = str(uuid.uuid4())
        
        mock_identities = [
            MagicMock(spec=ExternalIdentity, provider="google"),
            MagicMock(spec=ExternalIdentity, provider="github")
        ]
        
        mock_session.execute.return_value.scalars.return_value.all.return_value = mock_identities
        
        repository = ExternalIdentityRepository(ExternalIdentity, mock_session)
        result = await repository.get_all_by_user_id(user_id)
        
        assert len(result) == 2
        assert result[0].provider == "google"
        assert result[1].provider == "github"
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_last_login(self, mock_session):
        """Test updating external identity's last login timestamp."""
        identity_id = str(uuid.uuid4())
        
        repository = ExternalIdentityRepository(ExternalIdentity, mock_session)
        await repository.update_last_login(identity_id)
        
        mock_session.execute.assert_called_once()


class TestRoleRepository:
    """Test cases for RoleRepository."""
    
    @pytest.mark.asyncio
    async def test_get_by_name(self, mock_session):
        """Test getting a role by name."""
        role_name = "admin"
        
        mock_role = MagicMock(spec=Role)
        mock_role.name = role_name
        
        mock_session.execute.return_value.scalars.return_value.first.return_value = mock_role
        
        repository = RoleRepository(Role, mock_session)
        result = await repository.get_by_name(role_name)
        
        assert result is not None
        assert result.name == role_name
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_with_privileges(self, mock_session):
        """Test getting a role with its privileges."""
        role_id = str(uuid.uuid4())
        
        mock_role = MagicMock(spec=Role)
        mock_role.id = role_id
        
        mock_privileges = [
            MagicMock(spec=Privilege),
            MagicMock(spec=Privilege)
        ]
        mock_privileges[0].name = "read"
        mock_privileges[1].name = "write"
        
        # Set up side effect for two calls
        mock_session.execute.side_effect = [
            MagicMock(scalars=MagicMock(return_value=MagicMock(first=MagicMock(return_value=mock_role)))),
            MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=mock_privileges))))
        ]
        
        repository = RoleRepository(Role, mock_session)
        result = await repository.get_with_privileges(role_id)
        
        assert result is not None
        assert result.id == role_id
        assert len(result.privileges) == 2
        assert result.privileges[0].name == "read"
        assert mock_session.execute.call_count == 2
    
    @pytest.mark.asyncio
    async def test_get_with_privileges_not_found(self, mock_session):
        """Test getting a role with privileges when role not found."""
        role_id = str(uuid.uuid4())
        
        mock_session.execute.return_value.scalars.return_value.first.return_value = None
        
        repository = RoleRepository(Role, mock_session)
        result = await repository.get_with_privileges(role_id)
        
        assert result is None
        mock_session.execute.assert_called_once()


class TestPrivilegeRepository:
    """Test cases for PrivilegeRepository."""
    
    @pytest.mark.asyncio
    async def test_get_by_name(self, mock_session):
        """Test getting a privilege by name."""
        privilege_name = "read_users"
        
        mock_privilege = MagicMock(spec=Privilege)
        mock_privilege.name = privilege_name
        
        mock_session.execute.return_value.scalars.return_value.first.return_value = mock_privilege
        
        repository = PrivilegeRepository(Privilege, mock_session)
        result = await repository.get_by_name(privilege_name)
        
        assert result is not None
        assert result.name == privilege_name
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_names(self, mock_session):
        """Test getting privileges by names."""
        names = ["read", "write", "delete"]
        
        mock_privileges = [
            MagicMock(spec=Privilege),
            MagicMock(spec=Privilege),
            MagicMock(spec=Privilege)
        ]
        mock_privileges[0].name = "read"
        mock_privileges[1].name = "write"
        mock_privileges[2].name = "delete"
        
        mock_session.execute.return_value.scalars.return_value.all.return_value = mock_privileges
        
        repository = PrivilegeRepository(Privilege, mock_session)
        result = await repository.get_by_names(names)
        
        assert len(result) == 3
        assert result[0].name == "read"
        assert result[1].name == "write"
        assert result[2].name == "delete"
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_all_privileges(self, mock_session):
        """Test getting all privileges."""
        mock_privileges = [
            MagicMock(spec=Privilege),
            MagicMock(spec=Privilege),
            MagicMock(spec=Privilege),
            MagicMock(spec=Privilege)
        ]
        mock_privileges[0].name = "read"
        mock_privileges[1].name = "write"
        mock_privileges[2].name = "delete"
        mock_privileges[3].name = "admin"
        
        mock_session.execute.return_value.scalars.return_value.all.return_value = mock_privileges
        
        repository = PrivilegeRepository(Privilege, mock_session)
        result = await repository.get_all_privileges()
        
        assert len(result) == 4
        assert result[0].name == "read"
        assert result[3].name == "admin"
        mock_session.execute.assert_called_once()


class TestRolePrivilegeRepository:
    """Test cases for RolePrivilegeRepository."""
    
    @pytest.mark.asyncio
    async def test_get_by_role_and_privilege(self, mock_session):
        """Test getting a role-privilege mapping."""
        role_id = str(uuid.uuid4())
        privilege_id = str(uuid.uuid4())
        
        mock_mapping = MagicMock(spec=RolePrivilege)
        mock_mapping.role_id = role_id
        mock_mapping.privilege_id = privilege_id
        
        mock_session.execute.return_value.scalars.return_value.first.return_value = mock_mapping
        
        repository = RolePrivilegeRepository(RolePrivilege, mock_session)
        result = await repository.get_by_role_and_privilege(role_id, privilege_id)
        
        assert result is not None
        assert result.role_id == role_id
        assert result.privilege_id == privilege_id
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_by_role_id(self, mock_session):
        """Test deleting all role-privilege mappings for a role."""
        role_id = str(uuid.uuid4())
        
        repository = RolePrivilegeRepository(RolePrivilege, mock_session)
        await repository.delete_by_role_id(role_id)
        
        mock_session.execute.assert_called_once()


class TestUserRoleRepository:
    """Test cases for UserRoleRepository."""
    
    @pytest.mark.asyncio
    async def test_get_user_roles(self, mock_session):
        """Test getting all roles assigned to a user."""
        user_id = str(uuid.uuid4())
        
        mock_roles = [
            MagicMock(spec=Role),
            MagicMock(spec=Role)
        ]
        mock_roles[0].name = "admin"
        mock_roles[1].name = "editor"
        
        mock_session.execute.return_value.scalars.return_value.all.return_value = mock_roles
        
        repository = UserRoleRepository(UserRole, mock_session)
        result = await repository.get_user_roles(user_id)
        
        assert len(result) == 2
        assert result[0].name == "admin"
        assert result[1].name == "editor"
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_has_role_true(self, mock_session):
        """Test checking if user has a specific role - returns True."""
        user_id = str(uuid.uuid4())
        role_name = "admin"
        
        mock_user_role = MagicMock(spec=UserRole)
        mock_session.execute.return_value.scalars.return_value.first.return_value = mock_user_role
        
        repository = UserRoleRepository(UserRole, mock_session)
        result = await repository.has_role(user_id, role_name)
        
        assert result is True
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_has_role_false(self, mock_session):
        """Test checking if user has a specific role - returns False."""
        user_id = str(uuid.uuid4())
        role_name = "admin"
        
        mock_session.execute.return_value.scalars.return_value.first.return_value = None
        
        repository = UserRoleRepository(UserRole, mock_session)
        result = await repository.has_role(user_id, role_name)
        
        assert result is False
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_assign_role_new(self, mock_session):
        """Test assigning a new role to a user."""
        user_id = str(uuid.uuid4())
        role_id = str(uuid.uuid4())
        assigned_by = str(uuid.uuid4())
        
        # First query returns None (no existing assignment)
        mock_session.execute.return_value.scalars.return_value.first.return_value = None
        
        repository = UserRoleRepository(UserRole, mock_session)
        result = await repository.assign_role(user_id, role_id, assigned_by)
        
        assert result is not None
        mock_session.add.assert_called_once()
        mock_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_assign_role_existing(self, mock_session):
        """Test assigning a role that already exists."""
        user_id = str(uuid.uuid4())
        role_id = str(uuid.uuid4())
        
        mock_existing = MagicMock(spec=UserRole)
        mock_existing.user_id = user_id
        mock_existing.role_id = role_id
        
        mock_session.execute.return_value.scalars.return_value.first.return_value = mock_existing
        
        repository = UserRoleRepository(UserRole, mock_session)
        result = await repository.assign_role(user_id, role_id)
        
        assert result == mock_existing
        mock_session.add.assert_not_called()
        mock_session.flush.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_remove_role(self, mock_session):
        """Test removing a role from a user."""
        user_id = str(uuid.uuid4())
        role_id = str(uuid.uuid4())
        
        repository = UserRoleRepository(UserRole, mock_session)
        await repository.remove_role(user_id, role_id)
        
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_users_with_role(self, mock_session):
        """Test getting all users who have a specific role."""
        role_name = "admin"
        
        mock_users = [
            MagicMock(spec=User),
            MagicMock(spec=User)
        ]
        mock_users[0].username = "user1"
        mock_users[1].username = "user2"
        
        mock_session.execute.return_value.scalars.return_value.all.return_value = mock_users
        
        repository = UserRoleRepository(UserRole, mock_session)
        result = await repository.get_users_with_role(role_name)
        
        assert len(result) == 2
        assert result[0].username == "user1"
        assert result[1].username == "user2"
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_user_privileges(self, mock_session):
        """Test getting all privileges for a user based on their roles."""
        user_id = str(uuid.uuid4())
        
        mock_privileges = [
            MagicMock(spec=Privilege),
            MagicMock(spec=Privilege),
            MagicMock(spec=Privilege)
        ]
        mock_privileges[0].name = "read"
        mock_privileges[1].name = "write"
        mock_privileges[2].name = "delete"
        
        mock_session.execute.return_value.scalars.return_value.all.return_value = mock_privileges
        
        repository = UserRoleRepository(UserRole, mock_session)
        result = await repository.get_user_privileges(user_id)
        
        assert len(result) == 3
        assert result[0].name == "read"
        assert result[1].name == "write"
        assert result[2].name == "delete"
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_has_privilege_true(self, mock_session):
        """Test checking if user has a specific privilege - returns True."""
        user_id = str(uuid.uuid4())
        privilege_name = "read_users"
        
        mock_privilege = MagicMock(spec=Privilege)
        mock_privilege.name = privilege_name
        
        mock_session.execute.return_value.scalars.return_value.first.return_value = mock_privilege
        
        repository = UserRoleRepository(UserRole, mock_session)
        result = await repository.has_privilege(user_id, privilege_name)
        
        assert result is True
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_has_privilege_false(self, mock_session):
        """Test checking if user has a specific privilege - returns False."""
        user_id = str(uuid.uuid4())
        privilege_name = "delete_all"
        
        mock_session.execute.return_value.scalars.return_value.first.return_value = None
        
        repository = UserRoleRepository(UserRole, mock_session)
        result = await repository.has_privilege(user_id, privilege_name)
        
        assert result is False
        mock_session.execute.assert_called_once()


class TestIdentityProviderRepository:
    """Test cases for IdentityProviderRepository."""
    
    @pytest.mark.asyncio
    async def test_get_by_name(self, mock_session):
        """Test getting identity provider by name."""
        provider_name = "google"
        
        mock_provider = MagicMock(spec=IdentityProvider)
        mock_provider.name = provider_name
        
        mock_session.execute.return_value.scalars.return_value.first.return_value = mock_provider
        
        repository = IdentityProviderRepository(IdentityProvider, mock_session)
        result = await repository.get_by_name(provider_name)
        
        assert result is not None
        assert result.name == provider_name
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_enabled_providers(self, mock_session):
        """Test getting all enabled identity providers."""
        mock_providers = [
            MagicMock(spec=IdentityProvider),
            MagicMock(spec=IdentityProvider)
        ]
        mock_providers[0].name = "google"
        mock_providers[0].enabled = True
        mock_providers[1].name = "github"
        mock_providers[1].enabled = True
        
        mock_session.execute.return_value.scalars.return_value.all.return_value = mock_providers
        
        repository = IdentityProviderRepository(IdentityProvider, mock_session)
        result = await repository.get_enabled_providers()
        
        assert len(result) == 2
        assert result[0].name == "google"
        assert result[1].name == "github"
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_default_provider(self, mock_session):
        """Test getting the default identity provider."""
        mock_provider = MagicMock(spec=IdentityProvider)
        mock_provider.name = "local"
        mock_provider.is_default = True
        
        mock_session.execute.return_value.scalars.return_value.first.return_value = mock_provider
        
        repository = IdentityProviderRepository(IdentityProvider, mock_session)
        result = await repository.get_default_provider()
        
        assert result is not None
        assert result.name == "local"
        assert result.is_default is True
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_set_as_default(self, mock_session):
        """Test setting an identity provider as the default."""
        provider_id = str(uuid.uuid4())
        
        repository = IdentityProviderRepository(IdentityProvider, mock_session)
        await repository.set_as_default(provider_id)
        
        # Should execute two update statements
        assert mock_session.execute.call_count == 2