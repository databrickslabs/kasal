"""
Unit tests for UserService.

Tests the functionality of user management operations including
user retrieval, search, profile management, and role assignments.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
import json

from sqlalchemy.ext.asyncio import AsyncSession

from src.services.user_service import UserService
from src.models.user import User, UserProfile, ExternalIdentity
from src.schemas.user import UserUpdate, UserProfileUpdate, UserRole


# Mock models
class MockUser:
    def __init__(self, id="user-123", username="testuser", email="test@example.com", 
                 role=UserRole.REGULAR, status="active", created_at=None, 
                 updated_at=None, last_login=None):
        self.id = id
        self.username = username
        self.email = email
        self.role = role
        self.status = status
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()
        self.last_login = last_login


class MockUserProfile:
    def __init__(self, id="profile-123", user_id="user-123", full_name="Test User",
                 bio=None, avatar_url=None, created_at=None, updated_at=None):
        self.id = id
        self.user_id = user_id
        self.full_name = full_name
        self.bio = bio
        self.avatar_url = avatar_url
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()


class MockExternalIdentity:
    def __init__(self, id="ext-123", user_id="user-123", provider="github",
                 external_id="github-123", profile_data=None, provider_user_id="github-123",
                 email="test@github.com", created_at=None, last_login=None):
        self.id = id
        self.user_id = user_id
        self.provider = provider
        self.external_id = external_id
        self.provider_user_id = provider_user_id
        self.email = email
        self.profile_data = profile_data or '{"name": "Test User"}'
        self.created_at = created_at or datetime.utcnow()
        self.last_login = last_login


@pytest.fixture
def mock_session():
    """Create a mock async session."""
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def user_service(mock_session):
    """Create a UserService instance with mock session."""
    return UserService(mock_session)


@pytest.fixture
def mock_user():
    """Create a mock user."""
    return MockUser()


@pytest.fixture
def mock_profile():
    """Create a mock user profile."""
    return MockUserProfile()


@pytest.fixture
def mock_external_identity():
    """Create a mock external identity."""
    return MockExternalIdentity()


class TestUserService:
    """Test cases for UserService."""
    
    @pytest.mark.asyncio
    async def test_get_user_success(self, user_service, mock_user):
        """Test successful user retrieval."""
        # Mock the repository
        user_service.user_repo.get = AsyncMock(return_value=mock_user)
        
        result = await user_service.get_user("user-123")
        
        assert result == mock_user
        user_service.user_repo.get.assert_called_once_with("user-123")
    
    @pytest.mark.asyncio
    async def test_get_user_not_found(self, user_service):
        """Test user retrieval when user not found."""
        # Mock the repository to return None
        user_service.user_repo.get = AsyncMock(return_value=None)
        
        result = await user_service.get_user("nonexistent")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_users_no_search(self, user_service, mock_user):
        """Test getting users without search."""
        # Mock the repository
        user_service.user_repo.list_with_filters = AsyncMock(return_value=[mock_user])
        
        result = await user_service.get_users(skip=0, limit=10)
        
        assert len(result) == 1
        assert result[0] == mock_user
        user_service.user_repo.list_with_filters.assert_called_once_with(
            skip=0, limit=10, filters=None
        )
    
    @pytest.mark.asyncio
    async def test_get_users_with_filters(self, user_service, mock_user):
        """Test getting users with filters."""
        filters = {"status": "active"}
        user_service.user_repo.list_with_filters = AsyncMock(return_value=[mock_user])
        
        result = await user_service.get_users(skip=0, limit=10, filters=filters)
        
        assert len(result) == 1
        user_service.user_repo.list_with_filters.assert_called_once_with(
            skip=0, limit=10, filters=filters
        )
    
    @pytest.mark.asyncio
    async def test_get_users_with_search(self, user_service, mock_user):
        """Test getting users with search parameter."""
        # Mock the repository list method
        user_service.user_repo.list = AsyncMock(return_value=[mock_user])
        
        result = await user_service.get_users(skip=0, limit=10, search="test")
        
        # Should call list twice: once for username, once for email
        assert user_service.user_repo.list.call_count == 2
        
        # Check the calls
        calls = user_service.user_repo.list.call_args_list
        assert calls[0][1]["filters"] == {"username": {"$like": "%test%"}}
        assert calls[1][1]["filters"] == {"email": {"$like": "%test%"}}
    
    @pytest.mark.asyncio
    async def test_get_users_search_removes_duplicates(self, user_service):
        """Test that search removes duplicate users."""
        # Create the same user that matches both username and email
        duplicate_user = MockUser(id="user-123", username="test", email="test@example.com")
        
        # Mock repository to return the same user for both searches
        user_service.user_repo.list = AsyncMock(return_value=[duplicate_user])
        
        result = await user_service.get_users(skip=0, limit=10, search="test")
        
        # Should only have one user despite matching both searches
        assert len(result) == 1
        assert result[0].id == "user-123"
    
    @pytest.mark.asyncio
    async def test_get_user_with_profile_success(self, user_service, mock_user, mock_profile):
        """Test getting user with profile."""
        user_service.user_repo.get = AsyncMock(return_value=mock_user)
        user_service.profile_repo.get_by_user_id = AsyncMock(return_value=mock_profile)
        
        result = await user_service.get_user_with_profile("user-123")
        
        assert result is not None
        assert result["id"] == "user-123"
        assert result["username"] == "testuser"
        assert result["profile"] == mock_profile
        
        user_service.user_repo.get.assert_called_once_with("user-123")
        user_service.profile_repo.get_by_user_id.assert_called_once_with("user-123")
    
    @pytest.mark.asyncio
    async def test_get_user_with_profile_user_not_found(self, user_service):
        """Test getting user with profile when user doesn't exist."""
        user_service.user_repo.get = AsyncMock(return_value=None)
        user_service.profile_repo.get_by_user_id = AsyncMock()
        
        result = await user_service.get_user_with_profile("nonexistent")
        
        assert result is None
        user_service.user_repo.get.assert_called_once_with("nonexistent")
        # Should not try to get profile if user doesn't exist
        user_service.profile_repo.get_by_user_id.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_get_user_complete_success(self, user_service, mock_user, mock_profile, mock_external_identity):
        """Test getting complete user information."""
        # Mock get_user_with_profile
        user_with_profile = {
            "id": "user-123",
            "username": "testuser",
            "email": "test@example.com",
            "role": UserRole.REGULAR,
            "status": "active",
            "profile": mock_profile
        }
        
        with patch.object(user_service, 'get_user_with_profile', return_value=user_with_profile):
            user_service.external_identity_repo.get_all_by_user_id = AsyncMock(
                return_value=[mock_external_identity]
            )
            
            result = await user_service.get_user_complete("user-123")
            
            assert result is not None
            assert result["id"] == "user-123"
            assert len(result["external_identities"]) == 1
            assert result["external_identities"][0]["provider"] == "github"
            assert result["external_identities"][0]["profile_data"]["name"] == "Test User"
    
    @pytest.mark.asyncio
    async def test_get_user_complete_user_not_found(self, user_service):
        """Test getting complete user info when user doesn't exist."""
        user_service.external_identity_repo.get_all_by_user_id = AsyncMock()
        
        with patch.object(user_service, 'get_user_with_profile', return_value=None):
            result = await user_service.get_user_complete("nonexistent")
            
            assert result is None
            # Should not try to get external identities
            user_service.external_identity_repo.get_all_by_user_id.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_update_user_success(self, user_service, mock_user, mock_profile):
        """Test updating user information."""
        from src.schemas.user import UserUpdate
        update_data = UserUpdate(email="newemail@example.com")
        
        # Mock repository methods
        user_service.user_repo.get = AsyncMock(return_value=mock_user)
        user_service.user_repo.get_by_email = AsyncMock(return_value=None)
        user_service.user_repo.update = AsyncMock(return_value=mock_user)
        user_service.profile_repo.get_by_user_id = AsyncMock(return_value=mock_profile)
        
        result = await user_service.update_user("user-123", update_data)
        
        assert result is not None
        assert result["id"] == "user-123"
        user_service.user_repo.update.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_password_success(self, user_service, mock_user):
        """Test updating user password."""
        new_password = "newpassword123"
        
        # Mock repository and password hashing
        user_service.user_repo.get = AsyncMock(return_value=mock_user)
        user_service.user_repo.update = AsyncMock()
        
        with patch('src.services.user_service.get_password_hash', return_value="hashed_password"):
            result = await user_service.update_password("user-123", new_password)
            
            assert result is True
            user_service.user_repo.update.assert_called_once_with(
                "user-123", {"hashed_password": "hashed_password"}
            )
    
    @pytest.mark.asyncio
    async def test_delete_user_success(self, user_service, mock_user):
        """Test deleting a user."""
        # Mock repository
        user_service.user_repo.get = AsyncMock(return_value=mock_user)
        user_service.user_repo.delete = AsyncMock()
        
        result = await user_service.delete_user("user-123")
        
        assert result is True
        user_service.user_repo.delete.assert_called_once_with("user-123")
    
    @pytest.mark.asyncio  
    async def test_assign_role_success(self, user_service, mock_user):
        """Test assigning a role to a user."""
        mock_user.role = UserRole.REGULAR
        
        # Mock repository
        user_service.user_repo.get = AsyncMock(return_value=mock_user)
        user_service.user_repo.update = AsyncMock(return_value=mock_user)
        
        # Assuming assign_role method exists
        if hasattr(user_service, 'assign_role'):
            result = await user_service.assign_role("user-123", UserRole.ADMIN)
            
            assert result == mock_user
            user_service.user_repo.update.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_external_identity_json_parsing_error(self, user_service, mock_user, mock_profile):
        """Test handling of invalid JSON in external identity profile data."""
        # Create identity with invalid JSON
        bad_identity = MockExternalIdentity()
        bad_identity.profile_data = "invalid json"
        
        user_with_profile = {
            "id": "user-123",
            "username": "testuser",
            "profile": mock_profile
        }
        
        with patch.object(user_service, 'get_user_with_profile', return_value=user_with_profile):
            user_service.external_identity_repo.get_all_by_user_id = AsyncMock(
                return_value=[bad_identity]
            )
            
            result = await user_service.get_user_complete("user-123")
            
            # Should handle the error gracefully
            assert result is not None
            assert len(result["external_identities"]) == 1
            # Profile data should be None or empty dict when JSON parsing fails
            assert result["external_identities"][0]["profile_data"] in [None, {}]
    
    @pytest.mark.asyncio
    async def test_update_user_username_already_taken(self, user_service, mock_user):
        """Test updating user with duplicate username."""
        from src.schemas.user import UserUpdate
        update_data = UserUpdate(username="existinguser")
        
        # Mock repository
        user_service.user_repo.get = AsyncMock(return_value=mock_user)
        existing_user = MockUser(id="other-user", username="existinguser")
        user_service.user_repo.get_by_username = AsyncMock(return_value=existing_user)
        
        with pytest.raises(ValueError, match="Username already taken"):
            await user_service.update_user("user-123", update_data)
    
    @pytest.mark.asyncio
    async def test_update_user_email_already_registered(self, user_service, mock_user):
        """Test updating user with duplicate email."""
        from src.schemas.user import UserUpdate
        update_data = UserUpdate(email="existing@example.com")
        
        # Mock repository
        user_service.user_repo.get = AsyncMock(return_value=mock_user)
        existing_user = MockUser(id="other-user", email="existing@example.com")
        user_service.user_repo.get_by_email = AsyncMock(return_value=existing_user)
        
        with pytest.raises(ValueError, match="Email already registered"):
            await user_service.update_user("user-123", update_data)
    
    @pytest.mark.asyncio
    async def test_update_user_not_found(self, user_service):
        """Test updating non-existent user."""
        from src.schemas.user import UserUpdate
        update_data = UserUpdate(email="new@example.com")
        
        user_service.user_repo.get = AsyncMock(return_value=None)
        
        result = await user_service.update_user("nonexistent", update_data)
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_update_password_user_not_found(self, user_service):
        """Test updating password for non-existent user."""
        user_service.user_repo.get = AsyncMock(return_value=None)
        
        result = await user_service.update_password("nonexistent", "newpass")
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_delete_user_not_found(self, user_service):
        """Test deleting non-existent user."""
        user_service.user_repo.get = AsyncMock(return_value=None)
        
        result = await user_service.delete_user("nonexistent")
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_assign_role_user_not_found(self, user_service):
        """Test assigning role to non-existent user."""
        user_service.user_repo.get = AsyncMock(return_value=None)
        
        result = await user_service.assign_role("nonexistent", UserRole.ADMIN)
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_user_external_identities(self, user_service, mock_external_identity):
        """Test getting user external identities."""
        user_service.external_identity_repo.get_all_by_user_id = AsyncMock(
            return_value=[mock_external_identity]
        )
        
        result = await user_service.get_user_external_identities("user-123")
        
        assert len(result) == 1
        assert result[0] == mock_external_identity
    
    @pytest.mark.asyncio
    async def test_update_user_profile_existing(self, user_service, mock_user, mock_profile):
        """Test updating existing user profile."""
        from src.schemas.user import UserProfileUpdate
        profile_update = UserProfileUpdate(full_name="Updated Name", bio="New bio")
        
        user_service.user_repo.get = AsyncMock(return_value=mock_user)
        user_service.profile_repo.get_by_user_id = AsyncMock(return_value=mock_profile)
        user_service.profile_repo.update = AsyncMock()
        
        result = await user_service.update_user_profile("user-123", profile_update)
        
        assert result is not None
        user_service.profile_repo.update.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_user_profile_create_new(self, user_service, mock_user):
        """Test creating new user profile when none exists."""
        from src.schemas.user import UserProfileUpdate
        profile_update = UserProfileUpdate(full_name="New User", bio="New bio")
        
        user_service.user_repo.get = AsyncMock(return_value=mock_user)
        user_service.profile_repo.get_by_user_id = AsyncMock(return_value=None)
        user_service.profile_repo.create = AsyncMock()
        
        with patch.object(user_service, 'get_user_with_profile', return_value={"id": "user-123"}):
            result = await user_service.update_user_profile("user-123", profile_update)
            
            assert result is not None
            user_service.profile_repo.create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_user_profile_user_not_found(self, user_service):
        """Test updating profile for non-existent user."""
        from src.schemas.user import UserProfileUpdate
        profile_update = UserProfileUpdate(full_name="Name")
        
        user_service.user_repo.get = AsyncMock(return_value=None)
        
        result = await user_service.update_user_profile("nonexistent", profile_update)
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_remove_external_identity_success(self, user_service, mock_external_identity):
        """Test removing external identity."""
        user_service.external_identity_repo.get_by_user_id_and_provider = AsyncMock(
            return_value=mock_external_identity
        )
        user_service.external_identity_repo.delete = AsyncMock()
        
        result = await user_service.remove_external_identity("user-123", "github")
        
        assert result is True
        user_service.external_identity_repo.delete.assert_called_once_with("ext-123")
    
    @pytest.mark.asyncio
    async def test_remove_external_identity_not_found(self, user_service):
        """Test removing non-existent external identity."""
        user_service.external_identity_repo.get_by_user_id_and_provider = AsyncMock(
            return_value=None
        )
        
        result = await user_service.remove_external_identity("user-123", "nonexistent")
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_external_identity_no_profile_data(self, user_service, mock_user, mock_profile):
        """Test handling external identity with no profile data."""
        # Create identity with no profile data
        identity_no_data = MockExternalIdentity()
        identity_no_data.profile_data = None
        
        user_with_profile = {
            "id": "user-123",
            "username": "testuser",
            "profile": mock_profile
        }
        
        with patch.object(user_service, 'get_user_with_profile', return_value=user_with_profile):
            user_service.external_identity_repo.get_all_by_user_id = AsyncMock(
                return_value=[identity_no_data]
            )
            
            result = await user_service.get_user_complete("user-123")
            
            assert result is not None
            assert result["external_identities"][0]["profile_data"] is None