"""
Unit tests for UsersRouter.

Tests the functionality of the users router including
user CRUD operations, role assignments, and access control.
"""
import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException

from src.schemas.user import UserCreate, UserUpdate, UserResponse
from src.models.user import User


@pytest.fixture
def mock_user_service():
    """Create a mock user service."""
    service = AsyncMock()
    
    # Create mock user objects
    mock_user = MagicMock(spec=User)
    mock_user.id = uuid.uuid4()
    mock_user.username = "testuser"
    mock_user.email = "test@example.com"
    mock_user.full_name = "Test User"
    mock_user.is_active = True
    mock_user.is_superuser = False
    mock_user.roles = ["user"]
    mock_user.tenant_id = uuid.uuid4()
    
    # Setup service method returns
    service.get.return_value = mock_user
    service.list.return_value = [mock_user]
    service.create.return_value = mock_user
    service.update.return_value = mock_user
    service.delete.return_value = True
    service.get_by_username.return_value = mock_user
    service.get_by_email.return_value = mock_user
    
    return service


@pytest.fixture
def mock_current_user():
    """Create a mock current user."""
    user = MagicMock()
    user.id = uuid.uuid4()
    user.username = "admin"
    user.is_superuser = True
    user.tenant_id = uuid.uuid4()
    return user


@pytest.fixture
def user_create_data():
    """Create test data for user creation."""
    return UserCreate(
        username="newuser",
        email="newuser@example.com",
        password="SecurePassword123!",
        full_name="New User"
    )


@pytest.fixture
def user_update_data():
    """Create test data for user updates."""
    return UserUpdate(
        full_name="Updated User",
        email="updated@example.com",
        is_active=False
    )


class TestUsersRouter:
    """Test cases for UsersRouter."""
    
    @pytest.mark.asyncio
    async def test_create_user_success(self, mock_user_service, mock_current_user, user_create_data):
        """Test successful user creation."""
        with patch("src.api.users_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.users_router.UserService", return_value=mock_user_service):
            
            from src.api.users_router import create_user
            
            result = await create_user(user_create_data, current_user=mock_current_user)
            
            assert result is not None
            assert result.username == "testuser"
            mock_user_service.create.assert_called_once_with(user_create_data)
    
    @pytest.mark.asyncio
    async def test_create_user_unauthorized(self, mock_user_service, user_create_data):
        """Test user creation without proper permissions."""
        unauthorized_user = MagicMock()
        unauthorized_user.id = uuid.uuid4()
        unauthorized_user.is_superuser = False
        unauthorized_user.roles = ["user"]
        
        with patch("src.api.users_router.get_current_user", return_value=unauthorized_user), \
             patch("src.api.users_router.UserService", return_value=mock_user_service):
            
            from src.api.users_router import create_user
            
            with pytest.raises(HTTPException) as exc_info:
                await create_user(user_create_data, current_user=unauthorized_user)
            
            assert exc_info.value.status_code == 403
    
    @pytest.mark.asyncio
    async def test_get_user_success(self, mock_user_service, mock_current_user):
        """Test successful user retrieval."""
        user_id = uuid.uuid4()
        
        with patch("src.api.users_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.users_router.UserService", return_value=mock_user_service):
            
            from src.api.users_router import get_user
            
            result = await get_user(user_id, current_user=mock_current_user)
            
            assert result is not None
            assert result.username == "testuser"
            mock_user_service.get.assert_called_once_with(user_id)
    
    @pytest.mark.asyncio
    async def test_get_user_not_found(self, mock_user_service, mock_current_user):
        """Test getting a non-existent user."""
        user_id = uuid.uuid4()
        mock_user_service.get.return_value = None
        
        with patch("src.api.users_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.users_router.UserService", return_value=mock_user_service):
            
            from src.api.users_router import get_user
            
            with pytest.raises(HTTPException) as exc_info:
                await get_user(user_id, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 404
    
    @pytest.mark.asyncio
    async def test_list_users_success(self, mock_user_service, mock_current_user):
        """Test successful user listing."""
        with patch("src.api.users_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.users_router.UserService", return_value=mock_user_service):
            
            from src.api.users_router import list_users
            
            result = await list_users(current_user=mock_current_user)
            
            assert len(result) == 1
            assert result[0].username == "testuser"
            mock_user_service.list.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_user_success(self, mock_user_service, mock_current_user, user_update_data):
        """Test successful user update."""
        user_id = uuid.uuid4()
        
        with patch("src.api.users_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.users_router.UserService", return_value=mock_user_service):
            
            from src.api.users_router import update_user
            
            result = await update_user(user_id, user_update_data, current_user=mock_current_user)
            
            assert result is not None
            mock_user_service.update.assert_called_once_with(user_id, user_update_data)
    
    @pytest.mark.asyncio
    async def test_update_user_not_found(self, mock_user_service, mock_current_user, user_update_data):
        """Test updating a non-existent user."""
        user_id = uuid.uuid4()
        mock_user_service.update.return_value = None
        
        with patch("src.api.users_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.users_router.UserService", return_value=mock_user_service):
            
            from src.api.users_router import update_user
            
            with pytest.raises(HTTPException) as exc_info:
                await update_user(user_id, user_update_data, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 404
    
    @pytest.mark.asyncio
    async def test_delete_user_success(self, mock_user_service, mock_current_user):
        """Test successful user deletion."""
        user_id = uuid.uuid4()
        
        with patch("src.api.users_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.users_router.UserService", return_value=mock_user_service):
            
            from src.api.users_router import delete_user
            
            result = await delete_user(user_id, current_user=mock_current_user)
            
            assert result["message"] == "User deleted successfully"
            mock_user_service.delete.assert_called_once_with(user_id)
    
    @pytest.mark.asyncio
    async def test_delete_user_not_found(self, mock_user_service, mock_current_user):
        """Test deleting a non-existent user."""
        user_id = uuid.uuid4()
        mock_user_service.delete.return_value = False
        
        with patch("src.api.users_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.users_router.UserService", return_value=mock_user_service):
            
            from src.api.users_router import delete_user
            
            with pytest.raises(HTTPException) as exc_info:
                await delete_user(user_id, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 404
    
    @pytest.mark.asyncio
    async def test_get_current_user_profile(self, mock_user_service, mock_current_user):
        """Test getting current user's profile."""
        with patch("src.api.users_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.users_router.UserService", return_value=mock_user_service):
            
            from src.api.users_router import get_current_user_profile
            
            result = await get_current_user_profile(current_user=mock_current_user)
            
            assert result is not None
            assert result.username == "testuser"
    
    @pytest.mark.asyncio
    async def test_update_current_user_profile(self, mock_user_service, mock_current_user):
        """Test updating current user's profile."""
        profile_data = UserUpdate(
            full_name="Updated Name",
            email="updated@example.com"
        )
        
        with patch("src.api.users_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.users_router.UserService", return_value=mock_user_service):
            
            from src.api.users_router import update_current_user_profile
            
            result = await update_current_user_profile(profile_data, current_user=mock_current_user)
            
            assert result is not None
            mock_user_service.update.assert_called_once_with(mock_current_user.id, profile_data)
    
    @pytest.mark.asyncio
    async def test_assign_user_roles(self, mock_user_service, mock_current_user):
        """Test assigning roles to a user."""
        user_id = uuid.uuid4()
        roles = ["admin", "editor"]
        
        with patch("src.api.users_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.users_router.UserService", return_value=mock_user_service):
            
            from src.api.users_router import assign_user_roles
            
            result = await assign_user_roles(user_id, {"roles": roles}, current_user=mock_current_user)
            
            assert result["message"] == "Roles assigned successfully"
            mock_user_service.assign_roles.assert_called_once_with(user_id, roles)
    
    @pytest.mark.asyncio
    async def test_remove_user_roles(self, mock_user_service, mock_current_user):
        """Test removing roles from a user."""
        user_id = uuid.uuid4()
        roles = ["editor"]
        
        with patch("src.api.users_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.users_router.UserService", return_value=mock_user_service):
            
            from src.api.users_router import remove_user_roles
            
            result = await remove_user_roles(user_id, {"roles": roles}, current_user=mock_current_user)
            
            assert result["message"] == "Roles removed successfully"
            mock_user_service.remove_roles.assert_called_once_with(user_id, roles)
    
    @pytest.mark.asyncio
    async def test_get_user_permissions(self, mock_user_service, mock_current_user):
        """Test getting user permissions."""
        user_id = uuid.uuid4()
        mock_permissions = ["read", "write", "execute"]
        mock_user_service.get_permissions.return_value = mock_permissions
        
        with patch("src.api.users_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.users_router.UserService", return_value=mock_user_service):
            
            from src.api.users_router import get_user_permissions
            
            result = await get_user_permissions(user_id, current_user=mock_current_user)
            
            assert "permissions" in result
            assert len(result["permissions"]) == 3
            mock_user_service.get_permissions.assert_called_once_with(user_id)
    
    @pytest.mark.asyncio
    async def test_change_password(self, mock_user_service, mock_current_user):
        """Test changing user password."""
        password_data = {
            "current_password": "OldPassword123!",
            "new_password": "NewPassword123!"
        }
        
        with patch("src.api.users_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.users_router.UserService", return_value=mock_user_service):
            
            mock_user_service.change_password.return_value = True
            
            from src.api.users_router import change_password
            
            result = await change_password(password_data, current_user=mock_current_user)
            
            assert result["message"] == "Password changed successfully"
            mock_user_service.change_password.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_change_password_invalid_current(self, mock_user_service, mock_current_user):
        """Test changing password with invalid current password."""
        password_data = {
            "current_password": "WrongPassword",
            "new_password": "NewPassword123!"
        }
        
        with patch("src.api.users_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.users_router.UserService", return_value=mock_user_service):
            
            mock_user_service.change_password.return_value = False
            
            from src.api.users_router import change_password
            
            with pytest.raises(HTTPException) as exc_info:
                await change_password(password_data, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 400
    
    @pytest.mark.asyncio
    async def test_user_self_access_only(self, mock_user_service):
        """Test that users can only access their own data (unless admin)."""
        regular_user = MagicMock()
        regular_user.id = uuid.uuid4()
        regular_user.is_superuser = False
        regular_user.roles = ["user"]
        
        other_user_id = uuid.uuid4()  # Different user
        
        with patch("src.api.users_router.get_current_user", return_value=regular_user), \
             patch("src.api.users_router.UserService", return_value=mock_user_service):
            
            from src.api.users_router import get_user
            
            with pytest.raises(HTTPException) as exc_info:
                await get_user(other_user_id, current_user=regular_user)
            
            assert exc_info.value.status_code == 403
    
    @pytest.mark.asyncio
    async def test_deactivate_user(self, mock_user_service, mock_current_user):
        """Test user deactivation."""
        user_id = uuid.uuid4()
        
        with patch("src.api.users_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.users_router.UserService", return_value=mock_user_service):
            
            from src.api.users_router import deactivate_user
            
            result = await deactivate_user(user_id, current_user=mock_current_user)
            
            assert result["message"] == "User deactivated successfully"
            mock_user_service.deactivate.assert_called_once_with(user_id)
    
    @pytest.mark.asyncio
    async def test_reactivate_user(self, mock_user_service, mock_current_user):
        """Test user reactivation."""
        user_id = uuid.uuid4()
        
        with patch("src.api.users_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.users_router.UserService", return_value=mock_user_service):
            
            from src.api.users_router import reactivate_user
            
            result = await reactivate_user(user_id, current_user=mock_current_user)
            
            assert result["message"] == "User reactivated successfully"
            mock_user_service.reactivate.assert_called_once_with(user_id)
    
    @pytest.mark.asyncio
    async def test_user_validation(self, mock_user_service, mock_current_user):
        """Test user data validation."""
        invalid_user_data = UserCreate(
            username="",  # Invalid empty username
            email="invalid-email",  # Invalid email format
            password="weak",  # Weak password
            full_name=""
        )
        
        with patch("src.api.users_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.users_router.UserService", return_value=mock_user_service):
            
            mock_user_service.create.side_effect = ValueError("Invalid user data")
            
            from src.api.users_router import create_user
            
            with pytest.raises(HTTPException) as exc_info:
                await create_user(invalid_user_data, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 422