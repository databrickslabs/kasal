"""
Unit tests for UserRolesRouter.

Tests the functionality of the user roles router including
user-role assignment and management operations.
"""
import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException

from src.schemas.user import UserRoleAssignment, UserRoleUpdate


@pytest.fixture
def mock_user_role_service():
    """Create a mock user role service."""
    service = AsyncMock()
    
    # Create mock user role objects
    mock_user_role = MagicMock()
    mock_user_role.id = uuid.uuid4()
    mock_user_role.user_id = uuid.uuid4()
    mock_user_role.role_id = uuid.uuid4()
    mock_user_role.assigned_by = uuid.uuid4()
    mock_user_role.assigned_at = "2024-01-01T00:00:00Z"
    mock_user_role.is_active = True
    
    # Setup service method returns
    service.get.return_value = mock_user_role
    service.list.return_value = [mock_user_role]
    service.assign_role.return_value = mock_user_role
    service.remove_role.return_value = True
    service.update.return_value = mock_user_role
    service.get_user_roles.return_value = [mock_user_role]
    service.get_role_users.return_value = [mock_user_role]
    
    return service


@pytest.fixture
def mock_current_user():
    """Create a mock current user."""
    user = MagicMock()
    user.id = uuid.uuid4()
    user.username = "testuser"
    user.is_superuser = True
    user.roles = ["admin"]
    return user


@pytest.fixture
def user_role_assignment_data():
    """Create test data for user role assignment."""
    return UserRoleAssignment(
        user_id=uuid.uuid4(),
        role_id=uuid.uuid4(),
        expires_at=None
    )


@pytest.fixture
def user_role_update_data():
    """Create test data for user role updates."""
    return UserRoleUpdate(
        is_active=False,
        expires_at="2024-12-31T23:59:59Z"
    )


class TestUserRolesRouter:
    """Test cases for UserRolesRouter."""
    
    @pytest.mark.asyncio
    async def test_assign_role_to_user_success(self, mock_user_role_service, mock_current_user, user_role_assignment_data):
        """Test successful role assignment to user."""
        with patch("src.api.user_roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.user_roles_router.UserRoleService", return_value=mock_user_role_service):
            
            from src.api.user_roles_router import assign_role_to_user
            
            result = await assign_role_to_user(user_role_assignment_data, current_user=mock_current_user)
            
            assert result is not None
            assert result.user_id == user_role_assignment_data.user_id
            mock_user_role_service.assign_role.assert_called_once_with(user_role_assignment_data, mock_current_user.id)
    
    @pytest.mark.asyncio
    async def test_assign_role_unauthorized(self, mock_user_role_service, user_role_assignment_data):
        """Test role assignment without proper permissions."""
        unauthorized_user = MagicMock()
        unauthorized_user.id = uuid.uuid4()
        unauthorized_user.is_superuser = False
        unauthorized_user.roles = ["user"]
        
        with patch("src.api.user_roles_router.get_current_user", return_value=unauthorized_user), \
             patch("src.api.user_roles_router.UserRoleService", return_value=mock_user_role_service):
            
            from src.api.user_roles_router import assign_role_to_user
            
            with pytest.raises(HTTPException) as exc_info:
                await assign_role_to_user(user_role_assignment_data, current_user=unauthorized_user)
            
            assert exc_info.value.status_code == 403
    
    @pytest.mark.asyncio
    async def test_remove_role_from_user_success(self, mock_user_role_service, mock_current_user):
        """Test successful role removal from user."""
        user_id = uuid.uuid4()
        role_id = uuid.uuid4()
        
        with patch("src.api.user_roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.user_roles_router.UserRoleService", return_value=mock_user_role_service):
            
            from src.api.user_roles_router import remove_role_from_user
            
            result = await remove_role_from_user(user_id, role_id, current_user=mock_current_user)
            
            assert result["message"] == "Role removed from user successfully"
            mock_user_role_service.remove_role.assert_called_once_with(user_id, role_id)
    
    @pytest.mark.asyncio
    async def test_remove_role_not_found(self, mock_user_role_service, mock_current_user):
        """Test removing non-existent role assignment."""
        user_id = uuid.uuid4()
        role_id = uuid.uuid4()
        mock_user_role_service.remove_role.return_value = False
        
        with patch("src.api.user_roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.user_roles_router.UserRoleService", return_value=mock_user_role_service):
            
            from src.api.user_roles_router import remove_role_from_user
            
            with pytest.raises(HTTPException) as exc_info:
                await remove_role_from_user(user_id, role_id, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 404
    
    @pytest.mark.asyncio
    async def test_get_user_roles_success(self, mock_user_role_service, mock_current_user):
        """Test successful retrieval of user roles."""
        user_id = uuid.uuid4()
        
        with patch("src.api.user_roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.user_roles_router.UserRoleService", return_value=mock_user_role_service):
            
            from src.api.user_roles_router import get_user_roles
            
            result = await get_user_roles(user_id, current_user=mock_current_user)
            
            assert len(result) == 1
            assert result[0].user_id == mock_user_role_service.get_user_roles.return_value[0].user_id
            mock_user_role_service.get_user_roles.assert_called_once_with(user_id)
    
    @pytest.mark.asyncio
    async def test_get_role_users_success(self, mock_user_role_service, mock_current_user):
        """Test successful retrieval of role users."""
        role_id = uuid.uuid4()
        
        with patch("src.api.user_roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.user_roles_router.UserRoleService", return_value=mock_user_role_service):
            
            from src.api.user_roles_router import get_role_users
            
            result = await get_role_users(role_id, current_user=mock_current_user)
            
            assert len(result) == 1
            assert result[0].role_id == mock_user_role_service.get_role_users.return_value[0].role_id
            mock_user_role_service.get_role_users.assert_called_once_with(role_id)
    
    @pytest.mark.asyncio
    async def test_update_user_role_success(self, mock_user_role_service, mock_current_user, user_role_update_data):
        """Test successful user role update."""
        assignment_id = uuid.uuid4()
        
        with patch("src.api.user_roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.user_roles_router.UserRoleService", return_value=mock_user_role_service):
            
            from src.api.user_roles_router import update_user_role
            
            result = await update_user_role(assignment_id, user_role_update_data, current_user=mock_current_user)
            
            assert result is not None
            mock_user_role_service.update.assert_called_once_with(assignment_id, user_role_update_data)
    
    @pytest.mark.asyncio
    async def test_update_user_role_not_found(self, mock_user_role_service, mock_current_user, user_role_update_data):
        """Test updating non-existent user role assignment."""
        assignment_id = uuid.uuid4()
        mock_user_role_service.update.return_value = None
        
        with patch("src.api.user_roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.user_roles_router.UserRoleService", return_value=mock_user_role_service):
            
            from src.api.user_roles_router import update_user_role
            
            with pytest.raises(HTTPException) as exc_info:
                await update_user_role(assignment_id, user_role_update_data, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 404
    
    @pytest.mark.asyncio
    async def test_bulk_assign_roles(self, mock_user_role_service, mock_current_user):
        """Test bulk role assignment to user."""
        user_id = uuid.uuid4()
        role_ids = [str(uuid.uuid4()), str(uuid.uuid4())]
        
        with patch("src.api.user_roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.user_roles_router.UserRoleService", return_value=mock_user_role_service):
            
            from src.api.user_roles_router import bulk_assign_roles
            
            result = await bulk_assign_roles(
                {"user_id": str(user_id), "role_ids": role_ids}, 
                current_user=mock_current_user
            )
            
            assert result["message"] == "Roles assigned successfully"
            assert result["assigned_count"] == 2
            mock_user_role_service.bulk_assign_roles.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_bulk_remove_roles(self, mock_user_role_service, mock_current_user):
        """Test bulk role removal from user."""
        user_id = uuid.uuid4()
        role_ids = [str(uuid.uuid4()), str(uuid.uuid4())]
        
        with patch("src.api.user_roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.user_roles_router.UserRoleService", return_value=mock_user_role_service):
            
            from src.api.user_roles_router import bulk_remove_roles
            
            result = await bulk_remove_roles(
                {"user_id": str(user_id), "role_ids": role_ids}, 
                current_user=mock_current_user
            )
            
            assert result["message"] == "Roles removed successfully"
            assert result["removed_count"] == 2
            mock_user_role_service.bulk_remove_roles.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_user_effective_permissions(self, mock_user_role_service, mock_current_user):
        """Test getting user's effective permissions through roles."""
        user_id = uuid.uuid4()
        mock_permissions = ["read", "write", "execute"]
        mock_user_role_service.get_effective_permissions.return_value = mock_permissions
        
        with patch("src.api.user_roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.user_roles_router.UserRoleService", return_value=mock_user_role_service):
            
            from src.api.user_roles_router import get_user_effective_permissions
            
            result = await get_user_effective_permissions(user_id, current_user=mock_current_user)
            
            assert "permissions" in result
            assert len(result["permissions"]) == 3
            mock_user_role_service.get_effective_permissions.assert_called_once_with(user_id)
    
    @pytest.mark.asyncio
    async def test_check_user_role_access(self, mock_user_role_service, mock_current_user):
        """Test checking if user has specific role."""
        user_id = uuid.uuid4()
        role_name = "admin"
        
        mock_user_role_service.has_role.return_value = True
        
        with patch("src.api.user_roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.user_roles_router.UserRoleService", return_value=mock_user_role_service):
            
            from src.api.user_roles_router import check_user_role_access
            
            result = await check_user_role_access(
                user_id, 
                {"role_name": role_name}, 
                current_user=mock_current_user
            )
            
            assert result["has_role"] is True
            mock_user_role_service.has_role.assert_called_once_with(user_id, role_name)
    
    @pytest.mark.asyncio
    async def test_get_role_assignment_history(self, mock_user_role_service, mock_current_user):
        """Test getting role assignment history for user."""
        user_id = uuid.uuid4()
        mock_history = [
            {"action": "assigned", "role": "admin", "timestamp": "2024-01-01T00:00:00Z"},
            {"action": "removed", "role": "editor", "timestamp": "2024-01-02T00:00:00Z"}
        ]
        mock_user_role_service.get_assignment_history.return_value = mock_history
        
        with patch("src.api.user_roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.user_roles_router.UserRoleService", return_value=mock_user_role_service):
            
            from src.api.user_roles_router import get_role_assignment_history
            
            result = await get_role_assignment_history(user_id, current_user=mock_current_user)
            
            assert len(result) == 2
            assert result[0]["action"] == "assigned"
            mock_user_role_service.get_assignment_history.assert_called_once_with(user_id)
    
    @pytest.mark.asyncio
    async def test_expire_user_role(self, mock_user_role_service, mock_current_user):
        """Test expiring user role assignment."""
        assignment_id = uuid.uuid4()
        
        with patch("src.api.user_roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.user_roles_router.UserRoleService", return_value=mock_user_role_service):
            
            from src.api.user_roles_router import expire_user_role
            
            result = await expire_user_role(assignment_id, current_user=mock_current_user)
            
            assert result["message"] == "Role assignment expired successfully"
            mock_user_role_service.expire_assignment.assert_called_once_with(assignment_id)
    
    @pytest.mark.asyncio
    async def test_user_role_validation(self, mock_user_role_service, mock_current_user):
        """Test user role assignment validation."""
        invalid_assignment_data = UserRoleAssignment(
            user_id=None,  # Invalid None user_id
            role_id=uuid.uuid4(),
            expires_at=None
        )
        
        with patch("src.api.user_roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.user_roles_router.UserRoleService", return_value=mock_user_role_service):
            
            mock_user_role_service.assign_role.side_effect = ValueError("Invalid user role assignment")
            
            from src.api.user_roles_router import assign_role_to_user
            
            with pytest.raises(HTTPException) as exc_info:
                await assign_role_to_user(invalid_assignment_data, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 422
    
    @pytest.mark.asyncio
    async def test_self_role_modification_prevention(self, mock_user_role_service):
        """Test preventing users from modifying their own roles."""
        current_user = MagicMock()
        current_user.id = uuid.uuid4()
        current_user.is_superuser = False
        current_user.roles = ["admin"]
        
        # User trying to modify their own roles
        user_role_assignment_data = UserRoleAssignment(
            user_id=current_user.id,  # Same as current user
            role_id=uuid.uuid4(),
            expires_at=None
        )
        
        with patch("src.api.user_roles_router.get_current_user", return_value=current_user), \
             patch("src.api.user_roles_router.UserRoleService", return_value=mock_user_role_service):
            
            from src.api.user_roles_router import assign_role_to_user
            
            with pytest.raises(HTTPException) as exc_info:
                await assign_role_to_user(user_role_assignment_data, current_user=current_user)
            
            assert exc_info.value.status_code == 403
    
    @pytest.mark.asyncio
    async def test_get_role_statistics(self, mock_user_role_service, mock_current_user):
        """Test getting role assignment statistics."""
        mock_stats = {
            "total_assignments": 150,
            "active_assignments": 120,
            "expired_assignments": 30,
            "roles_by_user_count": {"admin": 5, "editor": 25, "viewer": 90}
        }
        mock_user_role_service.get_role_statistics.return_value = mock_stats
        
        with patch("src.api.user_roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.user_roles_router.UserRoleService", return_value=mock_user_role_service):
            
            from src.api.user_roles_router import get_role_statistics
            
            result = await get_role_statistics(current_user=mock_current_user)
            
            assert result["total_assignments"] == 150
            assert result["active_assignments"] == 120
            assert result["roles_by_user_count"]["admin"] == 5
            mock_user_role_service.get_role_statistics.assert_called_once()