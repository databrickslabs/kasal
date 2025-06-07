"""
Unit tests for RolesRouter.

Tests the functionality of the roles router including
role CRUD operations, permissions, and access control.
"""
import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient
from fastapi import HTTPException

from src.api.roles_router import router
from src.schemas.user import RoleCreate, RoleUpdate, Role
from src.models.user import Role as RoleModel


@pytest.fixture
def mock_role_service():
    """Create a mock role service."""
    service = AsyncMock()
    
    # Create mock role objects
    mock_role = MagicMock(spec=RoleModel)
    mock_role.id = uuid.uuid4()
    mock_role.name = "admin"
    mock_role.description = "Administrator role"
    mock_role.permissions = ["read", "write", "delete"]
    mock_role.is_active = True
    
    # Setup service method returns
    service.get.return_value = mock_role
    service.list.return_value = [mock_role]
    service.create.return_value = mock_role
    service.update.return_value = mock_role
    service.delete.return_value = True
    service.get_by_name.return_value = mock_role
    
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
def role_create_data():
    """Create test data for role creation."""
    return RoleCreate(
        name="test_role",
        description="Test role for testing",
        permissions=["read", "write"]
    )


@pytest.fixture
def role_update_data():
    """Create test data for role updates."""
    return RoleUpdate(
        description="Updated test role",
        permissions=["read", "write", "delete"]
    )


class TestRolesRouter:
    """Test cases for RolesRouter."""
    
    @pytest.mark.asyncio
    async def test_create_role_success(self, mock_role_service, mock_current_user, role_create_data):
        """Test successful role creation."""
        with patch("src.api.roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.roles_router.RoleService", return_value=mock_role_service):
            
            from src.api.roles_router import create_role
            
            result = await create_role(role_create_data, current_user=mock_current_user)
            
            assert result is not None
            assert result.name == "admin"
            mock_role_service.create.assert_called_once_with(role_create_data)
    
    @pytest.mark.asyncio
    async def test_create_role_unauthorized(self, mock_role_service, role_create_data):
        """Test role creation without proper permissions."""
        unauthorized_user = MagicMock()
        unauthorized_user.id = uuid.uuid4()
        unauthorized_user.is_superuser = False
        unauthorized_user.roles = ["user"]
        
        with patch("src.api.roles_router.get_current_user", return_value=unauthorized_user), \
             patch("src.api.roles_router.RoleService", return_value=mock_role_service):
            
            from src.api.roles_router import create_role
            
            with pytest.raises(HTTPException) as exc_info:
                await create_role(role_create_data, current_user=unauthorized_user)
            
            assert exc_info.value.status_code == 403
    
    @pytest.mark.asyncio
    async def test_get_role_success(self, mock_role_service, mock_current_user):
        """Test successful role retrieval."""
        role_id = uuid.uuid4()
        
        with patch("src.api.roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.roles_router.RoleService", return_value=mock_role_service):
            
            from src.api.roles_router import get_role
            
            result = await get_role(role_id, current_user=mock_current_user)
            
            assert result is not None
            assert result.name == "admin"
            mock_role_service.get.assert_called_once_with(role_id)
    
    @pytest.mark.asyncio
    async def test_get_role_not_found(self, mock_role_service, mock_current_user):
        """Test getting a non-existent role."""
        role_id = uuid.uuid4()
        mock_role_service.get.return_value = None
        
        with patch("src.api.roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.roles_router.RoleService", return_value=mock_role_service):
            
            from src.api.roles_router import get_role
            
            with pytest.raises(HTTPException) as exc_info:
                await get_role(role_id, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 404
    
    @pytest.mark.asyncio
    async def test_list_roles_success(self, mock_role_service, mock_current_user):
        """Test successful role listing."""
        with patch("src.api.roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.roles_router.RoleService", return_value=mock_role_service):
            
            from src.api.roles_router import list_roles
            
            result = await list_roles(current_user=mock_current_user)
            
            assert len(result) == 1
            assert result[0].name == "admin"
            mock_role_service.list.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_role_success(self, mock_role_service, mock_current_user, role_update_data):
        """Test successful role update."""
        role_id = uuid.uuid4()
        
        with patch("src.api.roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.roles_router.RoleService", return_value=mock_role_service):
            
            from src.api.roles_router import update_role
            
            result = await update_role(role_id, role_update_data, current_user=mock_current_user)
            
            assert result is not None
            mock_role_service.update.assert_called_once_with(role_id, role_update_data)
    
    @pytest.mark.asyncio
    async def test_update_role_not_found(self, mock_role_service, mock_current_user, role_update_data):
        """Test updating a non-existent role."""
        role_id = uuid.uuid4()
        mock_role_service.update.return_value = None
        
        with patch("src.api.roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.roles_router.RoleService", return_value=mock_role_service):
            
            from src.api.roles_router import update_role
            
            with pytest.raises(HTTPException) as exc_info:
                await update_role(role_id, role_update_data, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 404
    
    @pytest.mark.asyncio
    async def test_delete_role_success(self, mock_role_service, mock_current_user):
        """Test successful role deletion."""
        role_id = uuid.uuid4()
        
        with patch("src.api.roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.roles_router.RoleService", return_value=mock_role_service):
            
            from src.api.roles_router import delete_role
            
            result = await delete_role(role_id, current_user=mock_current_user)
            
            assert result["message"] == "Role deleted successfully"
            mock_role_service.delete.assert_called_once_with(role_id)
    
    @pytest.mark.asyncio
    async def test_delete_role_not_found(self, mock_role_service, mock_current_user):
        """Test deleting a non-existent role."""
        role_id = uuid.uuid4()
        mock_role_service.delete.return_value = False
        
        with patch("src.api.roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.roles_router.RoleService", return_value=mock_role_service):
            
            from src.api.roles_router import delete_role
            
            with pytest.raises(HTTPException) as exc_info:
                await delete_role(role_id, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 404
    
    @pytest.mark.asyncio
    async def test_get_role_permissions(self, mock_role_service, mock_current_user):
        """Test getting role permissions."""
        role_id = uuid.uuid4()
        
        with patch("src.api.roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.roles_router.RoleService", return_value=mock_role_service):
            
            from src.api.roles_router import get_role_permissions
            
            result = await get_role_permissions(role_id, current_user=mock_current_user)
            
            assert "permissions" in result
            assert len(result["permissions"]) == 3
            mock_role_service.get.assert_called_once_with(role_id)
    
    @pytest.mark.asyncio
    async def test_assign_role_permissions(self, mock_role_service, mock_current_user):
        """Test assigning permissions to a role."""
        role_id = uuid.uuid4()
        permissions = ["read", "write", "execute"]
        
        with patch("src.api.roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.roles_router.RoleService", return_value=mock_role_service):
            
            from src.api.roles_router import assign_role_permissions
            
            result = await assign_role_permissions(role_id, {"permissions": permissions}, current_user=mock_current_user)
            
            assert result["message"] == "Permissions assigned successfully"
            mock_role_service.assign_permissions.assert_called_once_with(role_id, permissions)
    
    @pytest.mark.asyncio
    async def test_role_validation(self, mock_role_service, mock_current_user):
        """Test role data validation."""
        invalid_role_data = RoleCreate(
            name="",  # Invalid empty name
            description="Test role",
            permissions=[]
        )
        
        with patch("src.api.roles_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.roles_router.RoleService", return_value=mock_role_service):
            
            mock_role_service.create.side_effect = ValueError("Role name cannot be empty")
            
            from src.api.roles_router import create_role
            
            with pytest.raises(HTTPException) as exc_info:
                await create_role(invalid_role_data, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 422
    
    @pytest.mark.asyncio
    async def test_role_hierarchy_check(self, mock_role_service, mock_current_user):
        """Test role hierarchy validation."""
        role_id = uuid.uuid4()
        
        # Test user trying to modify higher role
        lower_user = MagicMock()
        lower_user.id = uuid.uuid4()
        lower_user.is_superuser = False
        lower_user.roles = ["editor"]
        
        with patch("src.api.roles_router.get_current_user", return_value=lower_user), \
             patch("src.api.roles_router.RoleService", return_value=mock_role_service):
            
            from src.api.roles_router import delete_role
            
            with pytest.raises(HTTPException) as exc_info:
                await delete_role(role_id, current_user=lower_user)
            
            assert exc_info.value.status_code == 403