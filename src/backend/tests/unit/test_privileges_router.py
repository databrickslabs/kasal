"""
Unit tests for PrivilegesRouter.

Tests the functionality of the privileges router including
privilege CRUD operations and permission management.
"""
import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException

from src.schemas.user import PrivilegeCreate, PrivilegeUpdate


@pytest.fixture
def mock_privilege_service():
    """Create a mock privilege service."""
    service = AsyncMock()
    
    # Create mock privilege objects
    mock_privilege = MagicMock()
    mock_privilege.id = uuid.uuid4()
    mock_privilege.name = "read_data"
    mock_privilege.description = "Permission to read data"
    mock_privilege.resource_type = "data"
    mock_privilege.actions = ["read", "list"]
    mock_privilege.is_active = True
    
    # Setup service method returns
    service.get.return_value = mock_privilege
    service.list.return_value = [mock_privilege]
    service.create.return_value = mock_privilege
    service.update.return_value = mock_privilege
    service.delete.return_value = True
    service.get_by_name.return_value = mock_privilege
    
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
def privilege_create_data():
    """Create test data for privilege creation."""
    return PrivilegeCreate(
        name="test_privilege",
        description="Test privilege for testing",
        resource_type="test_resource",
        actions=["read", "write"]
    )


@pytest.fixture
def privilege_update_data():
    """Create test data for privilege updates."""
    return PrivilegeUpdate(
        description="Updated test privilege",
        actions=["read", "write", "delete"]
    )


class TestPrivilegesRouter:
    """Test cases for PrivilegesRouter."""
    
    @pytest.mark.asyncio
    async def test_create_privilege_success(self, mock_privilege_service, mock_current_user, privilege_create_data):
        """Test successful privilege creation."""
        with patch("src.api.privileges_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.privileges_router.PrivilegeService", return_value=mock_privilege_service):
            
            from src.api.privileges_router import create_privilege
            
            result = await create_privilege(privilege_create_data, current_user=mock_current_user)
            
            assert result is not None
            assert result.name == "read_data"
            mock_privilege_service.create.assert_called_once_with(privilege_create_data)
    
    @pytest.mark.asyncio
    async def test_create_privilege_unauthorized(self, mock_privilege_service, privilege_create_data):
        """Test privilege creation without proper permissions."""
        unauthorized_user = MagicMock()
        unauthorized_user.id = uuid.uuid4()
        unauthorized_user.is_superuser = False
        unauthorized_user.roles = ["user"]
        
        with patch("src.api.privileges_router.get_current_user", return_value=unauthorized_user), \
             patch("src.api.privileges_router.PrivilegeService", return_value=mock_privilege_service):
            
            from src.api.privileges_router import create_privilege
            
            with pytest.raises(HTTPException) as exc_info:
                await create_privilege(privilege_create_data, current_user=unauthorized_user)
            
            assert exc_info.value.status_code == 403
    
    @pytest.mark.asyncio
    async def test_get_privilege_success(self, mock_privilege_service, mock_current_user):
        """Test successful privilege retrieval."""
        privilege_id = uuid.uuid4()
        
        with patch("src.api.privileges_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.privileges_router.PrivilegeService", return_value=mock_privilege_service):
            
            from src.api.privileges_router import get_privilege
            
            result = await get_privilege(privilege_id, current_user=mock_current_user)
            
            assert result is not None
            assert result.name == "read_data"
            mock_privilege_service.get.assert_called_once_with(privilege_id)
    
    @pytest.mark.asyncio
    async def test_get_privilege_not_found(self, mock_privilege_service, mock_current_user):
        """Test getting a non-existent privilege."""
        privilege_id = uuid.uuid4()
        mock_privilege_service.get.return_value = None
        
        with patch("src.api.privileges_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.privileges_router.PrivilegeService", return_value=mock_privilege_service):
            
            from src.api.privileges_router import get_privilege
            
            with pytest.raises(HTTPException) as exc_info:
                await get_privilege(privilege_id, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 404
    
    @pytest.mark.asyncio
    async def test_list_privileges_success(self, mock_privilege_service, mock_current_user):
        """Test successful privilege listing."""
        with patch("src.api.privileges_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.privileges_router.PrivilegeService", return_value=mock_privilege_service):
            
            from src.api.privileges_router import list_privileges
            
            result = await list_privileges(current_user=mock_current_user)
            
            assert len(result) == 1
            assert result[0].name == "read_data"
            mock_privilege_service.list.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_privilege_success(self, mock_privilege_service, mock_current_user, privilege_update_data):
        """Test successful privilege update."""
        privilege_id = uuid.uuid4()
        
        with patch("src.api.privileges_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.privileges_router.PrivilegeService", return_value=mock_privilege_service):
            
            from src.api.privileges_router import update_privilege
            
            result = await update_privilege(privilege_id, privilege_update_data, current_user=mock_current_user)
            
            assert result is not None
            mock_privilege_service.update.assert_called_once_with(privilege_id, privilege_update_data)
    
    @pytest.mark.asyncio
    async def test_delete_privilege_success(self, mock_privilege_service, mock_current_user):
        """Test successful privilege deletion."""
        privilege_id = uuid.uuid4()
        
        with patch("src.api.privileges_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.privileges_router.PrivilegeService", return_value=mock_privilege_service):
            
            from src.api.privileges_router import delete_privilege
            
            result = await delete_privilege(privilege_id, current_user=mock_current_user)
            
            assert result["message"] == "Privilege deleted successfully"
            mock_privilege_service.delete.assert_called_once_with(privilege_id)
    
    @pytest.mark.asyncio
    async def test_get_privileges_by_resource_type(self, mock_privilege_service, mock_current_user):
        """Test getting privileges by resource type."""
        resource_type = "data"
        
        with patch("src.api.privileges_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.privileges_router.PrivilegeService", return_value=mock_privilege_service):
            
            from src.api.privileges_router import get_privileges_by_resource_type
            
            result = await get_privileges_by_resource_type(resource_type, current_user=mock_current_user)
            
            assert len(result) == 1
            assert result[0].resource_type == "data"
            mock_privilege_service.get_by_resource_type.assert_called_once_with(resource_type)
    
    @pytest.mark.asyncio
    async def test_check_privilege_permission(self, mock_privilege_service, mock_current_user):
        """Test checking if user has specific privilege."""
        privilege_name = "read_data"
        resource_id = str(uuid.uuid4())
        
        mock_privilege_service.check_user_privilege.return_value = True
        
        with patch("src.api.privileges_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.privileges_router.PrivilegeService", return_value=mock_privilege_service):
            
            from src.api.privileges_router import check_privilege_permission
            
            result = await check_privilege_permission(
                privilege_name, 
                {"resource_id": resource_id}, 
                current_user=mock_current_user
            )
            
            assert result["has_privilege"] is True
            mock_privilege_service.check_user_privilege.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_assign_privilege_to_role(self, mock_privilege_service, mock_current_user):
        """Test assigning privilege to role."""
        privilege_id = uuid.uuid4()
        role_id = uuid.uuid4()
        
        with patch("src.api.privileges_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.privileges_router.PrivilegeService", return_value=mock_privilege_service):
            
            from src.api.privileges_router import assign_privilege_to_role
            
            result = await assign_privilege_to_role(
                privilege_id, 
                {"role_id": str(role_id)}, 
                current_user=mock_current_user
            )
            
            assert result["message"] == "Privilege assigned to role successfully"
            mock_privilege_service.assign_to_role.assert_called_once_with(privilege_id, role_id)
    
    @pytest.mark.asyncio
    async def test_remove_privilege_from_role(self, mock_privilege_service, mock_current_user):
        """Test removing privilege from role."""
        privilege_id = uuid.uuid4()
        role_id = uuid.uuid4()
        
        with patch("src.api.privileges_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.privileges_router.PrivilegeService", return_value=mock_privilege_service):
            
            from src.api.privileges_router import remove_privilege_from_role
            
            result = await remove_privilege_from_role(privilege_id, role_id, current_user=mock_current_user)
            
            assert result["message"] == "Privilege removed from role successfully"
            mock_privilege_service.remove_from_role.assert_called_once_with(privilege_id, role_id)
    
    @pytest.mark.asyncio
    async def test_get_privilege_hierarchy(self, mock_privilege_service, mock_current_user):
        """Test getting privilege hierarchy."""
        mock_hierarchy = {
            "admin": ["read_data", "write_data", "delete_data"],
            "editor": ["read_data", "write_data"],
            "viewer": ["read_data"]
        }
        mock_privilege_service.get_privilege_hierarchy.return_value = mock_hierarchy
        
        with patch("src.api.privileges_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.privileges_router.PrivilegeService", return_value=mock_privilege_service):
            
            from src.api.privileges_router import get_privilege_hierarchy
            
            result = await get_privilege_hierarchy(current_user=mock_current_user)
            
            assert "admin" in result
            assert len(result["admin"]) == 3
            mock_privilege_service.get_privilege_hierarchy.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_bulk_assign_privileges(self, mock_privilege_service, mock_current_user):
        """Test bulk assigning privileges to role."""
        role_id = uuid.uuid4()
        privilege_ids = [str(uuid.uuid4()), str(uuid.uuid4())]
        
        with patch("src.api.privileges_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.privileges_router.PrivilegeService", return_value=mock_privilege_service):
            
            from src.api.privileges_router import bulk_assign_privileges
            
            result = await bulk_assign_privileges(
                {"role_id": str(role_id), "privilege_ids": privilege_ids}, 
                current_user=mock_current_user
            )
            
            assert result["message"] == "Privileges assigned successfully"
            assert result["assigned_count"] == 2
            mock_privilege_service.bulk_assign_to_role.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_privilege_validation(self, mock_privilege_service, mock_current_user):
        """Test privilege data validation."""
        invalid_privilege_data = PrivilegeCreate(
            name="",  # Invalid empty name
            description="Test privilege",
            resource_type="",  # Invalid empty resource type
            actions=[]  # Invalid empty actions
        )
        
        with patch("src.api.privileges_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.privileges_router.PrivilegeService", return_value=mock_privilege_service):
            
            mock_privilege_service.create.side_effect = ValueError("Invalid privilege data")
            
            from src.api.privileges_router import create_privilege
            
            with pytest.raises(HTTPException) as exc_info:
                await create_privilege(invalid_privilege_data, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 422
    
    @pytest.mark.asyncio
    async def test_duplicate_privilege_name(self, mock_privilege_service, mock_current_user, privilege_create_data):
        """Test creating privilege with duplicate name."""
        mock_privilege_service.get_by_name.return_value = MagicMock()  # Existing privilege
        mock_privilege_service.create.side_effect = ValueError("Privilege name already exists")
        
        with patch("src.api.privileges_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.privileges_router.PrivilegeService", return_value=mock_privilege_service):
            
            from src.api.privileges_router import create_privilege
            
            with pytest.raises(HTTPException) as exc_info:
                await create_privilege(privilege_create_data, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 422
    
    @pytest.mark.asyncio
    async def test_get_effective_privileges(self, mock_privilege_service, mock_current_user):
        """Test getting effective privileges for user."""
        user_id = uuid.uuid4()
        mock_effective_privileges = [
            {"name": "read_data", "granted_by": "role:admin"},
            {"name": "write_data", "granted_by": "role:editor"}
        ]
        mock_privilege_service.get_effective_privileges.return_value = mock_effective_privileges
        
        with patch("src.api.privileges_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.privileges_router.PrivilegeService", return_value=mock_privilege_service):
            
            from src.api.privileges_router import get_effective_privileges
            
            result = await get_effective_privileges(user_id, current_user=mock_current_user)
            
            assert len(result) == 2
            assert result[0]["name"] == "read_data"
            mock_privilege_service.get_effective_privileges.assert_called_once_with(user_id)