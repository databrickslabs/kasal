"""
Unit tests for DatabricksRoleRouter.

Tests the functionality of the Databricks role router including
Databricks role management and integration.
"""
import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException

from src.schemas.user import DatabricksRoleCreate, DatabricksRoleUpdate


@pytest.fixture
def mock_databricks_role_service():
    """Create a mock Databricks role service."""
    service = AsyncMock()
    
    # Create mock Databricks role objects
    mock_role = MagicMock()
    mock_role.id = uuid.uuid4()
    mock_role.name = "databricks_admin"
    mock_role.description = "Databricks Administrator Role"
    mock_role.databricks_workspace_url = "https://test.databricks.com"
    mock_role.permissions = ["admin", "read", "write"]
    mock_role.is_active = True
    
    # Setup service method returns
    service.get.return_value = mock_role
    service.list.return_value = [mock_role]
    service.create.return_value = mock_role
    service.update.return_value = mock_role
    service.delete.return_value = True
    service.sync_with_databricks.return_value = {"synced": True, "count": 5}
    
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
def databricks_role_create_data():
    """Create test data for Databricks role creation."""
    return DatabricksRoleCreate(
        name="test_databricks_role",
        description="Test Databricks role for testing",
        databricks_workspace_url="https://test.databricks.com",
        permissions=["read", "write"]
    )


@pytest.fixture
def databricks_role_update_data():
    """Create test data for Databricks role updates."""
    return DatabricksRoleUpdate(
        description="Updated Databricks role",
        permissions=["read", "write", "admin"]
    )


class TestDatabricksRoleRouter:
    """Test cases for DatabricksRoleRouter."""
    
    @pytest.mark.asyncio
    async def test_create_databricks_role_success(self, mock_databricks_role_service, mock_current_user, databricks_role_create_data):
        """Test successful Databricks role creation."""
        with patch("src.api.databricks_role_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.databricks_role_router.DatabricksRoleService", return_value=mock_databricks_role_service):
            
            from src.api.databricks_role_router import create_databricks_role
            
            result = await create_databricks_role(databricks_role_create_data, current_user=mock_current_user)
            
            assert result is not None
            assert result.name == "databricks_admin"
            mock_databricks_role_service.create.assert_called_once_with(databricks_role_create_data)
    
    @pytest.mark.asyncio
    async def test_create_databricks_role_unauthorized(self, mock_databricks_role_service, databricks_role_create_data):
        """Test Databricks role creation without proper permissions."""
        unauthorized_user = MagicMock()
        unauthorized_user.id = uuid.uuid4()
        unauthorized_user.is_superuser = False
        unauthorized_user.roles = ["user"]
        
        with patch("src.api.databricks_role_router.get_current_user", return_value=unauthorized_user), \
             patch("src.api.databricks_role_router.DatabricksRoleService", return_value=mock_databricks_role_service):
            
            from src.api.databricks_role_router import create_databricks_role
            
            with pytest.raises(HTTPException) as exc_info:
                await create_databricks_role(databricks_role_create_data, current_user=unauthorized_user)
            
            assert exc_info.value.status_code == 403
    
    @pytest.mark.asyncio
    async def test_get_databricks_role_success(self, mock_databricks_role_service, mock_current_user):
        """Test successful Databricks role retrieval."""
        role_id = uuid.uuid4()
        
        with patch("src.api.databricks_role_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.databricks_role_router.DatabricksRoleService", return_value=mock_databricks_role_service):
            
            from src.api.databricks_role_router import get_databricks_role
            
            result = await get_databricks_role(role_id, current_user=mock_current_user)
            
            assert result is not None
            assert result.name == "databricks_admin"
            mock_databricks_role_service.get.assert_called_once_with(role_id)
    
    @pytest.mark.asyncio
    async def test_get_databricks_role_not_found(self, mock_databricks_role_service, mock_current_user):
        """Test getting a non-existent Databricks role."""
        role_id = uuid.uuid4()
        mock_databricks_role_service.get.return_value = None
        
        with patch("src.api.databricks_role_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.databricks_role_router.DatabricksRoleService", return_value=mock_databricks_role_service):
            
            from src.api.databricks_role_router import get_databricks_role
            
            with pytest.raises(HTTPException) as exc_info:
                await get_databricks_role(role_id, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 404
    
    @pytest.mark.asyncio
    async def test_list_databricks_roles_success(self, mock_databricks_role_service, mock_current_user):
        """Test successful Databricks role listing."""
        with patch("src.api.databricks_role_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.databricks_role_router.DatabricksRoleService", return_value=mock_databricks_role_service):
            
            from src.api.databricks_role_router import list_databricks_roles
            
            result = await list_databricks_roles(current_user=mock_current_user)
            
            assert len(result) == 1
            assert result[0].name == "databricks_admin"
            mock_databricks_role_service.list.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_databricks_role_success(self, mock_databricks_role_service, mock_current_user, databricks_role_update_data):
        """Test successful Databricks role update."""
        role_id = uuid.uuid4()
        
        with patch("src.api.databricks_role_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.databricks_role_router.DatabricksRoleService", return_value=mock_databricks_role_service):
            
            from src.api.databricks_role_router import update_databricks_role
            
            result = await update_databricks_role(role_id, databricks_role_update_data, current_user=mock_current_user)
            
            assert result is not None
            mock_databricks_role_service.update.assert_called_once_with(role_id, databricks_role_update_data)
    
    @pytest.mark.asyncio
    async def test_delete_databricks_role_success(self, mock_databricks_role_service, mock_current_user):
        """Test successful Databricks role deletion."""
        role_id = uuid.uuid4()
        
        with patch("src.api.databricks_role_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.databricks_role_router.DatabricksRoleService", return_value=mock_databricks_role_service):
            
            from src.api.databricks_role_router import delete_databricks_role
            
            result = await delete_databricks_role(role_id, current_user=mock_current_user)
            
            assert result["message"] == "Databricks role deleted successfully"
            mock_databricks_role_service.delete.assert_called_once_with(role_id)
    
    @pytest.mark.asyncio
    async def test_sync_databricks_roles(self, mock_databricks_role_service, mock_current_user):
        """Test syncing roles with Databricks."""
        workspace_url = "https://test.databricks.com"
        
        with patch("src.api.databricks_role_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.databricks_role_router.DatabricksRoleService", return_value=mock_databricks_role_service):
            
            from src.api.databricks_role_router import sync_databricks_roles
            
            result = await sync_databricks_roles({"workspace_url": workspace_url}, current_user=mock_current_user)
            
            assert result["synced"] is True
            assert result["count"] == 5
            mock_databricks_role_service.sync_with_databricks.assert_called_once_with(workspace_url)
    
    @pytest.mark.asyncio
    async def test_validate_databricks_connection(self, mock_databricks_role_service, mock_current_user):
        """Test validating Databricks connection."""
        workspace_url = "https://test.databricks.com"
        mock_databricks_role_service.validate_connection.return_value = {"valid": True, "version": "v2.1"}
        
        with patch("src.api.databricks_role_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.databricks_role_router.DatabricksRoleService", return_value=mock_databricks_role_service):
            
            from src.api.databricks_role_router import validate_databricks_connection
            
            result = await validate_databricks_connection({"workspace_url": workspace_url}, current_user=mock_current_user)
            
            assert result["valid"] is True
            assert result["version"] == "v2.1"
            mock_databricks_role_service.validate_connection.assert_called_once_with(workspace_url)
    
    @pytest.mark.asyncio
    async def test_get_databricks_workspaces(self, mock_databricks_role_service, mock_current_user):
        """Test getting available Databricks workspaces."""
        mock_workspaces = [
            {"id": "ws-1", "name": "Production", "url": "https://prod.databricks.com"},
            {"id": "ws-2", "name": "Development", "url": "https://dev.databricks.com"}
        ]
        mock_databricks_role_service.get_workspaces.return_value = mock_workspaces
        
        with patch("src.api.databricks_role_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.databricks_role_router.DatabricksRoleService", return_value=mock_databricks_role_service):
            
            from src.api.databricks_role_router import get_databricks_workspaces
            
            result = await get_databricks_workspaces(current_user=mock_current_user)
            
            assert len(result) == 2
            assert result[0]["name"] == "Production"
            mock_databricks_role_service.get_workspaces.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_assign_databricks_role_to_user(self, mock_databricks_role_service, mock_current_user):
        """Test assigning Databricks role to user."""
        role_id = uuid.uuid4()
        user_id = uuid.uuid4()
        
        with patch("src.api.databricks_role_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.databricks_role_router.DatabricksRoleService", return_value=mock_databricks_role_service):
            
            from src.api.databricks_role_router import assign_databricks_role_to_user
            
            result = await assign_databricks_role_to_user(role_id, {"user_id": str(user_id)}, current_user=mock_current_user)
            
            assert result["message"] == "Databricks role assigned successfully"
            mock_databricks_role_service.assign_to_user.assert_called_once_with(role_id, user_id)
    
    @pytest.mark.asyncio
    async def test_databricks_role_validation(self, mock_databricks_role_service, mock_current_user):
        """Test Databricks role data validation."""
        invalid_role_data = DatabricksRoleCreate(
            name="",  # Invalid empty name
            description="Test role",
            databricks_workspace_url="invalid-url",  # Invalid URL
            permissions=[]
        )
        
        with patch("src.api.databricks_role_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.databricks_role_router.DatabricksRoleService", return_value=mock_databricks_role_service):
            
            mock_databricks_role_service.create.side_effect = ValueError("Invalid Databricks role data")
            
            from src.api.databricks_role_router import create_databricks_role
            
            with pytest.raises(HTTPException) as exc_info:
                await create_databricks_role(invalid_role_data, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 422
    
    @pytest.mark.asyncio
    async def test_databricks_connection_error(self, mock_databricks_role_service, mock_current_user):
        """Test handling Databricks connection errors."""
        workspace_url = "https://unreachable.databricks.com"
        mock_databricks_role_service.validate_connection.side_effect = Exception("Connection failed")
        
        with patch("src.api.databricks_role_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.databricks_role_router.DatabricksRoleService", return_value=mock_databricks_role_service):
            
            from src.api.databricks_role_router import validate_databricks_connection
            
            with pytest.raises(HTTPException) as exc_info:
                await validate_databricks_connection({"workspace_url": workspace_url}, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 503