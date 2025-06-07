"""
Unit tests for DatabricksRoleService.

Tests the functionality of the Databricks role service including
Databricks integration, role synchronization, and management.
"""
import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, UTC

from src.services.databricks_role_service import DatabricksRoleService
from src.schemas.user import DatabricksRoleCreate, DatabricksRoleUpdate
from src.models.user import DatabricksRole
from src.core.unit_of_work import UnitOfWork


@pytest.fixture
def mock_uow():
    """Create a mock unit of work."""
    uow = MagicMock(spec=UnitOfWork)
    uow.session = AsyncMock()
    uow.commit = AsyncMock()
    uow.rollback = AsyncMock()
    return uow


@pytest.fixture
def mock_databricks_role_repository():
    """Create a mock Databricks role repository."""
    repo = AsyncMock()
    
    # Create mock Databricks role objects
    mock_role = MagicMock(spec=DatabricksRole)
    mock_role.id = uuid.uuid4()
    mock_role.name = "databricks_admin"
    mock_role.description = "Databricks Administrator Role"
    mock_role.databricks_workspace_url = "https://test.databricks.com"
    mock_role.permissions = ["admin", "read", "write"]
    mock_role.is_active = True
    mock_role.created_at = datetime.now(UTC)
    mock_role.updated_at = datetime.now(UTC)
    
    # Setup repository method returns
    repo.get.return_value = mock_role
    repo.list.return_value = [mock_role]
    repo.create.return_value = mock_role
    repo.update.return_value = mock_role
    repo.delete.return_value = True
    repo.get_by_name.return_value = mock_role
    repo.get_by_workspace.return_value = [mock_role]
    
    return repo


@pytest.fixture
def mock_databricks_client():
    """Create a mock Databricks client."""
    client = MagicMock()
    client.validate_connection.return_value = {"valid": True, "version": "v2.1"}
    client.get_roles.return_value = [
        {"id": "role1", "name": "admin", "permissions": ["admin"]},
        {"id": "role2", "name": "user", "permissions": ["read"]}
    ]
    client.get_workspaces.return_value = [
        {"id": "ws1", "name": "Production", "url": "https://prod.databricks.com"},
        {"id": "ws2", "name": "Development", "url": "https://dev.databricks.com"}
    ]
    return client


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


class TestDatabricksRoleService:
    """Test cases for DatabricksRoleService."""
    
    @pytest.mark.asyncio
    async def test_create_databricks_role_success(self, mock_uow, mock_databricks_role_repository, databricks_role_create_data):
        """Test successful Databricks role creation."""
        with patch("src.services.databricks_role_service.DatabricksRoleRepository", return_value=mock_databricks_role_repository):
            service = DatabricksRoleService(mock_uow)
            
            result = await service.create(databricks_role_create_data)
            
            assert result is not None
            assert result.name == "databricks_admin"
            mock_databricks_role_repository.create.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_databricks_role_validation_error(self, mock_uow, mock_databricks_role_repository):
        """Test Databricks role creation with invalid data."""
        with patch("src.services.databricks_role_service.DatabricksRoleRepository", return_value=mock_databricks_role_repository):
            service = DatabricksRoleService(mock_uow)
            
            invalid_data = DatabricksRoleCreate(
                name="",  # Invalid empty name
                description="Test role",
                databricks_workspace_url="invalid-url",
                permissions=[]
            )
            
            mock_databricks_role_repository.create.side_effect = ValueError("Invalid Databricks role data")
            
            with pytest.raises(ValueError, match="Invalid Databricks role data"):
                await service.create(invalid_data)
            
            mock_uow.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_databricks_role_by_id(self, mock_uow, mock_databricks_role_repository):
        """Test getting a Databricks role by ID."""
        role_id = uuid.uuid4()
        
        with patch("src.services.databricks_role_service.DatabricksRoleRepository", return_value=mock_databricks_role_repository):
            service = DatabricksRoleService(mock_uow)
            
            result = await service.get(role_id)
            
            assert result is not None
            assert result.name == "databricks_admin"
            mock_databricks_role_repository.get.assert_called_once_with(role_id)
    
    @pytest.mark.asyncio
    async def test_update_databricks_role_success(self, mock_uow, mock_databricks_role_repository, databricks_role_update_data):
        """Test successful Databricks role update."""
        role_id = uuid.uuid4()
        
        with patch("src.services.databricks_role_service.DatabricksRoleRepository", return_value=mock_databricks_role_repository):
            service = DatabricksRoleService(mock_uow)
            
            result = await service.update(role_id, databricks_role_update_data)
            
            assert result is not None
            mock_databricks_role_repository.update.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_databricks_role_success(self, mock_uow, mock_databricks_role_repository):
        """Test successful Databricks role deletion."""
        role_id = uuid.uuid4()
        
        with patch("src.services.databricks_role_service.DatabricksRoleRepository", return_value=mock_databricks_role_repository):
            service = DatabricksRoleService(mock_uow)
            
            result = await service.delete(role_id)
            
            assert result is True
            mock_databricks_role_repository.delete.assert_called_once_with(role_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_sync_with_databricks_success(self, mock_uow, mock_databricks_role_repository, mock_databricks_client):
        """Test successful synchronization with Databricks."""
        workspace_url = "https://test.databricks.com"
        
        with patch("src.services.databricks_role_service.DatabricksRoleRepository", return_value=mock_databricks_role_repository), \
             patch("src.services.databricks_role_service.DatabricksClient", return_value=mock_databricks_client):
            
            service = DatabricksRoleService(mock_uow)
            
            result = await service.sync_with_databricks(workspace_url)
            
            assert result["synced"] is True
            assert result["count"] == 2
            mock_databricks_client.get_roles.assert_called_once()
            mock_uow.commit.assert_called()
    
    @pytest.mark.asyncio
    async def test_validate_connection_success(self, mock_uow, mock_databricks_client):
        """Test successful Databricks connection validation."""
        workspace_url = "https://test.databricks.com"
        
        with patch("src.services.databricks_role_service.DatabricksClient", return_value=mock_databricks_client):
            service = DatabricksRoleService(mock_uow)
            
            result = await service.validate_connection(workspace_url)
            
            assert result["valid"] is True
            assert result["version"] == "v2.1"
            mock_databricks_client.validate_connection.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_validate_connection_failure(self, mock_uow, mock_databricks_client):
        """Test Databricks connection validation failure."""
        workspace_url = "https://unreachable.databricks.com"
        mock_databricks_client.validate_connection.side_effect = Exception("Connection failed")
        
        with patch("src.services.databricks_role_service.DatabricksClient", return_value=mock_databricks_client):
            service = DatabricksRoleService(mock_uow)
            
            with pytest.raises(Exception, match="Connection failed"):
                await service.validate_connection(workspace_url)
    
    @pytest.mark.asyncio
    async def test_get_workspaces(self, mock_uow, mock_databricks_client):
        """Test getting available Databricks workspaces."""
        with patch("src.services.databricks_role_service.DatabricksClient", return_value=mock_databricks_client):
            service = DatabricksRoleService(mock_uow)
            
            result = await service.get_workspaces()
            
            assert len(result) == 2
            assert result[0]["name"] == "Production"
            mock_databricks_client.get_workspaces.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_assign_to_user(self, mock_uow, mock_databricks_role_repository):
        """Test assigning Databricks role to user."""
        role_id = uuid.uuid4()
        user_id = uuid.uuid4()
        
        with patch("src.services.databricks_role_service.DatabricksRoleRepository", return_value=mock_databricks_role_repository):
            service = DatabricksRoleService(mock_uow)
            
            result = await service.assign_to_user(role_id, user_id)
            
            assert result is True
            mock_databricks_role_repository.assign_to_user.assert_called_once_with(role_id, user_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_remove_from_user(self, mock_uow, mock_databricks_role_repository):
        """Test removing Databricks role from user."""
        role_id = uuid.uuid4()
        user_id = uuid.uuid4()
        
        with patch("src.services.databricks_role_service.DatabricksRoleRepository", return_value=mock_databricks_role_repository):
            service = DatabricksRoleService(mock_uow)
            
            result = await service.remove_from_user(role_id, user_id)
            
            assert result is True
            mock_databricks_role_repository.remove_from_user.assert_called_once_with(role_id, user_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_roles_by_workspace(self, mock_uow, mock_databricks_role_repository):
        """Test getting roles by workspace."""
        workspace_url = "https://test.databricks.com"
        
        with patch("src.services.databricks_role_service.DatabricksRoleRepository", return_value=mock_databricks_role_repository):
            service = DatabricksRoleService(mock_uow)
            
            result = await service.get_roles_by_workspace(workspace_url)
            
            assert len(result) == 1
            assert result[0].databricks_workspace_url == "https://test.databricks.com"
            mock_databricks_role_repository.get_by_workspace.assert_called_once_with(workspace_url)
    
    @pytest.mark.asyncio
    async def test_validate_role_permissions(self, mock_uow):
        """Test validation of Databricks role permissions."""
        service = DatabricksRoleService(mock_uow)
        
        # Test valid permissions
        valid_permissions = ["admin", "read", "write", "execute"]
        service._validate_permissions(valid_permissions)  # Should not raise
        
        # Test invalid permissions
        invalid_permissions = ["invalid_permission"]
        
        with pytest.raises(ValueError, match="Invalid permission"):
            service._validate_permissions(invalid_permissions)
    
    @pytest.mark.asyncio
    async def test_validate_workspace_url(self, mock_uow):
        """Test validation of Databricks workspace URL."""
        service = DatabricksRoleService(mock_uow)
        
        # Test valid URL
        valid_url = "https://test.databricks.com"
        service._validate_workspace_url(valid_url)  # Should not raise
        
        # Test invalid URL
        invalid_url = "not-a-valid-url"
        
        with pytest.raises(ValueError, match="Invalid workspace URL"):
            service._validate_workspace_url(invalid_url)
    
    @pytest.mark.asyncio
    async def test_duplicate_role_name_in_workspace(self, mock_uow, mock_databricks_role_repository, databricks_role_create_data):
        """Test creating role with duplicate name in same workspace."""
        mock_databricks_role_repository.get_by_name_and_workspace.return_value = MagicMock()  # Existing role
        mock_databricks_role_repository.create.side_effect = ValueError("Role name already exists in workspace")
        
        with patch("src.services.databricks_role_service.DatabricksRoleRepository", return_value=mock_databricks_role_repository):
            service = DatabricksRoleService(mock_uow)
            
            with pytest.raises(ValueError, match="Role name already exists in workspace"):
                await service.create(databricks_role_create_data)
    
    @pytest.mark.asyncio
    async def test_sync_role_conflict_resolution(self, mock_uow, mock_databricks_role_repository, mock_databricks_client):
        """Test handling conflicts during role synchronization."""
        workspace_url = "https://test.databricks.com"
        
        # Mock conflicting role (exists locally but different in Databricks)
        local_role = MagicMock()
        local_role.name = "admin"
        local_role.permissions = ["read"]  # Different from Databricks
        
        mock_databricks_role_repository.get_by_name_and_workspace.return_value = local_role
        
        with patch("src.services.databricks_role_service.DatabricksRoleRepository", return_value=mock_databricks_role_repository), \
             patch("src.services.databricks_role_service.DatabricksClient", return_value=mock_databricks_client):
            
            service = DatabricksRoleService(mock_uow)
            
            result = await service.sync_with_databricks(workspace_url, resolve_conflicts=True)
            
            assert result["synced"] is True
            assert result["conflicts_resolved"] >= 0
            mock_databricks_role_repository.update.assert_called()
    
    @pytest.mark.asyncio
    async def test_get_user_databricks_roles(self, mock_uow, mock_databricks_role_repository):
        """Test getting user's Databricks roles."""
        user_id = uuid.uuid4()
        
        with patch("src.services.databricks_role_service.DatabricksRoleRepository", return_value=mock_databricks_role_repository):
            service = DatabricksRoleService(mock_uow)
            
            result = await service.get_user_roles(user_id)
            
            assert len(result) == 1
            mock_databricks_role_repository.get_user_roles.assert_called_once_with(user_id)
    
    @pytest.mark.asyncio
    async def test_databricks_role_metrics(self, mock_uow, mock_databricks_role_repository):
        """Test getting Databricks role metrics."""
        with patch("src.services.databricks_role_service.DatabricksRoleRepository", return_value=mock_databricks_role_repository):
            service = DatabricksRoleService(mock_uow)
            
            metrics = await service.get_role_metrics()
            
            assert "total_roles" in metrics
            assert "active_roles" in metrics
            assert "roles_by_workspace" in metrics
            mock_databricks_role_repository.get_metrics.assert_called_once()