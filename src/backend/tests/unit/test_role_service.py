"""
Unit tests for RoleService.

Tests the functionality of the role service including
role management, permission assignment, and RBAC operations.
"""
import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, UTC

from src.services.role_service import RoleService
from src.schemas.user import RoleCreate, RoleUpdate
from src.models.user import Role
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
def mock_role_repository():
    """Create a mock role repository."""
    repo = AsyncMock()
    
    # Create mock role objects
    mock_role = MagicMock(spec=Role)
    mock_role.id = uuid.uuid4()
    mock_role.name = "admin"
    mock_role.description = "Administrator role"
    mock_role.permissions = ["read", "write", "delete"]
    mock_role.is_active = True
    mock_role.is_system_role = False
    mock_role.created_at = datetime.now(UTC)
    mock_role.updated_at = datetime.now(UTC)
    
    # Setup repository method returns
    repo.get.return_value = mock_role
    repo.list.return_value = [mock_role]
    repo.create.return_value = mock_role
    repo.update.return_value = mock_role
    repo.delete.return_value = True
    repo.get_by_name.return_value = mock_role
    repo.get_user_roles.return_value = [mock_role]
    
    return repo


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


class TestRoleService:
    """Test cases for RoleService."""
    
    @pytest.mark.asyncio
    async def test_create_role_success(self, mock_uow, mock_role_repository, role_create_data):
        """Test successful role creation."""
        with patch("src.services.role_service.RoleRepository", return_value=mock_role_repository):
            service = RoleService(mock_uow)
            
            result = await service.create(role_create_data)
            
            assert result is not None
            assert result.name == "admin"
            mock_role_repository.create.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_role_validation_error(self, mock_uow, mock_role_repository):
        """Test role creation with invalid data."""
        with patch("src.services.role_service.RoleRepository", return_value=mock_role_repository):
            service = RoleService(mock_uow)
            
            invalid_data = RoleCreate(
                name="",  # Invalid empty name
                description="Test role",
                permissions=[]
            )
            
            mock_role_repository.create.side_effect = ValueError("Role name cannot be empty")
            
            with pytest.raises(ValueError, match="Role name cannot be empty"):
                await service.create(invalid_data)
            
            mock_uow.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_role_by_id(self, mock_uow, mock_role_repository):
        """Test getting a role by ID."""
        role_id = uuid.uuid4()
        
        with patch("src.services.role_service.RoleRepository", return_value=mock_role_repository):
            service = RoleService(mock_uow)
            
            result = await service.get(role_id)
            
            assert result is not None
            assert result.name == "admin"
            mock_role_repository.get.assert_called_once_with(role_id)
    
    @pytest.mark.asyncio
    async def test_update_role_success(self, mock_uow, mock_role_repository, role_update_data):
        """Test successful role update."""
        role_id = uuid.uuid4()
        
        with patch("src.services.role_service.RoleRepository", return_value=mock_role_repository):
            service = RoleService(mock_uow)
            
            result = await service.update(role_id, role_update_data)
            
            assert result is not None
            mock_role_repository.update.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_role_success(self, mock_uow, mock_role_repository):
        """Test successful role deletion."""
        role_id = uuid.uuid4()
        
        # Mock that role is not a system role and has no dependencies
        mock_role = MagicMock()
        mock_role.is_system_role = False
        mock_role_repository.get.return_value = mock_role
        mock_role_repository.has_users.return_value = False
        
        with patch("src.services.role_service.RoleRepository", return_value=mock_role_repository):
            service = RoleService(mock_uow)
            
            result = await service.delete(role_id)
            
            assert result is True
            mock_role_repository.delete.assert_called_once_with(role_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_system_role_error(self, mock_uow, mock_role_repository):
        """Test deletion of system role should fail."""
        role_id = uuid.uuid4()
        
        # Mock that role is a system role
        mock_role = MagicMock()
        mock_role.is_system_role = True
        mock_role_repository.get.return_value = mock_role
        
        with patch("src.services.role_service.RoleRepository", return_value=mock_role_repository):
            service = RoleService(mock_uow)
            
            with pytest.raises(ValueError, match="Cannot delete system role"):
                await service.delete(role_id)
    
    @pytest.mark.asyncio
    async def test_delete_role_with_users_error(self, mock_uow, mock_role_repository):
        """Test deletion of role with assigned users should fail."""
        role_id = uuid.uuid4()
        
        # Mock that role has users assigned
        mock_role = MagicMock()
        mock_role.is_system_role = False
        mock_role_repository.get.return_value = mock_role
        mock_role_repository.has_users.return_value = True
        
        with patch("src.services.role_service.RoleRepository", return_value=mock_role_repository):
            service = RoleService(mock_uow)
            
            with pytest.raises(ValueError, match="Cannot delete role with assigned users"):
                await service.delete(role_id)
    
    @pytest.mark.asyncio
    async def test_assign_permissions_success(self, mock_uow, mock_role_repository):
        """Test successful permission assignment to role."""
        role_id = uuid.uuid4()
        permissions = ["read", "write", "execute"]
        
        with patch("src.services.role_service.RoleRepository", return_value=mock_role_repository):
            service = RoleService(mock_uow)
            
            result = await service.assign_permissions(role_id, permissions)
            
            assert result is True
            mock_role_repository.assign_permissions.assert_called_once_with(role_id, permissions)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_remove_permissions_success(self, mock_uow, mock_role_repository):
        """Test successful permission removal from role."""
        role_id = uuid.uuid4()
        permissions = ["delete"]
        
        with patch("src.services.role_service.RoleRepository", return_value=mock_role_repository):
            service = RoleService(mock_uow)
            
            result = await service.remove_permissions(role_id, permissions)
            
            assert result is True
            mock_role_repository.remove_permissions.assert_called_once_with(role_id, permissions)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_user_roles(self, mock_uow, mock_role_repository):
        """Test getting roles for a user."""
        user_id = uuid.uuid4()
        
        with patch("src.services.role_service.RoleRepository", return_value=mock_role_repository):
            service = RoleService(mock_uow)
            
            result = await service.get_user_roles(user_id)
            
            assert len(result) == 1
            assert result[0].name == "admin"
            mock_role_repository.get_user_roles.assert_called_once_with(user_id)
    
    @pytest.mark.asyncio
    async def test_check_user_has_role(self, mock_uow, mock_role_repository):
        """Test checking if user has specific role."""
        user_id = uuid.uuid4()
        role_name = "admin"
        
        mock_role_repository.user_has_role.return_value = True
        
        with patch("src.services.role_service.RoleRepository", return_value=mock_role_repository):
            service = RoleService(mock_uow)
            
            result = await service.user_has_role(user_id, role_name)
            
            assert result is True
            mock_role_repository.user_has_role.assert_called_once_with(user_id, role_name)
    
    @pytest.mark.asyncio
    async def test_get_role_hierarchy(self, mock_uow, mock_role_repository):
        """Test getting role hierarchy."""
        mock_hierarchy = {
            "superuser": {"level": 0, "inherits": []},
            "admin": {"level": 1, "inherits": ["superuser"]},
            "editor": {"level": 2, "inherits": ["admin"]},
            "viewer": {"level": 3, "inherits": ["editor"]}
        }
        mock_role_repository.get_role_hierarchy.return_value = mock_hierarchy
        
        with patch("src.services.role_service.RoleRepository", return_value=mock_role_repository):
            service = RoleService(mock_uow)
            
            result = await service.get_role_hierarchy()
            
            assert "admin" in result
            assert result["admin"]["level"] == 1
            mock_role_repository.get_role_hierarchy.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_validate_role_permissions(self, mock_uow):
        """Test validation of role permissions."""
        service = RoleService(mock_uow)
        
        # Test valid permissions
        valid_permissions = ["read", "write", "delete", "execute", "admin"]
        service._validate_permissions(valid_permissions)  # Should not raise
        
        # Test invalid permissions
        invalid_permissions = ["invalid_permission"]
        
        with pytest.raises(ValueError, match="Invalid permission"):
            service._validate_permissions(invalid_permissions)
    
    @pytest.mark.asyncio
    async def test_duplicate_role_name(self, mock_uow, mock_role_repository, role_create_data):
        """Test creating role with duplicate name."""
        mock_role_repository.get_by_name.return_value = MagicMock()  # Existing role
        mock_role_repository.create.side_effect = ValueError("Role name already exists")
        
        with patch("src.services.role_service.RoleRepository", return_value=mock_role_repository):
            service = RoleService(mock_uow)
            
            with pytest.raises(ValueError, match="Role name already exists"):
                await service.create(role_create_data)
    
    @pytest.mark.asyncio
    async def test_role_inheritance_check(self, mock_uow, mock_role_repository):
        """Test role inheritance validation."""
        parent_role_id = uuid.uuid4()
        child_role_id = uuid.uuid4()
        
        # Mock circular inheritance check
        mock_role_repository.would_create_circular_inheritance.return_value = True
        
        with patch("src.services.role_service.RoleRepository", return_value=mock_role_repository):
            service = RoleService(mock_uow)
            
            with pytest.raises(ValueError, match="Circular inheritance detected"):
                await service.set_role_inheritance(child_role_id, parent_role_id)
    
    @pytest.mark.asyncio
    async def test_get_effective_permissions(self, mock_uow, mock_role_repository):
        """Test getting effective permissions for user through roles."""
        user_id = uuid.uuid4()
        mock_permissions = ["read", "write", "admin"]
        mock_role_repository.get_effective_permissions.return_value = mock_permissions
        
        with patch("src.services.role_service.RoleRepository", return_value=mock_role_repository):
            service = RoleService(mock_uow)
            
            result = await service.get_effective_permissions(user_id)
            
            assert len(result) == 3
            assert "admin" in result
            mock_role_repository.get_effective_permissions.assert_called_once_with(user_id)
    
    @pytest.mark.asyncio
    async def test_bulk_assign_roles(self, mock_uow, mock_role_repository):
        """Test bulk role assignment to users."""
        user_ids = [uuid.uuid4(), uuid.uuid4()]
        role_ids = [uuid.uuid4(), uuid.uuid4()]
        
        with patch("src.services.role_service.RoleRepository", return_value=mock_role_repository):
            service = RoleService(mock_uow)
            
            result = await service.bulk_assign_roles(user_ids, role_ids)
            
            assert result == 4  # 2 users * 2 roles
            mock_role_repository.bulk_assign_roles.assert_called_once_with(user_ids, role_ids)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_role_expiration_handling(self, mock_uow, mock_role_repository):
        """Test handling of role expiration."""
        user_id = uuid.uuid4()
        role_id = uuid.uuid4()
        expires_at = datetime.now(UTC)
        
        with patch("src.services.role_service.RoleRepository", return_value=mock_role_repository):
            service = RoleService(mock_uow)
            
            result = await service.assign_role_with_expiration(user_id, role_id, expires_at)
            
            assert result is True
            mock_role_repository.assign_role_with_expiration.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_cleanup_expired_roles(self, mock_uow, mock_role_repository):
        """Test cleanup of expired role assignments."""
        mock_role_repository.cleanup_expired_assignments.return_value = 5  # 5 expired assignments
        
        with patch("src.services.role_service.RoleRepository", return_value=mock_role_repository):
            service = RoleService(mock_uow)
            
            result = await service.cleanup_expired_roles()
            
            assert result == 5
            mock_role_repository.cleanup_expired_assignments.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_role_audit_logging(self, mock_uow, mock_role_repository, role_create_data):
        """Test that role operations are audited."""
        with patch("src.services.role_service.RoleRepository", return_value=mock_role_repository), \
             patch("src.services.role_service.audit_logger") as mock_audit:
            
            service = RoleService(mock_uow)
            
            await service.create(role_create_data)
            
            mock_audit.log_role_creation.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_role_template_operations(self, mock_uow, mock_role_repository):
        """Test role template creation and application."""
        template_name = "admin_template"
        template_data = {
            "permissions": ["read", "write", "admin"],
            "description": "Administrator template"
        }
        
        with patch("src.services.role_service.RoleRepository", return_value=mock_role_repository):
            service = RoleService(mock_uow)
            
            # Create template
            await service.create_role_template(template_name, template_data)
            mock_role_repository.create_template.assert_called_once()
            
            # Apply template
            role_id = uuid.uuid4()
            await service.apply_role_template(role_id, template_name)
            mock_role_repository.apply_template.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_role_metrics(self, mock_uow, mock_role_repository):
        """Test getting role metrics."""
        with patch("src.services.role_service.RoleRepository", return_value=mock_role_repository):
            service = RoleService(mock_uow)
            
            metrics = await service.get_role_metrics()
            
            assert "total_roles" in metrics
            assert "active_roles" in metrics
            assert "roles_by_permission_count" in metrics
            mock_role_repository.get_metrics.assert_called_once()