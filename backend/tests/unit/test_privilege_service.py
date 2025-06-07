"""
Unit tests for PrivilegeService.

Tests the functionality of the privilege service including
privilege management, permission checking, and RBAC operations.
"""
import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, UTC

from src.services.privilege_service import PrivilegeService
from src.schemas.user import PrivilegeCreate, PrivilegeUpdate
from src.models.user import Privilege
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
def mock_privilege_repository():
    """Create a mock privilege repository."""
    repo = AsyncMock()
    
    # Create mock privilege objects
    mock_privilege = MagicMock(spec=Privilege)
    mock_privilege.id = uuid.uuid4()
    mock_privilege.name = "read_data"
    mock_privilege.description = "Permission to read data"
    mock_privilege.resource_type = "data"
    mock_privilege.actions = ["read", "list"]
    mock_privilege.is_active = True
    mock_privilege.created_at = datetime.now(UTC)
    mock_privilege.updated_at = datetime.now(UTC)
    
    # Setup repository method returns
    repo.get.return_value = mock_privilege
    repo.list.return_value = [mock_privilege]
    repo.create.return_value = mock_privilege
    repo.update.return_value = mock_privilege
    repo.delete.return_value = True
    repo.get_by_name.return_value = mock_privilege
    repo.get_by_resource_type.return_value = [mock_privilege]
    
    return repo


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


class TestPrivilegeService:
    """Test cases for PrivilegeService."""
    
    @pytest.mark.asyncio
    async def test_create_privilege_success(self, mock_uow, mock_privilege_repository, privilege_create_data):
        """Test successful privilege creation."""
        with patch("src.services.privilege_service.PrivilegeRepository", return_value=mock_privilege_repository):
            service = PrivilegeService(mock_uow)
            
            result = await service.create(privilege_create_data)
            
            assert result is not None
            assert result.name == "read_data"
            mock_privilege_repository.create.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_privilege_validation_error(self, mock_uow, mock_privilege_repository):
        """Test privilege creation with invalid data."""
        with patch("src.services.privilege_service.PrivilegeRepository", return_value=mock_privilege_repository):
            service = PrivilegeService(mock_uow)
            
            invalid_data = PrivilegeCreate(
                name="",  # Invalid empty name
                description="Test privilege",
                resource_type="",
                actions=[]
            )
            
            mock_privilege_repository.create.side_effect = ValueError("Invalid privilege data")
            
            with pytest.raises(ValueError, match="Invalid privilege data"):
                await service.create(invalid_data)
            
            mock_uow.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_privilege_by_id(self, mock_uow, mock_privilege_repository):
        """Test getting a privilege by ID."""
        privilege_id = uuid.uuid4()
        
        with patch("src.services.privilege_service.PrivilegeRepository", return_value=mock_privilege_repository):
            service = PrivilegeService(mock_uow)
            
            result = await service.get(privilege_id)
            
            assert result is not None
            assert result.name == "read_data"
            mock_privilege_repository.get.assert_called_once_with(privilege_id)
    
    @pytest.mark.asyncio
    async def test_update_privilege_success(self, mock_uow, mock_privilege_repository, privilege_update_data):
        """Test successful privilege update."""
        privilege_id = uuid.uuid4()
        
        with patch("src.services.privilege_service.PrivilegeRepository", return_value=mock_privilege_repository):
            service = PrivilegeService(mock_uow)
            
            result = await service.update(privilege_id, privilege_update_data)
            
            assert result is not None
            mock_privilege_repository.update.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_privilege_success(self, mock_uow, mock_privilege_repository):
        """Test successful privilege deletion."""
        privilege_id = uuid.uuid4()
        
        with patch("src.services.privilege_service.PrivilegeRepository", return_value=mock_privilege_repository):
            service = PrivilegeService(mock_uow)
            
            result = await service.delete(privilege_id)
            
            assert result is True
            mock_privilege_repository.delete.assert_called_once_with(privilege_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_privileges_by_resource_type(self, mock_uow, mock_privilege_repository):
        """Test getting privileges by resource type."""
        resource_type = "data"
        
        with patch("src.services.privilege_service.PrivilegeRepository", return_value=mock_privilege_repository):
            service = PrivilegeService(mock_uow)
            
            result = await service.get_by_resource_type(resource_type)
            
            assert len(result) == 1
            assert result[0].resource_type == "data"
            mock_privilege_repository.get_by_resource_type.assert_called_once_with(resource_type)
    
    @pytest.mark.asyncio
    async def test_check_user_privilege(self, mock_uow, mock_privilege_repository):
        """Test checking if user has specific privilege."""
        user_id = uuid.uuid4()
        privilege_name = "read_data"
        resource_id = str(uuid.uuid4())
        
        mock_privilege_repository.check_user_privilege.return_value = True
        
        with patch("src.services.privilege_service.PrivilegeRepository", return_value=mock_privilege_repository):
            service = PrivilegeService(mock_uow)
            
            result = await service.check_user_privilege(user_id, privilege_name, resource_id)
            
            assert result is True
            mock_privilege_repository.check_user_privilege.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_assign_privilege_to_role(self, mock_uow, mock_privilege_repository):
        """Test assigning privilege to role."""
        privilege_id = uuid.uuid4()
        role_id = uuid.uuid4()
        
        with patch("src.services.privilege_service.PrivilegeRepository", return_value=mock_privilege_repository):
            service = PrivilegeService(mock_uow)
            
            result = await service.assign_to_role(privilege_id, role_id)
            
            assert result is True
            mock_privilege_repository.assign_to_role.assert_called_once_with(privilege_id, role_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_remove_privilege_from_role(self, mock_uow, mock_privilege_repository):
        """Test removing privilege from role."""
        privilege_id = uuid.uuid4()
        role_id = uuid.uuid4()
        
        with patch("src.services.privilege_service.PrivilegeRepository", return_value=mock_privilege_repository):
            service = PrivilegeService(mock_uow)
            
            result = await service.remove_from_role(privilege_id, role_id)
            
            assert result is True
            mock_privilege_repository.remove_from_role.assert_called_once_with(privilege_id, role_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_privilege_hierarchy(self, mock_uow, mock_privilege_repository):
        """Test getting privilege hierarchy."""
        mock_hierarchy = {
            "admin": ["read_data", "write_data", "delete_data"],
            "editor": ["read_data", "write_data"],
            "viewer": ["read_data"]
        }
        mock_privilege_repository.get_privilege_hierarchy.return_value = mock_hierarchy
        
        with patch("src.services.privilege_service.PrivilegeRepository", return_value=mock_privilege_repository):
            service = PrivilegeService(mock_uow)
            
            result = await service.get_privilege_hierarchy()
            
            assert "admin" in result
            assert len(result["admin"]) == 3
            mock_privilege_repository.get_privilege_hierarchy.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_bulk_assign_privileges_to_role(self, mock_uow, mock_privilege_repository):
        """Test bulk assigning privileges to role."""
        role_id = uuid.uuid4()
        privilege_ids = [uuid.uuid4(), uuid.uuid4()]
        
        with patch("src.services.privilege_service.PrivilegeRepository", return_value=mock_privilege_repository):
            service = PrivilegeService(mock_uow)
            
            result = await service.bulk_assign_to_role(role_id, privilege_ids)
            
            assert result == 2
            mock_privilege_repository.bulk_assign_to_role.assert_called_once_with(role_id, privilege_ids)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_effective_privileges(self, mock_uow, mock_privilege_repository):
        """Test getting effective privileges for user."""
        user_id = uuid.uuid4()
        mock_privileges = [
            {"name": "read_data", "granted_by": "role:admin"},
            {"name": "write_data", "granted_by": "role:editor"}
        ]
        mock_privilege_repository.get_effective_privileges.return_value = mock_privileges
        
        with patch("src.services.privilege_service.PrivilegeRepository", return_value=mock_privilege_repository):
            service = PrivilegeService(mock_uow)
            
            result = await service.get_effective_privileges(user_id)
            
            assert len(result) == 2
            assert result[0]["name"] == "read_data"
            mock_privilege_repository.get_effective_privileges.assert_called_once_with(user_id)
    
    @pytest.mark.asyncio
    async def test_validate_privilege_actions(self, mock_uow):
        """Test validation of privilege actions."""
        service = PrivilegeService(mock_uow)
        
        # Test valid actions
        valid_actions = ["read", "write", "delete", "execute", "admin"]
        service._validate_actions(valid_actions)  # Should not raise
        
        # Test invalid actions
        invalid_actions = ["invalid_action"]
        
        with pytest.raises(ValueError, match="Invalid action"):
            service._validate_actions(invalid_actions)
    
    @pytest.mark.asyncio
    async def test_validate_resource_type(self, mock_uow):
        """Test validation of resource type."""
        service = PrivilegeService(mock_uow)
        
        # Test valid resource types
        valid_resource_types = ["data", "model", "flow", "agent", "task", "tool"]
        for resource_type in valid_resource_types:
            service._validate_resource_type(resource_type)  # Should not raise
        
        # Test invalid resource type
        invalid_resource_type = "invalid_resource"
        
        with pytest.raises(ValueError, match="Invalid resource type"):
            service._validate_resource_type(invalid_resource_type)
    
    @pytest.mark.asyncio
    async def test_duplicate_privilege_name(self, mock_uow, mock_privilege_repository, privilege_create_data):
        """Test creating privilege with duplicate name."""
        mock_privilege_repository.get_by_name.return_value = MagicMock()  # Existing privilege
        mock_privilege_repository.create.side_effect = ValueError("Privilege name already exists")
        
        with patch("src.services.privilege_service.PrivilegeRepository", return_value=mock_privilege_repository):
            service = PrivilegeService(mock_uow)
            
            with pytest.raises(ValueError, match="Privilege name already exists"):
                await service.create(privilege_create_data)
    
    @pytest.mark.asyncio
    async def test_privilege_inheritance_check(self, mock_uow, mock_privilege_repository):
        """Test privilege inheritance through role hierarchy."""
        user_id = uuid.uuid4()
        privilege_name = "read_data"
        
        # Mock that user has privilege through inheritance
        mock_privilege_repository.check_inherited_privilege.return_value = True
        
        with patch("src.services.privilege_service.PrivilegeRepository", return_value=mock_privilege_repository):
            service = PrivilegeService(mock_uow)
            
            result = await service.check_inherited_privilege(user_id, privilege_name)
            
            assert result is True
            mock_privilege_repository.check_inherited_privilege.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_privilege_scope_validation(self, mock_uow):
        """Test privilege scope validation."""
        service = PrivilegeService(mock_uow)
        
        # Test valid scopes
        valid_scopes = ["global", "tenant", "group", "user"]
        for scope in valid_scopes:
            service._validate_scope(scope)  # Should not raise
        
        # Test invalid scope
        invalid_scope = "invalid_scope"
        
        with pytest.raises(ValueError, match="Invalid scope"):
            service._validate_scope(invalid_scope)
    
    @pytest.mark.asyncio
    async def test_privilege_dependency_check(self, mock_uow, mock_privilege_repository):
        """Test checking privilege dependencies before deletion."""
        privilege_id = uuid.uuid4()
        
        # Mock that privilege has dependencies
        mock_privilege_repository.has_dependencies.return_value = True
        
        with patch("src.services.privilege_service.PrivilegeRepository", return_value=mock_privilege_repository):
            service = PrivilegeService(mock_uow)
            
            with pytest.raises(ValueError, match="Cannot delete privilege with dependencies"):
                await service.delete(privilege_id)
    
    @pytest.mark.asyncio
    async def test_privilege_audit_logging(self, mock_uow, mock_privilege_repository, privilege_create_data):
        """Test that privilege operations are audited."""
        with patch("src.services.privilege_service.PrivilegeRepository", return_value=mock_privilege_repository), \
             patch("src.services.privilege_service.audit_logger") as mock_audit:
            
            service = PrivilegeService(mock_uow)
            
            await service.create(privilege_create_data)
            
            mock_audit.log_privilege_creation.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_privilege_caching(self, mock_uow, mock_privilege_repository):
        """Test privilege caching for performance."""
        user_id = uuid.uuid4()
        privilege_name = "read_data"
        
        with patch("src.services.privilege_service.PrivilegeRepository", return_value=mock_privilege_repository), \
             patch("src.services.privilege_service.cache") as mock_cache:
            
            # First call - not cached
            mock_cache.get.return_value = None
            mock_privilege_repository.check_user_privilege.return_value = True
            
            service = PrivilegeService(mock_uow)
            
            result = await service.check_user_privilege(user_id, privilege_name)
            
            assert result is True
            mock_cache.set.assert_called_once()
            
            # Second call - cached
            mock_cache.get.return_value = True
            
            result = await service.check_user_privilege(user_id, privilege_name)
            
            assert result is True
            # Repository should not be called again
            assert mock_privilege_repository.check_user_privilege.call_count == 1
    
    @pytest.mark.asyncio
    async def test_privilege_metrics(self, mock_uow, mock_privilege_repository):
        """Test getting privilege metrics."""
        with patch("src.services.privilege_service.PrivilegeRepository", return_value=mock_privilege_repository):
            service = PrivilegeService(mock_uow)
            
            metrics = await service.get_privilege_metrics()
            
            assert "total_privileges" in metrics
            assert "privileges_by_resource_type" in metrics
            assert "most_used_privileges" in metrics
            mock_privilege_repository.get_metrics.assert_called_once()