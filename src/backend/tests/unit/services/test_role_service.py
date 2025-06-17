"""
Unit tests for RoleService.

Tests the functionality of role management operations including
role CRUD operations, privilege management, and role-privilege assignments.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime

from src.services.role_service import RoleService
from src.models.user import Role, Privilege, RolePrivilege
from src.schemas.user import RoleCreate, RoleUpdate


# Mock models
class MockRole:
    def __init__(self, id="role-123", name="test_role", description="Test Role", 
                 created_at=None, updated_at=None, privileges=None):
        self.id = id
        self.name = name
        self.description = description
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()
        self.privileges = privileges or []


class MockPrivilege:
    def __init__(self, id="priv-123", name="users:read", description="Read users",
                 created_at=None):
        self.id = id
        self.name = name
        self.description = description
        self.created_at = created_at or datetime.utcnow()


class MockRolePrivilege:
    def __init__(self, id="rp-123", role_id="role-123", privilege_id="priv-123"):
        self.id = id
        self.role_id = role_id
        self.privilege_id = privilege_id


@pytest.fixture
def mock_session():
    """Create a mock async session."""
    return AsyncMock()


@pytest.fixture
def role_service(mock_session):
    """Create a RoleService instance with mock session."""
    service = RoleService(mock_session)
    service.role_repo = AsyncMock()
    service.privilege_repo = AsyncMock()
    service.role_privilege_repo = AsyncMock()
    return service


@pytest.fixture
def mock_role():
    """Create a mock role."""
    return MockRole()


@pytest.fixture
def mock_privilege():
    """Create a mock privilege."""
    return MockPrivilege()


@pytest.fixture
def mock_role_privilege():
    """Create a mock role-privilege mapping."""
    return MockRolePrivilege()


@pytest.fixture
def mock_role_with_privileges():
    """Create a mock role with privileges."""
    privilege1 = MockPrivilege(id="priv-1", name="users:read")
    privilege2 = MockPrivilege(id="priv-2", name="users:write")
    return MockRole(privileges=[privilege1, privilege2])


class TestRoleService:
    """Test cases for RoleService."""
    
    @pytest.mark.asyncio
    async def test_get_role_success(self, role_service, mock_role):
        """Test successful role retrieval."""
        role_service.role_repo.get.return_value = mock_role
        
        result = await role_service.get_role("role-123")
        
        assert result == mock_role
        role_service.role_repo.get.assert_called_once_with("role-123")
    
    @pytest.mark.asyncio
    async def test_get_role_not_found(self, role_service):
        """Test role retrieval when role not found."""
        role_service.role_repo.get.return_value = None
        
        result = await role_service.get_role("nonexistent")
        
        assert result is None
        role_service.role_repo.get.assert_called_once_with("nonexistent")
    
    @pytest.mark.asyncio
    async def test_get_role_with_privileges_success(self, role_service, mock_role_with_privileges):
        """Test successful role retrieval with privileges."""
        role_service.role_repo.get_with_privileges.return_value = mock_role_with_privileges
        
        result = await role_service.get_role_with_privileges("role-123")
        
        assert result is not None
        assert result["id"] == "role-123"
        assert result["name"] == "test_role"
        assert result["description"] == "Test Role"
        assert len(result["privileges"]) == 2
        assert result["privileges"][0].name == "users:read"
        assert result["privileges"][1].name == "users:write"
        
        role_service.role_repo.get_with_privileges.assert_called_once_with("role-123")
    
    @pytest.mark.asyncio
    async def test_get_role_with_privileges_not_found(self, role_service):
        """Test role with privileges retrieval when role not found."""
        role_service.role_repo.get_with_privileges.return_value = None
        
        result = await role_service.get_role_with_privileges("nonexistent")
        
        assert result is None
        role_service.role_repo.get_with_privileges.assert_called_once_with("nonexistent")
    
    @pytest.mark.asyncio
    async def test_get_roles_success(self, role_service):
        """Test successful roles listing."""
        mock_roles = [
            MockRole(id="role-1", name="admin"),
            MockRole(id="role-2", name="user")
        ]
        role_service.role_repo.list.return_value = mock_roles
        
        result = await role_service.get_roles(skip=0, limit=10)
        
        assert len(result) == 2
        assert result == mock_roles
        role_service.role_repo.list.assert_called_once_with(skip=0, limit=10)
    
    @pytest.mark.asyncio
    async def test_get_roles_with_pagination(self, role_service):
        """Test roles listing with pagination."""
        mock_roles = [MockRole(id="role-1", name="admin")]
        role_service.role_repo.list.return_value = mock_roles
        
        result = await role_service.get_roles(skip=10, limit=5)
        
        assert len(result) == 1
        role_service.role_repo.list.assert_called_once_with(skip=10, limit=5)
    
    @pytest.mark.asyncio
    async def test_create_role_success(self, role_service, mock_role):
        """Test successful role creation."""
        role_data = RoleCreate(
            name="new_role",
            description="New Role",
            privileges=["users:read", "users:write"]
        )
        
        privilege1 = MockPrivilege(id="priv-1", name="users:read")
        privilege2 = MockPrivilege(id="priv-2", name="users:write")
        
        # Mock repository calls
        role_service.role_repo.get_by_name.return_value = None
        role_service.role_repo.create.return_value = mock_role
        role_service.privilege_repo.get_by_name.side_effect = [privilege1, privilege2]
        role_service.role_privilege_repo.create.return_value = None
        
        result = await role_service.create_role(role_data)
        
        assert result is not None
        assert result["id"] == "role-123"
        assert result["name"] == "test_role"
        assert len(result["privileges"]) == 2
        
        # Verify repository calls
        role_service.role_repo.get_by_name.assert_called_once_with("new_role")
        role_service.role_repo.create.assert_called_once()
        assert role_service.privilege_repo.get_by_name.call_count == 2
        assert role_service.role_privilege_repo.create.call_count == 2
    
    @pytest.mark.asyncio
    async def test_create_role_name_already_exists(self, role_service, mock_role):
        """Test role creation when name already exists."""
        role_data = RoleCreate(
            name="existing_role",
            description="Existing Role",
            privileges=["users:read"]
        )
        
        role_service.role_repo.get_by_name.return_value = mock_role
        
        with pytest.raises(ValueError, match="Role with name 'existing_role' already exists"):
            await role_service.create_role(role_data)
        
        role_service.role_repo.create.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_create_role_with_new_privileges(self, role_service, mock_role):
        """Test role creation with privileges that don't exist yet."""
        role_data = RoleCreate(
            name="new_role",
            description="New Role",
            privileges=["new:privilege"]
        )
        
        new_privilege = MockPrivilege(id="priv-new", name="new:privilege")
        
        # Mock repository calls
        role_service.role_repo.get_by_name.return_value = None
        role_service.role_repo.create.return_value = mock_role
        role_service.privilege_repo.get_by_name.return_value = None
        role_service.privilege_repo.create.return_value = new_privilege
        role_service.role_privilege_repo.create.return_value = None
        
        result = await role_service.create_role(role_data)
        
        assert result is not None
        
        # Verify new privilege was created
        role_service.privilege_repo.create.assert_called_once_with({"name": "new:privilege"})
        role_service.role_privilege_repo.create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_role_without_privileges(self, role_service, mock_role):
        """Test role creation without privileges."""
        role_data = RoleCreate(
            name="simple_role",
            description="Simple Role",
            privileges=[]
        )
        
        role_service.role_repo.get_by_name.return_value = None
        role_service.role_repo.create.return_value = mock_role
        
        result = await role_service.create_role(role_data)
        
        assert result is not None
        assert len(result["privileges"]) == 0
        
        # No privilege operations should be called
        role_service.privilege_repo.get_by_name.assert_not_called()
        role_service.role_privilege_repo.create.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_update_role_success(self, role_service, mock_role, mock_role_with_privileges):
        """Test successful role update."""
        role_data = RoleUpdate(
            name="updated_role",
            description="Updated Role",
            privileges=["users:read"]
        )
        
        privilege = MockPrivilege(id="priv-1", name="users:read")
        
        # Mock repository calls
        role_service.role_repo.get.return_value = mock_role
        role_service.role_repo.get_by_name.return_value = None
        role_service.role_repo.update.return_value = None
        role_service.role_privilege_repo.delete_by_role_id.return_value = None
        role_service.privilege_repo.get_by_name.return_value = privilege
        role_service.role_privilege_repo.create.return_value = None
        role_service.get_role_with_privileges = AsyncMock(return_value=mock_role_with_privileges.__dict__)
        
        result = await role_service.update_role("role-123", role_data)
        
        assert result is not None
        
        # Verify repository calls
        role_service.role_repo.get.assert_called_once_with("role-123")
        role_service.role_repo.update.assert_called_once()
        role_service.role_privilege_repo.delete_by_role_id.assert_called_once_with("role-123")
        role_service.role_privilege_repo.create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_role_not_found(self, role_service):
        """Test role update when role not found."""
        role_data = RoleUpdate(name="updated_role")
        
        role_service.role_repo.get.return_value = None
        
        result = await role_service.update_role("nonexistent", role_data)
        
        assert result is None
        role_service.role_repo.update.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_update_role_name_already_exists(self, role_service, mock_role):
        """Test role update when new name already exists."""
        role_data = RoleUpdate(name="existing_role")
        
        existing_role = MockRole(id="other-role", name="existing_role")
        
        role_service.role_repo.get.return_value = mock_role
        role_service.role_repo.get_by_name.return_value = existing_role
        
        with pytest.raises(ValueError, match="Role with name 'existing_role' already exists"):
            await role_service.update_role("role-123", role_data)
        
        role_service.role_repo.update.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_update_role_partial_update(self, role_service, mock_role):
        """Test role update with only some fields."""
        role_data = RoleUpdate(description="Updated description only")
        
        role_service.role_repo.get.return_value = mock_role
        role_service.role_repo.update.return_value = None
        role_service.get_role_with_privileges = AsyncMock(return_value={"id": "role-123"})
        
        result = await role_service.update_role("role-123", role_data)
        
        assert result is not None
        
        # Verify only description was updated
        role_service.role_repo.update.assert_called_once_with(
            "role-123", {"description": "Updated description only"}
        )
    
    @pytest.mark.asyncio
    async def test_update_role_with_missing_privilege(self, role_service, mock_role):
        """Test role update with privilege that needs to be created."""
        role_data = RoleUpdate(privileges=["new:privilege"])
        new_privilege = MockPrivilege(id="new-priv", name="new:privilege")
        
        role_service.role_repo.get.return_value = mock_role
        role_service.role_repo.update.return_value = None
        role_service.role_privilege_repo.delete_by_role_id.return_value = None
        role_service.privilege_repo.get_by_name.return_value = None  # Privilege doesn't exist
        role_service.privilege_repo.create.return_value = new_privilege  # Create new privilege
        role_service.role_privilege_repo.create.return_value = None
        role_service.get_role_with_privileges = AsyncMock(return_value={"id": "role-123"})
        
        result = await role_service.update_role("role-123", role_data)
        
        assert result is not None
        
        # Verify privilege was created
        role_service.privilege_repo.create.assert_called_once_with({"name": "new:privilege"})
        role_service.role_privilege_repo.create.assert_called_once()
        # Verify old privileges were deleted first
        role_service.role_privilege_repo.delete_by_role_id.assert_called_once_with("role-123")
    
    @pytest.mark.asyncio
    async def test_delete_role_success(self, role_service, mock_role):
        """Test successful role deletion."""
        role_service.role_repo.get.return_value = mock_role
        role_service.role_repo.delete.return_value = None
        
        result = await role_service.delete_role("role-123")
        
        assert result is True
        role_service.role_repo.get.assert_called_once_with("role-123")
        role_service.role_repo.delete.assert_called_once_with("role-123")
    
    @pytest.mark.asyncio
    async def test_delete_role_not_found(self, role_service):
        """Test role deletion when role not found."""
        role_service.role_repo.get.return_value = None
        
        result = await role_service.delete_role("nonexistent")
        
        assert result is False
        role_service.role_repo.delete.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_check_role_has_privilege_success(self, role_service, mock_role_with_privileges):
        """Test checking if role has privilege - success case."""
        privilege = MockPrivilege(id="priv-1", name="users:read")
        
        role_service.privilege_repo.get_by_name.return_value = privilege
        role_service.role_repo.get_with_privileges.return_value = mock_role_with_privileges
        
        result = await role_service.check_role_has_privilege("role-123", "users:read")
        
        assert result is True
        role_service.privilege_repo.get_by_name.assert_called_once_with("users:read")
        role_service.role_repo.get_with_privileges.assert_called_once_with("role-123")
    
    @pytest.mark.asyncio
    async def test_check_role_has_privilege_privilege_not_found(self, role_service):
        """Test checking if role has privilege when privilege doesn't exist."""
        role_service.privilege_repo.get_by_name.return_value = None
        
        result = await role_service.check_role_has_privilege("role-123", "nonexistent:privilege")
        
        assert result is False
        role_service.role_repo.get_with_privileges.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_check_role_has_privilege_role_not_found(self, role_service, mock_privilege):
        """Test checking if role has privilege when role doesn't exist."""
        role_service.privilege_repo.get_by_name.return_value = mock_privilege
        role_service.role_repo.get_with_privileges.return_value = None
        
        result = await role_service.check_role_has_privilege("nonexistent", "users:read")
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_check_role_has_privilege_false(self, role_service, mock_role_with_privileges):
        """Test checking if role has privilege when it doesn't."""
        privilege = MockPrivilege(id="priv-other", name="other:privilege")
        
        role_service.privilege_repo.get_by_name.return_value = privilege
        role_service.role_repo.get_with_privileges.return_value = mock_role_with_privileges
        
        result = await role_service.check_role_has_privilege("role-123", "other:privilege")
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_get_role_privileges_success(self, role_service, mock_role_with_privileges):
        """Test getting role privileges."""
        role_service.role_repo.get_with_privileges.return_value = mock_role_with_privileges
        
        result = await role_service.get_role_privileges("role-123")
        
        assert len(result) == 2
        assert result[0].name == "users:read"
        assert result[1].name == "users:write"
        role_service.role_repo.get_with_privileges.assert_called_once_with("role-123")
    
    @pytest.mark.asyncio
    async def test_get_role_privileges_role_not_found(self, role_service):
        """Test getting role privileges when role not found."""
        role_service.role_repo.get_with_privileges.return_value = None
        
        result = await role_service.get_role_privileges("nonexistent")
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_assign_privilege_to_role_success(self, role_service, mock_role, mock_privilege):
        """Test successful privilege assignment to role."""
        role_service.role_repo.get.return_value = mock_role
        role_service.privilege_repo.get.return_value = mock_privilege
        role_service.role_privilege_repo.get_by_role_and_privilege.return_value = None
        role_service.role_privilege_repo.create.return_value = None
        
        result = await role_service.assign_privilege_to_role("role-123", "priv-123")
        
        assert result is True
        role_service.role_privilege_repo.create.assert_called_once_with({
            "role_id": "role-123",
            "privilege_id": "priv-123"
        })
    
    @pytest.mark.asyncio
    async def test_assign_privilege_to_role_role_not_found(self, role_service):
        """Test privilege assignment when role not found."""
        role_service.role_repo.get.return_value = None
        
        result = await role_service.assign_privilege_to_role("nonexistent", "priv-123")
        
        assert result is False
        role_service.privilege_repo.get.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_assign_privilege_to_role_privilege_not_found(self, role_service, mock_role):
        """Test privilege assignment when privilege not found."""
        role_service.role_repo.get.return_value = mock_role
        role_service.privilege_repo.get.return_value = None
        
        result = await role_service.assign_privilege_to_role("role-123", "nonexistent")
        
        assert result is False
        role_service.role_privilege_repo.get_by_role_and_privilege.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_assign_privilege_to_role_already_assigned(self, role_service, mock_role, mock_privilege, mock_role_privilege):
        """Test privilege assignment when already assigned."""
        role_service.role_repo.get.return_value = mock_role
        role_service.privilege_repo.get.return_value = mock_privilege
        role_service.role_privilege_repo.get_by_role_and_privilege.return_value = mock_role_privilege
        
        result = await role_service.assign_privilege_to_role("role-123", "priv-123")
        
        assert result is True
        role_service.role_privilege_repo.create.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_remove_privilege_from_role_success(self, role_service, mock_role_privilege):
        """Test successful privilege removal from role."""
        role_service.role_privilege_repo.get_by_role_and_privilege.return_value = mock_role_privilege
        role_service.role_privilege_repo.delete.return_value = None
        
        result = await role_service.remove_privilege_from_role("role-123", "priv-123")
        
        assert result is True
        role_service.role_privilege_repo.delete.assert_called_once_with("rp-123")
    
    @pytest.mark.asyncio
    async def test_remove_privilege_from_role_not_assigned(self, role_service):
        """Test privilege removal when not assigned."""
        role_service.role_privilege_repo.get_by_role_and_privilege.return_value = None
        
        result = await role_service.remove_privilege_from_role("role-123", "priv-123")
        
        assert result is True
        role_service.role_privilege_repo.delete.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_get_role_with_privileges_no_privileges_attribute(self, role_service):
        """Test get_role_with_privileges when role has no privileges attribute."""
        # Create role without privileges attribute
        mock_role_no_privs = MockRole()
        delattr(mock_role_no_privs, 'privileges')
        
        role_service.role_repo.get_with_privileges.return_value = mock_role_no_privs
        
        result = await role_service.get_role_with_privileges("role-123")
        
        assert result is not None
        assert result["privileges"] == []
    
    @pytest.mark.asyncio
    async def test_check_role_has_privilege_no_privileges_attribute(self, role_service, mock_privilege):
        """Test check_role_has_privilege when role has no privileges attribute."""
        mock_role_no_privs = MockRole()
        delattr(mock_role_no_privs, 'privileges')
        
        role_service.privilege_repo.get_by_name.return_value = mock_privilege
        role_service.role_repo.get_with_privileges.return_value = mock_role_no_privs
        
        result = await role_service.check_role_has_privilege("role-123", "users:read")
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_get_role_privileges_no_privileges_attribute(self, role_service):
        """Test get_role_privileges when role has no privileges attribute."""
        mock_role_no_privs = MockRole()
        delattr(mock_role_no_privs, 'privileges')
        
        role_service.role_repo.get_with_privileges.return_value = mock_role_no_privs
        
        result = await role_service.get_role_privileges("role-123")
        
        assert result == []