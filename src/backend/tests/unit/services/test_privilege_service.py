"""
Unit tests for PrivilegeService.

Tests the functionality of privilege operations including
privilege CRUD operations, name uniqueness validation, and role dependency checks.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock

from src.services.privilege_service import PrivilegeService
from src.schemas.user import PrivilegeCreate, PrivilegeUpdate


# Mock models
class MockPrivilege:
    def __init__(self, id="priv-123", name="test_privilege", description="Test privilege description"):
        self.id = id
        self.name = name
        self.description = description


@pytest.fixture
def mock_session():
    """Create a mock async session."""
    return AsyncMock()


@pytest.fixture
def mock_privilege_repo():
    """Create a mock PrivilegeRepository."""
    return AsyncMock()


@pytest.fixture
def privilege_service(mock_session):
    """Create a PrivilegeService instance with mock session."""
    from unittest.mock import patch
    with patch('src.services.privilege_service.PrivilegeRepository') as MockRepository:
        mock_repo = AsyncMock()
        MockRepository.return_value = mock_repo
        
        service = PrivilegeService(mock_session)
        service.privilege_repo = mock_repo
        return service


@pytest.fixture
def mock_privilege():
    """Create a mock privilege."""
    return MockPrivilege()


@pytest.fixture
def privilege_create_data():
    """Create sample PrivilegeCreate data."""
    return {
        "name": "create_user",
        "description": "Permission to create new users"
    }


@pytest.fixture
def privilege_update_data():
    """Create sample PrivilegeUpdate data."""
    return {
        "description": "Permission to update existing users"
    }


class TestPrivilegeService:
    """Test cases for PrivilegeService."""
    
    def test_privilege_service_initialization(self, privilege_service, mock_session):
        """Test PrivilegeService initialization."""
        assert privilege_service.session == mock_session
        assert privilege_service.privilege_repo is not None
    
    @pytest.mark.asyncio
    async def test_get_privilege_success(self, privilege_service, mock_privilege):
        """Test successful privilege retrieval by ID."""
        privilege_service.privilege_repo.get.return_value = mock_privilege
        
        result = await privilege_service.get_privilege("priv-123")
        
        assert result == mock_privilege
        privilege_service.privilege_repo.get.assert_called_once_with("priv-123")
    
    @pytest.mark.asyncio
    async def test_get_privilege_not_found(self, privilege_service):
        """Test privilege retrieval when privilege not found."""
        privilege_service.privilege_repo.get.return_value = None
        
        result = await privilege_service.get_privilege("nonexistent")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_privilege_by_name_success(self, privilege_service, mock_privilege):
        """Test successful privilege retrieval by name."""
        privilege_service.privilege_repo.get_by_name.return_value = mock_privilege
        
        result = await privilege_service.get_privilege_by_name("test_privilege")
        
        assert result == mock_privilege
        privilege_service.privilege_repo.get_by_name.assert_called_once_with("test_privilege")
    
    @pytest.mark.asyncio
    async def test_get_privilege_by_name_not_found(self, privilege_service):
        """Test privilege retrieval by name when privilege not found."""
        privilege_service.privilege_repo.get_by_name.return_value = None
        
        result = await privilege_service.get_privilege_by_name("nonexistent")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_privileges_success(self, privilege_service):
        """Test successful privileges list retrieval."""
        mock_privileges = [MockPrivilege(id="1", name="priv1"), MockPrivilege(id="2", name="priv2")]
        privilege_service.privilege_repo.list.return_value = mock_privileges
        
        result = await privilege_service.get_privileges(skip=0, limit=10)
        
        assert result == mock_privileges
        assert len(result) == 2
        privilege_service.privilege_repo.list.assert_called_once_with(skip=0, limit=10)
    
    @pytest.mark.asyncio
    async def test_get_privileges_default_params(self, privilege_service):
        """Test privileges list retrieval with default parameters."""
        mock_privileges = []
        privilege_service.privilege_repo.list.return_value = mock_privileges
        
        result = await privilege_service.get_privileges()
        
        assert result == mock_privileges
        privilege_service.privilege_repo.list.assert_called_once_with(skip=0, limit=100)
    
    @pytest.mark.asyncio
    async def test_create_privilege_success(self, privilege_service, privilege_create_data, mock_privilege):
        """Test successful privilege creation."""
        privilege_service.privilege_repo.get_by_name.return_value = None  # No existing privilege
        privilege_service.privilege_repo.create.return_value = mock_privilege
        
        privilege_create = PrivilegeCreate(**privilege_create_data)
        result = await privilege_service.create_privilege(privilege_create)
        
        assert result == mock_privilege
        privilege_service.privilege_repo.get_by_name.assert_called_once_with(privilege_create_data["name"])
        privilege_service.privilege_repo.create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_privilege_duplicate_name(self, privilege_service, privilege_create_data, mock_privilege):
        """Test privilege creation with duplicate name."""
        privilege_service.privilege_repo.get_by_name.return_value = mock_privilege  # Existing privilege
        
        privilege_create = PrivilegeCreate(**privilege_create_data)
        
        with pytest.raises(ValueError) as exc_info:
            await privilege_service.create_privilege(privilege_create)
        
        assert "already exists" in str(exc_info.value)
        assert privilege_create_data["name"] in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_update_privilege_success(self, privilege_service, privilege_update_data, mock_privilege):
        """Test successful privilege update."""
        privilege_service.privilege_repo.get.side_effect = [mock_privilege, mock_privilege]  # exists, then updated
        privilege_service.privilege_repo.update.return_value = None
        
        privilege_update = PrivilegeUpdate(**privilege_update_data)
        result = await privilege_service.update_privilege("priv-123", privilege_update)
        
        assert result == mock_privilege
        privilege_service.privilege_repo.update.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_privilege_not_found(self, privilege_service, privilege_update_data):
        """Test privilege update when privilege not found."""
        privilege_service.privilege_repo.get.return_value = None
        
        privilege_update = PrivilegeUpdate(**privilege_update_data)
        result = await privilege_service.update_privilege("nonexistent", privilege_update)
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_update_privilege_duplicate_name(self, privilege_service, mock_privilege):
        """Test privilege update - this test is not applicable since PrivilegeUpdate doesn't have name field."""
        # Since PrivilegeUpdate schema doesn't have a name field, this test scenario doesn't apply
        # Instead, test that update works correctly with just description
        privilege_service.privilege_repo.get.side_effect = [mock_privilege, mock_privilege]
        privilege_service.privilege_repo.update.return_value = None
        
        privilege_update = PrivilegeUpdate(description="Updated description")
        result = await privilege_service.update_privilege("priv-123", privilege_update)
        
        assert result == mock_privilege
    
    @pytest.mark.asyncio
    async def test_update_privilege_same_name(self, privilege_service, mock_privilege):
        """Test privilege update - this test is not applicable since PrivilegeUpdate doesn't have name field."""
        # Since PrivilegeUpdate schema doesn't have a name field, test description-only update
        privilege_service.privilege_repo.get.side_effect = [mock_privilege, mock_privilege]
        privilege_service.privilege_repo.update.return_value = None
        
        privilege_update = PrivilegeUpdate(description="Updated description")
        result = await privilege_service.update_privilege("priv-123", privilege_update)
        
        assert result == mock_privilege
    
    @pytest.mark.asyncio
    async def test_update_privilege_partial_update(self, privilege_service, mock_privilege):
        """Test privilege update with only description change."""
        privilege_service.privilege_repo.get.side_effect = [mock_privilege, mock_privilege]
        privilege_service.privilege_repo.update.return_value = None
        
        privilege_update = PrivilegeUpdate(description="New description only")
        result = await privilege_service.update_privilege("priv-123", privilege_update)
        
        assert result == mock_privilege
        privilege_service.privilege_repo.update.assert_called_once()
        
        # Verify update was called with correct data
        call_args = privilege_service.privilege_repo.update.call_args
        update_data = call_args[0][1]
        assert "description" in update_data
    
    @pytest.mark.asyncio
    async def test_update_privilege_no_changes(self, privilege_service, mock_privilege):
        """Test privilege update with no actual changes."""
        privilege_service.privilege_repo.get.side_effect = [mock_privilege, mock_privilege]
        
        privilege_update = PrivilegeUpdate()  # No fields set
        result = await privilege_service.update_privilege("priv-123", privilege_update)
        
        assert result == mock_privilege
        # Should not call update if no changes
        privilege_service.privilege_repo.update.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_delete_privilege_success(self, privilege_service, mock_privilege):
        """Test successful privilege deletion."""
        privilege_service.privilege_repo.get.return_value = mock_privilege
        privilege_service.privilege_repo.get_role_privileges_by_privilege.return_value = []  # No dependencies
        privilege_service.privilege_repo.delete.return_value = None
        
        result = await privilege_service.delete_privilege("priv-123")
        
        assert result is True
        privilege_service.privilege_repo.delete.assert_called_once_with("priv-123")
    
    @pytest.mark.asyncio
    async def test_delete_privilege_not_found(self, privilege_service):
        """Test privilege deletion when privilege not found."""
        privilege_service.privilege_repo.get.return_value = None
        
        result = await privilege_service.delete_privilege("nonexistent")
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_delete_privilege_in_use(self, privilege_service, mock_privilege):
        """Test privilege deletion when privilege is in use by roles."""
        privilege_service.privilege_repo.get.return_value = mock_privilege
        privilege_service.privilege_repo.get_role_privileges_by_privilege.return_value = ["role1", "role2"]  # Has dependencies
        
        with pytest.raises(ValueError) as exc_info:
            await privilege_service.delete_privilege("priv-123")
        
        assert "Cannot delete privilege" in str(exc_info.value)
        assert "being used by 2 roles" in str(exc_info.value)
    
    def test_method_existence(self, privilege_service):
        """Test that all expected methods exist."""
        expected_methods = [
            'get_privilege',
            'get_privilege_by_name',
            'get_privileges',
            'create_privilege',
            'update_privilege',
            'delete_privilege'
        ]
        
        for method_name in expected_methods:
            assert hasattr(privilege_service, method_name)
            assert callable(getattr(privilege_service, method_name))
    
    @pytest.mark.asyncio
    async def test_privilege_create_data_validation(self, privilege_service):
        """Test that PrivilegeCreate data is properly validated."""
        # Test with valid data
        valid_data = {"name": "valid_privilege", "description": "Valid description"}
        privilege_create = PrivilegeCreate(**valid_data)
        
        assert privilege_create.name == "valid_privilege"
        assert privilege_create.description == "Valid description"
    
    @pytest.mark.asyncio
    async def test_privilege_update_data_validation(self, privilege_service):
        """Test that PrivilegeUpdate data is properly validated."""
        # Test with partial data
        update_data = {"description": "Updated description"}
        privilege_update = PrivilegeUpdate(**update_data)
        
        assert privilege_update.description == "Updated description"
    
    @pytest.mark.asyncio
    async def test_repository_integration(self, privilege_service):
        """Test that service properly integrates with repository."""
        # Test that repository methods are called correctly
        privilege_service.privilege_repo.list.return_value = []
        
        await privilege_service.get_privileges(skip=5, limit=20)
        
        privilege_service.privilege_repo.list.assert_called_once_with(skip=5, limit=20)
    
    @pytest.mark.asyncio
    async def test_error_handling_in_create(self, privilege_service, privilege_create_data):
        """Test error handling during privilege creation."""
        privilege_service.privilege_repo.get_by_name.return_value = None
        privilege_service.privilege_repo.create.side_effect = Exception("Database error")
        
        privilege_create = PrivilegeCreate(**privilege_create_data)
        
        with pytest.raises(Exception) as exc_info:
            await privilege_service.create_privilege(privilege_create)
        
        assert "Database error" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_error_handling_in_update(self, privilege_service, privilege_update_data, mock_privilege):
        """Test error handling during privilege update."""
        privilege_service.privilege_repo.get.return_value = mock_privilege
        privilege_service.privilege_repo.update.side_effect = Exception("Update failed")
        
        privilege_update = PrivilegeUpdate(**privilege_update_data)
        
        with pytest.raises(Exception) as exc_info:
            await privilege_service.update_privilege("priv-123", privilege_update)
        
        assert "Update failed" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_error_handling_in_delete(self, privilege_service, mock_privilege):
        """Test error handling during privilege deletion."""
        privilege_service.privilege_repo.get.return_value = mock_privilege
        privilege_service.privilege_repo.get_role_privileges_by_privilege.return_value = []
        privilege_service.privilege_repo.delete.side_effect = Exception("Delete failed")
        
        with pytest.raises(Exception) as exc_info:
            await privilege_service.delete_privilege("priv-123")
        
        assert "Delete failed" in str(exc_info.value)