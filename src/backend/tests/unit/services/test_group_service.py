"""
Unit tests for GroupService.

Tests the functionality of group management operations including
group creation, user management, auto-creation, and group operations.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from src.services.group_service import GroupService
from src.models.group import Group, GroupUser
from src.models.enums import GroupStatus, GroupUserRole, GroupUserStatus, UserRole, UserStatus
from src.models.user import User
from src.utils.user_context import GroupContext


# Mock models
class MockGroup:
    def __init__(self, id="group-123", name="Test Group", email_domain="example.com",
                 status=GroupStatus.ACTIVE, description="Test group", auto_created=True,
                 created_by_email="test@example.com", created_at=None, updated_at=None):
        self.id = id
        self.name = name
        self.email_domain = email_domain
        self.status = status
        self.description = description
        self.auto_created = auto_created
        self.created_by_email = created_by_email
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()

    @staticmethod
    def generate_group_id(email_domain, name):
        return f"{email_domain.replace('.', '_')}_{name.replace(' ', '_').lower()}"


class MockGroupUser:
    def __init__(self, id="group-user-123", group_id="group-123", user_id="user-123",
                 role=GroupUserRole.USER, status=GroupUserStatus.ACTIVE,
                 joined_at=None, auto_created=True, created_at=None, updated_at=None,
                 group=None, user=None):
        self.id = id
        self.group_id = group_id
        self.user_id = user_id
        self.role = role
        self.status = status
        self.joined_at = joined_at or datetime.utcnow()
        self.auto_created = auto_created
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()
        self.group = group or MockGroup()
        self.user = user


class MockUser:
    def __init__(self, id="user-123", username="testuser", email="test@example.com",
                 role=UserRole.REGULAR, status=UserStatus.ACTIVE,
                 created_at=None, updated_at=None):
        self.id = id
        self.username = username
        self.email = email
        self.role = role
        self.status = status
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()


@pytest.fixture
def mock_session():
    """Create a mock async session."""
    return AsyncMock()


@pytest.fixture
def group_service(mock_session):
    """Create a GroupService instance with mock session."""
    service = GroupService(mock_session)
    service.group_repo = AsyncMock()
    service.group_user_repo = AsyncMock()
    return service


@pytest.fixture
def mock_group():
    """Create a mock group."""
    return MockGroup()


@pytest.fixture
def mock_group_user():
    """Create a mock group user."""
    return MockGroupUser()


@pytest.fixture
def mock_user():
    """Create a mock user."""
    return MockUser()


@pytest.fixture
def group_context():
    """Create a mock group context."""
    return GroupContext(
        group_ids=["group-123"],
        group_email="test@example.com",
        email_domain="example.com"
    )


class TestGroupService:
    """Test cases for GroupService."""
    
    @pytest.mark.asyncio
    async def test_ensure_group_exists_existing_group(self, group_service, group_context, mock_group):
        """Test ensure_group_exists with existing group."""
        group_service.group_repo.get.return_value = mock_group
        
        result = await group_service.ensure_group_exists(group_context)
        
        assert result == mock_group
        group_service.group_repo.get.assert_called_once_with("group-123")
        group_service.group_repo.add.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_ensure_group_exists_auto_create(self, group_service, group_context, mock_group):
        """Test ensure_group_exists with auto-creation."""
        group_service.group_repo.get.return_value = None
        group_service.group_repo.add.return_value = mock_group
        
        with patch('src.services.group_service.Group') as MockGroupClass:
            MockGroupClass.return_value = mock_group
            
            result = await group_service.ensure_group_exists(group_context)
            
            assert result == mock_group
            group_service.group_repo.add.assert_called_once()
            MockGroupClass.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_ensure_group_exists_missing_context(self, group_service):
        """Test ensure_group_exists with missing context."""
        empty_context = GroupContext()
        
        result = await group_service.ensure_group_exists(empty_context)
        
        assert result is None
        group_service.group_repo.get.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_ensure_group_user_exists_existing(self, group_service, group_context, mock_group_user):
        """Test ensure_group_user_exists with existing association."""
        group_service.group_user_repo.get_by_group_and_user.return_value = mock_group_user
        
        result = await group_service.ensure_group_user_exists(group_context, "user-123")
        
        assert result == mock_group_user
        group_service.group_user_repo.get_by_group_and_user.assert_called_once_with("group-123", "user-123")
        group_service.group_user_repo.add.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_ensure_group_user_exists_auto_create(self, group_service, group_context, mock_group_user):
        """Test ensure_group_user_exists with auto-creation."""
        group_service.group_user_repo.get_by_group_and_user.return_value = None
        group_service.group_user_repo.add.return_value = mock_group_user
        
        with patch('src.services.group_service.GroupUser') as MockGroupUserClass:
            MockGroupUserClass.return_value = mock_group_user
            
            result = await group_service.ensure_group_user_exists(group_context, "user-123")
            
            assert result == mock_group_user
            group_service.group_user_repo.add.assert_called_once()
            MockGroupUserClass.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_ensure_group_user_exists_missing_context(self, group_service):
        """Test ensure_group_user_exists with missing context."""
        empty_context = GroupContext()
        
        result = await group_service.ensure_group_user_exists(empty_context, "user-123")
        
        assert result is None
        group_service.group_user_repo.get_by_group_and_user.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_get_group_by_email_domain(self, group_service, mock_group):
        """Test getting group by email domain."""
        group_service.group_repo.get_by_email_domain.return_value = mock_group
        
        result = await group_service.get_group_by_email_domain("example.com")
        
        assert result == mock_group
        group_service.group_repo.get_by_email_domain.assert_called_once_with("example.com")
    
    @pytest.mark.asyncio
    async def test_get_user_groups(self, group_service):
        """Test getting user groups."""
        mock_group_users = [
            MockGroupUser(group=MockGroup(status=GroupStatus.ACTIVE), status=GroupUserStatus.ACTIVE),
            MockGroupUser(group=MockGroup(status=GroupStatus.SUSPENDED), status=GroupUserStatus.ACTIVE)
        ]
        group_service.group_user_repo.get_groups_by_user.return_value = mock_group_users
        
        result = await group_service.get_user_groups("user-123")
        
        # Should only return active groups (suspended groups are filtered out)
        assert len(result) == 1
        assert result[0].status == GroupStatus.ACTIVE
        group_service.group_user_repo.get_groups_by_user.assert_called_once_with("user-123")
    
    @pytest.mark.asyncio
    async def test_get_user_group_memberships_existing_user(self, group_service, mock_user):
        """Test getting user group memberships by email for existing user."""
        # Mock database query to return user
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_user
        group_service.session.execute = AsyncMock(return_value=mock_result)
        
        # Mock get_user_groups
        mock_groups = [MockGroup()]
        with patch.object(group_service, 'get_user_groups', new_callable=AsyncMock, return_value=mock_groups):
            result = await group_service.get_user_group_memberships("test@example.com")
            
            assert result == mock_groups
    
    @pytest.mark.asyncio
    async def test_get_user_group_memberships_nonexistent_user(self, group_service):
        """Test getting user group memberships for nonexistent user."""
        # Mock database query to return None
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        group_service.session.execute = AsyncMock(return_value=mock_result)
        
        result = await group_service.get_user_group_memberships("nonexistent@example.com")
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_create_group(self, group_service, mock_group):
        """Test manual group creation."""
        group_service.group_repo.add.return_value = mock_group
        
        with patch('src.services.group_service.Group') as MockGroupClass:
            MockGroupClass.return_value = mock_group
            MockGroupClass.generate_group_id.return_value = "example_com_test_group"
            
            result = await group_service.create_group(
                name="Test Group",
                email_domain="example.com",
                description="Test description",
                created_by_email="admin@example.com"
            )
            
            assert result == mock_group
            group_service.group_repo.add.assert_called_once()
            MockGroupClass.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_groups(self, group_service):
        """Test listing groups with user counts."""
        mock_groups_data = [
            {"id": "group-1", "name": "Group 1", "user_count": 5},
            {"id": "group-2", "name": "Group 2", "user_count": 3}
        ]
        group_service.group_repo.list_with_user_counts.return_value = mock_groups_data
        
        result = await group_service.list_groups(skip=0, limit=10)
        
        assert result == mock_groups_data
        group_service.group_repo.list_with_user_counts.assert_called_once_with(0, 10)
    
    @pytest.mark.asyncio
    async def test_get_group_by_id(self, group_service, mock_group):
        """Test getting group by ID."""
        group_service.group_repo.get.return_value = mock_group
        
        result = await group_service.get_group_by_id("group-123")
        
        assert result == mock_group
        group_service.group_repo.get.assert_called_once_with("group-123")
    
    @pytest.mark.asyncio
    async def test_update_group_success(self, group_service, mock_group):
        """Test successful group update."""
        group_service.group_repo.get.return_value = mock_group
        group_service.group_repo.update.return_value = mock_group
        
        result = await group_service.update_group("group-123", name="Updated Name", description="Updated desc")
        
        assert result == mock_group
        group_service.group_repo.get.assert_called_once_with("group-123")
        group_service.group_repo.update.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_group_not_found(self, group_service):
        """Test group update when group not found."""
        group_service.group_repo.get.return_value = None
        
        with pytest.raises(ValueError, match="Group group-123 not found"):
            await group_service.update_group("group-123", name="Updated Name")
    
    @pytest.mark.asyncio
    async def test_get_group_user_count(self, group_service):
        """Test getting group user count."""
        # Mock database query result
        mock_result = MagicMock()
        mock_result.scalar.return_value = 5
        group_service.session.execute = AsyncMock(return_value=mock_result)
        
        result = await group_service.get_group_user_count("group-123")
        
        assert result == 5
        group_service.session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_group_user_count_none(self, group_service):
        """Test getting group user count when result is None."""
        # Mock database query result
        mock_result = MagicMock()
        mock_result.scalar.return_value = None
        group_service.session.execute = AsyncMock(return_value=mock_result)
        
        result = await group_service.get_group_user_count("group-123")
        
        assert result == 0
    
    @pytest.mark.asyncio
    async def test_list_group_users(self, group_service):
        """Test listing group users."""
        mock_group_users = [
            MockGroupUser(user=MockUser(email="user1@example.com")),
            MockGroupUser(user=MockUser(email="user2@example.com")),
            MockGroupUser(user=None, user_id="user-no-email")  # User without email
        ]
        group_service.group_user_repo.get_users_by_group.return_value = mock_group_users
        
        result = await group_service.list_group_users("group-123", skip=0, limit=10)
        
        assert len(result) == 3
        assert result[0]["email"] == "user1@example.com"
        assert result[1]["email"] == "user2@example.com"
        assert result[2]["email"] == "user-no-email@databricks.com"
        group_service.group_user_repo.get_users_by_group.assert_called_once_with("group-123", 0, 10)
    
    @pytest.mark.asyncio
    async def test_assign_user_to_group_new_user(self, group_service, mock_group_user):
        """Test assigning new user to group."""
        # Mock database queries - user doesn't exist initially
        mock_user_result = MagicMock()
        mock_user_result.scalar_one_or_none.return_value = None
        group_service.session.execute = AsyncMock(return_value=mock_user_result)
        group_service.session.add = MagicMock()
        group_service.session.flush = AsyncMock()
        
        group_service.group_user_repo.get_by_group_and_user.return_value = None
        group_service.group_user_repo.add.return_value = mock_group_user
        
        with patch('src.services.group_service.GroupUser') as MockGroupUserClass, \
             patch('uuid.uuid4', return_value="new-user-id"):
            
            MockGroupUserClass.return_value = mock_group_user
            
            result = await group_service.assign_user_to_group(
                "group-123", 
                "newuser@example.com", 
                GroupUserRole.ADMIN,
                "admin@example.com"
            )
            
            assert result["email"] == "newuser@example.com"
            assert result["role"] == mock_group_user.role
            group_service.session.add.assert_called_once()
            group_service.session.flush.assert_called_once()
            group_service.group_user_repo.add.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_assign_user_to_group_existing_user(self, group_service, mock_user, mock_group_user):
        """Test assigning existing user to group."""
        # Mock database queries
        mock_user_result = MagicMock()
        mock_user_result.scalar_one_or_none.return_value = mock_user
        group_service.session.execute = AsyncMock(return_value=mock_user_result)
        
        group_service.group_user_repo.get_by_group_and_user.return_value = None
        group_service.group_user_repo.add.return_value = mock_group_user
        
        with patch('src.services.group_service.GroupUser') as MockGroupUserClass:
            MockGroupUserClass.return_value = mock_group_user
            
            result = await group_service.assign_user_to_group(
                "group-123", 
                "test@example.com", 
                GroupUserRole.USER
            )
            
            assert result["email"] == "test@example.com"
            group_service.session.add.assert_not_called()  # User already exists
            group_service.group_user_repo.add.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_assign_user_to_group_update_existing_association(self, group_service, mock_user, mock_group_user):
        """Test updating existing user-group association."""
        # Mock database queries
        mock_user_result = MagicMock()
        mock_user_result.scalar_one_or_none.return_value = mock_user
        group_service.session.execute = AsyncMock(return_value=mock_user_result)
        
        group_service.group_user_repo.get_by_group_and_user.return_value = mock_group_user
        group_service.group_user_repo.update.return_value = mock_group_user
        
        result = await group_service.assign_user_to_group(
            "group-123", 
            "test@example.com", 
            GroupUserRole.ADMIN
        )
        
        assert result["email"] == "test@example.com"
        group_service.group_user_repo.update.assert_called_once()
        group_service.group_user_repo.add.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_update_group_user_success(self, group_service, mock_group_user):
        """Test successful group user update."""
        group_service.group_user_repo.get_by_group_and_user.return_value = mock_group_user
        group_service.group_user_repo.update.return_value = mock_group_user
        
        result = await group_service.update_group_user("group-123", "user-123", role=GroupUserRole.ADMIN)
        
        assert result == mock_group_user
        group_service.group_user_repo.get_by_group_and_user.assert_called_once_with("group-123", "user-123")
        group_service.group_user_repo.update.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_group_user_not_found(self, group_service):
        """Test group user update when not found."""
        group_service.group_user_repo.get_by_group_and_user.return_value = None
        
        with pytest.raises(ValueError, match="User user-123 not found in group group-123"):
            await group_service.update_group_user("group-123", "user-123", role=GroupUserRole.ADMIN)
    
    @pytest.mark.asyncio
    async def test_remove_user_from_group_success(self, group_service):
        """Test successful user removal from group."""
        group_service.group_user_repo.remove_user_from_group.return_value = True
        
        await group_service.remove_user_from_group("group-123", "user-123")
        
        group_service.group_user_repo.remove_user_from_group.assert_called_once_with("group-123", "user-123")
    
    @pytest.mark.asyncio
    async def test_remove_user_from_group_not_found(self, group_service):
        """Test user removal when not found in group."""
        group_service.group_user_repo.remove_user_from_group.return_value = False
        
        with pytest.raises(ValueError, match="User user-123 not found in group group-123"):
            await group_service.remove_user_from_group("group-123", "user-123")
    
    @pytest.mark.asyncio
    async def test_delete_group_success(self, group_service, mock_group):
        """Test successful group deletion."""
        group_service.group_repo.get.return_value = mock_group
        group_service.group_repo.delete.return_value = None
        
        await group_service.delete_group("group-123")
        
        group_service.group_repo.get.assert_called_once_with("group-123")
        group_service.group_repo.delete.assert_called_once_with("group-123")
    
    @pytest.mark.asyncio
    async def test_delete_group_not_found(self, group_service):
        """Test group deletion when group not found."""
        group_service.group_repo.get.return_value = None
        
        with pytest.raises(ValueError, match="Group group-123 not found"):
            await group_service.delete_group("group-123")
        
        group_service.group_repo.delete.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_delete_group_error(self, group_service, mock_group):
        """Test group deletion error handling."""
        group_service.group_repo.get.return_value = mock_group
        group_service.group_repo.delete.side_effect = Exception("Database error")
        
        with pytest.raises(ValueError, match="Failed to delete group: Database error"):
            await group_service.delete_group("group-123")
    
    @pytest.mark.asyncio
    async def test_get_group_stats(self, group_service):
        """Test getting group statistics."""
        mock_stats = {"total_groups": 5, "total_users": 25, "active_groups": 4}
        group_service.group_repo.get_stats.return_value = mock_stats
        
        result = await group_service.get_group_stats()
        
        assert result == mock_stats
        group_service.group_repo.get_stats.assert_called_once()
    
    def test_generate_group_name(self, group_service):
        """Test group name generation from email domain."""
        assert group_service._generate_group_name("acme-corp.com") == "Acme Corp"
        assert group_service._generate_group_name("example.org") == "Example"
        assert group_service._generate_group_name("big-company.co.uk") == "Big Company"
        assert group_service._generate_group_name("test_company.com") == "Test Company"
    
    @pytest.mark.asyncio
    async def test_ensure_group_exists_legacy_context(self, group_service, mock_group):
        """Test ensure_group_exists with legacy tenant context."""
        # Create a mock context with legacy attributes
        legacy_context = MagicMock()
        legacy_context.primary_group_id = None
        legacy_context.primary_tenant_id = "tenant-123"
        legacy_context.email_domain = "example.com"
        legacy_context.group_email = None
        legacy_context.tenant_email = "test@example.com"
        
        group_service.group_repo.get.return_value = None
        group_service.group_repo.add.return_value = mock_group
        
        with patch('src.services.group_service.Group') as MockGroupClass:
            MockGroupClass.return_value = mock_group
            
            result = await group_service.ensure_group_exists(legacy_context)
            
            assert result == mock_group
            group_service.group_repo.get.assert_called_once_with("tenant-123")
    
    @pytest.mark.asyncio
    async def test_ensure_group_user_exists_legacy_context(self, group_service, mock_group_user):
        """Test ensure_group_user_exists with legacy tenant context."""
        # Create a mock context with legacy attributes
        legacy_context = MagicMock()
        legacy_context.primary_group_id = None
        legacy_context.primary_tenant_id = "tenant-123"
        
        group_service.group_user_repo.get_by_group_and_user.return_value = mock_group_user
        
        result = await group_service.ensure_group_user_exists(legacy_context, "user-123")
        
        assert result == mock_group_user
        group_service.group_user_repo.get_by_group_and_user.assert_called_once_with("tenant-123", "user-123")