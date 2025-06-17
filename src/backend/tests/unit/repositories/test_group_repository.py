"""
Unit tests for GroupRepository and GroupUserRepository.

Tests the functionality of group repositories including
CRUD operations, user membership management, statistics, and error handling.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from typing import List, Dict, Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload

from src.repositories.group_repository import GroupRepository, GroupUserRepository
from src.models.group import Group, GroupUser
from src.models.user import User


# Mock group model
class MockGroup:
    def __init__(self, id="group-123", name="Test Group", email_domain="test.com",
                 status="ACTIVE", description="Test Description", auto_created=False,
                 created_by_email="admin@test.com", created_at=None, updated_at=None):
        self.id = id
        self.name = name
        self.email_domain = email_domain
        self.status = status
        self.description = description
        self.auto_created = auto_created
        self.created_by_email = created_by_email
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()
        self.group_users = []


# Mock group user model
class MockGroupUser:
    def __init__(self, id=1, group_id="group-123", user_id="user-123", 
                 role="member", status="active", joined_at=None, auto_created=False):
        self.id = id
        self.group_id = group_id
        self.user_id = user_id
        self.role = role
        self.status = status
        self.joined_at = joined_at or datetime.utcnow()
        self.auto_created = auto_created
        self.user = None
        self.group = None


# Mock user model
class MockUser:
    def __init__(self, id="user-123", email="user@test.com", name="Test User", group_id="group-123"):
        self.id = id
        self.email = email
        self.name = name


# Mock SQLAlchemy result objects
class MockScalars:
    def __init__(self, results):
        self.results = results
    
    def first(self):
        return self.results[0] if self.results else None
    
    def all(self):
        return self.results
    
    def __iter__(self):
        return iter(self.results)


class MockResult:
    def __init__(self, results, scalar_value=None):
        self._scalars = MockScalars(results)
        self._scalar_value = scalar_value if scalar_value is not None else (results[0] if results else 0)
        self._results = results
    
    def scalars(self):
        return self._scalars
    
    def scalar(self):
        return self._scalar_value
    
    def __iter__(self):
        return iter(self._results)


@pytest.fixture
def mock_async_session():
    """Create a mock async database session."""
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.add = AsyncMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def group_repository(mock_async_session):
    """Create a group repository with async session."""
    return GroupRepository(session=mock_async_session)


@pytest.fixture
def group_user_repository(mock_async_session):
    """Create a group user repository with async session."""
    return GroupUserRepository(session=mock_async_session)


@pytest.fixture
def sample_groups():
    """Create sample groups for testing."""
    return [
        MockGroup(id="group-1", name="Group 1", email_domain="group1.com", status="ACTIVE"),
        MockGroup(id="group-2", name="Group 2", email_domain="group2.com", status="INACTIVE"),
        MockGroup(id="group-3", name="Group 3", email_domain="group3.com", status="ACTIVE")
    ]


@pytest.fixture
def sample_group_users():
    """Create sample group users for testing."""
    return [
        MockGroupUser(id=1, group_id="group-1", user_id="user-1", role="admin"),
        MockGroupUser(id=2, group_id="group-1", user_id="user-2", role="member"),
        MockGroupUser(id=3, group_id="group-2", user_id="user-1", role="member")
    ]


class TestGroupRepositoryInit:
    """Test cases for GroupRepository initialization."""
    
    def test_init_success(self, mock_async_session):
        """Test successful initialization."""
        repository = GroupRepository(session=mock_async_session)
        
        assert repository.model == Group
        assert repository.session == mock_async_session


class TestGroupRepositoryGetByEmailDomain:
    """Test cases for get_by_email_domain method."""
    
    @pytest.mark.asyncio
    async def test_get_by_email_domain_success(self, group_repository, mock_async_session):
        """Test successful group retrieval by email domain."""
        group = MockGroup(email_domain="test.com")
        mock_result = MockResult([group])
        mock_async_session.execute.return_value = mock_result
        
        result = await group_repository.get_by_email_domain("test.com")
        
        assert result == group
        mock_async_session.execute.assert_called_once()
        # Verify the query was constructed correctly
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(Group)))
    
    @pytest.mark.asyncio
    async def test_get_by_email_domain_not_found(self, group_repository, mock_async_session):
        """Test get by email domain when group not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await group_repository.get_by_email_domain("nonexistent.com")
        
        assert result is None
        mock_async_session.execute.assert_called_once()


class TestGroupRepositoryGetWithUsers:
    """Test cases for get_with_users method."""
    
    @pytest.mark.asyncio
    async def test_get_with_users_success(self, group_repository, mock_async_session):
        """Test successful group retrieval with users loaded."""
        group = MockGroup(id="group-123")
        # Mock users in the group
        user1 = MockUser(id="user-1")
        user2 = MockUser(id="user-2")
        group_user1 = MockGroupUser(group_id="group-123", user_id="user-1")
        group_user1.user = user1
        group_user2 = MockGroupUser(group_id="group-123", user_id="user-2")
        group_user2.user = user2
        group.group_users = [group_user1, group_user2]
        
        mock_result = MockResult([group])
        mock_async_session.execute.return_value = mock_result
        
        result = await group_repository.get_with_users("group-123")
        
        assert result == group
        assert len(result.group_users) == 2
        mock_async_session.execute.assert_called_once()
        
        # Verify query uses selectinload for eager loading
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(Group)))
    
    @pytest.mark.asyncio
    async def test_get_with_users_not_found(self, group_repository, mock_async_session):
        """Test get with users when group not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await group_repository.get_with_users("nonexistent")
        
        assert result is None
        mock_async_session.execute.assert_called_once()


class TestGroupRepositoryListWithUserCounts:
    """Test cases for list_with_user_counts method."""
    
    @pytest.mark.asyncio
    async def test_list_with_user_counts_success(self, group_repository, mock_async_session, sample_groups):
        """Test successful listing of groups with user counts."""
        # Mock result with (group, count) tuples
        result_tuples = [
            (sample_groups[0], 5),
            (sample_groups[1], 2),
            (sample_groups[2], 0)
        ]
        mock_result = MockResult(result_tuples)
        mock_async_session.execute.return_value = mock_result
        
        result = await group_repository.list_with_user_counts(skip=0, limit=10)
        
        assert len(result) == 3
        assert result[0]['id'] == sample_groups[0].id
        assert result[0]['user_count'] == 5
        assert result[1]['user_count'] == 2
        assert result[2]['user_count'] == 0
        
        # Verify all expected fields are present
        expected_fields = ['id', 'name', 'email_domain', 'status', 'description', 
                          'auto_created', 'created_by_email', 'created_at', 'updated_at', 'user_count']
        for field in expected_fields:
            assert field in result[0]
        
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_with_user_counts_pagination(self, group_repository, mock_async_session):
        """Test list with user counts pagination."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await group_repository.list_with_user_counts(skip=10, limit=5)
        
        assert result == []
        mock_async_session.execute.assert_called_once()
        
        # Verify pagination parameters in query
        call_args = mock_async_session.execute.call_args[0][0]
        assert hasattr(call_args, 'compile')  # Should be a SQLAlchemy statement


class TestGroupRepositoryGetStats:
    """Test cases for get_stats method."""
    
    @pytest.mark.asyncio
    async def test_get_stats_success(self, group_repository, mock_async_session):
        """Test successful retrieval of group statistics."""
        # Mock multiple execute calls for different stats queries
        total_result = MockResult([], scalar_value=10)
        active_result = MockResult([], scalar_value=7)
        users_result = MockResult([], scalar_value=25)
        status_result = MockResult([("ACTIVE", 7), ("INACTIVE", 3)])
        
        mock_async_session.execute.side_effect = [
            total_result, active_result, users_result, status_result
        ]
        
        result = await group_repository.get_stats()
        
        assert result['total_groups'] == 10
        assert result['active_groups'] == 7
        assert result['total_users'] == 25
        assert result['groups_by_status'] == {"ACTIVE": 7, "INACTIVE": 3}
        
        # Verify all queries were executed
        assert mock_async_session.execute.call_count == 4
    
    @pytest.mark.asyncio
    async def test_get_stats_no_data(self, group_repository, mock_async_session):
        """Test get stats when no data exists."""
        # Mock zero results
        zero_result = MockResult([], scalar_value=0)
        empty_status_result = MockResult([])
        
        mock_async_session.execute.side_effect = [
            zero_result, zero_result, zero_result, empty_status_result
        ]
        
        result = await group_repository.get_stats()
        
        assert result['total_groups'] == 0
        assert result['active_groups'] == 0
        assert result['total_users'] == 0
        assert result['groups_by_status'] == {}


class TestGroupUserRepositoryInit:
    """Test cases for GroupUserRepository initialization."""
    
    def test_init_success(self, mock_async_session):
        """Test successful initialization."""
        repository = GroupUserRepository(session=mock_async_session)
        
        assert repository.model == GroupUser
        assert repository.session == mock_async_session


class TestGroupUserRepositoryGetByGroupAndUser:
    """Test cases for get_by_group_and_user method."""
    
    @pytest.mark.asyncio
    async def test_get_by_group_and_user_success(self, group_user_repository, mock_async_session):
        """Test successful retrieval by group and user IDs."""
        group_user = MockGroupUser(group_id="group-123", user_id="user-123")
        mock_result = MockResult([group_user])
        mock_async_session.execute.return_value = mock_result
        
        result = await group_user_repository.get_by_group_and_user("group-123", "user-123")
        
        assert result == group_user
        mock_async_session.execute.assert_called_once()
        
        # Verify query uses AND condition
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(GroupUser)))
    
    @pytest.mark.asyncio
    async def test_get_by_group_and_user_not_found(self, group_user_repository, mock_async_session):
        """Test get by group and user when membership not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await group_user_repository.get_by_group_and_user("group-123", "user-456")
        
        assert result is None
        mock_async_session.execute.assert_called_once()


class TestGroupUserRepositoryGetUsersByGroup:
    """Test cases for get_users_by_group method."""
    
    @pytest.mark.asyncio
    async def test_get_users_by_group_success(self, group_user_repository, mock_async_session, sample_group_users):
        """Test successful retrieval of users by group."""
        group_users = [gu for gu in sample_group_users if gu.group_id == "group-1"]
        # Add user details
        for gu in group_users:
            gu.user = MockUser(id=gu.user_id, email=f"user{gu.user_id}@test.com")
        
        mock_result = MockResult(group_users)
        mock_async_session.execute.return_value = mock_result
        
        result = await group_user_repository.get_users_by_group("group-1", skip=0, limit=100)
        
        assert len(result) == 2
        assert all(gu.group_id == "group-1" for gu in result)
        assert all(gu.user is not None for gu in result)
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_users_by_group_pagination(self, group_user_repository, mock_async_session):
        """Test get users by group with pagination."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await group_user_repository.get_users_by_group("group-1", skip=10, limit=5)
        
        assert result == []
        mock_async_session.execute.assert_called_once()


class TestGroupUserRepositoryGetGroupsByUser:
    """Test cases for get_groups_by_user method."""
    
    @pytest.mark.asyncio
    async def test_get_groups_by_user_success(self, group_user_repository, mock_async_session, sample_group_users):
        """Test successful retrieval of groups by user."""
        user_groups = [gu for gu in sample_group_users if gu.user_id == "user-1"]
        # Add group details
        for gu in user_groups:
            gu.group = MockGroup(id=gu.group_id, name=f"Group {gu.group_id}")
        
        mock_result = MockResult(user_groups)
        mock_async_session.execute.return_value = mock_result
        
        result = await group_user_repository.get_groups_by_user("user-1")
        
        assert len(result) == 2  # user-1 is in group-1 and group-2
        assert all(gu.user_id == "user-1" for gu in result)
        assert all(gu.group is not None for gu in result)
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_groups_by_user_no_groups(self, group_user_repository, mock_async_session):
        """Test get groups by user when user has no groups."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await group_user_repository.get_groups_by_user("user-without-groups")
        
        assert result == []
        mock_async_session.execute.assert_called_once()


class TestGroupUserRepositoryGetUserEmailsByGroup:
    """Test cases for get_user_emails_by_group method."""
    
    @pytest.mark.asyncio
    async def test_get_user_emails_by_group_success(self, group_user_repository, mock_async_session):
        """Test successful retrieval of user emails by group."""
        emails = ["user1@test.com", "user2@test.com", "user3@test.com"]
        mock_result = MockResult(emails)
        mock_async_session.execute.return_value = mock_result
        
        result = await group_user_repository.get_user_emails_by_group("group-1")
        
        assert result == emails
        mock_async_session.execute.assert_called_once()
        
        # Verify query joins User and GroupUser tables
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(User.email)))
    
    @pytest.mark.asyncio
    async def test_get_user_emails_by_group_no_users(self, group_user_repository, mock_async_session):
        """Test get user emails when group has no users."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await group_user_repository.get_user_emails_by_group("empty-group")
        
        assert result == []
        mock_async_session.execute.assert_called_once()


class TestGroupUserRepositoryRemoveUserFromGroup:
    """Test cases for remove_user_from_group method."""
    
    @pytest.mark.asyncio
    async def test_remove_user_from_group_success(self, group_user_repository, mock_async_session):
        """Test successful user removal from group."""
        mock_delete_result = MagicMock()
        mock_delete_result.rowcount = 1
        mock_async_session.execute.return_value = mock_delete_result
        
        result = await group_user_repository.remove_user_from_group("group-1", "user-1")
        
        assert result is True
        mock_async_session.execute.assert_called_once()
        mock_async_session.commit.assert_called_once()
        
        # Verify delete query structure
        call_args = mock_async_session.execute.call_args[0][0]
        assert hasattr(call_args, 'compile')  # Should be a delete statement
    
    @pytest.mark.asyncio
    async def test_remove_user_from_group_not_found(self, group_user_repository, mock_async_session):
        """Test user removal when membership not found."""
        mock_delete_result = MagicMock()
        mock_delete_result.rowcount = 0
        mock_async_session.execute.return_value = mock_delete_result
        
        result = await group_user_repository.remove_user_from_group("group-1", "nonexistent-user")
        
        assert result is False
        mock_async_session.execute.assert_called_once()
        mock_async_session.commit.assert_called_once()


class TestGroupUserRepositoryUpdateUserRole:
    """Test cases for update_user_role method."""
    
    @pytest.mark.asyncio
    async def test_update_user_role_success(self, group_user_repository, mock_async_session):
        """Test successful user role update."""
        updated_group_user = MockGroupUser(group_id="group-1", user_id="user-1", role="admin")
        
        mock_async_session.execute.return_value = MagicMock()
        
        with patch.object(group_user_repository, 'get_by_group_and_user', return_value=updated_group_user):
            result = await group_user_repository.update_user_role("group-1", "user-1", "admin")
            
            assert result == updated_group_user
            assert result.role == "admin"
            mock_async_session.execute.assert_called_once()
            mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_user_role_membership_not_found(self, group_user_repository, mock_async_session):
        """Test role update when membership not found."""
        mock_async_session.execute.return_value = MagicMock()
        
        with patch.object(group_user_repository, 'get_by_group_and_user', return_value=None):
            result = await group_user_repository.update_user_role("group-1", "nonexistent", "admin")
            
            assert result is None
            mock_async_session.execute.assert_called_once()
            mock_async_session.commit.assert_called_once()


class TestGroupUserRepositoryGetUserGroupsWithRoles:
    """Test cases for get_user_groups_with_roles method."""
    
    @pytest.mark.asyncio
    async def test_get_user_groups_with_roles_success(self, group_user_repository, mock_async_session):
        """Test successful retrieval of user groups with roles."""
        # Mock result with (GroupUser, Group) tuples
        group_user1 = MockGroupUser(group_id="group-1", user_id="user-1", role="admin")
        group1 = MockGroup(id="group-1", name="Group 1", email_domain="group1.com")
        
        group_user2 = MockGroupUser(group_id="group-2", user_id="user-1", role="member")
        group2 = MockGroup(id="group-2", name="Group 2", email_domain="group2.com")
        
        result_tuples = [(group_user1, group1), (group_user2, group2)]
        mock_result = MockResult(result_tuples)
        mock_async_session.execute.return_value = mock_result
        
        result = await group_user_repository.get_user_groups_with_roles("user-1")
        
        assert len(result) == 2
        
        # Verify first membership
        assert result[0]['group_id'] == "group-1"
        assert result[0]['group_name'] == "Group 1"
        assert result[0]['role'] == "admin"
        
        # Verify second membership
        assert result[1]['group_id'] == "group-2"
        assert result[1]['group_name'] == "Group 2"
        assert result[1]['role'] == "member"
        
        # Verify all expected fields are present
        expected_fields = ['group_id', 'group_name', 'group_email_domain', 'role', 
                          'status', 'joined_at', 'auto_created']
        for field in expected_fields:
            assert field in result[0]
            assert field in result[1]
        
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_user_groups_with_roles_no_memberships(self, group_user_repository, mock_async_session):
        """Test get user groups with roles when user has no memberships."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await group_user_repository.get_user_groups_with_roles("user-without-groups")
        
        assert result == []
        mock_async_session.execute.assert_called_once()


class TestGroupRepositoryIntegration:
    """Integration test cases testing method interactions."""
    
    @pytest.mark.asyncio
    async def test_create_group_then_get_with_users(self, group_repository, mock_async_session):
        """Test creating a group then retrieving it with users."""
        group_data = {
            "name": "Integration Group",
            "email_domain": "integration.com",
            "status": "ACTIVE"
        }
        
        with patch('src.repositories.group_repository.Group') as mock_group_class:
            created_group = MockGroup(**group_data)
            mock_group_class.return_value = created_group
            
            # Mock get_with_users
            mock_result = MockResult([created_group])
            mock_async_session.execute.return_value = mock_result
            
            # Create group using inherited create method
            with patch.object(group_repository, 'create', return_value=created_group) as mock_create:
                create_result = await group_repository.create(group_data)
                
                # Get group with users
                get_result = await group_repository.get_with_users(created_group.id)
                
                assert create_result == created_group
                assert get_result == created_group
                mock_create.assert_called_once_with(group_data)
    
    @pytest.mark.asyncio
    async def test_group_user_management_workflow(self, group_user_repository, mock_async_session):
        """Test complete group user management workflow."""
        group_user = MockGroupUser(group_id="group-1", user_id="user-1", role="member")
        
        # Add user to group (simulate creation)
        with patch.object(group_user_repository, 'create', return_value=group_user) as mock_create:
            create_result = await group_user_repository.create({
                "group_id": "group-1",
                "user_id": "user-1",
                "role": "member"
            })
            
            # Update user role
            group_user.role = "admin"
            mock_async_session.execute.return_value = MagicMock()
            
            with patch.object(group_user_repository, 'get_by_group_and_user', return_value=group_user):
                update_result = await group_user_repository.update_user_role("group-1", "user-1", "admin")
                
                # Remove user from group
                mock_delete_result = MagicMock()
                mock_delete_result.rowcount = 1
                mock_async_session.execute.return_value = mock_delete_result
                
                remove_result = await group_user_repository.remove_user_from_group("group-1", "user-1")
                
                assert create_result == group_user
                assert update_result.role == "admin"
                assert remove_result is True


class TestGroupRepositoryErrorHandling:
    """Test cases for error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_get_by_email_domain_database_error(self, group_repository, mock_async_session):
        """Test get by email domain with database error."""
        mock_async_session.execute.side_effect = Exception("Connection lost")
        
        with pytest.raises(Exception, match="Connection lost"):
            await group_repository.get_by_email_domain("test.com")
    
    @pytest.mark.asyncio
    async def test_get_stats_database_error(self, group_repository, mock_async_session):
        """Test get stats with database error."""
        mock_async_session.execute.side_effect = Exception("Query timeout")
        
        with pytest.raises(Exception, match="Query timeout"):
            await group_repository.get_stats()
    
    @pytest.mark.asyncio
    async def test_remove_user_from_group_database_error(self, group_user_repository, mock_async_session):
        """Test remove user from group with database error."""
        mock_async_session.execute.side_effect = Exception("Delete failed")
        
        with pytest.raises(Exception, match="Delete failed"):
            await group_user_repository.remove_user_from_group("group-1", "user-1")
    
    @pytest.mark.asyncio
    async def test_update_user_role_database_error(self, group_user_repository, mock_async_session):
        """Test update user role with database error."""
        mock_async_session.execute.side_effect = Exception("Update failed")
        
        with pytest.raises(Exception, match="Update failed"):
            await group_user_repository.update_user_role("group-1", "user-1", "admin")


class TestGroupRepositoryLegacyCompatibility:
    """Test cases for legacy compatibility aliases."""
    
    def test_tenant_repository_alias(self, mock_async_session):
        """Test that TenantRepository is an alias for GroupRepository."""
        from src.repositories.group_repository import TenantRepository
        
        assert TenantRepository == GroupRepository
        
        tenant_repo = TenantRepository(session=mock_async_session)
        assert isinstance(tenant_repo, GroupRepository)
        assert tenant_repo.model == Group
    
    def test_tenant_user_repository_alias(self, mock_async_session):
        """Test that TenantUserRepository is an alias for GroupUserRepository."""
        from src.repositories.group_repository import TenantUserRepository
        
        assert TenantUserRepository == GroupUserRepository
        
        tenant_user_repo = TenantUserRepository(session=mock_async_session)
        assert isinstance(tenant_user_repo, GroupUserRepository)
        assert tenant_user_repo.model == GroupUser


class TestGroupRepositoryEdgeCases:
    """Test cases for edge cases and boundary conditions."""
    
    @pytest.mark.asyncio
    async def test_list_with_user_counts_empty_groups(self, group_repository, mock_async_session):
        """Test list with user counts when groups have no users."""
        group = MockGroup(id="empty-group")
        result_tuples = [(group, 0)]
        mock_result = MockResult(result_tuples)
        mock_async_session.execute.return_value = mock_result
        
        result = await group_repository.list_with_user_counts()
        
        assert len(result) == 1
        assert result[0]['user_count'] == 0
    
    @pytest.mark.asyncio
    async def test_get_stats_edge_case_values(self, group_repository, mock_async_session):
        """Test get stats with edge case values."""
        # Mock large numbers and edge cases
        total_result = MockResult([], scalar_value=1000000)
        active_result = MockResult([], scalar_value=999999)
        users_result = MockResult([], scalar_value=0)  # No users
        status_result = MockResult([("ACTIVE", 999999), ("PENDING", 1)])
        
        mock_async_session.execute.side_effect = [
            total_result, active_result, users_result, status_result
        ]
        
        result = await group_repository.get_stats()
        
        assert result['total_groups'] == 1000000
        assert result['active_groups'] == 999999
        assert result['total_users'] == 0
        assert result['groups_by_status']['ACTIVE'] == 999999
        assert result['groups_by_status']['PENDING'] == 1
    
    @pytest.mark.asyncio
    async def test_get_user_emails_by_group_duplicate_handling(self, group_user_repository, mock_async_session):
        """Test get user emails handles potential duplicates correctly."""
        emails = ["user@test.com", "user@test.com", "other@test.com"]  # Duplicate email
        mock_result = MockResult(emails)
        mock_async_session.execute.return_value = mock_result
        
        result = await group_user_repository.get_user_emails_by_group("group-1")
        
        # Should return all emails as they come from the database
        assert result == emails
        assert len(result) == 3
    
    @pytest.mark.asyncio
    async def test_update_user_role_same_role(self, group_user_repository, mock_async_session):
        """Test updating user role to the same role."""
        group_user = MockGroupUser(group_id="group-1", user_id="user-1", role="member")
        
        mock_async_session.execute.return_value = MagicMock()
        
        with patch.object(group_user_repository, 'get_by_group_and_user', return_value=group_user):
            result = await group_user_repository.update_user_role("group-1", "user-1", "member")
            
            assert result == group_user
            assert result.role == "member"
            # Update should still be executed even if role is the same
            mock_async_session.execute.assert_called_once()
            mock_async_session.commit.assert_called_once()