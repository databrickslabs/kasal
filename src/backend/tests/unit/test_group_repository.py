"""
Unit tests for GroupRepository.

Tests the functionality of the group repository including
CRUD operations, member management, and database interactions.
"""
import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, UTC

from src.repositories.group_repository import GroupRepository
from src.models.group import Group
from src.schemas.user import GroupCreate, GroupUpdate


@pytest.fixture
def mock_session():
    """Create a mock database session."""
    session = AsyncMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.execute = AsyncMock()
    session.scalar = AsyncMock()
    session.scalars = AsyncMock()
    return session


@pytest.fixture
def mock_group():
    """Create a mock group object."""
    group = MagicMock(spec=Group)
    group.id = uuid.uuid4()
    group.name = "Test Group"
    group.description = "A test group for testing"
    group.is_active = True
    group.created_at = datetime.now(UTC)
    group.updated_at = datetime.now(UTC)
    group.tenant_id = uuid.uuid4()
    return group


@pytest.fixture
def group_create_data():
    """Create test data for group creation."""
    return GroupCreate(
        name="Test Group",
        description="A test group for testing",
        tenant_id=uuid.uuid4()
    )


@pytest.fixture
def group_update_data():
    """Create test data for group updates."""
    return GroupUpdate(
        description="Updated test group",
        is_active=False
    )


class TestGroupRepository:
    """Test cases for GroupRepository."""
    
    @pytest.mark.asyncio
    async def test_create_group_success(self, mock_session, group_create_data):
        """Test successful group creation."""
        repository = GroupRepository(mock_session)
        
        result = await repository.create(group_create_data)
        
        assert result is not None
        mock_session.add.assert_called_once()
        mock_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_group_by_id(self, mock_session, mock_group):
        """Test getting a group by ID."""
        group_id = uuid.uuid4()
        mock_session.scalar.return_value = mock_group
        
        repository = GroupRepository(mock_session)
        
        result = await repository.get(group_id)
        
        assert result is not None
        assert result.name == "Test Group"
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_group_not_found(self, mock_session):
        """Test getting a non-existent group."""
        group_id = uuid.uuid4()
        mock_session.scalar.return_value = None
        
        repository = GroupRepository(mock_session)
        
        result = await repository.get(group_id)
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_list_groups(self, mock_session, mock_group):
        """Test listing all groups."""
        mock_result = MagicMock()
        mock_result.all.return_value = [mock_group]
        mock_session.scalars.return_value = mock_result
        
        repository = GroupRepository(mock_session)
        
        result = await repository.list()
        
        assert len(result) == 1
        assert result[0].name == "Test Group"
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_group_success(self, mock_session, mock_group, group_update_data):
        """Test successful group update."""
        group_id = uuid.uuid4()
        mock_session.scalar.return_value = mock_group
        
        repository = GroupRepository(mock_session)
        
        result = await repository.update(group_id, group_update_data)
        
        assert result is not None
        mock_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_group_not_found(self, mock_session, group_update_data):
        """Test updating a non-existent group."""
        group_id = uuid.uuid4()
        mock_session.scalar.return_value = None
        
        repository = GroupRepository(mock_session)
        
        result = await repository.update(group_id, group_update_data)
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_delete_group_success(self, mock_session, mock_group):
        """Test successful group deletion."""
        group_id = uuid.uuid4()
        mock_session.scalar.return_value = mock_group
        
        repository = GroupRepository(mock_session)
        
        result = await repository.delete(group_id)
        
        assert result is True
        mock_session.delete.assert_called_once_with(mock_group)
        mock_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_group_not_found(self, mock_session):
        """Test deleting a non-existent group."""
        group_id = uuid.uuid4()
        mock_session.scalar.return_value = None
        
        repository = GroupRepository(mock_session)
        
        result = await repository.delete(group_id)
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_get_group_by_name(self, mock_session, mock_group):
        """Test getting a group by name."""
        group_name = "Test Group"
        mock_session.scalar.return_value = mock_group
        
        repository = GroupRepository(mock_session)
        
        result = await repository.get_by_name(group_name)
        
        assert result is not None
        assert result.name == group_name
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_groups_by_tenant(self, mock_session, mock_group):
        """Test getting groups by tenant."""
        tenant_id = uuid.uuid4()
        mock_result = MagicMock()
        mock_result.all.return_value = [mock_group]
        mock_session.scalars.return_value = mock_result
        
        repository = GroupRepository(mock_session)
        
        result = await repository.get_by_tenant(tenant_id)
        
        assert len(result) == 1
        assert result[0].tenant_id == mock_group.tenant_id
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_add_member_to_group(self, mock_session, mock_group):
        """Test adding a member to a group."""
        group_id = uuid.uuid4()
        user_id = uuid.uuid4()
        mock_session.scalar.return_value = mock_group
        
        repository = GroupRepository(mock_session)
        
        result = await repository.add_member(group_id, user_id)
        
        assert result is True
        mock_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_remove_member_from_group(self, mock_session, mock_group):
        """Test removing a member from a group."""
        group_id = uuid.uuid4()
        user_id = uuid.uuid4()
        mock_session.scalar.return_value = mock_group
        
        # Mock membership exists
        mock_membership = MagicMock()
        mock_session.scalar.side_effect = [mock_group, mock_membership]
        
        repository = GroupRepository(mock_session)
        
        result = await repository.remove_member(group_id, user_id)
        
        assert result is True
        mock_session.delete.assert_called_once_with(mock_membership)
        mock_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_group_members(self, mock_session):
        """Test getting group members."""
        group_id = uuid.uuid4()
        mock_users = [
            MagicMock(id=uuid.uuid4(), username="user1"),
            MagicMock(id=uuid.uuid4(), username="user2")
        ]
        mock_result = MagicMock()
        mock_result.all.return_value = mock_users
        mock_session.scalars.return_value = mock_result
        
        repository = GroupRepository(mock_session)
        
        result = await repository.get_members(group_id)
        
        assert len(result) == 2
        assert result[0].username == "user1"
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_user_groups(self, mock_session, mock_group):
        """Test getting groups for a user."""
        user_id = uuid.uuid4()
        mock_result = MagicMock()
        mock_result.all.return_value = [mock_group]
        mock_session.scalars.return_value = mock_result
        
        repository = GroupRepository(mock_session)
        
        result = await repository.get_user_groups(user_id)
        
        assert len(result) == 1
        assert result[0].name == "Test Group"
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_search_groups(self, mock_session, mock_group):
        """Test searching groups by query."""
        search_query = "test"
        mock_result = MagicMock()
        mock_result.all.return_value = [mock_group]
        mock_session.scalars.return_value = mock_result
        
        repository = GroupRepository(mock_session)
        
        result = await repository.search(search_query)
        
        assert len(result) == 1
        assert result[0].name == "Test Group"
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_check_member_exists(self, mock_session):
        """Test checking if user is member of group."""
        group_id = uuid.uuid4()
        user_id = uuid.uuid4()
        mock_membership = MagicMock()
        mock_session.scalar.return_value = mock_membership
        
        repository = GroupRepository(mock_session)
        
        result = await repository.is_member(group_id, user_id)
        
        assert result is True
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_check_member_not_exists(self, mock_session):
        """Test checking if user is not member of group."""
        group_id = uuid.uuid4()
        user_id = uuid.uuid4()
        mock_session.scalar.return_value = None
        
        repository = GroupRepository(mock_session)
        
        result = await repository.is_member(group_id, user_id)
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_count_groups(self, mock_session):
        """Test counting total groups."""
        mock_session.scalar.return_value = 5
        
        repository = GroupRepository(mock_session)
        
        result = await repository.count()
        
        assert result == 5
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_count_active_groups(self, mock_session):
        """Test counting active groups."""
        mock_session.scalar.return_value = 3
        
        repository = GroupRepository(mock_session)
        
        result = await repository.count_active()
        
        assert result == 3
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_group_with_members_count(self, mock_session):
        """Test getting groups with member counts."""
        mock_group_data = [
            {"group": mock_group, "member_count": 5}
        ]
        mock_result = MagicMock()
        mock_result.all.return_value = mock_group_data
        mock_session.execute.return_value = mock_result
        
        repository = GroupRepository(mock_session)
        
        result = await repository.get_with_member_counts()
        
        assert len(result) == 1
        assert result[0]["member_count"] == 5
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_bulk_add_members(self, mock_session, mock_group):
        """Test bulk adding members to group."""
        group_id = uuid.uuid4()
        user_ids = [uuid.uuid4(), uuid.uuid4(), uuid.uuid4()]
        mock_session.scalar.return_value = mock_group
        
        repository = GroupRepository(mock_session)
        
        result = await repository.bulk_add_members(group_id, user_ids)
        
        assert result == 3
        assert mock_session.add.call_count == 3
        mock_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_bulk_remove_members(self, mock_session):
        """Test bulk removing members from group."""
        group_id = uuid.uuid4()
        user_ids = [uuid.uuid4(), uuid.uuid4()]
        
        repository = GroupRepository(mock_session)
        
        result = await repository.bulk_remove_members(group_id, user_ids)
        
        assert result == 2
        mock_session.execute.assert_called_once()
        mock_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_activate_group(self, mock_session, mock_group):
        """Test activating a group."""
        group_id = uuid.uuid4()
        mock_group.is_active = False
        mock_session.scalar.return_value = mock_group
        
        repository = GroupRepository(mock_session)
        
        result = await repository.activate(group_id)
        
        assert result is True
        assert mock_group.is_active is True
        mock_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_deactivate_group(self, mock_session, mock_group):
        """Test deactivating a group."""
        group_id = uuid.uuid4()
        mock_group.is_active = True
        mock_session.scalar.return_value = mock_group
        
        repository = GroupRepository(mock_session)
        
        result = await repository.deactivate(group_id)
        
        assert result is True
        assert mock_group.is_active is False
        mock_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_group_hierarchy(self, mock_session):
        """Test getting group hierarchy."""
        parent_group = MagicMock()
        parent_group.id = uuid.uuid4()
        parent_group.name = "Parent Group"
        parent_group.parent_id = None
        
        child_group = MagicMock()
        child_group.id = uuid.uuid4()
        child_group.name = "Child Group"
        child_group.parent_id = parent_group.id
        
        mock_result = MagicMock()
        mock_result.all.return_value = [parent_group, child_group]
        mock_session.scalars.return_value = mock_result
        
        repository = GroupRepository(mock_session)
        
        result = await repository.get_hierarchy()
        
        assert len(result) == 2
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_database_error_handling(self, mock_session, group_create_data):
        """Test database error handling."""
        mock_session.add.side_effect = Exception("Database connection error")
        
        repository = GroupRepository(mock_session)
        
        with pytest.raises(Exception, match="Database connection error"):
            await repository.create(group_create_data)