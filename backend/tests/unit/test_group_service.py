"""
Unit tests for GroupService.

Tests the functionality of the group service including
group management, member operations, and business logic.
"""
import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, UTC

from src.services.group_service import GroupService
from src.schemas.group import GroupCreate, GroupUpdate
from src.models.group import Group
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
def mock_group_repository():
    """Create a mock group repository."""
    repo = AsyncMock()
    
    # Create mock group objects
    mock_group = MagicMock(spec=Group)
    mock_group.id = uuid.uuid4()
    mock_group.name = "Developers"
    mock_group.description = "Development team group"
    mock_group.is_active = True
    mock_group.created_at = datetime.now(UTC)
    mock_group.updated_at = datetime.now(UTC)
    mock_group.tenant_id = uuid.uuid4()
    
    # Setup repository method returns
    repo.get.return_value = mock_group
    repo.list.return_value = [mock_group]
    repo.create.return_value = mock_group
    repo.update.return_value = mock_group
    repo.delete.return_value = True
    repo.get_by_name.return_value = mock_group
    repo.get_members.return_value = []
    
    return repo


@pytest.fixture
def group_create_data():
    """Create test data for group creation."""
    return GroupCreate(
        name="Test Group",
        description="A test group for testing",
        is_active=True
    )


@pytest.fixture
def group_update_data():
    """Create test data for group updates."""
    return GroupUpdate(
        description="Updated test group",
        is_active=False
    )


class TestGroupService:
    """Test cases for GroupService."""
    
    @pytest.mark.asyncio
    async def test_create_group_success(self, mock_uow, mock_group_repository, group_create_data):
        """Test successful group creation."""
        with patch("src.services.group_service.GroupRepository", return_value=mock_group_repository):
            mock_group_repository.get_by_name.return_value = None  # Group doesn't exist
            
            service = GroupService(mock_uow)
            
            result = await service.create(group_create_data)
            
            assert result is not None
            assert result.name == "Developers"
            mock_group_repository.create.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_group_duplicate_name(self, mock_uow, mock_group_repository, group_create_data):
        """Test group creation with duplicate name."""
        with patch("src.services.group_service.GroupRepository", return_value=mock_group_repository):
            mock_group_repository.get_by_name.return_value = MagicMock()  # Group exists
            
            service = GroupService(mock_uow)
            
            with pytest.raises(ValueError, match="Group name already exists"):
                await service.create(group_create_data)
    
    @pytest.mark.asyncio
    async def test_get_group_by_id(self, mock_uow, mock_group_repository):
        """Test getting a group by ID."""
        group_id = uuid.uuid4()
        
        with patch("src.services.group_service.GroupRepository", return_value=mock_group_repository):
            service = GroupService(mock_uow)
            
            result = await service.get(group_id)
            
            assert result is not None
            assert result.name == "Developers"
            mock_group_repository.get.assert_called_once_with(group_id)
    
    @pytest.mark.asyncio
    async def test_update_group_success(self, mock_uow, mock_group_repository, group_update_data):
        """Test successful group update."""
        group_id = uuid.uuid4()
        
        with patch("src.services.group_service.GroupRepository", return_value=mock_group_repository):
            service = GroupService(mock_uow)
            
            result = await service.update(group_id, group_update_data)
            
            assert result is not None
            mock_group_repository.update.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_group_success(self, mock_uow, mock_group_repository):
        """Test successful group deletion."""
        group_id = uuid.uuid4()
        
        # Mock that group has no members
        mock_group_repository.get_members.return_value = []
        
        with patch("src.services.group_service.GroupRepository", return_value=mock_group_repository):
            service = GroupService(mock_uow)
            
            result = await service.delete(group_id)
            
            assert result is True
            mock_group_repository.delete.assert_called_once_with(group_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_group_with_members(self, mock_uow, mock_group_repository):
        """Test deletion of group with members should fail."""
        group_id = uuid.uuid4()
        
        # Mock that group has members
        mock_members = [{"id": uuid.uuid4(), "username": "user1"}]
        mock_group_repository.get_members.return_value = mock_members
        
        with patch("src.services.group_service.GroupRepository", return_value=mock_group_repository):
            service = GroupService(mock_uow)
            
            with pytest.raises(ValueError, match="Cannot delete group with members"):
                await service.delete(group_id)
    
    @pytest.mark.asyncio
    async def test_add_member_to_group(self, mock_uow, mock_group_repository):
        """Test adding member to group."""
        group_id = uuid.uuid4()
        user_id = uuid.uuid4()
        
        # Mock that user is not already a member
        mock_group_repository.is_member.return_value = False
        
        with patch("src.services.group_service.GroupRepository", return_value=mock_group_repository):
            service = GroupService(mock_uow)
            
            result = await service.add_member(group_id, user_id)
            
            assert result is True
            mock_group_repository.add_member.assert_called_once_with(group_id, user_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_add_existing_member_to_group(self, mock_uow, mock_group_repository):
        """Test adding existing member to group should fail."""
        group_id = uuid.uuid4()
        user_id = uuid.uuid4()
        
        # Mock that user is already a member
        mock_group_repository.is_member.return_value = True
        
        with patch("src.services.group_service.GroupRepository", return_value=mock_group_repository):
            service = GroupService(mock_uow)
            
            with pytest.raises(ValueError, match="User is already a member"):
                await service.add_member(group_id, user_id)
    
    @pytest.mark.asyncio
    async def test_remove_member_from_group(self, mock_uow, mock_group_repository):
        """Test removing member from group."""
        group_id = uuid.uuid4()
        user_id = uuid.uuid4()
        
        # Mock that user is a member
        mock_group_repository.is_member.return_value = True
        
        with patch("src.services.group_service.GroupRepository", return_value=mock_group_repository):
            service = GroupService(mock_uow)
            
            result = await service.remove_member(group_id, user_id)
            
            assert result is True
            mock_group_repository.remove_member.assert_called_once_with(group_id, user_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_remove_non_member_from_group(self, mock_uow, mock_group_repository):
        """Test removing non-member from group should fail."""
        group_id = uuid.uuid4()
        user_id = uuid.uuid4()
        
        # Mock that user is not a member
        mock_group_repository.is_member.return_value = False
        
        with patch("src.services.group_service.GroupRepository", return_value=mock_group_repository):
            service = GroupService(mock_uow)
            
            with pytest.raises(ValueError, match="User is not a member"):
                await service.remove_member(group_id, user_id)
    
    @pytest.mark.asyncio
    async def test_get_group_members(self, mock_uow, mock_group_repository):
        """Test getting group members."""
        group_id = uuid.uuid4()
        mock_members = [
            {"id": uuid.uuid4(), "username": "user1", "email": "user1@test.com"},
            {"id": uuid.uuid4(), "username": "user2", "email": "user2@test.com"}
        ]
        mock_group_repository.get_members.return_value = mock_members
        
        with patch("src.services.group_service.GroupRepository", return_value=mock_group_repository):
            service = GroupService(mock_uow)
            
            result = await service.get_members(group_id)
            
            assert len(result) == 2
            assert result[0]["username"] == "user1"
            mock_group_repository.get_members.assert_called_once_with(group_id)
    
    @pytest.mark.asyncio
    async def test_bulk_add_members(self, mock_uow, mock_group_repository):
        """Test bulk adding members to group."""
        group_id = uuid.uuid4()
        user_ids = [uuid.uuid4(), uuid.uuid4(), uuid.uuid4()]
        
        # Mock that none of the users are already members
        mock_group_repository.is_member.return_value = False
        mock_group_repository.bulk_add_members.return_value = 3
        
        with patch("src.services.group_service.GroupRepository", return_value=mock_group_repository):
            service = GroupService(mock_uow)
            
            result = await service.bulk_add_members(group_id, user_ids)
            
            assert result == 3
            mock_group_repository.bulk_add_members.assert_called_once_with(group_id, user_ids)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_bulk_remove_members(self, mock_uow, mock_group_repository):
        """Test bulk removing members from group."""
        group_id = uuid.uuid4()
        user_ids = [uuid.uuid4(), uuid.uuid4()]
        
        mock_group_repository.bulk_remove_members.return_value = 2
        
        with patch("src.services.group_service.GroupRepository", return_value=mock_group_repository):
            service = GroupService(mock_uow)
            
            result = await service.bulk_remove_members(group_id, user_ids)
            
            assert result == 2
            mock_group_repository.bulk_remove_members.assert_called_once_with(group_id, user_ids)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_activate_group(self, mock_uow, mock_group_repository):
        """Test group activation."""
        group_id = uuid.uuid4()
        
        with patch("src.services.group_service.GroupRepository", return_value=mock_group_repository):
            service = GroupService(mock_uow)
            
            result = await service.activate(group_id)
            
            assert result is True
            mock_group_repository.activate.assert_called_once_with(group_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_deactivate_group(self, mock_uow, mock_group_repository):
        """Test group deactivation."""
        group_id = uuid.uuid4()
        
        with patch("src.services.group_service.GroupRepository", return_value=mock_group_repository):
            service = GroupService(mock_uow)
            
            result = await service.deactivate(group_id)
            
            assert result is True
            mock_group_repository.deactivate.assert_called_once_with(group_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_search_groups(self, mock_uow, mock_group_repository):
        """Test searching groups."""
        search_query = "dev"
        
        with patch("src.services.group_service.GroupRepository", return_value=mock_group_repository):
            service = GroupService(mock_uow)
            
            result = await service.search(search_query)
            
            assert len(result) == 1
            mock_group_repository.search.assert_called_once_with(search_query)
    
    @pytest.mark.asyncio
    async def test_get_user_groups(self, mock_uow, mock_group_repository):
        """Test getting groups for a user."""
        user_id = uuid.uuid4()
        
        with patch("src.services.group_service.GroupRepository", return_value=mock_group_repository):
            service = GroupService(mock_uow)
            
            result = await service.get_user_groups(user_id)
            
            assert len(result) == 1
            mock_group_repository.get_user_groups.assert_called_once_with(user_id)
    
    @pytest.mark.asyncio
    async def test_validate_group_name(self, mock_uow):
        """Test group name validation."""
        service = GroupService(mock_uow)
        
        # Test valid names
        valid_names = ["Developers", "Marketing Team", "QA-Engineers"]
        for name in valid_names:
            service._validate_group_name(name)  # Should not raise
        
        # Test invalid names
        invalid_names = ["", "AB", "Group with very long name that exceeds maximum length limit"]
        for name in invalid_names:
            with pytest.raises(ValueError):
                service._validate_group_name(name)
    
    @pytest.mark.asyncio
    async def test_group_hierarchy_operations(self, mock_uow, mock_group_repository):
        """Test group hierarchy operations."""
        parent_group_id = uuid.uuid4()
        child_group_id = uuid.uuid4()
        
        with patch("src.services.group_service.GroupRepository", return_value=mock_group_repository):
            service = GroupService(mock_uow)
            
            # Test setting parent group
            result = await service.set_parent_group(child_group_id, parent_group_id)
            assert result is True
            
            # Test getting group hierarchy
            mock_hierarchy = [
                {"id": parent_group_id, "name": "Parent", "children": [child_group_id]},
                {"id": child_group_id, "name": "Child", "parent": parent_group_id}
            ]
            mock_group_repository.get_hierarchy.return_value = mock_hierarchy
            
            hierarchy = await service.get_group_hierarchy()
            assert len(hierarchy) == 2
    
    @pytest.mark.asyncio
    async def test_group_permissions_inheritance(self, mock_uow, mock_group_repository):
        """Test group permissions inheritance."""
        group_id = uuid.uuid4()
        user_id = uuid.uuid4()
        
        mock_permissions = ["read", "write", "execute"]
        mock_group_repository.get_inherited_permissions.return_value = mock_permissions
        
        with patch("src.services.group_service.GroupRepository", return_value=mock_group_repository):
            service = GroupService(mock_uow)
            
            permissions = await service.get_user_inherited_permissions(group_id, user_id)
            
            assert len(permissions) == 3
            assert "execute" in permissions
    
    @pytest.mark.asyncio
    async def test_group_metrics(self, mock_uow, mock_group_repository):
        """Test getting group metrics."""
        mock_metrics = {
            "total_groups": 10,
            "active_groups": 8,
            "total_members": 150,
            "average_group_size": 15,
            "largest_group_size": 25
        }
        mock_group_repository.get_metrics.return_value = mock_metrics
        
        with patch("src.services.group_service.GroupRepository", return_value=mock_group_repository):
            service = GroupService(mock_uow)
            
            metrics = await service.get_group_metrics()
            
            assert metrics["total_groups"] == 10
            assert metrics["active_groups"] == 8
            mock_group_repository.get_metrics.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_group_audit_logging(self, mock_uow, mock_group_repository, group_create_data):
        """Test that group operations are audited."""
        with patch("src.services.group_service.GroupRepository", return_value=mock_group_repository), \
             patch("src.services.group_service.audit_logger") as mock_audit:
            
            mock_group_repository.get_by_name.return_value = None
            
            service = GroupService(mock_uow)
            
            await service.create(group_create_data)
            
            mock_audit.log_group_creation.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_group_cleanup_operations(self, mock_uow, mock_group_repository):
        """Test group cleanup operations."""
        with patch("src.services.group_service.GroupRepository", return_value=mock_group_repository):
            service = GroupService(mock_uow)
            
            # Test cleanup inactive groups
            mock_group_repository.cleanup_inactive_groups.return_value = 3
            result = await service.cleanup_inactive_groups(days=90)
            assert result == 3
            
            # Test cleanup empty groups
            mock_group_repository.cleanup_empty_groups.return_value = 2
            result = await service.cleanup_empty_groups()
            assert result == 2