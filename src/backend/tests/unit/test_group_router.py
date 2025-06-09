"""
Unit tests for GroupRouter.

Tests the functionality of the group router including
group CRUD operations, member management, and access control.
"""
import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException

from src.schemas.group import GroupCreate, GroupUpdate


@pytest.fixture
def mock_group_service():
    """Create a mock group service."""
    service = AsyncMock()
    
    # Create mock group objects
    mock_group = MagicMock()
    mock_group.id = uuid.uuid4()
    mock_group.name = "Developers"
    mock_group.description = "Development team group"
    mock_group.is_active = True
    mock_group.member_count = 12
    mock_group.created_at = "2024-01-01T00:00:00Z"
    
    # Setup service method returns
    service.get.return_value = mock_group
    service.list.return_value = [mock_group]
    service.create.return_value = mock_group
    service.update.return_value = mock_group
    service.delete.return_value = True
    service.get_by_name.return_value = mock_group
    
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


class TestGroupRouter:
    """Test cases for GroupRouter."""
    
    @pytest.mark.asyncio
    async def test_create_group_success(self, mock_group_service, mock_current_user, group_create_data):
        """Test successful group creation."""
        with patch("src.api.group_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.group_router.GroupService", return_value=mock_group_service):
            
            from src.api.group_router import create_group
            
            result = await create_group(group_create_data, current_user=mock_current_user)
            
            assert result is not None
            assert result.name == "Developers"
            mock_group_service.create.assert_called_once_with(group_create_data)
    
    @pytest.mark.asyncio
    async def test_create_group_unauthorized(self, mock_group_service, group_create_data):
        """Test group creation without proper permissions."""
        unauthorized_user = MagicMock()
        unauthorized_user.id = uuid.uuid4()
        unauthorized_user.is_superuser = False
        unauthorized_user.roles = ["user"]
        
        with patch("src.api.group_router.get_current_user", return_value=unauthorized_user), \
             patch("src.api.group_router.GroupService", return_value=mock_group_service):
            
            from src.api.group_router import create_group
            
            with pytest.raises(HTTPException) as exc_info:
                await create_group(group_create_data, current_user=unauthorized_user)
            
            assert exc_info.value.status_code == 403
    
    @pytest.mark.asyncio
    async def test_get_group_success(self, mock_group_service, mock_current_user):
        """Test successful group retrieval."""
        group_id = uuid.uuid4()
        
        with patch("src.api.group_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.group_router.GroupService", return_value=mock_group_service):
            
            from src.api.group_router import get_group
            
            result = await get_group(group_id, current_user=mock_current_user)
            
            assert result is not None
            assert result.name == "Developers"
            mock_group_service.get.assert_called_once_with(group_id)
    
    @pytest.mark.asyncio
    async def test_get_group_not_found(self, mock_group_service, mock_current_user):
        """Test getting a non-existent group."""
        group_id = uuid.uuid4()
        mock_group_service.get.return_value = None
        
        with patch("src.api.group_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.group_router.GroupService", return_value=mock_group_service):
            
            from src.api.group_router import get_group
            
            with pytest.raises(HTTPException) as exc_info:
                await get_group(group_id, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 404
    
    @pytest.mark.asyncio
    async def test_list_groups_success(self, mock_group_service, mock_current_user):
        """Test successful group listing."""
        with patch("src.api.group_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.group_router.GroupService", return_value=mock_group_service):
            
            from src.api.group_router import list_groups
            
            result = await list_groups(current_user=mock_current_user)
            
            assert len(result) == 1
            assert result[0].name == "Developers"
            mock_group_service.list.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_group_success(self, mock_group_service, mock_current_user, group_update_data):
        """Test successful group update."""
        group_id = uuid.uuid4()
        
        with patch("src.api.group_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.group_router.GroupService", return_value=mock_group_service):
            
            from src.api.group_router import update_group
            
            result = await update_group(group_id, group_update_data, current_user=mock_current_user)
            
            assert result is not None
            mock_group_service.update.assert_called_once_with(group_id, group_update_data)
    
    @pytest.mark.asyncio
    async def test_update_group_not_found(self, mock_group_service, mock_current_user, group_update_data):
        """Test updating a non-existent group."""
        group_id = uuid.uuid4()
        mock_group_service.update.return_value = None
        
        with patch("src.api.group_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.group_router.GroupService", return_value=mock_group_service):
            
            from src.api.group_router import update_group
            
            with pytest.raises(HTTPException) as exc_info:
                await update_group(group_id, group_update_data, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 404
    
    @pytest.mark.asyncio
    async def test_delete_group_success(self, mock_group_service, mock_current_user):
        """Test successful group deletion."""
        group_id = uuid.uuid4()
        
        with patch("src.api.group_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.group_router.GroupService", return_value=mock_group_service):
            
            from src.api.group_router import delete_group
            
            result = await delete_group(group_id, current_user=mock_current_user)
            
            assert result["message"] == "Group deleted successfully"
            mock_group_service.delete.assert_called_once_with(group_id)
    
    @pytest.mark.asyncio
    async def test_delete_group_not_found(self, mock_group_service, mock_current_user):
        """Test deleting a non-existent group."""
        group_id = uuid.uuid4()
        mock_group_service.delete.return_value = False
        
        with patch("src.api.group_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.group_router.GroupService", return_value=mock_group_service):
            
            from src.api.group_router import delete_group
            
            with pytest.raises(HTTPException) as exc_info:
                await delete_group(group_id, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 404
    
    @pytest.mark.asyncio
    async def test_add_member_to_group(self, mock_group_service, mock_current_user):
        """Test adding member to group."""
        group_id = uuid.uuid4()
        user_id = uuid.uuid4()
        
        with patch("src.api.group_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.group_router.GroupService", return_value=mock_group_service):
            
            from src.api.group_router import add_member_to_group
            
            result = await add_member_to_group(group_id, {"user_id": str(user_id)}, current_user=mock_current_user)
            
            assert result["message"] == "Member added successfully"
            mock_group_service.add_member.assert_called_once_with(group_id, user_id)
    
    @pytest.mark.asyncio
    async def test_remove_member_from_group(self, mock_group_service, mock_current_user):
        """Test removing member from group."""
        group_id = uuid.uuid4()
        user_id = uuid.uuid4()
        
        with patch("src.api.group_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.group_router.GroupService", return_value=mock_group_service):
            
            from src.api.group_router import remove_member_from_group
            
            result = await remove_member_from_group(group_id, user_id, current_user=mock_current_user)
            
            assert result["message"] == "Member removed successfully"
            mock_group_service.remove_member.assert_called_once_with(group_id, user_id)
    
    @pytest.mark.asyncio
    async def test_get_group_members(self, mock_group_service, mock_current_user):
        """Test getting group members."""
        group_id = uuid.uuid4()
        mock_members = [
            {"id": uuid.uuid4(), "username": "user1", "email": "user1@test.com"},
            {"id": uuid.uuid4(), "username": "user2", "email": "user2@test.com"}
        ]
        mock_group_service.get_members.return_value = mock_members
        
        with patch("src.api.group_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.group_router.GroupService", return_value=mock_group_service):
            
            from src.api.group_router import get_group_members
            
            result = await get_group_members(group_id, current_user=mock_current_user)
            
            assert len(result) == 2
            assert result[0]["username"] == "user1"
            mock_group_service.get_members.assert_called_once_with(group_id)
    
    @pytest.mark.asyncio
    async def test_bulk_add_members(self, mock_group_service, mock_current_user):
        """Test bulk adding members to group."""
        group_id = uuid.uuid4()
        user_ids = [str(uuid.uuid4()), str(uuid.uuid4())]
        
        with patch("src.api.group_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.group_router.GroupService", return_value=mock_group_service):
            
            from src.api.group_router import bulk_add_members
            
            result = await bulk_add_members(
                group_id, 
                {"user_ids": user_ids}, 
                current_user=mock_current_user
            )
            
            assert result["message"] == "Members added successfully"
            assert result["added_count"] == 2
            mock_group_service.bulk_add_members.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_group_validation(self, mock_group_service, mock_current_user):
        """Test group data validation."""
        invalid_group_data = GroupCreate(
            name="",  # Invalid empty name
            description="Test group",
            is_active=True
        )
        
        with patch("src.api.group_router.get_current_user", return_value=mock_current_user), \
             patch("src.api.group_router.GroupService", return_value=mock_group_service):
            
            mock_group_service.create.side_effect = ValueError("Invalid group data")
            
            from src.api.group_router import create_group
            
            with pytest.raises(HTTPException) as exc_info:
                await create_group(invalid_group_data, current_user=mock_current_user)
            
            assert exc_info.value.status_code == 422