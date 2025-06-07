"""
Unit tests for Group model.

Tests the functionality of the Group model including
validation, relationships, and business logic.
"""
import pytest
import uuid
from datetime import datetime, UTC
from unittest.mock import MagicMock, patch

from src.models.group import Group


class TestGroupModel:
    """Test cases for Group model."""
    
    def test_group_model_creation(self):
        """Test basic group model creation."""
        group_id = uuid.uuid4()
        tenant_id = uuid.uuid4()
        
        group = Group(
            id=group_id,
            name="Test Group",
            description="A test group for testing",
            is_active=True,
            tenant_id=tenant_id
        )
        
        assert group.id == group_id
        assert group.name == "Test Group"
        assert group.description == "A test group for testing"
        assert group.is_active is True
        assert group.tenant_id == tenant_id
    
    def test_group_model_defaults(self):
        """Test group model default values."""
        group = Group(
            name="Test Group",
            tenant_id=uuid.uuid4()
        )
        
        # Test default values
        assert group.is_active is True
        assert group.created_at is not None
        assert group.updated_at is not None
        assert isinstance(group.id, uuid.UUID)
    
    def test_group_name_validation(self):
        """Test group name validation."""
        # Test valid names
        valid_names = [
            "Developers",
            "Marketing Team",
            "QA-Engineers",
            "Support_Staff",
            "Project Managers"
        ]
        
        for name in valid_names:
            group = Group(name=name, tenant_id=uuid.uuid4())
            assert group.name == name
    
    def test_group_name_length_limits(self):
        """Test group name length validation."""
        tenant_id = uuid.uuid4()
        
        # Test minimum length
        with pytest.raises(ValueError, match="Group name must be at least 2 characters"):
            group = Group(name="A", tenant_id=tenant_id)
            group.validate_name()
        
        # Test maximum length
        long_name = "A" * 101  # Assuming 100 char limit
        with pytest.raises(ValueError, match="Group name cannot exceed 100 characters"):
            group = Group(name=long_name, tenant_id=tenant_id)
            group.validate_name()
    
    def test_group_description_validation(self):
        """Test group description validation."""
        tenant_id = uuid.uuid4()
        
        # Test valid description
        group = Group(
            name="Test Group",
            description="A valid description for the group",
            tenant_id=tenant_id
        )
        assert group.description == "A valid description for the group"
        
        # Test empty description (should be allowed)
        group = Group(name="Test Group", description="", tenant_id=tenant_id)
        assert group.description == ""
        
        # Test None description (should be allowed)
        group = Group(name="Test Group", description=None, tenant_id=tenant_id)
        assert group.description is None
    
    def test_group_hierarchy_relationships(self):
        """Test group hierarchy with parent-child relationships."""
        parent_group = Group(
            name="Parent Group",
            tenant_id=uuid.uuid4()
        )
        
        child_group = Group(
            name="Child Group",
            parent_id=parent_group.id,
            tenant_id=parent_group.tenant_id
        )
        
        assert child_group.parent_id == parent_group.id
        assert child_group.tenant_id == parent_group.tenant_id
    
    def test_group_member_relationships(self):
        """Test group member relationships."""
        group = Group(
            name="Test Group",
            tenant_id=uuid.uuid4()
        )
        
        # Mock members relationship
        mock_member = MagicMock()
        mock_member.id = uuid.uuid4()
        mock_member.username = "testuser"
        
        # Simulate adding members (in real scenario, this would be through SQLAlchemy relationship)
        group.members = [mock_member]
        
        assert len(group.members) == 1
        assert group.members[0].username == "testuser"
    
    def test_group_permission_inheritance(self):
        """Test group permission inheritance logic."""
        parent_group = Group(
            name="Parent Group",
            tenant_id=uuid.uuid4()
        )
        parent_group.permissions = ["read", "write"]
        
        child_group = Group(
            name="Child Group",
            parent_id=parent_group.id,
            tenant_id=parent_group.tenant_id
        )
        child_group.permissions = ["execute"]
        
        # Test inherited permissions method
        inherited_permissions = child_group.get_inherited_permissions(parent_permissions=parent_group.permissions)
        
        assert "read" in inherited_permissions
        assert "write" in inherited_permissions
        assert "execute" in inherited_permissions
    
    def test_group_activation_deactivation(self):
        """Test group activation and deactivation."""
        group = Group(
            name="Test Group",
            tenant_id=uuid.uuid4(),
            is_active=True
        )
        
        # Test deactivation
        group.deactivate()
        assert group.is_active is False
        assert group.updated_at is not None
        
        # Test reactivation
        group.activate()
        assert group.is_active is True
    
    def test_group_member_count_property(self):
        """Test group member count property."""
        group = Group(
            name="Test Group",
            tenant_id=uuid.uuid4()
        )
        
        # Mock members
        mock_members = [MagicMock() for _ in range(5)]
        group.members = mock_members
        
        assert group.member_count == 5
    
    def test_group_is_empty_property(self):
        """Test group is_empty property."""
        group = Group(
            name="Test Group",
            tenant_id=uuid.uuid4()
        )
        
        # Test empty group
        group.members = []
        assert group.is_empty is True
        
        # Test non-empty group
        group.members = [MagicMock()]
        assert group.is_empty is False
    
    def test_group_can_be_deleted_method(self):
        """Test group deletion validation."""
        group = Group(
            name="Test Group",
            tenant_id=uuid.uuid4()
        )
        
        # Test can delete empty group
        group.members = []
        assert group.can_be_deleted() is True
        
        # Test cannot delete group with members
        group.members = [MagicMock()]
        assert group.can_be_deleted() is False
        
        # Test force delete option
        assert group.can_be_deleted(force=True) is True
    
    def test_group_add_member_method(self):
        """Test adding member to group."""
        group = Group(
            name="Test Group",
            tenant_id=uuid.uuid4()
        )
        
        mock_user = MagicMock()
        mock_user.id = uuid.uuid4()
        mock_user.tenant_id = group.tenant_id
        
        # Test adding valid member
        result = group.add_member(mock_user)
        assert result is True
        
        # Test adding member from different tenant
        different_tenant_user = MagicMock()
        different_tenant_user.id = uuid.uuid4()
        different_tenant_user.tenant_id = uuid.uuid4()
        
        with pytest.raises(ValueError, match="User must belong to the same tenant"):
            group.add_member(different_tenant_user)
    
    def test_group_remove_member_method(self):
        """Test removing member from group."""
        group = Group(
            name="Test Group",
            tenant_id=uuid.uuid4()
        )
        
        mock_user = MagicMock()
        mock_user.id = uuid.uuid4()
        
        # Mock that user is a member
        group.members = [mock_user]
        
        # Test removing existing member
        result = group.remove_member(mock_user.id)
        assert result is True
        
        # Test removing non-member
        non_member_id = uuid.uuid4()
        result = group.remove_member(non_member_id)
        assert result is False
    
    def test_group_has_member_method(self):
        """Test checking if user is member of group."""
        group = Group(
            name="Test Group",
            tenant_id=uuid.uuid4()
        )
        
        mock_user = MagicMock()
        mock_user.id = uuid.uuid4()
        group.members = [mock_user]
        
        # Test existing member
        assert group.has_member(mock_user.id) is True
        
        # Test non-member
        non_member_id = uuid.uuid4()
        assert group.has_member(non_member_id) is False
    
    def test_group_get_member_roles_method(self):
        """Test getting member roles within group."""
        group = Group(
            name="Test Group",
            tenant_id=uuid.uuid4()
        )
        
        mock_user = MagicMock()
        mock_user.id = uuid.uuid4()
        mock_user.group_roles = {"group_admin": True, "group_member": True}
        
        group.members = [mock_user]
        
        member_roles = group.get_member_roles(mock_user.id)
        assert "group_admin" in member_roles
        assert "group_member" in member_roles
    
    def test_group_search_members_method(self):
        """Test searching members within group."""
        group = Group(
            name="Test Group",
            tenant_id=uuid.uuid4()
        )
        
        # Mock members
        user1 = MagicMock()
        user1.username = "john.doe"
        user1.email = "john@example.com"
        
        user2 = MagicMock()
        user2.username = "jane.smith"
        user2.email = "jane@example.com"
        
        group.members = [user1, user2]
        
        # Test search by username
        results = group.search_members("john")
        assert len(results) == 1
        assert results[0].username == "john.doe"
        
        # Test search by email domain
        results = group.search_members("example.com")
        assert len(results) == 2
    
    def test_group_export_data_method(self):
        """Test exporting group data."""
        group = Group(
            name="Test Group",
            description="Test description",
            tenant_id=uuid.uuid4(),
            created_at=datetime.now(UTC)
        )
        
        # Mock members
        mock_user = MagicMock()
        mock_user.id = str(uuid.uuid4())
        mock_user.username = "testuser"
        mock_user.email = "test@example.com"
        
        group.members = [mock_user]
        
        export_data = group.export_data()
        
        assert export_data["name"] == "Test Group"
        assert export_data["description"] == "Test description"
        assert export_data["is_active"] is True
        assert export_data["member_count"] == 1
        assert len(export_data["members"]) == 1
        assert export_data["members"][0]["username"] == "testuser"
    
    def test_group_clone_method(self):
        """Test cloning group."""
        original_group = Group(
            name="Original Group",
            description="Original description",
            tenant_id=uuid.uuid4()
        )
        
        cloned_group = original_group.clone(new_name="Cloned Group")
        
        assert cloned_group.name == "Cloned Group"
        assert cloned_group.description == "Original description"
        assert cloned_group.tenant_id == original_group.tenant_id
        assert cloned_group.id != original_group.id
        assert cloned_group.created_at != original_group.created_at
    
    def test_group_str_representation(self):
        """Test group string representation."""
        group = Group(
            name="Test Group",
            tenant_id=uuid.uuid4()
        )
        
        str_repr = str(group)
        assert "Test Group" in str_repr
        assert group.tenant_id.hex in str_repr
    
    def test_group_repr_representation(self):
        """Test group repr representation."""
        group = Group(
            name="Test Group",
            tenant_id=uuid.uuid4()
        )
        
        repr_str = repr(group)
        assert "Group" in repr_str
        assert "Test Group" in repr_str
        assert str(group.id) in repr_str
    
    def test_group_equality_comparison(self):
        """Test group equality comparison."""
        group_id = uuid.uuid4()
        tenant_id = uuid.uuid4()
        
        group1 = Group(
            id=group_id,
            name="Test Group",
            tenant_id=tenant_id
        )
        
        group2 = Group(
            id=group_id,
            name="Test Group",
            tenant_id=tenant_id
        )
        
        group3 = Group(
            name="Different Group",
            tenant_id=tenant_id
        )
        
        # Test equality
        assert group1 == group2
        assert group1 != group3
        
        # Test hash
        assert hash(group1) == hash(group2)
        assert hash(group1) != hash(group3)