"""
Comprehensive unit tests for group schemas.

Tests all Pydantic models for group management API.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError

from src.schemas.group import (
    GroupBase,
    GroupCreateRequest,
    GroupUpdateRequest,
    GroupCreate,
    GroupUpdate,
    GroupResponse,
    GroupWithRoleResponse,
    GroupUserBase,
    GroupUserCreateRequest,
    GroupUserUpdateRequest,
    GroupUserResponse,
    GroupStatsResponse
)
from src.models.enums import GroupStatus, GroupUserRole, GroupUserStatus


class TestGroupBase:
    """Test GroupBase schema."""

    def test_group_base_minimal(self):
        """Test GroupBase with minimal data."""
        group = GroupBase(name="Test Group")
        
        assert group.name == "Test Group"
        assert group.description is None

    def test_group_base_full(self):
        """Test GroupBase with full data."""
        group = GroupBase(
            name="Test Group",
            description="Test description"
        )
        
        assert group.name == "Test Group"
        assert group.description == "Test description"

    def test_group_base_name_validation(self):
        """Test GroupBase name validation."""
        # Test empty name
        with pytest.raises(ValidationError):
            GroupBase(name="")
        
        # Test too long name
        with pytest.raises(ValidationError):
            GroupBase(name="x" * 256)
        
        # Test valid name
        group = GroupBase(name="x" * 255)
        assert group.name == "x" * 255

    def test_group_base_description_validation(self):
        """Test GroupBase description validation."""
        # Test too long description
        with pytest.raises(ValidationError):
            GroupBase(name="Test", description="x" * 1001)
        
        # Test valid description
        group = GroupBase(name="Test", description="x" * 1000)
        assert group.description == "x" * 1000


class TestGroupCreateRequest:
    """Test GroupCreateRequest schema."""

    def test_group_create_request_inherits_group_base(self):
        """Test GroupCreateRequest inherits from GroupBase."""
        assert issubclass(GroupCreateRequest, GroupBase)

    def test_group_create_request_minimal(self):
        """Test GroupCreateRequest with minimal data."""
        request = GroupCreateRequest(name="Test Group")
        
        assert request.name == "Test Group"
        assert request.description is None

    def test_group_create_request_full(self):
        """Test GroupCreateRequest with full data."""
        request = GroupCreateRequest(
            name="Test Group",
            description="Test description"
        )
        
        assert request.name == "Test Group"
        assert request.description == "Test description"


class TestGroupUpdateRequest:
    """Test GroupUpdateRequest schema."""

    def test_group_update_request_all_optional(self):
        """Test GroupUpdateRequest with all fields optional."""
        request = GroupUpdateRequest()
        
        assert request.name is None
        assert request.description is None
        assert request.status is None

    def test_group_update_request_partial(self):
        """Test GroupUpdateRequest with partial data."""
        request = GroupUpdateRequest(name="Updated Name")
        
        assert request.name == "Updated Name"
        assert request.description is None
        assert request.status is None

    def test_group_update_request_full(self):
        """Test GroupUpdateRequest with full data."""
        request = GroupUpdateRequest(
            name="Updated Name",
            description="Updated description",
            status=GroupStatus.SUSPENDED
        )

        assert request.name == "Updated Name"
        assert request.description == "Updated description"
        assert request.status == GroupStatus.SUSPENDED

    def test_group_update_request_name_validation(self):
        """Test GroupUpdateRequest name validation."""
        # Test empty name
        with pytest.raises(ValidationError):
            GroupUpdateRequest(name="")
        
        # Test too long name
        with pytest.raises(ValidationError):
            GroupUpdateRequest(name="x" * 256)


class TestBackwardCompatibilityAliases:
    """Test backward compatibility aliases."""

    def test_group_create_alias(self):
        """Test GroupCreate is alias for GroupCreateRequest."""
        assert GroupCreate is GroupCreateRequest

    def test_group_update_alias(self):
        """Test GroupUpdate is alias for GroupUpdateRequest."""
        assert GroupUpdate is GroupUpdateRequest


class TestGroupResponse:
    """Test GroupResponse schema."""

    def test_group_response_inherits_group_base(self):
        """Test GroupResponse inherits from GroupBase."""
        assert issubclass(GroupResponse, GroupBase)

    def test_group_response_minimal(self):
        """Test GroupResponse with minimal data."""
        now = datetime.now()
        response = GroupResponse(
            name="Test Group",
            id="group-123",
            status=GroupStatus.ACTIVE,
            auto_created=False,
            created_at=now,
            updated_at=now,
            user_count=5
        )
        
        assert response.name == "Test Group"
        assert response.id == "group-123"
        assert response.status == GroupStatus.ACTIVE
        assert response.auto_created is False
        assert response.created_by_email is None
        assert response.created_at == now
        assert response.updated_at == now
        assert response.user_count == 5

    def test_group_response_full(self):
        """Test GroupResponse with full data."""
        now = datetime.now()
        response = GroupResponse(
            name="Test Group",
            description="Test description",
            id="group-123",
            status=GroupStatus.ACTIVE,
            auto_created=True,
            created_by_email="test@example.com",
            created_at=now,
            updated_at=now,
            user_count=10
        )
        
        assert response.name == "Test Group"
        assert response.description == "Test description"
        assert response.id == "group-123"
        assert response.status == GroupStatus.ACTIVE
        assert response.auto_created is True
        assert response.created_by_email == "test@example.com"
        assert response.created_at == now
        assert response.updated_at == now
        assert response.user_count == 10

    def test_group_response_config(self):
        """Test GroupResponse model config."""
        assert hasattr(GroupResponse, 'model_config')
        # ConfigDict creates a dict, not an object with attributes
        assert GroupResponse.model_config['from_attributes'] is True


class TestGroupWithRoleResponse:
    """Test GroupWithRoleResponse schema."""

    def test_group_with_role_response_inherits_group_response(self):
        """Test GroupWithRoleResponse inherits from GroupResponse."""
        assert issubclass(GroupWithRoleResponse, GroupResponse)

    def test_group_with_role_response_minimal(self):
        """Test GroupWithRoleResponse with minimal data."""
        now = datetime.now()
        response = GroupWithRoleResponse(
            name="Test Group",
            id="group-123",
            status=GroupStatus.ACTIVE,
            auto_created=False,
            created_at=now,
            updated_at=now,
            user_count=5
        )
        
        assert response.user_role is None

    def test_group_with_role_response_with_role(self):
        """Test GroupWithRoleResponse with user role."""
        now = datetime.now()
        response = GroupWithRoleResponse(
            name="Test Group",
            id="group-123",
            status=GroupStatus.ACTIVE,
            auto_created=False,
            created_at=now,
            updated_at=now,
            user_count=5,
            user_role=GroupUserRole.ADMIN
        )
        
        assert response.user_role == GroupUserRole.ADMIN


class TestGroupUserBase:
    """Test GroupUserBase schema."""

    def test_group_user_base_defaults(self):
        """Test GroupUserBase with default values."""
        user = GroupUserBase()
        
        assert user.role == GroupUserRole.OPERATOR
        assert user.status == GroupUserStatus.ACTIVE

    def test_group_user_base_custom_values(self):
        """Test GroupUserBase with custom values."""
        user = GroupUserBase(
            role=GroupUserRole.ADMIN,
            status=GroupUserStatus.INACTIVE
        )
        
        assert user.role == GroupUserRole.ADMIN
        assert user.status == GroupUserStatus.INACTIVE


class TestGroupUserCreateRequest:
    """Test GroupUserCreateRequest schema."""

    def test_group_user_create_request_minimal(self):
        """Test GroupUserCreateRequest with minimal data."""
        request = GroupUserCreateRequest(user_email="test@example.com")
        
        assert request.user_email == "test@example.com"
        assert request.role == GroupUserRole.OPERATOR

    def test_group_user_create_request_full(self):
        """Test GroupUserCreateRequest with full data."""
        request = GroupUserCreateRequest(
            user_email="test@example.com",
            role=GroupUserRole.ADMIN
        )
        
        assert request.user_email == "test@example.com"
        assert request.role == GroupUserRole.ADMIN

    def test_group_user_create_request_email_validation(self):
        """Test GroupUserCreateRequest email validation."""
        # Test invalid email
        with pytest.raises(ValidationError):
            GroupUserCreateRequest(user_email="invalid-email")
        
        # Test valid email
        request = GroupUserCreateRequest(user_email="valid@example.com")
        assert request.user_email == "valid@example.com"


class TestGroupUserUpdateRequest:
    """Test GroupUserUpdateRequest schema."""

    def test_group_user_update_request_all_optional(self):
        """Test GroupUserUpdateRequest with all fields optional."""
        request = GroupUserUpdateRequest()
        
        assert request.role is None
        assert request.status is None

    def test_group_user_update_request_partial(self):
        """Test GroupUserUpdateRequest with partial data."""
        request = GroupUserUpdateRequest(role=GroupUserRole.EDITOR)
        
        assert request.role == GroupUserRole.EDITOR
        assert request.status is None

    def test_group_user_update_request_full(self):
        """Test GroupUserUpdateRequest with full data."""
        request = GroupUserUpdateRequest(
            role=GroupUserRole.ADMIN,
            status=GroupUserStatus.INACTIVE
        )
        
        assert request.role == GroupUserRole.ADMIN
        assert request.status == GroupUserStatus.INACTIVE


class TestGroupUserResponse:
    """Test GroupUserResponse schema."""

    def test_group_user_response_inherits_group_user_base(self):
        """Test GroupUserResponse inherits from GroupUserBase."""
        assert issubclass(GroupUserResponse, GroupUserBase)

    def test_group_user_response_minimal(self):
        """Test GroupUserResponse with minimal data."""
        now = datetime.now()
        response = GroupUserResponse(
            id="user-123",
            group_id="group-123",
            user_id="user-456",
            email="test@example.com",
            joined_at=now,
            auto_created=False,
            created_at=now,
            updated_at=now
        )
        
        assert response.id == "user-123"
        assert response.group_id == "group-123"
        assert response.user_id == "user-456"
        assert response.email == "test@example.com"
        assert response.joined_at == now
        assert response.auto_created is False
        assert response.created_at == now
        assert response.updated_at == now
        assert response.role == GroupUserRole.OPERATOR  # Default from base
        assert response.status == GroupUserStatus.ACTIVE  # Default from base

    def test_group_user_response_full(self):
        """Test GroupUserResponse with full data."""
        now = datetime.now()
        response = GroupUserResponse(
            id="user-123",
            group_id="group-123",
            user_id="user-456",
            email="test@example.com",
            joined_at=now,
            auto_created=True,
            created_at=now,
            updated_at=now,
            role=GroupUserRole.ADMIN,
            status=GroupUserStatus.INACTIVE
        )
        
        assert response.role == GroupUserRole.ADMIN
        assert response.status == GroupUserStatus.INACTIVE

    def test_group_user_response_role_migration(self):
        """Test GroupUserResponse role migration validator."""
        now = datetime.now()
        
        # Test legacy role mapping
        response = GroupUserResponse(
            id="user-123",
            group_id="group-123",
            user_id="user-456",
            email="test@example.com",
            joined_at=now,
            auto_created=False,
            created_at=now,
            updated_at=now,
            role="manager"  # Legacy role
        )
        
        assert response.role == "editor"  # Should be migrated

    def test_group_user_response_role_migration_all_mappings(self):
        """Test all role migration mappings."""
        now = datetime.now()
        base_data = {
            "id": "user-123",
            "group_id": "group-123",
            "user_id": "user-456",
            "email": "test@example.com",
            "joined_at": now,
            "auto_created": False,
            "created_at": now,
            "updated_at": now
        }
        
        # Test all legacy mappings
        mappings = {
            "manager": "editor",
            "user": "operator",
            "viewer": "operator"
        }
        
        for legacy_role, expected_role in mappings.items():
            response = GroupUserResponse(**base_data, role=legacy_role)
            assert response.role == expected_role

    def test_group_user_response_role_migration_passthrough(self):
        """Test role migration with valid enum role."""
        now = datetime.now()
        response = GroupUserResponse(
            id="user-123",
            group_id="group-123",
            user_id="user-456",
            email="test@example.com",
            joined_at=now,
            auto_created=False,
            created_at=now,
            updated_at=now,
            role=GroupUserRole.ADMIN  # Valid enum role
        )

        assert response.role == GroupUserRole.ADMIN  # Should remain unchanged

    def test_group_user_response_config(self):
        """Test GroupUserResponse model config."""
        assert hasattr(GroupUserResponse, 'model_config')
        # ConfigDict creates a dict, not an object with attributes
        assert GroupUserResponse.model_config['from_attributes'] is True


class TestGroupStatsResponse:
    """Test GroupStatsResponse schema."""

    def test_group_stats_response_minimal(self):
        """Test GroupStatsResponse with minimal data."""
        stats = GroupStatsResponse(
            total_groups=10,
            active_groups=8,
            auto_created_groups=5,
            manual_groups=5,
            total_users=50,
            active_users=45
        )
        
        assert stats.total_groups == 10
        assert stats.active_groups == 8
        assert stats.auto_created_groups == 5
        assert stats.manual_groups == 5
        assert stats.total_users == 50
        assert stats.active_users == 45

    def test_group_stats_response_zero_values(self):
        """Test GroupStatsResponse with zero values."""
        stats = GroupStatsResponse(
            total_groups=0,
            active_groups=0,
            auto_created_groups=0,
            manual_groups=0,
            total_users=0,
            active_users=0
        )
        
        assert stats.total_groups == 0
        assert stats.active_groups == 0
        assert stats.auto_created_groups == 0
        assert stats.manual_groups == 0
        assert stats.total_users == 0
        assert stats.active_users == 0

    def test_group_stats_response_config(self):
        """Test GroupStatsResponse model config."""
        assert hasattr(GroupStatsResponse, 'model_config')
        # ConfigDict creates a dict, not an object with attributes
        assert GroupStatsResponse.model_config['from_attributes'] is True
