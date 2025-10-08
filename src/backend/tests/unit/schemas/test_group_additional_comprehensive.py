"""
Additional comprehensive unit tests for group Pydantic schemas to achieve 100% coverage.

Tests edge cases and validators in group.py schemas.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError
from src.schemas.group import (
    GroupBase, GroupCreateRequest, GroupUpdateRequest, GroupCreate, GroupUpdate,
    GroupResponse, GroupWithRoleResponse, GroupUserBase, GroupUserCreateRequest,
    GroupUserUpdateRequest, GroupUserResponse, GroupStatsResponse
)
from src.models.enums import GroupStatus, GroupUserRole, GroupUserStatus


class TestGroupUserResponseValidator:
    """Test GroupUserResponse role validator to achieve 100% coverage."""

    def test_migrate_legacy_roles_manager_to_editor(self):
        """Test legacy role migration: manager -> editor."""
        user_response = GroupUserResponse(
            id="test-id",
            group_id="group-123",
            user_id="user-456",
            email="test@example.com",
            role="manager",  # Legacy role
            status=GroupUserStatus.ACTIVE,
            joined_at=datetime.now(),
            auto_created=False,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        assert user_response.role == GroupUserRole.EDITOR

    def test_migrate_legacy_roles_user_to_operator(self):
        """Test legacy role migration: user -> operator."""
        user_response = GroupUserResponse(
            id="test-id",
            group_id="group-123",
            user_id="user-456",
            email="test@example.com",
            role="user",  # Legacy role
            status=GroupUserStatus.ACTIVE,
            joined_at=datetime.now(),
            auto_created=False,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        assert user_response.role == GroupUserRole.OPERATOR

    def test_migrate_legacy_roles_viewer_to_operator(self):
        """Test legacy role migration: viewer -> operator."""
        user_response = GroupUserResponse(
            id="test-id",
            group_id="group-123",
            user_id="user-456",
            email="test@example.com",
            role="viewer",  # Legacy role
            status=GroupUserStatus.ACTIVE,
            joined_at=datetime.now(),
            auto_created=False,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        assert user_response.role == GroupUserRole.OPERATOR

    def test_migrate_legacy_roles_unknown_string_unchanged(self):
        """Test legacy role migration: unknown string that maps to valid enum."""
        user_response = GroupUserResponse(
            id="test-id",
            group_id="group-123",
            user_id="user-456",
            email="test@example.com",
            role="admin",  # Valid enum value not in mapping
            status=GroupUserStatus.ACTIVE,
            joined_at=datetime.now(),
            auto_created=False,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )

        # Should remain unchanged since it's not in the mapping but is valid enum
        assert user_response.role == GroupUserRole.ADMIN

    def test_migrate_legacy_roles_non_string_unchanged(self):
        """Test legacy role migration: non-string values remain unchanged."""
        user_response = GroupUserResponse(
            id="test-id",
            group_id="group-123",
            user_id="user-456",
            email="test@example.com",
            role=GroupUserRole.ADMIN,  # Already proper enum
            status=GroupUserStatus.ACTIVE,
            joined_at=datetime.now(),
            auto_created=False,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        # Should remain unchanged since it's not a string
        assert user_response.role == GroupUserRole.ADMIN

    def test_migrate_legacy_roles_with_all_mappings(self):
        """Test all legacy role mappings comprehensively."""
        legacy_mappings = {
            'manager': GroupUserRole.EDITOR,
            'user': GroupUserRole.OPERATOR,
            'viewer': GroupUserRole.OPERATOR,
        }
        
        for legacy_role, expected_role in legacy_mappings.items():
            user_response = GroupUserResponse(
                id="test-id",
                group_id="group-123",
                user_id="user-456",
                email="test@example.com",
                role=legacy_role,
                status=GroupUserStatus.ACTIVE,
                joined_at=datetime.now(),
                auto_created=False,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            
            assert user_response.role == expected_role


class TestGroupSchemaEdgeCases:
    """Test edge cases for group schemas."""

    def test_group_base_minimum_name_length(self):
        """Test GroupBase with minimum name length."""
        group = GroupBase(name="a")  # Single character
        assert group.name == "a"

    def test_group_base_maximum_name_length(self):
        """Test GroupBase with maximum name length."""
        long_name = "a" * 255  # Maximum length
        group = GroupBase(name=long_name)
        assert group.name == long_name

    def test_group_base_name_too_short(self):
        """Test GroupBase with name too short."""
        with pytest.raises(ValidationError) as exc_info:
            GroupBase(name="")  # Empty string
        assert "String should have at least 1 character" in str(exc_info.value)

    def test_group_base_name_too_long(self):
        """Test GroupBase with name too long."""
        long_name = "a" * 256  # Over maximum length
        with pytest.raises(ValidationError) as exc_info:
            GroupBase(name=long_name)
        assert "String should have at most 255 characters" in str(exc_info.value)

    def test_group_base_maximum_description_length(self):
        """Test GroupBase with maximum description length."""
        long_description = "a" * 1000  # Maximum length
        group = GroupBase(name="test", description=long_description)
        assert group.description == long_description

    def test_group_base_description_too_long(self):
        """Test GroupBase with description too long."""
        long_description = "a" * 1001  # Over maximum length
        with pytest.raises(ValidationError) as exc_info:
            GroupBase(name="test", description=long_description)
        assert "String should have at most 1000 characters" in str(exc_info.value)

    def test_group_update_request_minimum_name_length(self):
        """Test GroupUpdateRequest with minimum name length."""
        update = GroupUpdateRequest(name="a")  # Single character
        assert update.name == "a"

    def test_group_update_request_name_too_short(self):
        """Test GroupUpdateRequest with name too short."""
        with pytest.raises(ValidationError) as exc_info:
            GroupUpdateRequest(name="")  # Empty string
        assert "String should have at least 1 character" in str(exc_info.value)

    def test_group_update_request_all_none(self):
        """Test GroupUpdateRequest with all None values."""
        update = GroupUpdateRequest()
        assert update.name is None
        assert update.description is None
        assert update.status is None

    def test_group_user_create_request_invalid_email(self):
        """Test GroupUserCreateRequest with invalid email."""
        with pytest.raises(ValidationError) as exc_info:
            GroupUserCreateRequest(user_email="invalid-email")
        assert "value is not a valid email address" in str(exc_info.value)

    def test_group_user_update_request_all_none(self):
        """Test GroupUserUpdateRequest with all None values."""
        update = GroupUserUpdateRequest()
        assert update.role is None
        assert update.status is None


class TestBackwardCompatibilityAliases:
    """Test backward compatibility aliases."""

    def test_group_create_alias(self):
        """Test GroupCreate is alias for GroupCreateRequest."""
        assert GroupCreate is GroupCreateRequest

    def test_group_update_alias(self):
        """Test GroupUpdate is alias for GroupUpdateRequest."""
        assert GroupUpdate is GroupUpdateRequest

    def test_group_create_alias_functionality(self):
        """Test GroupCreate alias works functionally."""
        group = GroupCreate(name="test", description="test description")
        assert isinstance(group, GroupCreateRequest)
        assert group.name == "test"
        assert group.description == "test description"

    def test_group_update_alias_functionality(self):
        """Test GroupUpdate alias works functionally."""
        update = GroupUpdate(name="updated", status=GroupStatus.ACTIVE)
        assert isinstance(update, GroupUpdateRequest)
        assert update.name == "updated"
        assert update.status == GroupStatus.ACTIVE


class TestGroupSchemaDefaults:
    """Test default values in group schemas."""

    def test_group_user_base_defaults(self):
        """Test GroupUserBase default values."""
        user = GroupUserBase()
        assert user.role == GroupUserRole.OPERATOR
        assert user.status == GroupUserStatus.ACTIVE

    def test_group_user_create_request_defaults(self):
        """Test GroupUserCreateRequest default values."""
        user = GroupUserCreateRequest(user_email="test@example.com")
        assert user.role == GroupUserRole.OPERATOR

    def test_group_user_update_request_no_defaults(self):
        """Test GroupUserUpdateRequest has no defaults."""
        update = GroupUserUpdateRequest()
        assert update.role is None
        assert update.status is None


class TestGroupSchemaConfigDict:
    """Test ConfigDict settings for group schemas."""

    def test_group_response_config_dict(self):
        """Test GroupResponse ConfigDict settings."""
        config = GroupResponse.model_config
        assert config['from_attributes'] is True

    def test_group_user_response_config_dict(self):
        """Test GroupUserResponse ConfigDict settings."""
        config = GroupUserResponse.model_config
        assert config['from_attributes'] is True

    def test_group_stats_response_config_dict(self):
        """Test GroupStatsResponse ConfigDict settings."""
        config = GroupStatsResponse.model_config
        assert config['from_attributes'] is True


class TestGroupSchemaFieldDescriptions:
    """Test field descriptions are properly set."""

    def test_group_base_field_descriptions(self):
        """Test GroupBase field descriptions."""
        fields = GroupBase.model_fields
        assert fields['name'].description == "Human-readable group name"
        assert fields['description'].description == "Optional group description"

    def test_group_response_field_descriptions(self):
        """Test GroupResponse field descriptions."""
        fields = GroupResponse.model_fields
        assert fields['id'].description == "Unique group identifier"
        assert fields['status'].description == "Group status"
        assert fields['auto_created'].description == "Whether group was auto-created"
        assert fields['created_by_email'].description == "Email of user who created the group"
        assert fields['created_at'].description == "Group creation timestamp"
        assert fields['updated_at'].description == "Group last update timestamp"
        assert fields['user_count'].description == "Number of users in the group"

    def test_group_with_role_response_field_descriptions(self):
        """Test GroupWithRoleResponse field descriptions."""
        fields = GroupWithRoleResponse.model_fields
        assert fields['user_role'].description == "Current user's role in this group"

    def test_group_user_response_field_descriptions(self):
        """Test GroupUserResponse field descriptions."""
        fields = GroupUserResponse.model_fields
        assert fields['id'].description == "Unique group user identifier"
        assert fields['group_id'].description == "Group identifier"
        assert fields['user_id'].description == "User identifier"
        assert fields['email'].description == "User email address"
        assert fields['joined_at'].description == "When user joined the group"
        assert fields['auto_created'].description == "Whether association was auto-created"
        assert fields['created_at'].description == "Association creation timestamp"
        assert fields['updated_at'].description == "Association last update timestamp"

    def test_group_stats_response_field_descriptions(self):
        """Test GroupStatsResponse field descriptions."""
        fields = GroupStatsResponse.model_fields
        assert fields['total_groups'].description == "Total number of groups"
        assert fields['active_groups'].description == "Number of active groups"
        assert fields['auto_created_groups'].description == "Number of auto-created groups"
        assert fields['manual_groups'].description == "Number of manually created groups"
        assert fields['total_users'].description == "Total number of group users"
        assert fields['active_users'].description == "Number of active group users"


class TestGroupSchemaInheritance:
    """Test schema inheritance relationships."""

    def test_group_create_request_inherits_group_base(self):
        """Test GroupCreateRequest inherits from GroupBase."""
        assert issubclass(GroupCreateRequest, GroupBase)

    def test_group_response_inherits_group_base(self):
        """Test GroupResponse inherits from GroupBase."""
        assert issubclass(GroupResponse, GroupBase)

    def test_group_with_role_response_inherits_group_response(self):
        """Test GroupWithRoleResponse inherits from GroupResponse."""
        assert issubclass(GroupWithRoleResponse, GroupResponse)

    def test_group_user_response_inherits_group_user_base(self):
        """Test GroupUserResponse inherits from GroupUserBase."""
        assert issubclass(GroupUserResponse, GroupUserBase)
