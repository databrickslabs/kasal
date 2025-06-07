"""
Unit tests for group schemas.

Tests the functionality of Pydantic schemas for group management
including validation, serialization, and field constraints.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError

from src.schemas.group import (
    GroupBase, GroupCreateRequest, GroupUpdateRequest, GroupResponse,
    GroupUserBase, GroupUserCreateRequest, GroupUserUpdateRequest,
    GroupUserResponse, GroupStatsResponse
)
from src.models.enums import GroupStatus, GroupUserRole, GroupUserStatus


class TestGroupBase:
    """Test cases for GroupBase schema."""
    
    def test_valid_group_base(self):
        """Test valid GroupBase creation."""
        group_data = {
            "name": "Test Group",
            "email_domain": "test.com",
            "description": "A test group"
        }
        
        group = GroupBase(**group_data)
        
        assert group.name == "Test Group"
        assert group.email_domain == "test.com"
        assert group.description == "A test group"
    
    def test_group_base_without_description(self):
        """Test GroupBase without optional description."""
        group_data = {
            "name": "Test Group",
            "email_domain": "test.com"
        }
        
        group = GroupBase(**group_data)
        
        assert group.name == "Test Group"
        assert group.email_domain == "test.com"
        assert group.description is None
    
    def test_group_base_empty_name(self):
        """Test GroupBase with empty name fails validation."""
        group_data = {
            "name": "",
            "email_domain": "test.com"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            GroupBase(**group_data)
        
        assert "at least 1 characters" in str(exc_info.value)
    
    def test_group_base_long_name(self):
        """Test GroupBase with too long name fails validation."""
        group_data = {
            "name": "x" * 256,  # Exceeds max_length of 255
            "email_domain": "test.com"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            GroupBase(**group_data)
        
        assert "at most 255 characters" in str(exc_info.value)
    
    def test_group_base_empty_email_domain(self):
        """Test GroupBase with empty email domain fails validation."""
        group_data = {
            "name": "Test Group",
            "email_domain": ""
        }
        
        with pytest.raises(ValidationError) as exc_info:
            GroupBase(**group_data)
        
        assert "at least 1 characters" in str(exc_info.value)
    
    def test_group_base_long_description(self):
        """Test GroupBase with too long description fails validation."""
        group_data = {
            "name": "Test Group",
            "email_domain": "test.com",
            "description": "x" * 1001  # Exceeds max_length of 1000
        }
        
        with pytest.raises(ValidationError) as exc_info:
            GroupBase(**group_data)
        
        assert "at most 1000 characters" in str(exc_info.value)


class TestGroupCreateRequest:
    """Test cases for GroupCreateRequest schema."""
    
    def test_valid_group_create_request(self):
        """Test valid GroupCreateRequest creation."""
        create_data = {
            "name": "New Group",
            "email_domain": "newgroup.com",
            "description": "A new group for testing"
        }
        
        request = GroupCreateRequest(**create_data)
        
        assert request.name == "New Group"
        assert request.email_domain == "newgroup.com"
        assert request.description == "A new group for testing"
    
    def test_group_create_request_inherits_validation(self):
        """Test that GroupCreateRequest inherits validation from GroupBase."""
        create_data = {
            "name": "",  # Invalid
            "email_domain": "test.com"
        }
        
        with pytest.raises(ValidationError):
            GroupCreateRequest(**create_data)


class TestGroupUpdateRequest:
    """Test cases for GroupUpdateRequest schema."""
    
    def test_valid_group_update_request(self):
        """Test valid GroupUpdateRequest creation."""
        update_data = {
            "name": "Updated Group",
            "email_domain": "updated.com",
            "description": "Updated description",
            "status": GroupStatus.ACTIVE
        }
        
        request = GroupUpdateRequest(**update_data)
        
        assert request.name == "Updated Group"
        assert request.email_domain == "updated.com"
        assert request.description == "Updated description"
        assert request.status == GroupStatus.ACTIVE
    
    def test_group_update_request_all_optional(self):
        """Test GroupUpdateRequest with all optional fields."""
        request = GroupUpdateRequest()
        
        assert request.name is None
        assert request.email_domain is None
        assert request.description is None
        assert request.status is None
    
    def test_group_update_request_partial(self):
        """Test GroupUpdateRequest with partial data."""
        update_data = {
            "name": "Partially Updated Group"
        }
        
        request = GroupUpdateRequest(**update_data)
        
        assert request.name == "Partially Updated Group"
        assert request.email_domain is None
        assert request.status is None
    
    def test_group_update_request_invalid_status(self):
        """Test GroupUpdateRequest with invalid status."""
        update_data = {
            "status": "INVALID_STATUS"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            GroupUpdateRequest(**update_data)
        
        assert "Input should be" in str(exc_info.value)


class TestGroupResponse:
    """Test cases for GroupResponse schema."""
    
    def test_valid_group_response(self):
        """Test valid GroupResponse creation."""
        response_data = {
            "id": "group_123",
            "name": "Response Group",
            "email_domain": "response.com",
            "description": "A response group",
            "status": GroupStatus.ACTIVE,
            "auto_created": False,
            "created_by_email": "admin@response.com",
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "user_count": 5
        }
        
        response = GroupResponse(**response_data)
        
        assert response.id == "group_123"
        assert response.name == "Response Group"
        assert response.status == GroupStatus.ACTIVE
        assert response.auto_created is False
        assert response.user_count == 5
    
    def test_group_response_without_optional_fields(self):
        """Test GroupResponse without optional fields."""
        response_data = {
            "id": "group_123",
            "name": "Response Group",
            "email_domain": "response.com",
            "status": GroupStatus.ACTIVE,
            "auto_created": True,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "user_count": 0
        }
        
        response = GroupResponse(**response_data)
        
        assert response.created_by_email is None
        assert response.description is None
    
    def test_group_response_config(self):
        """Test GroupResponse configuration."""
        assert hasattr(GroupResponse.Config, 'from_attributes')
        assert GroupResponse.Config.from_attributes is True


class TestGroupUserBase:
    """Test cases for GroupUserBase schema."""
    
    def test_valid_group_user_base(self):
        """Test valid GroupUserBase creation."""
        user_data = {
            "role": GroupUserRole.ADMIN,
            "status": GroupUserStatus.ACTIVE
        }
        
        user = GroupUserBase(**user_data)
        
        assert user.role == GroupUserRole.ADMIN
        assert user.status == GroupUserStatus.ACTIVE
    
    def test_group_user_base_defaults(self):
        """Test GroupUserBase default values."""
        user = GroupUserBase()
        
        assert user.role == GroupUserRole.USER
        assert user.status == GroupUserStatus.ACTIVE
    
    def test_group_user_base_invalid_role(self):
        """Test GroupUserBase with invalid role."""
        user_data = {
            "role": "INVALID_ROLE"
        }
        
        with pytest.raises(ValidationError):
            GroupUserBase(**user_data)


class TestGroupUserCreateRequest:
    """Test cases for GroupUserCreateRequest schema."""
    
    def test_valid_group_user_create_request(self):
        """Test valid GroupUserCreateRequest creation."""
        create_data = {
            "user_email": "user@test.com",
            "role": GroupUserRole.ADMIN
        }
        
        request = GroupUserCreateRequest(**create_data)
        
        assert request.user_email == "user@test.com"
        assert request.role == GroupUserRole.ADMIN
    
    def test_group_user_create_request_default_role(self):
        """Test GroupUserCreateRequest with default role."""
        create_data = {
            "user_email": "user@test.com"
        }
        
        request = GroupUserCreateRequest(**create_data)
        
        assert request.user_email == "user@test.com"
        assert request.role == GroupUserRole.USER
    
    def test_group_user_create_request_invalid_email(self):
        """Test GroupUserCreateRequest with invalid email."""
        create_data = {
            "user_email": "invalid-email"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            GroupUserCreateRequest(**create_data)
        
        assert "valid email address" in str(exc_info.value).lower()


class TestGroupUserUpdateRequest:
    """Test cases for GroupUserUpdateRequest schema."""
    
    def test_valid_group_user_update_request(self):
        """Test valid GroupUserUpdateRequest creation."""
        update_data = {
            "role": GroupUserRole.ADMIN,
            "status": GroupUserStatus.INACTIVE
        }
        
        request = GroupUserUpdateRequest(**update_data)
        
        assert request.role == GroupUserRole.ADMIN
        assert request.status == GroupUserStatus.INACTIVE
    
    def test_group_user_update_request_all_optional(self):
        """Test GroupUserUpdateRequest with all optional fields."""
        request = GroupUserUpdateRequest()
        
        assert request.role is None
        assert request.status is None
    
    def test_group_user_update_request_partial(self):
        """Test GroupUserUpdateRequest with partial data."""
        update_data = {
            "role": GroupUserRole.ADMIN
        }
        
        request = GroupUserUpdateRequest(**update_data)
        
        assert request.role == GroupUserRole.ADMIN
        assert request.status is None


class TestGroupUserResponse:
    """Test cases for GroupUserResponse schema."""
    
    def test_valid_group_user_response(self):
        """Test valid GroupUserResponse creation."""
        response_data = {
            "id": "groupuser_123",
            "group_id": "group_123",
            "user_id": "user_123",
            "email": "user@test.com",
            "role": GroupUserRole.ADMIN,
            "status": GroupUserStatus.ACTIVE,
            "joined_at": datetime.now(),
            "auto_created": False,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }
        
        response = GroupUserResponse(**response_data)
        
        assert response.id == "groupuser_123"
        assert response.group_id == "group_123"
        assert response.user_id == "user_123"
        assert response.email == "user@test.com"
        assert response.role == GroupUserRole.ADMIN
        assert response.auto_created is False
    
    def test_group_user_response_config(self):
        """Test GroupUserResponse configuration."""
        assert hasattr(GroupUserResponse.Config, 'from_attributes')
        assert GroupUserResponse.Config.from_attributes is True


class TestGroupStatsResponse:
    """Test cases for GroupStatsResponse schema."""
    
    def test_valid_group_stats_response(self):
        """Test valid GroupStatsResponse creation."""
        stats_data = {
            "total_groups": 10,
            "active_groups": 8,
            "auto_created_groups": 6,
            "manual_groups": 4,
            "total_users": 50,
            "active_users": 45
        }
        
        stats = GroupStatsResponse(**stats_data)
        
        assert stats.total_groups == 10
        assert stats.active_groups == 8
        assert stats.auto_created_groups == 6
        assert stats.manual_groups == 4
        assert stats.total_users == 50
        assert stats.active_users == 45
    
    def test_group_stats_response_zero_values(self):
        """Test GroupStatsResponse with zero values."""
        stats_data = {
            "total_groups": 0,
            "active_groups": 0,
            "auto_created_groups": 0,
            "manual_groups": 0,
            "total_users": 0,
            "active_users": 0
        }
        
        stats = GroupStatsResponse(**stats_data)
        
        assert all(getattr(stats, field) == 0 for field in stats_data.keys())
    
    def test_group_stats_response_config(self):
        """Test GroupStatsResponse configuration."""
        assert hasattr(GroupStatsResponse.Config, 'from_attributes')
        assert GroupStatsResponse.Config.from_attributes is True


class TestSchemaInteraction:
    """Test cases for schema interactions and edge cases."""
    
    def test_group_response_serialization(self):
        """Test GroupResponse serialization."""
        response_data = {
            "id": "group_123",
            "name": "Test Group",
            "email_domain": "test.com",
            "status": GroupStatus.ACTIVE,
            "auto_created": False,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "user_count": 1
        }
        
        response = GroupResponse(**response_data)
        serialized = response.model_dump()
        
        assert isinstance(serialized, dict)
        assert serialized["id"] == "group_123"
        assert serialized["name"] == "Test Group"
    
    def test_group_user_response_serialization(self):
        """Test GroupUserResponse serialization."""
        response_data = {
            "id": "groupuser_123",
            "group_id": "group_123",
            "user_id": "user_123",
            "email": "user@test.com",
            "role": GroupUserRole.USER,
            "status": GroupUserStatus.ACTIVE,
            "joined_at": datetime.now(),
            "auto_created": True,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }
        
        response = GroupUserResponse(**response_data)
        serialized = response.model_dump()
        
        assert isinstance(serialized, dict)
        assert serialized["email"] == "user@test.com"
        assert serialized["role"] == GroupUserRole.USER
    
    def test_enum_validation_in_schemas(self):
        """Test that enum validation works correctly in schemas."""
        # Test valid enum values
        valid_data = {
            "role": GroupUserRole.ADMIN,
            "status": GroupUserStatus.ACTIVE
        }
        user = GroupUserBase(**valid_data)
        assert user.role == GroupUserRole.ADMIN
        
        # Test invalid enum values
        with pytest.raises(ValidationError):
            GroupUserBase(role="INVALID_ROLE")
    
    def test_field_descriptions_present(self):
        """Test that field descriptions are present in schemas."""
        # Check that Field descriptions are set
        fields = GroupResponse.model_fields
        
        assert fields["id"].description == "Unique group identifier"
        assert fields["name"].description == "Human-readable group name"
        assert fields["user_count"].description == "Number of users in the group"
    
    def test_model_validation_inheritance(self):
        """Test that child schemas inherit validation from parent schemas."""
        # GroupCreateRequest should inherit validation from GroupBase
        with pytest.raises(ValidationError):
            GroupCreateRequest(name="", email_domain="test.com")
        
        # Should work with valid data
        valid_request = GroupCreateRequest(name="Valid Name", email_domain="test.com")
        assert valid_request.name == "Valid Name"