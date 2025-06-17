"""
Unit tests for group schemas.

Tests the functionality of Pydantic schemas for group management operations
including validation, serialization, and field constraints.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError
from email_validator import EmailNotValidError

from src.schemas.group import (
    GroupBase, GroupCreateRequest, GroupUpdateRequest, GroupCreate, GroupUpdate,
    GroupResponse, GroupUserBase, GroupUserCreateRequest, GroupUserUpdateRequest,
    GroupUserResponse, GroupStatsResponse
)
from src.models.enums import GroupStatus, GroupUserRole, GroupUserStatus


class TestGroupBase:
    """Test cases for GroupBase schema."""
    
    def test_valid_group_base(self):
        """Test GroupBase with valid data."""
        group_data = {
            "name": "Engineering Team",
            "email_domain": "engineering.company.com",
            "description": "Software engineering team"
        }
        group = GroupBase(**group_data)
        assert group.name == "Engineering Team"
        assert group.email_domain == "engineering.company.com"
        assert group.description == "Software engineering team"
    
    def test_group_base_minimal(self):
        """Test GroupBase with minimal required fields."""
        group_data = {
            "name": "Sales",
            "email_domain": "sales.company.com"
        }
        group = GroupBase(**group_data)
        assert group.name == "Sales"
        assert group.email_domain == "sales.company.com"
        assert group.description is None
    
    def test_group_base_missing_required_fields(self):
        """Test GroupBase validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            GroupBase(name="Test Group")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "email_domain" in missing_fields
        
        with pytest.raises(ValidationError) as exc_info:
            GroupBase(email_domain="test.com")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "name" in missing_fields
    
    def test_group_base_field_length_constraints(self):
        """Test GroupBase field length constraints."""
        # Test minimum length constraints
        with pytest.raises(ValidationError) as exc_info:
            GroupBase(name="", email_domain="test.com")
        
        errors = exc_info.value.errors()
        assert any(error["type"] == "string_too_short" for error in errors)
        
        with pytest.raises(ValidationError) as exc_info:
            GroupBase(name="Test", email_domain="")
        
        errors = exc_info.value.errors()
        assert any(error["type"] == "string_too_short" for error in errors)
        
        # Test maximum length constraints
        long_name = "a" * 256
        with pytest.raises(ValidationError) as exc_info:
            GroupBase(name=long_name, email_domain="test.com")
        
        errors = exc_info.value.errors()
        assert any(error["type"] == "string_too_long" for error in errors)
        
        long_domain = "a" * 256
        with pytest.raises(ValidationError) as exc_info:
            GroupBase(name="Test", email_domain=long_domain)
        
        errors = exc_info.value.errors()
        assert any(error["type"] == "string_too_long" for error in errors)
        
        long_description = "a" * 1001
        with pytest.raises(ValidationError) as exc_info:
            GroupBase(name="Test", email_domain="test.com", description=long_description)
        
        errors = exc_info.value.errors()
        assert any(error["type"] == "string_too_long" for error in errors)
    
    def test_group_base_valid_lengths(self):
        """Test GroupBase with valid length values."""
        # At maximum allowed lengths
        max_name = "a" * 255
        max_domain = "b" * 255
        max_description = "c" * 1000
        
        group_data = {
            "name": max_name,
            "email_domain": max_domain,
            "description": max_description
        }
        group = GroupBase(**group_data)
        assert group.name == max_name
        assert group.email_domain == max_domain
        assert group.description == max_description
    
    def test_group_base_special_characters(self):
        """Test GroupBase with special characters."""
        group_data = {
            "name": "R&D Team (AI/ML)",
            "email_domain": "r-d.ai-ml.company.com",
            "description": "Research & Development team focusing on AI/ML technologies"
        }
        group = GroupBase(**group_data)
        assert group.name == "R&D Team (AI/ML)"
        assert group.email_domain == "r-d.ai-ml.company.com"


class TestGroupCreateRequest:
    """Test cases for GroupCreateRequest schema."""
    
    def test_group_create_request_inheritance(self):
        """Test that GroupCreateRequest inherits from GroupBase."""
        create_data = {
            "name": "DevOps Team",
            "email_domain": "devops.company.com",
            "description": "Infrastructure and deployment team"
        }
        create_request = GroupCreateRequest(**create_data)
        
        # Should have all base class attributes
        assert hasattr(create_request, 'name')
        assert hasattr(create_request, 'email_domain')
        assert hasattr(create_request, 'description')
        
        # Should behave like base class
        assert create_request.name == "DevOps Team"
        assert create_request.email_domain == "devops.company.com"
        assert create_request.description == "Infrastructure and deployment team"
    
    def test_group_create_alias(self):
        """Test GroupCreate alias."""
        create_data = {
            "name": "Alias Test",
            "email_domain": "alias.test.com"
        }
        
        # Both should create the same type of object
        create_request = GroupCreateRequest(**create_data)
        create_alias = GroupCreate(**create_data)
        
        assert type(create_request) == type(create_alias)
        assert create_request.name == create_alias.name
        assert create_request.email_domain == create_alias.email_domain
        
        # Verify they are actually the same class
        assert GroupCreate is GroupCreateRequest


class TestGroupUpdateRequest:
    """Test cases for GroupUpdateRequest schema."""
    
    def test_group_update_request_all_optional(self):
        """Test that all GroupUpdateRequest fields are optional."""
        update = GroupUpdateRequest()
        assert update.name is None
        assert update.email_domain is None
        assert update.description is None
        assert update.status is None
    
    def test_group_update_request_partial(self):
        """Test GroupUpdateRequest with partial fields."""
        update_data = {
            "name": "Updated Team Name",
            "status": GroupStatus.SUSPENDED
        }
        update = GroupUpdateRequest(**update_data)
        assert update.name == "Updated Team Name"
        assert update.status == GroupStatus.SUSPENDED
        assert update.email_domain is None
        assert update.description is None
    
    def test_group_update_request_full(self):
        """Test GroupUpdateRequest with all fields."""
        update_data = {
            "name": "Completely Updated Team",
            "email_domain": "updated.company.com",
            "description": "Updated description",
            "status": GroupStatus.ACTIVE
        }
        update = GroupUpdateRequest(**update_data)
        assert update.name == "Completely Updated Team"
        assert update.email_domain == "updated.company.com"
        assert update.description == "Updated description"
        assert update.status == GroupStatus.ACTIVE
    
    def test_group_update_request_status_enum(self):
        """Test GroupUpdateRequest with various status values."""
        for status in GroupStatus:
            update_data = {"status": status}
            update = GroupUpdateRequest(**update_data)
            assert update.status == status
    
    def test_group_update_alias(self):
        """Test GroupUpdate alias."""
        update_data = {
            "name": "Alias Update Test",
            "status": GroupStatus.SUSPENDED
        }
        
        # Both should create the same type of object
        update_request = GroupUpdateRequest(**update_data)
        update_alias = GroupUpdate(**update_data)
        
        assert type(update_request) == type(update_alias)
        assert update_request.name == update_alias.name
        assert update_request.status == update_alias.status
        assert update_request.status == GroupStatus.SUSPENDED
        
        # Verify they are actually the same class
        assert GroupUpdate is GroupUpdateRequest


class TestGroupResponse:
    """Test cases for GroupResponse schema."""
    
    def test_valid_group_response(self):
        """Test GroupResponse with all required fields."""
        now = datetime.now()
        response_data = {
            "id": "group-123",
            "name": "Engineering Team",
            "email_domain": "engineering.company.com",
            "description": "Software engineering team",
            "status": GroupStatus.ACTIVE,
            "auto_created": False,
            "created_by_email": "admin@company.com",
            "created_at": now,
            "updated_at": now,
            "user_count": 15
        }
        response = GroupResponse(**response_data)
        assert response.id == "group-123"
        assert response.name == "Engineering Team"
        assert response.email_domain == "engineering.company.com"
        assert response.description == "Software engineering team"
        assert response.status == GroupStatus.ACTIVE
        assert response.auto_created is False
        assert response.created_by_email == "admin@company.com"
        assert response.created_at == now
        assert response.updated_at == now
        assert response.user_count == 15
    
    def test_group_response_inheritance(self):
        """Test that GroupResponse inherits from GroupBase."""
        now = datetime.now()
        response_data = {
            "id": "group-456",
            "name": "Marketing Team",
            "email_domain": "marketing.company.com",
            "status": GroupStatus.ACTIVE,
            "auto_created": True,
            "created_at": now,
            "updated_at": now,
            "user_count": 8
        }
        response = GroupResponse(**response_data)
        
        # Should have all base class attributes
        assert hasattr(response, 'name')
        assert hasattr(response, 'email_domain')
        assert hasattr(response, 'description')
        
        # Should have response-specific attributes
        assert hasattr(response, 'id')
        assert hasattr(response, 'status')
        assert hasattr(response, 'auto_created')
        assert hasattr(response, 'created_by_email')
        assert hasattr(response, 'created_at')
        assert hasattr(response, 'updated_at')
        assert hasattr(response, 'user_count')
        
        # Should behave like base class with defaults
        assert response.description is None  # Default from base
    
    def test_group_response_config(self):
        """Test GroupResponse Config class."""
        assert hasattr(GroupResponse, 'model_config')
        assert GroupResponse.model_config.get('from_attributes') is True
    
    def test_group_response_auto_created_scenarios(self):
        """Test GroupResponse for auto-created vs manual groups."""
        now = datetime.now()
        
        # Auto-created group
        auto_created_data = {
            "id": "auto-group",
            "name": "Auto Created Team",
            "email_domain": "auto.company.com",
            "status": GroupStatus.ACTIVE,
            "auto_created": True,
            "created_by_email": None,  # No creator for auto-created
            "created_at": now,
            "updated_at": now,
            "user_count": 5
        }
        auto_response = GroupResponse(**auto_created_data)
        assert auto_response.auto_created is True
        assert auto_response.created_by_email is None
        
        # Manually created group
        manual_created_data = {
            "id": "manual-group",
            "name": "Manual Team",
            "email_domain": "manual.company.com",
            "status": GroupStatus.ACTIVE,
            "auto_created": False,
            "created_by_email": "creator@company.com",
            "created_at": now,
            "updated_at": now,
            "user_count": 12
        }
        manual_response = GroupResponse(**manual_created_data)
        assert manual_response.auto_created is False
        assert manual_response.created_by_email == "creator@company.com"


class TestGroupUserBase:
    """Test cases for GroupUserBase schema."""
    
    def test_group_user_base_defaults(self):
        """Test GroupUserBase with default values."""
        user_base = GroupUserBase()
        assert user_base.role == GroupUserRole.USER
        assert user_base.status == GroupUserStatus.ACTIVE
    
    def test_group_user_base_explicit_values(self):
        """Test GroupUserBase with explicit values."""
        user_data = {
            "role": GroupUserRole.ADMIN,
            "status": GroupUserStatus.INACTIVE
        }
        user_base = GroupUserBase(**user_data)
        assert user_base.role == GroupUserRole.ADMIN
        assert user_base.status == GroupUserStatus.INACTIVE
    
    def test_group_user_base_enum_values(self):
        """Test GroupUserBase with all enum values."""
        for role in GroupUserRole:
            for status in GroupUserStatus:
                user_data = {"role": role, "status": status}
                user_base = GroupUserBase(**user_data)
                assert user_base.role == role
                assert user_base.status == status


class TestGroupUserCreateRequest:
    """Test cases for GroupUserCreateRequest schema."""
    
    def test_valid_group_user_create_request(self):
        """Test GroupUserCreateRequest with valid data."""
        create_data = {
            "user_email": "user@company.com",
            "role": GroupUserRole.USER
        }
        create_request = GroupUserCreateRequest(**create_data)
        assert create_request.user_email == "user@company.com"
        assert create_request.role == GroupUserRole.USER
    
    def test_group_user_create_request_default_role(self):
        """Test GroupUserCreateRequest with default role."""
        create_data = {"user_email": "default@company.com"}
        create_request = GroupUserCreateRequest(**create_data)
        assert create_request.user_email == "default@company.com"
        assert create_request.role == GroupUserRole.USER  # Default
    
    def test_group_user_create_request_admin_role(self):
        """Test GroupUserCreateRequest with admin role."""
        create_data = {
            "user_email": "admin@company.com",
            "role": GroupUserRole.ADMIN
        }
        create_request = GroupUserCreateRequest(**create_data)
        assert create_request.user_email == "admin@company.com"
        assert create_request.role == GroupUserRole.ADMIN
    
    def test_group_user_create_request_invalid_email(self):
        """Test GroupUserCreateRequest with invalid email."""
        with pytest.raises(ValidationError) as exc_info:
            GroupUserCreateRequest(user_email="invalid-email")
        
        errors = exc_info.value.errors()
        assert any("value_error" in str(error) or "value is not a valid email address" in str(error) for error in errors)
    
    def test_group_user_create_request_missing_email(self):
        """Test GroupUserCreateRequest with missing email."""
        with pytest.raises(ValidationError) as exc_info:
            GroupUserCreateRequest()
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "user_email" in missing_fields
    
    def test_group_user_create_request_various_email_formats(self):
        """Test GroupUserCreateRequest with various valid email formats."""
        valid_emails = [
            "user@company.com",
            "user.name@company.com",
            "user+tag@company.com",
            "user123@company123.com",
            "user@sub.company.com",
            "test@example.org"
        ]
        
        for email in valid_emails:
            create_data = {"user_email": email}
            create_request = GroupUserCreateRequest(**create_data)
            assert create_request.user_email == email


class TestGroupUserUpdateRequest:
    """Test cases for GroupUserUpdateRequest schema."""
    
    def test_group_user_update_request_all_optional(self):
        """Test that all GroupUserUpdateRequest fields are optional."""
        update = GroupUserUpdateRequest()
        assert update.role is None
        assert update.status is None
    
    def test_group_user_update_request_role_only(self):
        """Test GroupUserUpdateRequest with role update only."""
        update_data = {"role": GroupUserRole.ADMIN}
        update = GroupUserUpdateRequest(**update_data)
        assert update.role == GroupUserRole.ADMIN
        assert update.status is None
    
    def test_group_user_update_request_status_only(self):
        """Test GroupUserUpdateRequest with status update only."""
        update_data = {"status": GroupUserStatus.INACTIVE}
        update = GroupUserUpdateRequest(**update_data)
        assert update.role is None
        assert update.status == GroupUserStatus.INACTIVE
    
    def test_group_user_update_request_both_fields(self):
        """Test GroupUserUpdateRequest with both fields."""
        update_data = {
            "role": GroupUserRole.ADMIN,
            "status": GroupUserStatus.ACTIVE
        }
        update = GroupUserUpdateRequest(**update_data)
        assert update.role == GroupUserRole.ADMIN
        assert update.status == GroupUserStatus.ACTIVE


class TestGroupUserResponse:
    """Test cases for GroupUserResponse schema."""
    
    def test_valid_group_user_response(self):
        """Test GroupUserResponse with all required fields."""
        now = datetime.now()
        response_data = {
            "id": "group-user-123",
            "group_id": "group-456",
            "user_id": "user-789",
            "email": "user@company.com",
            "role": GroupUserRole.USER,
            "status": GroupUserStatus.ACTIVE,
            "joined_at": now,
            "auto_created": False,
            "created_at": now,
            "updated_at": now
        }
        response = GroupUserResponse(**response_data)
        assert response.id == "group-user-123"
        assert response.group_id == "group-456"
        assert response.user_id == "user-789"
        assert response.email == "user@company.com"
        assert response.role == GroupUserRole.USER
        assert response.status == GroupUserStatus.ACTIVE
        assert response.joined_at == now
        assert response.auto_created is False
        assert response.created_at == now
        assert response.updated_at == now
    
    def test_group_user_response_inheritance(self):
        """Test that GroupUserResponse inherits from GroupUserBase."""
        now = datetime.now()
        response_data = {
            "id": "group-user-456",
            "group_id": "group-789",
            "user_id": "user-123",
            "email": "admin@company.com",
            "joined_at": now,
            "auto_created": True,
            "created_at": now,
            "updated_at": now
        }
        response = GroupUserResponse(**response_data)
        
        # Should have all base class attributes
        assert hasattr(response, 'role')
        assert hasattr(response, 'status')
        
        # Should have response-specific attributes
        assert hasattr(response, 'id')
        assert hasattr(response, 'group_id')
        assert hasattr(response, 'user_id')
        assert hasattr(response, 'email')
        assert hasattr(response, 'joined_at')
        assert hasattr(response, 'auto_created')
        assert hasattr(response, 'created_at')
        assert hasattr(response, 'updated_at')
        
        # Should behave like base class with defaults
        assert response.role == GroupUserRole.USER  # Default from base
        assert response.status == GroupUserStatus.ACTIVE  # Default from base
    
    def test_group_user_response_config(self):
        """Test GroupUserResponse Config class."""
        assert hasattr(GroupUserResponse, 'model_config')
        assert GroupUserResponse.model_config.get('from_attributes') is True


class TestGroupStatsResponse:
    """Test cases for GroupStatsResponse schema."""
    
    def test_valid_group_stats_response(self):
        """Test GroupStatsResponse with all required fields."""
        stats_data = {
            "total_groups": 25,
            "active_groups": 20,
            "auto_created_groups": 15,
            "manual_groups": 10,
            "total_users": 150,
            "active_users": 140
        }
        stats = GroupStatsResponse(**stats_data)
        assert stats.total_groups == 25
        assert stats.active_groups == 20
        assert stats.auto_created_groups == 15
        assert stats.manual_groups == 10
        assert stats.total_users == 150
        assert stats.active_users == 140
    
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
        assert stats.total_groups == 0
        assert stats.active_groups == 0
        assert stats.auto_created_groups == 0
        assert stats.manual_groups == 0
        assert stats.total_users == 0
        assert stats.active_users == 0
    
    def test_group_stats_response_config(self):
        """Test GroupStatsResponse Config class."""
        assert hasattr(GroupStatsResponse, 'model_config')
        assert GroupStatsResponse.model_config.get('from_attributes') is True
    
    def test_group_stats_response_missing_fields(self):
        """Test GroupStatsResponse validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            GroupStatsResponse(total_groups=10)
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        expected_fields = {"active_groups", "auto_created_groups", "manual_groups", "total_users", "active_users"}
        assert expected_fields.intersection(set(missing_fields)) == expected_fields


class TestSchemaIntegration:
    """Integration tests for group schema interactions."""
    
    def test_group_lifecycle_workflow(self):
        """Test complete group lifecycle workflow."""
        # Create group
        create_data = {
            "name": "Product Team",
            "email_domain": "product.company.com",
            "description": "Product development team"
        }
        create_request = GroupCreateRequest(**create_data)
        
        # Update group
        update_data = {
            "description": "Updated product development team description",
            "status": GroupStatus.ACTIVE
        }
        update_request = GroupUpdateRequest(**update_data)
        
        # Group response (simulating what would come from database)
        now = datetime.now()
        response_data = {
            "id": "product-team-1",
            "name": create_request.name,  # Original name
            "email_domain": create_request.email_domain,  # Original domain
            "description": update_data["description"],  # Updated description
            "status": update_data["status"],  # Updated status
            "auto_created": False,
            "created_by_email": "manager@company.com",
            "created_at": now,
            "updated_at": now,
            "user_count": 0  # Initially empty
        }
        group_response = GroupResponse(**response_data)
        
        # Verify the complete workflow
        assert create_request.name == "Product Team"
        assert create_request.description == "Product development team"
        assert update_request.description == "Updated product development team description"
        assert group_response.id == "product-team-1"
        assert group_response.name == "Product Team"  # From creation
        assert group_response.description == "Updated product development team description"  # From update
        assert group_response.status == GroupStatus.ACTIVE  # From update
        assert group_response.user_count == 0
    
    def test_group_user_management_workflow(self):
        """Test group user management workflow."""
        now = datetime.now()
        
        # Add user to group
        add_user_data = {
            "user_email": "developer@company.com",
            "role": GroupUserRole.USER
        }
        add_request = GroupUserCreateRequest(**add_user_data)
        
        # User response after being added
        user_response_data = {
            "id": "group-user-1",
            "group_id": "product-team-1",
            "user_id": "developer-123",
            "email": add_request.user_email,
            "role": add_request.role,
            "status": GroupUserStatus.ACTIVE,  # Default
            "joined_at": now,
            "auto_created": False,
            "created_at": now,
            "updated_at": now
        }
        user_response = GroupUserResponse(**user_response_data)
        
        # Promote user to admin
        promote_data = {"role": GroupUserRole.ADMIN}
        promote_request = GroupUserUpdateRequest(**promote_data)
        
        # Updated user response
        updated_user_data = {
            **user_response_data,
            "role": promote_data["role"],  # Updated role
            "updated_at": now
        }
        updated_response = GroupUserResponse(**updated_user_data)
        
        # Verify workflow
        assert add_request.user_email == "developer@company.com"
        assert add_request.role == GroupUserRole.USER
        assert user_response.role == GroupUserRole.USER
        assert user_response.status == GroupUserStatus.ACTIVE
        assert promote_request.role == GroupUserRole.ADMIN
        assert updated_response.role == GroupUserRole.ADMIN  # Promoted
        assert updated_response.email == "developer@company.com"  # Same user
    
    def test_group_statistics_scenario(self):
        """Test group statistics scenarios."""
        # Small organization stats
        small_org_stats = GroupStatsResponse(
            total_groups=5,
            active_groups=4,
            auto_created_groups=2,
            manual_groups=3,
            total_users=25,
            active_users=23
        )
        
        # Large organization stats
        large_org_stats = GroupStatsResponse(
            total_groups=150,
            active_groups=140,
            auto_created_groups=100,
            manual_groups=50,
            total_users=2000,
            active_users=1850
        )
        
        # Verify stats make sense
        assert small_org_stats.total_groups == 5
        assert small_org_stats.active_groups <= small_org_stats.total_groups
        assert small_org_stats.active_users <= small_org_stats.total_users
        
        assert large_org_stats.total_groups == 150
        assert large_org_stats.active_groups <= large_org_stats.total_groups
        assert large_org_stats.active_users <= large_org_stats.total_users
        
        # Calculate derived stats
        small_inactive_groups = small_org_stats.total_groups - small_org_stats.active_groups
        large_inactive_groups = large_org_stats.total_groups - large_org_stats.active_groups
        
        assert small_inactive_groups == 1
        assert large_inactive_groups == 10
    
    def test_group_enum_usage_scenarios(self):
        """Test various enum usage scenarios."""
        now = datetime.now()
        
        # Test all group statuses
        for status in GroupStatus:
            group_data = {
                "id": f"group-{status.value}",
                "name": f"Test Group {status.value}",
                "email_domain": f"{status.value}.company.com",
                "status": status,
                "auto_created": False,
                "created_at": now,
                "updated_at": now,
                "user_count": 1
            }
            group = GroupResponse(**group_data)
            assert group.status == status
        
        # Test all user roles and statuses
        for role in GroupUserRole:
            for user_status in GroupUserStatus:
                user_data = {
                    "id": f"user-{role.value}-{user_status.value}",
                    "group_id": "test-group",
                    "user_id": "test-user",
                    "email": f"{role.value}@company.com",
                    "role": role,
                    "status": user_status,
                    "joined_at": now,
                    "auto_created": False,
                    "created_at": now,
                    "updated_at": now
                }
                user = GroupUserResponse(**user_data)
                assert user.role == role
                assert user.status == user_status