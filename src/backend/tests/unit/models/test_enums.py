"""
Unit tests for model enums.

Tests the functionality of enum definitions including
values, inheritance, and backward compatibility.
"""
import pytest
from enum import Enum

from src.models.enums import (
    UserRole, UserStatus, GroupStatus, GroupUserRole, GroupUserStatus,
    TenantStatus, TenantUserRole, TenantUserStatus, IdentityProviderType
)


class TestUserRole:
    """Test cases for UserRole enum."""
    
    def test_user_role_values(self):
        """Test UserRole enum values."""
        assert UserRole.ADMIN == "admin"
        assert UserRole.TECHNICAL == "technical"
        assert UserRole.REGULAR == "regular"
    
    def test_user_role_is_string_enum(self):
        """Test that UserRole inherits from str and Enum."""
        assert issubclass(UserRole, str)
        assert issubclass(UserRole, Enum)
    
    def test_user_role_string_behavior(self):
        """Test that UserRole behaves like a string."""
        role = UserRole.ADMIN
        assert role.value == "admin"
        assert role == "admin"
        assert role.value.upper() == "ADMIN"
    
    def test_user_role_all_values(self):
        """Test that all expected UserRole values are present."""
        expected_values = {"admin", "technical", "regular"}
        actual_values = {role.value for role in UserRole}
        assert actual_values == expected_values
    
    def test_user_role_membership(self):
        """Test UserRole membership testing."""
        assert "admin" in UserRole.__members__.values()
        assert "invalid_role" not in UserRole.__members__.values()


class TestUserStatus:
    """Test cases for UserStatus enum."""
    
    def test_user_status_values(self):
        """Test UserStatus enum values."""
        assert UserStatus.ACTIVE == "active"
        assert UserStatus.INACTIVE == "inactive"
        assert UserStatus.SUSPENDED == "suspended"
    
    def test_user_status_is_string_enum(self):
        """Test that UserStatus inherits from str and Enum."""
        assert issubclass(UserStatus, str)
        assert issubclass(UserStatus, Enum)
    
    def test_user_status_all_values(self):
        """Test that all expected UserStatus values are present."""
        expected_values = {"active", "inactive", "suspended"}
        actual_values = {status.value for status in UserStatus}
        assert actual_values == expected_values


class TestGroupStatus:
    """Test cases for GroupStatus enum."""
    
    def test_group_status_values(self):
        """Test GroupStatus enum values."""
        assert GroupStatus.ACTIVE == "active"
        assert GroupStatus.SUSPENDED == "suspended"
        assert GroupStatus.ARCHIVED == "archived"
    
    def test_group_status_is_string_enum(self):
        """Test that GroupStatus inherits from str and Enum."""
        assert issubclass(GroupStatus, str)
        assert issubclass(GroupStatus, Enum)
    
    def test_group_status_all_values(self):
        """Test that all expected GroupStatus values are present."""
        expected_values = {"active", "suspended", "archived"}
        actual_values = {status.value for status in GroupStatus}
        assert actual_values == expected_values


class TestGroupUserRole:
    """Test cases for GroupUserRole enum."""
    
    def test_group_user_role_values(self):
        """Test GroupUserRole enum values."""
        assert GroupUserRole.ADMIN == "admin"
        assert GroupUserRole.MANAGER == "manager"
        assert GroupUserRole.USER == "user"
        assert GroupUserRole.VIEWER == "viewer"
    
    def test_group_user_role_is_string_enum(self):
        """Test that GroupUserRole inherits from str and Enum."""
        assert issubclass(GroupUserRole, str)
        assert issubclass(GroupUserRole, Enum)
    
    def test_group_user_role_hierarchy(self):
        """Test GroupUserRole hierarchy implications."""
        # Test that all roles are distinct
        roles = [GroupUserRole.ADMIN, GroupUserRole.MANAGER, GroupUserRole.USER, GroupUserRole.VIEWER]
        assert len(set(roles)) == 4
        
        # Test role values for hierarchy logic
        assert GroupUserRole.ADMIN == "admin"  # Highest privilege
        assert GroupUserRole.VIEWER == "viewer"  # Lowest privilege
    
    def test_group_user_role_all_values(self):
        """Test that all expected GroupUserRole values are present."""
        expected_values = {"admin", "manager", "user", "viewer"}
        actual_values = {role.value for role in GroupUserRole}
        assert actual_values == expected_values


class TestGroupUserStatus:
    """Test cases for GroupUserStatus enum."""
    
    def test_group_user_status_values(self):
        """Test GroupUserStatus enum values."""
        assert GroupUserStatus.ACTIVE == "active"
        assert GroupUserStatus.INACTIVE == "inactive"
        assert GroupUserStatus.SUSPENDED == "suspended"
    
    def test_group_user_status_is_string_enum(self):
        """Test that GroupUserStatus inherits from str and Enum."""
        assert issubclass(GroupUserStatus, str)
        assert issubclass(GroupUserStatus, Enum)
    
    def test_group_user_status_all_values(self):
        """Test that all expected GroupUserStatus values are present."""
        expected_values = {"active", "inactive", "suspended"}
        actual_values = {status.value for status in GroupUserStatus}
        assert actual_values == expected_values


class TestTenantStatus:
    """Test cases for TenantStatus enum (legacy)."""
    
    def test_tenant_status_values(self):
        """Test TenantStatus enum values."""
        assert TenantStatus.ACTIVE == "active"
        assert TenantStatus.SUSPENDED == "suspended"
        assert TenantStatus.ARCHIVED == "archived"
    
    def test_tenant_status_backward_compatibility(self):
        """Test TenantStatus backward compatibility with GroupStatus."""
        # Should have same values as GroupStatus
        assert TenantStatus.ACTIVE == GroupStatus.ACTIVE
        assert TenantStatus.SUSPENDED == GroupStatus.SUSPENDED
        assert TenantStatus.ARCHIVED == GroupStatus.ARCHIVED
    
    def test_tenant_status_is_string_enum(self):
        """Test that TenantStatus inherits from str and Enum."""
        assert issubclass(TenantStatus, str)
        assert issubclass(TenantStatus, Enum)


class TestTenantUserRole:
    """Test cases for TenantUserRole enum (legacy)."""
    
    def test_tenant_user_role_values(self):
        """Test TenantUserRole enum values."""
        assert TenantUserRole.ADMIN == "admin"
        assert TenantUserRole.MANAGER == "manager"
        assert TenantUserRole.USER == "user"
        assert TenantUserRole.VIEWER == "viewer"
    
    def test_tenant_user_role_backward_compatibility(self):
        """Test TenantUserRole backward compatibility with GroupUserRole."""
        # Should have same values as GroupUserRole
        assert TenantUserRole.ADMIN == GroupUserRole.ADMIN
        assert TenantUserRole.MANAGER == GroupUserRole.MANAGER
        assert TenantUserRole.USER == GroupUserRole.USER
        assert TenantUserRole.VIEWER == GroupUserRole.VIEWER
    
    def test_tenant_user_role_is_string_enum(self):
        """Test that TenantUserRole inherits from str and Enum."""
        assert issubclass(TenantUserRole, str)
        assert issubclass(TenantUserRole, Enum)


class TestTenantUserStatus:
    """Test cases for TenantUserStatus enum (legacy)."""
    
    def test_tenant_user_status_values(self):
        """Test TenantUserStatus enum values."""
        assert TenantUserStatus.ACTIVE == "active"
        assert TenantUserStatus.INACTIVE == "inactive"
        assert TenantUserStatus.SUSPENDED == "suspended"
    
    def test_tenant_user_status_backward_compatibility(self):
        """Test TenantUserStatus backward compatibility with GroupUserStatus."""
        # Should have same values as GroupUserStatus
        assert TenantUserStatus.ACTIVE == GroupUserStatus.ACTIVE
        assert TenantUserStatus.INACTIVE == GroupUserStatus.INACTIVE
        assert TenantUserStatus.SUSPENDED == GroupUserStatus.SUSPENDED
    
    def test_tenant_user_status_is_string_enum(self):
        """Test that TenantUserStatus inherits from str and Enum."""
        assert issubclass(TenantUserStatus, str)
        assert issubclass(TenantUserStatus, Enum)


class TestIdentityProviderType:
    """Test cases for IdentityProviderType enum."""
    
    def test_identity_provider_type_values(self):
        """Test IdentityProviderType enum values."""
        assert IdentityProviderType.LOCAL == "local"
        assert IdentityProviderType.OAUTH == "oauth"
    
    def test_identity_provider_type_is_string_enum(self):
        """Test that IdentityProviderType inherits from str and Enum."""
        assert issubclass(IdentityProviderType, str)
        assert issubclass(IdentityProviderType, Enum)
    
    def test_identity_provider_type_all_values(self):
        """Test that all expected IdentityProviderType values are present."""
        expected_values = {"local", "oauth", "oidc", "saml", "custom"}
        actual_values = {provider.value for provider in IdentityProviderType}
        assert actual_values == expected_values


class TestEnumComparisons:
    """Test cases for enum comparisons and operations."""
    
    def test_string_comparison(self):
        """Test that enums can be compared with strings."""
        assert UserRole.ADMIN == "admin"
        assert GroupStatus.ACTIVE == "active"
        assert GroupUserRole.MANAGER == "manager"
        
        # Test inequality
        assert UserRole.ADMIN != "user"
        assert GroupStatus.ACTIVE != "suspended"
    
    def test_enum_equality(self):
        """Test enum equality comparisons."""
        # Same enum values should be equal
        assert UserRole.ADMIN == UserRole.ADMIN
        assert GroupStatus.ACTIVE == GroupStatus.ACTIVE
        
        # Different enum values should not be equal
        assert UserRole.ADMIN != UserRole.TECHNICAL
        assert GroupStatus.ACTIVE != GroupStatus.SUSPENDED
    
    def test_enum_in_collections(self):
        """Test that enums work correctly in collections."""
        user_roles = [UserRole.ADMIN, UserRole.TECHNICAL]
        assert UserRole.ADMIN in user_roles
        assert UserRole.REGULAR not in user_roles
        
        group_statuses = {GroupStatus.ACTIVE, GroupStatus.SUSPENDED}
        assert GroupStatus.ACTIVE in group_statuses
        assert GroupStatus.ARCHIVED not in group_statuses
    
    def test_enum_as_dict_keys(self):
        """Test that enums can be used as dictionary keys."""
        role_permissions = {
            UserRole.ADMIN: ["read", "write", "delete"],
            UserRole.TECHNICAL: ["read", "write"],
            UserRole.REGULAR: ["read"]
        }
        
        assert role_permissions[UserRole.ADMIN] == ["read", "write", "delete"]
        assert role_permissions[UserRole.REGULAR] == ["read"]
    
    def test_enum_sorting(self):
        """Test that enums can be sorted (as strings)."""
        roles = [UserRole.TECHNICAL, UserRole.ADMIN, UserRole.REGULAR]
        sorted_roles = sorted(roles)
        
        # Should sort by string value
        expected_order = [UserRole.ADMIN, UserRole.REGULAR, UserRole.TECHNICAL]  # admin, regular, technical
        assert sorted_roles == expected_order


class TestEnumUsagePatterns:
    """Test cases for common enum usage patterns."""
    
    def test_enum_iteration(self):
        """Test iterating over enum values."""
        user_roles = list(UserRole)
        assert len(user_roles) == 3
        assert UserRole.ADMIN in user_roles
        assert UserRole.TECHNICAL in user_roles
        assert UserRole.REGULAR in user_roles
    
    def test_enum_names_and_values(self):
        """Test accessing enum names and values."""
        assert UserRole.ADMIN.name == "ADMIN"
        assert UserRole.ADMIN.value == "admin"
        
        assert GroupStatus.ACTIVE.name == "ACTIVE"
        assert GroupStatus.ACTIVE.value == "active"
    
    def test_enum_from_string(self):
        """Test creating enum instances from string values."""
        # Test valid string conversion
        assert UserRole("admin") == UserRole.ADMIN
        assert GroupStatus("active") == GroupStatus.ACTIVE
        
        # Test invalid string conversion raises ValueError
        with pytest.raises(ValueError):
            UserRole("invalid_role")
        
        with pytest.raises(ValueError):
            GroupStatus("invalid_status")
    
    def test_enum_members_property(self):
        """Test accessing enum members."""
        user_role_members = UserRole.__members__
        assert "ADMIN" in user_role_members
        assert "TECHNICAL" in user_role_members
        assert "REGULAR" in user_role_members
        
        assert user_role_members["ADMIN"] == UserRole.ADMIN
    
    def test_enum_repr_and_str(self):
        """Test enum string representation."""
        role = UserRole.ADMIN
        
        # String representation includes enum name
        assert str(role) == "UserRole.ADMIN"
        
        # Value attribute gives the actual string value
        assert role.value == "admin"
        
        # Repr should include enum information
        assert "UserRole" in repr(role)
        assert "admin" in repr(role)


class TestBackwardCompatibility:
    """Test cases for backward compatibility between old and new enums."""
    
    def test_group_tenant_status_compatibility(self):
        """Test that Group and Tenant status enums are compatible."""
        # All values should be identical
        for group_status in GroupStatus:
            tenant_status = TenantStatus(group_status.value)
            assert group_status.value == tenant_status.value
    
    def test_group_tenant_user_role_compatibility(self):
        """Test that Group and Tenant user role enums are compatible."""
        # All values should be identical
        for group_role in GroupUserRole:
            tenant_role = TenantUserRole(group_role.value)
            assert group_role.value == tenant_role.value
    
    def test_group_tenant_user_status_compatibility(self):
        """Test that Group and Tenant user status enums are compatible."""
        # All values should be identical
        for group_status in GroupUserStatus:
            tenant_status = TenantUserStatus(group_status.value)
            assert group_status.value == tenant_status.value
    
    def test_migration_safety(self):
        """Test that migration from tenant to group enums is safe."""
        # Code using tenant enums should work with group enum values
        tenant_active = TenantStatus.ACTIVE
        group_active = GroupStatus.ACTIVE
        
        # Should be able to compare across enum types via string values
        assert tenant_active.value == group_active.value
        # Compare just the values, not the full string representation which includes class name
        assert tenant_active.value == group_active.value
        assert tenant_active == "active"
        assert group_active == "active"