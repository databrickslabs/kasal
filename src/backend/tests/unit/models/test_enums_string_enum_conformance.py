"""
Comprehensive unit tests for model enums.

Tests all enum classes in enums.py including values and inheritance.
"""

from enum import Enum

from src.models.enums import (
    GroupStatus,
    GroupUserRole,
    GroupUserStatus,
    IdentityProviderType,
    UserRole,
    UserStatus,
)


class TestUserRole:
    """Test UserRole enum."""

    def test_user_role_values(self):
        """Test UserRole enum values."""
        assert UserRole.ADMIN == "admin"
        assert UserRole.TECHNICAL == "technical"
        assert UserRole.REGULAR == "regular"

    def test_user_role_inheritance(self):
        """Test UserRole inherits from str and Enum."""
        assert issubclass(UserRole, str)
        assert issubclass(UserRole, Enum)

    def test_user_role_all_members(self):
        """Test UserRole has expected members."""
        expected_members = {"ADMIN", "TECHNICAL", "REGULAR"}
        actual_members = set(UserRole.__members__.keys())
        assert actual_members == expected_members

    def test_user_role_string_comparison(self):
        """Test UserRole can be compared with strings."""
        assert UserRole.ADMIN == "admin"
        assert UserRole.TECHNICAL == "technical"
        assert UserRole.REGULAR == "regular"

    def test_user_role_iteration(self):
        """Test UserRole can be iterated."""
        roles = list(UserRole)
        assert len(roles) == 3
        assert UserRole.ADMIN in roles
        assert UserRole.TECHNICAL in roles
        assert UserRole.REGULAR in roles


class TestUserStatus:
    """Test UserStatus enum."""

    def test_user_status_values(self):
        """Test UserStatus enum values."""
        assert UserStatus.ACTIVE == "active"
        assert UserStatus.INACTIVE == "inactive"
        assert UserStatus.SUSPENDED == "suspended"

    def test_user_status_inheritance(self):
        """Test UserStatus inherits from str and Enum."""
        assert issubclass(UserStatus, str)
        assert issubclass(UserStatus, Enum)

    def test_user_status_all_members(self):
        """Test UserStatus has expected members."""
        expected_members = {"ACTIVE", "INACTIVE", "SUSPENDED"}
        actual_members = set(UserStatus.__members__.keys())
        assert actual_members == expected_members

    def test_user_status_string_comparison(self):
        """Test UserStatus can be compared with strings."""
        assert UserStatus.ACTIVE == "active"
        assert UserStatus.INACTIVE == "inactive"
        assert UserStatus.SUSPENDED == "suspended"


class TestGroupStatus:
    """Test GroupStatus enum."""

    def test_group_status_values(self):
        """Test GroupStatus enum values."""
        assert GroupStatus.ACTIVE == "active"
        assert GroupStatus.SUSPENDED == "suspended"
        assert GroupStatus.ARCHIVED == "archived"

    def test_group_status_inheritance(self):
        """Test GroupStatus inherits from str and Enum."""
        assert issubclass(GroupStatus, str)
        assert issubclass(GroupStatus, Enum)

    def test_group_status_all_members(self):
        """Test GroupStatus has expected members."""
        expected_members = {"ACTIVE", "SUSPENDED", "ARCHIVED"}
        actual_members = set(GroupStatus.__members__.keys())
        assert actual_members == expected_members

    def test_group_status_string_comparison(self):
        """Test GroupStatus can be compared with strings."""
        assert GroupStatus.ACTIVE == "active"
        assert GroupStatus.SUSPENDED == "suspended"
        assert GroupStatus.ARCHIVED == "archived"


class TestGroupUserRole:
    """Test GroupUserRole enum."""

    def test_group_user_role_values(self):
        """Test GroupUserRole enum values."""
        assert GroupUserRole.ADMIN == "admin"
        assert GroupUserRole.EDITOR == "editor"
        assert GroupUserRole.OPERATOR == "operator"

    def test_group_user_role_inheritance(self):
        """Test GroupUserRole inherits from str and Enum."""
        assert issubclass(GroupUserRole, str)
        assert issubclass(GroupUserRole, Enum)

    def test_group_user_role_all_members(self):
        """Test GroupUserRole has expected members."""
        expected_members = {"ADMIN", "EDITOR", "OPERATOR"}
        actual_members = set(GroupUserRole.__members__.keys())
        assert actual_members == expected_members

    def test_group_user_role_string_comparison(self):
        """Test GroupUserRole can be compared with strings."""
        assert GroupUserRole.ADMIN == "admin"
        assert GroupUserRole.EDITOR == "editor"
        assert GroupUserRole.OPERATOR == "operator"


class TestGroupUserStatus:
    """Test GroupUserStatus enum."""

    def test_group_user_status_values(self):
        """Test GroupUserStatus enum values."""
        assert GroupUserStatus.ACTIVE == "active"
        assert GroupUserStatus.INACTIVE == "inactive"
        assert GroupUserStatus.SUSPENDED == "suspended"

    def test_group_user_status_inheritance(self):
        """Test GroupUserStatus inherits from str and Enum."""
        assert issubclass(GroupUserStatus, str)
        assert issubclass(GroupUserStatus, Enum)

    def test_group_user_status_all_members(self):
        """Test GroupUserStatus has expected members."""
        expected_members = {"ACTIVE", "INACTIVE", "SUSPENDED"}
        actual_members = set(GroupUserStatus.__members__.keys())
        assert actual_members == expected_members

    def test_group_user_status_string_comparison(self):
        """Test GroupUserStatus can be compared with strings."""
        assert GroupUserStatus.ACTIVE == "active"
        assert GroupUserStatus.INACTIVE == "inactive"
        assert GroupUserStatus.SUSPENDED == "suspended"


class TestIdentityProviderType:
    """Test IdentityProviderType enum."""

    def test_identity_provider_type_values(self):
        """Test IdentityProviderType enum values."""
        assert IdentityProviderType.LOCAL == "local"
        assert IdentityProviderType.OAUTH == "oauth"
        assert IdentityProviderType.OIDC == "oidc"
        assert IdentityProviderType.SAML == "saml"
        assert IdentityProviderType.CUSTOM == "custom"

    def test_identity_provider_type_inheritance(self):
        """Test IdentityProviderType inherits from str and Enum."""
        assert issubclass(IdentityProviderType, str)
        assert issubclass(IdentityProviderType, Enum)

    def test_identity_provider_type_all_members(self):
        """Test IdentityProviderType has expected members."""
        expected_members = {"LOCAL", "OAUTH", "OIDC", "SAML", "CUSTOM"}
        actual_members = set(IdentityProviderType.__members__.keys())
        assert actual_members == expected_members

    def test_identity_provider_type_string_comparison(self):
        """Test IdentityProviderType can be compared with strings."""
        assert IdentityProviderType.LOCAL == "local"
        assert IdentityProviderType.OAUTH == "oauth"
        assert IdentityProviderType.OIDC == "oidc"
        assert IdentityProviderType.SAML == "saml"
        assert IdentityProviderType.CUSTOM == "custom"


class TestEnumInteroperability:
    """Test enum interoperability and edge cases."""

    def test_enum_in_collections(self):
        """Test enums work correctly in collections."""
        roles = {UserRole.ADMIN, UserRole.TECHNICAL, UserRole.REGULAR}
        assert len(roles) == 3
        assert UserRole.ADMIN in roles

        statuses = [UserStatus.ACTIVE, UserStatus.INACTIVE, UserStatus.SUSPENDED]
        assert len(statuses) == 3
        assert UserStatus.ACTIVE in statuses

    def test_enum_serialization(self):
        """Test enum serialization to string."""
        assert UserRole.ADMIN.value == "admin"
        assert GroupStatus.ACTIVE.value == "active"
        assert IdentityProviderType.OAUTH.value == "oauth"

    def test_enum_representation(self):
        """Test enum representation."""
        assert repr(UserRole.ADMIN) == "<UserRole.ADMIN: 'admin'>"
        assert repr(GroupStatus.ACTIVE) == "<GroupStatus.ACTIVE: 'active'>"

    def test_enum_membership_testing(self):
        """Test enum membership testing."""
        assert "admin" in [role.value for role in UserRole]
        assert "active" in [status.value for status in GroupStatus]
        assert "oauth" in [provider.value for provider in IdentityProviderType]

    def test_enum_case_sensitivity(self):
        """Test enum case sensitivity."""
        assert UserRole.ADMIN != "ADMIN"  # Case sensitive
        assert UserRole.ADMIN == "admin"  # Exact match
        assert GroupStatus.ACTIVE != "ACTIVE"
        assert GroupStatus.ACTIVE == "active"
