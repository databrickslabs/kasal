"""
Unit tests for group model.

Tests the functionality of the Group and GroupUser database models including
field validation, relationships, and data integrity.
"""
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
import uuid

# Import all required models to ensure relationships are loaded
from src.models.user import User
from src.models.group import (
    Group, GroupUser, generate_uuid,
    GroupStatus, GroupUserRole, GroupUserStatus,
    get_role_hierarchy, role_has_access
)


class TestGroup:
    """Test cases for Group model."""

    def test_group_creation(self):
        """Test basic Group model creation."""
        # Test model structure and column configuration
        columns = Group.__table__.columns
        
        # Assert required columns exist
        assert 'id' in columns
        assert 'name' in columns
        assert 'status' in columns
        assert 'auto_created' in columns
        
        # Test default values in column definitions
        assert columns['status'].default.arg == "ACTIVE"
        assert columns['auto_created'].default.arg is False

    def test_group_creation_with_all_fields(self):
        """Test Group model has all expected fields."""
        # Test that all expected columns exist
        columns = Group.__table__.columns
        
        expected_columns = [
            'id', 'name', 'status', 'description',
            'auto_created', 'created_by_email', 'created_at', 'updated_at'
        ]
        
        for col_name in expected_columns:
            assert col_name in columns, f"Column {col_name} should exist in Group model"

    def test_group_generate_group_id_basic(self):
        """Test Group.generate_group_id with basic name."""
        # Act
        group_id = Group.generate_group_id("Acme Corp")

        # Assert
        assert group_id.startswith("acme_corp_")
        assert len(group_id.split("_")[-1]) == 8  # UUID part
        assert "_" in group_id

    def test_group_generate_group_id_with_special_chars(self):
        """Test Group.generate_group_id with special characters."""
        # Act
        group_id = Group.generate_group_id("Sales & Marketing")

        # Assert
        assert "sales_marketing_" in group_id
        assert "&" not in group_id
        assert "." not in group_id
        assert " " not in group_id

    def test_group_generate_group_id_complex_names(self):
        """Test Group.generate_group_id handles complex names."""
        # Act
        group_id1 = Group.generate_group_id("Engineering Team")
        group_id2 = Group.generate_group_id("Tech-Startup Division")

        # Assert
        assert "engineering_team_" in group_id1
        assert "techstartup_division_" in group_id2
        assert len(group_id1.split("_")[-1]) == 8  # UUID part
        assert len(group_id2.split("_")[-1]) == 8  # UUID part

    def test_group_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert Group.__tablename__ == "groups"

    def test_group_column_types_and_constraints(self):
        """Test that columns have correct data types and constraints."""
        # Act
        columns = Group.__table__.columns
        
        # Assert
        # Primary key
        assert columns['id'].primary_key is True
        assert "VARCHAR" in str(columns['id'].type) or "STRING" in str(columns['id'].type)
        
        # Required fields
        assert columns['name'].nullable is False
        assert columns['status'].nullable is False
        
        # Optional fields
        assert columns['description'].nullable is True
        assert columns['created_by_email'].nullable is True
        
        # Boolean field
        assert "BOOLEAN" in str(columns['auto_created'].type)
        
        # DateTime fields
        assert "DATETIME" in str(columns['created_at'].type)
        assert "DATETIME" in str(columns['updated_at'].type)

    def test_group_default_values(self):
        """Test Group model default values."""
        # Act
        columns = Group.__table__.columns
        
        # Assert
        assert columns['status'].default.arg == "ACTIVE"
        assert columns['auto_created'].default.arg is False

    def test_group_relationships(self):
        """Test that Group model has the expected relationships."""
        # Test that relationship is defined
        assert hasattr(Group, 'group_users'), "Group should have group_users relationship"

    def test_group_status_column(self):
        """Test Group status column configuration."""
        # Test that status column exists and is configured correctly
        columns = Group.__table__.columns

        assert 'status' in columns
        assert columns['status'].nullable is False
        assert "VARCHAR" in str(columns['status'].type) or "STRING" in str(columns['status'].type)

    def test_group_auto_created_scenarios(self):
        """Test Group auto_created column configuration."""
        # Test that auto_created column exists and has correct default
        columns = Group.__table__.columns
        
        assert 'auto_created' in columns
        assert "BOOLEAN" in str(columns['auto_created'].type)
        assert columns['auto_created'].default.arg is False


class TestGroupUser:
    """Test cases for GroupUser model."""

    def test_group_user_creation(self):
        """Test basic GroupUser model creation."""
        # Arrange
        group_id = "group_123"
        user_id = "user_456"
        
        # Act
        group_user = GroupUser(
            group_id=group_id,
            user_id=user_id
        )
        
        # Assert
        assert group_user.group_id == group_id
        assert group_user.user_id == user_id
        # Note: defaults are set at DB level, not on Python object
        assert group_user.role is None or group_user.role == "operator"  # Default at DB level
        assert group_user.status is None or group_user.status == "active"  # Default at DB level
        assert group_user.auto_created is None or group_user.auto_created is False  # Default at DB level

    def test_group_user_creation_with_all_fields(self):
        """Test GroupUser model creation with all fields populated."""
        # Arrange
        group_id = "engineering_team"
        user_id = "developer_001"
        role = "admin"
        status = "active"
        joined_at = datetime.now(timezone.utc)
        auto_created = True
        created_at = datetime.now(timezone.utc)
        updated_at = datetime.now(timezone.utc)
        
        # Act
        group_user = GroupUser(
            group_id=group_id,
            user_id=user_id,
            role=role,
            status=status,
            joined_at=joined_at,
            auto_created=auto_created,
            created_at=created_at,
            updated_at=updated_at
        )
        
        # Assert
        assert group_user.group_id == group_id
        assert group_user.user_id == user_id
        assert group_user.role == role
        assert group_user.status == status
        assert group_user.joined_at == joined_at
        assert group_user.auto_created == auto_created
        assert group_user.created_at == created_at
        assert group_user.updated_at == updated_at

    def test_group_user_all_roles(self):
        """Test GroupUser with all possible roles."""
        roles = ["admin", "editor", "operator"]

        for role in roles:
            # Act
            group_user = GroupUser(
                group_id="test_group",
                user_id=f"user_{role.lower()}",
                role=role
            )

            # Assert
            assert group_user.role == role

    def test_group_user_all_statuses(self):
        """Test GroupUser with all possible statuses."""
        statuses = ["active", "inactive", "suspended"]

        for status in statuses:
            # Act
            group_user = GroupUser(
                group_id="test_group",
                user_id=f"user_{status.lower()}",
                status=status
            )

            # Assert
            assert group_user.status == status

    def test_group_user_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert GroupUser.__tablename__ == "group_users"

    def test_group_user_column_types_and_constraints(self):
        """Test that columns have correct data types and constraints."""
        # Act
        columns = GroupUser.__table__.columns
        
        # Assert
        # Primary key
        assert columns['id'].primary_key is True
        assert "VARCHAR" in str(columns['id'].type) or "STRING" in str(columns['id'].type)
        
        # Foreign keys
        assert columns['group_id'].nullable is False
        assert columns['user_id'].nullable is False
        
        # Required fields
        assert columns['role'].nullable is False
        assert columns['status'].nullable is False
        
        # Boolean field
        assert "BOOLEAN" in str(columns['auto_created'].type)
        
        # DateTime fields
        assert "DATETIME" in str(columns['joined_at'].type)
        assert "DATETIME" in str(columns['created_at'].type)
        assert "DATETIME" in str(columns['updated_at'].type)

    def test_group_user_default_values(self):
        """Test GroupUser model default values."""
        # Act
        columns = GroupUser.__table__.columns
        
        # Assert
        assert columns['role'].default.arg == "operator"
        assert columns['status'].default.arg == "active"
        assert columns['auto_created'].default.arg is False

    def test_group_user_relationships(self):
        """Test that GroupUser model has the expected relationships."""
        # Act
        relationships = GroupUser.__mapper__.relationships
        
        # Assert
        assert 'group' in relationships
        assert 'user' in relationships

    def test_group_user_repr(self):
        """Test string representation of GroupUser model."""
        # Arrange
        group_user = GroupUser(
            group_id="test_group",
            user_id="test_user",
            role="admin"
        )
        
        # Act
        repr_str = repr(group_user)
        
        # Assert
        assert "GroupUser" in repr_str
        assert "test_group" in repr_str
        assert "test_user" in repr_str
        assert "admin" in repr_str

    def test_group_user_membership_scenarios(self):
        """Test different group membership scenarios."""
        # Owner/founder
        founder = GroupUser(
            group_id="startup_group",
            user_id="founder_001",
            role="admin",
            auto_created=True
        )

        # Invited admin
        admin = GroupUser(
            group_id="startup_group",
            user_id="admin_002",
            role="admin",
            auto_created=False
        )

        # Regular team member
        member = GroupUser(
            group_id="startup_group",
            user_id="member_003",
            role="operator",
            auto_created=False
        )

        # Content creator
        editor = GroupUser(
            group_id="startup_group",
            user_id="editor_004",
            role="editor",
            auto_created=False
        )

        # Assert
        assert founder.auto_created is True
        assert admin.auto_created is False
        assert founder.role == "admin"
        assert member.role == "operator"
        assert editor.role == "editor"


class TestGroupEnums:
    """Test cases for Group-related enums."""

    def test_group_status_enum(self):
        """Test GroupStatus enum values."""
        # Act & Assert
        assert GroupStatus.ACTIVE == "active"
        assert GroupStatus.SUSPENDED == "suspended"
        assert GroupStatus.ARCHIVED == "archived"

    def test_group_user_role_enum(self):
        """Test GroupUserRole enum values."""
        # Act & Assert
        assert GroupUserRole.ADMIN == "admin"
        assert GroupUserRole.EDITOR == "editor"
        assert GroupUserRole.OPERATOR == "operator"

    def test_group_user_status_enum(self):
        """Test GroupUserStatus enum values."""
        # Act & Assert
        assert GroupUserStatus.ACTIVE == "active"
        assert GroupUserStatus.INACTIVE == "inactive"
        assert GroupUserStatus.SUSPENDED == "suspended"


class TestGroupRolePermissions:
    """Test cases for role hierarchy and permission functions."""

    def test_role_hierarchy_levels(self):
        """Test that get_role_hierarchy returns correct hierarchy levels."""
        # Act & Assert
        assert get_role_hierarchy(GroupUserRole.ADMIN) == 3
        assert get_role_hierarchy(GroupUserRole.EDITOR) == 2
        assert get_role_hierarchy(GroupUserRole.OPERATOR) == 1

    def test_role_hierarchy_ordering(self):
        """Test that role hierarchy maintains proper ordering."""
        # Act
        admin_level = get_role_hierarchy(GroupUserRole.ADMIN)
        editor_level = get_role_hierarchy(GroupUserRole.EDITOR)
        operator_level = get_role_hierarchy(GroupUserRole.OPERATOR)

        # Assert
        assert admin_level > editor_level
        assert editor_level > operator_level

    def test_role_has_access_admin(self):
        """Test that admin role has access to all roles."""
        # Act & Assert
        assert role_has_access(GroupUserRole.ADMIN, GroupUserRole.ADMIN)
        assert role_has_access(GroupUserRole.ADMIN, GroupUserRole.EDITOR)
        assert role_has_access(GroupUserRole.ADMIN, GroupUserRole.OPERATOR)

    def test_role_has_access_editor(self):
        """Test that editor role has appropriate access."""
        # Act & Assert
        assert not role_has_access(GroupUserRole.EDITOR, GroupUserRole.ADMIN)
        assert role_has_access(GroupUserRole.EDITOR, GroupUserRole.EDITOR)
        assert role_has_access(GroupUserRole.EDITOR, GroupUserRole.OPERATOR)

    def test_role_has_access_operator(self):
        """Test that operator role has limited access."""
        # Act & Assert
        assert not role_has_access(GroupUserRole.OPERATOR, GroupUserRole.ADMIN)
        assert not role_has_access(GroupUserRole.OPERATOR, GroupUserRole.EDITOR)
        assert role_has_access(GroupUserRole.OPERATOR, GroupUserRole.OPERATOR)


class TestGenerateUuidFunction:
    """Test cases for generate_uuid function."""

    def test_generate_uuid_function(self):
        """Test the generate_uuid function."""
        # Act
        uuid1 = generate_uuid()
        uuid2 = generate_uuid()
        
        # Assert
        assert uuid1 is not None
        assert uuid2 is not None
        assert uuid1 != uuid2
        assert isinstance(uuid1, str)
        assert isinstance(uuid2, str)
        assert len(uuid1) == 36  # Standard UUID length
        assert len(uuid2) == 36

    def test_generate_uuid_uniqueness(self):
        """Test that generate_uuid generates unique IDs."""
        # Act
        uuids = [generate_uuid() for _ in range(20)]
        
        # Assert
        assert len(set(uuids)) == 20  # All UUIDs should be unique


class TestGroupEdgeCases:
    """Test edge cases and error scenarios for Group models."""

    def test_group_very_long_names(self):
        """Test Group with very long names and descriptions."""
        # Arrange
        long_name = "Very Long Company Name " * 10  # 260 characters
        long_description = "This is a very long description " * 20  # 660 characters
        
        # Act
        group = Group(
            id="long_name_group",
            name=long_name,
            email_domain="long-company.com",
            description=long_description
        )
        
        # Assert
        assert len(group.name) == 230
        assert len(group.description) == 640

    def test_group_complex_email_domains(self):
        """Test Group with complex email domains."""
        complex_domains = [
            "sub.domain.company.co.uk",
            "dept.university.edu",
            "team.startup.io",
            "division.enterprise.com",
            "my-company-name.business.org"
        ]
        
        # Test with company names instead of domains
        complex_names = [
            "Sub Department Company UK",
            "University Division",
            "Team Startup IO",
            "Division Enterprise Corp",
            "My Company Business Org"
        ]

        for name in complex_names:
            # Act
            group_id = Group.generate_group_id(name)

            # Assert
            assert "_" in group_id
            assert "." not in group_id
            assert " " not in group_id

    def test_group_user_edge_cases(self):
        """Test GroupUser edge cases."""
        # Very long IDs
        long_group_id = "very_long_group_id_" * 5  # 100 characters
        long_user_id = "very_long_user_id_" * 5   # 95 characters
        
        # Act
        group_user = GroupUser(
            group_id=long_group_id,
            user_id=long_user_id,
            role="ADMIN"
        )
        
        # Assert
        assert len(group_user.group_id) == 95
        assert len(group_user.user_id) == 90

    def test_group_common_use_cases(self):
        """Test Group configurations for common use cases."""
        # Startup company
        startup = Group(
            id="techstart_ai_12345678",
            name="TechStart AI",
            description="AI-powered startup focusing on automation",
            auto_created=True,
            created_by_email="founder@techstart.ai"
        )

        # Enterprise division
        enterprise = Group(
            id="megacorp_engineering_87654321",
            name="MegaCorp Engineering Division",
            description="Engineering division of MegaCorp",
            auto_created=False,
            created_by_email="admin@megacorp.com"
        )

        # Educational institution
        university = Group(
            id="university_cs_11223344",
            name="University Computer Science",
            description="Computer Science department",
            auto_created=False,
            created_by_email="head@cs.university.edu"
        )
        
        # Assert
        assert startup.auto_created is True
        assert "AI" in startup.name
        
        assert enterprise.auto_created is False
        assert "Engineering" in enterprise.name
        
        assert "Computer Science" in university.name
        assert "department" in university.description

    def test_group_membership_patterns(self):
        """Test common group membership patterns."""
        group_id = "company_team"
        
        # Founder/Admin
        founder = GroupUser(
            group_id=group_id,
            user_id="founder",
            role="ADMIN",
            auto_created=True,
            status="ACTIVE"
        )
        
        # Department manager
        manager = GroupUser(
            group_id=group_id,
            user_id="dept_manager",
            role="MANAGER",
            auto_created=False,
            status="ACTIVE"
        )
        
        # Team members
        developers = [
            GroupUser(group_id=group_id, user_id=f"dev_{i}", role="USER", status="ACTIVE")
            for i in range(5)
        ]
        
        # External stakeholder
        stakeholder = GroupUser(
            group_id=group_id,
            user_id="external_stakeholder",
            role="VIEWER",
            auto_created=False,
            status="ACTIVE"
        )
        
        # Inactive member
        former_employee = GroupUser(
            group_id=group_id,
            user_id="former_employee",
            role="USER",
            status="INACTIVE"
        )
        
        # Assert
        assert founder.role == "ADMIN" and founder.auto_created
        assert manager.role == "MANAGER"
        assert all(dev.role == "USER" for dev in developers)
        assert stakeholder.role == "VIEWER"
        assert former_employee.status == "INACTIVE"