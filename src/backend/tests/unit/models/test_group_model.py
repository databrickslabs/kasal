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
    GROUP_PERMISSIONS
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
        assert 'email_domain' in columns
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
            'id', 'name', 'email_domain', 'status', 'description',
            'auto_created', 'created_by_email', 'created_at', 'updated_at'
        ]
        
        for col_name in expected_columns:
            assert col_name in columns, f"Column {col_name} should exist in Group model"

    def test_group_generate_group_id_basic(self):
        """Test Group.generate_group_id with basic domain."""
        # Act
        group_id = Group.generate_group_id("acme-corp.com")
        
        # Assert
        assert group_id.startswith("acme_corp_")
        assert len(group_id.split("_")[-1]) == 8  # UUID part
        assert "_" in group_id

    def test_group_generate_group_id_with_name(self):
        """Test Group.generate_group_id with domain and group name."""
        # Act
        group_id = Group.generate_group_id("tech-startup.io", "Engineering Team")
        
        # Assert
        assert "tech_startup_io_engineering_team_" in group_id
        assert len(group_id.split("_")[-1]) == 8  # UUID part

    def test_group_generate_group_id_special_characters(self):
        """Test Group.generate_group_id handles special characters."""
        # Act
        group_id = Group.generate_group_id("my-company.co.uk", "Sales & Marketing")
        
        # Assert
        assert "my_company_co_uk_sales__marketing_" in group_id
        assert "&" not in group_id
        assert "." not in group_id
        assert " " not in group_id

    def test_group_generate_group_name_basic(self):
        """Test Group.generate_group_name with basic domain."""
        # Act
        name = Group.generate_group_name("acme-corp.com")
        
        # Assert
        assert name == "Acme Corp"

    def test_group_generate_group_name_complex(self):
        """Test Group.generate_group_name with complex domain."""
        # Act
        name1 = Group.generate_group_name("tech-startup.io")
        name2 = Group.generate_group_name("my_company.co.uk")
        name3 = Group.generate_group_name("simple")
        
        # Assert
        assert name1 == "Tech Startup"
        assert name2 == "My Company"
        assert name3 == "Simple"

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
        assert columns['email_domain'].nullable is False
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

    def test_group_different_domains(self):
        """Test Group email domain column configuration."""
        # Test that email_domain column exists and is configured correctly
        columns = Group.__table__.columns
        
        assert 'email_domain' in columns
        assert columns['email_domain'].nullable is False
        assert "VARCHAR" in str(columns['email_domain'].type) or "STRING" in str(columns['email_domain'].type)

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
        assert group_user.role is None or group_user.role == "USER"  # Default at DB level
        assert group_user.status is None or group_user.status == "ACTIVE"  # Default at DB level
        assert group_user.auto_created is None or group_user.auto_created is False  # Default at DB level

    def test_group_user_creation_with_all_fields(self):
        """Test GroupUser model creation with all fields populated."""
        # Arrange
        group_id = "engineering_team"
        user_id = "developer_001"
        role = "ADMIN"
        status = "ACTIVE"
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
        roles = ["ADMIN", "MANAGER", "USER", "VIEWER"]
        
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
        statuses = ["ACTIVE", "INACTIVE", "SUSPENDED"]
        
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
        assert columns['role'].default.arg == "USER"
        assert columns['status'].default.arg == "ACTIVE"
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
            role="ADMIN"
        )
        
        # Act
        repr_str = repr(group_user)
        
        # Assert
        assert "GroupUser" in repr_str
        assert "test_group" in repr_str
        assert "test_user" in repr_str
        assert "ADMIN" in repr_str

    def test_group_user_membership_scenarios(self):
        """Test different group membership scenarios."""
        # Owner/founder
        founder = GroupUser(
            group_id="startup_group",
            user_id="founder_001",
            role="ADMIN",
            auto_created=True
        )
        
        # Invited admin
        admin = GroupUser(
            group_id="startup_group",
            user_id="admin_002",
            role="ADMIN",
            auto_created=False
        )
        
        # Regular team member
        member = GroupUser(
            group_id="startup_group",
            user_id="member_003",
            role="USER",
            auto_created=False
        )
        
        # Read-only stakeholder
        viewer = GroupUser(
            group_id="startup_group",
            user_id="viewer_004",
            role="VIEWER",
            auto_created=False
        )
        
        # Assert
        assert founder.auto_created is True
        assert admin.auto_created is False
        assert founder.role == "ADMIN"
        assert member.role == "USER"
        assert viewer.role == "VIEWER"


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
        assert GroupUserRole.MANAGER == "manager"
        assert GroupUserRole.USER == "user"
        assert GroupUserRole.VIEWER == "viewer"

    def test_group_user_status_enum(self):
        """Test GroupUserStatus enum values."""
        # Act & Assert
        assert GroupUserStatus.ACTIVE == "active"
        assert GroupUserStatus.INACTIVE == "inactive"
        assert GroupUserStatus.SUSPENDED == "suspended"


class TestGroupPermissions:
    """Test cases for GROUP_PERMISSIONS mapping."""

    def test_group_permissions_structure(self):
        """Test that GROUP_PERMISSIONS has correct structure."""
        # Act & Assert
        assert isinstance(GROUP_PERMISSIONS, dict)
        assert len(GROUP_PERMISSIONS) == 4
        
        # Check all roles are present
        assert GroupUserRole.ADMIN in GROUP_PERMISSIONS
        assert GroupUserRole.MANAGER in GROUP_PERMISSIONS
        assert GroupUserRole.USER in GROUP_PERMISSIONS
        assert GroupUserRole.VIEWER in GROUP_PERMISSIONS

    def test_admin_permissions(self):
        """Test that admin role has comprehensive permissions."""
        # Act
        admin_perms = GROUP_PERMISSIONS[GroupUserRole.ADMIN]
        
        # Assert
        assert len(admin_perms) > 20  # Should have many permissions
        assert any("group:manage" in str(perm) for perm in admin_perms)
        assert any("user:" in str(perm) for perm in admin_perms)
        assert any("agent:" in str(perm) for perm in admin_perms)
        assert any("api_key:" in str(perm) for perm in admin_perms)

    def test_manager_permissions(self):
        """Test that manager role has appropriate permissions."""
        # Act
        manager_perms = GROUP_PERMISSIONS[GroupUserRole.MANAGER]
        
        # Assert
        assert len(manager_perms) > 10  # Should have moderate permissions
        assert len(manager_perms) < len(GROUP_PERMISSIONS[GroupUserRole.ADMIN])

    def test_user_permissions(self):
        """Test that user role has execution permissions."""
        # Act
        user_perms = GROUP_PERMISSIONS[GroupUserRole.USER]
        
        # Assert
        assert len(user_perms) > 5  # Should have basic permissions
        assert any(":read" in str(perm) for perm in user_perms)
        assert any(":execute" in str(perm) for perm in user_perms)

    def test_viewer_permissions(self):
        """Test that viewer role has minimal read-only permissions."""
        # Act
        viewer_perms = GROUP_PERMISSIONS[GroupUserRole.VIEWER]
        
        # Assert
        assert len(viewer_perms) > 0  # Should have some permissions
        assert len(viewer_perms) < len(GROUP_PERMISSIONS[GroupUserRole.USER])
        assert all(":read" in str(perm) for perm in viewer_perms)  # Should only have read permissions

    def test_permission_hierarchy(self):
        """Test that permission hierarchy makes sense (admin > manager > user > viewer)."""
        # Act
        admin_count = len(GROUP_PERMISSIONS[GroupUserRole.ADMIN])
        manager_count = len(GROUP_PERMISSIONS[GroupUserRole.MANAGER])
        user_count = len(GROUP_PERMISSIONS[GroupUserRole.USER])
        viewer_count = len(GROUP_PERMISSIONS[GroupUserRole.VIEWER])
        
        # Assert
        assert admin_count > manager_count
        assert manager_count > user_count
        assert user_count > viewer_count


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
        
        for domain in complex_domains:
            # Act
            group_id = Group.generate_group_id(domain)
            group_name = Group.generate_group_name(domain)
            
            # Assert
            assert "_" in group_id
            assert "." not in group_id
            assert " " in group_name or group_name.isalpha()

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
            email_domain="techstart.ai",
            description="AI-powered startup focusing on automation",
            auto_created=True,
            created_by_email="founder@techstart.ai"
        )
        
        # Enterprise division
        enterprise = Group(
            id="megacorp_engineering_87654321", 
            name="MegaCorp Engineering Division",
            email_domain="engineering.megacorp.com",
            description="Engineering division of MegaCorp",
            auto_created=False,
            created_by_email="admin@megacorp.com"
        )
        
        # Educational institution
        university = Group(
            id="university_cs_11223344",
            name="University Computer Science",
            email_domain="cs.university.edu",
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
        assert ".edu" in university.email_domain

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