"""
Unit tests for DatabricksRoleService.

Tests all functionality of the Databricks-based role assignment service with 100% coverage.
"""
import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call
from datetime import datetime, timezone
import aiohttp

from sqlalchemy.ext.asyncio import AsyncSession

from src.services.databricks_role_service import DatabricksRoleService
from src.models.user import User, Role, UserRole
from src.models.enums import UserRole as UserRoleEnum, UserStatus


class TestDatabricksRoleService:
    """Test suite for DatabricksRoleService with 100% coverage."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        session = AsyncMock(spec=AsyncSession)
        session.commit = AsyncMock()
        session.flush = AsyncMock()
        session.add = MagicMock()
        return session

    @pytest.fixture
    def mock_user(self):
        """Create a mock user."""
        user = MagicMock(spec=User)
        user.id = "user-123"
        user.email = "test@example.com"
        user.username = "testuser"
        user.role = UserRoleEnum.REGULAR
        user.status = UserStatus.ACTIVE
        return user

    @pytest.fixture
    def mock_admin_role(self):
        """Create a mock admin role."""
        role = MagicMock(spec=Role)
        role.id = "role-admin"
        role.name = "admin"
        role.description = "Administrator role"
        return role

    @pytest.fixture
    def service(self, mock_session):
        """Create a DatabricksRoleService instance with mocked dependencies."""
        with patch.dict(os.environ, {
            "DATABRICKS_APP_NAME": "test-app",
            "DATABRICKS_HOST": "https://test.databricks.com",
            "DATABRICKS_TOKEN": "test-token",
            "ENVIRONMENT": "production"
        }):
            service = DatabricksRoleService(mock_session)
            # Mock the repositories
            service.user_repository = AsyncMock()
            service.role_repository = AsyncMock()
            service.user_role_repository = AsyncMock()
            service.user_service = AsyncMock()
            return service

    @pytest.fixture
    def service_local_dev(self, mock_session):
        """Create a DatabricksRoleService instance in local development mode."""
        with patch.dict(os.environ, {
            "DATABRICKS_APP_NAME": "",
            "DATABRICKS_HOST": "",
            "DATABRICKS_TOKEN": "",
            "ENVIRONMENT": "development"
        }, clear=True):
            service = DatabricksRoleService(mock_session)
            # Mock the repositories
            service.user_repository = AsyncMock()
            service.role_repository = AsyncMock()
            service.user_role_repository = AsyncMock()
            service.user_service = AsyncMock()
            return service

    @pytest.mark.asyncio
    async def test_init_production_mode(self, mock_session):
        """Test service initialization in production mode."""
        with patch.dict(os.environ, {
            "DATABRICKS_APP_NAME": "prod-app",
            "DATABRICKS_HOST": "https://prod.databricks.com",
            "DATABRICKS_TOKEN": "prod-token",
            "ENVIRONMENT": "production"
        }):
            service = DatabricksRoleService(mock_session)
            assert service.app_name == "prod-app"
            assert service.databricks_host == "https://prod.databricks.com"
            assert service.databricks_token == "prod-token"
            assert service.is_local_dev is False

    @pytest.mark.asyncio
    async def test_init_local_dev_mode(self, mock_session):
        """Test service initialization in local development mode."""
        with patch.dict(os.environ, {
            "ENVIRONMENT": "development"
        }, clear=True):
            service = DatabricksRoleService(mock_session)
            assert service.is_local_dev is True

    @pytest.mark.asyncio
    async def test_sync_admin_roles_success(self, service, mock_admin_role):
        """Test successful admin role synchronization."""
        # Mock the methods
        service.get_databricks_app_managers = AsyncMock(return_value=["admin1@example.com", "admin2@example.com"])
        service._get_or_create_admin_role = AsyncMock(return_value=mock_admin_role)
        service._process_admin_user = AsyncMock(side_effect=[
            {"email": "admin1@example.com", "role_assigned": True},
            {"email": "admin2@example.com", "already_admin": True}
        ])

        result = await service.sync_admin_roles()

        assert result["success"] is True
        assert result["admin_emails"] == ["admin1@example.com", "admin2@example.com"]
        assert len(result["processed_users"]) == 2
        assert result["errors"] == []
        service.get_databricks_app_managers.assert_called_once()
        service._get_or_create_admin_role.assert_called_once()
        assert service._process_admin_user.call_count == 2

    @pytest.mark.asyncio
    async def test_sync_admin_roles_no_emails(self, service):
        """Test admin role synchronization with no admin emails found."""
        service.get_databricks_app_managers = AsyncMock(return_value=[])

        result = await service.sync_admin_roles()

        assert result["success"] is False
        assert result["error"] == "No admin emails found"

    @pytest.mark.asyncio
    async def test_sync_admin_roles_with_errors(self, service, mock_admin_role):
        """Test admin role synchronization with processing errors."""
        service.get_databricks_app_managers = AsyncMock(return_value=["admin1@example.com", "admin2@example.com"])
        service._get_or_create_admin_role = AsyncMock(return_value=mock_admin_role)
        service._process_admin_user = AsyncMock(side_effect=[
            {"email": "admin1@example.com", "role_assigned": True},
            Exception("Processing error")
        ])

        result = await service.sync_admin_roles()

        assert result["success"] is True
        assert len(result["processed_users"]) == 1
        assert len(result["errors"]) == 1
        assert "Error processing admin user admin2@example.com" in result["errors"][0]

    @pytest.mark.asyncio
    async def test_sync_admin_roles_exception(self, service):
        """Test admin role synchronization with general exception."""
        service.get_databricks_app_managers = AsyncMock(side_effect=Exception("API error"))

        result = await service.sync_admin_roles()

        assert result["success"] is False
        assert result["error"] == "API error"

    @pytest.mark.asyncio
    async def test_get_databricks_app_managers_local_dev(self, service_local_dev):
        """Test getting admin emails in local development mode."""
        service_local_dev._get_fallback_admins = MagicMock(return_value=["dev@localhost"])

        result = await service_local_dev.get_databricks_app_managers()

        assert result == ["dev@localhost"]
        service_local_dev._get_fallback_admins.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_databricks_app_managers_production_missing_config(self, service):
        """Test getting admin emails in production with missing configuration."""
        service.app_name = None

        with pytest.raises(Exception) as exc_info:
            await service.get_databricks_app_managers()

        assert "Databricks configuration incomplete" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_databricks_app_managers_production_success(self, service):
        """Test getting admin emails from Databricks API in production."""
        service._fetch_databricks_permissions = AsyncMock(return_value=["admin@prod.com"])

        result = await service.get_databricks_app_managers()

        assert result == ["admin@prod.com"]
        service._fetch_databricks_permissions.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_databricks_app_managers_production_api_error(self, service):
        """Test getting admin emails with API error in production."""
        service._fetch_databricks_permissions = AsyncMock(side_effect=Exception("API failed"))

        with pytest.raises(Exception) as exc_info:
            await service.get_databricks_app_managers()

        assert "Failed to fetch Databricks permissions in production" in str(exc_info.value)

    def test_get_fallback_admins_production_mode(self, service):
        """Test fallback admins method raises error in production mode."""
        with pytest.raises(Exception) as exc_info:
            service._get_fallback_admins()

        assert "SECURITY: Fallback admin method cannot be used in production" in str(exc_info.value)

    def test_get_fallback_admins_with_admin_emails_env(self, service_local_dev):
        """Test fallback admins with ADMIN_EMAILS environment variable."""
        with patch.dict(os.environ, {"ADMIN_EMAILS": "admin1@test.com, admin2@test.com"}):
            result = service_local_dev._get_fallback_admins()

        assert result == ["admin1@test.com", "admin2@test.com"]

    def test_get_fallback_admins_with_developer_email_env(self, service_local_dev):
        """Test fallback admins with DEVELOPER_EMAIL environment variable."""
        with patch.dict(os.environ, {"DEVELOPER_EMAIL": "dev@test.com"}):
            result = service_local_dev._get_fallback_admins()

        assert result == ["dev@test.com"]

    def test_get_fallback_admins_default_mock_users(self, service_local_dev):
        """Test fallback admins returns default mock users."""
        result = service_local_dev._get_fallback_admins()

        assert "alice@acme-corp.com" in result
        assert "bob@tech-startup.io" in result
        assert "charlie@big-enterprise.com" in result
        assert "admin@localhost" in result

    @pytest.mark.asyncio
    async def test_fetch_databricks_permissions_success(self, service):
        """Test successful fetch of Databricks permissions."""
        mock_response_data = {
            "access_control_list": [
                {
                    "user_name": "admin1@example.com",
                    "all_permissions": [{"permission_level": "CAN_MANAGE"}]
                },
                {
                    "user_name": "user@example.com",
                    "all_permissions": [{"permission_level": "CAN_VIEW"}]
                }
            ]
        }

        with patch('aiohttp.ClientSession') as mock_session_class:
            # Create the async context manager for ClientSession
            mock_session = MagicMock()
            mock_session_class.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_class.return_value.__aexit__ = AsyncMock()
            
            # Create the async context manager for the GET request
            mock_response = MagicMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value=mock_response_data)
            
            mock_get_cm = MagicMock()
            mock_get_cm.__aenter__ = AsyncMock(return_value=mock_response)
            mock_get_cm.__aexit__ = AsyncMock()
            
            mock_session.get = MagicMock(return_value=mock_get_cm)

            result = await service._fetch_databricks_permissions()

            # The method should extract the manage users
            assert len(result) == 1
            assert "admin1@example.com" in result
            mock_session.get.assert_called_once()
            call_args = mock_session.get.call_args
            assert service.app_name in call_args[0][0]
            assert call_args[1]["headers"]["Authorization"] == f"Bearer {service.databricks_token}"

    @pytest.mark.asyncio
    async def test_fetch_databricks_permissions_404(self, service):
        """Test fetch permissions when app not found."""
        with patch('aiohttp.ClientSession') as mock_session_class:
            # Create the async context manager for ClientSession
            mock_session = MagicMock()
            mock_session_class.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_class.return_value.__aexit__ = AsyncMock()
            
            # Create the async context manager for the GET request
            mock_response = MagicMock()
            mock_response.status = 404
            
            mock_get_cm = MagicMock()
            mock_get_cm.__aenter__ = AsyncMock(return_value=mock_response)
            mock_get_cm.__aexit__ = AsyncMock()
            
            mock_session.get = MagicMock(return_value=mock_get_cm)

            result = await service._fetch_databricks_permissions()

            assert result == []

    @pytest.mark.asyncio
    async def test_fetch_databricks_permissions_error(self, service):
        """Test fetch permissions with API error."""
        # Mock the aiohttp session and response
        mock_response = MagicMock()
        mock_response.status = 500
        mock_response.text = AsyncMock(return_value="Internal Server Error")
        
        # Patch the entire aiohttp module since we're dealing with async context managers
        with patch('src.services.databricks_role_service.aiohttp.ClientSession') as mock_session_class:
            # Mock the ClientSession context manager
            mock_session = MagicMock()
            mock_session_instance = MagicMock()
            mock_session_instance.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_instance.__aexit__ = AsyncMock(return_value=None)
            mock_session_class.return_value = mock_session_instance
            
            # Mock the get method's context manager  
            mock_get_instance = MagicMock()
            mock_get_instance.__aenter__ = AsyncMock(return_value=mock_response)
            mock_get_instance.__aexit__ = AsyncMock(return_value=None)
            mock_session.get = MagicMock(return_value=mock_get_instance)

            with pytest.raises(Exception) as exc_info:
                await service._fetch_databricks_permissions()

            assert "Databricks API error 500" in str(exc_info.value)

    def test_extract_manage_users(self, service):
        """Test extracting users with CAN_MANAGE permission."""
        permissions_data = {
            "access_control_list": [
                {
                    "user_name": "admin1@example.com",
                    "all_permissions": [{"permission_level": "CAN_MANAGE"}]
                },
                {
                    "user_name": "viewer@example.com",
                    "all_permissions": [{"permission_level": "CAN_VIEW"}]
                },
                {
                    "user_name": "admin2@example.com",
                    "all_permissions": [
                        {"permission_level": "CAN_VIEW"},
                        {"permission_level": "CAN_MANAGE"}
                    ]
                },
                {
                    # User without user_name
                    "all_permissions": [{"permission_level": "CAN_MANAGE"}]
                }
            ]
        }

        result = service._extract_manage_users(permissions_data)

        assert len(result) == 2
        assert "admin1@example.com" in result
        assert "admin2@example.com" in result
        assert "viewer@example.com" not in result

    def test_extract_manage_users_empty_acl(self, service):
        """Test extracting users with empty access control list."""
        permissions_data = {"access_control_list": []}

        result = service._extract_manage_users(permissions_data)

        assert result == []

    def test_extract_manage_users_no_acl(self, service):
        """Test extracting users with no access control list."""
        permissions_data = {}

        result = service._extract_manage_users(permissions_data)

        assert result == []

    @pytest.mark.asyncio
    async def test_get_or_create_admin_role_exists(self, service, mock_admin_role):
        """Test getting existing admin role."""
        service.role_repository.get_by_name.return_value = mock_admin_role

        result = await service._get_or_create_admin_role()

        assert result == mock_admin_role
        service.role_repository.get_by_name.assert_called_once_with("admin")

    @pytest.mark.asyncio
    async def test_get_or_create_admin_role_not_exists(self, service):
        """Test when admin role doesn't exist."""
        service.role_repository.get_by_name.return_value = None

        with pytest.raises(Exception) as exc_info:
            await service._get_or_create_admin_role()

        assert "Admin role not found" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_process_admin_user_new_user(self, service, mock_admin_role):
        """Test processing a new admin user."""
        service.user_repository.get_by_email.return_value = None
        
        mock_new_user = MagicMock()
        mock_new_user.id = "new-user-123"
        service._create_placeholder_user = AsyncMock(return_value=mock_new_user)
        
        service.user_role_repository.has_role.return_value = False
        service.user_role_repository.assign_role = AsyncMock()

        result = await service._process_admin_user("newadmin@example.com", mock_admin_role)

        assert result["email"] == "newadmin@example.com"
        assert result["user_created"] is True
        assert result["role_assigned"] is True
        assert result["already_admin"] is False
        
        service._create_placeholder_user.assert_called_once_with("newadmin@example.com")
        service.user_role_repository.assign_role.assert_called_once()
        service.session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_admin_user_existing_user_new_admin(self, service, mock_user, mock_admin_role):
        """Test processing an existing user who needs admin role."""
        service.user_repository.get_by_email.return_value = mock_user
        service.user_role_repository.has_role.return_value = False
        service.user_role_repository.assign_role = AsyncMock()

        result = await service._process_admin_user(mock_user.email, mock_admin_role)

        assert result["email"] == mock_user.email
        assert result["user_created"] is False
        assert result["role_assigned"] is True
        assert result["already_admin"] is False

    @pytest.mark.asyncio
    async def test_process_admin_user_already_admin(self, service, mock_user, mock_admin_role):
        """Test processing a user who already has admin role."""
        service.user_repository.get_by_email.return_value = mock_user
        service.user_role_repository.has_role.return_value = True

        result = await service._process_admin_user(mock_user.email, mock_admin_role)

        assert result["email"] == mock_user.email
        assert result["user_created"] is False
        assert result["role_assigned"] is False
        assert result["already_admin"] is True

    @pytest.mark.asyncio
    async def test_process_admin_user_exception(self, service, mock_admin_role):
        """Test processing admin user with exception."""
        service.user_repository.get_by_email.side_effect = Exception("Database error")

        result = await service._process_admin_user("error@example.com", mock_admin_role)

        assert result["email"] == "error@example.com"
        assert "error" in result
        assert "Database error" in result["error"]

    @pytest.mark.asyncio
    async def test_create_placeholder_user(self, service):
        """Test creating a placeholder user."""
        email = "placeholder@example.com"

        result = await service._create_placeholder_user(email)

        assert result.username == "placeholder"
        assert result.email == email
        assert result.hashed_password == "placeholder_password"
        assert result.role == UserRoleEnum.ADMIN
        assert result.status == UserStatus.ACTIVE
        
        service.session.add.assert_called_once()
        service.session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_check_user_admin_access_true(self, service):
        """Test checking user admin access when user is admin."""
        service.user_role_repository.has_role.return_value = True

        result = await service.check_user_admin_access("user-123")

        assert result is True
        service.user_role_repository.has_role.assert_called_once_with("user-123", "admin")

    @pytest.mark.asyncio
    async def test_check_user_admin_access_false(self, service):
        """Test checking user admin access when user is not admin."""
        service.user_role_repository.has_role.return_value = False

        result = await service.check_user_admin_access("user-123")

        assert result is False

    @pytest.mark.asyncio
    async def test_check_user_admin_access_exception(self, service):
        """Test checking user admin access with exception."""
        service.user_role_repository.has_role.side_effect = Exception("Database error")

        result = await service.check_user_admin_access("user-123")

        assert result is False

    @pytest.mark.asyncio
    async def test_check_user_privilege_true(self, service):
        """Test checking user privilege when user has it."""
        service.user_role_repository.has_privilege.return_value = True

        result = await service.check_user_privilege("user-123", "manage_crews")

        assert result is True
        service.user_role_repository.has_privilege.assert_called_once_with("user-123", "manage_crews")

    @pytest.mark.asyncio
    async def test_check_user_privilege_false(self, service):
        """Test checking user privilege when user doesn't have it."""
        service.user_role_repository.has_privilege.return_value = False

        result = await service.check_user_privilege("user-123", "manage_crews")

        assert result is False

    @pytest.mark.asyncio
    async def test_check_user_privilege_exception(self, service):
        """Test checking user privilege with exception."""
        service.user_role_repository.has_privilege.side_effect = Exception("Database error")

        result = await service.check_user_privilege("user-123", "manage_crews")

        assert result is False

    @pytest.mark.asyncio
    async def test_get_user_roles_success(self, service):
        """Test getting user roles successfully."""
        mock_roles = [MagicMock(name="admin"), MagicMock(name="user")]
        service.user_role_repository.get_user_roles.return_value = mock_roles

        result = await service.get_user_roles("user-123")

        assert result == mock_roles
        service.user_role_repository.get_user_roles.assert_called_once_with("user-123")

    @pytest.mark.asyncio
    async def test_get_user_roles_exception(self, service):
        """Test getting user roles with exception."""
        service.user_role_repository.get_user_roles.side_effect = Exception("Database error")

        result = await service.get_user_roles("user-123")

        assert result == []

    @pytest.mark.asyncio
    async def test_get_user_privileges_success(self, service):
        """Test getting user privileges successfully."""
        # Create mock privilege objects with proper name attribute
        mock_privilege1 = MagicMock()
        mock_privilege1.name = "manage_crews"
        
        mock_privilege2 = MagicMock()
        mock_privilege2.name = "view_executions"
        
        mock_privileges = [mock_privilege1, mock_privilege2]
        service.user_role_repository.get_user_privileges.return_value = mock_privileges

        result = await service.get_user_privileges("user-123")

        assert result == ["manage_crews", "view_executions"]
        service.user_role_repository.get_user_privileges.assert_called_once_with("user-123")

    @pytest.mark.asyncio
    async def test_get_user_privileges_exception(self, service):
        """Test getting user privileges with exception."""
        service.user_role_repository.get_user_privileges.side_effect = Exception("Database error")

        result = await service.get_user_privileges("user-123")

        assert result == []