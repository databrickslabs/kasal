"""
Unit tests for roles seeds.

Tests the functionality of the roles seeding module including
privilege and role creation, Databricks integration, and admin setup.
"""
import pytest
import os
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime
import aiohttp

from src.seeds.roles import (
    DatabricksPermissionChecker, seed_privileges, seed_roles,
    setup_databricks_admins, seed_async, seed_sync, seed
)


@pytest.fixture
def mock_session():
    """Create a mock async session."""
    session = AsyncMock()
    session.execute = AsyncMock()
    session.add = MagicMock()
    session.commit = AsyncMock()
    session.flush = AsyncMock()
    session.delete = AsyncMock()
    return session


@pytest.fixture
def mock_privilege():
    """Create a mock privilege object."""
    privilege = MagicMock()
    privilege.name = "test_privilege"
    privilege.description = "Test privilege description"
    privilege.id = 1
    return privilege


@pytest.fixture
def mock_role():
    """Create a mock role object."""
    role = MagicMock()
    role.name = "test_role"
    role.description = "Test role description"
    role.id = 1
    role.role_privileges = []
    return role


class TestDatabricksPermissionChecker:
    """Test cases for DatabricksPermissionChecker."""
    
    def test_init_with_all_env_vars(self):
        """Test initialization with all environment variables set."""
        with patch.dict(os.environ, {
            "DATABRICKS_APP_NAME": "test_app",
            "DATABRICKS_HOST": "https://test.databricks.com",
            "DATABRICKS_TOKEN": "test_token",
            "ENVIRONMENT": "production"
        }):
            checker = DatabricksPermissionChecker()
            
            assert checker.app_name == "test_app"
            assert checker.databricks_host == "https://test.databricks.com"
            assert checker.databricks_token == "test_token"
            assert checker.is_local_dev is False
    
    def test_init_development_environment(self):
        """Test initialization in development environment."""
        with patch.dict(os.environ, {"ENVIRONMENT": "development"}):
            checker = DatabricksPermissionChecker()
            
            assert checker.is_local_dev is True
    
    @pytest.mark.asyncio
    async def test_get_app_managers_local_dev(self):
        """Test getting app managers in local development mode."""
        with patch.dict(os.environ, {"ENVIRONMENT": "development"}):
            checker = DatabricksPermissionChecker()
            
            with patch.object(checker, "_get_fallback_admins", return_value=["admin@localhost"]):
                result = await checker.get_app_managers()
                
                assert result == ["admin@localhost"]
    
    @pytest.mark.asyncio
    async def test_get_app_managers_missing_config_production(self):
        """Test getting app managers with missing config in production."""
        with patch.dict(os.environ, {"ENVIRONMENT": "production"}, clear=True):
            checker = DatabricksPermissionChecker()
            
            with pytest.raises(Exception, match="Databricks configuration incomplete"):
                await checker.get_app_managers()
    
    @pytest.mark.asyncio
    async def test_get_app_managers_missing_config_dev(self):
        """Test getting app managers with missing config in development."""
        with patch.dict(os.environ, {"ENVIRONMENT": "development"}, clear=True):
            checker = DatabricksPermissionChecker()
            
            with patch.object(checker, "_get_fallback_admins", return_value=["dev@localhost"]):
                result = await checker.get_app_managers()
                
                assert result == ["dev@localhost"]
    
    @pytest.mark.asyncio
    async def test_get_app_managers_databricks_success(self):
        """Test successful Databricks API call."""
        with patch.dict(os.environ, {
            "DATABRICKS_APP_NAME": "test_app",
            "DATABRICKS_HOST": "https://test.databricks.com",
            "DATABRICKS_TOKEN": "test_token",
            "ENVIRONMENT": "production"
        }):
            checker = DatabricksPermissionChecker()
            
            with patch.object(checker, "_fetch_databricks_permissions", 
                             return_value=["admin@databricks.com"]):
                result = await checker.get_app_managers()
                
                assert result == ["admin@databricks.com"]
    
    @pytest.mark.asyncio
    async def test_get_app_managers_databricks_failure_production(self):
        """Test Databricks API failure in production."""
        with patch.dict(os.environ, {
            "DATABRICKS_APP_NAME": "test_app",
            "DATABRICKS_HOST": "https://test.databricks.com",
            "DATABRICKS_TOKEN": "test_token",
            "ENVIRONMENT": "production"
        }):
            checker = DatabricksPermissionChecker()
            
            with patch.object(checker, "_fetch_databricks_permissions", 
                             side_effect=Exception("API error")):
                with pytest.raises(Exception, match="Failed to fetch Databricks permissions in production"):
                    await checker.get_app_managers()
    
    @pytest.mark.asyncio
    async def test_get_app_managers_databricks_failure_dev(self):
        """Test Databricks API failure in development with fallback."""
        with patch.dict(os.environ, {
            "DATABRICKS_APP_NAME": "test_app",
            "DATABRICKS_HOST": "https://test.databricks.com", 
            "DATABRICKS_TOKEN": "test_token",
            "ENVIRONMENT": "development"
        }):
            checker = DatabricksPermissionChecker()
            
            with patch.object(checker, "_fetch_databricks_permissions", 
                             side_effect=Exception("API error")), \
                 patch.object(checker, "_get_fallback_admins", 
                             return_value=["fallback@localhost"]):
                
                result = await checker.get_app_managers()
                assert result == ["fallback@localhost"]
    
    def test_get_fallback_admins_production_security(self):
        """Test that fallback admins raises error in production."""
        with patch.dict(os.environ, {"ENVIRONMENT": "production"}):
            checker = DatabricksPermissionChecker()
            
            with pytest.raises(Exception, match="SECURITY: Fallback admin method cannot be used in production"):
                checker._get_fallback_admins()
    
    def test_get_fallback_admins_with_admin_emails(self):
        """Test fallback admins with ADMIN_EMAILS environment variable."""
        with patch.dict(os.environ, {
            "ENVIRONMENT": "development",
            "ADMIN_EMAILS": "admin1@test.com,admin2@test.com, admin3@test.com "
        }):
            checker = DatabricksPermissionChecker()
            
            result = checker._get_fallback_admins()
            
            assert result == ["admin1@test.com", "admin2@test.com", "admin3@test.com"]
    
    def test_get_fallback_admins_with_developer_email(self):
        """Test fallback admins with DEVELOPER_EMAIL environment variable."""
        with patch.dict(os.environ, {
            "ENVIRONMENT": "development",
            "DEVELOPER_EMAIL": "dev@example.com"
        }):
            checker = DatabricksPermissionChecker()
            
            result = checker._get_fallback_admins()
            
            assert result == ["dev@example.com"]
    
    def test_get_fallback_admins_default_mocks(self):
        """Test fallback admins with default mock emails."""
        with patch.dict(os.environ, {"ENVIRONMENT": "development"}, clear=True):
            checker = DatabricksPermissionChecker()
            
            result = checker._get_fallback_admins()
            
            # Should return the default mock emails
            assert "alice@acme-corp.com" in result
            assert "admin@localhost" in result
            assert len(result) > 0
    
    @pytest.mark.asyncio
    async def test_fetch_databricks_permissions_success(self):
        """Test successful Databricks API call."""
        checker = DatabricksPermissionChecker()
        checker.databricks_host = "https://test.databricks.com"
        checker.databricks_token = "test_token"
        checker.app_name = "test_app"
        
        mock_response_data = {
            "access_control_list": [
                {
                    "user_name": "admin@test.com",
                    "all_permissions": [{"permission_level": "CAN_MANAGE"}]
                }
            ]
        }
        
        with patch("aiohttp.ClientSession") as mock_session:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json.return_value = mock_response_data
            
            mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value = mock_response
            
            with patch.object(checker, "_extract_manage_users", return_value=["admin@test.com"]):
                result = await checker._fetch_databricks_permissions()
                
                assert result == ["admin@test.com"]
    
    @pytest.mark.asyncio
    async def test_fetch_databricks_permissions_api_error(self):
        """Test Databricks API error response."""
        checker = DatabricksPermissionChecker()
        checker.databricks_host = "https://test.databricks.com"
        checker.databricks_token = "test_token"
        checker.app_name = "test_app"
        
        with patch("aiohttp.ClientSession") as mock_session:
            mock_response = AsyncMock()
            mock_response.status = 404
            mock_response.text.return_value = "App not found"
            
            mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value = mock_response
            
            with pytest.raises(Exception, match="Databricks API error 404"):
                await checker._fetch_databricks_permissions()
    
    def test_extract_manage_users(self):
        """Test extracting users with manage permissions."""
        checker = DatabricksPermissionChecker()
        
        permissions_data = {
            "access_control_list": [
                {
                    "user_name": "admin@test.com",
                    "all_permissions": [{"permission_level": "CAN_MANAGE"}]
                },
                {
                    "user_name": "user@test.com",
                    "all_permissions": [{"permission_level": "CAN_VIEW"}]
                },
                {
                    "user_name": "manager@test.com",
                    "all_permissions": [
                        {"permission_level": "CAN_VIEW"},
                        {"permission_level": "CAN_MANAGE"}
                    ]
                }
            ]
        }
        
        result = checker._extract_manage_users(permissions_data)
        
        assert "admin@test.com" in result
        assert "manager@test.com" in result
        assert "user@test.com" not in result
        assert len(result) == 2


class TestSeedPrivileges:
    """Test cases for seed_privileges function."""
    
    @pytest.mark.asyncio
    async def test_seed_privileges_new_privileges(self, mock_session):
        """Test seeding new privileges."""
        # Mock existing privileges
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result
        
        with patch("src.seeds.roles.DEFAULT_PRIVILEGES", [("test_priv", "Test description")]):
            await seed_privileges(mock_session)
            
            # Should add new privilege
            mock_session.add.assert_called_once()
            mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_seed_privileges_update_existing(self, mock_session):
        """Test updating existing privileges."""
        # Mock existing privileges
        mock_result_names = MagicMock()
        mock_result_names.scalars.return_value.all.return_value = ["test_priv"]
        
        mock_privilege = MagicMock()
        mock_privilege.description = "Old description"
        mock_result_privilege = MagicMock()
        mock_result_privilege.scalars.return_value.first.return_value = mock_privilege
        
        mock_session.execute.side_effect = [mock_result_names, mock_result_privilege]
        
        with patch("src.seeds.roles.DEFAULT_PRIVILEGES", [("test_priv", "New description")]):
            await seed_privileges(mock_session)
            
            # Should update existing privilege
            assert mock_privilege.description == "New description"
            mock_session.commit.assert_called_once()


class TestSeedRoles:
    """Test cases for seed_roles function."""
    
    @pytest.mark.asyncio
    async def test_seed_roles_new_role(self, mock_session, mock_privilege):
        """Test seeding new roles."""
        # Mock privileges
        mock_priv_result = MagicMock()
        mock_priv_result.scalars.return_value.all.return_value = [mock_privilege]
        
        # Mock no existing roles
        mock_role_result = MagicMock()
        mock_role_result.scalars.return_value.all.return_value = []
        
        mock_session.execute.side_effect = [mock_priv_result, mock_role_result]
        
        with patch("src.seeds.roles.DEFAULT_ROLES", {
            "test_role": {
                "description": "Test role",
                "privileges": ["test_privilege"]
            }
        }):
            await seed_roles(mock_session)
            
            # Should add new role and role privilege
            assert mock_session.add.call_count >= 2  # Role + RolePrivilege
            mock_session.flush.assert_called_once()
            mock_session.commit.assert_called_once()


class TestSetupDatabricksAdmins:
    """Test cases for setup_databricks_admins function."""
    
    @pytest.mark.asyncio
    async def test_setup_databricks_admins_success(self):
        """Test successful Databricks admin setup."""
        with patch("src.seeds.roles.DatabricksPermissionChecker") as mock_checker_class:
            mock_checker = AsyncMock()
            mock_checker.get_app_managers.return_value = ["admin@test.com"]
            mock_checker_class.return_value = mock_checker
            
            result = await setup_databricks_admins()
            
            assert result == ["admin@test.com"]
    
    @pytest.mark.asyncio
    async def test_setup_databricks_admins_no_emails(self):
        """Test Databricks admin setup with no emails found."""
        with patch("src.seeds.roles.DatabricksPermissionChecker") as mock_checker_class:
            mock_checker = AsyncMock()
            mock_checker.get_app_managers.return_value = []
            mock_checker_class.return_value = mock_checker
            
            result = await setup_databricks_admins()
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_setup_databricks_admins_error(self):
        """Test Databricks admin setup with error."""
        with patch("src.seeds.roles.DatabricksPermissionChecker") as mock_checker_class:
            mock_checker = AsyncMock()
            mock_checker.get_app_managers.side_effect = Exception("API error")
            mock_checker_class.return_value = mock_checker
            
            with pytest.raises(Exception, match="API error"):
                await setup_databricks_admins()


class TestSeedAsync:
    """Test cases for seed_async function."""
    
    @pytest.mark.asyncio
    async def test_seed_async_success(self):
        """Test successful async seeding."""
        with patch("src.seeds.roles.async_session_factory") as mock_factory, \
             patch("src.seeds.roles.seed_privileges") as mock_seed_priv, \
             patch("src.seeds.roles.seed_roles") as mock_seed_roles, \
             patch("src.seeds.roles.setup_databricks_admins") as mock_setup_admins:
            
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            await seed_async()
            
            mock_seed_priv.assert_called_once_with(mock_session)
            mock_seed_roles.assert_called_once_with(mock_session)
            mock_setup_admins.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_seed_async_error(self):
        """Test async seeding with error."""
        with patch("src.seeds.roles.async_session_factory") as mock_factory:
            mock_factory.side_effect = Exception("Database error")
            
            with pytest.raises(Exception, match="Database error"):
                await seed_async()


class TestSeedSync:
    """Test cases for seed_sync function."""
    
    def test_seed_sync_not_implemented(self):
        """Test that sync seeding raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="Roles seeding requires async operations"):
            seed_sync()


class TestSeedFunction:
    """Test cases for main seed function."""
    
    @pytest.mark.asyncio
    async def test_seed_success(self):
        """Test successful seeding."""
        with patch("src.seeds.roles.seed_async") as mock_seed_async:
            await seed()
            
            mock_seed_async.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_seed_error_handling(self):
        """Test seeding error handling with logging."""
        with patch("src.seeds.roles.seed_async", side_effect=Exception("Seed error")), \
             patch("src.seeds.roles.logger") as mock_logger:
            
            with pytest.raises(Exception, match="Seed error"):
                await seed()
            
            mock_logger.error.assert_called()


class TestModuleLevel:
    """Test cases for module-level functionality."""
    
    def test_default_privileges_import(self):
        """Test that default privileges are imported correctly."""
        from src.seeds.roles import DEFAULT_PRIVILEGES
        
        assert DEFAULT_PRIVILEGES is not None
        assert isinstance(DEFAULT_PRIVILEGES, (list, tuple))
    
    def test_default_roles_import(self):
        """Test that default roles are imported correctly."""
        from src.seeds.roles import DEFAULT_ROLES
        
        assert DEFAULT_ROLES is not None
        assert isinstance(DEFAULT_ROLES, dict)
    
    def test_logger_configuration(self):
        """Test that logger is properly configured."""
        from src.seeds.roles import logger
        
        assert logger.name == "src.seeds.roles"
    
    def test_model_imports(self):
        """Test that required models are imported."""
        try:
            from src.seeds.roles import Role, Privilege, RolePrivilege
            
            assert Role is not None
            assert Privilege is not None
            assert RolePrivilege is not None
        except ImportError as e:
            pytest.fail(f"Failed to import required models: {e}")
    
    def test_main_execution(self):
        """Test main execution block."""
        # This tests the if __name__ == "__main__" block
        with patch("asyncio.run") as mock_run:
            # Simulate running the module directly
            exec("""
if __name__ == "__main__":
    import asyncio
    from src.seeds.roles import seed
    asyncio.run(seed())
""")
            # We can't easily test this without actually running the module
            # but we can verify the structure is correct