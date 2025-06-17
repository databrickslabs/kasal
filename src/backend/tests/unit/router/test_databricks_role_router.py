"""
Unit tests for DatabricksRoleRouter.

Tests the functionality of Databricks role management and admin synchronization endpoints.
"""
import pytest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies.admin_auth import require_authenticated_user, get_authenticated_user, get_admin_user


# Mock admin user
class MockAdminUser:
    def __init__(self, email="admin@example.com", id="admin-123"):
        self.email = email
        self.id = id
        self.is_admin = True


# Mock user model
class MockUser:
    def __init__(self, id="user-123", email="user@example.com"):
        self.id = id
        self.email = email


@pytest.fixture
def mock_admin_user():
    """Create a mock admin user."""
    return MockAdminUser()


@pytest.fixture
def mock_db_session():
    """Create a mock database session."""
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def app(mock_admin_user, mock_db_session):
    """Create a FastAPI app with mocked dependencies."""
    from fastapi import FastAPI
    from src.api.databricks_role_router import router
    from src.core.dependencies import get_db
    from src.dependencies.admin_auth import get_admin_user
    
    app = FastAPI()
    app.include_router(router)
    
    # Create override functions
    async def override_get_db():
        return mock_db_session
        
    async def override_get_admin_user():
        return mock_admin_user
    
    # Override dependencies
    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_admin_user] = override_get_admin_user
    
    return app



@pytest.fixture
def mock_current_user():
    """Create a mock authenticated user."""
    from src.models.enums import UserRole, UserStatus
    from datetime import datetime
    
    class MockUser:
        def __init__(self):
            self.id = "current-user-123"
            self.username = "testuser"
            self.email = "test@example.com"
            self.role = UserRole.REGULAR
            self.status = UserStatus.ACTIVE
            self.created_at = datetime.utcnow()
            self.updated_at = datetime.utcnow()
    
    return MockUser()


@pytest.fixture
def client(app, mock_current_user):
    """Create a test client."""
    # Override authentication dependencies for testing
    app.dependency_overrides[require_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_admin_user] = lambda: mock_current_user

    return TestClient(app)


class TestSyncDatabricksAdminRoles:
    """Test cases for sync Databricks admin roles endpoint."""
    
    @patch('src.api.databricks_role_router.DatabricksRoleService')
    def test_sync_admin_roles_success(self, mock_service_class, client, mock_admin_user, mock_db_session):
        """Test successful admin role synchronization."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        sync_results = {
            "success": True,
            "processed_users": 5,
            "new_admins": 2,
            "updated_admins": 1,
            "errors": []
        }
        mock_service.sync_admin_roles.return_value = sync_results
        
        response = client.post("/admin/databricks-roles/sync")
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["message"] == "Admin role synchronization completed"
        assert data["triggered_by"] == "test@example.com"
        assert data["results"]["processed_users"] == 5
        mock_service.sync_admin_roles.assert_called_once()
    
    @patch('src.api.databricks_role_router.DatabricksRoleService')
    def test_sync_admin_roles_partial_success(self, mock_service_class, client, mock_admin_user, mock_db_session):
        """Test admin role synchronization with partial success."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        sync_results = {
            "success": False,
            "processed_users": 3,
            "new_admins": 1,
            "updated_admins": 0,
            "errors": ["Failed to process user@example.com"]
        }
        mock_service.sync_admin_roles.return_value = sync_results
        
        response = client.post("/admin/databricks-roles/sync")
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is False
        assert len(data["results"]["errors"]) == 1
    
    @patch('src.api.databricks_role_router.DatabricksRoleService')
    def test_sync_admin_roles_service_error(self, mock_service_class, client, mock_admin_user, mock_db_session):
        """Test admin role synchronization with service error."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.sync_admin_roles.side_effect = Exception("Databricks API error")
        
        response = client.post("/admin/databricks-roles/sync")
        
        assert response.status_code == 500
        assert "Failed to sync admin roles" in response.json()["detail"]


class TestGetDatabricksAdminEmails:
    """Test cases for get Databricks admin emails endpoint."""
    
    @patch('src.api.databricks_role_router.DatabricksRoleService')
    def test_get_admin_emails_success(self, mock_service_class, client, mock_admin_user, mock_db_session):
        """Test successful retrieval of admin emails."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        admin_emails = ["admin1@example.com", "admin2@example.com"]
        mock_service.get_databricks_app_managers.return_value = admin_emails
        mock_service.is_local_dev = False
        mock_service.app_name = "kasal-app"
        mock_service.databricks_host = "https://test.databricks.com"
        mock_service.databricks_token = "token-123"
        
        response = client.get("/admin/databricks-roles/admin-emails")
        
        assert response.status_code == 200
        data = response.json()
        assert data["admin_emails"] == admin_emails
        assert data["source"] == "databricks"
        assert data["is_local_dev"] is False
        assert data["databricks_config"]["app_name"] == "kasal-app"
        assert data["databricks_config"]["host_configured"] is True
        assert data["databricks_config"]["token_configured"] is True
    
    @patch('src.api.databricks_role_router.DatabricksRoleService')
    def test_get_admin_emails_local_dev(self, mock_service_class, client, mock_admin_user, mock_db_session):
        """Test admin emails retrieval in local development mode."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        admin_emails = ["localdev@example.com"]
        mock_service.get_databricks_app_managers.return_value = admin_emails
        mock_service.is_local_dev = True
        mock_service.app_name = "kasal-dev"
        mock_service.databricks_host = None
        mock_service.databricks_token = None
        
        response = client.get("/admin/databricks-roles/admin-emails")
        
        assert response.status_code == 200
        data = response.json()
        assert data["source"] == "fallback"
        assert data["is_local_dev"] is True
        assert data["databricks_config"]["host_configured"] is False
        assert data["databricks_config"]["token_configured"] is False
    
    @patch('src.api.databricks_role_router.DatabricksRoleService')
    def test_get_admin_emails_service_error(self, mock_service_class, client, mock_admin_user, mock_db_session):
        """Test admin emails retrieval with service error."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.get_databricks_app_managers.side_effect = Exception("API connection failed")
        
        response = client.get("/admin/databricks-roles/admin-emails")
        
        assert response.status_code == 500
        assert "Failed to get admin emails" in response.json()["detail"]


class TestCheckUserAdminStatus:
    """Test cases for check user admin status endpoint."""
    
    @patch('src.api.databricks_role_router.DatabricksRoleService')
    @patch('src.repositories.user_repository.UserRepository')
    def test_check_user_admin_status_existing_admin(self, mock_repo_class, mock_service_class,
                                                   client, mock_admin_user, mock_db_session):
        """Test checking admin status for existing admin user."""
        # Mock user repository
        mock_repo = AsyncMock()
        mock_repo_class.return_value = mock_repo
        user = MockUser(email="admin@example.com")
        mock_repo.get_by_email.return_value = user
        
        # Mock databricks role service
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.check_user_admin_access.return_value = True
        mock_service.get_user_roles.return_value = ["admin"]
        
        response = client.get("/admin/databricks-roles/check-admin/admin@example.com")
        
        assert response.status_code == 200
        data = response.json()
        assert data["email"] == "admin@example.com"
        assert data["user_exists"] is True
        assert data["has_admin_access"] is True
        assert "admin_tenants" in data
        assert "admin_roles" in data
        mock_repo.get_by_email.assert_called_once_with("admin@example.com")
    
    @patch('src.api.databricks_role_router.DatabricksRoleService')
    @patch('src.repositories.user_repository.UserRepository')
    def test_check_user_admin_status_existing_non_admin(self, mock_repo_class, mock_service_class,
                                                       client, mock_admin_user, mock_db_session):
        """Test checking admin status for existing non-admin user."""
        # Mock user repository
        mock_repo = AsyncMock()
        mock_repo_class.return_value = mock_repo
        user = MockUser(email="user@example.com")
        mock_repo.get_by_email.return_value = user
        
        # Mock databricks role service
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.check_user_admin_access.return_value = False
        mock_service.get_user_roles.return_value = []
        
        response = client.get("/admin/databricks-roles/check-admin/user@example.com")
        
        assert response.status_code == 200
        data = response.json()
        assert data["email"] == "user@example.com"
        assert data["user_exists"] is True
        assert data["has_admin_access"] is False
        assert len(data["admin_tenants"]) == 0
    
    @patch('src.repositories.user_repository.UserRepository')
    def test_check_user_admin_status_non_existing_user(self, mock_repo_class, client, mock_admin_user, mock_db_session):
        """Test checking admin status for non-existing user."""
        # Mock user repository
        mock_repo = AsyncMock()
        mock_repo_class.return_value = mock_repo
        mock_repo.get_by_email.return_value = None
        
        response = client.get("/admin/databricks-roles/check-admin/nonexistent@example.com")
        
        assert response.status_code == 200
        data = response.json()
        assert data["email"] == "nonexistent@example.com"
        assert data["user_exists"] is False
        assert data["has_admin_access"] is False
        assert len(data["admin_tenants"]) == 0
    
    @patch('src.repositories.user_repository.UserRepository')
    def test_check_user_admin_status_service_error(self, mock_repo_class, client, mock_admin_user, mock_db_session):
        """Test checking admin status with service error."""
        # Mock user repository
        mock_repo = AsyncMock()
        mock_repo_class.return_value = mock_repo
        mock_repo.get_by_email.side_effect = Exception("Database error")
        
        response = client.get("/admin/databricks-roles/check-admin/user@example.com")
        
        assert response.status_code == 500
        assert "Failed to check admin status" in response.json()["detail"]