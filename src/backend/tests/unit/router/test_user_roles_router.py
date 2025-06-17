"""
Unit tests for UserRolesRouter.

Tests the functionality of user-role assignment management endpoints.
"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime

from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies.admin_auth import require_authenticated_user, get_authenticated_user, get_admin_user
import src.api.user_roles_router  # Import to ensure coverage is collected


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
    from src.api.user_roles_router import router
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


class TestUserRolesRouter:
    """Test cases for user roles management endpoints."""
    
    @patch('src.api.user_roles_router.UserRoleRepository')
    @patch('src.api.user_roles_router.UserRepository')
    @patch('src.api.user_roles_router.RoleRepository')
    def test_get_user_role_assignments_success(self, mock_role_repo_class, mock_user_repo_class, mock_user_role_repo_class, client, mock_admin_user):
        """Test successful user role assignments retrieval."""
        # Setup mocks
        mock_user_role_repo = AsyncMock()
        mock_user_repo = AsyncMock()
        mock_role_repo = AsyncMock()
        
        mock_user_role_repo_class.return_value = mock_user_role_repo
        mock_user_repo_class.return_value = mock_user_repo
        mock_role_repo_class.return_value = mock_role_repo
        
        # Mock user role assignment
        mock_user_role = MagicMock()
        mock_user_role.id = "assignment-1"
        mock_user_role.user_id = "user-1"
        mock_user_role.role_id = "role-1"
        mock_user_role.assigned_at = datetime.now()
        mock_user_role.assigned_by = "admin@example.com"
        
        mock_user_role_repo.list.return_value = [mock_user_role]
        
        # Mock user and role
        mock_user = MagicMock()
        mock_user.email = "user@example.com"
        mock_role = MagicMock()
        mock_role.name = "admin"
        
        mock_user_repo.get.return_value = mock_user
        mock_role_repo.get.return_value = mock_role
        
        response = client.get("/user-roles/")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["user_id"] == "user-1"
        assert data[0]["role_id"] == "role-1"
        assert data[0]["user_email"] == "user@example.com"
        assert data[0]["role_name"] == "admin"

    @patch('src.api.user_roles_router.UserRoleRepository')
    @patch('src.api.user_roles_router.UserRepository')
    @patch('src.api.user_roles_router.RoleRepository')
    def test_get_user_role_assignments_exception(self, mock_role_repo_class, mock_user_repo_class, mock_user_role_repo_class, client):
        """Test user role assignments retrieval with exception."""
        # Setup mocks to raise exception
        mock_user_role_repo = AsyncMock()
        mock_user_role_repo_class.return_value = mock_user_role_repo
        mock_user_role_repo.list.side_effect = Exception("Database error")
        
        response = client.get("/user-roles/")
        
        assert response.status_code == 500
        assert "Failed to get user-role assignments" in response.json()["detail"]
    
    @patch('src.api.user_roles_router.UserRoleRepository')
    @patch('src.api.user_roles_router.UserRepository')
    @patch('src.api.user_roles_router.RoleRepository')
    def test_assign_role_to_user_success(self, mock_role_repo_class, mock_user_repo_class, mock_user_role_repo_class, client, mock_admin_user, mock_db_session):
        """Test successful role assignment to user."""
        # Setup mocks
        mock_user_role_repo = AsyncMock()
        mock_user_repo = AsyncMock()
        mock_role_repo = AsyncMock()
        
        mock_user_role_repo_class.return_value = mock_user_role_repo
        mock_user_repo_class.return_value = mock_user_repo
        mock_role_repo_class.return_value = mock_role_repo
        
        # Mock user and role existence
        mock_user = MagicMock()
        mock_user.email = "user@example.com"
        mock_role = MagicMock()
        mock_role.name = "admin"
        
        mock_user_repo.get.return_value = mock_user
        mock_role_repo.get.return_value = mock_role
        
        # Mock assignment creation
        mock_user_role = MagicMock()
        mock_user_role.id = "assignment-1"
        mock_user_role.user_id = "user-1"
        mock_user_role.role_id = "role-1"
        mock_user_role.assigned_at = datetime.now()
        mock_user_role.assigned_by = "admin@example.com"
        
        mock_user_role_repo.assign_role.return_value = mock_user_role
        
        assignment_data = {
            "user_id": "user-1",
            "role_id": "role-1"
        }
        
        response = client.post("/user-roles/", json=assignment_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["user_id"] == "user-1"
        assert data["role_id"] == "role-1"
        assert data["user_email"] == "user@example.com"
        assert data["role_name"] == "admin"
        mock_db_session.commit.assert_called_once()

    @patch('src.api.user_roles_router.UserRoleRepository')
    @patch('src.api.user_roles_router.UserRepository')
    @patch('src.api.user_roles_router.RoleRepository')
    def test_assign_role_to_user_user_not_found(self, mock_role_repo_class, mock_user_repo_class, mock_user_role_repo_class, client):
        """Test role assignment when user not found."""
        # Setup mocks
        mock_user_repo = AsyncMock()
        mock_user_repo_class.return_value = mock_user_repo
        mock_user_repo.get.return_value = None  # User not found
        
        assignment_data = {
            "user_id": "nonexistent-user",
            "role_id": "role-1"
        }
        
        response = client.post("/user-roles/", json=assignment_data)
        
        assert response.status_code == 404
        assert "User nonexistent-user not found" in response.json()["detail"]

    @patch('src.api.user_roles_router.UserRoleRepository')
    @patch('src.api.user_roles_router.UserRepository')
    @patch('src.api.user_roles_router.RoleRepository')
    def test_assign_role_to_user_role_not_found(self, mock_role_repo_class, mock_user_repo_class, mock_user_role_repo_class, client):
        """Test role assignment when role not found."""
        # Setup mocks
        mock_user_repo = AsyncMock()
        mock_role_repo = AsyncMock()
        mock_user_repo_class.return_value = mock_user_repo
        mock_role_repo_class.return_value = mock_role_repo
        
        # Mock user exists but role doesn't
        mock_user = MagicMock()
        mock_user.email = "user@example.com"
        mock_user_repo.get.return_value = mock_user
        mock_role_repo.get.return_value = None  # Role not found
        
        assignment_data = {
            "user_id": "user-1",
            "role_id": "nonexistent-role"
        }
        
        response = client.post("/user-roles/", json=assignment_data)
        
        assert response.status_code == 404
        assert "Role nonexistent-role not found" in response.json()["detail"]

    @patch('src.api.user_roles_router.UserRoleRepository')
    @patch('src.api.user_roles_router.UserRepository')
    @patch('src.api.user_roles_router.RoleRepository')
    def test_assign_role_to_user_exception(self, mock_role_repo_class, mock_user_repo_class, mock_user_role_repo_class, client):
        """Test role assignment with exception."""
        # Setup mocks
        mock_user_repo = AsyncMock()
        mock_user_repo_class.return_value = mock_user_repo
        mock_user_repo.get.side_effect = Exception("Database error")
        
        assignment_data = {
            "user_id": "user-1",
            "role_id": "role-1"
        }
        
        response = client.post("/user-roles/", json=assignment_data)
        
        assert response.status_code == 500
        assert "Failed to assign role to user" in response.json()["detail"]
    
    @patch('src.api.user_roles_router.UserRoleRepository')
    def test_remove_role_from_user_success(self, mock_user_role_repo_class, client, mock_db_session):
        """Test successful role removal from user."""
        mock_user_role_repo = AsyncMock()
        mock_user_role_repo_class.return_value = mock_user_role_repo
        
        response = client.delete("/user-roles/user-1/roles/role-1")
        
        assert response.status_code == 200
        data = response.json()
        assert "removed" in data["message"].lower()
        mock_user_role_repo.remove_role.assert_called_once_with("user-1", "role-1")
        mock_db_session.commit.assert_called_once()

    @patch('src.api.user_roles_router.UserRoleRepository')
    def test_remove_role_from_user_exception(self, mock_user_role_repo_class, client):
        """Test role removal with exception."""
        mock_user_role_repo = AsyncMock()
        mock_user_role_repo_class.return_value = mock_user_role_repo
        mock_user_role_repo.remove_role.side_effect = Exception("Database error")
        
        response = client.delete("/user-roles/user-1/roles/role-1")
        
        assert response.status_code == 500
        assert "Failed to remove role from user" in response.json()["detail"]
    
    @patch('src.api.user_roles_router.DatabricksRoleService')
    def test_get_user_roles_success(self, mock_databricks_service_class, client):
        """Test successful user roles retrieval."""
        mock_service = AsyncMock()
        mock_databricks_service_class.return_value = mock_service
        
        # Mock roles
        mock_role1 = MagicMock()
        mock_role1.name = "admin"
        mock_role2 = MagicMock()
        mock_role2.name = "user"
        
        mock_service.get_user_roles.return_value = [mock_role1, mock_role2]
        
        response = client.get("/user-roles/users/user-1/roles")
        
        assert response.status_code == 200
        data = response.json()
        assert data == ["admin", "user"]

    @patch('src.api.user_roles_router.DatabricksRoleService')
    def test_get_user_roles_exception(self, mock_databricks_service_class, client):
        """Test user roles retrieval with exception."""
        mock_service = AsyncMock()
        mock_databricks_service_class.return_value = mock_service
        mock_service.get_user_roles.side_effect = Exception("Service error")
        
        response = client.get("/user-roles/users/user-1/roles")
        
        assert response.status_code == 500
        assert "Failed to get user roles" in response.json()["detail"]
    
    @patch('src.api.user_roles_router.DatabricksRoleService')
    def test_get_user_privileges_success(self, mock_databricks_service_class, client):
        """Test successful user privileges retrieval."""
        mock_service = AsyncMock()
        mock_databricks_service_class.return_value = mock_service
        
        mock_service.get_user_privileges.return_value = ["read", "write", "delete"]
        
        response = client.get("/user-roles/users/user-1/privileges")
        
        assert response.status_code == 200
        data = response.json()
        assert data == ["read", "write", "delete"]

    @patch('src.api.user_roles_router.DatabricksRoleService')
    def test_get_user_privileges_exception(self, mock_databricks_service_class, client):
        """Test user privileges retrieval with exception."""
        mock_service = AsyncMock()
        mock_databricks_service_class.return_value = mock_service
        mock_service.get_user_privileges.side_effect = Exception("Service error")
        
        response = client.get("/user-roles/users/user-1/privileges")
        
        assert response.status_code == 500
        assert "Failed to get user privileges" in response.json()["detail"]

    @patch('src.api.user_roles_router.UserRepository')
    @patch('src.api.user_roles_router.DatabricksRoleService')
    def test_get_users_with_role_info_success(self, mock_databricks_service_class, mock_user_repo_class, client):
        """Test successful users with role info retrieval."""
        # Setup mocks
        mock_user_repo = AsyncMock()
        mock_service = AsyncMock()
        
        mock_user_repo_class.return_value = mock_user_repo
        mock_databricks_service_class.return_value = mock_service
        
        # Mock users
        mock_user1 = MagicMock()
        mock_user1.id = "user-1"
        mock_user1.username = "user1"
        mock_user1.email = "user1@example.com"
        mock_user1.status = MagicMock()
        mock_user1.status.value = "ACTIVE"
        mock_user1.created_at = datetime.now()
        
        mock_user2 = MagicMock()
        mock_user2.id = "user-2"
        mock_user2.username = "user2"
        mock_user2.email = "user2@example.com"
        mock_user2.status = "INACTIVE"  # Test string status
        mock_user2.created_at = datetime.now()
        
        mock_user_repo.list.return_value = [mock_user1, mock_user2]
        
        # Mock roles and privileges for each user
        mock_role1 = MagicMock()
        mock_role1.name = "admin"
        mock_role2 = MagicMock()
        mock_role2.name = "user"
        
        def mock_get_user_roles(user_id):
            if user_id == "user-1":
                return [mock_role1]
            return [mock_role2]
        
        def mock_get_user_privileges(user_id):
            if user_id == "user-1":
                return ["read", "write", "delete"]
            return ["read"]
        
        mock_service.get_user_roles.side_effect = mock_get_user_roles
        mock_service.get_user_privileges.side_effect = mock_get_user_privileges
        
        response = client.get("/user-roles/users")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        
        # Check first user
        assert data[0]["id"] == "user-1"
        assert data[0]["username"] == "user1"
        assert data[0]["email"] == "user1@example.com"
        assert data[0]["status"] == "ACTIVE"
        assert data[0]["roles"] == ["admin"]
        assert data[0]["privileges"] == ["read", "write", "delete"]
        
        # Check second user
        assert data[1]["id"] == "user-2"
        assert data[1]["username"] == "user2"
        assert data[1]["email"] == "user2@example.com"
        assert data[1]["status"] == "INACTIVE"
        assert data[1]["roles"] == ["user"]
        assert data[1]["privileges"] == ["read"]

    @patch('src.api.user_roles_router.UserRepository')
    @patch('src.api.user_roles_router.DatabricksRoleService')
    def test_get_users_with_role_info_exception(self, mock_databricks_service_class, mock_user_repo_class, client):
        """Test users with role info retrieval with exception."""
        mock_user_repo = AsyncMock()
        mock_user_repo_class.return_value = mock_user_repo
        mock_user_repo.list.side_effect = Exception("Database error")
        
        response = client.get("/user-roles/users")
        
        assert response.status_code == 500
        assert "Failed to get users with role info" in response.json()["detail"]

