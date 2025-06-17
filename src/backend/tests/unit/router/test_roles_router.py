"""
Unit tests for RolesRouter.

Tests the functionality of role-based access control endpoints.
"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime
from fastapi import status
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession


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
    from src.api.roles_router import router, privilege_router
    from src.core.dependencies import get_db
    from src.dependencies.admin_auth import get_admin_user
    
    app = FastAPI()
    app.include_router(router)
    app.include_router(privilege_router)
    
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
    from src.dependencies.admin_auth import require_authenticated_user, get_authenticated_user, get_admin_user
    
    # Override authentication dependencies for testing
    app.dependency_overrides[require_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_admin_user] = lambda: mock_current_user

    return TestClient(app)


# Helper function to create role data with timestamps
def create_role_data(id, name, description, privileges=None):
    """Create role data with all required fields."""
    now = datetime.utcnow()
    return {
        "id": id,
        "name": name,
        "description": description,
        "created_at": now.isoformat(),
        "updated_at": now.isoformat(),
        "privileges": privileges or []
    }


# Helper function to create privilege data with timestamps
def create_privilege_data(id, name, description):
    """Create privilege data with all required fields."""
    now = datetime.utcnow()
    return {
        "id": id,
        "name": name,
        "description": description,
        "created_at": now.isoformat(),
        "updated_at": now.isoformat()
    }


class TestRolesRouter:
    """Test cases for roles management endpoints."""
    
    @patch('src.api.roles_router.RoleService')
    def test_get_roles_success(self, mock_service_class, client):
        """Test successful roles retrieval."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.get_roles.return_value = [
            create_role_data("1", "admin", "Administrator role"),
            create_role_data("2", "user", "Standard user role")
        ]
        
        response = client.get("/roles")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["name"] == "admin"
        mock_service.get_roles.assert_called_once_with(skip=0, limit=100)
    
    @patch('src.api.roles_router.RoleService')
    def test_get_roles_with_pagination(self, mock_service_class, client):
        """Test roles retrieval with pagination."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.get_roles.return_value = []
        
        response = client.get("/roles?skip=10&limit=20")
        
        assert response.status_code == 200
        mock_service.get_roles.assert_called_once_with(skip=10, limit=20)
    
    @patch('src.api.roles_router.RoleService')
    def test_create_role_success(self, mock_service_class, client):
        """Test successful role creation."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.create_role.return_value = create_role_data(
            "3", "moderator", "Moderator role"
        )
        
        role_data = {
            "name": "moderator",
            "description": "Moderator role",
            "privileges": []  # Empty list of privileges
        }
        
        response = client.post("/roles", json=role_data)
        
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "moderator"
    
    @patch('src.api.roles_router.RoleService')
    def test_create_role_duplicate(self, mock_service_class, client):
        """Test role creation with duplicate name."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.create_role.side_effect = ValueError("Role with this name already exists")
        
        role_data = {
            "name": "admin",
            "description": "Duplicate admin role",
            "privileges": []  # Empty list of privileges
        }
        
        response = client.post("/roles", json=role_data)
        
        assert response.status_code == 400
        assert "Role with this name already exists" in response.json()["detail"]
    
    @patch('src.api.roles_router.RoleService')
    def test_get_role_by_id_success(self, mock_service_class, client):
        """Test successful role retrieval by ID."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.get_role_with_privileges.return_value = create_role_data(
            "1", "admin", "Administrator role",
            privileges=[create_privilege_data("p1", "manage_users", "Can manage users")]
        )
        
        response = client.get("/roles/1")
        
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "admin"
        assert len(data["privileges"]) == 1
    
    @patch('src.api.roles_router.RoleService')
    def test_get_role_by_id_not_found(self, mock_service_class, client):
        """Test role retrieval for non-existent role."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.get_role_with_privileges.return_value = None
        
        response = client.get("/roles/999")
        
        assert response.status_code == 404
        assert "Role not found" in response.json()["detail"]
    
    @patch('src.api.roles_router.RoleService')
    def test_update_role_success(self, mock_service_class, client):
        """Test successful role update."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.update_role.return_value = create_role_data(
            "1", "admin", "Updated admin role"
        )
        
        update_data = {
            "description": "Updated admin role"
        }
        
        response = client.put("/roles/1", json=update_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["description"] == "Updated admin role"
    
    @patch('src.api.roles_router.RoleService')
    def test_update_role_not_found(self, mock_service_class, client):
        """Test updating non-existent role."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.update_role.return_value = None
        
        update_data = {
            "description": "Updated description"
        }
        
        response = client.put("/roles/999", json=update_data)
        
        assert response.status_code == 404
        assert "Role not found" in response.json()["detail"]
    
    @patch('src.api.roles_router.RoleService')
    def test_update_role_value_error(self, mock_service_class, client):
        """Test role update with invalid data."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.update_role.side_effect = ValueError("Invalid role data")
        
        update_data = {
            "name": ""  # Invalid empty name
        }
        
        response = client.put("/roles/1", json=update_data)
        
        assert response.status_code == 400
        assert "Invalid role data" in response.json()["detail"]
    
    @patch('src.api.roles_router.RoleService')
    def test_delete_role_success(self, mock_service_class, client):
        """Test successful role deletion."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.delete_role.return_value = True
        
        response = client.delete("/roles/custom-role")
        
        assert response.status_code == 204
    
    @patch('src.api.roles_router.RoleService')
    def test_delete_builtin_role(self, mock_service_class, client):
        """Test deletion of built-in role (should fail)."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        response = client.delete("/roles/admin")
        
        assert response.status_code == 400
        assert "Cannot delete built-in roles" in response.json()["detail"]
        
        # Test all built-in roles
        for role in ["admin", "technical", "regular"]:
            response = client.delete(f"/roles/{role}")
            assert response.status_code == 400
    
    @patch('src.api.roles_router.RoleService')
    def test_delete_role_not_found(self, mock_service_class, client):
        """Test deletion of non-existent role."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.delete_role.return_value = False
        
        response = client.delete("/roles/non-existent")
        
        assert response.status_code == 404
        assert "Role not found" in response.json()["detail"]
    
    @patch('src.api.roles_router.RoleService')
    def test_get_role_privileges(self, mock_service_class, client):
        """Test retrieving privileges for a specific role."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.get_role_privileges.return_value = [
            create_privilege_data("p1", "manage_users", "Can manage users"),
            create_privilege_data("p2", "manage_roles", "Can manage roles")
        ]
        
        response = client.get("/roles/admin/privileges")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["name"] == "manage_users"
    
    @patch('src.api.roles_router.RoleService')
    def test_assign_privilege_to_role_success(self, mock_service_class, client):
        """Test successful privilege assignment to role."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.assign_privilege_to_role.return_value = True
        
        privilege_data = {
            "privilege_id": "privilege-1"
        }
        
        response = client.post("/roles/role-1/privileges", json=privilege_data)
        
        assert response.status_code == 201
        data = response.json()
        assert "assigned successfully" in data["message"]
    
    @patch('src.api.roles_router.RoleService')
    def test_assign_privilege_missing_id(self, mock_service_class, client):
        """Test privilege assignment without privilege_id."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        privilege_data = {}  # Missing privilege_id
        
        response = client.post("/roles/role-1/privileges", json=privilege_data)
        
        assert response.status_code == 400
        assert "privilege_id is required" in response.json()["detail"]
    
    @patch('src.api.roles_router.RoleService')
    def test_assign_privilege_not_found(self, mock_service_class, client):
        """Test privilege assignment when role or privilege not found."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.assign_privilege_to_role.return_value = False
        
        privilege_data = {
            "privilege_id": "non-existent"
        }
        
        response = client.post("/roles/role-1/privileges", json=privilege_data)
        
        assert response.status_code == 404
        assert "Role or privilege not found" in response.json()["detail"]
    
    @patch('src.api.roles_router.RoleService')
    def test_remove_privilege_from_role_success(self, mock_service_class, client):
        """Test successful privilege removal from role."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.remove_privilege_from_role.return_value = True
        
        response = client.delete("/roles/role-1/privileges/privilege-1")
        
        assert response.status_code == 204
    
    @patch('src.api.roles_router.RoleService')
    def test_remove_privilege_from_role_not_found(self, mock_service_class, client):
        """Test privilege removal when role or privilege not found."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.remove_privilege_from_role.return_value = False
        
        response = client.delete("/roles/role-1/privileges/non-existent")
        
        assert response.status_code == 404
        assert "Role or privilege not found" in response.json()["detail"]


class TestPrivilegesRouter:
    """Test cases for privileges management endpoints."""
    
    @patch('src.api.roles_router.PrivilegeService')
    def test_get_privileges_success(self, mock_service_class, client):
        """Test successful privileges retrieval."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.get_privileges.return_value = [
            create_privilege_data("p1", "manage_users", "Can manage users"),
            create_privilege_data("p2", "manage_roles", "Can manage roles")
        ]
        
        response = client.get("/privileges")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["name"] == "manage_users"
        mock_service.get_privileges.assert_called_once_with(skip=0, limit=100)
    
    @patch('src.api.roles_router.PrivilegeService')
    def test_get_privileges_with_pagination(self, mock_service_class, client):
        """Test privileges retrieval with pagination."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.get_privileges.return_value = []
        
        response = client.get("/privileges?skip=5&limit=10")
        
        assert response.status_code == 200
        mock_service.get_privileges.assert_called_once_with(skip=5, limit=10)
    
    @patch('src.api.roles_router.PrivilegeService')
    def test_create_privilege_success(self, mock_service_class, client):
        """Test successful privilege creation."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.create_privilege.return_value = create_privilege_data(
            "p3", "view_reports", "Can view reports"
        )
        
        privilege_data = {
            "name": "view_reports",
            "description": "Can view reports"
        }
        
        response = client.post("/privileges", json=privilege_data)
        
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "view_reports"
    
    @patch('src.api.roles_router.PrivilegeService')
    def test_create_privilege_duplicate(self, mock_service_class, client):
        """Test privilege creation with duplicate name."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.create_privilege.side_effect = ValueError("Privilege with this name already exists")
        
        privilege_data = {
            "name": "manage_users",
            "description": "Duplicate privilege"
        }
        
        response = client.post("/privileges", json=privilege_data)
        
        assert response.status_code == 400
        assert "Privilege with this name already exists" in response.json()["detail"]
    
    @patch('src.api.roles_router.PrivilegeService')
    def test_get_privilege_by_id_success(self, mock_service_class, client):
        """Test successful privilege retrieval by ID."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.get_privilege.return_value = create_privilege_data(
            "p1", "manage_users", "Can manage users"
        )
        
        response = client.get("/privileges/p1")
        
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "manage_users"
    
    @patch('src.api.roles_router.PrivilegeService')
    def test_get_privilege_by_id_not_found(self, mock_service_class, client):
        """Test privilege retrieval for non-existent privilege."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.get_privilege.return_value = None
        
        response = client.get("/privileges/non-existent")
        
        assert response.status_code == 404
        assert "Privilege not found" in response.json()["detail"]
    
    @patch('src.api.roles_router.PrivilegeService')
    def test_update_privilege_success(self, mock_service_class, client):
        """Test successful privilege update."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.update_privilege.return_value = create_privilege_data(
            "p1", "manage_users", "Updated description"
        )
        
        update_data = {
            "description": "Updated description"
        }
        
        response = client.put("/privileges/p1", json=update_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["description"] == "Updated description"
    
    @patch('src.api.roles_router.PrivilegeService')
    def test_update_privilege_not_found(self, mock_service_class, client):
        """Test updating non-existent privilege."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.update_privilege.return_value = None
        
        update_data = {
            "description": "Updated description"
        }
        
        response = client.put("/privileges/non-existent", json=update_data)
        
        assert response.status_code == 404
        assert "Privilege not found" in response.json()["detail"]
    
    @patch('src.api.roles_router.PrivilegeService')
    def test_delete_privilege_success(self, mock_service_class, client):
        """Test successful privilege deletion."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.delete_privilege.return_value = True
        
        response = client.delete("/privileges/p1")
        
        assert response.status_code == 204
    
    @patch('src.api.roles_router.PrivilegeService')
    def test_delete_privilege_not_found(self, mock_service_class, client):
        """Test deletion of non-existent privilege."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.delete_privilege.return_value = False
        
        response = client.delete("/privileges/non-existent")
        
        assert response.status_code == 404
        assert "Privilege not found" in response.json()["detail"]