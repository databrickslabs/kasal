"""
Unit tests for UsersRouter.

Tests the functionality of user management endpoints including
current user operations, admin operations, and external identities.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.user import User
from src.models.enums import UserRole, UserStatus
from src.schemas.user import (
    UserUpdate, UserProfileUpdate, UserRoleAssign
)


# Mock user model
class MockUser:
    def __init__(self, id="user-123", username="testuser", email="test@example.com",
                 role=UserRole.REGULAR, status=UserStatus.ACTIVE):
        self.id = id
        self.username = username
        self.email = email
        self.role = role
        self.status = status
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()


# Mock external identity
class MockExternalIdentity:
    def __init__(self, id="ext-123", user_id="user-123", provider="github"):
        from datetime import datetime
        self.id = id
        self.user_id = user_id
        self.provider = provider
        self.provider_user_id = "github-123"
        self.email = "test@github.com"
        self.profile_data = None
        self.created_at = datetime.now()
        self.last_login = None


@pytest.fixture
def mock_session():
    """Create a mock async session."""
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def mock_current_user():
    """Create a mock authenticated user."""
    return MockUser(id="current-user-123", username="currentuser")


@pytest.fixture
def mock_admin_user():
    """Create a mock admin user."""
    return MockUser(id="admin-123", username="admin", role=UserRole.ADMIN)


@pytest.fixture
def mock_user_service():
    """Create a mock user service."""
    service = AsyncMock()
    return service


@pytest.fixture
def client(mock_current_user, mock_session):
    """Create a test client with dependency overrides."""
    from fastapi import FastAPI
    from src.api.users_router import router
    from src.db.session import get_db
    from src.dependencies.admin_auth import (
        require_authenticated_user, get_authenticated_user, get_admin_user
    )
    
    app = FastAPI()
    app.include_router(router)
    
    # Override the actual dependency functions for testing
    app.dependency_overrides[require_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_admin_user] = lambda: mock_current_user
    app.dependency_overrides[get_db] = lambda: mock_session
    
    return TestClient(app)


class TestCurrentUserEndpoints:
    """Test cases for current user endpoints."""
    
    def test_read_users_me(self, client, mock_current_user, mock_user_service):
        """Test getting current user information."""
        from datetime import datetime
        from src.models.enums import UserRole, UserStatus
        
        user_with_profile = {
            "id": "current-user-123",
            "username": "currentuser",
            "email": "current@example.com",
            "role": UserRole.REGULAR,
            "status": UserStatus.ACTIVE,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "profile": {
                "id": "profile-123",
                "user_id": "current-user-123",
                "full_name": "Current User",
                "bio": "Test bio"
            }
        }
        
        mock_user_service.get_user_with_profile.return_value = user_with_profile
        
        with patch('src.api.users_router.UserService', return_value=mock_user_service):
            response = client.get("/users/me")
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "current-user-123"
        assert data["username"] == "currentuser"
    
    def test_update_users_me(self, client, mock_current_user, mock_user_service):
        """Test updating current user information."""
        from datetime import datetime
        from src.models.enums import UserRole, UserStatus
        
        update_data = {"email": "newemail@example.com"}
        updated_user = {
            "id": "current-user-123",
            "username": "currentuser",
            "email": "newemail@example.com",
            "role": UserRole.REGULAR.value,
            "status": UserStatus.ACTIVE.value,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "last_login": None,
            "profile": {
                "id": "profile-123",
                "user_id": "current-user-123"
            }
        }
        
        mock_user_service.update_user.return_value = updated_user
        
        with patch('src.api.users_router.UserService', return_value=mock_user_service):
            with patch('src.api.users_router.SessionDep', return_value=AsyncMock()):
                with patch('src.api.users_router.AuthenticatedUserDep', return_value=mock_current_user):
                    response = client.put("/users/me", json=update_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["email"] == "newemail@example.com"
    
    def test_update_users_profile(self, client, mock_current_user, mock_user_service):
        """Test updating current user profile."""
        from datetime import datetime
        from src.models.enums import UserRole, UserStatus
        
        profile_data = {"display_name": "Updated Name", "avatar_url": "http://example.com/avatar.jpg"}
        updated_user = {
            "id": "current-user-123",
            "username": "currentuser",
            "email": "test@example.com",
            "role": UserRole.REGULAR.value,
            "status": UserStatus.ACTIVE.value,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "last_login": None,
            "profile": {
                "id": "profile-123",
                "user_id": "current-user-123",
                "display_name": "Updated Name",
                "avatar_url": "http://example.com/avatar.jpg",
                "preferences": None
            }
        }
        
        mock_user_service.update_user_profile.return_value = updated_user
        
        with patch('src.api.users_router.UserService', return_value=mock_user_service):
            with patch('src.api.users_router.SessionDep', return_value=AsyncMock()):
                with patch('src.api.users_router.AuthenticatedUserDep', return_value=mock_current_user):
                    response = client.put("/users/me/profile", json=profile_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["profile"]["display_name"] == "Updated Name"
    
    def test_read_users_external_identities(self, client, mock_current_user, mock_user_service):
        """Test getting current user's external identities."""
        mock_identities = [MockExternalIdentity()]
        mock_user_service.get_user_external_identities.return_value = mock_identities
        
        with patch('src.api.users_router.UserService', return_value=mock_user_service):
            with patch('src.api.users_router.SessionDep', return_value=AsyncMock()):
                with patch('src.api.users_router.AuthenticatedUserDep', return_value=mock_current_user):
                    response = client.get("/users/me/external-identities")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["provider"] == "github"
    
    def test_delete_external_identity_success(self, client, mock_current_user, mock_user_service):
        """Test successfully deleting an external identity."""
        mock_user_service.remove_external_identity.return_value = True
        
        with patch('src.api.users_router.UserService', return_value=mock_user_service):
            with patch('src.api.users_router.SessionDep', return_value=AsyncMock()):
                with patch('src.api.users_router.AuthenticatedUserDep', return_value=mock_current_user):
                    response = client.delete("/users/me/external-identities/github")
        
        assert response.status_code == 204
    
    def test_delete_external_identity_not_found(self, client, mock_current_user, mock_user_service):
        """Test deleting non-existent external identity."""
        mock_user_service.remove_external_identity.return_value = False
        
        with patch('src.api.users_router.UserService', return_value=mock_user_service):
            with patch('src.api.users_router.SessionDep', return_value=AsyncMock()):
                with patch('src.api.users_router.AuthenticatedUserDep', return_value=mock_current_user):
                    response = client.delete("/users/me/external-identities/nonexistent")
        
        assert response.status_code == 404
        assert "No external identity found" in response.json()["detail"]


class TestAdminEndpoints:
    """Test cases for admin endpoints."""
    
    def test_read_users_no_filters(self, client, mock_admin_user, mock_user_service):
        """Test getting users list without filters."""
        mock_users = [MockUser(id="user-1"), MockUser(id="user-2")]
        mock_user_service.get_users.return_value = mock_users
        
        with patch('src.api.users_router.UserService', return_value=mock_user_service):
            with patch('src.api.users_router.SessionDep', return_value=AsyncMock()):
                with patch('src.api.users_router.AdminUserDep', return_value=mock_admin_user):
                    response = client.get("/users/")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        
        # Verify get_users was called with correct params
        mock_user_service.get_users.assert_called_once_with(
            skip=0, limit=100, filters={}, search=None
        )
    
    def test_read_users_with_filters(self, client, mock_admin_user, mock_user_service):
        """Test getting users list with filters."""
        mock_users = [MockUser(role=UserRole.ADMIN)]
        mock_user_service.get_users.return_value = mock_users
        
        with patch('src.api.users_router.UserService', return_value=mock_user_service):
            with patch('src.api.users_router.SessionDep', return_value=AsyncMock()):
                with patch('src.api.users_router.AdminUserDep', return_value=mock_admin_user):
                    response = client.get("/users/?role=admin&status=active&search=test")
        
        assert response.status_code == 200
        
        # Verify filters were passed correctly
        mock_user_service.get_users.assert_called_once_with(
            skip=0, limit=100, filters={"role": "admin", "status": "active"}, search="test"
        )
    
    def test_read_user_success(self, client, mock_admin_user, mock_user_service):
        """Test getting a specific user by ID."""
        from datetime import datetime
        from src.models.enums import UserRole, UserStatus
        
        user_complete = {
            "id": "user-123",
            "username": "testuser",
            "email": "test@example.com",
            "role": UserRole.REGULAR.value,
            "status": UserStatus.ACTIVE.value,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "last_login": None,
            "profile": {
                "id": "profile-123",
                "user_id": "user-123"
            },
            "external_identities": []
        }
        
        mock_user_service.get_user_complete.return_value = user_complete
        
        with patch('src.api.users_router.UserService', return_value=mock_user_service):
            with patch('src.api.users_router.SessionDep', return_value=AsyncMock()):
                with patch('src.api.users_router.AdminUserDep', return_value=mock_admin_user):
                    response = client.get("/users/user-123")
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "user-123"
    
    def test_read_user_not_found(self, client, mock_admin_user, mock_user_service):
        """Test getting non-existent user."""
        mock_user_service.get_user_complete.return_value = None
        
        with patch('src.api.users_router.UserService', return_value=mock_user_service):
            with patch('src.api.users_router.SessionDep', return_value=AsyncMock()):
                with patch('src.api.users_router.AdminUserDep', return_value=mock_admin_user):
                    response = client.get("/users/nonexistent")
        
        assert response.status_code == 404
        assert "User not found" in response.json()["detail"]
    
    def test_update_user_success(self, client, mock_admin_user, mock_user_service):
        """Test updating a user."""
        update_data = {"email": "updated@example.com"}
        updated_user = MockUser(email="updated@example.com")
        
        mock_user_service.update_user.return_value = updated_user
        
        with patch('src.api.users_router.UserService', return_value=mock_user_service):
            with patch('src.api.users_router.SessionDep', return_value=AsyncMock()):
                with patch('src.api.users_router.AdminUserDep', return_value=mock_admin_user):
                    response = client.put("/users/user-123", json=update_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["email"] == "updated@example.com"
    
    def test_update_user_not_found(self, client, mock_admin_user, mock_user_service):
        """Test updating non-existent user."""
        update_data = {"email": "updated@example.com"}
        mock_user_service.update_user.return_value = None
        
        with patch('src.api.users_router.UserService', return_value=mock_user_service):
            with patch('src.api.users_router.SessionDep', return_value=AsyncMock()):
                with patch('src.api.users_router.AdminUserDep', return_value=mock_admin_user):
                    response = client.put("/users/nonexistent", json=update_data)
        
        assert response.status_code == 404
        assert "User not found" in response.json()["detail"]
    
    def test_assign_user_role_success(self, client, mock_admin_user, mock_user_service):
        """Test assigning a role to a user."""
        role_data = {"role_id": "admin"}
        updated_user = MockUser(role=UserRole.ADMIN)
        
        mock_user_service.assign_role.return_value = updated_user
        
        with patch('src.api.users_router.UserService', return_value=mock_user_service):
            with patch('src.api.users_router.SessionDep', return_value=AsyncMock()):
                with patch('src.api.users_router.AdminUserDep', return_value=mock_admin_user):
                    response = client.put("/users/user-123/role", json=role_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["role"] == "admin"
    
    def test_assign_user_role_not_found(self, client, mock_admin_user, mock_user_service):
        """Test assigning role to non-existent user."""
        role_data = {"role_id": "admin"}
        mock_user_service.assign_role.return_value = None
        
        with patch('src.api.users_router.UserService', return_value=mock_user_service):
            with patch('src.api.users_router.SessionDep', return_value=AsyncMock()):
                with patch('src.api.users_router.AdminUserDep', return_value=mock_admin_user):
                    response = client.put("/users/nonexistent/role", json=role_data)
        
        assert response.status_code == 404
        assert "User not found" in response.json()["detail"]
    
    def test_delete_user_success(self, client, mock_admin_user, mock_user_service):
        """Test deleting a user."""
        mock_user_service.delete_user.return_value = True
        
        with patch('src.api.users_router.UserService', return_value=mock_user_service):
            with patch('src.api.users_router.SessionDep', return_value=AsyncMock()):
                with patch('src.api.users_router.AdminUserDep', return_value=mock_admin_user):
                    response = client.delete("/users/user-123")
        
        assert response.status_code == 204
    
    def test_delete_user_not_found(self, client, mock_admin_user, mock_user_service):
        """Test deleting non-existent user."""
        mock_user_service.delete_user.return_value = False
        
        with patch('src.api.users_router.UserService', return_value=mock_user_service):
            with patch('src.api.users_router.SessionDep', return_value=AsyncMock()):
                with patch('src.api.users_router.AdminUserDep', return_value=mock_admin_user):
                    response = client.delete("/users/nonexistent")
        
        assert response.status_code == 404
        assert "User not found" in response.json()["detail"]
    
    def test_read_users_with_pagination(self, client, mock_admin_user, mock_user_service):
        """Test getting users with pagination parameters."""
        mock_users = [MockUser(id=f"user-{i}") for i in range(5)]
        mock_user_service.get_users.return_value = mock_users
        
        with patch('src.api.users_router.UserService', return_value=mock_user_service):
            with patch('src.api.users_router.SessionDep', return_value=AsyncMock()):
                with patch('src.api.users_router.AdminUserDep', return_value=mock_admin_user):
                    response = client.get("/users/?skip=10&limit=5")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 5
        
        # Verify pagination params were passed
        mock_user_service.get_users.assert_called_once_with(
            skip=10, limit=5, filters={}, search=None
        )