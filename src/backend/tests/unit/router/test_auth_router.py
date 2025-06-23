"""
Unit tests for AuthRouter.

Tests the functionality of authentication endpoints.
"""
import pytest
from unittest.mock import AsyncMock, patch
from datetime import datetime
from src.dependencies.admin_auth import (
    require_authenticated_user, get_authenticated_user, get_admin_user
)

from fastapi.testclient import TestClient
from src.models.enums import UserRole, UserStatus


# Mock user model
class MockUser:
    def __init__(self, id="user-123", username="testuser", email="test@example.com",
                 role=UserRole.REGULAR, status=UserStatus.ACTIVE,
                 hashed_password="$2b$12$hashed"):
        self.id = id
        self.username = username
        self.email = email
        self.role = role
        self.status = status
        self.hashed_password = hashed_password
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        self.last_login = None


@pytest.fixture
def mock_user():
    """Create a mock user."""
    return MockUser()


@pytest.fixture
def app(mock_user):
    """Create a FastAPI app with dependency overrides."""
    from fastapi import FastAPI
    from src.api.auth_router import router
    from src.db.session import get_db
    from src.dependencies.auth import get_current_user, get_current_active_user
    
    app = FastAPI()
    app.include_router(router)
    
    # Override dependencies
    app.dependency_overrides[get_db] = lambda: AsyncMock()
    app.dependency_overrides[get_current_user] = lambda: mock_user
    app.dependency_overrides[get_current_active_user] = lambda: mock_user
    
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
def client(app):
    """Create a test client."""
    # Override authentication dependencies for testing
    app.dependency_overrides[require_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_admin_user] = lambda: mock_current_user


    return TestClient(app)


class TestAuthRouter:
    """Test cases for authentication router."""
    
    @patch('src.api.auth_router.AuthService')
    @patch('src.api.auth_router.settings')
    def test_login_success(self, mock_settings, mock_auth_service_class, client, mock_user):
        """Test successful login."""
        # Mock settings
        mock_settings.REFRESH_TOKEN_EXPIRE_DAYS = 7
        mock_settings.COOKIE_SECURE = False
        
        # Mock the service instance
        mock_service = AsyncMock()
        mock_auth_service_class.return_value = mock_service
        mock_service.authenticate_user.return_value = mock_user
        mock_service.create_user_tokens.return_value = {
            "access_token": "test_access_token",
            "refresh_token": "test_refresh_token",
            "token_type": "bearer"
        }
        
        form_data = {
            "username": "testuser",
            "password": "password123"
        }
        
        response = client.post("/auth/login", data=form_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["access_token"] == "test_access_token"
        assert data["token_type"] == "bearer"
    
    @patch('src.api.auth_router.AuthService')
    @patch('src.api.auth_router.settings')
    def test_login_alternative_json(self, mock_settings, mock_auth_service_class, client, mock_user):
        """Test JSON-based login endpoint."""
        # Mock settings
        mock_settings.REFRESH_TOKEN_EXPIRE_DAYS = 7
        mock_settings.COOKIE_SECURE = False
        
        # Mock the service instance
        mock_service = AsyncMock()
        mock_auth_service_class.return_value = mock_service
        mock_service.authenticate_user.return_value = mock_user
        mock_service.create_user_tokens.return_value = {
            "access_token": "test_access_token",
            "refresh_token": "test_refresh_token",
            "token_type": "bearer"
        }
        
        login_data = {
            "username_or_email": "testuser",
            "password": "password123"
        }
        
        response = client.post("/auth/login/alternative", json=login_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["access_token"] == "test_access_token"
    
    @patch('src.api.auth_router.AuthService')
    @patch('src.api.auth_router.settings')
    def test_refresh_token_from_cookie(self, mock_settings, mock_auth_service_class, client):
        """Test refresh token from cookie."""
        # Mock settings
        mock_settings.REFRESH_TOKEN_EXPIRE_DAYS = 7
        mock_settings.COOKIE_SECURE = False
        
        # Mock the service instance
        mock_service = AsyncMock()
        mock_auth_service_class.return_value = mock_service
        mock_service.refresh_access_token.return_value = {
            "access_token": "new_access_token",
            "refresh_token": "new_refresh_token",
            "token_type": "bearer"
        }
        
        client.cookies = {"refresh_token": "old_refresh_token"}
        response = client.post("/auth/refresh-token")
        
        assert response.status_code == 200
        data = response.json()
        assert data["access_token"] == "new_access_token"
    
    @patch('src.api.auth_router.AuthService')
    @patch('src.api.auth_router.settings')
    def test_refresh_token_from_body(self, mock_settings, mock_auth_service_class, client):
        """Test refresh token from request body."""
        # Mock settings
        mock_settings.REFRESH_TOKEN_EXPIRE_DAYS = 7
        mock_settings.COOKIE_SECURE = False
        
        # Mock the service instance
        mock_service = AsyncMock()
        mock_auth_service_class.return_value = mock_service
        mock_service.refresh_access_token.return_value = {
            "access_token": "new_access_token",
            "refresh_token": "new_refresh_token",
            "token_type": "bearer"
        }
        
        response = client.post(
            "/auth/refresh-token",
            json={"refresh_token": "old_refresh_token"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["access_token"] == "new_access_token"
    
    @patch('src.api.auth_router.AuthService')
    def test_logout_success(self, mock_auth_service_class, client, mock_user):
        """Test successful logout."""
        # Mock the service instance
        mock_service = AsyncMock()
        mock_auth_service_class.return_value = mock_service
        mock_service.revoke_refresh_token.return_value = True
        
        response = client.post(
            "/auth/logout",
            json={"refresh_token": "token_to_revoke"}
        )
        
        assert response.status_code == 204
    
    @patch('src.api.auth_router.AuthService')
    @patch('src.api.auth_router.UserService')
    def test_password_change_success(self, mock_user_service_class, mock_auth_service_class, client, mock_user):
        """Test successful password change."""
        # Mock the service instances
        mock_auth_service = AsyncMock()
        mock_auth_service_class.return_value = mock_auth_service
        mock_auth_service.authenticate_user.return_value = mock_user
        mock_auth_service.revoke_all_user_tokens.return_value = True
        
        mock_user_service = AsyncMock()
        mock_user_service_class.return_value = mock_user_service
        mock_user_service.update_password.return_value = True
        
        password_data = {
            "current_password": "oldpassword",
            "new_password": "NewPassword123!"
        }
        
        response = client.post("/auth/password-change", json=password_data)
        
        assert response.status_code == 200
        assert "Password has been changed" in response.json()["message"]
    
    @patch('src.api.auth_router.AuthService')
    @patch('src.api.auth_router.UserService')
    def test_password_change_wrong_current(self, mock_user_service_class, mock_auth_service_class, client, mock_user):
        """Test password change with wrong current password."""
        # Mock the service instances
        mock_auth_service = AsyncMock()
        mock_auth_service_class.return_value = mock_auth_service
        mock_auth_service.authenticate_user.return_value = None
        
        mock_user_service = AsyncMock()
        mock_user_service_class.return_value = mock_user_service
        
        password_data = {
            "current_password": "wrongpassword",
            "new_password": "NewPassword123!"
        }
        
        response = client.post("/auth/password-change", json=password_data)
        
        assert response.status_code == 401
        assert "Current password is incorrect" in response.json()["detail"]
    
    @patch('src.api.auth_router.AuthService')
    def test_register_user_success(self, mock_auth_service_class, client):
        """Test successful user registration."""
        # Mock the service instance
        mock_service = AsyncMock()
        mock_auth_service_class.return_value = mock_service
        from datetime import datetime
        mock_service.register_user.return_value = {
            "id": "user-123",
            "username": "newuser",
            "email": "new@example.com",
            "role": "regular",
            "status": "active",
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "last_login": None
        }
        
        user_data = {
            "username": "newuser",
            "email": "new@example.com",
            "password": "Password123"
        }
        
        response = client.post("/auth/register", json=user_data)
        
        assert response.status_code == 201
        data = response.json()
        assert data["username"] == "newuser"
        assert data["email"] == "new@example.com"
    
    @patch('src.api.auth_router.AuthService')
    def test_register_user_error(self, mock_auth_service_class, client):
        """Test user registration with validation error."""
        # Mock the service instance to raise ValueError
        mock_service = AsyncMock()
        mock_auth_service_class.return_value = mock_service
        mock_service.register_user.side_effect = ValueError("Username already exists")
        
        user_data = {
            "username": "existinguser",
            "email": "existing@example.com",
            "password": "Password123"
        }
        
        response = client.post("/auth/register", json=user_data)
        
        assert response.status_code == 400
        assert "Username already exists" in response.json()["detail"]
    
    @patch('src.api.auth_router.AuthService')
    @patch('src.api.auth_router.settings')
    def test_login_invalid_credentials(self, mock_settings, mock_auth_service_class, client):
        """Test login with invalid credentials."""
        # Mock settings
        mock_settings.REFRESH_TOKEN_EXPIRE_DAYS = 7
        mock_settings.COOKIE_SECURE = False
        
        # Mock the service instance to return None (authentication failure)
        mock_service = AsyncMock()
        mock_auth_service_class.return_value = mock_service
        mock_service.authenticate_user.return_value = None
        
        form_data = {
            "username": "invaliduser",
            "password": "wrongpassword"
        }
        
        response = client.post("/auth/login", data=form_data)
        
        assert response.status_code == 401
        assert "Incorrect username or password" in response.json()["detail"]
        assert response.headers.get("WWW-Authenticate") == "Bearer"
    
    @patch('src.api.auth_router.AuthService')
    @patch('src.api.auth_router.settings')
    def test_login_alternative_invalid_credentials(self, mock_settings, mock_auth_service_class, client):
        """Test JSON login with invalid credentials."""
        # Mock settings
        mock_settings.REFRESH_TOKEN_EXPIRE_DAYS = 7
        mock_settings.COOKIE_SECURE = False
        
        # Mock the service instance to return None (authentication failure)
        mock_service = AsyncMock()
        mock_auth_service_class.return_value = mock_service
        mock_service.authenticate_user.return_value = None
        
        login_data = {
            "username_or_email": "invaliduser",
            "password": "wrongpassword"
        }
        
        response = client.post("/auth/login/alternative", json=login_data)
        
        assert response.status_code == 401
        assert "Incorrect username or password" in response.json()["detail"]
        assert response.headers.get("WWW-Authenticate") == "Bearer"
    
    @patch('src.api.auth_router.AuthService')
    def test_refresh_token_missing(self, mock_auth_service_class, client):
        """Test refresh token endpoint with missing token."""
        # Mock the service instance
        mock_service = AsyncMock()
        mock_auth_service_class.return_value = mock_service
        
        response = client.post("/auth/refresh-token")
        
        assert response.status_code == 401
        assert "Refresh token required" in response.json()["detail"]
        assert response.headers.get("WWW-Authenticate") == "Bearer"
    
    @patch('src.api.auth_router.AuthService')
    @patch('src.api.auth_router.settings')
    def test_refresh_token_invalid_with_cookie(self, mock_settings, mock_auth_service_class, client):
        """Test refresh token with invalid token from cookie."""
        # Mock settings
        mock_settings.REFRESH_TOKEN_EXPIRE_DAYS = 7
        mock_settings.COOKIE_SECURE = False
        
        # Mock the service instance to return None (invalid token)
        mock_service = AsyncMock()
        mock_auth_service_class.return_value = mock_service
        mock_service.refresh_access_token.return_value = None
        
        client.cookies = {"refresh_token": "invalid_token"}
        response = client.post("/auth/refresh-token")
        
        assert response.status_code == 401
        assert "Invalid or expired refresh token" in response.json()["detail"]
        assert response.headers.get("WWW-Authenticate") == "Bearer"
    
    @patch('src.api.auth_router.AuthService')
    def test_refresh_token_invalid_without_cookie(self, mock_auth_service_class, client):
        """Test refresh token with invalid token from body."""
        # Mock the service instance to return None (invalid token)
        mock_service = AsyncMock()
        mock_auth_service_class.return_value = mock_service
        mock_service.refresh_access_token.return_value = None
        
        response = client.post(
            "/auth/refresh-token",
            json={"refresh_token": "invalid_token"}
        )
        
        assert response.status_code == 401
        assert "Invalid or expired refresh token" in response.json()["detail"]
        assert response.headers.get("WWW-Authenticate") == "Bearer"
    
    @patch('src.api.auth_router.AuthService')
    def test_logout_without_token(self, mock_auth_service_class, client, mock_user):
        """Test logout without refresh token."""
        # Mock the service instance
        mock_service = AsyncMock()
        mock_auth_service_class.return_value = mock_service
        
        response = client.post("/auth/logout")
        
        assert response.status_code == 204
        # Should not call revoke_refresh_token if no token provided
        mock_service.revoke_refresh_token.assert_not_called()
    
    @patch('src.api.auth_router.AuthService')
    def test_logout_with_cookie(self, mock_auth_service_class, client, mock_user):
        """Test logout with refresh token from cookie."""
        # Mock the service instance
        mock_service = AsyncMock()
        mock_auth_service_class.return_value = mock_service
        mock_service.revoke_refresh_token.return_value = True
        
        client.cookies = {"refresh_token": "token_to_revoke"}
        response = client.post("/auth/logout")
        
        assert response.status_code == 204
        mock_service.revoke_refresh_token.assert_called_once_with("token_to_revoke")
    
    def test_password_reset_request(self, client):
        """Test password reset request endpoint."""
        reset_request = {
            "email": "user@example.com"
        }
        
        response = client.post("/auth/password-reset-request", json=reset_request)
        
        assert response.status_code == 202
        data = response.json()
        assert "password reset link has been sent" in data["message"]
    
    def test_password_reset(self, client):
        """Test password reset endpoint."""
        reset_data = {
            "token": "reset_token_123",
            "new_password": "NewPassword123!"
        }
        
        response = client.post("/auth/password-reset", json=reset_data)
        
        assert response.status_code == 200
        data = response.json()
        assert "Password has been reset" in data["message"]
    
    def test_oauth_authorize(self, client):
        """Test OAuth authorization endpoint."""
        provider = "databricks"
        redirect_uri = "http://localhost:3000/callback"
        
        response = client.get(f"/auth/oauth/{provider}/authorize?redirect_uri={redirect_uri}")
        
        assert response.status_code == 200
        data = response.json()
        assert "auth_url" in data
        assert f"https://{provider}.example.com/authorize" in data["auth_url"]
        assert redirect_uri in data["auth_url"]
    
    def test_oauth_authorize_no_redirect(self, client):
        """Test OAuth authorization without redirect URI."""
        provider = "github"
        
        response = client.get(f"/auth/oauth/{provider}/authorize")
        
        assert response.status_code == 200
        data = response.json()
        assert "auth_url" in data
        assert f"https://{provider}.example.com/authorize" in data["auth_url"]
        assert "default" in data["auth_url"]
    
    @patch('src.api.auth_router.settings')
    def test_oauth_callback(self, mock_settings, client):
        """Test OAuth callback endpoint."""
        # Mock settings
        mock_settings.REFRESH_TOKEN_EXPIRE_DAYS = 7
        mock_settings.COOKIE_SECURE = False
        
        provider = "databricks"
        code = "auth_code_123"
        state = "state_token"
        redirect_uri = "http://localhost:3000/callback"
        
        response = client.post(
            f"/auth/oauth/{provider}/callback?code={code}&state={state}&redirect_uri={redirect_uri}"
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert "refresh_token" in data
        assert data["token_type"] == "bearer"
        assert "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" in data["access_token"]
    
    @patch('src.api.auth_router.settings')
    def test_oauth_callback_minimal(self, mock_settings, client):
        """Test OAuth callback with minimal parameters."""
        # Mock settings
        mock_settings.REFRESH_TOKEN_EXPIRE_DAYS = 7
        mock_settings.COOKIE_SECURE = False
        
        provider = "github"
        code = "minimal_code"
        
        response = client.post(f"/auth/oauth/{provider}/callback?code={code}")
        
        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert "refresh_token" in data
        assert data["token_type"] == "bearer"