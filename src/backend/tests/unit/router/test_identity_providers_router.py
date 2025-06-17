"""
Unit tests for IdentityProvidersRouter.

Tests the functionality of authentication provider management endpoints.
"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi.testclient import TestClient
from fastapi import HTTPException


@pytest.fixture
def app():
    """Create a FastAPI app."""
    from fastapi import FastAPI
    
    app = FastAPI()
    
    # Mock database dependency to avoid settings issues
    from unittest.mock import MagicMock
    mock_db = MagicMock()
    
    def get_mock_db():
        return mock_db
    
    # Import and setup router with mocked dependencies
    with patch('src.api.identity_providers_router.get_db', get_mock_db):
        from src.api.identity_providers_router import router
        app.include_router(router)
    
    return app



@pytest.fixture
def mock_regular_user():
    """Create a mock regular user."""
    from datetime import datetime, timezone
    
    class MockUser:
        def __init__(self):
            self.id = "regular-user-123"
            self.username = "testuser"
            self.email = "test@example.com"
            self.role = "user"
            self.status = "active"
            self.created_at = datetime.now(timezone.utc)
            self.updated_at = datetime.now(timezone.utc)
    
    return MockUser()


@pytest.fixture
def mock_admin_user():
    """Create a mock admin user."""
    from datetime import datetime, timezone
    
    class MockUser:
        def __init__(self):
            self.id = "admin-user-123"
            self.username = "adminuser"
            self.email = "admin@example.com"
            self.role = "admin"
            self.status = "active"
            self.created_at = datetime.now(timezone.utc)
            self.updated_at = datetime.now(timezone.utc)
    
    return MockUser()


@pytest.fixture
def client_regular(app, mock_regular_user):
    """Create a test client with regular user authentication."""
    from src.dependencies.auth import get_current_user
    from src.dependencies.auth import check_user_role
    
    app.dependency_overrides[get_current_user] = lambda: mock_regular_user
    app.dependency_overrides[check_user_role] = lambda allowed_roles=None: None
    
    return TestClient(app)


@pytest.fixture
def client_admin(app, mock_admin_user):
    """Create a test client with admin user authentication."""
    from src.dependencies.auth import get_current_user
    from src.dependencies.auth import check_user_role
    
    app.dependency_overrides[get_current_user] = lambda: mock_admin_user
    app.dependency_overrides[check_user_role] = lambda allowed_roles=None: None
    
    return TestClient(app)


def create_mock_provider(provider_id: str, name: str, provider_type: str = "oauth", 
                        enabled: bool = True, is_default: bool = False, config: dict = None):
    """Create a mock provider with proper schema structure."""
    from datetime import datetime, timezone
    
    if config is None:
        config = {"client_id": f"{name.lower()}_id"}
    
    class MockProvider:
        def __init__(self):
            self.id = provider_id
            self.name = name
            self.type = provider_type
            self.enabled = enabled
            self.is_default = is_default
            self.config = config
            self.created_at = datetime.now(timezone.utc)
            self.updated_at = datetime.now(timezone.utc)
    
    return MockProvider()


class TestIdentityProvidersRouter:
    """Test cases for identity providers endpoints."""
    
    # Test GET /identity-providers
    @patch('src.api.identity_providers_router.IdentityProviderService')
    def test_get_providers_success_admin(self, mock_service_class, client_admin):
        """Test successful identity providers retrieval as admin."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        # Create mock providers with proper schema structure
        mock_providers = [
            create_mock_provider("1", "Google", "oauth", enabled=True),
            create_mock_provider("2", "Azure AD", "saml", enabled=False, 
                               config={"tenant_id": "azure_tenant"})
        ]
        mock_service.get_providers.return_value = mock_providers
        
        response = client_admin.get("/identity-providers/")
        
        assert response.status_code == 200
        mock_service.get_providers.assert_called_once_with(skip=0, limit=100, enabled_only=False)
    
    @patch('src.api.identity_providers_router.IdentityProviderService')
    def test_get_providers_success_regular_user(self, mock_service_class, client_regular):
        """Test identity providers retrieval as regular user (enabled only)."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        # Create mock providers 
        mock_providers = [
            create_mock_provider("1", "Google", "oauth", enabled=True)
        ]
        mock_service.get_providers.return_value = mock_providers
        
        # For regular users, we expect the response to work and config to be removed
        response = client_regular.get("/identity-providers/")
        
        assert response.status_code == 200
        # Should call with enabled_only=True for regular users
        mock_service.get_providers.assert_called_once_with(skip=0, limit=100, enabled_only=True)
        # Config should be None after the router processes it for non-admin users
        assert mock_providers[0].config is None
    
    @patch('src.api.identity_providers_router.IdentityProviderService')
    def test_get_providers_with_pagination(self, mock_service_class, client_admin):
        """Test identity providers retrieval with pagination."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.get_providers.return_value = []
        
        response = client_admin.get("/identity-providers/?skip=10&limit=50&enabled_only=true")
        
        assert response.status_code == 200
        # Admin users can use the enabled_only parameter if provided
        mock_service.get_providers.assert_called_once_with(skip=10, limit=50, enabled_only=True)
    
    # Test POST /identity-providers
    @patch('src.api.identity_providers_router.IdentityProviderService')
    def test_create_provider_success(self, mock_service_class, client_admin):
        """Test successful identity provider creation."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        created_provider = create_mock_provider("3", "GitHub", "oauth", enabled=True,
                                              config={"client_id": "github_client_id", "client_secret": "github_secret"})
        mock_service.create_provider.return_value = created_provider
        
        provider_data = {
            "name": "GitHub",
            "type": "oauth",
            "config": {
                "client_id": "github_client_id",
                "client_secret": "github_secret"
            },
            "enabled": True
        }
        
        response = client_admin.post("/identity-providers/", json=provider_data)
        
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "GitHub"
    
    @patch('src.api.identity_providers_router.IdentityProviderService')
    def test_create_provider_value_error(self, mock_service_class, client_admin):
        """Test identity provider creation with validation error."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.create_provider.side_effect = ValueError("Invalid provider configuration")
        
        provider_data = {
            "name": "InvalidProvider",
            "type": "oauth",
            "config": {
                "client_id": "invalid_id"
            }
        }
        
        response = client_admin.post("/identity-providers/", json=provider_data)
        
        assert response.status_code == 400
        assert "Invalid provider configuration" in response.json()["detail"]
    
    # Test GET /identity-providers/{provider_id}
    @patch('src.api.identity_providers_router.IdentityProviderService')
    def test_get_provider_success_admin(self, mock_service_class, client_admin):
        """Test successful identity provider retrieval by ID as admin."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        mock_provider = create_mock_provider("1", "Google", "oauth", enabled=True)
        mock_service.get_provider.return_value = mock_provider
        
        response = client_admin.get("/identity-providers/1")
        
        assert response.status_code == 200
        mock_service.get_provider.assert_called_once_with("1")
    
    @patch('src.api.identity_providers_router.IdentityProviderService')
    def test_get_provider_success_regular_user(self, mock_service_class, client_regular):
        """Test identity provider retrieval by ID as regular user (config removed)."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        mock_provider = create_mock_provider("1", "Google", "oauth", enabled=True)
        mock_service.get_provider.return_value = mock_provider
        
        response = client_regular.get("/identity-providers/1")
        
        assert response.status_code == 200
        # Verify config was set to None for regular user
        assert mock_provider.config is None
    
    @patch('src.api.identity_providers_router.IdentityProviderService')
    def test_get_provider_not_found(self, mock_service_class, client_admin):
        """Test identity provider retrieval when provider not found."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.get_provider.return_value = None
        
        response = client_admin.get("/identity-providers/999")
        
        assert response.status_code == 404
        assert "Identity provider not found" in response.json()["detail"]
    
    @patch('src.api.identity_providers_router.IdentityProviderService')
    def test_get_provider_disabled_regular_user(self, mock_service_class, client_regular):
        """Test regular user cannot access disabled provider."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        mock_provider = create_mock_provider("1", "Google", "oauth", enabled=False)
        mock_service.get_provider.return_value = mock_provider
        
        response = client_regular.get("/identity-providers/1")
        
        assert response.status_code == 404
        assert "Identity provider not found" in response.json()["detail"]
    
    # Test PUT /identity-providers/{provider_id}
    @patch('src.api.identity_providers_router.IdentityProviderService')
    def test_update_provider_success(self, mock_service_class, client_admin):
        """Test successful identity provider update."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        updated_provider = create_mock_provider("1", "Google Updated", "oauth", enabled=False)
        mock_service.update_provider.return_value = updated_provider
        
        update_data = {
            "name": "Google Updated",
            "enabled": False
        }
        
        response = client_admin.put("/identity-providers/1", json=update_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Google Updated"
        assert data["enabled"] is False
    
    @patch('src.api.identity_providers_router.IdentityProviderService')
    def test_update_provider_not_found(self, mock_service_class, client_admin):
        """Test identity provider update when provider not found."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.update_provider.return_value = None
        
        update_data = {"name": "Updated Name"}
        
        response = client_admin.put("/identity-providers/999", json=update_data)
        
        assert response.status_code == 404
        assert "Identity provider not found" in response.json()["detail"]
    
    @patch('src.api.identity_providers_router.IdentityProviderService')
    def test_update_provider_value_error(self, mock_service_class, client_admin):
        """Test identity provider update with validation error."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.update_provider.side_effect = ValueError("Invalid update data")
        
        update_data = {"name": ""}
        
        response = client_admin.put("/identity-providers/1", json=update_data)
        
        assert response.status_code == 400
        assert "Invalid update data" in response.json()["detail"]
    
    # Test DELETE /identity-providers/{provider_id}
    @patch('src.api.identity_providers_router.IdentityProviderService')
    def test_delete_provider_success(self, mock_service_class, client_admin):
        """Test successful identity provider deletion."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.delete_provider.return_value = True
        
        response = client_admin.delete("/identity-providers/1")
        
        assert response.status_code == 204
        mock_service.delete_provider.assert_called_once_with("1")
    
    @patch('src.api.identity_providers_router.IdentityProviderService')
    def test_delete_provider_not_found(self, mock_service_class, client_admin):
        """Test identity provider deletion when provider not found."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.delete_provider.return_value = False
        
        response = client_admin.delete("/identity-providers/999")
        
        assert response.status_code == 404
        assert "Identity provider not found" in response.json()["detail"]
    
    @patch('src.api.identity_providers_router.IdentityProviderService')
    def test_delete_provider_value_error(self, mock_service_class, client_admin):
        """Test identity provider deletion with validation error."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.delete_provider.side_effect = ValueError("Cannot delete provider in use")
        
        response = client_admin.delete("/identity-providers/1")
        
        assert response.status_code == 400
        assert "Cannot delete provider in use" in response.json()["detail"]
    
    # Test PATCH /identity-providers/{provider_id}/toggle
    @patch('src.api.identity_providers_router.IdentityProviderService')
    def test_toggle_provider_success(self, mock_service_class, client_admin):
        """Test successful identity provider toggle."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        toggled_provider = create_mock_provider("1", "Google", "oauth", enabled=False)
        mock_service.toggle_provider_status.return_value = toggled_provider
        
        response = client_admin.patch("/identity-providers/1/toggle?enabled=false")
        
        assert response.status_code == 200
        data = response.json()
        assert data["enabled"] is False
        mock_service.toggle_provider_status.assert_called_once_with("1", False)
    
    @patch('src.api.identity_providers_router.IdentityProviderService')
    def test_toggle_provider_not_found(self, mock_service_class, client_admin):
        """Test identity provider toggle when provider not found."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.toggle_provider_status.return_value = None
        
        response = client_admin.patch("/identity-providers/999/toggle?enabled=true")
        
        assert response.status_code == 404
        assert "Identity provider not found" in response.json()["detail"]
    
    # Test GET /identity-providers/{provider_id}/stats
    @patch('src.api.identity_providers_router.IdentityProviderService')
    def test_get_provider_stats_success(self, mock_service_class, client_admin):
        """Test successful identity provider stats retrieval."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.get_provider_usage_stats.return_value = {
            "provider_id": "1",
            "provider_name": "Google",
            "user_count": 150,
            "login_count": 200,
            "last_login": "2024-01-01T00:00:00Z",
            "active_users": 120
        }
        
        response = client_admin.get("/identity-providers/1/stats")
        
        assert response.status_code == 200
        data = response.json()
        assert data["user_count"] == 150
        assert data["active_users"] == 120
        mock_service.get_provider_usage_stats.assert_called_once_with("1")
    
    @patch('src.api.identity_providers_router.IdentityProviderService')
    def test_get_provider_stats_error(self, mock_service_class, client_admin):
        """Test identity provider stats retrieval with error."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.get_provider_usage_stats.return_value = {
            "error": "Provider not found or stats unavailable"
        }
        
        response = client_admin.get("/identity-providers/999/stats")
        
        assert response.status_code == 404
        assert "Provider not found or stats unavailable" in response.json()["detail"]