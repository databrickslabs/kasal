"""
Unit tests for DatabricksRouter.

Tests the functionality of Databricks integration endpoints including
configuration management, connection testing, and token requirements.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from src.dependencies.admin_auth import (
    require_authenticated_user, get_authenticated_user, get_admin_user
)

from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.schemas.databricks_config import DatabricksConfigCreate


# Mock databricks config response
class MockDatabricksConfigResponse:
    def __init__(self, workspace_url="https://test.databricks.com", token_set=True):
        self.workspace_url = workspace_url
        self.token_set = token_set
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
    def model_dump(self):
        """Mock model_dump for Pydantic compatibility."""
        return {
            "workspace_url": self.workspace_url,
            "token_set": self.token_set,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }


@pytest.fixture
def mock_databricks_service():
    """Create a mock databricks service."""
    service = AsyncMock()
    return service


@pytest.fixture
def mock_api_keys_service():
    """Create a mock API keys service."""
    service = AsyncMock()
    return service


@pytest.fixture
def mock_db_session():
    """Create a mock database session."""
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def app(mock_databricks_service, mock_api_keys_service, mock_db_session):
    """Create a FastAPI app with mocked dependencies."""
    from fastapi import FastAPI
    from src.api.databricks_router import router, get_databricks_service, get_api_keys_service
    from src.core.dependencies import get_db
    
    app = FastAPI()
    app.include_router(router)
    
    # Override dependencies properly - return the mocks directly
    app.dependency_overrides[get_db] = lambda: mock_db_session
    app.dependency_overrides[get_api_keys_service] = lambda session=None: mock_api_keys_service
    app.dependency_overrides[get_databricks_service] = lambda session=None, api_keys_service=None: mock_databricks_service
    
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


@pytest.fixture
def sample_databricks_config_create():
    """Create a sample databricks configuration request."""
    return DatabricksConfigCreate(
        workspace_url="https://test.databricks.com",
        warehouse_id="test-warehouse",
        catalog="test-catalog",
        schema="test-schema",  # Use the alias 'schema' instead of 'db_schema'
        secret_scope="test-scope",
        enabled=True,
        apps_enabled=False
    )


class TestSetDatabricksConfig:
    """Test cases for set databricks config endpoint."""
    
    def test_set_databricks_config_success(self, client, mock_databricks_service, sample_databricks_config_create):
        """Test successful databricks configuration setting."""
        config_response = {
            "status": "success",
            "message": "Databricks configuration set successfully",
            "workspace_url": "https://test.databricks.com"
        }
        mock_databricks_service.set_databricks_config.return_value = config_response
        
        response = client.post("/databricks/config", json=sample_databricks_config_create.model_dump(by_alias=True))
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert "configuration" in data["message"] and "successfully" in data["message"]
    
    def test_set_databricks_config_service_error(self, client, mock_databricks_service, sample_databricks_config_create):
        """Test setting databricks config with service error."""
        mock_databricks_service.set_databricks_config.side_effect = Exception("Configuration error")
        
        response = client.post("/databricks/config", json=sample_databricks_config_create.model_dump(by_alias=True))
        
        assert response.status_code == 500
        assert "Configuration error" in response.json()["detail"]
    
    def test_set_databricks_config_invalid_data(self, client, mock_databricks_service):
        """Test setting databricks config with invalid data."""
        invalid_config = {
            "workspace_url": "not-a-valid-url",
            "personal_access_token": ""
        }
        
        response = client.post("/databricks/config", json=invalid_config)
        
        assert response.status_code == 422  # Validation error


class TestGetDatabricksConfig:
    """Test cases for get databricks config endpoint."""
    
    def test_get_databricks_config_success(self, client, mock_databricks_service):
        """Test successful databricks configuration retrieval."""
        config_response = {
            "workspace_url": "https://test.databricks.com",
            "warehouse_id": "test-warehouse",
            "catalog": "test-catalog",
            "db_schema": "test-schema",
            "secret_scope": "test-scope",
            "enabled": True,
            "apps_enabled": False
        }
        mock_databricks_service.get_databricks_config.return_value = config_response
        
        response = client.get("/databricks/config")
        
        assert response.status_code == 200
        data = response.json()
        assert data["workspace_url"] == "https://test.databricks.com"
    
    def test_get_databricks_config_not_found(self, client, mock_databricks_service):
        """Test getting databricks config when not configured."""
        mock_databricks_service.get_databricks_config.side_effect = HTTPException(
            status_code=404, detail="Databricks configuration not found"
        )
        
        response = client.get("/databricks/config")
        
        assert response.status_code == 404
        assert "configuration not found" in response.json()["detail"]
    
    def test_get_databricks_config_service_error(self, client, mock_databricks_service):
        """Test getting databricks config with service error."""
        mock_databricks_service.get_databricks_config.side_effect = Exception("Database error")
        
        response = client.get("/databricks/config")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestCheckPersonalTokenRequired:
    """Test cases for check personal token required endpoint."""
    
    def test_check_personal_token_required_true(self, client, mock_databricks_service):
        """Test checking personal token requirement when required."""
        token_status = {
            "personal_token_required": True,
            "message": "OAuth not configured"
        }
        mock_databricks_service.check_personal_token_required.return_value = token_status
        
        response = client.get("/databricks/status/personal-token-required")
        
        assert response.status_code == 200
        data = response.json()
        assert data["personal_token_required"] is True
    
    def test_check_personal_token_required_false(self, client, mock_databricks_service):
        """Test checking personal token requirement when not required."""
        token_status = {
            "personal_token_required": False,
            "message": "OAuth configured and working"
        }
        mock_databricks_service.check_personal_token_required.return_value = token_status
        
        response = client.get("/databricks/status/personal-token-required")
        
        assert response.status_code == 200
        data = response.json()
        assert data["personal_token_required"] is False
    
    def test_check_personal_token_required_service_error(self, client, mock_databricks_service):
        """Test checking personal token requirement with service error."""
        mock_databricks_service.check_personal_token_required.side_effect = Exception("Service error")
        
        response = client.get("/databricks/status/personal-token-required")
        
        assert response.status_code == 500
        assert "Service error" in response.json()["detail"]


class TestCheckDatabricksConnection:
    """Test cases for check databricks connection endpoint."""
    
    def test_check_databricks_connection_success(self, client, mock_databricks_service):
        """Test successful databricks connection check."""
        connection_status = {
            "connected": True,
            "workspace_url": "https://test.databricks.com",
            "user": "test@example.com",
            "message": "Connection successful"
        }
        mock_databricks_service.check_databricks_connection.return_value = connection_status
        
        response = client.get("/databricks/connection")
        
        assert response.status_code == 200
        data = response.json()
        assert data["connected"] is True
        assert data["workspace_url"] == "https://test.databricks.com"
        assert data["user"] == "test@example.com"
    
    def test_check_databricks_connection_failed(self, client, mock_databricks_service):
        """Test failed databricks connection check."""
        connection_status = {
            "connected": False,
            "error": "Invalid credentials",
            "message": "Connection failed"
        }
        mock_databricks_service.check_databricks_connection.return_value = connection_status
        
        response = client.get("/databricks/connection")
        
        assert response.status_code == 200
        data = response.json()
        assert data["connected"] is False
        assert "Invalid credentials" in data["error"]
        assert "Connection failed" in data["message"]
    
    def test_check_databricks_connection_not_configured(self, client, mock_databricks_service):
        """Test connection check when databricks not configured."""
        connection_status = {
            "connected": False,
            "error": "Databricks not configured",
            "message": "Please configure Databricks first"
        }
        mock_databricks_service.check_databricks_connection.return_value = connection_status
        
        response = client.get("/databricks/connection")
        
        assert response.status_code == 200
        data = response.json()
        assert data["connected"] is False
        assert "not configured" in data["error"]
    
    def test_check_databricks_connection_service_error(self, client, mock_databricks_service):
        """Test databricks connection check with service error."""
        mock_databricks_service.check_databricks_connection.side_effect = Exception("Network error")
        
        response = client.get("/databricks/connection")
        
        assert response.status_code == 500
        assert "Network error" in response.json()["detail"]


class TestDependencyInjection:
    """Test cases for dependency injection functions."""
    
    @patch('src.api.databricks_router.ApiKeysService')
    def test_get_api_keys_service(self, mock_api_keys_service_class):
        """Test get_api_keys_service dependency function."""
        from src.api.databricks_router import get_api_keys_service
        
        # Mock session
        mock_session = MagicMock()
        
        # Mock service instance
        mock_service_instance = MagicMock()
        mock_api_keys_service_class.return_value = mock_service_instance
        
        # Call the dependency function
        result = get_api_keys_service(mock_session)
        
        # Verify service is created with session
        mock_api_keys_service_class.assert_called_once_with(mock_session)
        assert result == mock_service_instance
    
    @patch('src.api.databricks_router.DatabricksService')
    def test_get_databricks_service(self, mock_databricks_service_class):
        """Test get_databricks_service dependency function."""
        from src.api.databricks_router import get_databricks_service
        
        # Mock session and api_keys_service
        mock_session = MagicMock()
        mock_api_keys_service = MagicMock()
        
        # Mock service instance
        mock_service_instance = MagicMock()
        mock_databricks_service_class.from_session.return_value = mock_service_instance
        
        # Call the dependency function
        result = get_databricks_service(mock_session, mock_api_keys_service)
        
        # Verify service is created properly
        mock_databricks_service_class.from_session.assert_called_once_with(mock_session, mock_api_keys_service)
        assert result == mock_service_instance


class TestRouterConfiguration:
    """Test cases for router configuration and setup."""
    
    def test_router_prefix_and_tags(self):
        """Test that router has correct prefix and tags."""
        from src.api.databricks_router import router
        
        assert router.prefix == "/databricks"
        assert "databricks" in router.tags
        assert 404 in router.responses
        assert router.responses[404]["description"] == "Not found"
    
    def test_router_endpoints_exist(self):
        """Test that all expected endpoints are registered."""
        from src.api.databricks_router import router
        
        # Get all routes from the router
        route_paths = [route.path for route in router.routes]
        
        # Check expected endpoints exist
        expected_paths = [
            "/config",  # POST and GET
            "/status/personal-token-required",  # GET
            "/connection"  # GET
        ]
        
        for path in expected_paths:
            assert any(path in route_path for route_path in route_paths), f"Missing route: {path}"


class TestEdgeCasesAndErrorHandling:
    """Test edge cases and comprehensive error handling scenarios."""
    
    def test_set_databricks_config_with_http_exception_wrapped(self, client, mock_databricks_service):
        """Test that HTTPExceptions from service are wrapped in 500 error for set_databricks_config."""
        # Service raises HTTPException
        mock_databricks_service.set_databricks_config.side_effect = HTTPException(
            status_code=400, detail="Invalid workspace URL"
        )
        
        sample_config = {
            "workspace_url": "https://test.databricks.com",
            "warehouse_id": "test-warehouse",
            "catalog": "test-catalog",
            "schema": "test-schema",
            "secret_scope": "test-scope",
            "enabled": True,
            "apps_enabled": False
        }
        
        response = client.post("/databricks/config", json=sample_config)
        
        # Should get wrapped in 500 error (unlike get_databricks_config)
        assert response.status_code == 500
        assert "Invalid workspace URL" in response.json()["detail"]
    
    def test_get_databricks_config_http_exception_re_raised(self, client, mock_databricks_service):
        """Test that HTTPExceptions from get_databricks_config are re-raised."""
        # This tests line 80 in the router where HTTPException is re-raised
        mock_databricks_service.get_databricks_config.side_effect = HTTPException(
            status_code=403, detail="Access denied"
        )
        
        response = client.get("/databricks/config")
        
        # Should re-raise the HTTPException
        assert response.status_code == 403
        assert "Access denied" in response.json()["detail"]
    
    def test_databricks_config_validation_errors(self, client):
        """Test comprehensive validation scenarios."""
        # Test missing required fields
        invalid_configs = [
            {},  # Empty config
            {"workspace_url": ""},  # Empty workspace URL
            {"workspace_url": "not-a-url"},  # Invalid URL format
            {"workspace_url": "https://test.com", "enabled": "not-boolean"},  # Invalid boolean
        ]
        
        for invalid_config in invalid_configs:
            response = client.post("/databricks/config", json=invalid_config)
            assert response.status_code == 422  # Validation error
    
    def test_logger_error_calls(self, client, mock_databricks_service):
        """Test that logger.error is called when exceptions occur."""
        with patch('src.api.databricks_router.logger') as mock_logger:
            # Test error logging in set_databricks_config
            mock_databricks_service.set_databricks_config.side_effect = Exception("Test error")
            
            sample_config = {
                "workspace_url": "https://test.databricks.com",
                "warehouse_id": "test-warehouse",
                "catalog": "test-catalog",
                "schema": "test-schema",
                "secret_scope": "test-scope",
                "enabled": True,
                "apps_enabled": False
            }
            
            response = client.post("/databricks/config", json=sample_config)
            
            assert response.status_code == 500
            mock_logger.error.assert_called()
            error_call_args = mock_logger.error.call_args[0][0]
            assert "Error setting Databricks configuration" in error_call_args
            assert "Test error" in error_call_args
            
            # Test error logging in get_databricks_config
            mock_databricks_service.get_databricks_config.side_effect = Exception("Get error")
            
            response = client.get("/databricks/config")
            
            assert response.status_code == 500
            # Logger should be called twice now
            assert mock_logger.error.call_count == 2
            
            # Test error logging in check_personal_token_required
            mock_databricks_service.check_personal_token_required.side_effect = Exception("Token error")
            
            response = client.get("/databricks/status/personal-token-required")
            
            assert response.status_code == 500
            assert mock_logger.error.call_count == 3
            
            # Test error logging in check_databricks_connection
            mock_databricks_service.check_databricks_connection.side_effect = Exception("Connection error")
            
            response = client.get("/databricks/connection")
            
            assert response.status_code == 500
            assert mock_logger.error.call_count == 4


class TestComprehensiveScenarios:
    """Test comprehensive scenarios to ensure full coverage."""
    
    def test_all_endpoints_with_different_response_types(self, client, mock_databricks_service):
        """Test all endpoints with various response scenarios."""
        # Test set_databricks_config with different response types
        responses = [
            {"status": "success", "config_id": "123"},
            {"message": "Configuration updated", "timestamp": datetime.now().isoformat()},
            {"result": "OK", "data": {"workspace_url": "https://test.databricks.com"}}
        ]
        
        sample_config = {
            "workspace_url": "https://test.databricks.com",
            "warehouse_id": "test-warehouse",
            "catalog": "test-catalog",
            "schema": "test-schema",
            "secret_scope": "test-scope",
            "enabled": True,
            "apps_enabled": False
        }
        
        for response_data in responses:
            mock_databricks_service.set_databricks_config.return_value = response_data
            response = client.post("/databricks/config", json=sample_config)
            assert response.status_code == 200
            assert response.json() == response_data
    
    def test_dependency_override_behavior(self, mock_databricks_service, mock_api_keys_service, mock_db_session):
        """Test that dependency overrides work correctly."""
        from fastapi import FastAPI
        from src.api.databricks_router import router, get_databricks_service, get_api_keys_service
        from src.core.dependencies import get_db
        
        app = FastAPI()
        app.include_router(router)
        
        # Test that we can override dependencies
        app.dependency_overrides[get_db] = lambda: mock_db_session
        app.dependency_overrides[get_api_keys_service] = lambda session=None: mock_api_keys_service
        app.dependency_overrides[get_databricks_service] = lambda session=None, api_keys_service=None: mock_databricks_service
        
        client = TestClient(app)
        
        # Override auth dependencies
        app.dependency_overrides[require_authenticated_user] = lambda: None
        app.dependency_overrides[get_authenticated_user] = lambda: None
        app.dependency_overrides[get_admin_user] = lambda: None
        
        # Test that endpoints work with overridden dependencies
        mock_databricks_service.get_databricks_config.return_value = {
            "workspace_url": "https://override-test.databricks.com",
            "enabled": True
        }
        
        response = client.get("/databricks/config")
        assert response.status_code == 200
        assert "override-test" in response.json()["workspace_url"]