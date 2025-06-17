"""
Unit tests for health check API router.

Tests the functionality of the health check API endpoint.
"""
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from src.dependencies.admin_auth import (
    require_authenticated_user, get_authenticated_user, get_admin_user
)

from src.api.healthcheck_router import router


@pytest.fixture
def app():
    """Create a FastAPI app for testing."""
    app = FastAPI()
    app.include_router(router)
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
    """Create a test client for the app."""
    # Override authentication dependencies for testing
    app.dependency_overrides[require_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_admin_user] = lambda: mock_current_user


    return TestClient(app)


def test_health_check(client):
    """Test that the health check endpoint returns the expected response."""
    response = client.get("/health")
    
    # Check status code
    assert response.status_code == 200
    
    # Check response content
    result = response.json()
    assert result["status"] == "ok"
    assert result["message"] == "Service is healthy" 