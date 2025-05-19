"""
Unit tests for health check API router.

Tests the functionality of the health check API endpoint.
"""
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.healthcheck_router import router


@pytest.fixture
def app():
    """Create a FastAPI app for testing."""
    app = FastAPI()
    app.include_router(router)
    return app


@pytest.fixture
def client(app):
    """Create a test client for the app."""
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