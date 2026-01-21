"""
Unit tests for Converter Router.

Tests the functionality of converter API endpoints including
history tracking, job management, and saved configuration endpoints.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime
from fastapi.testclient import TestClient
from fastapi import FastAPI

from src.api.converter_router import router, get_converter_service
from src.schemas.conversion import (
    ConversionHistoryResponse,
    ConversionHistoryListResponse,
    ConversionStatistics,
    ConversionJobResponse,
    ConversionJobListResponse,
    SavedConfigurationResponse,
    SavedConfigurationListResponse,
)


# Mock responses
class MockHistoryResponse:
    def __init__(self, id=1, source_format="powerbi", target_format="dax", status="success"):
        self.id = id
        self.source_format = source_format
        self.target_format = target_format
        self.status = status
        self.group_id = "group-1"
        self.created_by_email = "user@example.com"
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()

    def model_dump(self):
        return {
            "id": self.id,
            "source_format": self.source_format,
            "target_format": self.target_format,
            "status": self.status,
            "group_id": self.group_id,
            "created_by_email": self.created_by_email,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


class MockJobResponse:
    def __init__(self, id="job-123", status="pending", source_format="powerbi", target_format="dax"):
        self.id = id
        self.status = status
        self.source_format = source_format
        self.target_format = target_format
        self.configuration = {}
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()

    def model_dump(self):
        return {
            "id": self.id,
            "status": self.status,
            "source_format": self.source_format,
            "target_format": self.target_format,
            "configuration": self.configuration,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


class MockConfigResponse:
    def __init__(self, id=1, name="My Config", source_format="powerbi", target_format="dax"):
        self.id = id
        self.name = name
        self.source_format = source_format
        self.target_format = target_format
        self.configuration = {}
        self.created_by_email = "user@example.com"
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()

    def model_dump(self):
        return {
            "id": self.id,
            "name": self.name,
            "source_format": self.source_format,
            "target_format": self.target_format,
            "configuration": self.configuration,
            "created_by_email": self.created_by_email,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


@pytest.fixture
def mock_converter_service():
    """Create a mock converter service."""
    return AsyncMock()


@pytest.fixture
def app(mock_converter_service):
    """Create a FastAPI app with mocked dependencies."""
    app = FastAPI()
    app.include_router(router)

    # Override dependency
    app.dependency_overrides[get_converter_service] = lambda: mock_converter_service

    return app


@pytest.fixture
def client(app):
    """Create a test client."""
    return TestClient(app)


# ===== Conversion History Endpoint Tests =====

class TestConversionHistoryEndpoints:
    """Test cases for conversion history endpoints."""

    def test_create_history_success(self, client, mock_converter_service):
        """Test successful history creation."""
        mock_response = MockHistoryResponse()
        mock_converter_service.create_history.return_value = mock_response

        response = client.post(
            "/api/converters/history",
            json={
                "source_format": "powerbi",
                "target_format": "dax",
                "status": "success"
            }
        )

        assert response.status_code == 201
        data = response.json()
        assert data["source_format"] == "powerbi"
        assert data["target_format"] == "dax"

    def test_get_history_success(self, client, mock_converter_service):
        """Test successful history retrieval."""
        mock_response = MockHistoryResponse(id=123)
        mock_converter_service.get_history.return_value = mock_response

        response = client.get("/api/converters/history/123")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == 123

    def test_get_history_not_found(self, client, mock_converter_service):
        """Test history retrieval when not found."""
        from fastapi import HTTPException
        mock_converter_service.get_history.side_effect = HTTPException(
            status_code=404,
            detail="Conversion history 999 not found"
        )

        response = client.get("/api/converters/history/999")

        assert response.status_code == 404

    def test_update_history_success(self, client, mock_converter_service):
        """Test successful history update."""
        mock_response = MockHistoryResponse(id=123, status="failed")
        mock_converter_service.update_history.return_value = mock_response

        response = client.patch(
            "/api/converters/history/123",
            json={"status": "failed", "error_message": "Conversion error"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == 123

    def test_list_history_success(self, client, mock_converter_service):
        """Test successful history listing."""
        mock_list = MagicMock()
        mock_list.history = [MockHistoryResponse(id=1), MockHistoryResponse(id=2)]
        mock_list.count = 2
        mock_list.limit = 100
        mock_list.offset = 0
        mock_list.model_dump.return_value = {
            "history": [h.model_dump() for h in mock_list.history],
            "count": 2,
            "limit": 100,
            "offset": 0
        }
        mock_converter_service.list_history.return_value = mock_list

        response = client.get("/api/converters/history?limit=100&offset=0")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2
        assert len(data["history"]) == 2

    def test_list_history_with_filters(self, client, mock_converter_service):
        """Test history listing with filters."""
        mock_list = MagicMock()
        mock_list.history = [MockHistoryResponse()]
        mock_list.count = 1
        mock_list.limit = 100
        mock_list.offset = 0
        mock_list.model_dump.return_value = {
            "history": [h.model_dump() for h in mock_list.history],
            "count": 1,
            "limit": 100,
            "offset": 0
        }
        mock_converter_service.list_history.return_value = mock_list

        response = client.get(
            "/api/converters/history?source_format=powerbi&target_format=dax&status=success"
        )

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1

    def test_get_statistics_success(self, client, mock_converter_service):
        """Test successful statistics retrieval."""
        mock_stats = MagicMock()
        mock_stats.total_conversions = 100
        mock_stats.successful = 85
        mock_stats.failed = 15
        mock_stats.success_rate = 85.0
        mock_stats.average_execution_time_ms = 1500.0
        mock_stats.popular_conversions = []
        mock_stats.period_days = 30
        mock_stats.model_dump.return_value = {
            "total_conversions": 100,
            "successful": 85,
            "failed": 15,
            "success_rate": 85.0,
            "average_execution_time_ms": 1500.0,
            "popular_conversions": [],
            "period_days": 30
        }
        mock_converter_service.get_statistics.return_value = mock_stats

        response = client.get("/api/converters/history/statistics?days=30")

        assert response.status_code == 200
        data = response.json()
        assert data["total_conversions"] == 100
        assert data["success_rate"] == 85.0


# ===== Conversion Job Endpoint Tests =====

class TestConversionJobEndpoints:
    """Test cases for conversion job endpoints."""

    def test_create_job_success(self, client, mock_converter_service):
        """Test successful job creation."""
        mock_response = MockJobResponse()
        mock_converter_service.create_job.return_value = mock_response

        response = client.post(
            "/api/converters/jobs",
            json={
                "source_format": "powerbi",
                "target_format": "dax",
                "configuration": {"option1": "value1"}
            }
        )

        assert response.status_code == 201
        data = response.json()
        assert data["status"] == "pending"

    def test_get_job_success(self, client, mock_converter_service):
        """Test successful job retrieval."""
        mock_response = MockJobResponse(id="job-123")
        mock_converter_service.get_job.return_value = mock_response

        response = client.get("/api/converters/jobs/job-123")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "job-123"

    def test_get_job_not_found(self, client, mock_converter_service):
        """Test job retrieval when not found."""
        from fastapi import HTTPException
        mock_converter_service.get_job.side_effect = HTTPException(
            status_code=404,
            detail="Conversion job nonexistent not found"
        )

        response = client.get("/api/converters/jobs/nonexistent")

        assert response.status_code == 404

    def test_update_job_success(self, client, mock_converter_service):
        """Test successful job update."""
        mock_response = MockJobResponse(id="job-123", status="running")
        mock_converter_service.update_job.return_value = mock_response

        response = client.patch(
            "/api/converters/jobs/job-123",
            json={"status": "running"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "running"

    def test_update_job_status_success(self, client, mock_converter_service):
        """Test successful job status update."""
        mock_response = MockJobResponse(id="job-123", status="running")
        mock_converter_service.update_job_status.return_value = mock_response

        response = client.patch(
            "/api/converters/jobs/job-123/status",
            json={"status": "running", "progress": 0.5}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "running"

    def test_list_jobs_success(self, client, mock_converter_service):
        """Test successful job listing."""
        mock_list = MagicMock()
        mock_list.jobs = [MockJobResponse(), MockJobResponse()]
        mock_list.count = 2
        mock_list.model_dump.return_value = {
            "jobs": [j.model_dump() for j in mock_list.jobs],
            "count": 2
        }
        mock_converter_service.list_jobs.return_value = mock_list

        response = client.get("/api/converters/jobs")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2

    def test_list_jobs_with_status_filter(self, client, mock_converter_service):
        """Test job listing with status filter."""
        mock_list = MagicMock()
        mock_list.jobs = [MockJobResponse(status="running")]
        mock_list.count = 1
        mock_list.model_dump.return_value = {
            "jobs": [j.model_dump() for j in mock_list.jobs],
            "count": 1
        }
        mock_converter_service.list_jobs.return_value = mock_list

        response = client.get("/api/converters/jobs?status=running")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1

    def test_cancel_job_success(self, client, mock_converter_service):
        """Test successful job cancellation."""
        mock_response = MockJobResponse(id="job-123", status="cancelled")
        mock_converter_service.cancel_job.return_value = mock_response

        response = client.post("/api/converters/jobs/job-123/cancel")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "cancelled"


# ===== Saved Configuration Endpoint Tests =====

class TestSavedConfigurationEndpoints:
    """Test cases for saved configuration endpoints."""

    def test_create_config_success(self, client, mock_converter_service):
        """Test successful configuration creation."""
        mock_response = MockConfigResponse()
        mock_converter_service.create_saved_config.return_value = mock_response

        response = client.post(
            "/api/converters/configs",
            json={
                "name": "My Config",
                "source_format": "powerbi",
                "target_format": "dax",
                "configuration": {"option1": "value1"}
            }
        )

        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "My Config"

    def test_get_config_success(self, client, mock_converter_service):
        """Test successful configuration retrieval."""
        mock_response = MockConfigResponse(id=123)
        mock_converter_service.get_saved_config.return_value = mock_response

        response = client.get("/api/converters/configs/123")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == 123

    def test_get_config_not_found(self, client, mock_converter_service):
        """Test configuration retrieval when not found."""
        from fastapi import HTTPException
        mock_converter_service.get_saved_config.side_effect = HTTPException(
            status_code=404,
            detail="Configuration 999 not found"
        )

        response = client.get("/api/converters/configs/999")

        assert response.status_code == 404

    def test_update_config_success(self, client, mock_converter_service):
        """Test successful configuration update."""
        mock_response = MockConfigResponse(id=123, name="Updated Config")
        mock_converter_service.update_saved_config.return_value = mock_response

        response = client.patch(
            "/api/converters/configs/123",
            json={"name": "Updated Config"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Updated Config"

    def test_delete_config_success(self, client, mock_converter_service):
        """Test successful configuration deletion."""
        mock_converter_service.delete_saved_config.return_value = {
            "message": "Configuration 123 deleted successfully"
        }

        response = client.delete("/api/converters/configs/123")

        assert response.status_code == 200

    def test_list_configs_success(self, client, mock_converter_service):
        """Test successful configuration listing."""
        mock_list = MagicMock()
        mock_list.configurations = [MockConfigResponse(), MockConfigResponse()]
        mock_list.count = 2
        mock_list.model_dump.return_value = {
            "configurations": [c.model_dump() for c in mock_list.configurations],
            "count": 2
        }
        mock_converter_service.list_saved_configs.return_value = mock_list

        response = client.get("/api/converters/configs")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2

    def test_list_configs_with_filters(self, client, mock_converter_service):
        """Test configuration listing with filters."""
        mock_list = MagicMock()
        mock_list.configurations = [MockConfigResponse()]
        mock_list.count = 1
        mock_list.model_dump.return_value = {
            "configurations": [c.model_dump() for c in mock_list.configurations],
            "count": 1
        }
        mock_converter_service.list_saved_configs.return_value = mock_list

        response = client.get(
            "/api/converters/configs?source_format=powerbi&is_public=true&search=PowerBI"
        )

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1

    def test_use_config_success(self, client, mock_converter_service):
        """Test successful config use tracking."""
        mock_response = MockConfigResponse(id=123)
        mock_converter_service.use_saved_config.return_value = mock_response

        response = client.post("/api/converters/configs/123/use")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == 123


# ===== Health Check Endpoint Test =====

class TestHealthCheckEndpoint:
    """Test cases for health check endpoint."""

    def test_health_check_success(self, client):
        """Test successful health check."""
        response = client.get("/api/converters/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "converter"
        assert "version" in data
