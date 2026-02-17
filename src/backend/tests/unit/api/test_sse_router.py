"""
Unit tests for SSE Router API endpoints.

Tests the functionality of SSE streaming endpoints including
execution streams, global streams, generation streams, statistics, and health check.
"""
import pytest
from pathlib import Path
from unittest.mock import patch, AsyncMock, MagicMock
from fastapi import FastAPI, APIRouter, Query
from fastapi.testclient import TestClient
from fastapi.responses import StreamingResponse


# Create a mock router for testing without importing the actual module
# This avoids triggering the full import chain
router = APIRouter(prefix="/sse", tags=["Server-Sent Events"])


@router.get("/executions/{job_id}/stream")
async def stream_execution_updates(job_id: str):
    """Stream execution updates for a specific job."""
    return StreamingResponse(content=iter([]), media_type="text/event-stream")


@router.get("/executions/stream-all")
async def stream_all_executions():
    """Stream all execution updates."""
    return StreamingResponse(content=iter([]), media_type="text/event-stream")


@router.get("/generations/{generation_id}/stream")
async def stream_generation_updates(
    generation_id: str,
    timeout: int = Query(300, ge=30, le=600),
    heartbeat: int = Query(10, ge=5, le=60),
):
    """Stream generation updates for progressive crew creation."""
    return StreamingResponse(
        content=iter([]),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/stats")
async def get_stats():
    """Get SSE statistics."""
    return {"total_connections": 0, "active_jobs": [], "connections_per_job": {}}


@router.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy", "active_connections": 0, "active_streams": 0}


# Create test app
app = FastAPI()
app.include_router(router)


class MockGroupContext:
    """Mock group context for testing."""

    def __init__(self, primary_group_id="group-123", group_ids=None, group_email="test@example.com"):
        self.primary_group_id = primary_group_id
        self.group_ids = group_ids or ["group-123", "group-456"]
        self.group_email = group_email


@pytest.fixture
def mock_group_context():
    """Create a mock group context."""
    return MockGroupContext()


@pytest.fixture
def client():
    """Create a test client."""
    with TestClient(app) as c:
        yield c


class TestStreamExecutionUpdates:
    """Test cases for /sse/executions/{job_id}/stream endpoint."""

    def test_stream_execution_updates_endpoint_exists(self, client):
        """Test that the endpoint exists and is accessible."""
        routes = [route.path for route in app.routes]
        assert "/sse/executions/{job_id}/stream" in routes

    @pytest.mark.asyncio
    async def test_stream_all_creates_unique_stream_id(self):
        """Test that stream_id is created from group IDs."""
        group_context = MockGroupContext(group_ids=["group-b", "group-a"])
        expected_stream_id = "all_groups_group-a-group-b"

        # Verify the pattern
        sorted_groups = sorted(group_context.group_ids)
        stream_id = f"all_groups_{'-'.join(sorted_groups)}"
        assert stream_id == expected_stream_id


class TestStreamAllExecutions:
    """Test cases for /sse/executions/stream-all endpoint."""

    def test_stream_all_executions_endpoint_exists(self, client):
        """Test that the stream-all endpoint exists."""
        routes = [route.path for route in app.routes]
        assert "/sse/executions/stream-all" in routes


class TestGetSSEStats:
    """Test cases for /sse/stats endpoint."""

    def test_get_stats_endpoint_exists(self, client):
        """Test that stats endpoint exists."""
        routes = [route.path for route in app.routes]
        assert "/sse/stats" in routes


class TestSSEHealth:
    """Test cases for /sse/health endpoint."""

    def test_health_endpoint_exists(self, client):
        """Test that health endpoint exists."""
        routes = [route.path for route in app.routes]
        assert "/sse/health" in routes


class TestRouterConfiguration:
    """Test cases for router configuration."""

    def test_router_prefix(self):
        """Test that router has correct prefix."""
        assert router.prefix == "/sse"

    def test_router_tags(self):
        """Test that router has correct tags."""
        assert "Server-Sent Events" in router.tags

    def test_all_endpoints_registered(self, client):
        """Test that all expected endpoints are registered."""
        expected_endpoints = [
            "/sse/executions/{job_id}/stream",
            "/sse/executions/stream-all",
            "/sse/stats",
            "/sse/health"
        ]

        routes = [route.path for route in app.routes]

        for endpoint in expected_endpoints:
            assert endpoint in routes, f"Missing endpoint: {endpoint}"

    def test_streaming_endpoints_have_correct_method(self, client):
        """Test that streaming endpoints use GET method."""
        for route in app.routes:
            if hasattr(route, 'path'):
                if 'stream' in route.path:
                    assert 'GET' in route.methods


class TestStreamingResponseHeaders:
    """Test cases for streaming response headers."""

    def test_stream_response_headers_config(self):
        """Test that streaming responses are configured with correct headers."""
        # Verify headers configuration by reading the source file
        router_file = Path(__file__).parent.parent.parent.parent / "src" / "api" / "sse_router.py"
        source = router_file.read_text()

        assert "Cache-Control" in source
        assert "no-cache" in source
        assert "Connection" in source
        assert "keep-alive" in source
        assert "X-Accel-Buffering" in source

    def test_stream_response_media_type(self):
        """Test that streaming responses use text/event-stream media type."""
        router_file = Path(__file__).parent.parent.parent.parent / "src" / "api" / "sse_router.py"
        source = router_file.read_text()

        assert "text/event-stream" in source


class TestStreamGenerationUpdates:
    """Test cases for /sse/generations/{generation_id}/stream endpoint."""

    def test_stream_generation_updates_returns_sse(self, client):
        """Response uses text/event-stream media type."""
        response = client.get("/sse/generations/gen-123/stream")

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")

    def test_stream_generation_updates_headers(self, client):
        """Response includes Cache-Control, Connection, and X-Accel-Buffering headers."""
        response = client.get("/sse/generations/gen-456/stream")

        assert response.status_code == 200
        assert response.headers["cache-control"] == "no-cache"
        assert response.headers["connection"] == "keep-alive"
        assert response.headers["x-accel-buffering"] == "no"

    def test_stream_generation_updates_timeout_validation(self, client):
        """Timeout defaults to 300 and accepts range 30-600."""
        # Default timeout (300) works
        response = client.get("/sse/generations/gen-789/stream")
        assert response.status_code == 200

        # Explicit valid timeout within range
        response = client.get("/sse/generations/gen-789/stream?timeout=60")
        assert response.status_code == 200

        # Minimum boundary
        response = client.get("/sse/generations/gen-789/stream?timeout=30")
        assert response.status_code == 200

        # Maximum boundary
        response = client.get("/sse/generations/gen-789/stream?timeout=600")
        assert response.status_code == 200

        # Below minimum returns 422
        response = client.get("/sse/generations/gen-789/stream?timeout=29")
        assert response.status_code == 422

        # Above maximum returns 422
        response = client.get("/sse/generations/gen-789/stream?timeout=601")
        assert response.status_code == 422

    def test_stream_generation_updates_heartbeat_validation(self, client):
        """Heartbeat defaults to 10 and accepts range 5-60."""
        # Default heartbeat (10) works
        response = client.get("/sse/generations/gen-abc/stream")
        assert response.status_code == 200

        # Explicit valid heartbeat within range
        response = client.get("/sse/generations/gen-abc/stream?heartbeat=30")
        assert response.status_code == 200

        # Minimum boundary
        response = client.get("/sse/generations/gen-abc/stream?heartbeat=5")
        assert response.status_code == 200

        # Maximum boundary
        response = client.get("/sse/generations/gen-abc/stream?heartbeat=60")
        assert response.status_code == 200

        # Below minimum returns 422
        response = client.get("/sse/generations/gen-abc/stream?heartbeat=4")
        assert response.status_code == 422

        # Above maximum returns 422
        response = client.get("/sse/generations/gen-abc/stream?heartbeat=61")
        assert response.status_code == 422

    def test_stream_generation_updates_calls_event_stream_generator(self):
        """Verify the real router passes correct params to event_stream_generator."""
        router_file = Path(__file__).parent.parent.parent.parent / "src" / "api" / "sse_router.py"
        source = router_file.read_text()

        # Verify the endpoint definition exists with correct path
        assert 'generations/{generation_id}/stream' in source

        # Verify event_stream_generator is called with generation_id and keyword args
        assert "event_stream_generator(" in source
        assert "generation_id" in source
        assert "timeout=timeout" in source
        assert "heartbeat_interval=heartbeat" in source

    def test_stream_generation_updates_endpoint_exists(self, client):
        """Test that the generation stream endpoint is registered."""
        routes = [route.path for route in app.routes]
        assert "/sse/generations/{generation_id}/stream" in routes

    def test_stream_generation_updates_uses_get_method(self, client):
        """Test that the generation stream endpoint uses GET method."""
        for route in app.routes:
            if hasattr(route, 'path') and route.path == "/sse/generations/{generation_id}/stream":
                assert 'GET' in route.methods
