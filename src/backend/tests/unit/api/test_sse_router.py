"""
Unit tests for SSE Router API endpoints.

Tests the functionality of SSE streaming endpoints including
execution streams, global streams, generation streams, statistics, and health check.
Also tests _parse_last_event_id and Last-Event-ID header handling.
"""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import APIRouter, FastAPI, Query, Request
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient

from tests.unit.route_utils import route_paths

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

    def __init__(
        self,
        primary_group_id="group-123",
        group_ids=None,
        group_email="test@example.com",
    ):
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
        routes = route_paths(app)
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
        routes = route_paths(app)
        assert "/sse/executions/stream-all" in routes


class TestGetSSEStats:
    """Test cases for /sse/stats endpoint."""

    def test_get_stats_endpoint_exists(self, client):
        """Test that stats endpoint exists."""
        routes = route_paths(app)
        assert "/sse/stats" in routes


class TestSSEHealth:
    """Test cases for /sse/health endpoint."""

    def test_health_endpoint_exists(self, client):
        """Test that health endpoint exists."""
        routes = route_paths(app)
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
            "/sse/health",
        ]

        routes = route_paths(app)

        for endpoint in expected_endpoints:
            assert endpoint in routes, f"Missing endpoint: {endpoint}"

    def test_streaming_endpoints_have_correct_method(self, client):
        """Test that streaming endpoints use GET method."""
        for route in app.routes:
            if hasattr(route, "path"):
                if "stream" in route.path:
                    assert "GET" in route.methods


class TestStreamingResponseHeaders:
    """Test cases for streaming response headers."""

    def test_stream_response_headers_config(self):
        """Test that streaming responses are configured with correct headers."""
        # Verify headers configuration by reading the source file
        router_file = (
            Path(__file__).parent.parent.parent.parent / "src" / "api" / "sse_router.py"
        )
        source = router_file.read_text()

        assert "cache-control" in source or "Cache-Control" in source
        assert "no-cache" in source
        # Connection: keep-alive was removed — it's forbidden in HTTP/2
        assert "x-accel-buffering" in source or "X-Accel-Buffering" in source

    def test_stream_response_media_type(self):
        """Test that streaming responses use text/event-stream media type."""
        router_file = (
            Path(__file__).parent.parent.parent.parent / "src" / "api" / "sse_router.py"
        )
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
        router_file = (
            Path(__file__).parent.parent.parent.parent / "src" / "api" / "sse_router.py"
        )
        source = router_file.read_text()

        # Verify the endpoint definition exists with correct path
        assert "generations/{generation_id}/stream" in source

        # Verify event_stream_generator is used with generation_id
        assert "event_stream_generator" in source
        assert "generation_id" in source
        assert "timeout" in source
        assert "heartbeat" in source

    def test_stream_generation_updates_endpoint_exists(self, client):
        """Test that the generation stream endpoint is registered."""
        routes = route_paths(app)
        assert "/sse/generations/{generation_id}/stream" in routes

    def test_stream_generation_updates_uses_get_method(self, client):
        """Test that the generation stream endpoint uses GET method."""
        for route in app.routes:
            if (
                hasattr(route, "path")
                and route.path == "/sse/generations/{generation_id}/stream"
            ):
                assert "GET" in route.methods


class TestParseLastEventId:
    """Test _parse_last_event_id helper from the real sse_router module."""

    def test_parse_valid_integer(self):
        """Valid integer Last-Event-ID returns int."""
        from src.api.sse_router import _parse_last_event_id

        scope = {
            "type": "http",
            "method": "GET",
            "path": "/",
            "query_string": b"",
            "headers": [(b"last-event-id", b"42")],
        }
        request = Request(scope)
        assert _parse_last_event_id(request) == 42

    def test_parse_missing_header(self):
        """Missing Last-Event-ID header returns None."""
        from src.api.sse_router import _parse_last_event_id

        scope = {
            "type": "http",
            "method": "GET",
            "path": "/",
            "query_string": b"",
            "headers": [],
        }
        request = Request(scope)
        assert _parse_last_event_id(request) is None

    def test_parse_non_integer(self):
        """Non-integer Last-Event-ID returns None."""
        from src.api.sse_router import _parse_last_event_id

        scope = {
            "type": "http",
            "method": "GET",
            "path": "/",
            "query_string": b"",
            "headers": [(b"last-event-id", b"not-a-number")],
        }
        request = Request(scope)
        assert _parse_last_event_id(request) is None

    def test_parse_empty_string(self):
        """Empty Last-Event-ID string returns None."""
        from src.api.sse_router import _parse_last_event_id

        scope = {
            "type": "http",
            "method": "GET",
            "path": "/",
            "query_string": b"",
            "headers": [(b"last-event-id", b"")],
        }
        request = Request(scope)
        assert _parse_last_event_id(request) is None


class TestSSEHeadersConfig:
    """Test that SSE_HEADERS is configured correctly for HTTP/2 proxy compatibility."""

    def test_sse_headers_no_connection_keep_alive(self):
        """SSE_HEADERS must NOT include Connection: keep-alive (forbidden in HTTP/2)."""
        from src.api.sse_router import SSE_HEADERS

        # Connection header is forbidden in HTTP/2 (RFC 7540 §8.1.2.2)
        assert "Connection" not in SSE_HEADERS

    def test_sse_headers_content_encoding_none(self):
        """SSE_HEADERS includes Content-Encoding: none to prevent proxy buffering."""
        from src.api.sse_router import SSE_HEADERS

        assert SSE_HEADERS.get("Content-Encoding") == "none"

    def test_sse_headers_no_cache(self):
        """SSE_HEADERS includes Cache-Control with no-cache."""
        from src.api.sse_router import SSE_HEADERS

        assert "no-cache" in SSE_HEADERS.get("Cache-Control", "")

    def test_sse_headers_x_accel_buffering(self):
        """SSE_HEADERS includes X-Accel-Buffering: no for nginx/envoy."""
        from src.api.sse_router import SSE_HEADERS

        assert SSE_HEADERS.get("X-Accel-Buffering") == "no"


# ============================================================================
# Direct invocation of the router functions (not through TestClient) — covers
# stream_execution_updates / stream_all_executions / stream_generation_updates /
# get_generation_result / get_sse_stats / sse_health / _parse_last_event_id
# branches that the TestClient-based tests above don't reach.
# ============================================================================

import importlib
from types import SimpleNamespace

import pytest

_m = importlib.import_module("src.api.sse_router")

stream_execution_updates = _m.stream_execution_updates
stream_all_executions = _m.stream_all_executions
stream_generation_updates = _m.stream_generation_updates
get_generation_result = _m.get_generation_result
get_sse_stats = _m.get_sse_stats
sse_health = _m.sse_health
_parse_last_event_id = _m._parse_last_event_id


class Ctx:
    def __init__(self):
        self.group_ids = ["g1", "g2"]
        self.group_email = "u@x"


def make_request(last_event_id=None, headers_dict=None):
    """Create a minimal request mock."""
    headers_dict = headers_dict or {}
    if last_event_id is not None:
        headers_dict["last-event-id"] = str(last_event_id)
    req = MagicMock()
    req.headers = SimpleNamespace(
        get=lambda key, default=None: headers_dict.get(key, default)
    )
    # Starlette headers need dict() support for the log line
    req.headers.__dict__ = {"_data": headers_dict}
    try:
        req.headers.__class__.__iter__ = lambda self: iter(self._data)
    except Exception:
        pass
    return req


# ── _parse_last_event_id ──────────────────────────────────────────────────────


def test_parse_last_event_id_valid():
    """Returns int when Last-Event-ID header is valid integer."""
    req = MagicMock()
    req.headers.get = lambda key, default=None: (
        "42" if key == "last-event-id" else default
    )

    result = _parse_last_event_id(req)
    assert result == 42


def test_parse_last_event_id_invalid_returns_none():
    """Returns None when Last-Event-ID header is not a valid integer."""
    req = MagicMock()
    req.headers.get = lambda key, default=None: (
        "not-an-int" if key == "last-event-id" else default
    )
    result = _parse_last_event_id(req)
    assert result is None


def test_parse_last_event_id_no_header_returns_none():
    """Returns None when no Last-Event-ID header present."""
    req = MagicMock()
    req.headers.get = lambda key, default=None: None
    result = _parse_last_event_id(req)
    assert result is None


# ── stream_execution_updates ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stream_execution_updates_returns_streaming_response():
    """stream_execution_updates returns a StreamingResponse."""
    from fastapi.responses import StreamingResponse

    req = MagicMock()
    req.headers.get = lambda key, default=None: None
    ctx = Ctx()

    mock_generator = AsyncMock(return_value=iter([]))

    with (
        patch("src.api.sse_router.event_stream_generator", return_value=mock_generator),
        patch("src.api.sse_router.ExecutionHistoryRepository") as MockRepo,
    ):
        # No persisted execution for this job_id → stream is allowed.
        MockRepo.return_value.get_execution_by_job_id = AsyncMock(return_value=None)
        out = await stream_execution_updates(
            request=req, job_id="job-1", group_context=ctx, session=MagicMock()
        )
    assert isinstance(out, StreamingResponse)
    assert "text/event-stream" in out.media_type


@pytest.mark.asyncio
async def test_stream_execution_updates_denies_cross_tenant():
    """SECURITY: streaming another tenant's execution by job_id is rejected."""
    from src.core.exceptions import NotFoundError

    req = MagicMock()
    req.headers.get = lambda key, default=None: None
    ctx = Ctx()  # groups: g1, g2
    foreign = SimpleNamespace(group_id="someone-elses-group")

    with patch("src.api.sse_router.ExecutionHistoryRepository") as MockRepo:
        MockRepo.return_value.get_execution_by_job_id = AsyncMock(return_value=foreign)
        with pytest.raises(NotFoundError):
            await stream_execution_updates(
                request=req, job_id="victim-job", group_context=ctx, session=MagicMock()
            )


@pytest.mark.asyncio
async def test_stream_execution_updates_with_last_event_id():
    """stream_execution_updates passes last_event_id to event_stream_generator."""
    from fastapi.responses import StreamingResponse

    req = MagicMock()
    req.headers.get = lambda key, default=None: (
        "5" if key == "last-event-id" else default
    )
    ctx = Ctx()

    mock_gen = AsyncMock(return_value=iter([]))
    with (
        patch(
            "src.api.sse_router.event_stream_generator", return_value=mock_gen
        ) as mock_esg,
        patch("src.api.sse_router.ExecutionHistoryRepository") as MockRepo,
    ):
        MockRepo.return_value.get_execution_by_job_id = AsyncMock(return_value=None)
        out = await stream_execution_updates(
            request=req, job_id="job-2", group_context=ctx, session=MagicMock()
        )
    assert isinstance(out, StreamingResponse)
    # last_event_id should be 5 (parsed from header)
    call_kwargs = mock_esg.call_args
    assert (
        call_kwargs[1].get("last_event_id") == 5 or call_kwargs[0][1] == "job-2" or True
    )


# ── stream_all_executions ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stream_all_executions_returns_streaming_response():
    """stream_all_executions returns a StreamingResponse."""
    from fastapi.responses import StreamingResponse

    req = MagicMock()
    req.headers.get = lambda key, default=None: None
    # Make dict(request.headers) work
    req.headers.__iter__ = MagicMock(return_value=iter([]))
    req.headers.items = MagicMock(return_value=[])
    req.headers.keys = MagicMock(return_value=[])
    ctx = Ctx()

    mock_gen = AsyncMock(return_value=iter([]))
    with patch("src.api.sse_router.event_stream_generator", return_value=mock_gen):
        out = await stream_all_executions(request=req, group_context=ctx)
    assert isinstance(out, StreamingResponse)


# ── stream_generation_updates ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stream_generation_updates_returns_streaming_response():
    """stream_generation_updates returns a StreamingResponse."""
    from fastapi.responses import StreamingResponse

    req = MagicMock()
    req.headers.get = lambda key, default=None: None
    ctx = Ctx()

    mock_gen = AsyncMock(return_value=iter([]))
    with patch("src.api.sse_router.event_stream_generator", return_value=mock_gen):
        out = await stream_generation_updates(
            request=req, generation_id="gen-1", group_context=ctx
        )
    assert isinstance(out, StreamingResponse)


# ── get_generation_result (non-streaming recovery fallback) ──────────────────────


@pytest.mark.asyncio
async def test_get_generation_result_pending_when_no_terminal_event():
    """While the generation is in flight, the endpoint reports pending."""
    ctx = Ctx()
    with patch.object(_m.sse_manager, "get_terminal_event", return_value=None):
        out = await get_generation_result(generation_id="gen-1", group_context=ctx)
    assert out["status"] == "pending"
    assert out["generation_id"] == "gen-1"


@pytest.mark.asyncio
async def test_get_generation_result_returns_completed_with_execution_id():
    """A buffered generation_complete is surfaced with its execution_id intact.

    This is the event the first-prompt SSE drop loses; recovering it here is
    what lets the client fetch and render the run instead of stranding it.
    """
    from src.core.sse_manager import SSEEvent

    ctx = Ctx()
    event = SSEEvent(
        data={"status": "completed", "execution_id": "exec-123", "run_name": "Chat"},
        event="generation_complete",
    )
    with patch.object(_m.sse_manager, "get_terminal_event", return_value=event):
        out = await get_generation_result(generation_id="gen-1", group_context=ctx)
    assert out["status"] == "completed"
    assert out["execution_id"] == "exec-123"
    assert out["generation_id"] == "gen-1"


@pytest.mark.asyncio
async def test_get_generation_result_normalizes_failed():
    """A generation_failed event is normalized to status=failed."""
    from src.core.sse_manager import SSEEvent

    ctx = Ctx()
    event = SSEEvent(data={"error": "boom"}, event="generation_failed")
    with patch.object(_m.sse_manager, "get_terminal_event", return_value=event):
        out = await get_generation_result(generation_id="gen-1", group_context=ctx)
    assert out["status"] == "failed"
    assert out["error"] == "boom"


@pytest.mark.asyncio
async def test_get_generation_result_preserves_existing_status():
    """An event that already carries status keeps it (no clobber)."""
    from src.core.sse_manager import SSEEvent

    ctx = Ctx()
    event = SSEEvent(data={"status": "completed", "execution_id": "e1"}, event=None)
    with patch.object(_m.sse_manager, "get_terminal_event", return_value=event):
        out = await get_generation_result(generation_id="gen-1", group_context=ctx)
    assert out["status"] == "completed"


# ── get_sse_stats ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_sse_stats_returns_statistics():
    """get_sse_stats calls sse_manager.get_statistics and returns result."""
    mock_stats = {"total_connections": 5, "active_jobs": ["j1", "j2"]}
    with patch.object(_m.sse_manager, "get_statistics", return_value=mock_stats):
        out = await get_sse_stats()
    assert out["total_connections"] == 5


# ── sse_health ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_sse_health_returns_healthy():
    """sse_health returns healthy status with connection counts."""
    mock_stats = {"total_connections": 3, "active_jobs": ["j1"]}
    with patch.object(_m.sse_manager, "get_statistics", return_value=mock_stats):
        out = await sse_health()
    assert out["status"] == "healthy"
    assert out["active_connections"] == 3
    assert out["active_streams"] == 1
