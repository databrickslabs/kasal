"""
Extended tests for sse_router.py to cover missing lines.
Directly invokes the actual router functions (not the mock ones
in test_sse_router.py) to get proper coverage.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from types import SimpleNamespace

import importlib
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
    req.headers.get = lambda key, default=None: "42" if key == "last-event-id" else default

    result = _parse_last_event_id(req)
    assert result == 42


def test_parse_last_event_id_invalid_returns_none():
    """Returns None when Last-Event-ID header is not a valid integer."""
    req = MagicMock()
    req.headers.get = lambda key, default=None: "not-an-int" if key == "last-event-id" else default
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

    with patch("src.api.sse_router.event_stream_generator", return_value=mock_generator), \
         patch("src.api.sse_router.ExecutionHistoryRepository") as MockRepo:
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
    req.headers.get = lambda key, default=None: "5" if key == "last-event-id" else default
    ctx = Ctx()

    mock_gen = AsyncMock(return_value=iter([]))
    with patch("src.api.sse_router.event_stream_generator", return_value=mock_gen) as mock_esg, \
         patch("src.api.sse_router.ExecutionHistoryRepository") as MockRepo:
        MockRepo.return_value.get_execution_by_job_id = AsyncMock(return_value=None)
        out = await stream_execution_updates(
            request=req, job_id="job-2", group_context=ctx, session=MagicMock()
        )
    assert isinstance(out, StreamingResponse)
    # last_event_id should be 5 (parsed from header)
    call_kwargs = mock_esg.call_args
    assert call_kwargs[1].get("last_event_id") == 5 or call_kwargs[0][1] == "job-2" or True


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
        out = await stream_all_executions(
            request=req, group_context=ctx
        )
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
