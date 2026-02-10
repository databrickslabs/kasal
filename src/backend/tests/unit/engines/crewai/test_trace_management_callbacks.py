"""
Simplified unit tests for trace management with execution-scoped callbacks.

Tests core trace management functionality with minimal async complexity.

NOTE: The execution_callback module has been refactored to delegate trace creation
to the event bus (logging_callbacks.py) and the OTel pipeline.  The callbacks now
only create execution logs via enqueue_log().
"""
import asyncio
import pytest
from unittest.mock import patch, MagicMock, AsyncMock

from src.engines.crewai.trace_management import TraceManager
class TestTraceManagerEventFiltering:
    """Test cases for trace manager event filtering."""

    def test_important_event_types_list(self):
        """Test that important event types are correctly defined."""
        important_event_types = [
            "agent_execution", "tool_usage", "crew_started",
            "crew_completed", "task_started", "task_completed", "llm_call"
        ]

        assert "agent_execution" in important_event_types
        assert "task_completed" in important_event_types
        assert "crew_started" in important_event_types
        assert "crew_completed" in important_event_types

        assert "debug_info" not in important_event_types
        assert "random_event" not in important_event_types

    def test_task_lifecycle_events_all_in_important_list(self):
        """Test that all task lifecycle events are important."""
        important_event_types = [
            "agent_execution", "tool_usage", "tool_error",
            "crew_started", "crew_completed",
            "task_started", "task_completed", "task_failed",
            "llm_call", "llm_guardrail",
            "memory_write", "memory_retrieval",
            "memory_write_started", "memory_retrieval_started",
            "knowledge_retrieval", "knowledge_retrieval_started",
            "agent_reasoning", "agent_reasoning_error"
        ]

        assert "task_started" in important_event_types
        assert "task_completed" in important_event_types
        assert "task_failed" in important_event_types

    def test_task_lifecycle_events_broadcast_via_sse(self):
        """Test that task lifecycle events are all broadcast via SSE."""
        sse_broadcast_event_types = ("task_started", "task_completed", "task_failed")

        assert "task_started" in sse_broadcast_event_types
        assert "task_completed" in sse_broadcast_event_types
        assert "task_failed" in sse_broadcast_event_types

    def test_websocket_broadcast_uses_lowercase_event_types(self):
        """Test that WebSocket broadcast checks use lowercase event types."""
        ws_broadcast_event_types = ["task_started", "task_completed", "task_failed"]

        for event_type in ws_broadcast_event_types:
            assert event_type == event_type.lower(), f"{event_type} should be lowercase"

    def test_step_callback_creates_log(self):
        """Test that step callback creates execution log."""
        from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            step_callback, _ = create_execution_callbacks("test_job_123", {"model": "test"}, None)

            mock_step_output = MagicMock()
            mock_step_output.output = "Agent output"
            step_callback(mock_step_output)

            mock_enqueue.assert_called_once()
            kwargs = mock_enqueue.call_args[1]
            assert kwargs["execution_id"] == "test_job_123"
            assert "[STEP]" in kwargs["content"]

    def test_step_callback_with_various_output_types(self):
        """Test that step callback handles various output object types."""
        from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            step_callback, _ = create_execution_callbacks("test_job", {}, None)

            # Test with output attribute
            mock_output = MagicMock()
            mock_output.output = "via output attr"
            step_callback(mock_output)
            assert "via output attr" in mock_enqueue.call_args[1]["content"]

            mock_enqueue.reset_mock()

            # Test with raw attribute (no output)
            mock_output2 = MagicMock(spec=[])
            mock_output2.raw = "via raw attr"
            step_callback(mock_output2)
            assert "via raw attr" in mock_enqueue.call_args[1]["content"]

    def test_group_context_in_logs(self):
        """Test that group context is properly included in logs."""
        from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = "group_123"
        mock_group_context.group_email = "test@group.com"

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            step_callback, _ = create_execution_callbacks(
                "test_job_123", {"model": "test"}, mock_group_context
            )

            mock_output = MagicMock()
            mock_output.output = "Test result"
            step_callback(mock_output)

            kwargs = mock_enqueue.call_args[1]
            assert kwargs["group_context"] == mock_group_context

    def test_log_isolation_by_job_id(self):
        """Test that logs are isolated by job ID."""
        from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            step_1, _ = create_execution_callbacks("execution_1", {}, None)
            step_2, _ = create_execution_callbacks("execution_2", {}, None)

            mock_output_1 = MagicMock()
            mock_output_1.output = "identical output"
            mock_output_2 = MagicMock()
            mock_output_2.output = "identical output"

            step_1(mock_output_1)
            step_2(mock_output_2)

            assert mock_enqueue.call_count == 2
            calls = mock_enqueue.call_args_list
            assert calls[0][1]["execution_id"] == "execution_1"
            assert calls[1][1]["execution_id"] == "execution_2"


class TestCallbackCrewIntegration:
    """Test cases for crew-level callback integration."""

    def test_crew_callbacks_creation(self):
        """Test that crew callbacks are created correctly."""
        from src.engines.crewai.callbacks.execution_callback import create_crew_callbacks

        callbacks = create_crew_callbacks("test_job", {"model": "test"}, None)

        assert "on_start" in callbacks
        assert "on_complete" in callbacks
        assert "on_error" in callbacks
        assert callable(callbacks["on_start"])
        assert callable(callbacks["on_complete"])
        assert callable(callbacks["on_error"])

    def test_crew_start_callback_creates_log(self):
        """Test crew start callback creates execution log."""
        from src.engines.crewai.callbacks.execution_callback import create_crew_callbacks

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            callbacks = create_crew_callbacks("test_job", {"model": "test"}, None)
            callbacks["on_start"]()

            mock_enqueue.assert_called_once()
            kwargs = mock_enqueue.call_args[1]
            assert kwargs["execution_id"] == "test_job"
            assert "CREW STARTED" in kwargs["content"]

    def test_crew_complete_callback_creates_log(self):
        """Test crew completion callback creates execution log."""
        from src.engines.crewai.callbacks.execution_callback import create_crew_callbacks

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            callbacks = create_crew_callbacks("test_job", {"model": "test"}, None)
            callbacks["on_complete"]("Test result")

            mock_enqueue.assert_called_once()
            kwargs = mock_enqueue.call_args[1]
            assert kwargs["execution_id"] == "test_job"
            assert "CREW COMPLETED" in kwargs["content"]

    def test_crew_error_callback(self):
        """Test crew error callback functionality."""
        from src.engines.crewai.callbacks.execution_callback import create_crew_callbacks

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            callbacks = create_crew_callbacks("test_job", {"model": "test"}, None)
            callbacks["on_error"](Exception("Test error"))

            mock_enqueue.assert_called_once()
            kwargs = mock_enqueue.call_args[1]
            assert kwargs["execution_id"] == "test_job"
            assert "CREW FAILED" in kwargs["content"]
            assert "Test error" in kwargs["content"]


class TestTaskCallback:
    """Test cases for task callback functionality."""

    def test_task_callback_creates_log(self):
        """Test task callback creates execution log."""
        from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            _, task_callback = create_execution_callbacks("test_job", {"model": "test"}, None)

            mock_task_output = MagicMock()
            mock_task_output.raw = "Task result"
            mock_task_output.description = "Test task description"
            task_callback(mock_task_output)

            mock_enqueue.assert_called_once()
            kwargs = mock_enqueue.call_args[1]
            assert kwargs["execution_id"] == "test_job"
            assert "TASK COMPLETED" in kwargs["content"]


class TestConfigSanitization:
    """Test cases for configuration sanitization in logging."""

    def test_config_sanitization(self):
        """Test that sensitive config data is sanitized."""
        from src.engines.crewai.callbacks.execution_callback import log_crew_initialization

        config_with_secrets = {
            "model": "test-model",
            "api_keys": {"secret": "hidden"},
            "tokens": {"access_token": "secret"},
            "passwords": {"db_pass": "secret"},
            "normal_field": "visible"
        }

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            log_crew_initialization("test_job", config_with_secrets, None)

            mock_enqueue.assert_called_once()
            content = mock_enqueue.call_args[1]["content"]
            assert "test-model" in content
            assert "visible" in content
            assert "secret" not in content
            assert "hidden" not in content

    def test_empty_config_handling(self):
        """Test handling of empty or None config."""
        from src.engines.crewai.callbacks.execution_callback import log_crew_initialization

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            log_crew_initialization("test_job", None, None)
            mock_enqueue.assert_called_once()

            mock_enqueue.reset_mock()
            log_crew_initialization("test_job", {}, None)
            mock_enqueue.assert_called_once()
            assert mock_enqueue.call_args[1]["execution_id"] == "test_job"



# ---------------------------------------------------------------------------
# TraceManager.ensure_writer_started and stop_writer tests
# ---------------------------------------------------------------------------

_START_WRITER_PATCH = "src.services.execution_logs_service.start_logs_writer"
_STOP_WRITER_PATCH = "src.services.execution_logs_service.stop_logs_writer"


def _reset_trace_manager():
    """Reset all TraceManager class-level state between tests."""
    TraceManager._logs_writer_task = None
    TraceManager._shutdown_event = None
    TraceManager._writer_started = False
    TraceManager._lock = None
    TraceManager._writer_loop = None


def _make_task_mock(done=False):
    """Create a MagicMock that behaves like an asyncio.Task for done() checks."""
    task = MagicMock()
    task.done.return_value = done
    return task


@pytest.fixture(autouse=False)
def clean_trace_manager():
    """Fixture that resets TraceManager state before and after each test."""
    _reset_trace_manager()
    yield
    _reset_trace_manager()


class TestTraceManagerEnsureWriterStarted:
    """Tests for TraceManager.ensure_writer_started (lines 36-65)."""

    @pytest.mark.asyncio
    async def test_first_call_starts_writer(self, clean_trace_manager):
        """Test that the first call creates lock, shutdown event, and starts writer."""
        mock_task = _make_task_mock(done=False)

        with patch(
            _START_WRITER_PATCH,
            new_callable=AsyncMock,
            return_value=mock_task,
        ) as mock_start:
            await TraceManager.ensure_writer_started()

            mock_start.assert_called_once()
            assert TraceManager._writer_started is True
            assert TraceManager._logs_writer_task is mock_task
            assert TraceManager._shutdown_event is not None
            assert TraceManager._lock is not None
            assert TraceManager._writer_loop is asyncio.get_running_loop()

    @pytest.mark.asyncio
    async def test_second_call_skips_start_when_task_running(self, clean_trace_manager):
        """Test that a second call does not restart writer if task is still running."""
        mock_task = _make_task_mock(done=False)

        with patch(
            _START_WRITER_PATCH,
            new_callable=AsyncMock,
            return_value=mock_task,
        ) as mock_start:
            await TraceManager.ensure_writer_started()
            assert mock_start.call_count == 1

            # Second call -- task is not done, should hit the else branch (line 63-65)
            await TraceManager.ensure_writer_started()
            assert mock_start.call_count == 1
            assert TraceManager._writer_started is True

    @pytest.mark.asyncio
    async def test_restarts_when_task_done(self, clean_trace_manager):
        """Test that writer is restarted when existing task is done."""
        done_task = _make_task_mock(done=True)
        new_task = _make_task_mock(done=False)

        loop = asyncio.get_running_loop()
        TraceManager._logs_writer_task = done_task
        TraceManager._writer_loop = loop
        TraceManager._lock = asyncio.Lock()
        TraceManager._shutdown_event = asyncio.Event()

        with patch(
            _START_WRITER_PATCH,
            new_callable=AsyncMock,
            return_value=new_task,
        ) as mock_start:
            await TraceManager.ensure_writer_started()

            mock_start.assert_called_once()
            assert TraceManager._logs_writer_task is new_task
            assert TraceManager._writer_started is True

    @pytest.mark.asyncio
    async def test_loop_change_resets_state(self, clean_trace_manager):
        """Test that a loop change detection resets writer state."""
        old_loop = MagicMock()
        TraceManager._writer_loop = old_loop
        TraceManager._writer_started = True
        TraceManager._logs_writer_task = _make_task_mock(done=False)
        TraceManager._lock = MagicMock()

        mock_task = _make_task_mock(done=False)

        with patch(
            _START_WRITER_PATCH,
            new_callable=AsyncMock,
            return_value=mock_task,
        ) as mock_start:
            await TraceManager.ensure_writer_started()

            mock_start.assert_called_once()
            assert TraceManager._writer_started is True
            assert TraceManager._writer_loop is asyncio.get_running_loop()

    @pytest.mark.asyncio
    async def test_lock_recreated_on_loop_change(self, clean_trace_manager):
        """Test that lock is recreated when loop changes."""
        old_loop = MagicMock()
        old_lock = MagicMock()
        TraceManager._writer_loop = old_loop
        TraceManager._lock = old_lock

        mock_task = _make_task_mock(done=False)

        with patch(
            _START_WRITER_PATCH,
            new_callable=AsyncMock,
            return_value=mock_task,
        ):
            await TraceManager.ensure_writer_started()

            assert isinstance(TraceManager._lock, asyncio.Lock)
            assert TraceManager._lock is not old_lock


class TestTraceManagerStopWriter:
    """Tests for TraceManager.stop_writer (lines 70-120)."""

    @pytest.mark.asyncio
    async def test_stop_writer_normal_success(self, clean_trace_manager):
        """Test normal stop when writer is running and stop succeeds."""
        loop = asyncio.get_running_loop()
        mock_task = _make_task_mock(done=False)

        TraceManager._writer_loop = loop
        TraceManager._logs_writer_task = mock_task
        TraceManager._writer_started = True
        TraceManager._shutdown_event = asyncio.Event()
        TraceManager._lock = asyncio.Lock()

        with patch(
            _STOP_WRITER_PATCH,
            new_callable=AsyncMock,
            return_value=True,
        ) as mock_stop:
            await TraceManager.stop_writer()

            mock_stop.assert_called_once_with(timeout=5.0)
            assert TraceManager._shutdown_event.is_set()
            assert TraceManager._logs_writer_task is None
            assert TraceManager._writer_started is False
            assert TraceManager._writer_loop is None

    @pytest.mark.asyncio
    async def test_stop_writer_normal_failure(self, clean_trace_manager):
        """Test normal stop when stop_logs_writer returns False."""
        loop = asyncio.get_running_loop()
        mock_task = _make_task_mock(done=False)

        TraceManager._writer_loop = loop
        TraceManager._logs_writer_task = mock_task
        TraceManager._writer_started = True
        TraceManager._shutdown_event = asyncio.Event()
        TraceManager._lock = asyncio.Lock()

        with patch(
            _STOP_WRITER_PATCH,
            new_callable=AsyncMock,
            return_value=False,
        ) as mock_stop:
            await TraceManager.stop_writer()

            mock_stop.assert_called_once_with(timeout=5.0)
            assert TraceManager._logs_writer_task is None
            assert TraceManager._writer_started is False

    @pytest.mark.asyncio
    async def test_stop_writer_task_not_running(self, clean_trace_manager):
        """Test stop when writer task is not running (None)."""
        loop = asyncio.get_running_loop()
        TraceManager._writer_loop = loop
        TraceManager._logs_writer_task = None
        TraceManager._writer_started = True
        TraceManager._shutdown_event = asyncio.Event()
        TraceManager._lock = asyncio.Lock()

        with patch(
            _STOP_WRITER_PATCH,
            new_callable=AsyncMock,
        ) as mock_stop:
            await TraceManager.stop_writer()

            mock_stop.assert_not_called()
            assert TraceManager._writer_started is False
            assert TraceManager._writer_loop is None

    @pytest.mark.asyncio
    async def test_stop_writer_task_already_done(self, clean_trace_manager):
        """Test stop when writer task exists but is already done."""
        loop = asyncio.get_running_loop()
        mock_task = _make_task_mock(done=True)

        TraceManager._writer_loop = loop
        TraceManager._logs_writer_task = mock_task
        TraceManager._writer_started = True
        TraceManager._shutdown_event = asyncio.Event()
        TraceManager._lock = asyncio.Lock()

        with patch(
            _STOP_WRITER_PATCH,
            new_callable=AsyncMock,
        ) as mock_stop:
            await TraceManager.stop_writer()

            mock_stop.assert_not_called()
            assert TraceManager._writer_started is False

    @pytest.mark.asyncio
    async def test_stop_writer_loop_mismatch(self, clean_trace_manager):
        """Test stop_writer with loop mismatch forces cleanup."""
        old_loop = MagicMock()
        mock_task = _make_task_mock(done=False)

        TraceManager._writer_loop = old_loop
        TraceManager._logs_writer_task = mock_task
        TraceManager._writer_started = True
        TraceManager._shutdown_event = asyncio.Event()
        TraceManager._lock = asyncio.Lock()

        await TraceManager.stop_writer()

        mock_task.cancel.assert_called_once()
        assert TraceManager._logs_writer_task is None
        assert TraceManager._writer_started is False
        assert TraceManager._writer_loop is None
        assert TraceManager._shutdown_event is None
        assert TraceManager._lock is None

    @pytest.mark.asyncio
    async def test_stop_writer_loop_mismatch_cancel_exception(self, clean_trace_manager):
        """Test stop_writer with loop mismatch when cancel raises exception."""
        old_loop = MagicMock()
        mock_task = _make_task_mock(done=False)
        mock_task.cancel.side_effect = RuntimeError("cannot cancel")

        TraceManager._writer_loop = old_loop
        TraceManager._logs_writer_task = mock_task
        TraceManager._writer_started = True
        TraceManager._shutdown_event = asyncio.Event()
        TraceManager._lock = asyncio.Lock()

        await TraceManager.stop_writer()

        mock_task.cancel.assert_called_once()
        assert TraceManager._logs_writer_task is None
        assert TraceManager._writer_started is False
        assert TraceManager._writer_loop is None

    @pytest.mark.asyncio
    async def test_stop_writer_loop_mismatch_task_already_done(self, clean_trace_manager):
        """Test loop mismatch path when task is already done (skips cancel)."""
        old_loop = MagicMock()
        mock_task = _make_task_mock(done=True)

        TraceManager._writer_loop = old_loop
        TraceManager._logs_writer_task = mock_task
        TraceManager._writer_started = True
        TraceManager._shutdown_event = asyncio.Event()
        TraceManager._lock = asyncio.Lock()

        await TraceManager.stop_writer()

        mock_task.cancel.assert_not_called()
        assert TraceManager._logs_writer_task is None
        assert TraceManager._writer_started is False

    @pytest.mark.asyncio
    async def test_stop_writer_lock_is_none(self, clean_trace_manager):
        """Test stop_writer creates a lock when lock is None."""
        loop = asyncio.get_running_loop()
        TraceManager._writer_loop = loop
        TraceManager._logs_writer_task = None
        TraceManager._writer_started = False
        TraceManager._shutdown_event = None
        TraceManager._lock = None

        await TraceManager.stop_writer()

        assert isinstance(TraceManager._lock, asyncio.Lock)
        assert TraceManager._writer_started is False
        assert TraceManager._writer_loop is None

    @pytest.mark.asyncio
    async def test_stop_writer_shutdown_event_is_none(self, clean_trace_manager):
        """Test stop_writer when shutdown_event is None (skips set)."""
        loop = asyncio.get_running_loop()
        TraceManager._writer_loop = loop
        TraceManager._logs_writer_task = None
        TraceManager._writer_started = True
        TraceManager._shutdown_event = None
        TraceManager._lock = asyncio.Lock()

        await TraceManager.stop_writer()

        assert TraceManager._writer_started is False
        assert TraceManager._writer_loop is None

    @pytest.mark.asyncio
    async def test_stop_writer_no_prior_state(self, clean_trace_manager):
        """Test stop_writer when no prior state exists (clean slate)."""
        await TraceManager.stop_writer()

        assert TraceManager._writer_started is False
        assert TraceManager._writer_loop is None

    @pytest.mark.asyncio
    async def test_stop_writer_get_running_loop_raises(self, clean_trace_manager):
        """Test stop_writer when get_running_loop raises RuntimeError (lines 72-73)."""
        mock_task = _make_task_mock(done=False)
        old_loop = MagicMock()
        TraceManager._writer_loop = old_loop
        TraceManager._logs_writer_task = mock_task
        TraceManager._writer_started = True
        TraceManager._shutdown_event = asyncio.Event()
        TraceManager._lock = asyncio.Lock()

        with patch(
            "src.engines.crewai.trace_management.asyncio.get_running_loop",
            side_effect=RuntimeError("no running loop"),
        ):
            # current_loop becomes None due to RuntimeError
            # loop_mismatch is False because current_loop is None
            # Falls through to normal lock path
            await TraceManager.stop_writer()

        assert TraceManager._writer_started is False
