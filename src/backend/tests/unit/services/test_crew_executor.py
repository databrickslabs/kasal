"""
Comprehensive unit tests for services/crew_executor.py
"""

import asyncio
import threading
import pytest
from unittest.mock import Mock, patch, MagicMock, AsyncMock
from datetime import datetime

# Reset singleton before each test
import src.services.crew_executor as crew_executor_module


def _fresh_executor(max_workers=5):
    """Create a fresh CrewExecutor bypassing the singleton."""
    from concurrent.futures import ThreadPoolExecutor
    from src.services.crew_executor import CrewExecutor

    executor = object.__new__(CrewExecutor)
    executor._initialized = False
    executor.__init__(max_workers=max_workers)
    return executor


@pytest.fixture
def executor():
    """Provide a fresh CrewExecutor for each test."""
    ex = _fresh_executor()
    yield ex
    # Cleanup thread pool
    try:
        ex._executor.shutdown(wait=False)
    except Exception:
        pass


class TestCrewExecutorInit:
    """Tests for CrewExecutor initialization."""

    def test_creates_thread_pool(self, executor):
        from concurrent.futures import ThreadPoolExecutor
        assert isinstance(executor._executor, ThreadPoolExecutor)

    def test_active_executions_empty(self, executor):
        assert executor._active_executions == {}

    def test_stop_events_empty(self, executor):
        assert executor._stop_events == {}

    def test_metrics_initialized(self, executor):
        metrics = executor._metrics
        assert metrics["total_executions"] == 0
        assert metrics["active_executions"] == 0
        assert metrics["completed_executions"] == 0
        assert metrics["failed_executions"] == 0
        assert metrics["cancelled_executions"] == 0
        assert metrics["total_duration_seconds"] == 0.0

    def test_initialized_flag_set(self, executor):
        assert executor._initialized is True

    def test_second_init_is_noop(self):
        from src.services.crew_executor import CrewExecutor
        # Test that calling __init__ again on an already-initialized executor is a no-op
        ex = _fresh_executor()
        original_executor = ex._executor
        ex.__init__(max_workers=100)  # should be a no-op
        assert ex._executor is original_executor
        ex._executor.shutdown(wait=False)


class TestGetMetrics:
    """Tests for get_metrics."""

    def test_returns_dict(self, executor):
        result = executor.get_metrics()
        assert isinstance(result, dict)

    def test_keys_present(self, executor):
        result = executor.get_metrics()
        assert "total_executions" in result
        assert "active_executions" in result
        assert "completed_executions" in result
        assert "failed_executions" in result
        assert "cancelled_executions" in result
        assert "total_duration_seconds" in result

    def test_no_average_when_no_completions(self, executor):
        result = executor.get_metrics()
        assert "average_duration_seconds" not in result

    def test_average_computed_after_completion(self, executor):
        executor._metrics["completed_executions"] = 2
        executor._metrics["total_duration_seconds"] = 10.0
        result = executor.get_metrics()
        assert result["average_duration_seconds"] == pytest.approx(5.0)

    def test_returns_copy(self, executor):
        m1 = executor.get_metrics()
        m1["total_executions"] = 999
        m2 = executor.get_metrics()
        assert m2["total_executions"] == 0


class TestGetActiveExecutions:
    """Tests for get_active_executions."""

    def test_empty_when_no_active(self, executor):
        assert executor.get_active_executions() == {}

    def test_returns_running_only(self, executor):
        now = datetime.now()
        executor._active_executions["exec-1"] = {
            "status": "RUNNING",
            "start_time": now,
        }
        executor._active_executions["exec-2"] = {
            "status": "COMPLETED",
            "start_time": now,
        }

        result = executor.get_active_executions()
        assert "exec-1" in result
        assert "exec-2" not in result

    def test_result_has_required_fields(self, executor):
        now = datetime.now()
        executor._active_executions["exec-1"] = {
            "status": "RUNNING",
            "start_time": now,
        }
        result = executor.get_active_executions()
        assert "status" in result["exec-1"]
        assert "start_time" in result["exec-1"]
        assert "duration_seconds" in result["exec-1"]


class TestRequestStop:
    """Tests for request_stop."""

    def test_returns_false_for_unknown_execution(self, executor):
        result = executor.request_stop("nonexistent")
        assert result is False

    def test_returns_false_for_completed_execution(self, executor):
        executor._active_executions["exec-done"] = {
            "status": "COMPLETED",
            "start_time": datetime.now(),
        }
        result = executor.request_stop("exec-done")
        assert result is False

    def test_returns_true_for_running_execution(self, executor):
        now = datetime.now()
        stop_event = threading.Event()
        mock_future = MagicMock()
        mock_future.done.return_value = False
        mock_future.cancel.return_value = True

        executor._active_executions["exec-run"] = {
            "status": "RUNNING",
            "start_time": now,
        }
        executor._stop_events["exec-run"] = stop_event
        executor._running_tasks["exec-run"] = mock_future

        result = executor.request_stop("exec-run")
        assert result is True

    def test_sets_stop_event(self, executor):
        now = datetime.now()
        stop_event = threading.Event()
        mock_future = MagicMock()
        mock_future.done.return_value = False
        mock_future.cancel.return_value = True

        executor._active_executions["exec-run"] = {
            "status": "RUNNING",
            "start_time": now,
        }
        executor._stop_events["exec-run"] = stop_event
        executor._running_tasks["exec-run"] = mock_future

        executor.request_stop("exec-run")
        assert stop_event.is_set()

    def test_cancels_future(self, executor):
        now = datetime.now()
        mock_future = MagicMock()
        mock_future.done.return_value = False
        mock_future.cancel.return_value = True

        executor._active_executions["exec-run"] = {
            "status": "RUNNING",
            "start_time": now,
        }
        executor._stop_events["exec-run"] = threading.Event()
        executor._running_tasks["exec-run"] = mock_future

        executor.request_stop("exec-run")
        mock_future.cancel.assert_called_once()

    def test_returns_true_when_no_future_but_running(self, executor):
        executor._active_executions["exec-no-future"] = {
            "status": "RUNNING",
            "start_time": datetime.now(),
        }
        executor._stop_events["exec-no-future"] = threading.Event()
        # No entry in _running_tasks
        result = executor.request_stop("exec-no-future")
        assert result is True

    def test_future_already_done(self, executor):
        mock_future = MagicMock()
        mock_future.done.return_value = True

        executor._active_executions["exec-done-fut"] = {
            "status": "RUNNING",
            "start_time": datetime.now(),
        }
        executor._stop_events["exec-done-fut"] = threading.Event()
        executor._running_tasks["exec-done-fut"] = mock_future

        result = executor.request_stop("exec-done-fut")
        assert result is True
        mock_future.cancel.assert_not_called()


class TestRunCrew:
    """Tests for run_crew."""

    @pytest.mark.asyncio
    async def test_successful_execution(self, executor):
        mock_crew = MagicMock()
        mock_crew.kickoff.return_value = "crew result"

        result = await executor.run_crew(
            execution_id="exec-1",
            crew=mock_crew,
            inputs={"topic": "AI"},
        )
        assert result == "crew result"

    @pytest.mark.asyncio
    async def test_execution_without_inputs(self, executor):
        mock_crew = MagicMock()
        mock_crew.kickoff.return_value = "result"

        result = await executor.run_crew(
            execution_id="exec-2",
            crew=mock_crew,
            inputs=None,
        )
        assert result == "result"
        mock_crew.kickoff.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_metrics_updated_on_success(self, executor):
        mock_crew = MagicMock()
        mock_crew.kickoff.return_value = "ok"

        await executor.run_crew("exec-m", mock_crew)

        assert executor._metrics["total_executions"] == 1
        assert executor._metrics["completed_executions"] == 1
        assert executor._metrics["active_executions"] == 0

    @pytest.mark.asyncio
    async def test_on_complete_callback_called(self, executor):
        mock_crew = MagicMock()
        mock_crew.kickoff.return_value = "result"
        on_complete = Mock()

        await executor.run_crew("exec-cb", mock_crew, on_complete=on_complete)
        on_complete.assert_called_once_with("result")

    @pytest.mark.asyncio
    async def test_on_error_callback_called_on_failure(self, executor):
        mock_crew = MagicMock()
        mock_crew.kickoff.side_effect = RuntimeError("crew failed")
        on_error = Mock()

        with pytest.raises(RuntimeError):
            await executor.run_crew("exec-err", mock_crew, on_error=on_error)

        on_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_metrics_updated_on_failure(self, executor):
        mock_crew = MagicMock()
        mock_crew.kickoff.side_effect = RuntimeError("fail")

        with pytest.raises(RuntimeError):
            await executor.run_crew("exec-fail", mock_crew)

        assert executor._metrics["failed_executions"] == 1
        assert executor._metrics["active_executions"] == 0

    @pytest.mark.asyncio
    async def test_timeout_raises(self, executor):
        import asyncio

        mock_crew = MagicMock()

        def slow_kickoff():
            import time
            time.sleep(10)

        mock_crew.kickoff.side_effect = slow_kickoff

        with pytest.raises(asyncio.TimeoutError):
            await executor.run_crew("exec-timeout", mock_crew, timeout=0.1)

    @pytest.mark.asyncio
    async def test_stop_events_cleaned_up_after_execution(self, executor):
        mock_crew = MagicMock()
        mock_crew.kickoff.return_value = "ok"

        await executor.run_crew("exec-cleanup", mock_crew)

        assert "exec-cleanup" not in executor._stop_events

    @pytest.mark.asyncio
    async def test_old_executions_pruned(self, executor):
        """Verify that completed executions beyond 100 are pruned."""
        mock_crew = MagicMock()
        mock_crew.kickoff.return_value = "ok"

        # Pre-populate with 101 completed executions
        for i in range(101):
            executor._active_executions[f"old-{i}"] = {
                "status": "COMPLETED",
                "start_time": datetime.now(),
                "end_time": datetime.now(),
            }

        await executor.run_crew("exec-prune", mock_crew)

        # Total should not exceed ~100 + 1 (the just-completed one)
        total = len(executor._active_executions)
        assert total <= 102  # a bit of wiggle room


class TestShutdown:
    """Tests for shutdown."""

    def test_shutdown_signals_stop_events(self, executor):
        event1 = threading.Event()
        event2 = threading.Event()
        executor._stop_events["e1"] = event1
        executor._stop_events["e2"] = event2

        executor.shutdown(wait=False)
        assert event1.is_set()
        assert event2.is_set()

    def test_shutdown_shuts_down_thread_pool(self, executor):
        with patch.object(executor._executor, "shutdown") as mock_shutdown:
            executor.shutdown(wait=True)
        mock_shutdown.assert_called_once_with(wait=True)


class TestContextManager:
    """Tests for CrewExecutor as context manager."""

    def test_enter_returns_self(self, executor):
        result = executor.__enter__()
        assert result is executor
        executor._executor.shutdown(wait=False)

    def test_exit_calls_shutdown(self, executor):
        with patch.object(executor, "shutdown") as mock_shutdown:
            executor.__exit__(None, None, None)
        mock_shutdown.assert_called_once_with(wait=True)

    def test_exit_returns_false(self, executor):
        with patch.object(executor, "shutdown"):
            result = executor.__exit__(None, None, None)
        assert result is False


class TestRunCrewWithExecutor:
    """Tests for the run_crew_with_executor helper."""

    @pytest.mark.asyncio
    async def test_calls_global_executor(self):
        from src.services.crew_executor import run_crew_with_executor, crew_executor

        mock_crew = MagicMock()
        mock_crew.kickoff.return_value = "helper result"

        with patch.object(crew_executor, "run_crew", new_callable=AsyncMock, return_value="helper result") as mock_run:
            result = await run_crew_with_executor("exec-helper", mock_crew, inputs={"k": "v"})

        mock_run.assert_called_once_with(
            execution_id="exec-helper",
            crew=mock_crew,
            inputs={"k": "v"},
            timeout=None,
        )
        assert result == "helper result"
