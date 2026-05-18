"""
Coverage-focused tests for crew_executor.py
Targets uncovered lines: run_crew, request_stop edge cases, metrics, shutdown.
"""
import asyncio
import threading
import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, Mock, patch, AsyncMock
from concurrent.futures import ThreadPoolExecutor

import src.services.crew_executor as crew_executor_module


def _fresh_executor(max_workers=3):
    """Bypass singleton to get a fresh instance."""
    from src.services.crew_executor import CrewExecutor
    ex = object.__new__(CrewExecutor)
    ex._initialized = False
    ex.__init__(max_workers=max_workers)
    return ex


@pytest.fixture
def executor():
    ex = _fresh_executor()
    yield ex
    try:
        ex._executor.shutdown(wait=False)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# run_crew - success path
# ---------------------------------------------------------------------------

class TestRunCrewSuccess:
    @pytest.mark.asyncio
    async def test_run_crew_with_inputs(self, executor):
        mock_crew = MagicMock()
        mock_crew.kickoff.return_value = "crew result"

        result = await executor.run_crew(
            execution_id="exec-success",
            crew=mock_crew,
            inputs={"query": "hello"},
        )
        assert result == "crew result"
        mock_crew.kickoff.assert_called_once_with(inputs={"query": "hello"})

    @pytest.mark.asyncio
    async def test_run_crew_without_inputs(self, executor):
        mock_crew = MagicMock()
        mock_crew.kickoff.return_value = "no input result"

        result = await executor.run_crew(
            execution_id="exec-noinput",
            crew=mock_crew,
            inputs=None,
        )
        assert result == "no input result"
        mock_crew.kickoff.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_metrics_updated_on_success(self, executor):
        mock_crew = MagicMock()
        mock_crew.kickoff.return_value = "done"

        await executor.run_crew(execution_id="m-exec", crew=mock_crew)

        metrics = executor.get_metrics()
        assert metrics["total_executions"] >= 1
        assert metrics["completed_executions"] >= 1
        assert metrics["active_executions"] == 0

    @pytest.mark.asyncio
    async def test_on_complete_callback_called(self, executor):
        mock_crew = MagicMock()
        mock_crew.kickoff.return_value = "result"

        callback_called = []

        def on_complete(result):
            callback_called.append(result)

        await executor.run_crew(
            execution_id="cb-exec",
            crew=mock_crew,
            on_complete=on_complete,
        )
        assert callback_called == ["result"]

    @pytest.mark.asyncio
    async def test_stop_event_checked_during_execution(self, executor):
        """Test that stop event set during kickoff causes CancelledError."""
        import threading as _threading

        mock_crew = MagicMock()
        exec_id = "stop-during"

        # The run_crew creates a NEW stop_event at the top and stores it in _stop_events
        # We need to set it during kickoff execution
        # Use an event to coordinate: after kickoff starts, set the stop_event
        kickoff_started = _threading.Event()

        def kickoff_with_stop_set(**kw):
            # Notify that kickoff started
            kickoff_started.set()
            # Set the stop event that run_crew created for this execution_id
            if exec_id in executor._stop_events:
                executor._stop_events[exec_id].set()
            return "partial result"

        mock_crew.kickoff = kickoff_with_stop_set

        with pytest.raises(asyncio.CancelledError):
            await executor.run_crew(
                execution_id=exec_id,
                crew=mock_crew,
            )

    @pytest.mark.asyncio
    async def test_metrics_updated_on_cancel(self, executor):
        mock_crew = MagicMock()
        mock_crew.kickoff.side_effect = asyncio.CancelledError("cancelled")

        with pytest.raises(asyncio.CancelledError):
            await executor.run_crew(execution_id="cancel-exec", crew=mock_crew)

        metrics = executor.get_metrics()
        assert metrics["cancelled_executions"] >= 1

    @pytest.mark.asyncio
    async def test_timeout_raises_and_updates_metrics(self, executor):
        mock_crew = MagicMock()

        import time

        def slow_kickoff(**kw):
            time.sleep(10)  # Will timeout
            return "never"

        mock_crew.kickoff = slow_kickoff

        on_error_called = []

        def on_error(err):
            on_error_called.append(err)

        with pytest.raises(asyncio.TimeoutError):
            await executor.run_crew(
                execution_id="timeout-exec",
                crew=mock_crew,
                timeout=0.05,  # Very short timeout
                on_error=on_error,
            )

        metrics = executor.get_metrics()
        assert metrics["failed_executions"] >= 1

    @pytest.mark.asyncio
    async def test_on_error_callback_on_failure(self, executor):
        mock_crew = MagicMock()
        mock_crew.kickoff.side_effect = ValueError("crew error")

        errors = []

        def on_error(e):
            errors.append(e)

        with pytest.raises(ValueError):
            await executor.run_crew(
                execution_id="err-exec",
                crew=mock_crew,
                on_error=on_error,
            )
        assert len(errors) == 1
        assert isinstance(errors[0], ValueError)


# ---------------------------------------------------------------------------
# request_stop
# ---------------------------------------------------------------------------

class TestRequestStop:
    def test_stop_running_execution_with_future(self, executor):
        mock_future = MagicMock()
        mock_future.done.return_value = False
        mock_future.cancel.return_value = True

        executor._active_executions["run-exec"] = {
            "status": "RUNNING",
            "start_time": datetime.now(),
        }
        executor._stop_events["run-exec"] = threading.Event()
        executor._running_tasks["run-exec"] = mock_future

        result = executor.request_stop("run-exec")
        assert result is True
        mock_future.cancel.assert_called_once()

    def test_stop_running_execution_future_cancel_fails(self, executor):
        mock_future = MagicMock()
        mock_future.done.return_value = False
        mock_future.cancel.return_value = False  # Can't cancel thread futures

        executor._active_executions["thread-exec"] = {
            "status": "RUNNING",
            "start_time": datetime.now(),
        }
        executor._stop_events["thread-exec"] = threading.Event()
        executor._running_tasks["thread-exec"] = mock_future

        result = executor.request_stop("thread-exec")
        assert result is True

    def test_stop_running_execution_future_already_done(self, executor):
        mock_future = MagicMock()
        mock_future.done.return_value = True  # Already done

        executor._active_executions["done-exec"] = {
            "status": "RUNNING",
            "start_time": datetime.now(),
        }
        executor._stop_events["done-exec"] = threading.Event()
        executor._running_tasks["done-exec"] = mock_future

        result = executor.request_stop("done-exec")
        assert result is True

    def test_stop_running_execution_no_future(self, executor):
        executor._active_executions["no-future-exec"] = {
            "status": "RUNNING",
            "start_time": datetime.now(),
        }
        executor._stop_events["no-future-exec"] = threading.Event()
        # No entry in _running_tasks

        result = executor.request_stop("no-future-exec")
        assert result is True

    def test_stop_non_running_execution(self, executor):
        executor._active_executions["completed-exec"] = {
            "status": "COMPLETED",
            "start_time": datetime.now(),
        }
        result = executor.request_stop("completed-exec")
        assert result is False

    def test_stop_non_existent_execution(self, executor):
        result = executor.request_stop("non-existent-exec")
        assert result is False


# ---------------------------------------------------------------------------
# get_metrics
# ---------------------------------------------------------------------------

class TestGetMetrics:
    def test_no_average_when_no_completions(self, executor):
        metrics = executor.get_metrics()
        assert "average_duration_seconds" not in metrics

    def test_average_calculated_with_completions(self, executor):
        executor._metrics["completed_executions"] = 2
        executor._metrics["total_duration_seconds"] = 10.0
        metrics = executor.get_metrics()
        assert metrics["average_duration_seconds"] == 5.0

    def test_returns_copy_not_reference(self, executor):
        m1 = executor.get_metrics()
        m1["total_executions"] = 9999
        m2 = executor.get_metrics()
        assert m2["total_executions"] != 9999


# ---------------------------------------------------------------------------
# get_active_executions
# ---------------------------------------------------------------------------

class TestGetActiveExecutions:
    def test_returns_only_running(self, executor):
        now = datetime.now()
        executor._active_executions["r1"] = {"status": "RUNNING", "start_time": now}
        executor._active_executions["c1"] = {"status": "COMPLETED", "start_time": now}

        result = executor.get_active_executions()
        assert "r1" in result
        assert "c1" not in result

    def test_includes_duration(self, executor):
        past = datetime.now() - timedelta(seconds=5)
        executor._active_executions["r2"] = {"status": "RUNNING", "start_time": past}

        result = executor.get_active_executions()
        assert result["r2"]["duration_seconds"] >= 5.0

    def test_empty_when_no_active(self, executor):
        executor._active_executions.clear()
        result = executor.get_active_executions()
        assert result == {}


# ---------------------------------------------------------------------------
# shutdown
# ---------------------------------------------------------------------------

class TestShutdown:
    def test_shutdown_stops_all_events(self, executor):
        ev1 = threading.Event()
        ev2 = threading.Event()
        executor._stop_events["e1"] = ev1
        executor._stop_events["e2"] = ev2

        executor.shutdown(wait=False)
        assert ev1.is_set()
        assert ev2.is_set()

    def test_shutdown_works_with_no_events(self, executor):
        executor._stop_events.clear()
        executor.shutdown(wait=False)  # Should not raise


# ---------------------------------------------------------------------------
# context manager
# ---------------------------------------------------------------------------

class TestContextManager:
    def test_enter_returns_self(self, executor):
        result = executor.__enter__()
        assert result is executor

    def test_exit_calls_shutdown(self, executor):
        with patch.object(executor, "shutdown") as mock_shutdown:
            result = executor.__exit__(None, None, None)
        assert result is False
        mock_shutdown.assert_called_once_with(wait=True)


# ---------------------------------------------------------------------------
# Old completed executions cleanup (> 100)
# ---------------------------------------------------------------------------

class TestCleanupOldExecutions:
    @pytest.mark.asyncio
    async def test_cleans_up_when_over_100_completed(self, executor):
        """Test that old completed executions are pruned when >100 exist."""
        base_time = datetime.now()
        # Add 101 completed executions
        for i in range(101):
            executor._active_executions[f"old-{i}"] = {
                "status": "COMPLETED",
                "start_time": base_time - timedelta(seconds=i),
                "end_time": base_time - timedelta(seconds=i - 1),
            }

        mock_crew = MagicMock()
        mock_crew.kickoff.return_value = "result"

        # Run a new execution to trigger cleanup
        result = await executor.run_crew(
            execution_id="trigger-cleanup",
            crew=mock_crew,
        )

        # Should have cleaned up some old executions
        completed = [
            k for k, v in executor._active_executions.items()
            if v.get("status") in ["COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "STOPPED"]
        ]
        assert len(completed) <= 100


# ---------------------------------------------------------------------------
# run_crew_with_executor helper function
# ---------------------------------------------------------------------------

class TestRunCrewWithExecutor:
    @pytest.mark.asyncio
    async def test_convenience_function(self):
        mock_crew = MagicMock()
        mock_crew.kickoff.return_value = "output"

        from src.services.crew_executor import run_crew_with_executor

        # Patch the global executor to use our fresh one
        fresh_ex = _fresh_executor()
        with patch("src.services.crew_executor.crew_executor", fresh_ex):
            result = await run_crew_with_executor(
                execution_id="conv-exec",
                crew=mock_crew,
                inputs={"k": "v"},
            )
        assert result == "output"
        try:
            fresh_ex._executor.shutdown(wait=False)
        except Exception:
            pass
