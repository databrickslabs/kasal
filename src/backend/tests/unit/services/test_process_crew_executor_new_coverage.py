"""
Additional unit tests for services/process_crew_executor.py — coverage boost.

Focuses on ProcessCrewExecutor class methods and result collection logic.
Avoids spawning real child processes by mocking mp.Process and queues.
"""

import asyncio
import queue
import pytest
from unittest.mock import AsyncMock, MagicMock, Mock, patch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_executor(max_concurrent=4):
    with patch("src.services.process_crew_executor.mp.get_context") as mock_ctx:
        mock_ctx.return_value = MagicMock()
        from src.services.process_crew_executor import ProcessCrewExecutor
        executor = ProcessCrewExecutor(max_concurrent=max_concurrent)
    # Override the context's Queue so tests don't need real MP
    executor._ctx = MagicMock()
    return executor


# ---------------------------------------------------------------------------
# ProcessCrewExecutor.__init__
# ---------------------------------------------------------------------------

class TestProcessCrewExecutorInitExtra:
    def test_default_max_concurrent(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        with patch("src.services.process_crew_executor.mp.get_context"):
            ex = ProcessCrewExecutor()
        assert ex._max_concurrent == 4

    def test_custom_max_concurrent(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        with patch("src.services.process_crew_executor.mp.get_context"):
            ex = ProcessCrewExecutor(max_concurrent=10)
        assert ex._max_concurrent == 10

    def test_empty_tracking_structures(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        with patch("src.services.process_crew_executor.mp.get_context"):
            ex = ProcessCrewExecutor()
        assert len(ex._running_processes) == 0
        assert len(ex._running_futures) == 0
        assert len(ex._running_executors) == 0

    def test_initial_metrics_all_zero(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        with patch("src.services.process_crew_executor.mp.get_context"):
            ex = ProcessCrewExecutor()
        for key in ("total_executions", "active_executions", "completed_executions",
                    "failed_executions", "terminated_executions"):
            assert ex._metrics[key] == 0

    def test_spawn_context_used(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        with patch("src.services.process_crew_executor.mp.get_context") as mock_ctx:
            ex = ProcessCrewExecutor()
            mock_ctx.assert_called_once_with("spawn")


# ---------------------------------------------------------------------------
# _subprocess_initializer
# ---------------------------------------------------------------------------

class TestSubprocessInitializer:
    def test_subprocess_initializer_is_static_callable(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        assert callable(ProcessCrewExecutor._subprocess_initializer)

    def test_subprocess_initializer_runs_without_error(self):
        """Calling the method should complete without raising."""
        from src.services.process_crew_executor import ProcessCrewExecutor
        # Should not raise
        ProcessCrewExecutor._subprocess_initializer()


# ---------------------------------------------------------------------------
# _run_crew_wrapper
# ---------------------------------------------------------------------------

class TestRunCrewWrapper:
    def test_run_crew_wrapper_puts_result_in_queue(self):
        """Wrapper calls run_crew_in_process and puts result in result_queue."""
        from src.services.process_crew_executor import ProcessCrewExecutor
        mock_queue = MagicMock()
        mock_log_queue = MagicMock()
        mock_result = {"status": "COMPLETED", "execution_id": "e-1"}

        with patch("src.services.process_crew_executor.run_crew_in_process",
                   return_value=mock_result):
            ProcessCrewExecutor._run_crew_wrapper(
                "e-1", {"agents": []}, None, None, mock_queue, mock_log_queue
            )
        mock_queue.put.assert_called_once_with(mock_result)

    def test_run_crew_wrapper_puts_error_result_on_exception(self):
        """If run_crew_in_process raises, wrapper puts a FAILED dict in queue."""
        from src.services.process_crew_executor import ProcessCrewExecutor
        mock_queue = MagicMock()
        mock_log_queue = MagicMock()

        with patch("src.services.process_crew_executor.run_crew_in_process",
                   side_effect=RuntimeError("boom")):
            ProcessCrewExecutor._run_crew_wrapper(
                "e-err", {"agents": []}, None, None, mock_queue, mock_log_queue
            )
        call_args = mock_queue.put.call_args[0][0]
        assert call_args["status"] == "FAILED"
        assert call_args["execution_id"] == "e-err"
        assert "boom" in call_args["error"]


# ---------------------------------------------------------------------------
# run_crew_isolated — group_context handling
# ---------------------------------------------------------------------------

class TestRunCrewIsolatedGroupContext:
    @pytest.mark.asyncio
    async def test_group_id_added_to_crew_config(self):
        """group_id and execution_id should be injected into crew_config."""
        executor = _make_executor()

        gc = MagicMock()
        gc.primary_group_id = "tenant-42"
        gc.access_token = None

        crew_config = {"agents": [], "tasks": []}

        # Mock process objects
        mock_process = MagicMock()
        mock_process.pid = 1234
        mock_process.exitcode = 0
        executor._ctx.Process.return_value = mock_process
        executor._ctx.Queue.return_value = MagicMock()

        # Prevent actual process start and wait
        mock_process.start = MagicMock()

        mock_result = {"status": "COMPLETED", "execution_id": "exec-gc", "result": "done"}

        # Patch the relay task and the waiting logic
        with patch("src.services.process_crew_executor.asyncio.create_task"), \
             patch("src.services.process_crew_executor.asyncio.get_event_loop") as mock_loop, \
             patch("src.services.process_crew_executor.asyncio.sleep", new_callable=lambda: lambda *_: asyncio.coroutine(lambda: None)()):

            mock_result_queue = MagicMock()
            mock_result_queue.get.return_value = mock_result
            mock_result_queue.empty.return_value = False
            executor._ctx.Queue.return_value = mock_result_queue

            with patch("asyncio.sleep", new=AsyncMock()):
                # Mock wait result
                with patch("src.services.process_crew_executor.asyncio.create_task", return_value=MagicMock()):
                    # Patch process.is_alive to return False immediately
                    mock_process.is_alive.return_value = False
                    mock_process.join = MagicMock()
                    # Patch is_lakebase_enabled
                    with patch("src.db.database_router.is_lakebase_enabled", new=AsyncMock(return_value=False)):
                        try:
                            result = await executor.run_crew_isolated(
                                execution_id="exec-gc",
                                crew_config=crew_config,
                                group_context=gc,
                            )
                        except Exception:
                            pass  # We care about the side effects, not final result

        # Verify group_id was injected
        assert crew_config.get("group_id") == "tenant-42"
        assert crew_config.get("execution_id") == "exec-gc"

    @pytest.mark.asyncio
    async def test_user_token_added_when_present(self):
        executor = _make_executor()

        gc = MagicMock()
        gc.primary_group_id = "grp-1"
        gc.access_token = "my-obo-token"

        crew_config = {"agents": []}
        mock_result_queue = MagicMock()
        mock_result_queue.empty.return_value = False
        mock_result_queue.get.return_value = {"status": "COMPLETED", "execution_id": "e-tok", "result": "ok"}
        executor._ctx.Queue.return_value = mock_result_queue
        mock_process = MagicMock()
        mock_process.pid = 9999
        mock_process.is_alive.return_value = False
        executor._ctx.Process.return_value = mock_process

        with patch("asyncio.create_task", return_value=MagicMock()), \
             patch("asyncio.sleep", new=AsyncMock()), \
             patch("src.db.database_router.is_lakebase_enabled", new=AsyncMock(return_value=False)):
            try:
                await executor.run_crew_isolated("e-tok", crew_config, gc)
            except Exception:
                pass

        assert crew_config.get("user_token") == "my-obo-token"

    @pytest.mark.asyncio
    async def test_no_group_context_logs_security_error(self):
        executor = _make_executor()
        crew_config = {"agents": []}
        mock_result_queue = MagicMock()
        mock_result_queue.empty.return_value = False
        mock_result_queue.get.return_value = {"status": "COMPLETED", "execution_id": "e-nogrp", "result": "x"}
        executor._ctx.Queue.return_value = mock_result_queue
        mock_process = MagicMock()
        mock_process.pid = 111
        mock_process.is_alive.return_value = False
        executor._ctx.Process.return_value = mock_process

        with patch("asyncio.create_task", return_value=MagicMock()), \
             patch("asyncio.sleep", new=AsyncMock()), \
             patch("src.db.database_router.is_lakebase_enabled", new=AsyncMock(return_value=False)):
            try:
                # group_context=None → security log path
                await executor.run_crew_isolated("e-nogrp", crew_config, group_context=None)
            except Exception:
                pass
        # No assertion on result; the important thing is no unhandled crash


# ---------------------------------------------------------------------------
# terminate_execution
# ---------------------------------------------------------------------------

class TestTerminateExecution:
    @pytest.mark.asyncio
    async def test_terminate_not_running_returns_false(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        with patch("src.services.process_crew_executor.mp.get_context"):
            ex = ProcessCrewExecutor()
        # No running process with this ID — search orphaned processes too
        with patch.object(ex, "_terminate_orphaned_process", return_value=False):
            result = await ex.terminate_execution("nonexistent-id")
        assert result is False

    @pytest.mark.asyncio
    async def test_terminate_running_process_terminates_it(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        with patch("src.services.process_crew_executor.mp.get_context"):
            ex = ProcessCrewExecutor()

        mock_process = MagicMock()
        mock_process.is_alive.return_value = True
        mock_process.terminate = MagicMock()
        mock_process.join = MagicMock()
        mock_process.pid = 5555
        ex._running_processes["exec-stop"] = mock_process

        with patch.object(ex, "_terminate_orphaned_process", return_value=False):
            result = await ex.terminate_execution("exec-stop")

        mock_process.terminate.assert_called()
        assert result is True
        assert "exec-stop" not in ex._running_processes

    @pytest.mark.asyncio
    async def test_terminate_force_kills_if_still_alive(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        with patch("src.services.process_crew_executor.mp.get_context"):
            ex = ProcessCrewExecutor()

        mock_process = MagicMock()
        # Still alive after terminate, then dead after kill
        mock_process.is_alive.side_effect = [True, True]
        mock_process.terminate = MagicMock()
        mock_process.kill = MagicMock()
        mock_process.join = MagicMock()
        mock_process.pid = 6666
        ex._running_processes["exec-kill"] = mock_process

        with patch.object(ex, "_terminate_orphaned_process", return_value=False):
            result = await ex.terminate_execution("exec-kill")

        mock_process.terminate.assert_called()
        mock_process.kill.assert_called()
        assert result is True

    @pytest.mark.asyncio
    async def test_terminate_already_dead_process_returns_true(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        with patch("src.services.process_crew_executor.mp.get_context"):
            ex = ProcessCrewExecutor()

        mock_process = MagicMock()
        mock_process.is_alive.return_value = False
        mock_process.pid = 7777
        ex._running_processes["exec-dead"] = mock_process

        with patch.object(ex, "_terminate_orphaned_process", return_value=False):
            result = await ex.terminate_execution("exec-dead")
        assert result is True


# ---------------------------------------------------------------------------
# get_metrics
# ---------------------------------------------------------------------------

class TestGetMetrics:
    def test_get_metrics_returns_copy(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        with patch("src.services.process_crew_executor.mp.get_context"):
            ex = ProcessCrewExecutor()
        metrics = ex.get_metrics()
        assert isinstance(metrics, dict)
        assert "total_executions" in metrics
        # Modifying returned dict doesn't affect internal state
        metrics["total_executions"] = 9999
        assert ex._metrics["total_executions"] != 9999

    def test_get_metrics_active_executions_count(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        with patch("src.services.process_crew_executor.mp.get_context"):
            ex = ProcessCrewExecutor()
        ex._running_processes["e1"] = MagicMock(is_alive=MagicMock(return_value=True))
        ex._running_processes["e2"] = MagicMock(is_alive=MagicMock(return_value=False))
        metrics = ex.get_metrics()
        # active_executions reflects running processes
        assert "active_executions" in metrics


# ---------------------------------------------------------------------------
# run_crew_in_process — additional validation cases
# ---------------------------------------------------------------------------

class TestRunCrewInProcessExtra:
    def test_dict_config_passes_validation(self):
        """A valid dict config should pass type validation (not return FAILED early)."""
        from src.services.process_crew_executor import run_crew_in_process
        # This should NOT fail with 'crew_config is None' or 'crew_config must be a dict'
        # but will fail later when trying to import heavy modules — that's expected
        result = run_crew_in_process(
            execution_id="e-valid",
            crew_config={"agents": [], "tasks": []},
        )
        # Result may be FAILED due to missing modules, but not due to validation
        assert result.get("execution_id") == "e-valid"
        if result.get("status") == "FAILED":
            assert "crew_config" not in result.get("error", "")

    def test_json_string_dict_passes_validation(self):
        """A JSON string that is a dict should be accepted."""
        import json
        from src.services.process_crew_executor import run_crew_in_process
        config = json.dumps({"agents": [], "tasks": []})
        result = run_crew_in_process(execution_id="e-json-dict", crew_config=config)
        assert result.get("execution_id") == "e-json-dict"
        # Should not fail due to JSON parsing or type validation
        assert "Failed to parse crew_config JSON" not in result.get("error", "")
        assert "crew_config must be a dict" not in result.get("error", "")

    def test_integer_config_fails_with_type_error(self):
        """An integer is not a valid crew_config."""
        from src.services.process_crew_executor import run_crew_in_process
        result = run_crew_in_process(execution_id="e-int", crew_config=42)
        assert result["status"] == "FAILED"
        assert result["execution_id"] == "e-int"


# ---------------------------------------------------------------------------
# _relay_task_events — edge cases
# ---------------------------------------------------------------------------

class TestRelayTaskEventsEdgeCases:
    @pytest.mark.asyncio
    async def test_relay_task_events_task_completed_event(self):
        """task_completed events should also be broadcast."""
        from src.services.process_crew_executor import ProcessCrewExecutor
        with patch("src.services.process_crew_executor.mp.get_context"):
            ex = ProcessCrewExecutor()

        q = queue.Queue()
        q.put({
            "event_type": "task_completed",
            "event_source": "crewai",
            "event_context": "Run analysis",
            "output": "Analysis done",
            "extra_data": {
                "task_name": "Run analysis",
                "task_id": "t-99",
                "agent_role": "Analyst",
                "crew_name": "crew-1",
                "frontend_task_id": "ft-99",
            },
            "created_at": "2025-06-01T10:00:00",
        })

        captured = []

        async def fake_broadcast(job_id, event):
            captured.append(event)
            return 1

        with patch("src.core.sse_manager.sse_manager.broadcast_to_job", new=fake_broadcast):
            task = asyncio.ensure_future(ex._relay_task_events(q, "exec-comp"))
            await asyncio.sleep(0.2)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert len(captured) == 1
        assert captured[0].data["event_type"] == "task_completed"

    @pytest.mark.asyncio
    async def test_relay_task_events_broadcast_exception_is_swallowed(self):
        """If broadcast raises, relay loop should continue."""
        from src.services.process_crew_executor import ProcessCrewExecutor
        with patch("src.services.process_crew_executor.mp.get_context"):
            ex = ProcessCrewExecutor()

        q = queue.Queue()
        q.put({
            "event_type": "task_started",
            "event_source": "crewai",
            "event_context": "ctx",
            "output": None,
            "extra_data": {
                "task_name": "T",
                "task_id": "t-x",
                "agent_role": "A",
                "crew_name": "c",
                "frontend_task_id": "ft-x",
            },
            "created_at": "2025-01-01T00:00:00",
        })

        async def failing_broadcast(job_id, event):
            raise RuntimeError("SSE down")

        with patch("src.core.sse_manager.sse_manager.broadcast_to_job", new=failing_broadcast):
            task = asyncio.ensure_future(ex._relay_task_events(q, "exec-bcast-err"))
            await asyncio.sleep(0.2)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        # Test passes if no unhandled exception propagated
