"""
Coverage tests for process_crew_executor.py - Part 4.

Focuses on the SSE relay broadcasting path and _process_log_queue.

Targets:
  2166-2170  _relay_task_events exception path
  2179-2217  _relay_task_events SSE broadcasting (task_started/completed/failed)
  2304-2350  _process_log_queue with crew.log found and logs written
  2458       _terminate_orphaned_process short-id match
  2514-2520  _terminate_orphaned_process NoSuchProcess/AccessDenied handling
  2600-2612  shutdown with child processes
  2677-2686  kill_orphan_crew_processes ImportError/Exception
"""
import asyncio
import queue
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call


def _make_executor():
    with patch("src.services.process_crew_executor.mp.get_context") as mock_ctx:
        mock_ctx.return_value = MagicMock()
        from src.services.process_crew_executor import ProcessCrewExecutor
        executor = ProcessCrewExecutor()
    executor._ctx = MagicMock()
    return executor


# ---------------------------------------------------------------------------
# _relay_task_events — SSE broadcasting (lines 2179-2217)
# ---------------------------------------------------------------------------

class TestRelayTaskEventsBroadcast:

    @pytest.mark.asyncio
    async def test_task_started_event_broadcasts_sse(self):
        """task_started event triggers SSE broadcast."""
        executor = _make_executor()
        from queue import Empty

        task_event = {
            "event_type": "task_started",
            "event_source": "crewai",
            "event_context": "Research task",
            "output": None,
            "extra_data": {
                "task_name": "Research",
                "task_id": "t-1",
                "agent_role": "Researcher",
                "crew_name": "MyCrew",
                "frontend_task_id": "ft-1",
            },
            "trace_metadata": {"extra": "info"},
            "created_at": "2025-01-01T00:00:00",
        }

        events_iter = iter([task_event])
        call_count = {"n": 0}

        def get_side_effect(block=True, timeout=0.5):
            call_count["n"] += 1
            try:
                return next(events_iter)
            except StopIteration:
                raise Empty()

        mock_queue = MagicMock()
        mock_queue.get = MagicMock(side_effect=get_side_effect)

        mock_sse_manager = AsyncMock()
        mock_sse_manager.broadcast_to_job = AsyncMock(return_value=1)

        mock_sse_event_class = MagicMock()
        mock_sse_event_class.return_value = MagicMock()

        with patch("src.core.sse_manager.sse_manager", mock_sse_manager), \
             patch("src.core.sse_manager.SSEEvent", mock_sse_event_class):
            task = asyncio.create_task(executor._relay_task_events(mock_queue, "exec-sse"))
            await asyncio.sleep(0.1)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        mock_sse_manager.broadcast_to_job.assert_awaited()

    @pytest.mark.asyncio
    async def test_task_completed_event_broadcasts_sse(self):
        """task_completed event triggers SSE broadcast."""
        executor = _make_executor()
        from queue import Empty

        task_event = {
            "event_type": "task_completed",
            "event_source": "crewai",
            "event_context": "Research task",
            "output": "Research complete",
            "extra_data": {"task_name": "Research"},
            "trace_metadata": {},
            "created_at": None,  # Test the non-string case
        }

        events_iter = iter([task_event])
        call_count = {"n": 0}

        def get_side_effect(block=True, timeout=0.5):
            call_count["n"] += 1
            try:
                return next(events_iter)
            except StopIteration:
                raise Empty()

        mock_queue = MagicMock()
        mock_queue.get = MagicMock(side_effect=get_side_effect)

        mock_sse_manager = AsyncMock()
        mock_sse_manager.broadcast_to_job = AsyncMock(return_value=0)

        with patch("src.core.sse_manager.sse_manager", mock_sse_manager), \
             patch("src.core.sse_manager.SSEEvent", MagicMock()):
            task = asyncio.create_task(executor._relay_task_events(mock_queue, "exec-sse2"))
            await asyncio.sleep(0.1)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        mock_sse_manager.broadcast_to_job.assert_awaited()

    @pytest.mark.asyncio
    async def test_sse_broadcast_error_is_caught(self):
        """Exception during SSE broadcast is caught and logged, not raised."""
        executor = _make_executor()
        from queue import Empty

        task_event = {
            "event_type": "task_failed",
            "extra_data": {},
            "trace_metadata": {},
        }

        events_iter = iter([task_event])

        def get_side_effect(block=True, timeout=0.5):
            try:
                return next(events_iter)
            except StopIteration:
                raise Empty()

        mock_queue = MagicMock()
        mock_queue.get = MagicMock(side_effect=get_side_effect)

        mock_sse_manager = AsyncMock()
        mock_sse_manager.broadcast_to_job = AsyncMock(side_effect=RuntimeError("SSE send failed"))

        with patch("src.core.sse_manager.sse_manager", mock_sse_manager), \
             patch("src.core.sse_manager.SSEEvent", MagicMock()):
            task = asyncio.create_task(executor._relay_task_events(mock_queue, "exec-sseerr"))
            await asyncio.sleep(0.1)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Should not raise; SSE error was caught

    @pytest.mark.asyncio
    async def test_queue_exception_continues_loop(self):
        """Non-Empty, non-CancelledError exceptions from queue.get are caught."""
        executor = _make_executor()
        from queue import Empty

        call_count = {"n": 0}

        def get_side_effect(block=True, timeout=0.5):
            call_count["n"] += 1
            if call_count["n"] <= 2:
                raise RuntimeError("queue error")
            raise Empty()

        mock_queue = MagicMock()
        mock_queue.get = MagicMock(side_effect=get_side_effect)

        with patch("src.core.sse_manager.sse_manager", AsyncMock()), \
             patch("src.core.sse_manager.SSEEvent", MagicMock()):
            task = asyncio.create_task(executor._relay_task_events(mock_queue, "exec-qerr"))
            await asyncio.sleep(0.1)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


# ---------------------------------------------------------------------------
# _process_log_queue — logs written path (lines 2304-2348)
# ---------------------------------------------------------------------------

class TestProcessLogQueueFull:

    @pytest.mark.asyncio
    async def test_only_header_log_when_no_matching_lines(self):
        """When no matching lines in crew.log, only the header log is written."""
        executor = _make_executor()
        execution_id = "aaaa1111-no-match"
        mock_queue = MagicMock()

        log_content = "2025-01-01 different-exec Something happened\n"

        mock_repo = AsyncMock()
        mock_session = AsyncMock()
        mock_session.commit = AsyncMock()

        async def mock_smart_session():
            yield mock_session

        from unittest.mock import mock_open
        m = mock_open(read_data=log_content)

        with patch("os.path.exists", return_value=True), \
             patch("builtins.open", m), \
             patch("src.db.database_router.get_smart_db_session", return_value=mock_smart_session()), \
             patch("src.repositories.execution_logs_repository.ExecutionLogsRepository", return_value=mock_repo):
            await executor._process_log_queue(mock_queue, execution_id, None)

        # create_log should be called at least once for the header
        mock_repo.create_log.assert_called()

    @pytest.mark.asyncio
    async def test_logs_written_with_group_context(self):
        """Logs are written with group_id and group_email from group_context."""
        executor = _make_executor()
        execution_id = "bbbb2222-with-context"
        mock_queue = MagicMock()

        group_ctx = MagicMock()
        group_ctx.primary_group_id = "grp-test"
        group_ctx.group_email = "test@example.com"

        log_content = f"2025-01-01 {execution_id[:8]} Task complete\n"

        mock_repo = AsyncMock()
        mock_session = AsyncMock()
        mock_session.commit = AsyncMock()

        async def mock_smart_session():
            yield mock_session

        from unittest.mock import mock_open
        m = mock_open(read_data=log_content)

        with patch("os.path.exists", return_value=True), \
             patch("builtins.open", m), \
             patch("src.db.database_router.get_smart_db_session", return_value=mock_smart_session()), \
             patch("src.repositories.execution_logs_repository.ExecutionLogsRepository", return_value=mock_repo):
            await executor._process_log_queue(mock_queue, execution_id, group_ctx)

        # At least header + 1 matching log
        assert mock_repo.create_log.call_count >= 2

    @pytest.mark.asyncio
    async def test_db_write_error_is_caught(self):
        """Errors during DB write are caught and logged, not propagated."""
        executor = _make_executor()
        execution_id = "cccc3333-dberr"
        mock_queue = MagicMock()

        log_content = f"2025-01-01 {execution_id[:8]} Task done\n"

        async def error_smart_session():
            raise RuntimeError("db session failed")
            yield  # make it a generator

        from unittest.mock import mock_open
        m = mock_open(read_data=log_content)

        with patch("os.path.exists", return_value=True), \
             patch("builtins.open", m), \
             patch("src.db.database_router.get_smart_db_session", side_effect=RuntimeError("db error")):
            # Should not raise
            await executor._process_log_queue(mock_queue, execution_id, None)


# ---------------------------------------------------------------------------
# _terminate_orphaned_process — short ID match (line 2458)
# ---------------------------------------------------------------------------

class TestTerminateOrphanedShortId:

    def test_short_exec_id_prefix_match(self):
        """Process with KASAL_EXECUTION_ID starting with exec_id[:8] is matched."""
        executor = _make_executor()

        full_exec_id = "exec1234-full-execution-id-here"
        short_id = full_exec_id[:8]  # "exec1234"

        mock_child = MagicMock()
        mock_parent_proc = MagicMock()
        mock_parent_proc.children = MagicMock(return_value=[mock_child])
        mock_parent_proc.kill = MagicMock()

        # Process has a different but startswith-matching ID
        mock_proc = MagicMock()
        mock_proc.info = {"pid": 7654, "name": "python", "cmdline": []}
        mock_proc.environ = MagicMock(return_value={
            "KASAL_EXECUTION_ID": short_id + "-extended-version"
        })

        import psutil as _psutil
        with patch("psutil.process_iter", return_value=[mock_proc]), \
             patch("psutil.Process", return_value=mock_parent_proc), \
             patch("psutil.wait_procs", return_value=([], [])):
            result = executor._terminate_orphaned_process(full_exec_id)

        # Should have found a match via startswith
        assert result is True


# ---------------------------------------------------------------------------
# shutdown — additional child process cleanup
# ---------------------------------------------------------------------------

class TestShutdownChildProcesses:

    def test_shutdown_force_kills_surviving_children(self):
        """Alive children after wait are force-killed."""
        executor = _make_executor()

        mock_alive_child = MagicMock()
        mock_alive_child.pid = 4444
        mock_alive_child.kill = MagicMock()

        mock_current = MagicMock()
        mock_current.children = MagicMock(return_value=[mock_alive_child])

        with patch("psutil.Process", return_value=mock_current), \
             patch("psutil.wait_procs", return_value=([], [mock_alive_child])):
            executor.shutdown(wait=True)

        mock_alive_child.kill.assert_called_once()

    def test_shutdown_no_such_process_on_child(self):
        """NoSuchProcess during child kill is silently handled."""
        executor = _make_executor()
        import psutil

        mock_alive_child = MagicMock()
        mock_alive_child.pid = 5555
        mock_alive_child.kill = MagicMock(side_effect=psutil.NoSuchProcess(5555))

        mock_current = MagicMock()
        mock_current.children = MagicMock(return_value=[mock_alive_child])

        with patch("psutil.Process", return_value=mock_current), \
             patch("psutil.wait_procs", return_value=([], [mock_alive_child])):
            executor.shutdown()  # Should not raise


# ---------------------------------------------------------------------------
# kill_orphan_crew_processes — ImportError and Exception paths
# ---------------------------------------------------------------------------

class TestKillOrphanCrewProcessesEdgeCases:

    def test_psutil_not_available_returns_zero(self):
        """When psutil fails to import, returns 0."""
        from src.services.process_crew_executor import ProcessCrewExecutor

        with patch("psutil.process_iter", side_effect=ImportError("no psutil")):
            result = ProcessCrewExecutor.kill_orphan_crew_processes()

        assert result == 0

    def test_non_psutil_exception_returns_zero(self):
        """Generic exceptions are caught and return 0."""
        from src.services.process_crew_executor import ProcessCrewExecutor

        with patch("psutil.process_iter", side_effect=Exception("unexpected error")):
            result = ProcessCrewExecutor.kill_orphan_crew_processes()

        assert result == 0

    def test_no_crew_processes_returns_zero(self):
        """No matching crew processes found returns 0."""
        from src.services.process_crew_executor import ProcessCrewExecutor

        non_crew_proc = MagicMock()
        non_crew_proc.info = {
            "pid": 1, "name": "chrome", "cmdline": ["chrome"], "ppid": 100, "create_time": 0
        }
        non_crew_proc.create_time = MagicMock(return_value=0)

        with patch("psutil.process_iter", return_value=[non_crew_proc]):
            result = ProcessCrewExecutor.kill_orphan_crew_processes()

        assert result == 0
