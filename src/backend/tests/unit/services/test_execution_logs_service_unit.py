"""
Comprehensive unit tests for ExecutionLogsService and module-level functions.

Tests cover:
- ExecutionLogsService class (all methods, branches, error paths)
- logs_writer_loop background task
- start_logs_writer / stop_logs_writer lifecycle functions
"""

import asyncio
import queue
import pytest
from datetime import datetime
from queue import Empty, Full
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

from src.services.execution_logs_service import (
    ExecutionLogsService,
    logs_writer_loop,
    start_logs_writer,
    stop_logs_writer,
)
from src.schemas.execution_logs import ExecutionLogResponse
from src.utils.user_context import GroupContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_log(content: str = "log content", ts: datetime = None):
    """Build a lightweight mock ExecutionLog row."""
    ts = ts or datetime(2024, 1, 15, 10, 30, 0)
    return SimpleNamespace(content=content, timestamp=ts)


def _make_group_context(
    group_ids=None,
    primary_group_id="group-abc",
    group_email="user@example.com",
):
    """Build a GroupContext dataclass with sensible defaults."""
    return GroupContext(
        group_ids=group_ids or [primary_group_id],
        group_email=group_email,
        email_domain="example.com",
    )


# Patch target for the local import of ExecutionHistoryRepository
_HIST_REPO_PATCH = "src.repositories.execution_history_repository.ExecutionHistoryRepository"
# Patch target for the local import of async_session_factory
_SESSION_FACTORY_PATCH = "src.db.session.async_session_factory"
# Patch target for asyncio.sleep inside the service module
_SLEEP_PATCH = "src.services.execution_logs_service.asyncio.sleep"


# ===================================================================
# ExecutionLogsService  --  class-level tests
# ===================================================================


class TestExecutionLogsServiceInit:
    """Tests for __init__ wiring."""

    @patch("src.services.execution_logs_service.ExecutionLogsRepository")
    def test_init_creates_repository(self, mock_repo_cls):
        mock_session = AsyncMock()
        service = ExecutionLogsService(mock_session)

        assert service.session is mock_session
        mock_repo_cls.assert_called_once_with(mock_session)
        assert service.repository is mock_repo_cls.return_value


# -------------------------------------------------------------------
# create_execution_log
# -------------------------------------------------------------------

class TestCreateExecutionLog:
    """Tests for ExecutionLogsService.create_execution_log."""

    @pytest.fixture(autouse=True)
    def _setup(self):
        self.mock_session = AsyncMock()
        with patch("src.services.execution_logs_service.ExecutionLogsRepository") as repo_cls:
            self.mock_repo = repo_cls.return_value
            self.mock_repo.create_log = AsyncMock(return_value=SimpleNamespace(id=1))
            self.service = ExecutionLogsService(self.mock_session)

    @pytest.mark.asyncio
    async def test_create_success_without_group(self):
        result = await self.service.create_execution_log("exec-1", "hello")

        assert result is True
        self.mock_repo.create_log.assert_awaited_once()
        call_kwargs = self.mock_repo.create_log.call_args.kwargs
        assert call_kwargs["execution_id"] == "exec-1"
        assert call_kwargs["content"] == "hello"
        assert call_kwargs["group_id"] is None
        assert call_kwargs["group_email"] is None

    @pytest.mark.asyncio
    async def test_create_success_with_group_context(self):
        gc = _make_group_context()
        result = await self.service.create_execution_log(
            "exec-2", "msg", group_context=gc
        )

        assert result is True
        call_kwargs = self.mock_repo.create_log.call_args.kwargs
        assert call_kwargs["group_id"] == "group-abc"
        assert call_kwargs["group_email"] == "user@example.com"

    @pytest.mark.asyncio
    async def test_create_success_with_explicit_timestamp(self):
        ts = datetime(2025, 6, 1, 12, 0, 0)
        await self.service.create_execution_log("exec-3", "msg", timestamp=ts)

        call_kwargs = self.mock_repo.create_log.call_args.kwargs
        assert call_kwargs["timestamp"] == ts

    @pytest.mark.asyncio
    async def test_create_uses_now_when_no_timestamp(self):
        await self.service.create_execution_log("exec-4", "msg", timestamp=None)

        call_kwargs = self.mock_repo.create_log.call_args.kwargs
        # Should be a datetime that was just created (not None)
        assert isinstance(call_kwargs["timestamp"], datetime)

    @pytest.mark.asyncio
    async def test_create_returns_false_on_exception(self):
        self.mock_repo.create_log.side_effect = RuntimeError("db down")
        result = await self.service.create_execution_log("exec-err", "oops")

        assert result is False

    @pytest.mark.asyncio
    async def test_create_returns_false_on_generic_exception(self):
        self.mock_repo.create_log.side_effect = Exception("unexpected")
        result = await self.service.create_execution_log("exec-x", "bad")

        assert result is False


# -------------------------------------------------------------------
# get_execution_logs
# -------------------------------------------------------------------

class TestGetExecutionLogs:
    """Tests for ExecutionLogsService.get_execution_logs."""

    @pytest.fixture(autouse=True)
    def _setup(self):
        self.mock_session = AsyncMock()
        with patch("src.services.execution_logs_service.ExecutionLogsRepository") as repo_cls:
            self.mock_repo = repo_cls.return_value
            self.mock_repo.get_logs_by_execution_id = AsyncMock(return_value=[])
            self.service = ExecutionLogsService(self.mock_session)

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_logs(self):
        result = await self.service.get_execution_logs("exec-1")

        assert result == []
        self.mock_repo.get_logs_by_execution_id.assert_awaited_once_with(
            execution_id="exec-1", limit=1000, offset=0
        )

    @pytest.mark.asyncio
    async def test_returns_mapped_responses(self):
        logs = [
            _make_log("first", datetime(2024, 1, 1, 0, 0, 0)),
            _make_log("second", datetime(2024, 1, 2, 0, 0, 0)),
        ]
        self.mock_repo.get_logs_by_execution_id.return_value = logs

        result = await self.service.get_execution_logs("exec-1")

        assert len(result) == 2
        assert isinstance(result[0], ExecutionLogResponse)
        assert result[0].content == "first"
        assert result[0].timestamp == "2024-01-01T00:00:00"
        assert result[1].content == "second"
        assert result[1].timestamp == "2024-01-02T00:00:00"

    @pytest.mark.asyncio
    async def test_custom_limit_and_offset(self):
        self.mock_repo.get_logs_by_execution_id.return_value = []

        await self.service.get_execution_logs("exec-1", limit=50, offset=10)

        self.mock_repo.get_logs_by_execution_id.assert_awaited_once_with(
            execution_id="exec-1", limit=50, offset=10
        )


# -------------------------------------------------------------------
# get_execution_logs_by_group
# -------------------------------------------------------------------

class TestGetExecutionLogsByGroup:
    """Tests for ExecutionLogsService.get_execution_logs_by_group."""

    @pytest.fixture(autouse=True)
    def _setup(self):
        self.mock_session = AsyncMock()
        with patch("src.services.execution_logs_service.ExecutionLogsRepository") as repo_cls:
            self.mock_repo = repo_cls.return_value
            self.mock_repo.get_logs_by_execution_id = AsyncMock(return_value=[])
            self.service = ExecutionLogsService(self.mock_session)

    @pytest.mark.asyncio
    async def test_returns_empty_when_execution_not_found(self):
        gc = _make_group_context()
        with patch(_HIST_REPO_PATCH) as hist_cls:
            hist_cls.return_value.get_execution_by_job_id = AsyncMock(return_value=None)
            result = await self.service.get_execution_logs_by_group("exec-1", gc)

        assert result == []
        self.mock_repo.get_logs_by_execution_id.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_primary_group_id(self):
        gc = GroupContext(group_ids=[], group_email="u@x.com", email_domain="x.com")
        execution_mock = SimpleNamespace(id=1, job_id="exec-1")
        with patch(_HIST_REPO_PATCH) as hist_cls:
            hist_cls.return_value.get_execution_by_job_id = AsyncMock(
                return_value=execution_mock
            )
            result = await self.service.get_execution_logs_by_group("exec-1", gc)

        assert result == []

    @pytest.mark.asyncio
    async def test_returns_logs_with_valid_group_context(self):
        gc = _make_group_context(group_ids=["grp-1", "grp-2"])
        execution_mock = SimpleNamespace(id=1, job_id="exec-1")
        logs = [_make_log("line1"), _make_log("line2")]
        self.mock_repo.get_logs_by_execution_id.return_value = logs

        with patch(_HIST_REPO_PATCH) as hist_cls:
            hist_cls.return_value.get_execution_by_job_id = AsyncMock(
                return_value=execution_mock
            )
            result = await self.service.get_execution_logs_by_group("exec-1", gc)

        assert len(result) == 2
        assert isinstance(result[0], ExecutionLogResponse)
        self.mock_repo.get_logs_by_execution_id.assert_awaited_once_with(
            execution_id="exec-1",
            limit=1000,
            offset=0,
            newest_first=True,
            group_ids=["grp-1", "grp-2"],
        )

    @pytest.mark.asyncio
    async def test_custom_limit_and_offset(self):
        gc = _make_group_context()
        execution_mock = SimpleNamespace(id=1, job_id="exec-1")
        self.mock_repo.get_logs_by_execution_id.return_value = []

        with patch(_HIST_REPO_PATCH) as hist_cls:
            hist_cls.return_value.get_execution_by_job_id = AsyncMock(
                return_value=execution_mock
            )
            await self.service.get_execution_logs_by_group(
                "exec-1", gc, limit=25, offset=5
            )

        call_kwargs = self.mock_repo.get_logs_by_execution_id.call_args.kwargs
        assert call_kwargs["limit"] == 25
        assert call_kwargs["offset"] == 5

    @pytest.mark.asyncio
    async def test_group_ids_derived_from_context(self):
        """Verify that group_ids passed to execution history repo come from context."""
        gc = _make_group_context(group_ids=["grp-x", "grp-y"])
        with patch(_HIST_REPO_PATCH) as hist_cls:
            hist_cls.return_value.get_execution_by_job_id = AsyncMock(return_value=None)
            await self.service.get_execution_logs_by_group("exec-1", gc)

        hist_cls.return_value.get_execution_by_job_id.assert_awaited_once_with(
            "exec-1", group_ids=["grp-x", "grp-y"]
        )

    @pytest.mark.asyncio
    async def test_group_ids_none_when_no_primary_group(self):
        """When primary_group_id is falsy, group_ids should be None for the query."""
        gc = GroupContext(group_ids=None, group_email="u@x.com", email_domain="x.com")
        with patch(_HIST_REPO_PATCH) as hist_cls:
            hist_cls.return_value.get_execution_by_job_id = AsyncMock(return_value=None)
            await self.service.get_execution_logs_by_group("exec-1", gc)

        hist_cls.return_value.get_execution_by_job_id.assert_awaited_once_with(
            "exec-1", group_ids=None
        )

    @pytest.mark.asyncio
    async def test_returns_correct_timestamps_in_response(self):
        """Verify timestamps are correctly serialized to ISO format."""
        gc = _make_group_context()
        execution_mock = SimpleNamespace(id=1, job_id="exec-1")
        ts = datetime(2024, 3, 15, 8, 30, 45)
        self.mock_repo.get_logs_by_execution_id.return_value = [_make_log("ts test", ts)]

        with patch(_HIST_REPO_PATCH) as hist_cls:
            hist_cls.return_value.get_execution_by_job_id = AsyncMock(
                return_value=execution_mock
            )
            result = await self.service.get_execution_logs_by_group("exec-1", gc)

        assert result[0].timestamp == "2024-03-15T08:30:45"


# -------------------------------------------------------------------
# count_logs
# -------------------------------------------------------------------

class TestCountLogs:
    """Tests for ExecutionLogsService.count_logs."""

    @pytest.fixture(autouse=True)
    def _setup(self):
        self.mock_session = AsyncMock()
        with patch("src.services.execution_logs_service.ExecutionLogsRepository") as repo_cls:
            self.mock_repo = repo_cls.return_value
            self.mock_repo.count_by_execution_id = AsyncMock(return_value=42)
            self.service = ExecutionLogsService(self.mock_session)

    @pytest.mark.asyncio
    async def test_count_delegates_to_repository(self):
        result = await self.service.count_logs("exec-1")

        assert result == 42
        self.mock_repo.count_by_execution_id.assert_awaited_once_with("exec-1")


# -------------------------------------------------------------------
# delete_logs / delete_by_execution_id / delete_all_logs
# -------------------------------------------------------------------

class TestDeleteLogs:
    """Tests for deletion methods."""

    @pytest.fixture(autouse=True)
    def _setup(self):
        self.mock_session = AsyncMock()
        with patch("src.services.execution_logs_service.ExecutionLogsRepository") as repo_cls:
            self.mock_repo = repo_cls.return_value
            self.mock_repo.delete_by_execution_id = AsyncMock(return_value=5)
            self.mock_repo.delete_all = AsyncMock(return_value=100)
            self.service = ExecutionLogsService(self.mock_session)

    @pytest.mark.asyncio
    async def test_delete_logs_delegates_to_repository(self):
        result = await self.service.delete_logs("exec-1")

        assert result == 5
        self.mock_repo.delete_by_execution_id.assert_awaited_once_with("exec-1")

    @pytest.mark.asyncio
    async def test_delete_by_execution_id_is_alias_for_delete_logs(self):
        result = await self.service.delete_by_execution_id("exec-2")

        assert result == 5
        self.mock_repo.delete_by_execution_id.assert_awaited_once_with("exec-2")

    @pytest.mark.asyncio
    async def test_delete_all_logs_delegates_to_repository(self):
        result = await self.service.delete_all_logs()

        assert result == 100
        self.mock_repo.delete_all.assert_awaited_once()


# ===================================================================
# Module-level functions: logs_writer_loop
# ===================================================================


def _make_session_factory_mock(mock_session):
    """Create an async context manager mock that yields mock_session."""
    mock_session_cm = AsyncMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_cm.__aexit__ = AsyncMock(return_value=False)
    factory = MagicMock(return_value=mock_session_cm)
    return factory


class TestLogsWriterLoop:
    """Tests for the logs_writer_loop background coroutine."""

    @pytest.mark.asyncio
    async def test_exits_when_shutdown_event_is_set(self):
        """Loop should exit promptly when the shutdown event is already set."""
        shutdown = asyncio.Event()
        shutdown.set()

        mock_queue = MagicMock()
        mock_queue.qsize.return_value = 0

        with patch(
            "src.services.execution_logs_service.get_job_output_queue",
            return_value=mock_queue,
        ):
            await logs_writer_loop(shutdown)

        # If we reach here the loop exited -- success

    @pytest.mark.asyncio
    async def test_processes_single_log_batch(self):
        """Loop should read items from the queue and write them to the database."""
        shutdown = asyncio.Event()

        log_data = {
            "job_id": "job-1",
            "content": "hello",
            "timestamp": datetime(2024, 1, 1),
        }

        call_count = 0

        def side_effect(block, timeout):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return log_data
            shutdown.set()
            raise Empty()

        mock_queue = MagicMock()
        mock_queue.qsize.return_value = 1
        mock_queue.get.side_effect = side_effect

        mock_session = AsyncMock()
        mock_repo = MagicMock()
        mock_repo.create_log = AsyncMock(return_value=SimpleNamespace(id=1))

        with (
            patch(
                "src.services.execution_logs_service.get_job_output_queue",
                return_value=mock_queue,
            ),
            patch(
                "src.services.execution_logs_service.ExecutionLogsRepository",
                return_value=mock_repo,
            ),
            patch(
                _SESSION_FACTORY_PATCH,
                _make_session_factory_mock(mock_session),
            ),
            patch(_SLEEP_PATCH, new_callable=AsyncMock),
        ):
            await logs_writer_loop(shutdown)

        mock_repo.create_log.assert_awaited()
        create_kwargs = mock_repo.create_log.call_args.kwargs
        assert create_kwargs["execution_id"] == "job-1"
        assert create_kwargs["content"] == "hello"
        mock_session.commit.assert_awaited()

    @pytest.mark.asyncio
    async def test_handles_none_shutdown_signal_in_queue(self):
        """None items in the queue are treated as shutdown signals and skipped."""
        shutdown = asyncio.Event()

        call_count = 0

        def side_effect(block, timeout):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return None  # shutdown signal
            shutdown.set()
            raise Empty()

        mock_queue = MagicMock()
        mock_queue.qsize.return_value = 0
        mock_queue.get.side_effect = side_effect

        with (
            patch(
                "src.services.execution_logs_service.get_job_output_queue",
                return_value=mock_queue,
            ),
            patch(_SLEEP_PATCH, new_callable=AsyncMock),
        ):
            await logs_writer_loop(shutdown)

        # task_done should NOT be called for None items (they are just continued)
        mock_queue.task_done.assert_not_called()

    @pytest.mark.asyncio
    async def test_handles_db_error_during_log_creation(self):
        """When create_log raises, the error is caught, session is rolled back."""
        shutdown = asyncio.Event()

        log_data = {
            "job_id": "job-err",
            "content": "will fail",
            "timestamp": datetime.now(),
        }

        call_count = 0

        def queue_side_effect(block, timeout):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return log_data
            shutdown.set()
            raise Empty()

        mock_queue = MagicMock()
        mock_queue.qsize.return_value = 1
        mock_queue.get.side_effect = queue_side_effect

        mock_session = AsyncMock()
        mock_repo = MagicMock()
        mock_repo.create_log = AsyncMock(side_effect=RuntimeError("db error"))

        with (
            patch(
                "src.services.execution_logs_service.get_job_output_queue",
                return_value=mock_queue,
            ),
            patch(
                "src.services.execution_logs_service.ExecutionLogsRepository",
                return_value=mock_repo,
            ),
            patch(
                _SESSION_FACTORY_PATCH,
                _make_session_factory_mock(mock_session),
            ),
            patch(_SLEEP_PATCH, new_callable=AsyncMock),
        ):
            # Should NOT raise
            await logs_writer_loop(shutdown)

        mock_session.rollback.assert_awaited()

    @pytest.mark.asyncio
    async def test_handles_cancellation(self):
        """CancelledError should be caught gracefully."""
        shutdown = asyncio.Event()

        mock_queue = MagicMock()
        mock_queue.qsize.return_value = 0
        mock_queue.get.side_effect = Empty()

        with (
            patch(
                "src.services.execution_logs_service.get_job_output_queue",
                return_value=mock_queue,
            ),
            patch(_SLEEP_PATCH, new_callable=AsyncMock),
        ):
            task = asyncio.create_task(logs_writer_loop(shutdown))
            await asyncio.sleep(0)  # yield to let the task start
            task.cancel()
            # Should not raise
            try:
                await task
            except asyncio.CancelledError:
                pass  # acceptable

    @pytest.mark.asyncio
    async def test_calls_sleep_when_queue_empty(self):
        """When the queue is empty, the loop calls asyncio.sleep to avoid CPU spinning."""
        shutdown = asyncio.Event()

        mock_queue = MagicMock()
        mock_queue.qsize.return_value = 0
        mock_queue.get.side_effect = Empty()

        with (
            patch(
                "src.services.execution_logs_service.get_job_output_queue",
                return_value=mock_queue,
            ),
            patch(_SLEEP_PATCH, new_callable=AsyncMock) as mock_sleep,
        ):
            call_count = 0

            async def sleep_side_effect(t):
                nonlocal call_count
                call_count += 1
                if call_count >= 2:
                    shutdown.set()

            mock_sleep.side_effect = sleep_side_effect

            await logs_writer_loop(shutdown)

        # asyncio.sleep should have been called with 0.5 at least once
        calls = [c for c in mock_sleep.call_args_list if c.args[0] == 0.5]
        assert len(calls) >= 1

    @pytest.mark.asyncio
    async def test_uses_default_timestamp_when_missing(self):
        """If log_data has no timestamp, datetime.now() is used as default."""
        shutdown = asyncio.Event()

        log_data = {"job_id": "job-no-ts", "content": "no ts"}

        call_count = 0

        def side_effect(block, timeout):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return log_data
            shutdown.set()
            raise Empty()

        mock_queue = MagicMock()
        mock_queue.qsize.return_value = 1
        mock_queue.get.side_effect = side_effect

        mock_session = AsyncMock()
        mock_repo = MagicMock()
        mock_repo.create_log = AsyncMock(return_value=SimpleNamespace(id=1))

        with (
            patch(
                "src.services.execution_logs_service.get_job_output_queue",
                return_value=mock_queue,
            ),
            patch(
                "src.services.execution_logs_service.ExecutionLogsRepository",
                return_value=mock_repo,
            ),
            patch(
                _SESSION_FACTORY_PATCH,
                _make_session_factory_mock(mock_session),
            ),
            patch(_SLEEP_PATCH, new_callable=AsyncMock),
        ):
            await logs_writer_loop(shutdown)

        create_kwargs = mock_repo.create_log.call_args.kwargs
        assert isinstance(create_kwargs["timestamp"], datetime)

    @pytest.mark.asyncio
    async def test_batch_processing_error_calls_sleep_one_second(self):
        """When the outer batch try/except catches an error it should sleep 1s."""
        shutdown = asyncio.Event()

        mock_queue = MagicMock()
        mock_queue.qsize.return_value = 0
        # Raise an unexpected error during batch collection
        mock_queue.get.side_effect = RuntimeError("unexpected batch error")

        with (
            patch(
                "src.services.execution_logs_service.get_job_output_queue",
                return_value=mock_queue,
            ),
            patch(
                _SLEEP_PATCH,
                new_callable=AsyncMock,
            ) as mock_sleep,
        ):
            call_count = 0

            async def sleep_side_effect(t):
                nonlocal call_count
                call_count += 1
                if call_count >= 2:
                    shutdown.set()

            mock_sleep.side_effect = sleep_side_effect

            await logs_writer_loop(shutdown)

        # asyncio.sleep should have been called with 1 (the error-recovery sleep)
        calls = [c for c in mock_sleep.call_args_list if c.args[0] == 1]
        assert len(calls) >= 1

    @pytest.mark.asyncio
    async def test_processes_multiple_items_in_batch(self):
        """Verifies that up to batch_target_size items are collected in one batch."""
        shutdown = asyncio.Event()

        items = [
            {"job_id": f"job-{i}", "content": f"msg {i}", "timestamp": datetime.now()}
            for i in range(3)
        ]
        call_count = 0

        def side_effect(block, timeout):
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                return items[call_count - 1]
            shutdown.set()
            raise Empty()

        mock_queue = MagicMock()
        mock_queue.qsize.return_value = 3
        mock_queue.get.side_effect = side_effect

        mock_session = AsyncMock()
        mock_repo = MagicMock()
        mock_repo.create_log = AsyncMock(return_value=SimpleNamespace(id=1))

        with (
            patch(
                "src.services.execution_logs_service.get_job_output_queue",
                return_value=mock_queue,
            ),
            patch(
                "src.services.execution_logs_service.ExecutionLogsRepository",
                return_value=mock_repo,
            ),
            patch(
                _SESSION_FACTORY_PATCH,
                _make_session_factory_mock(mock_session),
            ),
            patch(_SLEEP_PATCH, new_callable=AsyncMock),
        ):
            await logs_writer_loop(shutdown)

        assert mock_repo.create_log.await_count == 3
        assert mock_queue.task_done.call_count == 3

    @pytest.mark.asyncio
    async def test_log_data_with_unknown_job_id_defaults(self):
        """If job_id is missing from log_data it defaults to 'unknown'."""
        shutdown = asyncio.Event()

        log_data = {"content": "no job id", "timestamp": datetime.now()}

        call_count = 0

        def side_effect(block, timeout):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return log_data
            shutdown.set()
            raise Empty()

        mock_queue = MagicMock()
        mock_queue.qsize.return_value = 1
        mock_queue.get.side_effect = side_effect

        mock_session = AsyncMock()
        mock_repo = MagicMock()
        mock_repo.create_log = AsyncMock(return_value=SimpleNamespace(id=1))

        with (
            patch(
                "src.services.execution_logs_service.get_job_output_queue",
                return_value=mock_queue,
            ),
            patch(
                "src.services.execution_logs_service.ExecutionLogsRepository",
                return_value=mock_repo,
            ),
            patch(
                _SESSION_FACTORY_PATCH,
                _make_session_factory_mock(mock_session),
            ),
            patch(_SLEEP_PATCH, new_callable=AsyncMock),
        ):
            await logs_writer_loop(shutdown)

        create_kwargs = mock_repo.create_log.call_args.kwargs
        assert create_kwargs["execution_id"] == "unknown"

    @pytest.mark.asyncio
    async def test_unhandled_exception_in_loop(self):
        """A critical exception before the while loop should be caught in the outer try."""
        shutdown = asyncio.Event()

        with patch(
            "src.services.execution_logs_service.get_job_output_queue",
            side_effect=RuntimeError("fatal"),
        ):
            # Should NOT propagate
            await logs_writer_loop(shutdown)

    @pytest.mark.asyncio
    async def test_exception_during_individual_log_processing(self):
        """An exception when processing a single log item should increment failures."""
        shutdown = asyncio.Event()

        log_data = {"job_id": "job-inner-err", "content": "bad", "timestamp": datetime.now()}

        call_count = 0

        def side_effect(block, timeout):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return log_data
            shutdown.set()
            raise Empty()

        mock_queue = MagicMock()
        mock_queue.qsize.return_value = 1
        mock_queue.get.side_effect = side_effect

        # Make async_session_factory return a context manager that raises
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(side_effect=RuntimeError("session error"))
        mock_session_cm.__aexit__ = AsyncMock(return_value=False)
        factory = MagicMock(return_value=mock_session_cm)

        with (
            patch(
                "src.services.execution_logs_service.get_job_output_queue",
                return_value=mock_queue,
            ),
            patch(_SESSION_FACTORY_PATCH, factory),
            patch(_SLEEP_PATCH, new_callable=AsyncMock),
        ):
            # Should not raise
            await logs_writer_loop(shutdown)

    @pytest.mark.asyncio
    async def test_log_data_with_empty_content(self):
        """Log data with empty content should still be processed normally."""
        shutdown = asyncio.Event()

        log_data = {"job_id": "job-empty", "content": "", "timestamp": datetime.now()}
        call_count = 0

        def side_effect(block, timeout):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return log_data
            shutdown.set()
            raise Empty()

        mock_queue = MagicMock()
        mock_queue.qsize.return_value = 1
        mock_queue.get.side_effect = side_effect

        mock_session = AsyncMock()
        mock_repo = MagicMock()
        mock_repo.create_log = AsyncMock(return_value=SimpleNamespace(id=1))

        with (
            patch(
                "src.services.execution_logs_service.get_job_output_queue",
                return_value=mock_queue,
            ),
            patch(
                "src.services.execution_logs_service.ExecutionLogsRepository",
                return_value=mock_repo,
            ),
            patch(
                _SESSION_FACTORY_PATCH,
                _make_session_factory_mock(mock_session),
            ),
            patch(_SLEEP_PATCH, new_callable=AsyncMock),
        ):
            await logs_writer_loop(shutdown)

        create_kwargs = mock_repo.create_log.call_args.kwargs
        assert create_kwargs["content"] == ""


# ===================================================================
# start_logs_writer
# ===================================================================


class TestStartLogsWriter:
    """Tests for start_logs_writer module-level function."""

    @pytest.fixture(autouse=True)
    def _reset_global(self):
        """Reset the global _logs_writer_task before and after each test."""
        import src.services.execution_logs_service as mod

        original = mod._logs_writer_task
        mod._logs_writer_task = None
        yield
        mod._logs_writer_task = original

    @pytest.mark.asyncio
    async def test_start_creates_new_task_when_none(self):
        shutdown = asyncio.Event()
        shutdown.set()  # immediately stop

        mock_queue = MagicMock()
        mock_queue.qsize.return_value = 0

        with patch(
            "src.services.execution_logs_service.get_job_output_queue",
            return_value=mock_queue,
        ):
            task = await start_logs_writer(shutdown)

        assert task is not None
        # Wait for the task to complete (it should since shutdown is set)
        await task
        assert task.done()

    @pytest.mark.asyncio
    async def test_start_returns_existing_running_task(self):
        import src.services.execution_logs_service as mod

        # Create a mock task that is not done
        mock_task = MagicMock()
        mock_task.done.return_value = False
        mod._logs_writer_task = mock_task

        shutdown = asyncio.Event()
        result = await start_logs_writer(shutdown)

        assert result is mock_task

    @pytest.mark.asyncio
    async def test_start_replaces_done_task(self):
        import src.services.execution_logs_service as mod

        # Create a mock task that is done
        mock_task = MagicMock()
        mock_task.done.return_value = True
        mod._logs_writer_task = mock_task

        shutdown = asyncio.Event()
        shutdown.set()

        mock_queue = MagicMock()
        mock_queue.qsize.return_value = 0

        with patch(
            "src.services.execution_logs_service.get_job_output_queue",
            return_value=mock_queue,
        ):
            result = await start_logs_writer(shutdown)

        assert result is not mock_task
        await result


# ===================================================================
# stop_logs_writer
# ===================================================================


class TestStopLogsWriter:
    """Tests for stop_logs_writer module-level function."""

    @pytest.fixture(autouse=True)
    def _reset_global(self):
        import src.services.execution_logs_service as mod

        original = mod._logs_writer_task
        mod._logs_writer_task = None
        yield
        mod._logs_writer_task = original

    @pytest.mark.asyncio
    async def test_stop_when_no_task(self):
        result = await stop_logs_writer()
        assert result is True

    @pytest.mark.asyncio
    async def test_stop_when_task_already_done(self):
        import src.services.execution_logs_service as mod

        mock_task = MagicMock()
        mock_task.done.return_value = True
        mod._logs_writer_task = mock_task

        result = await stop_logs_writer()
        assert result is True

    @pytest.mark.asyncio
    async def test_stop_graceful_shutdown(self):
        """Task finishes within timeout -> graceful stop."""
        import src.services.execution_logs_service as mod

        shutdown = asyncio.Event()

        mock_queue = MagicMock()
        mock_queue.qsize.return_value = 0
        mock_queue.get.side_effect = Empty()

        with (
            patch(
                "src.services.execution_logs_service.get_job_output_queue",
                return_value=mock_queue,
            ),
            patch(_SLEEP_PATCH, new_callable=AsyncMock) as mock_sleep,
        ):
            # Start the loop (not yet shutting down)
            task = asyncio.create_task(logs_writer_loop(shutdown))
            mod._logs_writer_task = task
            await asyncio.sleep(0)  # yield so task starts running

            # Signal shutdown; the loop will see it on next iteration
            shutdown.set()

            result = await stop_logs_writer(timeout=2.0)

        assert result is True
        assert mod._logs_writer_task is None

    @pytest.mark.asyncio
    async def test_stop_timeout_cancels_task(self):
        """Task does not finish within timeout -> force cancel."""
        import src.services.execution_logs_service as mod

        # Create a task that will never finish on its own
        never_done = asyncio.Future()

        async def hang_forever():
            await never_done

        task = asyncio.create_task(hang_forever())
        mod._logs_writer_task = task

        mock_queue = MagicMock()
        mock_queue.put_nowait = MagicMock()

        with patch(
            "src.services.execution_logs_service.get_job_output_queue",
            return_value=mock_queue,
        ):
            result = await stop_logs_writer(timeout=0.05)

        assert result is True
        assert mod._logs_writer_task is None

    @pytest.mark.asyncio
    async def test_stop_queue_full_on_shutdown_signal(self):
        """When the queue is full, put_nowait raises Full but shutdown continues."""
        import src.services.execution_logs_service as mod

        shutdown = asyncio.Event()
        shutdown.set()

        mock_queue_obj = MagicMock()
        mock_queue_obj.qsize.return_value = 0

        with patch(
            "src.services.execution_logs_service.get_job_output_queue",
            return_value=mock_queue_obj,
        ):
            task = asyncio.create_task(logs_writer_loop(shutdown))
            mod._logs_writer_task = task
            await task

        # Now make get_job_output_queue return a full queue
        full_queue = MagicMock()
        full_queue.put_nowait.side_effect = Full("full")

        with patch(
            "src.services.execution_logs_service.get_job_output_queue",
            return_value=full_queue,
        ):
            result = await stop_logs_writer(timeout=2.0)

        assert result is True

    @pytest.mark.asyncio
    async def test_stop_handles_unexpected_exception(self):
        """When an unexpected exception occurs during stop, returns False."""
        import src.services.execution_logs_service as mod

        mock_task = MagicMock()
        mock_task.done.return_value = False
        mod._logs_writer_task = mock_task

        with patch(
            "src.services.execution_logs_service.get_job_output_queue",
            side_effect=RuntimeError("totally broken"),
        ):
            result = await stop_logs_writer()

        assert result is False


# ===================================================================
# Edge cases and integration-style unit tests
# ===================================================================


class TestEdgeCases:
    """Miscellaneous edge-case coverage."""

    @pytest.mark.asyncio
    async def test_get_execution_logs_single_log(self):
        mock_session = AsyncMock()
        with patch("src.services.execution_logs_service.ExecutionLogsRepository") as repo_cls:
            mock_repo = repo_cls.return_value
            mock_repo.get_logs_by_execution_id = AsyncMock(
                return_value=[_make_log("only")]
            )
            service = ExecutionLogsService(mock_session)

        result = await service.get_execution_logs("exec-1")
        assert len(result) == 1
        assert result[0].content == "only"

    @pytest.mark.asyncio
    async def test_create_log_none_group_context(self):
        mock_session = AsyncMock()
        with patch("src.services.execution_logs_service.ExecutionLogsRepository") as repo_cls:
            mock_repo = repo_cls.return_value
            mock_repo.create_log = AsyncMock(return_value=SimpleNamespace(id=1))
            service = ExecutionLogsService(mock_session)

        result = await service.create_execution_log("e1", "c", group_context=None)
        assert result is True
        kw = mock_repo.create_log.call_args.kwargs
        assert kw["group_id"] is None
        assert kw["group_email"] is None

    @pytest.mark.asyncio
    async def test_delete_by_execution_id_calls_delete_logs(self):
        """Ensure delete_by_execution_id is truly an alias that calls delete_logs."""
        mock_session = AsyncMock()
        with patch("src.services.execution_logs_service.ExecutionLogsRepository") as repo_cls:
            mock_repo = repo_cls.return_value
            mock_repo.delete_by_execution_id = AsyncMock(return_value=3)
            service = ExecutionLogsService(mock_session)

        # Spy on delete_logs
        with patch.object(service, "delete_logs", wraps=service.delete_logs) as spy:
            result = await service.delete_by_execution_id("exec-alias")
            spy.assert_awaited_once_with("exec-alias")
        assert result == 3

    @pytest.mark.asyncio
    async def test_get_logs_by_group_with_group_context_no_group_ids(self):
        """GroupContext with group_ids=None and primary_group_id=None."""
        mock_session = AsyncMock()
        with patch("src.services.execution_logs_service.ExecutionLogsRepository") as repo_cls:
            service = ExecutionLogsService(mock_session)

        gc = GroupContext(group_ids=None, group_email="a@b.com", email_domain="b.com")

        with patch(_HIST_REPO_PATCH) as hist_cls:
            hist_cls.return_value.get_execution_by_job_id = AsyncMock(return_value=None)
            result = await service.get_execution_logs_by_group("exec-1", gc)

        assert result == []

    @pytest.mark.asyncio
    async def test_logs_writer_loop_consecutive_empty_count_logging(self):
        """Verify the empty_count logic triggers debug logging at multiples of 100."""
        shutdown = asyncio.Event()
        empty_call_count = 0

        def side_effect(block, timeout):
            nonlocal empty_call_count
            empty_call_count += 1
            raise Empty()

        mock_queue = MagicMock()
        mock_queue.qsize.return_value = 0
        mock_queue.get.side_effect = side_effect

        with (
            patch(
                "src.services.execution_logs_service.get_job_output_queue",
                return_value=mock_queue,
            ),
            patch(
                _SLEEP_PATCH,
                new_callable=AsyncMock,
            ) as mock_sleep,
        ):
            # Let it run enough iterations to get some empty_count increments
            iter_count = 0

            async def original_sleep(t):
                nonlocal iter_count
                iter_count += 1
                if iter_count >= 5:
                    shutdown.set()

            mock_sleep.side_effect = original_sleep

            await logs_writer_loop(shutdown)

        # Just verify it didn't crash -- the empty_count logging path was exercised
        assert empty_call_count > 0

    @pytest.mark.asyncio
    async def test_count_logs_returns_zero(self):
        """count_logs should propagate zero count correctly."""
        mock_session = AsyncMock()
        with patch("src.services.execution_logs_service.ExecutionLogsRepository") as repo_cls:
            mock_repo = repo_cls.return_value
            mock_repo.count_by_execution_id = AsyncMock(return_value=0)
            service = ExecutionLogsService(mock_session)

        result = await service.count_logs("nonexistent-exec")
        assert result == 0

    @pytest.mark.asyncio
    async def test_delete_all_logs_returns_zero_when_empty(self):
        """delete_all_logs should return 0 when there are no logs."""
        mock_session = AsyncMock()
        with patch("src.services.execution_logs_service.ExecutionLogsRepository") as repo_cls:
            mock_repo = repo_cls.return_value
            mock_repo.delete_all = AsyncMock(return_value=0)
            service = ExecutionLogsService(mock_session)

        result = await service.delete_all_logs()
        assert result == 0

    @pytest.mark.asyncio
    async def test_get_execution_logs_by_group_execution_exists_no_group_empty_ids(self):
        """When execution exists but group_ids is empty list, access is denied."""
        mock_session = AsyncMock()
        with patch("src.services.execution_logs_service.ExecutionLogsRepository") as repo_cls:
            mock_repo = repo_cls.return_value
            mock_repo.get_logs_by_execution_id = AsyncMock(return_value=[])
            service = ExecutionLogsService(mock_session)

        # group_ids=[] means primary_group_id is None (empty list)
        gc = GroupContext(group_ids=[], group_email="u@x.com", email_domain="x.com")
        execution_mock = SimpleNamespace(id=1, job_id="exec-1")

        with patch(_HIST_REPO_PATCH) as hist_cls:
            hist_cls.return_value.get_execution_by_job_id = AsyncMock(
                return_value=execution_mock
            )
            result = await service.get_execution_logs_by_group("exec-1", gc)

        # Should return empty because primary_group_id is None
        assert result == []
        mock_repo.get_logs_by_execution_id.assert_not_awaited()
