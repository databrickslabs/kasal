"""
Comprehensive unit tests for ExecutionTraceService.

Covers all public methods, branches, error paths, and group-check variants.
Target: >=90% coverage of src/services/execution_trace_service.py.
"""

import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy.exc import SQLAlchemyError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_trace_obj(**overrides):
    """Create a SimpleNamespace that looks like an ExecutionTrace ORM object."""
    defaults = dict(
        id=1,
        run_id=10,
        job_id="job-abc-123",
        event_source="agent_1",
        event_context="task_1",
        event_type="task_started",
        output={"content": "hello"},
        trace_metadata=None,
        created_at=None,
        span_id=None,
        trace_id=None,
        parent_span_id=None,
        span_name=None,
        status_code=None,
        duration_ms=None,
        group_id=None,
        group_email=None,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_execution_obj(**overrides):
    """Create a SimpleNamespace that looks like an ExecutionHistory ORM object."""
    defaults = dict(
        id=10,
        job_id="job-abc-123",
        status="completed",
        group_id="grp-1",
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_group_context(group_ids=None, email="test@example.com"):
    """Create a SimpleNamespace that looks like a GroupContext dataclass."""
    return SimpleNamespace(
        group_ids=group_ids or ["grp-1"],
        group_email=email,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_session():
    session = AsyncMock()
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def mock_trace_repo():
    return AsyncMock()


@pytest.fixture
def mock_history_repo():
    return AsyncMock()


@pytest.fixture
def service(mock_session, mock_trace_repo, mock_history_repo):
    """
    Build an ExecutionTraceService with mocked repositories injected
    by patching the constructors so that __init__ uses our mocks.
    """
    with patch(
        "src.services.execution_trace_service.ExecutionTraceRepository",
        return_value=mock_trace_repo,
    ), patch(
        "src.services.execution_trace_service.ExecutionHistoryRepository",
        return_value=mock_history_repo,
    ):
        from src.services.execution_trace_service import ExecutionTraceService
        svc = ExecutionTraceService(mock_session)
    return svc


# =========================================================================
# get_traces_by_run_id
# =========================================================================
class TestGetTracesByRunId:

    @pytest.mark.asyncio
    async def test_returns_response_when_execution_exists(
        self, service, mock_history_repo, mock_trace_repo
    ):
        execution = _make_execution_obj()
        mock_history_repo.get_execution_by_id = AsyncMock(return_value=execution)

        trace = _make_trace_obj()
        mock_trace_repo.get_by_run_id = AsyncMock(return_value=[trace])

        ctx = _make_group_context()
        result = await service.get_traces_by_run_id(
            group_context=ctx, run_id=10, limit=50, offset=0
        )

        assert result is not None
        assert result.run_id == 10
        assert len(result.traces) == 1
        mock_history_repo.get_execution_by_id.assert_awaited_once_with(10, group_ids=["grp-1"])

    @pytest.mark.asyncio
    async def test_returns_none_when_execution_not_found(
        self, service, mock_history_repo
    ):
        mock_history_repo.get_execution_by_id = AsyncMock(return_value=None)

        ctx = _make_group_context()
        result = await service.get_traces_by_run_id(group_context=ctx, run_id=999)
        assert result is None

    @pytest.mark.asyncio
    async def test_fills_missing_job_ids_on_traces(
        self, service, mock_history_repo, mock_trace_repo
    ):
        execution = _make_execution_obj(job_id="job-xyz")
        mock_history_repo.get_execution_by_id = AsyncMock(return_value=execution)

        trace_missing = _make_trace_obj(job_id=None)
        trace_present = _make_trace_obj(id=2, job_id="job-xyz")
        mock_trace_repo.get_by_run_id = AsyncMock(return_value=[trace_missing, trace_present])

        ctx = _make_group_context()
        result = await service.get_traces_by_run_id(group_context=ctx, run_id=10)

        assert result is not None
        # The service should have back-filled the missing job_id
        assert trace_missing.job_id == "job-xyz"

    @pytest.mark.asyncio
    async def test_no_group_context_passes_none_group_ids(
        self, service, mock_history_repo, mock_trace_repo
    ):
        execution = _make_execution_obj()
        mock_history_repo.get_execution_by_id = AsyncMock(return_value=execution)
        mock_trace_repo.get_by_run_id = AsyncMock(return_value=[])

        result = await service.get_traces_by_run_id(group_context=None, run_id=10)
        assert result is not None
        mock_history_repo.get_execution_by_id.assert_awaited_once_with(10, group_ids=None)

    @pytest.mark.asyncio
    async def test_sqlalchemy_error_propagates(
        self, service, mock_history_repo
    ):
        mock_history_repo.get_execution_by_id = AsyncMock(
            side_effect=SQLAlchemyError("db boom")
        )
        with pytest.raises(SQLAlchemyError):
            await service.get_traces_by_run_id(group_context=None, run_id=10)

    @pytest.mark.asyncio
    async def test_generic_exception_propagates(
        self, service, mock_history_repo
    ):
        mock_history_repo.get_execution_by_id = AsyncMock(
            side_effect=RuntimeError("unexpected")
        )
        with pytest.raises(RuntimeError):
            await service.get_traces_by_run_id(group_context=None, run_id=10)

    @pytest.mark.asyncio
    async def test_all_traces_have_job_id_no_backfill(
        self, service, mock_history_repo, mock_trace_repo
    ):
        """When all traces already have job_id set, no backfill occurs."""
        execution = _make_execution_obj(job_id="job-xyz")
        mock_history_repo.get_execution_by_id = AsyncMock(return_value=execution)

        trace = _make_trace_obj(job_id="job-xyz")
        mock_trace_repo.get_by_run_id = AsyncMock(return_value=[trace])

        ctx = _make_group_context()
        result = await service.get_traces_by_run_id(group_context=ctx, run_id=10)
        assert result is not None
        assert trace.job_id == "job-xyz"

    @pytest.mark.asyncio
    async def test_empty_traces_list(
        self, service, mock_history_repo, mock_trace_repo
    ):
        execution = _make_execution_obj()
        mock_history_repo.get_execution_by_id = AsyncMock(return_value=execution)
        mock_trace_repo.get_by_run_id = AsyncMock(return_value=[])

        result = await service.get_traces_by_run_id(group_context=None, run_id=10)
        assert result is not None
        assert len(result.traces) == 0


# =========================================================================
# get_traces_by_job_id
# =========================================================================
class TestGetTracesByJobId:

    @pytest.mark.asyncio
    async def test_returns_traces_found_by_job_id(
        self, service, mock_history_repo, mock_trace_repo
    ):
        execution = _make_execution_obj(id=10)
        mock_history_repo.get_execution_by_job_id = AsyncMock(return_value=execution)

        trace = _make_trace_obj()
        mock_trace_repo.get_by_job_id = AsyncMock(return_value=[trace])

        ctx = _make_group_context()
        result = await service.get_traces_by_job_id(
            group_context=ctx, job_id="job-abc-123"
        )

        assert result is not None
        assert result.job_id == "job-abc-123"
        assert len(result.traces) == 1

    @pytest.mark.asyncio
    async def test_falls_back_to_run_id_when_no_direct_traces(
        self, service, mock_history_repo, mock_trace_repo
    ):
        execution = _make_execution_obj(id=10)
        mock_history_repo.get_execution_by_job_id = AsyncMock(return_value=execution)

        # First call (get_by_job_id) returns empty, second (get_by_run_id) returns data
        mock_trace_repo.get_by_job_id = AsyncMock(return_value=[])
        fallback_trace = _make_trace_obj(job_id=None)
        mock_trace_repo.get_by_run_id = AsyncMock(return_value=[fallback_trace])

        result = await service.get_traces_by_job_id(
            group_context=None, job_id="job-abc-123"
        )

        assert result is not None
        assert len(result.traces) == 1
        # job_id should have been backfilled
        assert fallback_trace.job_id == "job-abc-123"

    @pytest.mark.asyncio
    async def test_returns_none_when_execution_not_found_at_all(
        self, service, mock_history_repo
    ):
        """Execution not found even without group filter."""
        mock_history_repo.get_execution_by_job_id = AsyncMock(return_value=None)

        result = await service.get_traces_by_job_id(
            group_context=_make_group_context(), job_id="missing"
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_access_denied(
        self, service, mock_history_repo
    ):
        """Execution exists but user does not have group access."""
        # With group filter -> None (no access)
        # Without group filter -> found (exists for another group)
        mock_history_repo.get_execution_by_job_id = AsyncMock(
            side_effect=[None, _make_execution_obj(group_id="other-group")]
        )

        result = await service.get_traces_by_job_id(
            group_context=_make_group_context(), job_id="job-abc-123"
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_no_group_context_passes_none(
        self, service, mock_history_repo, mock_trace_repo
    ):
        execution = _make_execution_obj()
        mock_history_repo.get_execution_by_job_id = AsyncMock(return_value=execution)
        mock_trace_repo.get_by_job_id = AsyncMock(return_value=[])
        mock_trace_repo.get_by_run_id = AsyncMock(return_value=[])

        result = await service.get_traces_by_job_id(
            group_context=None, job_id="job-abc-123"
        )
        assert result is not None
        mock_history_repo.get_execution_by_job_id.assert_any_await(
            "job-abc-123", group_ids=None
        )

    @pytest.mark.asyncio
    async def test_sqlalchemy_error_propagates(
        self, service, mock_history_repo
    ):
        mock_history_repo.get_execution_by_job_id = AsyncMock(
            side_effect=SQLAlchemyError("db error")
        )
        with pytest.raises(SQLAlchemyError):
            await service.get_traces_by_job_id(
                group_context=None, job_id="job-abc-123"
            )

    @pytest.mark.asyncio
    async def test_generic_exception_propagates(
        self, service, mock_history_repo
    ):
        mock_history_repo.get_execution_by_job_id = AsyncMock(
            side_effect=RuntimeError("oops")
        )
        with pytest.raises(RuntimeError):
            await service.get_traces_by_job_id(
                group_context=None, job_id="job-abc-123"
            )

    @pytest.mark.asyncio
    async def test_fallback_traces_with_job_id_already_set(
        self, service, mock_history_repo, mock_trace_repo
    ):
        """Fallback traces that already have job_id set should not be overwritten."""
        execution = _make_execution_obj(id=10)
        mock_history_repo.get_execution_by_job_id = AsyncMock(return_value=execution)

        mock_trace_repo.get_by_job_id = AsyncMock(return_value=[])
        fallback_trace = _make_trace_obj(job_id="existing-job")
        mock_trace_repo.get_by_run_id = AsyncMock(return_value=[fallback_trace])

        result = await service.get_traces_by_job_id(
            group_context=None, job_id="job-abc-123"
        )
        assert result is not None
        # Should not overwrite the existing job_id
        assert fallback_trace.job_id == "existing-job"


# =========================================================================
# get_all_traces
# =========================================================================
class TestGetAllTraces:

    @pytest.mark.asyncio
    async def test_returns_paginated_list(self, service, mock_trace_repo):
        trace = _make_trace_obj()
        mock_trace_repo.get_all_traces = AsyncMock(return_value=([trace], 1))

        result = await service.get_all_traces(limit=50, offset=0)
        assert result.total == 1
        assert result.limit == 50
        assert result.offset == 0
        assert len(result.traces) == 1

    @pytest.mark.asyncio
    async def test_returns_empty_list(self, service, mock_trace_repo):
        mock_trace_repo.get_all_traces = AsyncMock(return_value=([], 0))

        result = await service.get_all_traces()
        assert result.total == 0
        assert len(result.traces) == 0

    @pytest.mark.asyncio
    async def test_sqlalchemy_error_propagates(self, service, mock_trace_repo):
        mock_trace_repo.get_all_traces = AsyncMock(
            side_effect=SQLAlchemyError("db fail")
        )
        with pytest.raises(SQLAlchemyError):
            await service.get_all_traces()

    @pytest.mark.asyncio
    async def test_generic_exception_propagates(self, service, mock_trace_repo):
        mock_trace_repo.get_all_traces = AsyncMock(
            side_effect=ValueError("bad")
        )
        with pytest.raises(ValueError):
            await service.get_all_traces()


# =========================================================================
# get_all_traces_for_group
# =========================================================================
class TestGetAllTracesForGroup:

    @pytest.mark.asyncio
    async def test_returns_traces_for_group(
        self, service, mock_history_repo, mock_trace_repo
    ):
        exec1 = _make_execution_obj(id=1, job_id="j1")
        exec2 = _make_execution_obj(id=2, job_id="j2")
        mock_history_repo.get_all_executions_for_groups = AsyncMock(
            return_value=[exec1, exec2]
        )

        t1 = _make_trace_obj(id=1, job_id="j1")
        t2 = _make_trace_obj(id=2, job_id="j2")
        mock_trace_repo.get_by_job_id = AsyncMock(side_effect=[[t1], [t2]])

        ctx = _make_group_context()
        result = await service.get_all_traces_for_group(ctx, limit=10, offset=0)
        assert result.total == 2
        assert len(result.traces) == 2

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_group_context(self, service):
        result = await service.get_all_traces_for_group(group_context=None)
        assert result.total == 0

    @pytest.mark.asyncio
    async def test_returns_empty_when_group_ids_empty(self, service):
        ctx = _make_group_context(group_ids=[])
        result = await service.get_all_traces_for_group(group_context=ctx)
        assert result.total == 0

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_executions(
        self, service, mock_history_repo
    ):
        mock_history_repo.get_all_executions_for_groups = AsyncMock(return_value=[])

        ctx = _make_group_context()
        result = await service.get_all_traces_for_group(ctx)
        assert result.total == 0

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_job_ids(
        self, service, mock_history_repo
    ):
        exec_no_job = _make_execution_obj(job_id=None)
        mock_history_repo.get_all_executions_for_groups = AsyncMock(
            return_value=[exec_no_job]
        )

        ctx = _make_group_context()
        result = await service.get_all_traces_for_group(ctx)
        assert result.total == 0

    @pytest.mark.asyncio
    async def test_pagination_applied(
        self, service, mock_history_repo, mock_trace_repo
    ):
        exec1 = _make_execution_obj(id=1, job_id="j1")
        mock_history_repo.get_all_executions_for_groups = AsyncMock(
            return_value=[exec1]
        )
        traces = [_make_trace_obj(id=i) for i in range(5)]
        mock_trace_repo.get_by_job_id = AsyncMock(return_value=traces)

        ctx = _make_group_context()
        result = await service.get_all_traces_for_group(ctx, limit=2, offset=1)
        assert result.total == 5
        assert len(result.traces) == 2
        assert result.offset == 1

    @pytest.mark.asyncio
    async def test_sqlalchemy_error_propagates(
        self, service, mock_history_repo
    ):
        mock_history_repo.get_all_executions_for_groups = AsyncMock(
            side_effect=SQLAlchemyError("db")
        )
        with pytest.raises(SQLAlchemyError):
            await service.get_all_traces_for_group(_make_group_context())

    @pytest.mark.asyncio
    async def test_generic_exception_propagates(
        self, service, mock_history_repo
    ):
        mock_history_repo.get_all_executions_for_groups = AsyncMock(
            side_effect=RuntimeError("fail")
        )
        with pytest.raises(RuntimeError):
            await service.get_all_traces_for_group(_make_group_context())


# =========================================================================
# get_trace_by_id
# =========================================================================
class TestGetTraceById:

    @pytest.mark.asyncio
    async def test_returns_trace_item(self, service, mock_trace_repo):
        trace = _make_trace_obj(id=42)
        mock_trace_repo.get_by_id = AsyncMock(return_value=trace)

        result = await service.get_trace_by_id(42)
        assert result is not None
        assert result.id == 42

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self, service, mock_trace_repo):
        mock_trace_repo.get_by_id = AsyncMock(return_value=None)
        result = await service.get_trace_by_id(999)
        assert result is None

    @pytest.mark.asyncio
    async def test_sqlalchemy_error_propagates(self, service, mock_trace_repo):
        mock_trace_repo.get_by_id = AsyncMock(
            side_effect=SQLAlchemyError("db")
        )
        with pytest.raises(SQLAlchemyError):
            await service.get_trace_by_id(1)

    @pytest.mark.asyncio
    async def test_generic_exception_propagates(self, service, mock_trace_repo):
        mock_trace_repo.get_by_id = AsyncMock(
            side_effect=RuntimeError("oops")
        )
        with pytest.raises(RuntimeError):
            await service.get_trace_by_id(1)


# =========================================================================
# get_trace_by_id_with_group_check
# =========================================================================
class TestGetTraceByIdWithGroupCheck:

    @pytest.mark.asyncio
    async def test_returns_trace_when_authorized(
        self, service, mock_trace_repo, mock_history_repo
    ):
        trace = _make_trace_obj(id=1, job_id="j1")
        mock_trace_repo.get_by_id = AsyncMock(return_value=trace)
        mock_history_repo.get_execution_by_job_id = AsyncMock(
            return_value=_make_execution_obj()
        )

        ctx = _make_group_context()
        result = await service.get_trace_by_id_with_group_check(1, ctx)
        assert result is not None
        assert result.id == 1

    @pytest.mark.asyncio
    async def test_returns_none_when_trace_not_found(
        self, service, mock_trace_repo
    ):
        mock_trace_repo.get_by_id = AsyncMock(return_value=None)

        ctx = _make_group_context()
        result = await service.get_trace_by_id_with_group_check(1, ctx)
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_not_authorized(
        self, service, mock_trace_repo, mock_history_repo
    ):
        trace = _make_trace_obj(id=1, job_id="j1")
        mock_trace_repo.get_by_id = AsyncMock(return_value=trace)
        mock_history_repo.get_execution_by_job_id = AsyncMock(return_value=None)

        ctx = _make_group_context()
        result = await service.get_trace_by_id_with_group_check(1, ctx)
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_trace_when_no_job_id(
        self, service, mock_trace_repo
    ):
        """Trace without job_id skips group check and returns the trace."""
        trace = _make_trace_obj(id=1, job_id=None)
        mock_trace_repo.get_by_id = AsyncMock(return_value=trace)

        ctx = _make_group_context()
        result = await service.get_trace_by_id_with_group_check(1, ctx)
        assert result is not None

    @pytest.mark.asyncio
    async def test_returns_trace_when_no_group_context(
        self, service, mock_trace_repo
    ):
        """Trace with job_id but no group context skips group check."""
        trace = _make_trace_obj(id=1, job_id="j1")
        mock_trace_repo.get_by_id = AsyncMock(return_value=trace)

        result = await service.get_trace_by_id_with_group_check(
            1, SimpleNamespace(group_ids=None)
        )
        assert result is not None

    @pytest.mark.asyncio
    async def test_returns_trace_when_group_ids_empty(
        self, service, mock_trace_repo
    ):
        """Empty group_ids list skips group check."""
        trace = _make_trace_obj(id=1, job_id="j1")
        mock_trace_repo.get_by_id = AsyncMock(return_value=trace)

        result = await service.get_trace_by_id_with_group_check(
            1, SimpleNamespace(group_ids=[])
        )
        assert result is not None

    @pytest.mark.asyncio
    async def test_sqlalchemy_error_propagates(
        self, service, mock_trace_repo
    ):
        mock_trace_repo.get_by_id = AsyncMock(
            side_effect=SQLAlchemyError("db")
        )
        with pytest.raises(SQLAlchemyError):
            await service.get_trace_by_id_with_group_check(
                1, _make_group_context()
            )

    @pytest.mark.asyncio
    async def test_generic_exception_propagates(
        self, service, mock_trace_repo
    ):
        mock_trace_repo.get_by_id = AsyncMock(
            side_effect=RuntimeError("fail")
        )
        with pytest.raises(RuntimeError):
            await service.get_trace_by_id_with_group_check(
                1, _make_group_context()
            )


# =========================================================================
# create_trace
# =========================================================================
class TestCreateTrace:

    @pytest.mark.asyncio
    async def test_creates_trace_and_broadcasts_sse(
        self, service, mock_trace_repo
    ):
        created = _make_trace_obj(id=7, job_id="j1", event_type="task_started")
        mock_trace_repo.create = AsyncMock(return_value=created)

        with patch(
            "src.services.execution_trace_service.sse_manager"
        ) as mock_sse, patch.dict(
            "os.environ", {}, clear=False
        ):
            mock_sse.get_statistics.return_value = {
                "total_connections": 1,
                "active_jobs": 1,
            }
            mock_sse.broadcast_to_job = AsyncMock(return_value=1)

            result = await service.create_trace(
                {"job_id": "j1", "event_type": "task_started"}
            )

            assert result.id == 7
            mock_sse.broadcast_to_job.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_skips_sse_in_subprocess_mode(
        self, service, mock_trace_repo
    ):
        created = _make_trace_obj(id=8, job_id="j1")
        mock_trace_repo.create = AsyncMock(return_value=created)

        with patch(
            "src.services.execution_trace_service.sse_manager"
        ) as mock_sse, patch.dict(
            "os.environ", {"CREW_SUBPROCESS_MODE": "true"}, clear=False
        ):
            # Pre-configure broadcast_to_job as AsyncMock so we can assert on it
            mock_sse.broadcast_to_job = AsyncMock()

            result = await service.create_trace({"job_id": "j1"})
            assert result.id == 8
            mock_sse.broadcast_to_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_sse_failure_does_not_fail_trace_creation(
        self, service, mock_trace_repo
    ):
        created = _make_trace_obj(id=9, job_id="j1")
        mock_trace_repo.create = AsyncMock(return_value=created)

        with patch(
            "src.services.execution_trace_service.sse_manager"
        ) as mock_sse, patch.dict(
            "os.environ", {}, clear=False
        ):
            mock_sse.get_statistics.return_value = {
                "total_connections": 0,
                "active_jobs": 0,
            }
            mock_sse.broadcast_to_job = AsyncMock(
                side_effect=RuntimeError("sse broken")
            )

            result = await service.create_trace({"job_id": "j1"})
            assert result.id == 9

    @pytest.mark.asyncio
    async def test_no_job_id_skips_sse(
        self, service, mock_trace_repo
    ):
        created = _make_trace_obj(id=10, job_id=None)
        mock_trace_repo.create = AsyncMock(return_value=created)

        with patch(
            "src.services.execution_trace_service.sse_manager"
        ) as mock_sse, patch.dict(
            "os.environ", {}, clear=False
        ):
            # Pre-configure broadcast_to_job as AsyncMock so we can assert on it
            mock_sse.broadcast_to_job = AsyncMock()

            result = await service.create_trace({"event_type": "test"})
            assert result.id == 10
            mock_sse.broadcast_to_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_value_error_propagates(
        self, service, mock_trace_repo
    ):
        mock_trace_repo.create = AsyncMock(
            side_effect=ValueError("Job xyz does not exist")
        )
        with pytest.raises(ValueError, match="Job xyz does not exist"):
            await service.create_trace({"job_id": "xyz"})

    @pytest.mark.asyncio
    async def test_sqlalchemy_error_propagates(
        self, service, mock_trace_repo
    ):
        mock_trace_repo.create = AsyncMock(
            side_effect=SQLAlchemyError("db err")
        )
        with pytest.raises(SQLAlchemyError):
            await service.create_trace({"job_id": "j1"})

    @pytest.mark.asyncio
    async def test_generic_exception_propagates(
        self, service, mock_trace_repo
    ):
        mock_trace_repo.create = AsyncMock(
            side_effect=RuntimeError("boom")
        )
        with pytest.raises(RuntimeError):
            await service.create_trace({"job_id": "j1"})

    @pytest.mark.asyncio
    async def test_default_event_type_unknown(
        self, service, mock_trace_repo
    ):
        """When event_type not provided, it defaults to 'unknown' in the log."""
        created = _make_trace_obj(id=11, job_id=None, event_type="unknown")
        mock_trace_repo.create = AsyncMock(return_value=created)

        with patch.dict("os.environ", {}, clear=False):
            result = await service.create_trace({})
            assert result is not None


# =========================================================================
# create_trace_with_group
# =========================================================================
class TestCreateTraceWithGroup:

    @pytest.mark.asyncio
    async def test_creates_when_authorized(
        self, service, mock_trace_repo, mock_history_repo
    ):
        mock_history_repo.get_execution_by_job_id = AsyncMock(
            return_value=_make_execution_obj()
        )
        created = _make_trace_obj(id=20)
        mock_trace_repo.create = AsyncMock(return_value=created)

        ctx = _make_group_context()
        result = await service.create_trace_with_group(
            {"job_id": "j1"}, ctx
        )
        assert result.id == 20

    @pytest.mark.asyncio
    async def test_raises_when_not_authorized(
        self, service, mock_history_repo
    ):
        mock_history_repo.get_execution_by_job_id = AsyncMock(return_value=None)

        ctx = _make_group_context()
        with pytest.raises(ValueError, match="Not authorized"):
            await service.create_trace_with_group({"job_id": "j1"}, ctx)

    @pytest.mark.asyncio
    async def test_creates_when_no_job_id(
        self, service, mock_trace_repo
    ):
        """When job_id is not in trace_data, no group check is done."""
        created = _make_trace_obj(id=21, job_id=None)
        mock_trace_repo.create = AsyncMock(return_value=created)

        ctx = _make_group_context()
        result = await service.create_trace_with_group({}, ctx)
        assert result.id == 21

    @pytest.mark.asyncio
    async def test_creates_when_job_id_empty_string(
        self, service, mock_trace_repo
    ):
        """Empty string job_id should skip group check."""
        created = _make_trace_obj(id=22, job_id="")
        mock_trace_repo.create = AsyncMock(return_value=created)

        ctx = _make_group_context()
        result = await service.create_trace_with_group({"job_id": ""}, ctx)
        assert result.id == 22

    @pytest.mark.asyncio
    async def test_creates_when_no_group_context(
        self, service, mock_trace_repo
    ):
        """No group context should skip authorization."""
        created = _make_trace_obj(id=23)
        mock_trace_repo.create = AsyncMock(return_value=created)

        result = await service.create_trace_with_group(
            {"job_id": "j1"}, None
        )
        assert result.id == 23

    @pytest.mark.asyncio
    async def test_creates_when_group_ids_empty(
        self, service, mock_trace_repo
    ):
        """Empty group_ids list should skip authorization."""
        created = _make_trace_obj(id=24)
        mock_trace_repo.create = AsyncMock(return_value=created)

        ctx = _make_group_context(group_ids=[])
        result = await service.create_trace_with_group(
            {"job_id": "j1"}, ctx
        )
        assert result.id == 24

    @pytest.mark.asyncio
    async def test_sqlalchemy_error_propagates(
        self, service, mock_trace_repo
    ):
        mock_trace_repo.create = AsyncMock(
            side_effect=SQLAlchemyError("db")
        )
        with pytest.raises(SQLAlchemyError):
            await service.create_trace_with_group({}, _make_group_context())

    @pytest.mark.asyncio
    async def test_generic_exception_propagates(
        self, service, mock_trace_repo
    ):
        mock_trace_repo.create = AsyncMock(
            side_effect=RuntimeError("nope")
        )
        with pytest.raises(RuntimeError):
            await service.create_trace_with_group({}, _make_group_context())


# =========================================================================
# delete_trace
# =========================================================================
class TestDeleteTrace:

    @pytest.mark.asyncio
    async def test_deletes_existing_trace(self, service, mock_trace_repo):
        trace = _make_trace_obj(id=5)
        mock_trace_repo.get_by_id = AsyncMock(return_value=trace)
        mock_trace_repo.delete_by_id = AsyncMock(return_value=1)

        result = await service.delete_trace(5)
        assert result is not None
        assert result.deleted_traces == 1
        assert "5" in result.message

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self, service, mock_trace_repo):
        mock_trace_repo.get_by_id = AsyncMock(return_value=None)

        result = await service.delete_trace(999)
        assert result is None

    @pytest.mark.asyncio
    async def test_sqlalchemy_error_propagates(self, service, mock_trace_repo):
        mock_trace_repo.get_by_id = AsyncMock(
            side_effect=SQLAlchemyError("db")
        )
        with pytest.raises(SQLAlchemyError):
            await service.delete_trace(1)

    @pytest.mark.asyncio
    async def test_generic_exception_propagates(self, service, mock_trace_repo):
        mock_trace_repo.get_by_id = AsyncMock(
            side_effect=RuntimeError("err")
        )
        with pytest.raises(RuntimeError):
            await service.delete_trace(1)


# =========================================================================
# delete_trace_with_group_check
# =========================================================================
class TestDeleteTraceWithGroupCheck:

    @pytest.mark.asyncio
    async def test_deletes_when_authorized(
        self, service, mock_trace_repo, mock_history_repo
    ):
        trace = _make_trace_obj(id=5, job_id="j1")
        mock_trace_repo.get_by_id = AsyncMock(return_value=trace)
        mock_history_repo.get_execution_by_job_id = AsyncMock(
            return_value=_make_execution_obj()
        )
        mock_trace_repo.delete_by_id = AsyncMock(return_value=1)

        ctx = _make_group_context()
        result = await service.delete_trace_with_group_check(5, ctx)
        assert result is not None
        assert result.deleted_traces == 1

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(
        self, service, mock_trace_repo
    ):
        mock_trace_repo.get_by_id = AsyncMock(return_value=None)

        result = await service.delete_trace_with_group_check(
            999, _make_group_context()
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_not_authorized(
        self, service, mock_trace_repo, mock_history_repo
    ):
        trace = _make_trace_obj(id=5, job_id="j1")
        mock_trace_repo.get_by_id = AsyncMock(return_value=trace)
        mock_history_repo.get_execution_by_job_id = AsyncMock(return_value=None)

        ctx = _make_group_context()
        result = await service.delete_trace_with_group_check(5, ctx)
        assert result is None

    @pytest.mark.asyncio
    async def test_deletes_when_no_job_id_on_trace(
        self, service, mock_trace_repo
    ):
        """Trace without job_id skips group check, proceeds to delete."""
        trace = _make_trace_obj(id=5, job_id=None)
        mock_trace_repo.get_by_id = AsyncMock(return_value=trace)
        mock_trace_repo.delete_by_id = AsyncMock(return_value=1)

        ctx = _make_group_context()
        result = await service.delete_trace_with_group_check(5, ctx)
        assert result is not None

    @pytest.mark.asyncio
    async def test_deletes_when_no_group_context(
        self, service, mock_trace_repo
    ):
        """No group_context skips auth check."""
        trace = _make_trace_obj(id=5, job_id="j1")
        mock_trace_repo.get_by_id = AsyncMock(return_value=trace)
        mock_trace_repo.delete_by_id = AsyncMock(return_value=1)

        result = await service.delete_trace_with_group_check(
            5, SimpleNamespace(group_ids=None)
        )
        assert result is not None

    @pytest.mark.asyncio
    async def test_deletes_when_group_ids_empty(
        self, service, mock_trace_repo
    ):
        trace = _make_trace_obj(id=5, job_id="j1")
        mock_trace_repo.get_by_id = AsyncMock(return_value=trace)
        mock_trace_repo.delete_by_id = AsyncMock(return_value=1)

        result = await service.delete_trace_with_group_check(
            5, SimpleNamespace(group_ids=[])
        )
        assert result is not None

    @pytest.mark.asyncio
    async def test_sqlalchemy_error_propagates(
        self, service, mock_trace_repo
    ):
        mock_trace_repo.get_by_id = AsyncMock(
            side_effect=SQLAlchemyError("db")
        )
        with pytest.raises(SQLAlchemyError):
            await service.delete_trace_with_group_check(
                1, _make_group_context()
            )

    @pytest.mark.asyncio
    async def test_generic_exception_propagates(
        self, service, mock_trace_repo
    ):
        mock_trace_repo.get_by_id = AsyncMock(
            side_effect=RuntimeError("fail")
        )
        with pytest.raises(RuntimeError):
            await service.delete_trace_with_group_check(
                1, _make_group_context()
            )


# =========================================================================
# delete_traces_by_run_id
# =========================================================================
class TestDeleteTracesByRunId:

    @pytest.mark.asyncio
    async def test_deletes_and_returns_count(self, service, mock_trace_repo):
        mock_trace_repo.delete_by_run_id = AsyncMock(return_value=3)

        result = await service.delete_traces_by_run_id(10)
        assert result.deleted_traces == 3
        assert "10" in result.message

    @pytest.mark.asyncio
    async def test_zero_deleted(self, service, mock_trace_repo):
        mock_trace_repo.delete_by_run_id = AsyncMock(return_value=0)

        result = await service.delete_traces_by_run_id(10)
        assert result.deleted_traces == 0

    @pytest.mark.asyncio
    async def test_sqlalchemy_error_propagates(self, service, mock_trace_repo):
        mock_trace_repo.delete_by_run_id = AsyncMock(
            side_effect=SQLAlchemyError("db")
        )
        with pytest.raises(SQLAlchemyError):
            await service.delete_traces_by_run_id(10)

    @pytest.mark.asyncio
    async def test_generic_exception_propagates(self, service, mock_trace_repo):
        mock_trace_repo.delete_by_run_id = AsyncMock(
            side_effect=RuntimeError("err")
        )
        with pytest.raises(RuntimeError):
            await service.delete_traces_by_run_id(10)


# =========================================================================
# delete_traces_by_run_id_with_group_check
# =========================================================================
class TestDeleteTracesByRunIdWithGroupCheck:

    @pytest.mark.asyncio
    async def test_deletes_when_authorized(
        self, service, mock_history_repo, mock_trace_repo
    ):
        mock_history_repo.get_execution_by_id = AsyncMock(
            return_value=_make_execution_obj()
        )
        mock_trace_repo.delete_by_run_id = AsyncMock(return_value=2)

        ctx = _make_group_context()
        result = await service.delete_traces_by_run_id_with_group_check(10, ctx)
        assert result.deleted_traces == 2

    @pytest.mark.asyncio
    async def test_returns_zero_when_not_authorized(
        self, service, mock_history_repo
    ):
        mock_history_repo.get_execution_by_id = AsyncMock(return_value=None)

        ctx = _make_group_context()
        result = await service.delete_traces_by_run_id_with_group_check(10, ctx)
        assert result.deleted_traces == 0
        assert "not authorized" in result.message.lower() or "not found" in result.message.lower()

    @pytest.mark.asyncio
    async def test_deletes_when_no_group_context(
        self, service, mock_trace_repo
    ):
        """No group context skips authorization, proceeds to delete."""
        mock_trace_repo.delete_by_run_id = AsyncMock(return_value=1)

        result = await service.delete_traces_by_run_id_with_group_check(10, None)
        assert result.deleted_traces == 1

    @pytest.mark.asyncio
    async def test_deletes_when_group_ids_empty(
        self, service, mock_trace_repo
    ):
        mock_trace_repo.delete_by_run_id = AsyncMock(return_value=1)

        ctx = _make_group_context(group_ids=[])
        result = await service.delete_traces_by_run_id_with_group_check(10, ctx)
        assert result.deleted_traces == 1

    @pytest.mark.asyncio
    async def test_sqlalchemy_error_propagates(
        self, service, mock_history_repo
    ):
        mock_history_repo.get_execution_by_id = AsyncMock(
            side_effect=SQLAlchemyError("db")
        )
        with pytest.raises(SQLAlchemyError):
            await service.delete_traces_by_run_id_with_group_check(
                10, _make_group_context()
            )

    @pytest.mark.asyncio
    async def test_generic_exception_propagates(
        self, service, mock_history_repo
    ):
        mock_history_repo.get_execution_by_id = AsyncMock(
            side_effect=RuntimeError("err")
        )
        with pytest.raises(RuntimeError):
            await service.delete_traces_by_run_id_with_group_check(
                10, _make_group_context()
            )


# =========================================================================
# delete_traces_by_job_id
# =========================================================================
class TestDeleteTracesByJobId:

    @pytest.mark.asyncio
    async def test_deletes_and_returns_count(self, service, mock_trace_repo):
        mock_trace_repo.delete_by_job_id = AsyncMock(return_value=5)

        result = await service.delete_traces_by_job_id("j1")
        assert result.deleted_traces == 5
        assert "j1" in result.message

    @pytest.mark.asyncio
    async def test_zero_deleted(self, service, mock_trace_repo):
        mock_trace_repo.delete_by_job_id = AsyncMock(return_value=0)

        result = await service.delete_traces_by_job_id("j1")
        assert result.deleted_traces == 0

    @pytest.mark.asyncio
    async def test_sqlalchemy_error_propagates(self, service, mock_trace_repo):
        mock_trace_repo.delete_by_job_id = AsyncMock(
            side_effect=SQLAlchemyError("db")
        )
        with pytest.raises(SQLAlchemyError):
            await service.delete_traces_by_job_id("j1")

    @pytest.mark.asyncio
    async def test_generic_exception_propagates(self, service, mock_trace_repo):
        mock_trace_repo.delete_by_job_id = AsyncMock(
            side_effect=RuntimeError("fail")
        )
        with pytest.raises(RuntimeError):
            await service.delete_traces_by_job_id("j1")


# =========================================================================
# delete_traces_by_job_id_with_group_check
# =========================================================================
class TestDeleteTracesByJobIdWithGroupCheck:

    @pytest.mark.asyncio
    async def test_deletes_when_authorized(
        self, service, mock_history_repo, mock_trace_repo
    ):
        mock_history_repo.get_execution_by_job_id = AsyncMock(
            return_value=_make_execution_obj()
        )
        mock_trace_repo.delete_by_job_id = AsyncMock(return_value=3)

        ctx = _make_group_context()
        result = await service.delete_traces_by_job_id_with_group_check("j1", ctx)
        assert result.deleted_traces == 3

    @pytest.mark.asyncio
    async def test_returns_zero_when_not_authorized(
        self, service, mock_history_repo
    ):
        mock_history_repo.get_execution_by_job_id = AsyncMock(return_value=None)

        ctx = _make_group_context()
        result = await service.delete_traces_by_job_id_with_group_check("j1", ctx)
        assert result.deleted_traces == 0
        assert "not found" in result.message.lower() or "not authorized" in result.message.lower()

    @pytest.mark.asyncio
    async def test_deletes_when_no_group_context(
        self, service, mock_trace_repo
    ):
        mock_trace_repo.delete_by_job_id = AsyncMock(return_value=2)

        result = await service.delete_traces_by_job_id_with_group_check("j1", None)
        assert result.deleted_traces == 2

    @pytest.mark.asyncio
    async def test_deletes_when_group_ids_empty(
        self, service, mock_trace_repo
    ):
        mock_trace_repo.delete_by_job_id = AsyncMock(return_value=2)

        ctx = _make_group_context(group_ids=[])
        result = await service.delete_traces_by_job_id_with_group_check("j1", ctx)
        assert result.deleted_traces == 2

    @pytest.mark.asyncio
    async def test_sqlalchemy_error_propagates(
        self, service, mock_history_repo
    ):
        mock_history_repo.get_execution_by_job_id = AsyncMock(
            side_effect=SQLAlchemyError("db")
        )
        with pytest.raises(SQLAlchemyError):
            await service.delete_traces_by_job_id_with_group_check(
                "j1", _make_group_context()
            )

    @pytest.mark.asyncio
    async def test_generic_exception_propagates(
        self, service, mock_history_repo
    ):
        mock_history_repo.get_execution_by_job_id = AsyncMock(
            side_effect=RuntimeError("err")
        )
        with pytest.raises(RuntimeError):
            await service.delete_traces_by_job_id_with_group_check(
                "j1", _make_group_context()
            )


# =========================================================================
# delete_all_traces
# =========================================================================
class TestDeleteAllTraces:

    @pytest.mark.asyncio
    async def test_deletes_all(self, service, mock_trace_repo):
        mock_trace_repo.delete_all = AsyncMock(return_value=100)

        result = await service.delete_all_traces()
        assert result.deleted_traces == 100
        assert "100" in result.message

    @pytest.mark.asyncio
    async def test_zero_deleted(self, service, mock_trace_repo):
        mock_trace_repo.delete_all = AsyncMock(return_value=0)

        result = await service.delete_all_traces()
        assert result.deleted_traces == 0

    @pytest.mark.asyncio
    async def test_sqlalchemy_error_propagates(self, service, mock_trace_repo):
        mock_trace_repo.delete_all = AsyncMock(
            side_effect=SQLAlchemyError("db")
        )
        with pytest.raises(SQLAlchemyError):
            await service.delete_all_traces()

    @pytest.mark.asyncio
    async def test_generic_exception_propagates(self, service, mock_trace_repo):
        mock_trace_repo.delete_all = AsyncMock(
            side_effect=RuntimeError("fail")
        )
        with pytest.raises(RuntimeError):
            await service.delete_all_traces()


# =========================================================================
# delete_all_traces_for_group
# =========================================================================
class TestDeleteAllTracesForGroup:

    @pytest.mark.asyncio
    async def test_deletes_for_group(
        self, service, mock_history_repo, mock_trace_repo
    ):
        exec1 = _make_execution_obj(id=1, job_id="j1")
        exec2 = _make_execution_obj(id=2, job_id="j2")
        mock_history_repo.get_all_executions_for_groups = AsyncMock(
            return_value=[exec1, exec2]
        )
        mock_trace_repo.delete_by_job_id = AsyncMock(side_effect=[3, 2])

        ctx = _make_group_context()
        result = await service.delete_all_traces_for_group(ctx)
        assert result.deleted_traces == 5

    @pytest.mark.asyncio
    async def test_returns_zero_when_no_group_context(self, service):
        result = await service.delete_all_traces_for_group(None)
        assert result.deleted_traces == 0
        assert "no group context" in result.message.lower()

    @pytest.mark.asyncio
    async def test_returns_zero_when_group_ids_empty(self, service):
        ctx = _make_group_context(group_ids=[])
        result = await service.delete_all_traces_for_group(ctx)
        assert result.deleted_traces == 0

    @pytest.mark.asyncio
    async def test_returns_zero_when_group_ids_none(self, service):
        ctx = _make_group_context(group_ids=None)
        result = await service.delete_all_traces_for_group(ctx)
        assert result.deleted_traces == 0

    @pytest.mark.asyncio
    async def test_returns_zero_when_no_executions(
        self, service, mock_history_repo
    ):
        mock_history_repo.get_all_executions_for_groups = AsyncMock(
            return_value=[]
        )
        ctx = _make_group_context()
        result = await service.delete_all_traces_for_group(ctx)
        assert result.deleted_traces == 0
        assert "no executions" in result.message.lower()

    @pytest.mark.asyncio
    async def test_skips_executions_without_job_id(
        self, service, mock_history_repo, mock_trace_repo
    ):
        exec_no_job = _make_execution_obj(id=1, job_id=None)
        exec_with_job = _make_execution_obj(id=2, job_id="j2")
        mock_history_repo.get_all_executions_for_groups = AsyncMock(
            return_value=[exec_no_job, exec_with_job]
        )
        mock_trace_repo.delete_by_job_id = AsyncMock(return_value=4)

        ctx = _make_group_context()
        result = await service.delete_all_traces_for_group(ctx)
        assert result.deleted_traces == 4
        # Only called once for exec_with_job
        mock_trace_repo.delete_by_job_id.assert_awaited_once_with("j2")

    @pytest.mark.asyncio
    async def test_sqlalchemy_error_propagates(
        self, service, mock_history_repo
    ):
        mock_history_repo.get_all_executions_for_groups = AsyncMock(
            side_effect=SQLAlchemyError("db")
        )
        with pytest.raises(SQLAlchemyError):
            await service.delete_all_traces_for_group(_make_group_context())

    @pytest.mark.asyncio
    async def test_generic_exception_propagates(
        self, service, mock_history_repo
    ):
        mock_history_repo.get_all_executions_for_groups = AsyncMock(
            side_effect=RuntimeError("fail")
        )
        with pytest.raises(RuntimeError):
            await service.delete_all_traces_for_group(_make_group_context())


# =========================================================================
# __init__ test
# =========================================================================
class TestInit:

    def test_service_initializes_repositories(self, mock_session):
        with patch(
            "src.services.execution_trace_service.ExecutionTraceRepository"
        ) as mock_tr_cls, patch(
            "src.services.execution_trace_service.ExecutionHistoryRepository"
        ) as mock_hr_cls:
            from src.services.execution_trace_service import ExecutionTraceService
            svc = ExecutionTraceService(mock_session)

            mock_tr_cls.assert_called_once_with(mock_session)
            mock_hr_cls.assert_called_once_with(mock_session)
            assert svc.session is mock_session
