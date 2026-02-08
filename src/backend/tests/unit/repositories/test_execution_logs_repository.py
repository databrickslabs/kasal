"""
Unit tests for ExecutionLogsRepository.

Tests the session-injected repository pattern including:
- CRUD operations (create, read, delete)
- Group-based multi-tenant filtering
- Timestamp normalization
- Error handling and rollback behavior
- Pagination and ordering
- SQL injection prevention via ORM
"""
import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timezone, timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from src.repositories.execution_logs_repository import ExecutionLogsRepository
from src.models.execution_logs import ExecutionLog


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class MockScalars:
    """Mock for SQLAlchemy Result.scalars()."""

    def __init__(self, results):
        self._results = results

    def first(self):
        return self._results[0] if self._results else None

    def all(self):
        return self._results


class MockResult:
    """Mock for SQLAlchemy Result object."""

    def __init__(self, results=None, scalar_value=None, rowcount=0):
        self._scalars = MockScalars(results or [])
        self._scalar_value = scalar_value
        self.rowcount = rowcount

    def scalars(self):
        return self._scalars

    def scalar_one(self):
        return self._scalar_value


def _make_log(
    log_id=1,
    execution_id="exec-123",
    content="Test log",
    timestamp=None,
    group_id=None,
    group_email=None,
):
    """Create a mock ExecutionLog-like object."""
    log = MagicMock(spec=ExecutionLog)
    log.id = log_id
    log.execution_id = execution_id
    log.content = content
    log.timestamp = timestamp or datetime(2024, 1, 15, 12, 0, 0)
    log.group_id = group_id
    log.group_email = group_email
    return log


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_session():
    """Create a mock AsyncSession with common methods."""
    session = AsyncMock(spec=AsyncSession)
    session.add = MagicMock()  # session.add is synchronous
    session.flush = AsyncMock()
    session.execute = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def repo(mock_session):
    """Create an ExecutionLogsRepository with an injected mock session."""
    return ExecutionLogsRepository(session=mock_session)


# ===========================================================================
# TestNormalizeTimestamp
# ===========================================================================

class TestNormalizeTimestamp:
    """Tests for _normalize_timestamp - a synchronous helper method."""

    def test_none_returns_none(self, repo):
        assert repo._normalize_timestamp(None) is None

    def test_naive_datetime_returned_as_is(self, repo):
        naive = datetime(2024, 6, 15, 10, 30, 0)
        result = repo._normalize_timestamp(naive)
        assert result == naive
        assert result.tzinfo is None

    def test_utc_aware_datetime_stripped(self, repo):
        aware = datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
        result = repo._normalize_timestamp(aware)
        assert result.tzinfo is None
        assert result == datetime(2024, 6, 15, 10, 30, 0)

    def test_non_utc_aware_datetime_converted_to_utc(self, repo):
        eastern = timezone(timedelta(hours=-5))
        aware = datetime(2024, 6, 15, 10, 0, 0, tzinfo=eastern)
        result = repo._normalize_timestamp(aware)
        assert result.tzinfo is None
        # 10:00 EST = 15:00 UTC
        assert result.hour == 15

    def test_iso_string_parsed(self, repo):
        iso_str = "2024-06-15T10:30:00"
        result = repo._normalize_timestamp(iso_str)
        assert isinstance(result, datetime)
        assert result == datetime(2024, 6, 15, 10, 30, 0)

    def test_iso_string_with_tz_converted(self, repo):
        iso_str = "2024-06-15T10:30:00+05:00"
        result = repo._normalize_timestamp(iso_str)
        assert result.tzinfo is None
        # 10:30 +05:00 = 05:30 UTC
        assert result.hour == 5
        assert result.minute == 30

    def test_invalid_string_returns_none(self, repo):
        assert repo._normalize_timestamp("not-a-date") is None

    def test_unsupported_type_returns_none(self, repo):
        assert repo._normalize_timestamp(12345) is None
        assert repo._normalize_timestamp([]) is None

    def test_empty_string_returns_none(self, repo):
        assert repo._normalize_timestamp("") is None


# ===========================================================================
# TestCreateLog
# ===========================================================================

class TestCreateLog:
    """Tests for create_log method."""

    @pytest.mark.asyncio
    async def test_create_log_basic(self, repo, mock_session):
        """create_log adds an ExecutionLog to session and flushes."""
        result = await repo.create_log(
            execution_id="exec-1",
            content="hello world",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
        )

        mock_session.add.assert_called_once()
        added_obj = mock_session.add.call_args[0][0]
        assert isinstance(added_obj, ExecutionLog)
        assert added_obj.execution_id == "exec-1"
        assert added_obj.content == "hello world"
        mock_session.flush.assert_awaited_once()
        assert result is added_obj

    @pytest.mark.asyncio
    async def test_create_log_with_group_fields(self, repo, mock_session):
        """create_log stores group_id and group_email for multi-tenancy."""
        await repo.create_log(
            execution_id="exec-2",
            content="group log",
            group_id="grp-abc",
            group_email="user@example.com",
        )

        added_obj = mock_session.add.call_args[0][0]
        assert added_obj.group_id == "grp-abc"
        assert added_obj.group_email == "user@example.com"

    @pytest.mark.asyncio
    async def test_create_log_without_group_fields(self, repo, mock_session):
        """create_log defaults group fields to None."""
        await repo.create_log(
            execution_id="exec-3",
            content="no group",
        )

        added_obj = mock_session.add.call_args[0][0]
        assert added_obj.group_id is None
        assert added_obj.group_email is None

    @pytest.mark.asyncio
    async def test_create_log_normalizes_timestamp(self, repo, mock_session):
        """Timezone-aware timestamps are normalized to naive UTC."""
        tz_aware = datetime(2024, 3, 10, 8, 0, 0, tzinfo=timezone(timedelta(hours=3)))
        await repo.create_log(
            execution_id="exec-5",
            content="tz log",
            timestamp=tz_aware,
        )

        added_obj = mock_session.add.call_args[0][0]
        # 08:00 +03:00 = 05:00 UTC
        assert added_obj.timestamp.hour == 5
        assert added_obj.timestamp.tzinfo is None

    @pytest.mark.asyncio
    async def test_create_log_error_triggers_rollback_and_reraise(self, repo, mock_session):
        """On flush failure, rollback is attempted and exception is re-raised."""
        mock_session.flush.side_effect = Exception("DB write failed")

        with pytest.raises(Exception, match="DB write failed"):
            await repo.create_log(execution_id="exec-err", content="will fail")

        mock_session.rollback.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_create_log_rollback_failure_still_reraises(self, repo, mock_session):
        """Even if rollback fails, the original exception is re-raised."""
        mock_session.flush.side_effect = Exception("DB write failed")
        mock_session.rollback.side_effect = Exception("Rollback also failed")

        with pytest.raises(Exception, match="DB write failed"):
            await repo.create_log(execution_id="exec-err2", content="double fail")


# ===========================================================================
# TestGetLogsByExecutionId
# ===========================================================================

class TestGetLogsByExecutionId:
    """Tests for get_logs_by_execution_id method."""

    @pytest.mark.asyncio
    async def test_basic_retrieval(self, repo, mock_session):
        logs = [_make_log(log_id=i) for i in range(3)]
        mock_session.execute.return_value = MockResult(results=logs)

        result = await repo.get_logs_by_execution_id("exec-123")

        assert result == logs
        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_logs(self, repo, mock_session):
        mock_session.execute.return_value = MockResult(results=[])
        result = await repo.get_logs_by_execution_id("nonexistent")
        assert result == []

    @pytest.mark.asyncio
    async def test_without_group_ids_no_group_filter(self, repo, mock_session):
        """When group_ids is None, no .in_ filter is applied."""
        mock_session.execute.return_value = MockResult(results=[])
        await repo.get_logs_by_execution_id("exec-1", group_ids=None)

        query = mock_session.execute.call_args[0][0]
        compiled = str(query.compile(compile_kwargs={"literal_binds": True}))
        assert "group_id IN" not in compiled

    @pytest.mark.asyncio
    async def test_with_group_ids_adds_in_filter(self, repo, mock_session):
        """When group_ids is provided, .in_ filter is applied."""
        mock_session.execute.return_value = MockResult(results=[])
        await repo.get_logs_by_execution_id("exec-1", group_ids=["grp-1", "grp-2"])

        query = mock_session.execute.call_args[0][0]
        compiled = str(query.compile(compile_kwargs={"literal_binds": True}))
        assert "group_id IN" in compiled

    @pytest.mark.asyncio
    async def test_with_empty_group_ids_list_no_filter(self, repo, mock_session):
        """An empty group_ids list is falsy, so no filter is applied."""
        mock_session.execute.return_value = MockResult(results=[])
        await repo.get_logs_by_execution_id("exec-1", group_ids=[])

        query = mock_session.execute.call_args[0][0]
        compiled = str(query.compile(compile_kwargs={"literal_binds": True}))
        assert "group_id IN" not in compiled

    @pytest.mark.asyncio
    async def test_newest_first_ordering(self, repo, mock_session):
        mock_session.execute.return_value = MockResult(results=[])
        await repo.get_logs_by_execution_id("exec-1", newest_first=True)

        query = mock_session.execute.call_args[0][0]
        compiled = str(query.compile(compile_kwargs={"literal_binds": True}))
        assert "DESC" in compiled

    @pytest.mark.asyncio
    async def test_oldest_first_ordering(self, repo, mock_session):
        mock_session.execute.return_value = MockResult(results=[])
        await repo.get_logs_by_execution_id("exec-1", newest_first=False)

        query = mock_session.execute.call_args[0][0]
        compiled = str(query.compile(compile_kwargs={"literal_binds": True}))
        assert "DESC" not in compiled

    @pytest.mark.asyncio
    async def test_pagination_limit_and_offset(self, repo, mock_session):
        mock_session.execute.return_value = MockResult(results=[])
        await repo.get_logs_by_execution_id("exec-1", limit=50, offset=10)

        query = mock_session.execute.call_args[0][0]
        compiled = str(query.compile(compile_kwargs={"literal_binds": True}))
        assert "LIMIT" in compiled
        assert "OFFSET" in compiled


# ===========================================================================
# TestGetById
# ===========================================================================

class TestGetById:
    @pytest.mark.asyncio
    async def test_found(self, repo, mock_session):
        log = _make_log(log_id=42)
        mock_session.execute.return_value = MockResult(results=[log])
        result = await repo.get_by_id(42)
        assert result is log

    @pytest.mark.asyncio
    async def test_not_found(self, repo, mock_session):
        mock_session.execute.return_value = MockResult(results=[])
        result = await repo.get_by_id(999)
        assert result is None


# ===========================================================================
# TestDeleteByExecutionId
# ===========================================================================

class TestDeleteByExecutionId:
    @pytest.mark.asyncio
    async def test_delete_returns_rowcount(self, repo, mock_session):
        mock_session.execute.return_value = MockResult(rowcount=5)
        result = await repo.delete_by_execution_id("exec-123")
        assert result == 5
        mock_session.flush.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_delete_no_matches(self, repo, mock_session):
        mock_session.execute.return_value = MockResult(rowcount=0)
        result = await repo.delete_by_execution_id("nonexistent")
        assert result == 0

    @pytest.mark.asyncio
    async def test_delete_without_group_ids(self, repo, mock_session):
        mock_session.execute.return_value = MockResult(rowcount=3)
        await repo.delete_by_execution_id("exec-1", group_ids=None)

        stmt = mock_session.execute.call_args[0][0]
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "group_id IN" not in compiled

    @pytest.mark.asyncio
    async def test_delete_with_group_ids(self, repo, mock_session):
        """With group_ids, delete includes .in_ filter for tenant isolation."""
        mock_session.execute.return_value = MockResult(rowcount=2)
        await repo.delete_by_execution_id("exec-1", group_ids=["grp-a", "grp-b"])

        stmt = mock_session.execute.call_args[0][0]
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "group_id IN" in compiled

    @pytest.mark.asyncio
    async def test_delete_uses_orm_not_raw_sql(self, repo, mock_session):
        """Verify delete uses ORM delete() - not raw SQL text()."""
        mock_session.execute.return_value = MockResult(rowcount=1)
        await repo.delete_by_execution_id("exec-1")

        stmt = mock_session.execute.call_args[0][0]
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "DELETE FROM execution_logs" in compiled
        assert "execution_id" in compiled


# ===========================================================================
# TestDeleteAll
# ===========================================================================

class TestDeleteAll:
    @pytest.mark.asyncio
    async def test_delete_all_returns_rowcount(self, repo, mock_session):
        mock_session.execute.return_value = MockResult(rowcount=100)
        result = await repo.delete_all()
        assert result == 100
        mock_session.flush.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_delete_all_empty_table(self, repo, mock_session):
        mock_session.execute.return_value = MockResult(rowcount=0)
        result = await repo.delete_all()
        assert result == 0


# ===========================================================================
# TestCountByExecutionId
# ===========================================================================

class TestCountByExecutionId:
    @pytest.mark.asyncio
    async def test_count_returns_scalar(self, repo, mock_session):
        mock_session.execute.return_value = MockResult(scalar_value=42)
        result = await repo.count_by_execution_id("exec-123")
        assert result == 42

    @pytest.mark.asyncio
    async def test_count_zero(self, repo, mock_session):
        mock_session.execute.return_value = MockResult(scalar_value=0)
        result = await repo.count_by_execution_id("nonexistent")
        assert result == 0

    @pytest.mark.asyncio
    async def test_count_without_group_ids(self, repo, mock_session):
        mock_session.execute.return_value = MockResult(scalar_value=10)
        await repo.count_by_execution_id("exec-1", group_ids=None)

        stmt = mock_session.execute.call_args[0][0]
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "group_id IN" not in compiled

    @pytest.mark.asyncio
    async def test_count_with_group_ids(self, repo, mock_session):
        mock_session.execute.return_value = MockResult(scalar_value=5)
        await repo.count_by_execution_id("exec-1", group_ids=["grp-x", "grp-y"])

        stmt = mock_session.execute.call_args[0][0]
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "group_id IN" in compiled

    @pytest.mark.asyncio
    async def test_count_uses_orm_not_raw_sql(self, repo, mock_session):
        """Verify count uses ORM func.count() - not raw SQL text()."""
        mock_session.execute.return_value = MockResult(scalar_value=3)
        await repo.count_by_execution_id("exec-1")

        stmt = mock_session.execute.call_args[0][0]
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "count" in compiled.lower()
        assert "execution_logs" in compiled


# ===========================================================================
# TestSessionInjection
# ===========================================================================

class TestSessionInjection:
    def test_init_stores_session(self, mock_session):
        repo = ExecutionLogsRepository(session=mock_session)
        assert repo.session is mock_session

    @pytest.mark.asyncio
    async def test_operations_use_injected_session(self, repo, mock_session):
        """All methods should use self.session, not create their own."""
        mock_session.execute.return_value = MockResult(results=[], scalar_value=0, rowcount=0)

        await repo.get_logs_by_execution_id("exec-1")
        await repo.get_by_id(1)
        await repo.count_by_execution_id("exec-1")
        await repo.delete_by_execution_id("exec-1")
        await repo.delete_all()

        assert mock_session.execute.await_count == 5


# ===========================================================================
# TestSqlInjectionPrevention
# ===========================================================================

class TestSqlInjectionPrevention:
    """Tests verifying that ORM usage prevents SQL injection."""

    @pytest.mark.asyncio
    async def test_malicious_execution_id_in_get(self, repo, mock_session):
        mock_session.execute.return_value = MockResult(results=[])
        malicious_id = "'; DROP TABLE execution_logs; --"
        await repo.get_logs_by_execution_id(malicious_id)
        mock_session.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_malicious_execution_id_in_delete(self, repo, mock_session):
        mock_session.execute.return_value = MockResult(rowcount=0)
        malicious_id = "'; DROP TABLE execution_logs; --"
        await repo.delete_by_execution_id(malicious_id)
        mock_session.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_malicious_execution_id_in_count(self, repo, mock_session):
        mock_session.execute.return_value = MockResult(scalar_value=0)
        malicious_id = "'; DROP TABLE execution_logs; --"
        await repo.count_by_execution_id(malicious_id)
        mock_session.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_malicious_content_in_create(self, repo, mock_session):
        malicious_content = "'; DROP TABLE execution_logs; --"
        await repo.create_log(execution_id="safe-id", content=malicious_content)
        added_obj = mock_session.add.call_args[0][0]
        assert added_obj.content == malicious_content
