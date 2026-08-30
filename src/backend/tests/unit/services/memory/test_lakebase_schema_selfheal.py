"""The Lakebase memory table must repair its own schema.

Its DDL lives in ``LakebaseService.initialize_tables``, reachable from exactly
one place — an admin-only HTTP endpoint. Nothing calls it at startup or when
memory is used, and ``CREATE TABLE IF NOT EXISTS`` is a no-op on a table that
already exists. So a column added to that DDL reaches a FRESH workspace and no
other.

The failure mode is what makes this worth a file of its own: memory swallows its
own errors by design, so a missing column does not crash a run. Every insert and
every select fails silently and forever, and it reads as "memory isn't very
good" rather than "memory is broken".
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.memory.engine import MemoryRecord
from src.services.memory.storage.lakebase import LakebaseStorageBackend
from src.services.memory.storage.lakebase_schema import (
    _ADDED_COLUMNS,
    ensure_memory_columns,
    reset_schema_cache,
)

_LEGACY_COLUMNS = [
    ("id",),
    ("crew_id",),
    ("group_id",),
    ("session_id",),
    ("agent",),
    ("content",),
    ("metadata",),
    ("score",),
    ("embedding",),
    ("created_at",),
    ("updated_at",),
]
_CURRENT_COLUMNS = _LEGACY_COLUMNS + [(name,) for name, _ in _ADDED_COLUMNS]


@pytest.fixture(autouse=True)
def _fresh_cache():
    """The cache is process-global, so it has to be cleared around each test or
    whichever test ran first would decide the outcome of all the others."""
    reset_schema_cache()
    yield
    reset_schema_cache()


def _session(columns, fail=False):
    """A session whose information_schema lookup returns ``columns``.

    Every other statement returns no rows — these tests are about the SQL that
    gets issued, not about parsing results.
    """
    session = AsyncMock()
    if fail:
        session.execute = AsyncMock(side_effect=RuntimeError("permission denied"))
        return session

    async def _execute(statement, *args, **kwargs):
        rows = columns if "information_schema" in str(statement) else []
        return MagicMock(
            fetchall=MagicMock(return_value=rows),
            fetchone=MagicMock(return_value=None),
            rowcount=0,
        )

    session.execute = AsyncMock(side_effect=_execute)
    return session


def _statements(session):
    return [str(call.args[0]) for call in session.execute.await_args_list]


class TestMissingColumns:
    @pytest.mark.asyncio
    async def test_every_missing_column_is_added(self):
        session = _session(_LEGACY_COLUMNS)

        await ensure_memory_columns(session, "crew_memory")

        sql = " ".join(_statements(session))
        for column, _ in _ADDED_COLUMNS:
            assert f"ADD COLUMN IF NOT EXISTS {column}" in sql

    @pytest.mark.asyncio
    async def test_kind_backfills_existing_rows_as_episodic(self):
        """Rows written before the column existed have no kind, and episodic is
        the correct reading — it decays and claims nothing."""
        session = _session(_LEGACY_COLUMNS)

        await ensure_memory_columns(session, "crew_memory")

        assert "kind TEXT NOT NULL DEFAULT 'episodic'" in " ".join(_statements(session))

    @pytest.mark.asyncio
    async def test_the_recall_index_is_created_with_the_columns(self):
        """Recall filters on valid_to and scores on kind — the index that
        supports that belongs with the columns, not with the table DDL."""
        session = _session(_LEGACY_COLUMNS)

        await ensure_memory_columns(session, "crew_memory")

        sql = " ".join(_statements(session))
        assert "CREATE INDEX IF NOT EXISTS idx_crew_memory_current" in sql
        assert "WHERE valid_to IS NULL" in sql

    @pytest.mark.asyncio
    async def test_only_the_missing_ones_are_added(self):
        partial = _LEGACY_COLUMNS + [("kind",), ("valid_from",)]
        session = _session(partial)

        await ensure_memory_columns(session, "crew_memory")

        sql = " ".join(_statements(session))
        assert "ADD COLUMN IF NOT EXISTS valid_to" in sql
        assert "ADD COLUMN IF NOT EXISTS superseded_by" in sql
        assert "ADD COLUMN IF NOT EXISTS kind" not in sql


class TestNoWorkNeeded:
    @pytest.mark.asyncio
    async def test_a_current_table_is_only_inspected(self):
        session = _session(_CURRENT_COLUMNS)

        await ensure_memory_columns(session, "crew_memory")

        assert len(_statements(session)) == 1  # the information_schema lookup
        assert "ALTER TABLE" not in _statements(session)[0]

    @pytest.mark.asyncio
    async def test_runs_at_most_once_per_process(self):
        """Steady state has to be free — this sits in front of EVERY memory
        read and write."""
        first = _session(_CURRENT_COLUMNS)
        await ensure_memory_columns(first, "crew_memory")

        second = _session(_CURRENT_COLUMNS)
        await ensure_memory_columns(second, "crew_memory")

        assert second.execute.await_count == 0

    @pytest.mark.asyncio
    async def test_each_table_is_tracked_separately(self):
        await ensure_memory_columns(_session(_CURRENT_COLUMNS), "crew_memory")
        other = _session(_LEGACY_COLUMNS)

        await ensure_memory_columns(other, "other_memory")

        assert other.execute.await_count > 0


class TestFailureHandling:
    @pytest.mark.asyncio
    async def test_a_failure_never_propagates(self):
        """The caller is about to run its own SQL and will report the real
        error, which is more useful than one raised from a migration helper."""
        await ensure_memory_columns(_session([], fail=True), "crew_memory")

    @pytest.mark.asyncio
    async def test_a_failure_is_not_retried_on_every_operation(self):
        """A workspace whose credentials cannot ALTER must not pay for five
        failing statements on every single memory read and write."""
        await ensure_memory_columns(_session([], fail=True), "crew_memory")
        second = _session([], fail=True)

        await ensure_memory_columns(second, "crew_memory")

        assert second.execute.await_count == 0

    @pytest.mark.asyncio
    async def test_an_invisible_table_is_left_alone(self):
        """An empty information_schema result means the table is not visible to
        this connection at all. ALTERing it would fail; initialize_tables owns
        creating it."""
        session = _session([])

        await ensure_memory_columns(session, "crew_memory")

        assert "ALTER TABLE" not in " ".join(_statements(session))


class TestBackendUsesIt:
    """Every database operation must route through the guard, not just some."""

    def _backend(self):
        return LakebaseStorageBackend(
            table_name="crew_memory",
            crew_id="g1_crew_abc",
            group_id="g1",
            embedding_dimension=4,
        )

    def _ctx(self, session):
        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(return_value=session)
        ctx.__aexit__ = AsyncMock(return_value=False)
        return ctx

    @pytest.mark.asyncio
    async def test_a_save_heals_the_table_first(self):
        session = _session(_LEGACY_COLUMNS)
        record = MemoryRecord(content="x", embedding=[0.1, 0.2, 0.3, 0.4])

        with patch(
            "src.services.memory.storage.lakebase.get_lakebase_session",
            return_value=self._ctx(session),
        ):
            await self._backend().asave([record])

        statements = _statements(session)
        assert any("ADD COLUMN IF NOT EXISTS kind" in s for s in statements)
        # ...and the ALTERs precede the INSERT that depends on them.
        assert statements.index(
            next(s for s in statements if "ADD COLUMN IF NOT EXISTS kind" in s)
        ) < statements.index(next(s for s in statements if "INSERT INTO" in s))

    @pytest.mark.asyncio
    async def test_a_search_heals_the_table_first(self):
        session = _session(_LEGACY_COLUMNS)

        with patch(
            "src.services.memory.storage.lakebase.get_lakebase_session",
            return_value=self._ctx(session),
        ):
            await self._backend().asearch(query_embedding=[0.1, 0.2, 0.3, 0.4])

        assert any("ADD COLUMN" in s for s in _statements(session))

    @pytest.mark.asyncio
    async def test_a_delete_heals_the_table_first(self):
        session = _session(_LEGACY_COLUMNS)

        with patch(
            "src.services.memory.storage.lakebase.get_lakebase_session",
            return_value=self._ctx(session),
        ):
            await self._backend().adelete(record_ids=["r1"])

        assert any("ADD COLUMN" in s for s in _statements(session))

    @pytest.mark.asyncio
    async def test_the_repair_runs_in_its_own_session(self):
        """Sessions roll back on exception and Postgres DDL is transactional, so
        sharing the caller's session would let a failed operation silently undo
        the repair — while the cache recorded it as done, meaning it would never
        be retried. Two sessions, so the repair commits on its own."""
        opened = []

        def _open(**kwargs):
            session = _session(_LEGACY_COLUMNS)
            opened.append(session)
            return self._ctx(session)

        with patch(
            "src.services.memory.storage.lakebase.get_lakebase_session",
            side_effect=_open,
        ):
            await self._backend().asave(
                [MemoryRecord(content="x", embedding=[0.1, 0.2, 0.3, 0.4])]
            )

        assert len(opened) == 2, "repair and write must not share a transaction"
        assert any("ADD COLUMN" in s for s in _statements(opened[0]))
        assert any("INSERT INTO" in s for s in _statements(opened[1]))
        assert not any("ADD COLUMN" in s for s in _statements(opened[1]))

    @pytest.mark.asyncio
    async def test_a_checked_table_opens_no_extra_session(self):
        """The steady-state cost has to be a set lookup, not a connection."""
        opened = []

        def _open(**kwargs):
            session = _session(_CURRENT_COLUMNS)
            opened.append(session)
            return self._ctx(session)

        backend = self._backend()
        with patch(
            "src.services.memory.storage.lakebase.get_lakebase_session",
            side_effect=_open,
        ):
            await backend.adelete(record_ids=["r1"])
            count_after_first = len(opened)
            await backend.adelete(record_ids=["r2"])

        assert count_after_first == 2  # repair + operation
        assert len(opened) == 3  # second operation only
