"""The Lakebase backend must implement the same kind/validity policy as local.

The local SQLite backend applies these in Python and is covered end to end by
``test_memory_kinds.py`` / ``test_memory_supersession.py``. Lakebase applies them
in SQL against a real Postgres, so these pin the generated statements: the
policy has to be identical on both backends or switching one changes what an
agent remembers.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.memory.engine import KIND_SEMANTIC, MemoryRecord
from src.services.memory.lakebase_storage_backend import (
    _RECORD_COLUMNS,
    LakebaseStorageBackend,
)


def _backend():
    return LakebaseStorageBackend(
        table_name="crew_memory",
        crew_id="group_1_crew_abc",
        group_id="group_1",
        session_id="sess_1",
        embedding_dimension=4,
    )


def _session_ctx(session):
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=session)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


async def _capture(coro_factory):
    session = AsyncMock()
    session.execute = AsyncMock()
    # A child of an AsyncMock is itself async, so result.fetchall() would hand
    # back a coroutine and asearch's row loop would fail on it. Pin a sync
    # result object returning no rows — these tests assert on the SQL, not rows.
    session.execute.return_value = MagicMock(
        fetchall=MagicMock(return_value=[]), fetchone=MagicMock(return_value=None)
    )
    with patch(
        "src.services.memory.lakebase_storage_backend.get_lakebase_session",
        return_value=_session_ctx(session),
    ):
        await coro_factory(session)
    return str(session.execute.call_args[0][0]), session.execute.call_args[0][1]


class TestSearchSql:
    @pytest.mark.asyncio
    async def test_filters_out_superseded_records(self):
        sql, _ = await _capture(
            lambda s: _backend().asearch(query_embedding=[0.1, 0.2, 0.3, 0.4])
        )
        assert "valid_to IS NULL OR valid_to > NOW()" in sql

    @pytest.mark.asyncio
    async def test_recency_decay_applies_to_episodic_only(self):
        sql, _ = await _capture(
            lambda s: _backend().asearch(query_embedding=[0.1, 0.2, 0.3, 0.4])
        )
        # The decay term is inside a CASE keyed on kind, with a flat 1.0 for
        # everything durable — age must not push a current fact out of recall.
        assert "WHEN c.kind = 'episodic' THEN EXP(" in sql
        assert "ELSE 1.0" in sql

    @pytest.mark.asyncio
    async def test_selects_the_columns_the_parser_expects(self):
        sql, _ = await _capture(
            lambda s: _backend().asearch(query_embedding=[0.1, 0.2, 0.3, 0.4])
        )
        for column in ("kind", "valid_from", "valid_to", "superseded_by"):
            assert column in sql

    @pytest.mark.asyncio
    async def test_tenant_filter_still_applies(self):
        """The validity clause must be ADDED to tenancy, never replace it."""
        sql, params = await _capture(
            lambda s: _backend().asearch(query_embedding=[0.1, 0.2, 0.3, 0.4])
        )
        assert "group_id = :group_id" in sql
        assert params["group_id"] == "group_1"


class TestSaveSql:
    @pytest.mark.asyncio
    async def test_writes_kind_and_validity_window(self):
        record = MemoryRecord(
            content="the user prefers duckdb",
            kind=KIND_SEMANTIC,
            valid_from=datetime(2026, 7, 1, tzinfo=timezone.utc),
            embedding=[0.1, 0.2, 0.3, 0.4],
        )
        _, params = await _capture(lambda s: _backend().asave([record]))

        assert params["kind"] == KIND_SEMANTIC
        assert params["valid_from"] == datetime(2026, 7, 1, tzinfo=timezone.utc)
        assert params["valid_to"] is None
        assert params["superseded_by"] is None

    @pytest.mark.asyncio
    async def test_naive_validity_timestamps_are_made_utc_aware(self):
        """Same TIMESTAMPTZ trap as created_at: asyncpg reads a naive datetime
        as MACHINE-LOCAL and shifts it by the host's UTC offset."""
        record = MemoryRecord(
            content="x",
            kind=KIND_SEMANTIC,
            valid_from=datetime(2026, 7, 1, 12, 0),  # naive
            embedding=[0.1, 0.2, 0.3, 0.4],
        )
        _, params = await _capture(lambda s: _backend().asave([record]))

        assert params["valid_from"] == datetime(2026, 7, 1, 12, 0, tzinfo=timezone.utc)

    @pytest.mark.asyncio
    async def test_upsert_carries_the_validity_window(self):
        """Retiring a fact goes through this same upsert (``update`` re-saves the
        whole record), so an ON CONFLICT that skipped these would make
        supersession silently never persist."""
        record = MemoryRecord(content="x", embedding=[0.1, 0.2, 0.3, 0.4])
        sql, _ = await _capture(lambda s: _backend().asave([record]))

        conflict = sql.split("ON CONFLICT")[1]
        assert "valid_to = EXCLUDED.valid_to" in conflict
        assert "superseded_by = EXCLUDED.superseded_by" in conflict
        assert "kind = EXCLUDED.kind" in conflict


class TestRowParsing:
    def test_record_columns_match_the_positional_unpack(self):
        """_row_to_record unpacks positionally, so the shared column list and
        the parser must agree or every field shifts by one."""
        columns = [c.strip() for c in _RECORD_COLUMNS.split(",")]
        assert columns == [
            "id",
            "content",
            "metadata",
            "created_at",
            "updated_at",
            "agent",
            "kind",
            "valid_from",
            "valid_to",
            "superseded_by",
        ]

    def test_parses_kind_and_validity(self):
        row = (
            "rec-1",
            "the user prefers duckdb",
            {"scope": "/g1", "importance": 0.8},
            datetime(2026, 7, 1, tzinfo=timezone.utc),
            datetime(2026, 7, 1, tzinfo=timezone.utc),
            "agent-a",
            "semantic",
            datetime(2026, 7, 1, tzinfo=timezone.utc),
            None,
            None,
        )
        record = _backend()._row_to_record(row)

        assert record.kind == KIND_SEMANTIC
        assert record.valid_from is not None
        assert record.is_current

    def test_legacy_row_without_kind_reads_as_episodic(self):
        """A row written before the columns existed comes back with NULLs."""
        row = (
            "rec-1",
            "old memory",
            {},
            datetime(2026, 7, 1, tzinfo=timezone.utc),
            None,
            "",
            None,
            None,
            None,
            None,
        )
        record = _backend()._row_to_record(row)

        assert record.kind == "episodic"
        assert record.is_current

    def test_search_row_with_trailing_score_still_parses(self):
        """asearch appends the blended score after the shared column list."""
        row = (
            "rec-1",
            "content",
            {},
            datetime(2026, 7, 1, tzinfo=timezone.utc),
            None,
            "",
            "semantic",
            None,
            None,
            None,
            0.87,  # score
        )
        record = _backend()._row_to_record(row)

        assert record.kind == KIND_SEMANTIC
        assert record.content == "content"
