"""Tests for LakebaseStorageBackend — focused on the TIMESTAMPTZ timezone fix.

Regression coverage for the Cognitive Memory Browser bug where Lakebase records
showed "0 this run" even though the table held rows: CrewAI hands the backend
naive ``datetime.utcnow()`` timestamps, and asyncpg's TIMESTAMPTZ encoder runs
``obj.astimezone(utc)`` — which presumes a naive datetime is in the HOST's local
timezone and shifts it by the machine's UTC offset. Every ``created_at`` then
landed hours off true UTC, so the browser's per-run time window (anchored on the
run's correctly-stored ``completed_at``) rejected all of a run's records.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
from crewai.memory.types import MemoryRecord

from src.engines.crewai.memory.lakebase_storage_backend import (
    LakebaseStorageBackend,
    _to_aware_utc,
)


def _make_lakebase_ctx(mock_session):
    """Async context manager mock for get_lakebase_session."""
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_session)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


@pytest.fixture
def backend():
    return LakebaseStorageBackend(
        table_name="crew_memory",
        crew_id="group_crew_abc123",
        group_id="group_1",
        session_id="job_1",
        embedding_dimension=4,
    )


class TestToAwareUtc:
    """The helper that prevents the naive-datetime shift."""

    def test_naive_gets_utc_tzinfo_without_shifting_walltime(self):
        naive = datetime(2026, 6, 22, 16, 45, 0)  # CrewAI's datetime.utcnow() shape
        aware = _to_aware_utc(naive)
        assert aware.tzinfo is not None
        # Same wall-clock, now explicitly UTC — NOT reinterpreted as local time.
        assert aware == naive.replace(tzinfo=timezone.utc)

    def test_aware_non_utc_is_converted_to_utc(self):
        # 16:45 at +02:00 is 14:45 UTC — convert, preserving the instant.
        aware_cest = datetime(2026, 6, 22, 16, 45, tzinfo=timezone(timedelta(hours=2)))
        result = _to_aware_utc(aware_cest)
        assert result == datetime(2026, 6, 22, 14, 45, tzinfo=timezone.utc)


class TestSaveTimestampTz:
    """asave() must bind offset-aware UTC to the TIMESTAMPTZ columns."""

    @pytest.mark.asyncio
    async def test_created_at_is_bound_offset_aware_utc(self, backend):
        session = AsyncMock()
        session.execute = AsyncMock()
        # Naive UTC instant, exactly what CrewAI produces via datetime.utcnow().
        created = datetime(2026, 6, 22, 16, 45, 0)
        record = MemoryRecord(
            content="hello",
            created_at=created,
            last_accessed=created,
            embedding=[0.1, 0.2, 0.3, 0.4],
        )

        with patch(
            "src.engines.crewai.memory.lakebase_storage_backend.get_lakebase_session",
            return_value=_make_lakebase_ctx(session),
        ):
            await backend.asave([record])

        session.execute.assert_called_once()
        params = session.execute.call_args[0][1]

        # Bound values must carry tzinfo so asyncpg does not localize them.
        assert params["created_at"].tzinfo is not None
        assert params["updated_at"].tzinfo is not None

        # The crux: simulate asyncpg's TIMESTAMPTZ encoder (obj.astimezone(utc)).
        # The persisted instant must equal the original UTC wall-clock — i.e. NO
        # host-offset shift. With the old naive bind this would shift on any host
        # whose local timezone is not UTC.
        persisted = params["created_at"].astimezone(timezone.utc).replace(tzinfo=None)
        assert persisted == created
