"""Scheduled memory maintenance (M9.b): coverage by store size, not run frequency.

Maintenance used to run only at run teardown, so a scope was tidied exactly as
often as somebody happened to run something in it — a big stale store on a
rarely-run crew got the least, a busy chat workspace re-scanned the same recent
records constantly.

The watermark inverts that: pick whatever has gone longest without maintenance.
It is a table rather than a dict because the chat throttle it complements is
process-local — lost on restart, and per-replica on a multi-replica deployment.
"""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.repositories.memory_maintenance_repository import MemoryMaintenanceRepository
from src.services.memory.maintenance.sweep import (
    _maintain_group,
    build_group_memory,
    sweep_enabled,
    sweep_memory_maintenance,
)


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def scalar_one_or_none(self):
        return self._rows[0] if self._rows else None


def _repo(*results):
    """Repository over a session returning ``results`` in order."""
    session = AsyncMock()
    session.execute = AsyncMock(side_effect=[_FakeResult(r) for r in results])
    session.add = MagicMock()
    repository = MemoryMaintenanceRepository(session)
    return repository, session


class TestDueGroups:
    @pytest.mark.asyncio
    async def test_a_group_never_maintained_comes_first(self):
        """A newly configured workspace must not wait a full interval for its
        first pass — and must not sit behind scopes that have had one."""
        old = datetime.utcnow() - timedelta(hours=48)
        repository, _ = _repo([("g-old",), ("g-new",)], [("g-old", old)])

        due = await repository.due_groups(interval_hours=6, limit=10)

        assert due == ["g-new", "g-old"]

    @pytest.mark.asyncio
    async def test_recently_maintained_groups_are_skipped(self):
        fresh = datetime.utcnow() - timedelta(minutes=5)
        repository, _ = _repo([("g1",)], [("g1", fresh)])

        assert await repository.due_groups(interval_hours=6, limit=10) == []

    @pytest.mark.asyncio
    async def test_oldest_first_among_overdue(self):
        older = datetime.utcnow() - timedelta(hours=100)
        newer = datetime.utcnow() - timedelta(hours=10)
        repository, _ = _repo(
            [("g-newer",), ("g-older",)], [("g-newer", newer), ("g-older", older)]
        )

        assert await repository.due_groups(interval_hours=6, limit=10) == [
            "g-older",
            "g-newer",
        ]

    @pytest.mark.asyncio
    async def test_batch_limit_is_respected(self):
        """One tick must be bounded — each scope can cost two LLM calls."""
        repository, _ = _repo([("a",), ("b",), ("c",), ("d",)], [])

        assert len(await repository.due_groups(interval_hours=6, limit=2)) == 2

    @pytest.mark.asyncio
    async def test_no_configured_backends_means_no_work(self):
        repository, session = _repo([])

        assert await repository.due_groups(interval_hours=6, limit=10) == []
        # Short-circuits before the watermark query.
        assert session.execute.await_count == 1


class TestRecordResult:
    @pytest.mark.asyncio
    async def test_creates_a_row_when_absent(self):
        repository, session = _repo([])

        await repository.record_result("g1", status="ok", stats={"deleted": 2})

        session.add.assert_called_once()
        added = session.add.call_args[0][0]
        assert added.group_id == "g1"
        assert added.last_status == "ok"
        assert added.last_stats == {"deleted": 2}
        assert added.last_maintained_at is not None

    @pytest.mark.asyncio
    async def test_failure_still_advances_the_watermark(self):
        """Otherwise a scope with an unreachable backend stays at the front of
        the queue forever and starves everything behind it."""
        existing = MagicMock(last_maintained_at=None)
        repository, session = _repo([existing])

        await repository.record_result("g1", status="error", error="boom")

        assert existing.last_maintained_at is not None
        assert existing.last_status == "error"
        assert existing.last_error == "boom"
        session.add.assert_not_called()

    @pytest.mark.asyncio
    async def test_long_errors_are_truncated(self):
        existing = MagicMock()
        repository, _ = _repo([existing])

        await repository.record_result("g1", status="error", error="x" * 5000)

        assert len(existing.last_error) == 500


class TestSweepTick:
    def _patch_session(self, repository):
        """Patch get_isolated_db_session and the repository the sweep builds."""
        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(return_value=AsyncMock())
        ctx.__aexit__ = AsyncMock(return_value=False)
        return (
            patch("src.db.session.get_isolated_db_session", return_value=ctx),
            patch(
                "src.repositories.memory_maintenance_repository."
                "MemoryMaintenanceRepository",
                return_value=repository,
            ),
        )

    @pytest.mark.asyncio
    async def test_disabled_sweep_does_nothing(self, monkeypatch):
        monkeypatch.setenv("KASAL_MEMORY_SWEEP", "false")
        assert sweep_enabled() is False
        assert await sweep_memory_maintenance() == {
            "scopes": 0,
            "maintained": 0,
            "skipped": 0,
        }

    @pytest.mark.asyncio
    async def test_enabled_by_default(self, monkeypatch):
        monkeypatch.delenv("KASAL_MEMORY_SWEEP", raising=False)
        assert sweep_enabled() is True

    @pytest.mark.asyncio
    async def test_each_due_scope_is_maintained_and_stamped(self, monkeypatch):
        repository = AsyncMock()
        repository.due_groups = AsyncMock(return_value=["g1", "g2"])
        repository.record_result = AsyncMock()
        session_patch, repo_patch = self._patch_session(repository)

        with (
            session_patch,
            repo_patch,
            patch(
                "src.services.memory.maintenance.sweep._maintain_group",
                new=AsyncMock(return_value=("ok", {"deleted": 1}, None)),
            ),
        ):
            result = await sweep_memory_maintenance()

        assert result == {"scopes": 2, "maintained": 2, "skipped": 0}
        assert repository.record_result.await_count == 2

    @pytest.mark.asyncio
    async def test_one_broken_scope_does_not_stop_the_others(self):
        repository = AsyncMock()
        repository.due_groups = AsyncMock(return_value=["broken", "fine"])
        repository.record_result = AsyncMock()
        session_patch, repo_patch = self._patch_session(repository)
        outcomes = {
            "broken": ("error", {}, "backend down"),
            "fine": ("ok", {"deleted": 3}, None),
        }

        async def _fake(group_id):
            return outcomes[group_id]

        with (
            session_patch,
            repo_patch,
            patch("src.services.memory.maintenance.sweep._maintain_group", new=_fake),
        ):
            result = await sweep_memory_maintenance()

        assert result == {"scopes": 2, "maintained": 1, "skipped": 1}
        statuses = {
            call.args[0] if call.args else call.kwargs["group_id"]: call.kwargs[
                "status"
            ]
            for call in repository.record_result.await_args_list
        }
        assert statuses == {"broken": "error", "fine": "ok"}

    @pytest.mark.asyncio
    async def test_unreadable_watermarks_end_the_tick_quietly(self):
        repository = AsyncMock()
        repository.due_groups = AsyncMock(side_effect=RuntimeError("db down"))
        session_patch, repo_patch = self._patch_session(repository)

        with session_patch, repo_patch:
            result = await sweep_memory_maintenance()

        assert result == {"scopes": 0, "maintained": 0, "skipped": 0}


class TestMaintainGroup:
    @pytest.mark.asyncio
    async def test_a_scope_with_no_usable_memory_is_unavailable(self):
        """Skipped whole rather than half-maintained: the merge and supersession
        passes re-save records and therefore need an embedder."""
        with patch(
            "src.services.memory.maintenance.sweep.build_group_memory",
            new=AsyncMock(return_value=None),
        ):
            status, stats, error = await _maintain_group("g1")

        assert status == "unavailable"
        assert stats == {}
        assert error

    @pytest.mark.asyncio
    async def test_maintenance_failure_is_reported_not_raised(self):
        with (
            patch(
                "src.services.memory.maintenance.sweep.build_group_memory",
                new=AsyncMock(return_value=MagicMock()),
            ),
            patch(
                "src.services.memory.maintenance.passes.run_memory_maintenance",
                side_effect=RuntimeError("pass exploded"),
            ),
        ):
            status, _, error = await _maintain_group("g1")

        assert status == "error"
        assert "pass exploded" in error

    @pytest.mark.asyncio
    async def test_stats_are_returned_for_the_watermark(self):
        with (
            patch(
                "src.services.memory.maintenance.sweep.build_group_memory",
                new=AsyncMock(return_value=MagicMock()),
            ),
            patch(
                "src.services.memory.maintenance.passes.run_memory_maintenance",
                return_value={"deleted": 4, "superseded": 1},
            ),
        ):
            status, stats, error = await _maintain_group("g1")

        assert status == "ok"
        assert stats == {"deleted": 4, "superseded": 1}
        assert error is None


class TestBuildGroupMemory:
    @pytest.mark.asyncio
    async def test_no_configured_backend_yields_nothing(self):
        service = AsyncMock()
        service.fetch_memory_backend_config = AsyncMock(return_value=None)

        with patch(
            "src.services.memory.run.crew_memory.CrewMemoryService",
            return_value=service,
        ):
            assert await build_group_memory("g1") is None

    @pytest.mark.asyncio
    async def test_construction_failure_is_swallowed(self):
        with patch(
            "src.services.memory.run.crew_memory.CrewMemoryService",
            side_effect=RuntimeError("no credentials"),
        ):
            assert await build_group_memory("g1") is None
