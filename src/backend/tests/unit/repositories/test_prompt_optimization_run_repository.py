"""
Unit tests for PromptOptimizationRunRepository.

Runs against a REAL in-memory SQLite session rather than a mocked one: the
load-bearing behavior here is the SQL itself — group scoping (including the
`IS NULL` branch for the no-auth path), newest-first ordering, and the
heartbeat-staleness predicate that tells a live run from one orphaned by a
backend restart.
"""

from datetime import datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.models.prompt_optimization_run import PromptOptimizationRun
from src.repositories.prompt_optimization_run_repository import (
    PromptOptimizationRunRepository,
)


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(PromptOptimizationRun.__table__.create)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as db_session:
        yield db_session
    await engine.dispose()


@pytest_asyncio.fixture
async def repo(session):
    return PromptOptimizationRunRepository(session)


async def _add(repo, run_id, **overrides):
    data = {
        "id": run_id,
        "kind": "template",
        "target_name": "detect_intent",
        "status": "pending",
        "dataset_size": 5,
        "applied": False,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }
    data.update(overrides)
    return await repo.create(data)


class TestCreateAndGet:
    @pytest.mark.asyncio
    async def test_create_flushes_without_committing(self, repo, session):
        row = await _add(repo, "r1", group_id="grp1")
        assert row.id == "r1"
        # Flushed (visible to this session) but NOT committed — the session
        # lifecycle owns the transaction.
        assert await repo.get("r1") is not None
        assert session.in_transaction()

    @pytest.mark.asyncio
    async def test_get_missing_is_none(self, repo):
        assert await repo.get("nope") is None

    @pytest.mark.asyncio
    async def test_json_columns_round_trip(self, repo):
        await _add(
            repo,
            "r1",
            group_id="grp1",
            optimized_fields={"agent.a1.role": "New role"},
            before_image={"agent.a1.role": "Old role"},
        )
        row = await repo.get("r1")
        assert row.optimized_fields == {"agent.a1.role": "New role"}
        assert row.before_image == {"agent.a1.role": "Old role"}


class TestGroupScoping:
    @pytest.mark.asyncio
    async def test_get_by_group_matches_the_owning_group(self, repo):
        await _add(repo, "r1", group_id="grp1")
        assert (await repo.get_by_group("r1", "grp1")).id == "r1"

    @pytest.mark.asyncio
    async def test_get_by_group_hides_other_groups(self, repo):
        await _add(repo, "r1", group_id="grp1")
        assert await repo.get_by_group("r1", "other") is None

    @pytest.mark.asyncio
    async def test_group_none_matches_only_ungrouped_rows(self, repo):
        """The single-user / no-auth path records runs without a group; `= NULL`
        never matches in SQL, so the repository must use IS NULL."""
        await _add(repo, "grouped", group_id="grp1")
        await _add(repo, "ungrouped", group_id=None)
        assert await repo.get_by_group("ungrouped", None) is not None
        assert await repo.get_by_group("grouped", None) is None

    @pytest.mark.asyncio
    async def test_list_is_group_scoped(self, repo):
        await _add(repo, "mine", group_id="grp1")
        await _add(repo, "theirs", group_id="grp2")
        rows = await repo.list_by_group("grp1")
        assert [r.id for r in rows] == ["mine"]

    @pytest.mark.asyncio
    async def test_list_none_group_returns_ungrouped_only(self, repo):
        await _add(repo, "grouped", group_id="grp1")
        await _add(repo, "ungrouped", group_id=None)
        rows = await repo.list_by_group(None)
        assert [r.id for r in rows] == ["ungrouped"]


class TestListing:
    @pytest.mark.asyncio
    async def test_newest_first(self, repo):
        now = datetime.utcnow()
        await _add(repo, "old", group_id="grp1", created_at=now - timedelta(hours=2))
        await _add(repo, "new", group_id="grp1", created_at=now)
        await _add(repo, "mid", group_id="grp1", created_at=now - timedelta(hours=1))
        assert [r.id for r in await repo.list_by_group("grp1")] == [
            "new",
            "mid",
            "old",
        ]

    @pytest.mark.asyncio
    async def test_limit_is_honored(self, repo):
        now = datetime.utcnow()
        for index in range(5):
            await _add(
                repo,
                f"r{index}",
                group_id="grp1",
                created_at=now - timedelta(minutes=index),
            )
        rows = await repo.list_by_group("grp1", limit=2)
        assert [r.id for r in rows] == ["r0", "r1"]

    @pytest.mark.asyncio
    async def test_empty_group_returns_empty(self, repo):
        assert await repo.list_by_group("grp1") == []


class TestUpdateFields:
    @pytest.mark.asyncio
    async def test_patches_and_bumps_updated_at(self, repo):
        stale = datetime.utcnow() - timedelta(hours=1)
        await _add(repo, "r1", group_id="grp1", updated_at=stale)
        assert await repo.update_fields("r1", {"status": "completed"}) is True
        row = await repo.get("r1")
        assert row.status == "completed"
        # The heartbeat depends on this: staleness is measured from updated_at.
        assert row.updated_at > stale

    @pytest.mark.asyncio
    async def test_missing_row_reports_false(self, repo):
        """A run whose record was pruned must not resurrect itself on a late
        status write from its background task."""
        assert await repo.update_fields("gone", {"status": "failed"}) is False

    @pytest.mark.asyncio
    async def test_empty_changes_is_a_noop(self, repo):
        await _add(repo, "r1", group_id="grp1")
        assert await repo.update_fields("r1", {}) is False

    @pytest.mark.asyncio
    async def test_clearing_the_before_image(self, repo):
        """Revert consumes the before-image; None must actually persist."""
        await _add(repo, "r1", group_id="grp1", before_image={"template": "OLD"})
        await repo.update_fields("r1", {"before_image": None, "applied": False})
        row = await repo.get("r1")
        assert row.before_image is None
        assert row.applied is False


class TestStaleActiveRuns:
    @pytest.mark.asyncio
    async def test_finds_only_stale_active_rows(self, repo):
        now = datetime.utcnow()
        await _add(repo, "fresh", group_id="grp1", status="running", updated_at=now)
        await _add(
            repo,
            "stale",
            group_id="grp1",
            status="running",
            updated_at=now - timedelta(seconds=600),
        )
        await _add(
            repo,
            "done",
            group_id="grp1",
            status="completed",
            updated_at=now - timedelta(seconds=600),
        )
        stale = await repo.find_stale_active("grp1", 300)
        assert [r.id for r in stale] == ["stale"]

    @pytest.mark.asyncio
    async def test_pending_counts_as_active(self, repo):
        await _add(
            repo,
            "never_started",
            group_id="grp1",
            status="pending",
            updated_at=datetime.utcnow() - timedelta(seconds=600),
        )
        assert len(await repo.find_stale_active("grp1", 300)) == 1

    @pytest.mark.asyncio
    async def test_is_group_scoped(self, repo):
        await _add(
            repo,
            "theirs",
            group_id="grp2",
            status="running",
            updated_at=datetime.utcnow() - timedelta(seconds=600),
        )
        assert await repo.find_stale_active("grp1", 300) == []
