"""TriggerQueueRepository — the claim + lifecycle, against real in-memory SQLite.

Runs against a real SQLite session (not mocked collaborators) so the claim SQL,
the status transitions, and the unique idempotency key are actually exercised.
On SQLite the claim omits ``FOR UPDATE SKIP LOCKED`` (single-worker); the
Postgres path is the same query with the lock clause appended.
"""

from datetime import datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from src.models.trigger_queue import (
    STATUS_CLAIMED,
    STATUS_DEAD,
    STATUS_DISPATCHED,
    STATUS_PENDING,
    TriggerQueue,
)
from src.repositories.trigger_queue_repository import TriggerQueueRepository


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(TriggerQueue.__table__.create)
    maker = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with maker() as s:
        yield s
    await engine.dispose()


@pytest_asyncio.fixture
async def repo(session):
    return TriggerQueueRepository(session)


async def _enqueue(repo, session, **fields):
    fields.setdefault("target", {"kind": "flow", "id": "f1"})
    row = await repo.enqueue(**fields)
    await session.commit()
    return row


async def _count(session, **filters) -> int:
    q = select(func.count()).select_from(TriggerQueue)
    result = await session.execute(q)
    return result.scalar()


class TestClaim:
    @pytest.mark.asyncio
    async def test_claim_returns_due_rows_and_marks_them_claimed(self, repo, session):
        await _enqueue(repo, session, group_id="g1")
        await _enqueue(repo, session, group_id="g1")

        rows = await repo.claim(10)
        await session.commit()

        assert len(rows) == 2
        assert all(r.status == STATUS_CLAIMED for r in rows)
        assert all(r.attempts == 1 for r in rows)
        assert all(r.claimed_at is not None for r in rows)

    @pytest.mark.asyncio
    async def test_claim_skips_non_pending_and_future_rows(self, repo, session):
        await _enqueue(repo, session, group_id="due")
        await _enqueue(repo, session, status=STATUS_DISPATCHED, group_id="done")
        await _enqueue(
            repo,
            session,
            group_id="later",
            available_at=datetime.utcnow() + timedelta(hours=1),
        )

        rows = await repo.claim(10)
        await session.commit()

        assert [r.group_id for r in rows] == ["due"]

    @pytest.mark.asyncio
    async def test_claim_respects_limit(self, repo, session):
        for _ in range(5):
            await _enqueue(repo, session, group_id="g1")

        rows = await repo.claim(2)
        await session.commit()

        assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_claim_empty_queue_returns_empty(self, repo, session):
        assert await repo.claim(10) == []


class TestLifecycle:
    @pytest.mark.asyncio
    async def test_mark_dispatched(self, repo, session):
        row = await _enqueue(repo, session)
        await repo.mark_dispatched(row.id)
        await session.commit()
        refreshed = await repo.get(row.id)
        assert refreshed.status == STATUS_DISPATCHED

    @pytest.mark.asyncio
    async def test_mark_failed_dead(self, repo, session):
        row = await _enqueue(repo, session)
        await repo.mark_failed(row.id, "boom", dead=True)
        await session.commit()
        refreshed = await repo.get(row.id)
        assert refreshed.status == STATUS_DEAD
        assert "boom" in (refreshed.last_error or "")

    @pytest.mark.asyncio
    async def test_requeue_makes_it_claimable_again_after_backoff(self, repo, session):
        row = await _enqueue(repo, session)
        await repo.claim(10)  # claim it
        future = datetime.utcnow() + timedelta(seconds=60)
        await repo.requeue(row.id, future, error="transient")
        await session.commit()

        refreshed = await repo.get(row.id)
        assert refreshed.status == STATUS_PENDING
        assert refreshed.available_at == future
        # The claim stamp is cleared — a requeued row must not look "stuck in
        # claimed" to the reclaim sweep.
        assert refreshed.claimed_at is None
        # Not yet due → not claimed.
        assert await repo.claim(10) == []


class TestReclaim:
    @pytest.mark.asyncio
    async def test_reclaims_only_old_claimed_rows(self, repo, session):
        old = await _enqueue(repo, session, group_id="old")
        recent = await _enqueue(repo, session, group_id="recent")
        # Claim both, then age only `old`.
        await repo.claim(10)
        await session.commit()
        old.claimed_at = datetime.utcnow() - timedelta(hours=1)
        await session.commit()

        cutoff = datetime.utcnow() - timedelta(minutes=15)
        n = await repo.reclaim_stuck(cutoff)
        await session.commit()

        assert n == 1
        assert (await repo.get(old.id)).status == STATUS_PENDING
        assert (await repo.get(recent.id)).status == STATUS_CLAIMED


class TestIdempotency:
    @pytest.mark.asyncio
    async def test_duplicate_idempotency_key_rejected(self, repo, session):
        await _enqueue(repo, session, idempotency_key="order.created:1")
        with pytest.raises(IntegrityError):
            await _enqueue(repo, session, idempotency_key="order.created:1")


class TestClaimScoping:
    @pytest.mark.asyncio
    async def test_group_ids_scopes_the_claim(self, repo, session):
        await _enqueue(repo, session, group_id="g1")
        await _enqueue(repo, session, group_id="g2")

        rows = await repo.claim(10, group_ids=["g1"])
        await session.commit()

        assert [r.group_id for r in rows] == ["g1"]
        # The other tenant's row is untouched, still claimable globally.
        rest = await repo.claim(10)
        assert [r.group_id for r in rest] == ["g2"]

    @pytest.mark.asyncio
    async def test_empty_group_list_claims_nothing(self, repo, session):
        # A caller with NO groups must not fall through to the global scan.
        await _enqueue(repo, session, group_id="g1")

        assert await repo.claim(10, group_ids=[]) == []
        assert (await repo.claim(10))[0].group_id == "g1"  # row was left alone

    @pytest.mark.asyncio
    async def test_none_claims_across_all_tenants(self, repo, session):
        await _enqueue(repo, session, group_id="g1")
        await _enqueue(repo, session, group_id="g2")

        rows = await repo.claim(10, group_ids=None)
        assert sorted(r.group_id for r in rows) == ["g1", "g2"]


class TestPurge:
    @pytest.mark.asyncio
    async def test_purges_only_old_finished_rows(self, repo, session):
        old_done = await _enqueue(repo, session, status=STATUS_DISPATCHED)
        old_dead = await _enqueue(repo, session, status=STATUS_DEAD)
        old_pending = await _enqueue(repo, session)  # pending is NEVER purged
        recent_done = await _enqueue(repo, session, status=STATUS_DISPATCHED)
        for row in (old_done, old_dead, old_pending):
            row.created_at = datetime.utcnow() - timedelta(days=10)
        await session.commit()

        n = await repo.purge_finished(datetime.utcnow() - timedelta(days=7))
        await session.commit()

        assert n == 2
        assert await repo.get(old_done.id) is None
        assert await repo.get(old_dead.id) is None
        assert (await repo.get(old_pending.id)) is not None
        assert (await repo.get(recent_done.id)) is not None
