"""Legacy billing retention uses async SQL and follows the parent run's age."""

from datetime import datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy import delete, insert, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import src.db.all_models  # noqa: F401 -- register ORM relationships
from src.models.billing import LLMUsageBilling
from src.models.execution_history import ExecutionHistory
from src.repositories.billing_repository import BillingRepository

CUTOFF = datetime(2026, 9, 1)


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        async with engine.begin() as connection:
            await connection.execute(text("PRAGMA foreign_keys=ON"))
            for model in (ExecutionHistory, LLMUsageBilling):
                await connection.run_sync(model.__table__.create)
        async with async_sessionmaker(engine)() as db:
            yield db
    finally:
        await engine.dispose()


async def seed_run(session, job_id, created_at, usage_date):
    await session.execute(
        insert(ExecutionHistory).values(job_id=job_id, created_at=created_at)
    )
    await session.execute(
        insert(LLMUsageBilling).values(
            execution_id=job_id,
            execution_type="crew",
            model_name="example-model",
            model_provider="example-provider",
            usage_date=usage_date,
        )
    )
    await session.commit()


@pytest.mark.asyncio
async def test_retention_uses_parent_age_and_preserves_cutoff_boundary(session):
    old = CUTOFF - timedelta(days=1)
    recent = CUTOFF + timedelta(days=1)
    await seed_run(session, "old-run", old, recent)
    await seed_run(session, "boundary-run", CUTOFF, old)
    await seed_run(session, "recent-run", recent, old)

    assert await BillingRepository(session).delete_older_than(CUTOFF) == 1
    retained = await session.scalars(select(LLMUsageBilling.execution_id))
    assert set(retained) == {"boundary-run", "recent-run"}
    assert await BillingRepository(session).delete_older_than(CUTOFF) == 0


@pytest.mark.asyncio
async def test_housekeeping_can_delete_parent_after_billing_cleanup(session):
    await seed_run(session, "old-run", CUTOFF - timedelta(days=1), CUTOFF)
    parent_delete = delete(ExecutionHistory).where(ExecutionHistory.job_id == "old-run")
    with pytest.raises(IntegrityError):
        await session.execute(parent_delete)
    await session.rollback()

    assert await BillingRepository(session).delete_older_than(CUTOFF) == 1
    result = await session.execute(parent_delete)
    assert result.rowcount == 1
    await session.commit()


@pytest.mark.asyncio
async def test_retention_leaves_transaction_control_with_caller(session):
    await seed_run(session, "old-run", CUTOFF - timedelta(days=1), CUTOFF)
    assert await BillingRepository(session).delete_older_than(CUTOFF) == 1
    await session.rollback()
    assert await session.scalar(select(LLMUsageBilling.execution_id)) == "old-run"
