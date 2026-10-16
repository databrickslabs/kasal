"""HITLApprovalRepository.update_status records a decision only while pending.

Against real SQLite: the property is the WHERE clause of the UPDATE, which a
mocked session would accept just as happily without it.
"""

from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

import src.models  # noqa: F401  registers every ORM model on Base.metadata
from src.db.base import Base
from src.models.execution_history import ExecutionHistory
from src.models.hitl_approval import (
    HITLApproval,
    HITLApprovalStatus,
    HITLRejectionAction,
)
from src.repositories.hitl_repository import HITLApprovalRepository


@pytest_asyncio.fixture
async def maker():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    await engine.dispose()


@pytest_asyncio.fixture
async def approval_id(maker):
    async with maker() as s:
        s.add(ExecutionHistory(job_id="run-1", status="WAITING_FOR_APPROVAL"))
        await s.flush()
        approval = HITLApproval(
            execution_id="run-1",
            flow_id="flow-1",
            gate_node_id="gate-1",
            crew_sequence=1,
            gate_config={"message": "Approve?"},
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            group_id="team_reports",
        )
        s.add(approval)
        await s.commit()
        return approval.id


async def _row(maker, approval_id):
    async with maker() as s:
        return await s.get(HITLApproval, approval_id)


class TestRecordADecision:
    @pytest.mark.asyncio
    async def test_a_pending_approval_takes_the_decision(self, maker, approval_id):
        async with maker() as s:
            decided = await HITLApprovalRepository(s).update_status(
                approval_id,
                HITLApprovalStatus.REJECTED,
                responded_by="reviewer@example.com",
                rejection_reason="Quality not sufficient",
                rejection_action=HITLRejectionAction.RETRY,
            )
            await s.commit()

        assert decided is True
        row = await _row(maker, approval_id)
        assert row.status == HITLApprovalStatus.REJECTED
        assert row.responded_by == "reviewer@example.com"
        assert row.rejection_reason == "Quality not sufficient"
        assert row.rejection_action == HITLRejectionAction.RETRY
        assert row.responded_at is not None
        assert row.approval_comment is None

    @pytest.mark.asyncio
    async def test_a_second_decision_changes_nothing(self, maker, approval_id):
        async with maker() as s:
            repo = HITLApprovalRepository(s)
            first = await repo.update_status(
                approval_id, HITLApprovalStatus.APPROVED, responded_by="a@example.com"
            )
            second = await repo.update_status(
                approval_id,
                HITLApprovalStatus.REJECTED,
                responded_by="b@example.com",
                rejection_reason="no",
            )
            await s.commit()

        assert (first, second) == (True, False)
        row = await _row(maker, approval_id)
        assert row.status == HITLApprovalStatus.APPROVED
        assert row.responded_by == "a@example.com"
        assert row.rejection_reason is None

    @pytest.mark.asyncio
    async def test_an_unknown_approval_is_not_decided(self, maker, approval_id):
        async with maker() as s:
            assert (
                await HITLApprovalRepository(s).update_status(
                    approval_id + 1000, HITLApprovalStatus.APPROVED
                )
                is False
            )

    @pytest.mark.asyncio
    async def test_the_loaded_row_shows_the_decision_that_won(self, maker, approval_id):
        """approve() hands the row it loaded to the resume, which reads the
        decision off it."""
        async with maker() as s:
            repo = HITLApprovalRepository(s)
            loaded = await repo.get_by_id(approval_id)
            await repo.update_status(
                approval_id,
                HITLApprovalStatus.APPROVED,
                responded_by="a@example.com",
                approval_comment="Ship it",
            )
            assert loaded.status == HITLApprovalStatus.APPROVED
            assert loaded.responded_by == "a@example.com"
            assert loaded.approval_comment == "Ship it"

    @pytest.mark.asyncio
    async def test_a_losing_decision_leaves_the_loaded_row_alone(
        self, maker, approval_id
    ):
        async with maker() as s:
            repo = HITLApprovalRepository(s)
            await repo.update_status(
                approval_id, HITLApprovalStatus.APPROVED, responded_by="a@example.com"
            )
            loaded = await repo.get_by_id(approval_id)
            await repo.update_status(
                approval_id, HITLApprovalStatus.TIMEOUT, responded_by="system"
            )
            assert loaded.status == HITLApprovalStatus.APPROVED
            assert loaded.responded_by == "a@example.com"
