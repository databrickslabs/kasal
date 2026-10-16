"""A tool-call or task-review gate whose worker is gone does not strand its run.

Those gates do not suspend their run: a worker thread blocks on the gate and
restores the run itself. When that worker dies (a server restart, a crashed
process) nothing else would ever clear the run's WAITING_FOR_APPROVAL, and an
external caller would see a question nobody can answer. Against real SQLite,
because what matters is what the run and approval rows hold afterwards.
"""

import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

import src.models  # noqa: F401  registers every ORM model on Base.metadata
from src.db.base import Base
from src.models.execution_history import ExecutionHistory
from src.models.hitl_approval import HITLApproval, HITLApprovalStatus
from src.services.hitl.service import HITLService
from src.services.hitl.timeout import HITLTimeoutService

GROUP = "team_reports"
TOOL_GATE = {"kind": "tool_call", "tool_name": "send_mail", "timeout_action": "reject"}
REVIEW_GATE = {"kind": "task_review", "timeout_action": "reject"}
FLOW_GATE = {"message": "Approve?", "timeout_action": "auto_reject"}
NOW = datetime.now(timezone.utc)


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


async def _run(maker, status: str) -> str:
    run_id = f"run-{uuid.uuid4()}"
    async with maker() as s:
        s.add(ExecutionHistory(job_id=run_id, status=status, group_id=GROUP))
        await s.commit()
    return run_id


async def _gate(
    maker,
    run_id: str,
    gate_config: dict,
    expires_in: timedelta = timedelta(hours=1),
    created_at: Optional[datetime] = None,
) -> int:
    async with maker() as s:
        approval = HITLApproval(
            execution_id=run_id,
            flow_id="flow-1",
            gate_node_id="gate-1",
            crew_sequence=1,
            gate_config=gate_config,
            expires_at=datetime.now(timezone.utc) + expires_in,
            created_at=created_at or datetime.now(timezone.utc),
            group_id=GROUP,
        )
        s.add(approval)
        await s.commit()
        return approval.id


async def _state(maker, run_id: str, approval_id: int) -> tuple:
    async with maker() as s:
        run = (
            await s.execute(
                ExecutionHistory.__table__.select().where(
                    ExecutionHistory.job_id == run_id
                )
            )
        ).first()
        return run.status, (await s.get(HITLApproval, approval_id)).status


async def _sweep(maker) -> list:
    async with maker() as s:
        processed = await HITLService(s).process_expired_approvals()
        await s.commit()
    return processed


async def _reap(maker, started_before: datetime) -> list:
    async with maker() as s:
        closed = await HITLService(s).reap_abandoned_gates(started_before)
        await s.commit()
    return closed


class TestExpiryPlusGrace:
    @pytest.mark.asyncio
    async def test_within_the_grace_the_gate_is_left_to_its_worker(self, maker):
        """Its worker decides at its own deadline, a moment after expiry; a row
        closed first would read to it as a rejection."""
        run_id = await _run(maker, "WAITING_FOR_APPROVAL")
        gate = await _gate(maker, run_id, TOOL_GATE, expires_in=timedelta(seconds=-30))

        assert await _sweep(maker) == []

        assert await _state(maker, run_id, gate) == (
            "WAITING_FOR_APPROVAL",
            HITLApprovalStatus.PENDING,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("kind", [TOOL_GATE, REVIEW_GATE])
    async def test_past_the_grace_a_run_still_waiting_is_failed(self, maker, kind):
        """A live worker would have restored RUNNING by now."""
        run_id = await _run(maker, "WAITING_FOR_APPROVAL")
        gate = await _gate(maker, run_id, kind, expires_in=timedelta(minutes=-5))

        assert await _sweep(maker) == [gate]

        assert await _state(maker, run_id, gate) == (
            "FAILED",
            HITLApprovalStatus.TIMEOUT,
        )

    @pytest.mark.asyncio
    async def test_a_run_still_waiting_on_another_gate_is_left_waiting(self, maker):
        run_id = await _run(maker, "WAITING_FOR_APPROVAL")
        expired = await _gate(
            maker, run_id, TOOL_GATE, expires_in=timedelta(minutes=-5)
        )
        await _gate(maker, run_id, TOOL_GATE)

        await _sweep(maker)

        assert await _state(maker, run_id, expired) == (
            "WAITING_FOR_APPROVAL",
            HITLApprovalStatus.TIMEOUT,
        )


class TestRestart:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("kind", [TOOL_GATE, REVIEW_GATE])
    async def test_a_run_waiting_on_an_in_run_gate_is_cancelled(self, maker, kind):
        run_id = await _run(maker, "WAITING_FOR_APPROVAL")
        gate = await _gate(maker, run_id, kind, created_at=NOW - timedelta(minutes=1))

        assert await _reap(maker, started_before=NOW) == [gate]

        assert await _state(maker, run_id, gate) == (
            "CANCELLED",
            HITLApprovalStatus.TIMEOUT,
        )

    @pytest.mark.asyncio
    async def test_a_flow_gate_survives_a_restart(self, maker):
        """A decision relaunches the flow, so its run is still answerable."""
        run_id = await _run(maker, "WAITING_FOR_APPROVAL")
        gate = await _gate(
            maker, run_id, FLOW_GATE, created_at=NOW - timedelta(minutes=1)
        )

        assert await _reap(maker, started_before=NOW) == []

        assert await _state(maker, run_id, gate) == (
            "WAITING_FOR_APPROVAL",
            HITLApprovalStatus.PENDING,
        )

    @pytest.mark.asyncio
    async def test_a_finished_run_is_not_relabelled(self, maker):
        run_id = await _run(maker, "COMPLETED")
        gate = await _gate(
            maker, run_id, TOOL_GATE, created_at=NOW - timedelta(minutes=1)
        )

        await _reap(maker, started_before=NOW)

        assert await _state(maker, run_id, gate) == (
            "COMPLETED",
            HITLApprovalStatus.TIMEOUT,
        )

    @pytest.mark.asyncio
    async def test_a_gate_raised_since_the_restart_is_left_alone(self, maker):
        run_id = await _run(maker, "WAITING_FOR_APPROVAL")
        gate = await _gate(maker, run_id, TOOL_GATE, created_at=NOW)

        assert await _reap(maker, started_before=NOW - timedelta(minutes=1)) == []

        assert await _state(maker, run_id, gate) == (
            "WAITING_FOR_APPROVAL",
            HITLApprovalStatus.PENDING,
        )

    @pytest.mark.asyncio
    async def test_the_timeout_service_reaps_once_when_it_starts(self):
        service = HITLTimeoutService()

        async def stop_after_first_sweep():
            service._running = False

        with (
            patch.object(service, "_reap_abandoned_gates", new=AsyncMock()) as reap,
            patch.object(
                service,
                "_check_expired_approvals",
                new=AsyncMock(side_effect=stop_after_first_sweep),
            ) as sweep,
            patch("src.services.hitl.timeout.asyncio.sleep", new=AsyncMock()),
        ):
            await service.start()

        reap.assert_awaited_once()
        sweep.assert_awaited_once()
