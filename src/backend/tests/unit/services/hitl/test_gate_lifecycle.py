"""An approval is decided once, and a timeout never relabels a run that moved on.

Against a real in-memory SQLite database: both properties live in SQL (a
conditional UPDATE, and the run row the sweeper reads), which a mocked session
would accept whether or not the condition is there.
"""

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

import src.models  # noqa: F401  registers every ORM model on Base.metadata
from src.db.base import Base
from src.models.execution_history import ExecutionHistory
from src.models.hitl_approval import HITLApproval, HITLApprovalStatus
from src.repositories.hitl_repository import HITLApprovalRepository
from src.schemas.hitl import HITLRejectionActionEnum
from src.services.hitl.service import (
    HITLApprovalAlreadyProcessedError,
    HITLService,
)

GROUP = "team_reports"
RELAUNCH = (
    "src.services.execution.kasal_service.KasalExecutionService.run_flow_execution"
)


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


async def _gate(maker, run_status: str, gate_config: dict, expired: bool) -> tuple:
    """A run in ``run_status`` with one PENDING approval on it."""
    run_id = f"run-{uuid.uuid4()}"
    offset = timedelta(minutes=-5) if expired else timedelta(hours=1)
    async with maker() as s:
        s.add(
            ExecutionHistory(
                job_id=run_id,
                status=run_status,
                execution_type="flow",
                group_id=GROUP,
                inputs={"nodes": [], "edges": []},
            )
        )
        await s.flush()
        approval = HITLApproval(
            execution_id=run_id,
            flow_id="flow-1",
            gate_node_id="gate-1",
            crew_sequence=1,
            gate_config=gate_config,
            expires_at=datetime.now(timezone.utc) + offset,
            group_id=GROUP,
        )
        s.add(approval)
        await s.commit()
        return run_id, approval.id


async def _stored(maker, run_id: str, approval_id: int) -> tuple:
    async with maker() as s:
        run = (
            await s.execute(
                ExecutionHistory.__table__.select().where(
                    ExecutionHistory.job_id == run_id
                )
            )
        ).first()
        return run.status, await s.get(HITLApproval, approval_id)


async def _sweep(maker) -> list:
    async with maker() as s:
        processed = await HITLService(s).process_expired_approvals()
        await s.commit()
    return processed


TOOL_GATE = {"kind": "tool_call", "tool_name": "send_mail", "timeout_action": "reject"}
REVIEW_GATE = {"kind": "task_review", "timeout_action": "reject"}
FLOW_GATE = {"message": "Approve?", "timeout_action": "auto_reject"}


class TestTimeoutNeverRelabelsAFinishedRun:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("gate", [TOOL_GATE, REVIEW_GATE, FLOW_GATE])
    async def test_a_completed_run_stays_completed(self, maker, gate):
        run_id, approval_id = await _gate(maker, "COMPLETED", gate, expired=True)

        assert await _sweep(maker) == [approval_id]

        status, approval = await _stored(maker, run_id, approval_id)
        assert status == "COMPLETED"
        # The gate itself is still closed, so it is not found again next sweep.
        assert approval.status == HITLApprovalStatus.TIMEOUT

    @pytest.mark.asyncio
    async def test_an_in_run_gate_does_not_fail_a_running_run(self, maker):
        """A tool gate does not suspend the run: its worker gave up at its own
        deadline and the run carried on."""
        run_id, approval_id = await _gate(maker, "RUNNING", TOOL_GATE, expired=True)

        await _sweep(maker)

        status, approval = await _stored(maker, run_id, approval_id)
        assert status == "RUNNING"
        assert approval.status == HITLApprovalStatus.TIMEOUT

    @pytest.mark.asyncio
    async def test_a_flow_still_paused_at_its_gate_is_rejected(self, maker):
        """The timeout still does its job for the run it was meant for."""
        run_id, approval_id = await _gate(
            maker, "WAITING_FOR_APPROVAL", FLOW_GATE, expired=True
        )

        await _sweep(maker)

        status, approval = await _stored(maker, run_id, approval_id)
        assert status == "REJECTED"
        assert approval.status == HITLApprovalStatus.TIMEOUT


class TestOneDecisionWins:
    @pytest.mark.asyncio
    async def test_two_concurrent_approvals_resume_the_flow_once(self, maker):
        run_id, approval_id = await _gate(
            maker, "WAITING_FOR_APPROVAL", FLOW_GATE, expired=False
        )

        # Both requests read the approval as PENDING before either writes:
        # the window a double click, or the UI and MCP at once, falls into.
        both_read = asyncio.Barrier(2)
        read = HITLApprovalRepository.get_by_id

        async def read_then_wait(self, *args, **kwargs):
            row = await read(self, *args, **kwargs)
            await both_read.wait()
            return row

        async def approve(email):
            async with maker() as s:
                try:
                    return await HITLService(s).approve(
                        approval_id=approval_id, approved_by=email, group_id=GROUP
                    )
                finally:
                    await s.commit()

        launch = AsyncMock(return_value={"status": "running"})
        with (
            patch.object(HITLApprovalRepository, "get_by_id", read_then_wait),
            patch(RELAUNCH, new=launch),
        ):
            outcomes = await asyncio.gather(
                approve("a@example.com"),
                approve("b@example.com"),
                return_exceptions=True,
            )
            for _ in range(20):
                await asyncio.sleep(0.01)

        won = [o for o in outcomes if not isinstance(o, BaseException)]
        lost = [o for o in outcomes if isinstance(o, BaseException)]
        assert len(won) == 1 and won[0].execution_resumed is True
        assert len(lost) == 1
        assert isinstance(lost[0], HITLApprovalAlreadyProcessedError)
        assert "already decided" in str(lost[0])
        assert launch.await_count == 1

    @pytest.mark.asyncio
    async def test_a_reject_racing_an_approve_does_not_also_fail_the_run(self, maker):
        run_id, approval_id = await _gate(
            maker, "WAITING_FOR_APPROVAL", FLOW_GATE, expired=False
        )
        async with maker() as s:
            stale = HITLService(s)
            # Loaded before the other decision lands, as a concurrent request
            # would have it. Held, so the session keeps this stale copy.
            held = await stale.approval_repo.get_by_id(approval_id, GROUP)
            assert held.status == HITLApprovalStatus.PENDING

            launch = AsyncMock(return_value={"status": "running"})
            with patch(RELAUNCH, new=launch):
                async with maker() as other:
                    await HITLService(other).approve(
                        approval_id=approval_id,
                        approved_by="a@example.com",
                        group_id=GROUP,
                    )
                    await other.commit()

                with pytest.raises(
                    HITLApprovalAlreadyProcessedError, match="already decided"
                ):
                    await stale.reject(
                        approval_id=approval_id,
                        rejected_by="b@example.com",
                        group_id=GROUP,
                        reason="no",
                    )

        status, approval = await _stored(maker, run_id, approval_id)
        assert approval.status == HITLApprovalStatus.APPROVED
        assert approval.responded_by == "a@example.com"
        assert status == "RUNNING"


class TestADecisionWhoseRelaunchCannotStart:
    """The gate is decided, so nothing is pending; a run left WAITING would be
    a question nobody can answer."""

    @pytest.mark.asyncio
    async def test_an_approval_marks_the_run_failed(self, maker):
        run_id, approval_id = await _gate(
            maker, "WAITING_FOR_APPROVAL", FLOW_GATE, expired=False
        )
        async with maker() as s:
            service = HITLService(s)
            with patch.object(
                service, "_resume_flow_execution", new=AsyncMock(return_value=False)
            ):
                outcome = await service.approve(
                    approval_id=approval_id, approved_by="a@example.com", group_id=GROUP
                )
            await s.commit()

        assert outcome.execution_resumed is False
        assert "could not be resumed" in outcome.message
        status, _ = await _stored(maker, run_id, approval_id)
        assert status == "FAILED"

    @pytest.mark.asyncio
    async def test_a_retry_marks_the_run_failed(self, maker):
        run_id, approval_id = await _gate(
            maker, "WAITING_FOR_APPROVAL", FLOW_GATE, expired=False
        )
        async with maker() as s:
            service = HITLService(s)
            with patch.object(
                service, "_retry_previous_crew", new=AsyncMock(return_value=False)
            ):
                outcome = await service.reject(
                    approval_id=approval_id,
                    rejected_by="a@example.com",
                    group_id=GROUP,
                    reason="again",
                    action=HITLRejectionActionEnum.RETRY,
                )
            await s.commit()

        assert outcome.execution_resumed is False
        status, _ = await _stored(maker, run_id, approval_id)
        assert status == "FAILED"
