"""A tool-call or task-review gate whose worker is gone does not strand its run.

Those gates do not suspend their run: a worker thread blocks on the gate and
restores the run itself. When that worker dies (a server restart, a crashed
process) nothing else would ever clear the run's WAITING_FOR_APPROVAL, and an
external caller would see a question nobody can answer. Against real SQLite,
because what matters is what the run and approval rows hold afterwards.
"""

import os
import subprocess
import sys
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional
from unittest.mock import AsyncMock, patch

import psutil
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

import src.models  # noqa: F401  registers every ORM model on Base.metadata
from src.db.base import Base
from src.models.execution_history import ExecutionHistory
from src.models.hitl_approval import HITLApproval, HITLApprovalStatus
from src.services.hitl import boot
from src.services.hitl.boot import BOOT_ID_KEY
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


def _dead_pid() -> int:
    """The pid of a process that has just exited."""
    child = subprocess.Popen([sys.executable, "-c", "pass"])
    child.wait()
    return child.pid


def _owned_by(owner: str, gate_config: dict = TOOL_GATE) -> dict:
    return {**gate_config, BOOT_ID_KEY: owner}


class TestRestartWithBootIds:
    """Gates that name the server owning their worker are reaped only when that
    server is provably gone, whenever they were raised: several deployments can
    share one database, and their gates are not this server's to close."""

    @pytest.mark.asyncio
    async def test_a_gate_whose_server_died_is_reaped(self, maker):
        run_id = await _run(maker, "WAITING_FOR_APPROVAL")
        gate = await _gate(maker, run_id, _owned_by(boot.boot_id(_dead_pid(), 1.0)))

        assert await _reap(maker, started_before=NOW - timedelta(hours=1)) == [gate]

        assert await _state(maker, run_id, gate) == (
            "CANCELLED",
            HITLApprovalStatus.TIMEOUT,
        )

    @pytest.mark.asyncio
    async def test_a_reused_pid_does_not_keep_a_dead_servers_gate(self, maker):
        process = psutil.Process()
        reused = boot.boot_id(process.pid, process.create_time() - 3600)
        run_id = await _run(maker, "WAITING_FOR_APPROVAL")
        gate = await _gate(maker, run_id, _owned_by(reused))

        assert await _reap(maker, started_before=NOW) == [gate]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "owner",
        [
            pytest.param(boot.current_boot_id(), id="this-server"),
            pytest.param("0123456789abcdef:1:1.000", id="another-host"),
        ],
    )
    async def test_a_gate_whose_server_may_be_alive_is_kept(self, maker, owner):
        run_id = await _run(maker, "WAITING_FOR_APPROVAL")
        gate = await _gate(
            maker, run_id, _owned_by(owner), created_at=NOW - timedelta(hours=1)
        )

        assert await _reap(maker, started_before=NOW) == []

        assert await _state(maker, run_id, gate) == (
            "WAITING_FOR_APPROVAL",
            HITLApprovalStatus.PENDING,
        )


class TestBootIdStamping:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "gate_config,stamped",
        [(TOOL_GATE, True), (REVIEW_GATE, True), (FLOW_GATE, False)],
    )
    async def test_in_run_gates_name_their_server(self, maker, gate_config, stamped):
        run_id = await _run(maker, "RUNNING")
        async with maker() as s:
            approval = await HITLService(s).create_approval_request(
                execution_id=run_id,
                flow_id="flow-1",
                gate_node_id="gate-1",
                crew_sequence=1,
                gate_config=gate_config,
                group_id=GROUP,
            )
            await s.commit()

        owner = approval.gate_config.get(BOOT_ID_KEY)
        assert owner == (boot.current_boot_id() if stamped else None)

    def test_a_run_subprocess_names_the_server_that_started_it(self, monkeypatch):
        from src.config.settings import settings
        from src.core import process_role

        monkeypatch.setattr(settings, "KASAL_BOOT_ID", "parent-host:42:1.000")
        process_role.mark_run_subprocess("crew")
        try:
            assert boot.current_boot_id() == "parent-host:42:1.000"
        finally:
            process_role.reset()
        assert boot.current_boot_id() != "parent-host:42:1.000"

    def test_the_server_hands_its_boot_id_to_its_subprocesses(self, monkeypatch):
        monkeypatch.delenv("KASAL_BOOT_ID", raising=False)
        boot.export_boot_id()
        try:
            assert os.environ["KASAL_BOOT_ID"] == boot.current_boot_id()
        finally:
            monkeypatch.delenv("KASAL_BOOT_ID", raising=False)
