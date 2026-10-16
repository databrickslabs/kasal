"""The approval round-trip over MCP and A2A, end to end through real services.

A flow run paused at an approval gate is answered over each protocol and must
resume. Everything between the protocol handler and the database is real: the
MCP tool and A2A task handlers, the external interaction layer, HITLService and
its repository, and the run status service, on an in-memory SQLite database.
The only stub is the flow relaunch itself, which would otherwise start a flow
subprocess that calls an LLM.
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
from src.core.exceptions import GoneError
from src.db.base import Base
from src.models.execution_history import ExecutionHistory
from src.models.hitl_approval import HITLApproval, HITLApprovalStatus
from src.schemas.a2a import Message, Part
from src.services.a2a.a2a_server import tasks as a2a_tasks
from src.services.external.identity import ExternalCaller
from src.services.hitl.service import HITLService
from src.services.mcp.mcp_server import tools as mcp_tools
from src.utils.user_context import GroupContext

#: The run lives in the publication's teamspace. The caller is a member of two
#: and is not pinned to either, so its context lists the OTHER one first.
RUN_TEAM = "team_reports"
OTHER_TEAM = "team_sales"
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


async def _paused_run(maker, expires_in: timedelta = timedelta(hours=1)) -> tuple:
    """A flow run WAITING_FOR_APPROVAL at a gate, with its pending approval."""
    run_id = f"run-{uuid.uuid4()}"
    async with maker() as s:
        s.add(
            ExecutionHistory(
                job_id=run_id,
                status="WAITING_FOR_APPROVAL",
                execution_type="flow",
                run_name="weekly report via mcp",
                group_id=RUN_TEAM,
                inputs={"nodes": [], "edges": [], "inputs": {"topic": "t"}},
            )
        )
        await s.flush()
        approval = HITLApproval(
            execution_id=run_id,
            flow_id="flow-1",
            gate_node_id="gate-1",
            crew_sequence=1,
            gate_config={"message": "Approve the draft?"},
            previous_crew_output="the draft",
            expires_at=datetime.now(timezone.utc) + expires_in,
            group_id=RUN_TEAM,
        )
        s.add(approval)
        await s.commit()
        return run_id, approval.id


def _caller(protocol: str, group_ids=(OTHER_TEAM, RUN_TEAM)) -> ExternalCaller:
    return ExternalCaller(
        group_context=GroupContext(
            group_ids=list(group_ids),
            group_email="caller@example.com",
            user_role="operator",
            access_token="caller-token",
        ),
        protocol=protocol,
        identifier="caller@example.com",
    )


async def _relaunched(launch: AsyncMock) -> None:
    """Let the background resume task run to the (stubbed) relaunch."""
    for _ in range(100):
        if launch.await_count:
            return
        await asyncio.sleep(0.01)


async def _stored(maker, run_id: str, approval_id: int) -> tuple:
    async with maker() as s:
        run = (
            await s.execute(
                ExecutionHistory.__table__.select().where(
                    ExecutionHistory.job_id == run_id
                )
            )
        ).first()
        approval = await s.get(HITLApproval, approval_id)
        return run.status, approval


class TestMcpRespondToRun:
    @pytest.mark.asyncio
    async def test_status_shows_what_the_run_is_waiting_for(self, maker):
        run_id, approval_id = await _paused_run(maker)
        async with maker() as session:
            payload = await mcp_tools.get_run_status(
                _caller("mcp"), run_id, session=session
            )

        assert payload["state"] == "input_required"
        assert payload["waiting_for"] == [
            {"approval_id": approval_id, "prompt": "Approve the draft?"}
        ]

    @pytest.mark.asyncio
    async def test_an_answer_approves_the_gate_and_resumes_the_run(self, maker):
        run_id, approval_id = await _paused_run(maker)
        launch = AsyncMock(return_value={"status": "running"})
        with patch(RELAUNCH, new=launch):
            async with maker() as session:
                answer = await mcp_tools.respond_to_run(
                    _caller("mcp"), run_id, "Looks good, ship it.", session=session
                )
                await session.commit()
            await _relaunched(launch)

        assert answer["accepted"] is True
        assert answer["execution_resumed"] is True
        assert answer["state"] == "working"
        status, approval = await _stored(maker, run_id, approval_id)
        assert approval.status == HITLApprovalStatus.APPROVED
        assert approval.responded_by == "caller@example.com"
        assert approval.approval_comment == "Looks good, ship it."
        assert status == "RUNNING"
        launch.assert_awaited_once()
        kwargs = launch.await_args.kwargs
        assert kwargs["job_id"] == run_id
        assert kwargs["config"]["resume_from_crew_sequence"] == 2

    @pytest.mark.asyncio
    async def test_a_refusal_rejects_the_gate(self, maker):
        run_id, approval_id = await _paused_run(maker)
        launch = AsyncMock(return_value={"status": "running"})
        with patch(RELAUNCH, new=launch):
            async with maker() as session:
                await mcp_tools.respond_to_run(
                    _caller("mcp"), run_id, "reject", session=session
                )
                await session.commit()

        status, approval = await _stored(maker, run_id, approval_id)
        assert approval.status == HITLApprovalStatus.REJECTED
        assert status == "REJECTED"
        launch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_approval_whose_flow_cannot_resume_fails_the_run(self, maker):
        """Not a run left waiting on nothing, reported as accepted."""
        run_id, approval_id = await _paused_run(maker)
        with patch.object(
            HITLService, "_resume_flow_execution", new=AsyncMock(return_value=False)
        ):
            async with maker() as session:
                answer = await mcp_tools.respond_to_run(
                    _caller("mcp"), run_id, "yes", session=session
                )
                await session.commit()

        assert answer["execution_resumed"] is False
        assert answer["state"] == "failed"
        assert "could not be resumed" in answer["message"]
        status, approval = await _stored(maker, run_id, approval_id)
        assert approval.status == HITLApprovalStatus.APPROVED
        assert status == "FAILED"

    @pytest.mark.asyncio
    async def test_an_answer_to_a_gate_that_just_expired_is_gone(self, maker):
        """Expired but not yet swept: the caller hears why, not "not waiting"."""
        run_id, approval_id = await _paused_run(maker, timedelta(seconds=-5))
        async with maker() as session:
            payload = await mcp_tools.get_run_status(
                _caller("mcp"), run_id, session=session
            )
            assert "waiting_for" not in payload
            with pytest.raises(GoneError):
                await mcp_tools.respond_to_run(
                    _caller("mcp"), run_id, "yes", session=session
                )

        _, approval = await _stored(maker, run_id, approval_id)
        assert approval.status == HITLApprovalStatus.PENDING


class TestCallerOutsideTheRunsTeamspace:
    @pytest.mark.asyncio
    async def test_can_neither_see_nor_answer_the_gate(self, maker):
        run_id, approval_id = await _paused_run(maker)
        outsider = ("mcp", ("team_elsewhere",))
        launch = AsyncMock(return_value={"status": "running"})
        message = Message(role="user", parts=[Part(kind="text", text="Approved.")])
        with patch(RELAUNCH, new=launch):
            async with maker() as session:
                with pytest.raises(mcp_tools.UnknownRunError):
                    await mcp_tools.get_run_status(
                        _caller(*outsider), run_id, session=session
                    )
                with pytest.raises(mcp_tools.UnknownRunError):
                    await mcp_tools.respond_to_run(
                        _caller(*outsider), run_id, "yes", session=session
                    )
                with pytest.raises(a2a_tasks.UnknownTaskError):
                    await a2a_tasks.send_message(
                        _caller("a2a", ("team_elsewhere",)),
                        message,
                        task_id=run_id,
                        session=session,
                    )

        status, approval = await _stored(maker, run_id, approval_id)
        assert approval.status == HITLApprovalStatus.PENDING
        assert status == "WAITING_FOR_APPROVAL"
        launch.assert_not_awaited()


class TestA2aContinueTask:
    @pytest.mark.asyncio
    async def test_a_reply_on_the_task_approves_the_gate_and_resumes_it(self, maker):
        run_id, approval_id = await _paused_run(maker)
        launch = AsyncMock(return_value={"status": "running"})
        message = Message(role="user", parts=[Part(kind="text", text="Approved.")])
        with patch(RELAUNCH, new=launch):
            async with maker() as session:
                task = await a2a_tasks.send_message(
                    _caller("a2a"), message, task_id=run_id, session=session
                )
                await session.commit()
            await _relaunched(launch)

        assert task.id == run_id
        assert task.status.state == "TASK_STATE_WORKING"
        status, approval = await _stored(maker, run_id, approval_id)
        assert approval.status == HITLApprovalStatus.APPROVED
        assert approval.responded_by == "caller@example.com"
        assert status == "RUNNING"
        launch.assert_awaited_once()
        assert launch.await_args.kwargs["job_id"] == run_id
