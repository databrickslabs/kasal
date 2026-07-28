"""A2A task operations.

Every one is a translation over the shared EIL. The tests check the translation
and the refusals — not the execution behaviour, which is covered once against
``services/external/`` for both protocols.
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.schemas.a2a import Message, Part
from src.schemas.crew_publication import PublishedCapability
from src.services.a2a import tasks as a2a_tasks
from src.services.external.identity import ExternalCaller
from src.services.external.interaction import PendingInteraction
from src.services.external.invocation import InvocationResult
from src.services.external.state import ExternalTaskState


class _Ctx:
    def __init__(self, group_ids=("acme_corp",)):
        self.group_ids = list(group_ids)
        self.group_email = "agent@example.com"
        self.access_token = "tok"

    @property
    def primary_group_id(self):
        return self.group_ids[0]


def _caller():
    return ExternalCaller(
        group_context=_Ctx(), protocol="a2a", identifier="agent@example.com"
    )


def _msg(text="do the thing"):
    return Message(role="user", parts=[Part(kind="text", text=text)])


class TestSendMessage:
    @pytest.mark.asyncio
    async def test_starts_the_named_skill(self):
        publication = PublishedCapability(
            crew_id="c1", name="acme_report", description="d"
        )
        with (
            patch("src.services.a2a.tasks.PublicationService") as svc,
            patch(
                "src.services.a2a.tasks.start_run",
                new=AsyncMock(
                    return_value=InvocationResult(
                        run_id="run-1", state=ExternalTaskState.SUBMITTED
                    )
                ),
            ),
        ):
            svc.return_value.resolve_capability = AsyncMock(return_value=publication)
            task = await a2a_tasks.send_message(
                _caller(), _msg(), skill_id="acme_report"
            )

        assert task.id == "run-1"
        assert task.status.state == "TASK_STATE_SUBMITTED"

    @pytest.mark.asyncio
    async def test_returns_immediately_rather_than_waiting(self):
        """Crew runs take minutes; SendMessage must hand back a handle."""
        publication = PublishedCapability(crew_id="c1", name="x", description="d")
        with (
            patch("src.services.a2a.tasks.PublicationService") as svc,
            patch(
                "src.services.a2a.tasks.start_run",
                new=AsyncMock(
                    return_value=InvocationResult(
                        run_id="run-1", state=ExternalTaskState.SUBMITTED
                    )
                ),
            ) as start,
        ):
            svc.return_value.resolve_capability = AsyncMock(return_value=publication)
            await a2a_tasks.send_message(_caller(), _msg(), skill_id="x")

        # A submitted state, not a completed one — nothing awaited the run.
        assert start.await_count == 1

    @pytest.mark.asyncio
    async def test_unknown_skill_is_refused(self):
        with patch("src.services.a2a.tasks.PublicationService") as svc:
            svc.return_value.resolve_capability = AsyncMock(return_value=None)
            with pytest.raises(a2a_tasks.UnknownSkillError):
                await a2a_tasks.send_message(_caller(), _msg(), skill_id="nope")

    @pytest.mark.asyncio
    async def test_a_message_with_neither_skill_nor_task_is_rejected(self):
        with pytest.raises(a2a_tasks.UnknownSkillError):
            await a2a_tasks.send_message(_caller(), _msg())


class TestContinuingATask:
    @pytest.mark.asyncio
    async def test_a_message_with_a_task_id_answers_a_paused_run(self):
        """This is how A2A expresses completing a human-in-the-loop gate — the
        same round-trip MCP reaches through respond_to_run."""
        with (
            patch(
                "src.services.a2a.tasks.interaction.respond",
                new=AsyncMock(return_value=True),
            ) as respond,
            patch(
                "src.services.a2a.tasks.get_task",
                new=AsyncMock(return_value="the-refreshed-task"),
            ),
        ):
            result = await a2a_tasks.send_message(
                _caller(), _msg("approved"), task_id="run-1"
            )

        assert respond.await_args.kwargs["response"] == "approved"
        assert result == "the-refreshed-task"

    @pytest.mark.asyncio
    async def test_answering_a_task_that_is_not_waiting_is_refused(self):
        with patch(
            "src.services.a2a.tasks.interaction.respond",
            new=AsyncMock(return_value=False),
        ):
            with pytest.raises(a2a_tasks.UnknownTaskError):
                await a2a_tasks.send_message(_caller(), _msg(), task_id="run-1")


class TestGetTask:
    @pytest.mark.asyncio
    async def test_a_paused_run_reports_input_required_with_the_question(self):
        """The state the whole shared layer exists to express. The question
        rides in status.message, so the caller sees WHAT is being asked without
        a second request."""
        with (
            patch(
                "src.services.a2a.tasks.run_status",
                new=AsyncMock(
                    return_value=InvocationResult(
                        run_id="run-1", state=ExternalTaskState.WORKING
                    )
                ),
            ),
            patch(
                "src.services.a2a.tasks.interaction.pending_for_run",
                new=AsyncMock(
                    return_value=[PendingInteraction(approval_id=1, prompt="Ship it?")]
                ),
            ),
        ):
            task = await a2a_tasks.get_task(_caller(), "run-1")

        assert task.status.state == "TASK_STATE_INPUT_REQUIRED"
        assert task.status.message.parts[0].text == "Ship it?"

    @pytest.mark.asyncio
    async def test_a_finished_run_carries_its_output_as_an_artifact(self):
        with (
            patch(
                "src.services.a2a.tasks.run_status",
                new=AsyncMock(
                    return_value=InvocationResult(
                        run_id="run-1",
                        state=ExternalTaskState.COMPLETED,
                        output="the answer",
                    )
                ),
            ),
            patch(
                "src.services.a2a.tasks.interaction.pending_for_run",
                new=AsyncMock(return_value=[]),
            ),
        ):
            task = await a2a_tasks.get_task(_caller(), "run-1")

        assert task.status.state == "TASK_STATE_COMPLETED"
        assert task.artifacts[0].parts[0].text == "the answer"

    @pytest.mark.asyncio
    async def test_a_task_the_caller_may_not_see_is_refused(self):
        """None from the EIL covers "no such run" AND "another tenant's" — task
        ids must not become an oracle for other workspaces' activity."""
        with patch(
            "src.services.a2a.tasks.run_status", new=AsyncMock(return_value=None)
        ):
            with pytest.raises(a2a_tasks.UnknownTaskError):
                await a2a_tasks.get_task(_caller(), "someone-elses-run")


class TestCancel:
    @pytest.mark.asyncio
    async def test_cancelling_reports_the_canceled_state(self):
        with patch(
            "src.services.a2a.tasks.cancel_run",
            new=AsyncMock(
                return_value=InvocationResult(
                    run_id="run-1", state=ExternalTaskState.CANCELED
                )
            ),
        ):
            task = await a2a_tasks.cancel_task(_caller(), "run-1")
        assert task.status.state == "TASK_STATE_CANCELED"

    @pytest.mark.asyncio
    async def test_cancelling_a_task_the_caller_may_not_see_is_refused(self):
        with patch(
            "src.services.a2a.tasks.cancel_run", new=AsyncMock(return_value=None)
        ):
            with pytest.raises(a2a_tasks.UnknownTaskError):
                await a2a_tasks.cancel_task(_caller(), "run-1")


class TestListTasks:
    @pytest.mark.asyncio
    async def test_listing_is_scoped_to_the_callers_groups(self):
        """An unscoped ListTasks is a cross-tenant leak in one call."""
        with patch("src.services.execution.service.ExecutionService") as svc:
            svc.return_value.list_executions = AsyncMock(return_value=[])
            await a2a_tasks.list_tasks(_caller())

        assert svc.return_value.list_executions.await_args.kwargs["group_ids"] == [
            "acme_corp"
        ]

    @pytest.mark.asyncio
    async def test_executions_become_tasks_with_wire_states(self):
        with patch("src.services.execution.service.ExecutionService") as svc:
            svc.return_value.list_executions = AsyncMock(
                return_value=[{"execution_id": "run-1", "status": "RUNNING"}]
            )
            tasks = await a2a_tasks.list_tasks(_caller())

        assert tasks[0].id == "run-1"
        assert tasks[0].status.state == "TASK_STATE_WORKING"
