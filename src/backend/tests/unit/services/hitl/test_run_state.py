"""A decided in-run gate never turns a settled run back into a running one.

The scenario: a hard kill leaves a run subprocess behind, the restarted server
reaps its gate and cancels the run, and the orphan's poller then reads the
closed gate as a decision and restores RUNNING.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.services.execution.kernel import tool_approval
from src.services.guardrails.core.human_review_guardrail import HumanReviewGuardrail
from src.services.hitl.run_state import restore_running

STATUS = "src.services.execution.status.ExecutionStatusService"


def _run_is(status):
    return (
        patch(
            f"{STATUS}.get_status",
            new=AsyncMock(return_value=SimpleNamespace(status=status)),
        ),
        patch(f"{STATUS}.update_status", new=AsyncMock(return_value=True)),
    )


class TestRestoreRunning:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "settled",
        ["CANCELLED", "FAILED", "COMPLETED", "REJECTED", "STOPPED", "STOPPING"],
    )
    async def test_a_settled_run_is_left_as_it_is(self, settled):
        read, write = _run_is(settled)
        with read, write as update:
            assert await restore_running("run-1", "decided") is True
        update.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_waiting_run_is_running_again(self):
        read, write = _run_is("WAITING_FOR_APPROVAL")
        with read, write as update:
            assert await restore_running("run-1", "decided") is True
        assert update.await_args.kwargs["status"] == "RUNNING"


class TestTheGateWorkersUseIt:
    @pytest.mark.asyncio
    async def test_the_tool_gate_does_not_revive_a_cancelled_run(self):
        read, write = _run_is("CANCELLED")
        with read, write as update:
            await tool_approval._restore_running("run-1")
        update.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_task_review_gate_does_not_revive_a_cancelled_run(self):
        guardrail = HumanReviewGuardrail(
            task_name="draft", execution_id="run-1", group_id="team_reports"
        )
        read, write = _run_is("CANCELLED")
        with read, write as update:
            await guardrail._restore_running()
        update.assert_not_awaited()
