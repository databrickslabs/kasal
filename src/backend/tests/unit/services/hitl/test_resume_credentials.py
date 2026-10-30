"""A flow resumed or retried at an HITL gate runs on the token of the person
who acted on the gate, never on one read back from the stored run.

Rows stored before the write side was fixed can still hold the original
caller's token in ``executionhistory.inputs``; it must not ride into the
resumed run's config.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.api.hitl_router import reject_gate
from src.schemas.hitl import HITLRejectionActionEnum, HITLRejectRequest
from src.services.hitl.service import HITLService

STORED = "stored-obo-token-value"
FRESH = "fresh-obo-token-value"


def _approval():
    return SimpleNamespace(
        id=1,
        execution_id="exec-1",
        group_id="ws-a",
        crew_sequence=1,
        previous_crew_name="Research Crew",
        rejection_reason="again",
        responded_by="b@example.com",
        status="approved",
    )


def _legacy_run():
    return SimpleNamespace(
        flow_id="flow-1",
        flow_uuid="uuid-1",
        inputs={
            "flow_id": "flow-1",
            "nodes": [{"id": "n1"}],
            "edges": [],
            "inputs": {"topic": "t"},
            "user_token": STORED,
            "access_token": STORED,
        },
    )


async def _relaunch(action):
    """Run the HITL relaunch and return what run_flow_execution was given."""
    service = HITLService(session=AsyncMock())
    scheduled = []
    with (
        patch(
            "src.services.execution.service.ExecutionService.get_run_by_job_id",
            new=AsyncMock(return_value=_legacy_run()),
        ),
        patch.object(service, "_update_execution_status", new=AsyncMock()),
        patch(
            "src.services.execution.kasal_service.KasalExecutionService."
            "run_flow_execution",
            new=AsyncMock(return_value={"status": "ok"}),
        ) as launch,
        patch("asyncio.create_task", side_effect=scheduled.append),
    ):
        assert await action(service) is True
        for coro in scheduled:
            await coro
    return launch.await_args.kwargs


class TestTheRelaunchUsesTheActorsToken:
    @pytest.mark.asyncio
    async def test_approve_resume(self):
        kwargs = await _relaunch(
            lambda s: s._resume_flow_execution(_approval(), user_token=FRESH)
        )
        assert kwargs["group_context"].access_token == FRESH
        assert "user_token" not in kwargs["config"]
        assert "access_token" not in kwargs["config"]
        assert STORED not in str(kwargs["config"])
        assert kwargs["config"]["inputs"] == {"topic": "t"}

    @pytest.mark.asyncio
    async def test_reject_and_retry(self):
        kwargs = await _relaunch(
            lambda s: s._retry_previous_crew(_approval(), user_token=FRESH)
        )
        assert kwargs["group_context"].access_token == FRESH
        assert STORED not in str(kwargs["config"])

    @pytest.mark.asyncio
    async def test_reject_hands_the_token_to_the_retry(self):
        service = HITLService(session=AsyncMock(), approval_repository=AsyncMock())
        approval = _approval()
        approval.status = "pending"
        approval.is_expired = False
        approval.gate_config = {}
        approval.can_be_approved_by = lambda _email: True
        service.approval_repo.get_by_id.return_value = approval
        with patch.object(
            service, "_retry_previous_crew", new=AsyncMock(return_value=True)
        ) as retry:
            await service.reject(
                approval_id=1,
                rejected_by="b@example.com",
                group_id="ws-a",
                reason="again",
                action=HITLRejectionActionEnum.RETRY,
                user_token=FRESH,
            )
        assert retry.await_args.kwargs["user_token"] == FRESH

    @pytest.mark.asyncio
    async def test_the_reject_route_passes_the_callers_token(self):
        service = AsyncMock()
        ctx = SimpleNamespace(
            group_email="b@example.com",
            primary_group_id="ws-a",
            group_ids=["ws-a"],
            access_token=FRESH,
        )
        await reject_gate(
            approval_id=1,
            request=HITLRejectRequest(
                reason="again", action=HITLRejectionActionEnum.RETRY
            ),
            service=service,
            group_context=ctx,
        )
        assert service.reject.await_args.kwargs["user_token"] == FRESH
