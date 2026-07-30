"""Tests for the flow-scoped checkpoint endpoints in flows_router.

These endpoints are deprecated in favour of ``/executions/{job_id}/checkpoints``
but remain flow-scoped ("every checkpoint for this saved flow"), which the
per-execution route deliberately is not.

They read the WRITTEN checkpoint only. The trace reconstruction that used to
back them is gone, so a run with no recorded checkpoint simply does not appear
rather than being inferred from telemetry.
"""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from src.api.flows_router import delete_checkpoint, get_flow_checkpoints
from src.utils.user_context import GroupContext


def gc():
    return GroupContext(
        group_ids=["g1"],
        group_email="u@test.com",
        email_domain="test.com",
        user_role="admin",
    )


def flow_service():
    svc = AsyncMock()
    svc.get_flow_with_group_check = AsyncMock(return_value=MagicMock())
    return svc


def summary(job_id="job-1", execution_id=1, units=None):
    """One entry as CheckpointService.list_for_flow returns it."""
    return {
        "job_id": job_id,
        "execution_id": execution_id,
        "flow_uuid": "flow-uuid-1",
        "checkpoint_method": "flow_complete",
        "status": "active",
        "created_at": datetime.utcnow(),
        "run_name": "Test Run",
        "units": units if units is not None else [],
    }


def checkpoint_service(summaries):
    svc = AsyncMock()
    svc.list_for_flow = AsyncMock(return_value=summaries)
    return svc


class TestGetFlowCheckpoints:
    @pytest.mark.asyncio
    async def test_returns_empty_when_nothing_was_recorded(self):
        result = await get_flow_checkpoints(
            flow_id=uuid4(),
            flow_service=flow_service(),
            checkpoint_service=checkpoint_service([]),
            group_context=gc(),
        )
        assert result.total == 0
        assert result.checkpoints == []

    @pytest.mark.asyncio
    async def test_reports_the_recorded_crews(self):
        units = [
            {
                "key": "1",
                "name": "research",
                "output_preview": "found it",
                "completed_at": "2026-07-30T10:00:00Z",
            },
            {
                "key": "2",
                "name": "write",
                "output_preview": "wrote it",
                "completed_at": "2026-07-30T10:05:00Z",
            },
        ]
        result = await get_flow_checkpoints(
            flow_id=uuid4(),
            flow_service=flow_service(),
            checkpoint_service=checkpoint_service([summary(units=units)]),
            group_context=gc(),
        )

        assert result.total == 1
        crews = result.checkpoints[0].crew_checkpoints
        assert [c.crew_name for c in crews] == ["research", "write"]
        assert [c.sequence for c in crews] == [1, 2]
        assert crews[0].output_preview == "found it"

    @pytest.mark.asyncio
    async def test_parses_a_string_timestamp(self):
        units = [
            {"key": "1", "name": "research", "completed_at": "2026-07-30T10:00:00Z"}
        ]
        result = await get_flow_checkpoints(
            flow_id=uuid4(),
            flow_service=flow_service(),
            checkpoint_service=checkpoint_service([summary(units=units)]),
            group_context=gc(),
        )
        assert isinstance(
            result.checkpoints[0].crew_checkpoints[0].completed_at, datetime
        )

    @pytest.mark.asyncio
    async def test_accepts_a_datetime_timestamp(self):
        units = [{"key": "1", "name": "research", "completed_at": datetime.utcnow()}]
        result = await get_flow_checkpoints(
            flow_id=uuid4(),
            flow_service=flow_service(),
            checkpoint_service=checkpoint_service([summary(units=units)]),
            group_context=gc(),
        )
        assert isinstance(
            result.checkpoints[0].crew_checkpoints[0].completed_at, datetime
        )

    @pytest.mark.asyncio
    async def test_passes_the_status_filter_through(self):
        svc = checkpoint_service([])
        await get_flow_checkpoints(
            flow_id=uuid4(),
            flow_service=flow_service(),
            checkpoint_service=svc,
            group_context=gc(),
            status_filter="expired",
        )
        assert svc.list_for_flow.call_args[1]["status_filter"] == "expired"

    @pytest.mark.asyncio
    async def test_scopes_to_the_callers_group(self):
        svc = checkpoint_service([])
        context = gc()
        await get_flow_checkpoints(
            flow_id=uuid4(),
            flow_service=flow_service(),
            checkpoint_service=svc,
            group_context=context,
        )
        assert svc.list_for_flow.call_args[1]["group_context"] is context

    @pytest.mark.asyncio
    async def test_verifies_flow_access_first(self):
        flow_svc = flow_service()
        flow_id = uuid4()
        context = gc()

        await get_flow_checkpoints(
            flow_id=flow_id,
            flow_service=flow_svc,
            checkpoint_service=checkpoint_service([]),
            group_context=context,
        )
        # A caller must not read another workspace's checkpoints by flow id.
        flow_svc.get_flow_with_group_check.assert_called_once_with(flow_id, context)


class TestDeleteCheckpoint:
    @pytest.mark.asyncio
    async def test_expires_the_checkpoint(self):
        exec_svc = AsyncMock()
        exec_svc.expire_checkpoint = AsyncMock(return_value=True)

        result = await delete_checkpoint(
            flow_id=uuid4(),
            execution_id=5,
            flow_service=flow_service(),
            execution_service=exec_svc,
            group_context=gc(),
        )

        assert result["status"] == "success"
        exec_svc.expire_checkpoint.assert_called_once_with(
            execution_id=5, group_id="g1"
        )

    @pytest.mark.asyncio
    async def test_verifies_flow_access_first(self):
        flow_svc = flow_service()
        flow_id = uuid4()
        context = gc()
        exec_svc = AsyncMock()
        exec_svc.expire_checkpoint = AsyncMock(return_value=True)

        await delete_checkpoint(
            flow_id=flow_id,
            execution_id=10,
            flow_service=flow_svc,
            execution_service=exec_svc,
            group_context=context,
        )
        flow_svc.get_flow_with_group_check.assert_called_once_with(flow_id, context)
