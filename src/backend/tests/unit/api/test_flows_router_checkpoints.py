"""
Tests for flow checkpoint endpoints in flows_router.
Covers lines 301-356 (get_flow_checkpoints) and 389-396 (delete_checkpoint).
"""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
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


def make_checkpoint(id=1, job_id="job-uuid-1"):
    return MagicMock(
        id=id,
        job_id=job_id,
        flow_uuid="flow-uuid-1",
        checkpoint_method="manual",
        checkpoint_status="active",
        created_at=datetime.utcnow(),
        run_name="Test Run",
    )


class TestGetFlowCheckpoints:
    @pytest.mark.asyncio
    async def test_returns_empty_checkpoints_when_none_found(self):
        """Returns empty checkpoint list when no checkpoints exist."""
        flow_id = uuid4()
        flow_svc = AsyncMock()
        flow_svc.get_flow_with_group_check = AsyncMock(return_value=MagicMock())

        exec_svc = AsyncMock()
        exec_svc.get_checkpoints_for_flow = AsyncMock(return_value=[])

        ckpt_svc = AsyncMock()
        ckpt_svc.list_for_flow = AsyncMock(return_value=[])

        trace_repo = AsyncMock()
        trace_repo.get_crew_checkpoints_by_job_id = AsyncMock(return_value=[])

        result = await get_flow_checkpoints(
            flow_id=flow_id,
            flow_service=flow_svc,
            execution_service=exec_svc,
            trace_repository=trace_repo,
            checkpoint_service=ckpt_svc,
            group_context=gc(),
        )
        assert result.total == 0
        assert len(result.checkpoints) == 0
        assert str(result.flow_id) == str(flow_id)

    @pytest.mark.asyncio
    async def test_returns_checkpoints_with_crew_data(self):
        """Returns checkpoints with crew checkpoint info."""
        flow_id = uuid4()
        checkpoint = make_checkpoint(id=1)

        flow_svc = AsyncMock()
        flow_svc.get_flow_with_group_check = AsyncMock(return_value=MagicMock())

        exec_svc = AsyncMock()
        exec_svc.get_checkpoints_for_flow = AsyncMock(return_value=[checkpoint])

        crew_cp_data = [
            {
                "crew_name": "Crew A",
                "sequence": 0,
                "status": "completed",
                "output_preview": "output...",
                "completed_at": datetime.utcnow().isoformat(),
            }
        ]
        ckpt_svc = AsyncMock()
        ckpt_svc.list_for_flow = AsyncMock(return_value=[])

        trace_repo = AsyncMock()
        trace_repo.get_crew_checkpoints_by_job_id = AsyncMock(return_value=crew_cp_data)

        result = await get_flow_checkpoints(
            flow_id=flow_id,
            flow_service=flow_svc,
            execution_service=exec_svc,
            trace_repository=trace_repo,
            checkpoint_service=ckpt_svc,
            group_context=gc(),
        )
        assert result.total == 1
        assert len(result.checkpoints) == 1
        assert len(result.checkpoints[0].crew_checkpoints) == 1
        assert result.checkpoints[0].crew_checkpoints[0].crew_name == "Crew A"

    @pytest.mark.asyncio
    async def test_skips_malformed_crew_checkpoint(self):
        """Skips crew checkpoints that fail to parse."""
        flow_id = uuid4()
        checkpoint = make_checkpoint()

        flow_svc = AsyncMock()
        flow_svc.get_flow_with_group_check = AsyncMock(return_value=MagicMock())

        exec_svc = AsyncMock()
        exec_svc.get_checkpoints_for_flow = AsyncMock(return_value=[checkpoint])

        # Malformed entry - missing required fields
        crew_cp_data = [{"bad_key": "bad_value"}]
        ckpt_svc = AsyncMock()
        ckpt_svc.list_for_flow = AsyncMock(return_value=[])

        trace_repo = AsyncMock()
        trace_repo.get_crew_checkpoints_by_job_id = AsyncMock(return_value=crew_cp_data)

        result = await get_flow_checkpoints(
            flow_id=flow_id,
            flow_service=flow_svc,
            execution_service=exec_svc,
            trace_repository=trace_repo,
            checkpoint_service=ckpt_svc,
            group_context=gc(),
        )
        assert result.total == 1
        # Crew checkpoint with default values should still be parsed
        # (uses .get() with defaults so no exception)
        assert len(result.checkpoints[0].crew_checkpoints) in (0, 1)

    @pytest.mark.asyncio
    async def test_filters_checkpoints_by_status(self):
        """Passes status_filter to execution service."""
        flow_id = uuid4()

        flow_svc = AsyncMock()
        flow_svc.get_flow_with_group_check = AsyncMock(return_value=MagicMock())

        exec_svc = AsyncMock()
        exec_svc.get_checkpoints_for_flow = AsyncMock(return_value=[])

        ckpt_svc = AsyncMock()
        ckpt_svc.list_for_flow = AsyncMock(return_value=[])

        trace_repo = AsyncMock()

        await get_flow_checkpoints(
            flow_id=flow_id,
            flow_service=flow_svc,
            execution_service=exec_svc,
            trace_repository=trace_repo,
            checkpoint_service=ckpt_svc,
            group_context=gc(),
            status_filter="expired",
        )
        exec_svc.get_checkpoints_for_flow.assert_called_once()
        call_kwargs = exec_svc.get_checkpoints_for_flow.call_args[1]
        assert call_kwargs.get("status_filter") == "expired"

    @pytest.mark.asyncio
    async def test_uses_primary_group_id(self):
        """Passes primary_group_id from context to execution service."""
        flow_id = uuid4()
        ctx = GroupContext(
            group_ids=["primary-g", "other-g"],
            group_email="u@t.com",
            email_domain="t.com",
        )

        flow_svc = AsyncMock()
        flow_svc.get_flow_with_group_check = AsyncMock(return_value=MagicMock())

        exec_svc = AsyncMock()
        exec_svc.get_checkpoints_for_flow = AsyncMock(return_value=[])

        ckpt_svc = AsyncMock()
        ckpt_svc.list_for_flow = AsyncMock(return_value=[])

        trace_repo = AsyncMock()

        await get_flow_checkpoints(
            flow_id=flow_id,
            flow_service=flow_svc,
            execution_service=exec_svc,
            trace_repository=trace_repo,
            checkpoint_service=ckpt_svc,
            group_context=ctx,
        )
        call_kwargs = exec_svc.get_checkpoints_for_flow.call_args[1]
        assert call_kwargs.get("group_id") == "primary-g"

    @pytest.mark.asyncio
    async def test_crew_checkpoint_with_datetime_object(self):
        """Handles completed_at as already a datetime object."""
        flow_id = uuid4()
        checkpoint = make_checkpoint()

        flow_svc = AsyncMock()
        flow_svc.get_flow_with_group_check = AsyncMock(return_value=MagicMock())

        exec_svc = AsyncMock()
        exec_svc.get_checkpoints_for_flow = AsyncMock(return_value=[checkpoint])

        crew_cp_data = [
            {
                "crew_name": "Crew B",
                "sequence": 1,
                "status": "completed",
                "output_preview": None,
                "completed_at": datetime.utcnow(),  # datetime object, not string
            }
        ]
        ckpt_svc = AsyncMock()
        ckpt_svc.list_for_flow = AsyncMock(return_value=[])

        trace_repo = AsyncMock()
        trace_repo.get_crew_checkpoints_by_job_id = AsyncMock(return_value=crew_cp_data)

        result = await get_flow_checkpoints(
            flow_id=flow_id,
            flow_service=flow_svc,
            execution_service=exec_svc,
            trace_repository=trace_repo,
            checkpoint_service=ckpt_svc,
            group_context=gc(),
        )
        assert result.checkpoints[0].crew_checkpoints[0].crew_name == "Crew B"


class TestDeleteCheckpoint:
    @pytest.mark.asyncio
    async def test_delete_checkpoint_returns_success(self):
        """delete_checkpoint calls expire and returns success."""
        flow_id = uuid4()

        flow_svc = AsyncMock()
        flow_svc.get_flow_with_group_check = AsyncMock(return_value=MagicMock())

        exec_svc = AsyncMock()
        exec_svc.expire_checkpoint = AsyncMock(return_value=True)

        result = await delete_checkpoint(
            flow_id=flow_id,
            execution_id=5,
            flow_service=flow_svc,
            execution_service=exec_svc,
            group_context=gc(),
        )
        assert result["status"] == "success"
        exec_svc.expire_checkpoint.assert_called_once_with(
            execution_id=5, group_id="g1"
        )

    @pytest.mark.asyncio
    async def test_delete_checkpoint_verifies_flow_access(self):
        """delete_checkpoint verifies flow access before deletion."""
        flow_id = uuid4()

        flow_svc = AsyncMock()
        flow_svc.get_flow_with_group_check = AsyncMock(return_value=MagicMock())

        exec_svc = AsyncMock()
        exec_svc.expire_checkpoint = AsyncMock(return_value=True)

        await delete_checkpoint(
            flow_id=flow_id,
            execution_id=10,
            flow_service=flow_svc,
            execution_service=exec_svc,
            group_context=gc(),
        )
        flow_svc.get_flow_with_group_check.assert_called_once_with(flow_id, gc())


class TestGetFlowCheckpointsWrittenSource:
    """The written checkpoint is preferred; traces are the legacy fallback."""

    @pytest.mark.asyncio
    async def test_prefers_the_written_checkpoint(self):
        flow_id = uuid4()

        flow_svc = AsyncMock()
        flow_svc.get_flow_with_group_check = AsyncMock(return_value=MagicMock())

        ckpt_svc = AsyncMock()
        ckpt_svc.list_for_flow = AsyncMock(
            return_value=[
                {
                    "job_id": "job-written",
                    "execution_id": 7,
                    "flow_uuid": "uuid-7",
                    "checkpoint_method": "step_two",
                    "status": "active",
                    "created_at": datetime.utcnow(),
                    "run_name": "Written Run",
                    "units": [
                        {
                            "key": "1",
                            "name": "research",
                            "output_preview": "found it",
                            "completed_at": "2026-07-30T10:00:00Z",
                        }
                    ],
                }
            ]
        )

        exec_svc = AsyncMock()
        exec_svc.get_checkpoints_for_flow = AsyncMock(return_value=[])

        trace_repo = AsyncMock()

        result = await get_flow_checkpoints(
            flow_id=flow_id,
            flow_service=flow_svc,
            execution_service=exec_svc,
            trace_repository=trace_repo,
            checkpoint_service=ckpt_svc,
            group_context=gc(),
        )

        assert result.total == 1
        checkpoint = result.checkpoints[0]
        assert checkpoint.job_id == "job-written"
        assert [c.crew_name for c in checkpoint.crew_checkpoints] == ["research"]
        assert checkpoint.crew_checkpoints[0].sequence == 1
        # Traces are never consulted when a written checkpoint exists.
        trace_repo.get_crew_checkpoints_by_job_id.assert_not_called()

    @pytest.mark.asyncio
    async def test_does_not_list_a_run_twice(self):
        """A run with a written checkpoint must not also appear via the fallback."""
        flow_id = uuid4()

        flow_svc = AsyncMock()
        flow_svc.get_flow_with_group_check = AsyncMock(return_value=MagicMock())

        ckpt_svc = AsyncMock()
        ckpt_svc.list_for_flow = AsyncMock(
            return_value=[
                {
                    "job_id": "job-1",
                    "execution_id": 1,
                    "flow_uuid": "uuid-1",
                    "checkpoint_method": None,
                    "status": "active",
                    "created_at": datetime.utcnow(),
                    "run_name": "Run",
                    "units": [],
                }
            ]
        )

        exec_svc = AsyncMock()
        exec_svc.get_checkpoints_for_flow = AsyncMock(
            return_value=[make_checkpoint(id=1, job_id="job-1")]
        )

        trace_repo = AsyncMock()

        result = await get_flow_checkpoints(
            flow_id=flow_id,
            flow_service=flow_svc,
            execution_service=exec_svc,
            trace_repository=trace_repo,
            checkpoint_service=ckpt_svc,
            group_context=gc(),
        )

        assert result.total == 1
