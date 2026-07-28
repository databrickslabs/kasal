"""Unit tests for ExecutionService.resume_execution (crew checkpoint resume)."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.execution.service import ExecutionService
from src.utils.user_context import GroupContext


@pytest.fixture
def group_context():
    return GroupContext(
        group_ids=["group-1"],
        group_email="user@example.com",
    )


@pytest.fixture
def service():
    svc = ExecutionService(session=MagicMock())
    yield svc
    ExecutionService.executions.pop("job-1", None)


def make_execution_row(
    status="FAILED",
    execution_type="crew",
    checkpoint_data=None,
    inputs=None,
):
    row = MagicMock()
    row.job_id = "job-1"
    row.status = status
    row.execution_type = execution_type
    row.run_name = "My Run"
    row.checkpoint_data = checkpoint_data
    row.inputs = inputs if inputs is not None else {
        "agents_yaml": {"agent_1": {"role": "worker"}},
        "tasks_yaml": {"task_1": {"description": "do it"}},
        "inputs": {"run_name": "My Run"},
        "planning": False,
        "model": "some-model",
        "schema_detection_enabled": True,
    }
    return row


def patch_repo(row):
    repo = MagicMock()
    repo.get_execution_by_job_id = AsyncMock(return_value=row)
    return patch(
        "src.repositories.execution_history_repository.ExecutionHistoryRepository",
        return_value=repo,
    ), repo


class TestResumeExecutionValidation:
    @pytest.mark.asyncio
    async def test_not_found_raises(self, service, group_context):
        patcher, _ = patch_repo(None)
        with patcher:
            with pytest.raises(ValueError, match="not found"):
                await service.resume_execution("job-1", group_context)

    @pytest.mark.asyncio
    async def test_flow_execution_rejected(self, service, group_context):
        patcher, _ = patch_repo(make_execution_row(execution_type="flow"))
        with patcher:
            with pytest.raises(ValueError, match="Only crew executions"):
                await service.resume_execution("job-1", group_context)

    @pytest.mark.asyncio
    async def test_running_execution_rejected(self, service, group_context):
        patcher, _ = patch_repo(make_execution_row(status="RUNNING"))
        with patcher:
            with pytest.raises(ValueError, match="only failed/stopped"):
                await service.resume_execution("job-1", group_context)

    @pytest.mark.asyncio
    async def test_completed_execution_rejected(self, service, group_context):
        patcher, _ = patch_repo(make_execution_row(status="COMPLETED"))
        with patcher:
            with pytest.raises(ValueError):
                await service.resume_execution("job-1", group_context)

    @pytest.mark.asyncio
    async def test_missing_stored_config_rejected(self, service, group_context):
        patcher, _ = patch_repo(make_execution_row(inputs={"agents_yaml": {}}))
        with patcher:
            with pytest.raises(ValueError, match="no stored crew configuration"):
                await service.resume_execution("job-1", group_context)

    @pytest.mark.asyncio
    async def test_group_filter_applied(self, service, group_context):
        patcher, repo = patch_repo(None)
        with patcher:
            with pytest.raises(ValueError):
                await service.resume_execution("job-1", group_context)
        repo.get_execution_by_job_id.assert_awaited_once_with(
            "job-1", group_ids=["group-1"]
        )


class TestResumeExecutionHappyPath:
    @pytest.mark.asyncio
    async def test_resume_with_checkpoint(self, service, group_context):
        checkpoint_data = {
            "crew_task_checkpoint": {
                "version": 1,
                "task_count": 3,
                "process": "sequential",
                "completed": {
                    "1": {"index": 1, "task_key": "k1", "output_raw": "b"},
                    "0": {"index": 0, "task_key": "k0", "output_raw": "a"},
                },
            }
        }
        row = make_execution_row(status="FAILED", checkpoint_data=checkpoint_data)
        patcher, _ = patch_repo(row)

        with patcher, \
             patch.object(
                 ExecutionService, "_run_in_background", new_callable=AsyncMock
             ) as run_bg, \
             patch(
                 "src.services.execution.service.ExecutionStatusService.update_status",
                 new_callable=AsyncMock,
                 return_value=True,
             ) as update_status:
            result = await service.resume_execution("job-1", group_context)
            # let the created background task start/finish
            await asyncio.sleep(0)

        assert result["execution_id"] == "job-1"
        assert result["status"] == "RUNNING"
        assert result["run_name"] == "My Run"
        assert result["restored_tasks"] == 2

        # status flipped back to RUNNING with a resume message
        update_status.assert_awaited_once()
        kwargs = update_status.await_args.kwargs
        assert kwargs["job_id"] == "job-1"
        assert kwargs["status"] == "RUNNING"
        assert "2 completed task(s) restored" in kwargs["message"]

        # relaunched with the checkpoint threaded through the CrewConfig
        run_bg.assert_awaited_once()
        bg_kwargs = run_bg.await_args.kwargs
        assert bg_kwargs["execution_id"] == "job-1"
        assert bg_kwargs["execution_type"] == "crew"
        config = bg_kwargs["config"]
        assert config.resume_checkpoint is not None
        assert [e["index"] for e in config.resume_checkpoint["completed"]] == [0, 1]
        assert config.agents_yaml == {"agent_1": {"role": "worker"}}
        assert config.tasks_yaml == {"task_1": {"description": "do it"}}

    @pytest.mark.asyncio
    async def test_resume_without_checkpoint_reruns_from_scratch(
        self, service, group_context
    ):
        row = make_execution_row(status="STOPPED", checkpoint_data=None)
        patcher, _ = patch_repo(row)

        with patcher, \
             patch.object(
                 ExecutionService, "_run_in_background", new_callable=AsyncMock
             ) as run_bg, \
             patch(
                 "src.services.execution.service.ExecutionStatusService.update_status",
                 new_callable=AsyncMock,
                 return_value=True,
             ) as update_status:
            result = await service.resume_execution("job-1", group_context)
            await asyncio.sleep(0)

        assert result["restored_tasks"] == 0
        assert "no checkpoint found" in update_status.await_args.kwargs["message"]
        assert run_bg.await_args.kwargs["config"].resume_checkpoint is None

    @pytest.mark.asyncio
    async def test_status_update_failure_aborts_resume(self, service, group_context):
        row = make_execution_row(status="FAILED")
        patcher, _ = patch_repo(row)

        with patcher, \
             patch.object(
                 ExecutionService, "_run_in_background", new_callable=AsyncMock
             ) as run_bg, \
             patch(
                 "src.services.execution.service.ExecutionStatusService.update_status",
                 new_callable=AsyncMock,
                 return_value=False,
             ):
            with pytest.raises(ValueError, match="Failed to reset status"):
                await service.resume_execution("job-1", group_context)
        run_bg.assert_not_awaited()
