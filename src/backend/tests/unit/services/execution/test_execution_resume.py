"""Unit tests for ExecutionService.resume_execution.

Resume creates a NEW execution linked to the source rather than re-running the
source record in place. The tests that used to assert the in-place behaviour
now assert the opposite, deliberately: the crashed run must stay FAILED.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.execution.service import ExecutionService
from src.utils.user_context import GroupContext

NEW_JOB_ID = "00000000-0000-0000-0000-00000000beef"

LEGACY_CHECKPOINT = {
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


@pytest.fixture
def group_context():
    return GroupContext(
        group_ids=["group-1"],
        group_email="user@example.com",
    )


@pytest.fixture
def service():
    session = MagicMock()
    session.commit = AsyncMock()
    svc = ExecutionService(session=session)
    yield svc
    ExecutionService.executions.pop(NEW_JOB_ID, None)
    ExecutionService.executions.pop("job-1", None)


def make_execution_row(
    status="FAILED",
    execution_type="crew",
    checkpoint_data=None,
    checkpoint_status="active",
    inputs=None,
):
    row = MagicMock()
    row.id = 42
    row.job_id = "job-1"
    row.status = status
    row.execution_type = execution_type
    row.run_name = "My Run"
    row.checkpoint_data = checkpoint_data
    row.checkpoint_status = checkpoint_status
    # Concrete, not MagicMock: these are pydantic-validated on the flow path.
    row.flow_uuid = None
    row.flow_id = None
    row.inputs = (
        inputs
        if inputs is not None
        else {
            "agents_yaml": {"agent_1": {"role": "worker"}},
            "tasks_yaml": {"task_1": {"description": "do it"}},
            "inputs": {"run_name": "My Run"},
            "planning": False,
            "model": "some-model",
            "schema_detection_enabled": True,
        }
    )
    return row


def patch_repo(row):
    repo = MagicMock()
    repo.get_execution_by_job_id = AsyncMock(return_value=row)
    repo.set_checkpoint_status = AsyncMock(return_value=True)
    repo.get_checkpoint_data = AsyncMock(
        return_value=row.checkpoint_data if row else None
    )
    repo.set_checkpoint_data = AsyncMock(return_value=True)
    return (
        patch(
            "src.repositories.execution_history_repository.ExecutionHistoryRepository",
            return_value=repo,
        ),
        repo,
    )


def resume_patches():
    """The collaborators a successful resume touches."""
    return (
        patch.object(ExecutionService, "_run_in_background", new_callable=AsyncMock),
        patch(
            "src.services.execution.service.ExecutionStatusService.create_execution",
            new_callable=AsyncMock,
            return_value=True,
        ),
        patch("src.services.execution.service.uuid.uuid4", return_value=NEW_JOB_ID),
    )


class TestResumeExecutionValidation:
    @pytest.mark.asyncio
    async def test_not_found_raises(self, service, group_context):
        patcher, _ = patch_repo(None)
        with patcher:
            with pytest.raises(ValueError, match="not found"):
                await service.resume_execution("job-1", group_context)

    @pytest.mark.asyncio
    async def test_flow_execution_is_accepted(self, service, group_context):
        """Flows used to be rejected outright; unification is the point."""
        row = make_execution_row(execution_type="flow", checkpoint_data=None)
        patcher, _ = patch_repo(row)
        run_bg, create_exec, uuid4 = resume_patches()

        with patcher, run_bg, create_exec, uuid4:
            result = await service.resume_execution("job-1", group_context)
            await asyncio.sleep(0)

        assert result["execution_id"] == NEW_JOB_ID


FLOW_CHECKPOINT = {
    "checkpoint": {
        "version": 1,
        "kind": "flow",
        "unit_count": 3,
        "units": {
            "1": {"key": "1", "name": "swiss news", "output_raw": "headlines"},
            "2": {"key": "2", "name": "send an email", "output_raw": "sent"},
        },
        "meta": {},
    }
}


class TestResumeFlowExecution:
    """A flow stores nodes/edges, never agents_yaml.

    Requiring a crew configuration rejected every flow resume with a 409 —
    the endpoint accepted flows at the type check and then ran crew-only code.
    """

    def _flow_row(self, checkpoint_data=FLOW_CHECKPOINT, **kw):
        row = make_execution_row(
            execution_type="flow",
            checkpoint_data=checkpoint_data,
            inputs={
                "nodes": [{"id": "n1"}],
                "edges": [{"source": "n1"}],
                "flow_config": {"startingPoints": []},
                "flow_id": "flow-uuid-1",
                "inputs": {"topic": "swiss news"},
                "model": "some-model",
            },
            **kw,
        )
        row.flow_uuid = "state-uuid-1"
        row.flow_id = "flow-uuid-1"
        return row

    @pytest.mark.asyncio
    async def test_a_completed_flow_resumes_without_a_crew_config(
        self, service, group_context
    ):
        patcher, _ = patch_repo(self._flow_row(status="COMPLETED"))
        run_bg_p, create_p, uuid_p = resume_patches()

        with patcher, run_bg_p as run_bg, create_p as create_exec, uuid_p:
            result = await service.resume_execution("job-1", group_context)
            await asyncio.sleep(0)

        assert result["execution_id"] == NEW_JOB_ID
        # Relaunched as a FLOW, not shoehorned through the crew path.
        assert run_bg.await_args.kwargs["execution_type"] == "flow"
        assert create_exec.await_args.args[0]["execution_type"] == "flow"

        config = run_bg.await_args.kwargs["config"]
        assert config.execution_type == "flow"
        assert config.nodes == [{"id": "n1"}]
        assert config.edges == [{"source": "n1"}]
        # Two crews completed, so the next one to run is sequence 3.
        assert config.resume_from_crew_sequence == 3
        assert config.resume_from_execution_id == "job-1"
        assert config.resume_from_flow_uuid == "state-uuid-1"
        assert result["restored_tasks"] == 2

    @pytest.mark.asyncio
    async def test_from_unit_selects_the_crew_to_re_run(self, service, group_context):
        patcher, _ = patch_repo(self._flow_row(status="COMPLETED"))
        run_bg_p, create_p, uuid_p = resume_patches()

        with patcher, run_bg_p as run_bg, create_p, uuid_p:
            result = await service.resume_execution(
                "job-1", group_context, from_unit="2"
            )
            await asyncio.sleep(0)

        # Redo from crew 2: crew 1 restored, crew 2 onward re-runs.
        assert run_bg.await_args.kwargs["config"].resume_from_crew_sequence == 2
        assert result["restored_tasks"] == 1

    @pytest.mark.asyncio
    async def test_the_resumed_run_stays_attached_to_its_flow(
        self, service, group_context
    ):
        patcher, _ = patch_repo(self._flow_row(status="FAILED"))
        run_bg_p, create_p, uuid_p = resume_patches()

        with patcher, run_bg_p, create_p as create_exec, uuid_p:
            await service.resume_execution("job-1", group_context)
            await asyncio.sleep(0)

        # Without flow_id the resumed run vanishes from its flow's checkpoint list.
        execution_data = create_exec.await_args.args[0]
        assert execution_data["flow_id"] == "flow-uuid-1"
        assert execution_data["resumed_from_execution_id"] == 42

    @pytest.mark.asyncio
    async def test_a_flow_with_no_checkpoint_runs_whole(self, service, group_context):
        patcher, _ = patch_repo(self._flow_row(checkpoint_data=None, status="FAILED"))
        run_bg_p, create_p, uuid_p = resume_patches()

        with patcher, run_bg_p as run_bg, create_p, uuid_p:
            result = await service.resume_execution("job-1", group_context)
            await asyncio.sleep(0)

        assert run_bg.await_args.kwargs["config"].resume_from_crew_sequence is None
        assert result["restored_tasks"] == 0

    @pytest.mark.asyncio
    async def test_chat_execution_rejected(self, service, group_context):
        """The chat path has nothing to resume and must say so."""
        patcher, _ = patch_repo(make_execution_row(execution_type="agent"))
        with patcher:
            with pytest.raises(ValueError, match="no checkpointing"):
                await service.resume_execution("job-1", group_context)

    @pytest.mark.asyncio
    async def test_running_execution_rejected(self, service, group_context):
        patcher, _ = patch_repo(make_execution_row(status="RUNNING"))
        with patcher:
            with pytest.raises(ValueError, match="still RUNNING"):
                await service.resume_execution("job-1", group_context)

    @pytest.mark.asyncio
    async def test_completed_execution_is_accepted(self, service, group_context):
        """Re-running a SUCCESSFUL run from the middle is a first-class action.

        It is how you iterate: change a downstream crew, keep the upstream
        results you were happy with.
        """
        row = make_execution_row(status="COMPLETED", checkpoint_data=LEGACY_CHECKPOINT)
        patcher, _ = patch_repo(row)
        run_bg, create_exec, uuid4 = resume_patches()

        with patcher, run_bg, create_exec, uuid4:
            result = await service.resume_execution(
                "job-1", group_context, from_unit="1"
            )
            await asyncio.sleep(0)

        assert result["execution_id"] == NEW_JOB_ID
        assert result["restored_tasks"] == 1

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


class TestResumeCreatesNewExecution:
    @pytest.mark.asyncio
    async def test_resume_with_checkpoint(self, service, group_context):
        row = make_execution_row(status="FAILED", checkpoint_data=LEGACY_CHECKPOINT)
        patcher, repo = patch_repo(row)
        run_bg_p, create_p, uuid_p = resume_patches()

        with patcher, run_bg_p as run_bg, create_p as create_exec, uuid_p:
            result = await service.resume_execution("job-1", group_context)
            await asyncio.sleep(0)

        # A NEW execution, not the old one re-run.
        assert result["execution_id"] == NEW_JOB_ID
        assert result["resumed_from"] == "job-1"
        assert result["status"] == "RUNNING"
        assert result["run_name"] == "My Run"
        assert result["restored_tasks"] == 2

        # ...linked back to the source by database id.
        create_exec.assert_awaited_once()
        execution_data = create_exec.await_args.args[0]
        assert execution_data["job_id"] == NEW_JOB_ID
        assert execution_data["resumed_from_execution_id"] == 42
        assert execution_data["status"] == "RUNNING"

        # The source checkpoint is spent, so the same crash cannot be resumed
        # twice in parallel.
        repo.set_checkpoint_status.assert_awaited_once()
        assert repo.set_checkpoint_status.await_args.args[1] == "resumed"

        # Relaunched under the NEW id with the checkpoint threaded through.
        run_bg.assert_awaited_once()
        bg_kwargs = run_bg.await_args.kwargs
        assert bg_kwargs["execution_id"] == NEW_JOB_ID
        assert bg_kwargs["execution_type"] == "crew"
        config = bg_kwargs["config"]
        assert [e["index"] for e in config.resume_checkpoint["completed"]] == [0, 1]
        assert config.agents_yaml == {"agent_1": {"role": "worker"}}

    @pytest.mark.asyncio
    async def test_source_record_is_never_flipped_back_to_running(
        self, service, group_context
    ):
        row = make_execution_row(status="FAILED", checkpoint_data=LEGACY_CHECKPOINT)
        patcher, _ = patch_repo(row)
        run_bg_p, create_p, uuid_p = resume_patches()

        with (
            patcher,
            run_bg_p,
            create_p,
            uuid_p,
            patch(
                "src.services.execution.service.ExecutionStatusService.update_status",
                new_callable=AsyncMock,
            ) as update_status,
        ):
            await service.resume_execution("job-1", group_context)
            await asyncio.sleep(0)

        # The whole point of the new semantic: a terminal record stays terminal.
        update_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_seeds_the_new_runs_checkpoint_from_the_restored_prefix(
        self, service, group_context
    ):
        row = make_execution_row(status="FAILED", checkpoint_data=LEGACY_CHECKPOINT)
        patcher, _ = patch_repo(row)
        run_bg_p, create_p, uuid_p = resume_patches()

        with (
            patcher,
            run_bg_p,
            create_p,
            uuid_p,
            patch(
                "src.services.execution.checkpointing.store.write_record",
                new_callable=AsyncMock,
                return_value=True,
            ) as write_record,
        ):
            await service.resume_execution("job-1", group_context)
            await asyncio.sleep(0)

        # So a SECOND crash resumes from everything already done, not only from
        # what this attempt manages to redo.
        write_record.assert_awaited_once()
        seeded = write_record.await_args.args[2]
        assert set(seeded["units"]) == {"0", "1"}

    @pytest.mark.asyncio
    async def test_from_unit_rewinds_further_back(self, service, group_context):
        row = make_execution_row(status="FAILED", checkpoint_data=LEGACY_CHECKPOINT)
        patcher, _ = patch_repo(row)
        run_bg_p, create_p, uuid_p = resume_patches()

        with patcher, run_bg_p as run_bg, create_p, uuid_p:
            result = await service.resume_execution(
                "job-1", group_context, from_unit="1"
            )
            await asyncio.sleep(0)

        # Resume AT unit 1 → only unit 0 restored; unit 1 onward re-runs.
        assert result["restored_tasks"] == 1
        config = run_bg.await_args.kwargs["config"]
        assert [e["index"] for e in config.resume_checkpoint["completed"]] == [0]

    @pytest.mark.asyncio
    async def test_resume_without_checkpoint_reruns_from_scratch(
        self, service, group_context
    ):
        row = make_execution_row(status="STOPPED", checkpoint_data=None)
        patcher, _ = patch_repo(row)
        run_bg_p, create_p, uuid_p = resume_patches()

        with patcher, run_bg_p as run_bg, create_p, uuid_p:
            result = await service.resume_execution("job-1", group_context)
            await asyncio.sleep(0)

        # A missing checkpoint is not an error — the run simply starts over.
        assert result["restored_tasks"] == 0
        assert run_bg.await_args.kwargs["config"].resume_checkpoint is None

    @pytest.mark.asyncio
    async def test_create_failure_aborts_before_launching(self, service, group_context):
        row = make_execution_row(status="FAILED")
        patcher, _ = patch_repo(row)
        run_bg_p, _, uuid_p = resume_patches()

        with (
            patcher,
            run_bg_p as run_bg,
            uuid_p,
            patch(
                "src.services.execution.service.ExecutionStatusService.create_execution",
                new_callable=AsyncMock,
                return_value=False,
            ),
        ):
            with pytest.raises(ValueError, match="Failed to create resumed execution"):
                await service.resume_execution("job-1", group_context)

        run_bg.assert_not_awaited()
