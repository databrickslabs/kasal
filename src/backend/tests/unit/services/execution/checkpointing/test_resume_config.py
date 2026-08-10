"""A resume runs the CURRENT definition, not the one that produced the run.

``execution_history.inputs`` is a snapshot frozen when the original run started.
Resuming from it meant an edit made afterwards was invisible — including to the
identity guard meant to catch exactly that, which was comparing a snapshot
against itself and so could never fire.

What is pinned here is which definition wins, and that the snapshot remains the
fallback for runs with nothing to rebuild from. What SURVIVES the rebuild is the
prefix rule, tested in runtime/test_checkpoint_restore.py.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.execution.checkpointing.resume_config import (
    build_crew_resume_config,
    build_flow_resume_config,
)

SNAPSHOT = {
    "agents_yaml": {"worker": {"role": "worker"}},
    "tasks_yaml": {"task_1": {"description": "the OLD text"}},
    "inputs": {"topic": "AI"},
    "model": "some-model",
}

CURRENT = (
    {"worker": {"role": "worker"}},
    {"task_1": {"description": "the NEW text"}},
)


def source(**overrides):
    row = SimpleNamespace(
        job_id="job-1",
        crew_id=None,
        flow_id=None,
        flow_uuid=None,
    )
    for key, value in overrides.items():
        setattr(row, key, value)
    return row


def patch_projection(result):
    return patch(
        "src.services.catalog.crew_config.build_crew_execution_config_by_id",
        new_callable=AsyncMock,
        return_value=result,
    )


class TestCrewRebuild:
    @pytest.mark.asyncio
    async def test_a_saved_crew_is_rebuilt_from_its_current_definition(self):
        with patch_projection(CURRENT):
            config, _, resume_inputs = await build_crew_resume_config(
                MagicMock(), source(crew_id="crew-1"), SNAPSHOT, None, None
            )

        assert config.tasks_yaml["task_1"]["description"] == "the NEW text"
        # ...and the NEW run records what it actually ran, so a second resume
        # does not fall back to text the first one already replaced.
        assert resume_inputs["tasks_yaml"]["task_1"]["description"] == "the NEW text"

    @pytest.mark.asyncio
    async def test_an_unsaved_crew_falls_back_to_its_snapshot(self):
        # No crew row to rebuild from — this run resumes exactly as it always
        # did, which is the whole reason the snapshot is kept.
        config, _, resume_inputs = await build_crew_resume_config(
            MagicMock(), source(), SNAPSHOT, None, None
        )

        assert config.tasks_yaml["task_1"]["description"] == "the OLD text"
        assert resume_inputs["tasks_yaml"] == SNAPSHOT["tasks_yaml"]

    @pytest.mark.asyncio
    async def test_a_deleted_crew_falls_back_to_its_snapshot(self):
        with patch_projection(None):
            config, _, _ = await build_crew_resume_config(
                MagicMock(), source(crew_id="crew-1"), SNAPSHOT, None, None
            )

        assert config.tasks_yaml["task_1"]["description"] == "the OLD text"

    @pytest.mark.asyncio
    async def test_a_half_resolved_crew_falls_back_rather_than_running_thin(self):
        # Agents resolved, tasks did not. Running this would be a SMALLER crew
        # than the one that was checkpointed, reported as a resume.
        with patch_projection(({"worker": {}}, {})):
            config, _, _ = await build_crew_resume_config(
                MagicMock(), source(crew_id="crew-1"), SNAPSHOT, None, None
            )

        assert config.tasks_yaml["task_1"]["description"] == "the OLD text"

    @pytest.mark.asyncio
    async def test_a_failing_rebuild_falls_back_rather_than_failing_the_resume(self):
        with patch(
            "src.services.catalog.crew_config.build_crew_execution_config_by_id",
            new_callable=AsyncMock,
            side_effect=RuntimeError("db down"),
        ):
            config, _, _ = await build_crew_resume_config(
                MagicMock(), source(crew_id="crew-1"), SNAPSHOT, None, None
            )

        assert config.tasks_yaml["task_1"]["description"] == "the OLD text"

    @pytest.mark.asyncio
    async def test_a_run_with_no_definition_at_all_is_rejected(self):
        with pytest.raises(ValueError, match="no stored crew configuration"):
            await build_crew_resume_config(
                MagicMock(), source(), {"inputs": {}}, None, None
            )


FLOW_SNAPSHOT = {
    "nodes": [{"id": "old"}],
    "edges": [],
    "flow_config": {"startingPoints": ["old"]},
    "inputs": {},
}


def patch_flow(flow):
    service = MagicMock()
    service.get_flow = AsyncMock(return_value=flow)
    service.get_flow_with_group_check = AsyncMock(return_value=flow)
    return patch(
        "src.services.flow_builder.flow_service.FlowService", return_value=service
    )


class TestFlowRebuild:
    @pytest.mark.asyncio
    async def test_a_saved_flow_is_rebuilt_from_its_current_definition(self):
        """Task TEXT was already current — the flow builder reads each task from
        the database by id. What the snapshot froze was the SHAPE: the
        startingPoints/listeners task-id lists, so a task added or rewired since
        the run was invisible."""
        flow = SimpleNamespace(
            nodes=[{"id": "new"}],
            edges=[{"id": "e1"}],
            flow_config={"startingPoints": ["new"]},
        )
        with patch_flow(flow):
            config, _, resume_inputs = await build_flow_resume_config(
                MagicMock(), source(flow_id="flow-1"), FLOW_SNAPSHOT, None, None
            )

        assert config.nodes == [{"id": "new"}]
        assert config.flow_config == {"startingPoints": ["new"]}
        assert resume_inputs["nodes"] == [{"id": "new"}]

    @pytest.mark.asyncio
    async def test_an_unsaved_flow_falls_back_to_its_snapshot(self):
        config, _, _ = await build_flow_resume_config(
            MagicMock(), source(), FLOW_SNAPSHOT, None, None
        )
        assert config.nodes == [{"id": "old"}]

    @pytest.mark.asyncio
    async def test_a_deleted_flow_falls_back_to_its_snapshot(self):
        with patch_flow(None):
            config, _, _ = await build_flow_resume_config(
                MagicMock(), source(flow_id="flow-1"), FLOW_SNAPSHOT, None, None
            )
        assert config.nodes == [{"id": "old"}]

    @pytest.mark.asyncio
    async def test_the_graph_falls_back_whole_never_field_by_field(self):
        """A current flow_config against stored nodes would reference node ids
        that may no longer exist."""
        flow = SimpleNamespace(nodes=[], edges=[{"id": "e1"}], flow_config={"x": 1})
        with patch_flow(flow):
            config, _, _ = await build_flow_resume_config(
                MagicMock(), source(flow_id="flow-1"), FLOW_SNAPSHOT, None, None
            )

        assert config.nodes == [{"id": "old"}]
        assert config.flow_config == {"startingPoints": ["old"]}


class TestTheResumePoint:
    """``resume_from_crew_sequence`` names the crew to RUN, so it is always one
    past the last crew being replayed. Leaving it undefined skipped nothing,
    which re-ran a whole flow while calling it a resume."""

    RECORD = {
        "kind": "flow",
        "units": {
            "1": {"key": "1", "name": "a"},
            "2": {"key": "2", "name": "b"},
        },
    }

    @pytest.mark.asyncio
    async def test_resuming_from_the_end_starts_past_the_last_recorded_crew(self):
        config, restored, _ = await build_flow_resume_config(
            MagicMock(), source(), FLOW_SNAPSHOT, self.RECORD, None
        )
        assert config.resume_from_crew_sequence == 3
        assert restored == 2

    @pytest.mark.asyncio
    async def test_an_explicit_point_rewinds_further_back(self):
        config, restored, _ = await build_flow_resume_config(
            MagicMock(), source(), FLOW_SNAPSHOT, self.RECORD, "2"
        )
        assert config.resume_from_crew_sequence == 2
        assert restored == 1

    @pytest.mark.asyncio
    async def test_nothing_recorded_runs_the_whole_flow(self):
        config, restored, _ = await build_flow_resume_config(
            MagicMock(), source(), FLOW_SNAPSHOT, None, None
        )
        assert config.resume_from_crew_sequence is None
        assert restored == 0

    @pytest.mark.asyncio
    async def test_a_non_numeric_point_is_rejected(self):
        with pytest.raises(ValueError, match="Invalid resume point"):
            await build_flow_resume_config(
                MagicMock(), source(), FLOW_SNAPSHOT, self.RECORD, "not-a-number"
            )
