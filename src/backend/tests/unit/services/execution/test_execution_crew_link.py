"""A crew run records the saved crew it was built from.

``flow_id`` has always been recorded for flows; the crew side recorded nothing,
which is why a crew resume could only replay the ``inputs`` snapshot frozen when
the original run started. These tests pin the link itself — what a resume then
does with it is tested with the resume.
"""

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.schemas.execution import CrewConfig
from src.services.execution.service import ExecutionService
from src.utils.user_context import GroupContext

CREW_ID = str(uuid.uuid4())


def crew_config(**overrides) -> CrewConfig:
    return CrewConfig(
        agents_yaml={"agent_1": {"role": "worker"}},
        tasks_yaml={"task_1": {"description": "do it"}},
        execution_type="crew",
        **overrides,
    )


RUN_GROUP = "ws-run"
RUN_CONTEXT = GroupContext(group_ids=[RUN_GROUP], group_email="u@example.com")


async def _crew_of_the_runs_group(crew_id, group_context):
    """CrewService.get_by_group, for a catalog holding CREW_ID in RUN_GROUP."""
    in_group = group_context.primary_group_id == RUN_GROUP
    return (
        SimpleNamespace(id=crew_id)
        if in_group and crew_id == uuid.UUID(CREW_ID)
        else None
    )


async def created_execution_data(config: CrewConfig) -> dict:
    """Run create_execution and return the row data it tried to persist."""
    service = ExecutionService(session=MagicMock())
    with (
        patch(
            "src.services.execution.status.ExecutionStatusService.create_execution",
            new_callable=AsyncMock,
            return_value=True,
        ) as create,
        patch.object(ExecutionService, "_run_in_background", new_callable=AsyncMock),
        patch("src.services.execution.service.ExecutionNameService") as names,
        patch(
            "src.services.catalog.crews.CrewService.get_by_group",
            new=AsyncMock(side_effect=_crew_of_the_runs_group),
        ),
    ):
        names.return_value.generate_execution_name = AsyncMock(
            return_value=MagicMock(name="run")
        )
        await service.create_execution(config, group_context=RUN_CONTEXT)
        return create.await_args.args[0]


class TestCrewLink:
    @pytest.mark.asyncio
    async def test_saved_crew_is_recorded_as_a_uuid(self):
        data = await created_execution_data(crew_config(crew_id=CREW_ID))
        # A UUID object, not the string: the column is UUID(as_uuid=True), and
        # a str reaches PostgreSQL as text.
        assert data["crew_id"] == uuid.UUID(CREW_ID)

    @pytest.mark.asyncio
    async def test_a_crew_outside_the_runs_workspace_is_not_recorded(self):
        # The id comes with the request; the run records it only for a crew of
        # the workspace the run is filed in.
        other = str(uuid.uuid4())
        data = await created_execution_data(crew_config(crew_id=other))
        assert data["crew_id"] is None

    @pytest.mark.asyncio
    async def test_unsaved_canvas_records_nothing(self):
        # There is no crew row to point at, and that absence is what makes the
        # stored snapshot the fallback rather than the source.
        assert "crew_id" not in await created_execution_data(crew_config())

    @pytest.mark.asyncio
    async def test_unparseable_crew_id_does_not_fail_the_run(self):
        # The link is optional context for a LATER resume. Refusing to start a
        # crew over a malformed optional reference would be a far worse trade.
        data = await created_execution_data(crew_config(crew_id="not-a-uuid"))
        assert data.get("crew_id") is None

    @pytest.mark.asyncio
    @patch(
        "src.services.flow_builder.flow_service.FlowService.get_flow_for_execution",
        AsyncMock(return_value=MagicMock()),
    )
    async def test_a_flow_run_does_not_take_the_crew_link(self):
        # A flow already records flow_id; crew_id on a flow row would be a
        # second, contradictory answer to "what definition was this built from".
        #
        # flow_id is given explicitly: a flow run without flow_id or nodes is
        # rejected before anything is written.
        config = CrewConfig(
            execution_type="flow",
            crew_id=CREW_ID,
            flow_id=str(uuid.uuid4()),
            nodes=[],
            edges=[],
        )
        data = await created_execution_data(config)
        assert "crew_id" not in data
        assert data["flow_id"] is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["crew", "flow"])
async def test_completed_run_details_preserve_catalog_identity_and_configuration(kind):
    """Run actions must resolve the saved crew/flow from the details endpoint."""
    from datetime import datetime
    from types import SimpleNamespace

    from src.schemas.execution import ExecutionResponse

    definition_id = uuid.uuid4()
    config = (
        {"nodes": [{"type": "crewNode", "data": {"crewId": str(definition_id)}}]}
        if kind == "flow"
        else {"agents_yaml": {"researcher": {"memory": True}}}
    )
    summary = SimpleNamespace(
        status="COMPLETED",
        created_at=datetime(2026, 9, 6),
        completed_at=None,
        run_name="Saved research",
        error=None,
        execution_type=kind,
        mlflow_trace_id=None,
        mlflow_experiment_name=None,
        mlflow_evaluation_run_id=None,
    )
    row = SimpleNamespace(
        result={"output": "done"},
        inputs=config,
        crew_id=definition_id if kind == "crew" else None,
        flow_id=definition_id if kind == "flow" else None,
    )
    repo = AsyncMock()
    repo.get_execution_summary_by_job_id.return_value = summary
    repo.get_execution_by_job_id.return_value = row
    service = ExecutionService(session=AsyncMock())
    with patch(
        "src.repositories.execution_history_repository.ExecutionHistoryRepository",
        return_value=repo,
    ):
        result = await service.get_execution_status("run-1", group_ids=["workspace-1"])
    response = ExecutionResponse(**result)
    assert getattr(response, f"{kind}_id") == str(definition_id)
    assert response.inputs == config
    repo.get_execution_by_job_id.assert_awaited_once_with(
        "run-1", group_ids=["workspace-1"]
    )
