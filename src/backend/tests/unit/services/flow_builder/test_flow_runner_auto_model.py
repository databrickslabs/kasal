"""``/flow-execution`` resolves "auto" before the flow's record and subprocess."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.services.decisions.model_selection import ModelSelection
from src.services.execution.config import auto_model
from src.services.flow_builder.flow_runner_service import FlowRunnerService

PICK = ModelSelection("databricks-claude-sonnet-4-5", "selected", 1.0)


@pytest.mark.asyncio
async def test_a_dynamic_flow_with_auto_runs_on_one_concrete_model():
    db = MagicMock(spec=AsyncSession)
    with (
        patch("src.services.flow_builder.flow_runner_service.FlowExecutionService"),
        patch("src.services.flow_builder.flow_runner_service.FlowRepository"),
    ):
        service = FlowRunnerService(db)
    execution = MagicMock(id=1)
    service.flow_execution_service.create_execution = AsyncMock(return_value=execution)
    service.flow_execution_service.update_execution_status = AsyncMock()
    service._run_dynamic_flow = AsyncMock(return_value={"success": True})
    context = MagicMock(primary_group_id="g")
    config = {
        "nodes": [{"id": "n1", "data": {"llm": "auto"}}],
        "edges": [],
        "flow_config": {"crews": [{"agents": [{"llm": {"model": "auto"}}]}]},
        "model": "auto",
        "group_id": "g",
        "group_context": context,
    }
    with (
        patch.object(
            auto_model, "select_for_workspace", new=AsyncMock(return_value=PICK)
        ) as select,
        patch.object(auto_model, "_write_trace", new=AsyncMock()) as write,
    ):
        await service.run_flow(flow_id=None, job_id="job-1", config=config)
        await asyncio.gather(*auto_model._pending)

    assert select.await_args.args[0] is db
    stored = service.flow_execution_service.create_execution.await_args.kwargs
    assert stored["config"]["model"] == PICK.model
    assert stored["config"]["nodes"][0]["data"]["llm"] == PICK.model
    handed = service._run_dynamic_flow.await_args.args[-1]
    assert handed["flow_config"]["crews"][0]["agents"][0]["llm"]["model"] == (
        PICK.model
    )
    assert write.await_args.args[1] == "job-1"
