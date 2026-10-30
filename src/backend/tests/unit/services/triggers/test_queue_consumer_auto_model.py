"""A queue-triggered run that asks for "auto" gets one concrete model."""

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.decisions.model_selection import ModelSelection
from src.services.execution.config import auto_model
from src.services.triggers.queue_consumer_service import TriggerQueueConsumerService

MODULE = "src.services.triggers.queue_consumer_service"
PICK = ModelSelection("databricks-claude-opus-5-5", "selected", 2.0)


@pytest.mark.asyncio
async def test_auto_is_resolved_before_the_run_is_recorded_or_started():
    session = MagicMock(commit=AsyncMock())

    @asynccontextmanager
    async def routed():
        yield session

    snap = {
        "id": 3,
        "group_id": "g1",
        "target": {
            "kind": "inline",
            "config": {
                "agents_yaml": {"a": {"role": "R", "llm": "auto"}},
                "tasks_yaml": {"t": {"description": "D"}},
                "model": "auto",
            },
        },
        "payload": {"inputs": {"manager_llm": "auto"}},
        "attempts": 1,
    }
    repo = MagicMock(mark_dispatched=AsyncMock())
    with (
        patch(f"{MODULE}.routed_scoped_session", routed),
        patch(f"{MODULE}.TriggerQueueRepository", return_value=repo),
        patch.object(
            auto_model, "select_for_workspace", new=AsyncMock(return_value=PICK)
        ) as select,
        patch.object(auto_model, "_write_trace", new=AsyncMock()) as write,
        patch(
            "src.services.execution.service.ExecutionService.create_run_record",
            new_callable=AsyncMock,
        ) as create_record,
        patch(
            "src.services.execution.service.ExecutionService.run_crew_execution",
            new_callable=AsyncMock,
        ) as run,
        patch(
            "src.services.execution.status.ExecutionStatusService."
            "broadcast_execution_created",
            new_callable=AsyncMock,
        ),
    ):
        await TriggerQueueConsumerService()._dispatch(snap)
        await asyncio.gather(*auto_model._pending)

    select.assert_awaited_once()
    stored = create_record.await_args.kwargs["inputs"]
    assert stored["model"] == PICK.model
    handed = run.await_args.kwargs["config"]
    # (The payload is also echoed into the task text as context; that is prose.)
    assert handed.model == handed.agents_yaml["a"]["llm"] == PICK.model
    assert handed.inputs["manager_llm"] == PICK.model
    job_id = create_record.await_args.kwargs["job_id"]
    assert write.await_args.args[1] == job_id
