"""The MCP/A2A ``ask`` tool resolves "auto" before the chat run starts."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.decisions.model_selection import ModelSelection
from src.services.execution.config import auto_model
from src.services.external import invocation

PICK = ModelSelection("databricks-gpt-5-5", "fallback", 1.0, "timeout")


@pytest.mark.asyncio
async def test_ask_with_auto_hands_the_run_a_concrete_model():
    caller = SimpleNamespace(
        group_context=MagicMock(primary_group_id="ws"),
        protocol="mcp",
        origin="mcp:test",
        group_ids=["ws"],
    )
    with (
        patch.object(
            auto_model, "select_for_workspace", new=AsyncMock(return_value=PICK)
        ),
        patch.object(auto_model, "_write_trace", new=AsyncMock()) as write,
        patch(
            "src.services.execution.status.ExecutionStatusService.create_execution",
            new_callable=AsyncMock,
        ),
        patch(
            "src.services.execution.service.ExecutionService.run_crew_execution",
            new_callable=AsyncMock,
            return_value={"execution_id": "x", "status": "completed"},
        ) as run,
    ):
        result = await invocation.ask(caller, "what is up?", "auto", MagicMock())
        await asyncio.gather(*auto_model._pending)
    assert run.await_args.kwargs["config"].model == PICK.model
    assert write.await_args.args[1] == result.run_id
