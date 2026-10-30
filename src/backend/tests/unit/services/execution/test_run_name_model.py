"""The model a run is named with: never the "default-model" placeholder.

A published crew has no top-level model (each agent carries its own ``llm``),
and naming it with "default-model" raised "Model configuration not found".
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.services.execution.naming import run_name_model

_DEFAULT = "src.services.decisions.model_selection.workspace_default"


def _config(model=None, agents=None):
    return SimpleNamespace(model=model, agents_yaml=agents)


@pytest.mark.asyncio
async def test_the_run_model_wins():
    config = _config("run-model", {"a": {"llm": "agent-model"}})
    with patch(_DEFAULT, new=AsyncMock(return_value="ws")) as ws:
        assert await run_name_model(config, None, "g") == "run-model"
    ws.assert_not_awaited()


@pytest.mark.asyncio
async def test_else_the_first_agent_llm():
    config = _config(agents={"a": {"role": "r"}, "b": {"llm": "agent-model"}})
    with patch(_DEFAULT, new=AsyncMock(return_value="ws")) as ws:
        assert await run_name_model(config, None, "g") == "agent-model"
    ws.assert_not_awaited()


@pytest.mark.asyncio
async def test_else_the_workspace_default():
    with patch(_DEFAULT, new=AsyncMock(return_value="ws-model")) as ws:
        assert await run_name_model(_config(agents={"a": {}}), "s", "g") == "ws-model"
    ws.assert_awaited_once_with("s", "g")


@pytest.mark.asyncio
@pytest.mark.parametrize("session,group_id", [("s", None), (None, "g"), ("s", "g")])
async def test_else_none_never_a_placeholder(session, group_id):
    with patch(_DEFAULT, new=AsyncMock(side_effect=RuntimeError("no db"))):
        assert await run_name_model(_config(), session, group_id) is None
