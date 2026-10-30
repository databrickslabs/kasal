"""The crew subprocess never sees "auto".

``create_execution`` resolves Auto before it hands the config to the runner,
which ships it to a spawned interpreter. This follows that config across a real
process boundary (services/execution/CLAUDE.md: in-process tests can pass while
the spawned interpreter cannot import what it needs) and checks, in the child:

* the crew subprocess entry module imports;
* no model field in the config it received asks for Auto;
* the child's own safety net still turns a stray "auto" into the default.
"""

import json
import pathlib
import subprocess
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.schemas.execution import CrewConfig
from src.services.decisions.model_selection import ModelSelection
from src.services.execution.config import auto_model
from src.services.execution.service import ExecutionService

BACKEND = pathlib.Path(__file__).resolve().parents[5]
PICK = ModelSelection("databricks-claude-opus-5-5", "selected", 3.0)

CHILD = """
import asyncio, json, sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import src.services.agent_builder.process_executor  # the crew subprocess entry
from src.services.decisions import model_selection
from src.services.execution.config.auto_model import _slots

config = json.load(sys.stdin)
roots = [config[k] for k in ("agents_yaml", "tasks_yaml", "inputs", "nodes")]
leaks = [field for _, field in _slots(roots)]
assert not model_selection.is_auto(config["model"]), config["model"]
assert not leaks, leaks

enabled = AsyncMock(return_value=[SimpleNamespace(key="child-default")])
with patch.object(model_selection, "_enabled_models", new=enabled):
    key = asyncio.run(model_selection.resolve_leaked_auto(None, "auto", "ws"))
assert key == "child-default", key
print("child-ok")
"""


@pytest.mark.asyncio
async def test_the_config_the_crew_subprocess_receives_holds_no_auto():
    config = CrewConfig(
        agents_yaml={"a": {"role": "r", "llm": "auto"}},
        tasks_yaml={"t": {"description": "d", "llm_guardrail": {"llm_model": "auto"}}},
        inputs={"manager_llm": "auto"},
        model="auto",
        execution_type="crew",
    )
    with (
        patch.object(
            auto_model, "select_for_workspace", new=AsyncMock(return_value=PICK)
        ),
        patch.object(auto_model, "_write_trace", new=AsyncMock()),
        patch(
            "src.services.execution.status.ExecutionStatusService.create_execution",
            new_callable=AsyncMock,
            return_value=True,
        ),
        patch.object(
            ExecutionService, "_run_in_background", new_callable=AsyncMock
        ) as runner,
        patch("src.services.execution.service.ExecutionNameService"),
        patch.object(ExecutionService, "_generate_run_name_async", AsyncMock()),
    ):
        await ExecutionService(session=MagicMock()).create_execution(
            config, group_context=MagicMock(primary_group_id="ws")
        )
    handed = runner.call_args.kwargs["config"].model_dump(mode="json")

    child = subprocess.run(
        [sys.executable, "-c", CHILD],
        input=json.dumps(handed),
        capture_output=True,
        text=True,
        cwd=BACKEND,
        timeout=240,
    )
    assert child.returncode == 0, child.stderr[-3000:]
    assert "child-ok" in child.stdout
