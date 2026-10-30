"""Building a crew task logs tool and callback config key names, never values.

The crew logger writes crew.log, which is copied into the run's execution logs.
"""

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.core.logger import LoggerManager
from src.services.agent_builder import task_adapter

SECRET = "sp-secret-value-123"
PASSWORD = "svc-password-456"
PAT = "dapi-pat-value-789"
TOOL = "Measure Conversion Pipeline"
OVERRIDE = {
    "powerbi_client_secret": SECRET,
    "powerbi_password": PASSWORD,
    "powerbi_access_token": PAT,
    "workspace_id": "w",
}


class _Capture(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.lines = []

    def emit(self, record):
        self.lines.append(record.getMessage())


@pytest.fixture
def crew_log():
    logger = LoggerManager.get_instance().crew
    handler, level = _Capture(), logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    yield handler.lines
    logger.removeHandler(handler)
    logger.setLevel(level)


class _LeakyTool:
    """A tool whose repr shows its configuration, as a pydantic model's does."""

    name = TOOL
    description = "converts measures"

    def __repr__(self):
        return f"_LeakyTool(config={OVERRIDE})"


async def _build(task_config, created, harness_error=None):
    factory = MagicMock()
    factory.create_tool.return_value = created
    service = SimpleNamespace(
        get_tool_config_by_name=AsyncMock(return_value={"client_secret": SECRET})
    )
    harness = MagicMock()
    if harness_error:
        harness.build_task.side_effect = harness_error
    with (
        patch.object(
            task_adapter, "resolve_tool_ids_to_names", AsyncMock(return_value=[TOOL])
        ),
        patch.object(
            task_adapter,
            "build_task_args",
            AsyncMock(return_value={"description": "d", "tools": [created]}),
        ),
        patch.object(task_adapter, "active_harness", return_value=harness),
        patch(
            "src.services.databricks.workspace.config_provider."
            "DatabricksConfigProvider.get",
            AsyncMock(return_value=None),
        ),
    ):
        try:
            await task_adapter.create_task(
                "extract",
                task_config,
                agent=SimpleNamespace(role="analyst"),
                config={"group_id": "ws"},
                tool_service=service,
                tool_factory=factory,
            )
        except RuntimeError:
            pass
    return factory


def _leaks(lines):
    return [line for line in lines if any(s in line for s in (SECRET, PASSWORD, PAT))]


@pytest.mark.parametrize("created", [_LeakyTool(), None])
async def test_a_resolved_tool_logs_no_secret(crew_log, created):
    factory = await _build(
        {"description": "d", "tools": ["73"], "tool_configs": {TOOL: OVERRIDE}},
        created,
    )
    assert factory.create_tool.call_args.kwargs["tool_config_override"] == OVERRIDE
    assert crew_log and not _leaks(crew_log)


async def test_an_auto_resolved_tool_logs_no_secret(crew_log):
    factory = await _build(
        {"description": "d", "tools": [], "tool_configs": {TOOL: OVERRIDE}},
        _LeakyTool(),
    )
    assert factory.create_tool.call_args.kwargs["tool_config_override"] == OVERRIDE
    assert not _leaks(crew_log)


async def test_a_failed_build_logs_no_secret(crew_log):
    await _build(
        {"description": "d", "tools": ["73"], "tool_configs": {TOOL: OVERRIDE}},
        _LeakyTool(),
        harness_error=RuntimeError("bad task"),
    )
    assert any("Task args keys" in line for line in crew_log)
    assert not _leaks(crew_log)
