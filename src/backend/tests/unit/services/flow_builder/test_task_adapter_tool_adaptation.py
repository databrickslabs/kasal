"""Regression: flow task-level tools must be ADAPTED for the active harness.

``TaskConfig._configure_task_tools`` resolves a task's tools straight from
``ToolFactory.create_tool`` (raw Kasal ``BaseTool`` instances) and assigns them
to ``agent.tools``. On the CrewAI harness an unadapted tool fails crewai's
``parse_tools`` with "Tool is not a CrewStructuredTool or BaseTool" at crew
kickoff — which broke every crewAI flow whose task carried its own tools (the
starting crew, with agent-level tools built through ``build_agent``, worked; a
``@listen`` crew whose task defined tools did not). The fix routes the
assignment through ``active_harness().adapt_tools`` (identity on Kasal).
"""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.execution.harnesses import active_harness
from src.services.execution.harnesses.selection import bind
from src.services.tools.base import BaseTool as KasalBaseTool


class _RawKasalTool(KasalBaseTool):
    name: str = "raw_kasal_tool"
    description: str = "a raw kasal tool as produced by ToolFactory.create_tool"

    def _run(self, *args, **kwargs):
        return "ok"


def _crewai_base_tool():
    from crewai.tools.base_tool import BaseTool as CrewBaseTool

    return CrewBaseTool


class TestHarnessAdaptToolsContract:
    """The invariant the fix relies on."""

    def test_crewai_adapts_raw_kasal_tool_so_parse_tools_accepts_it(self):
        from crewai.utilities.agent_utils import parse_tools

        raw = _RawKasalTool()
        # A raw Kasal tool is NOT a crewai BaseTool — parse_tools rejects it.
        assert not isinstance(raw, _crewai_base_tool())
        with pytest.raises(ValueError, match="CrewStructuredTool or BaseTool"):
            parse_tools([raw])

        with bind("crewai"):
            adapted = active_harness().adapt_tools([raw])
        assert adapted and all(isinstance(t, _crewai_base_tool()) for t in adapted)
        # Adapted tools sail through crewai's own validation.
        parse_tools(adapted)

    def test_kasal_adapt_tools_is_identity(self):
        raw = _RawKasalTool()
        with bind("kasal"):
            out = active_harness().adapt_tools([raw])
        assert out == [raw]


class TestConfigureTaskToolsAdaptsForHarness:
    """The fix, exercised through the real method."""

    async def _run_configure(self, harness: str):
        from src.services.flow_builder.modules.task_adapter import TaskConfig

        raw = _RawKasalTool()
        fake_factory = MagicMock()
        fake_factory.create_tool = MagicMock(return_value=raw)

        agent = SimpleNamespace(role="Specialist", tools=[])
        task_data = SimpleNamespace(
            name="Gather Solution Providers", id="t1", tools=["tool-1"]
        )

        @asynccontextmanager
        async def _fake_session():
            yield MagicMock()

        with (
            patch(
                "src.db.session.routed_scoped_session",
                lambda: _fake_session(),
            ),
            patch(
                "src.services.settings.api_keys.ApiKeysService",
                MagicMock(),
            ),
            patch(
                "src.services.tools.tool_factory.ToolFactory.create",
                new=AsyncMock(return_value=fake_factory),
            ),
            patch(
                "src.services.flow_builder.modules.task_adapter._resolve_tool_override",
                return_value=None,
            ),
            bind(harness),
        ):
            await TaskConfig._configure_task_tools(task_data, agent, flow_data=None)
        return agent

    @pytest.mark.asyncio
    async def test_task_tools_are_crewai_tools_on_crewai_harness(self):
        from crewai.utilities.agent_utils import parse_tools

        agent = await self._run_configure("crewai")
        assert agent.tools, "task tools should have been assigned to the agent"
        assert all(isinstance(t, _crewai_base_tool()) for t in agent.tools)
        # The whole point: crew kickoff (parse_tools) no longer raises.
        parse_tools(agent.tools)

    @pytest.mark.asyncio
    async def test_task_tools_pass_through_on_kasal_harness(self):
        agent = await self._run_configure("kasal")
        assert [type(t).__name__ for t in agent.tools] == ["_RawKasalTool"]
