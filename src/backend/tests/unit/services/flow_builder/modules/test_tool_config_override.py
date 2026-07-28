"""Tests for tool_config_override propagation in flow path.

Covers:
- _resolve_tool_override helper (direct ID match, title-based match, no match, empty)
- TaskConfig._configure_task_tools passes tool_config_override to create_tool
- AgentConfig._create_tools_from_ids passes tool_config_override to create_tool
"""

import os
import sys
import importlib
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


from src.services.flow_builder.modules.task_adapter import (
    _resolve_tool_override,
    TaskConfig,
)
from src.services.flow_builder.modules.agent_adapter import AgentConfig

# This file used to load those two modules through ~100 lines of sys.modules
# surgery: stub out `src.*` and `crewai.*`, exec the source files by path, then
# put everything back. That was written when `crewai` was an absent third-party
# dependency. It has been obsolete since the engine was vendored as
# `kasal_engine`, and it was actively harmful:
#
#   * it stubbed `kasal_engine.*`, which is REAL here, so
#     `from kasal_engine.core import Task` only resolved when some earlier test
#     had already imported it — this file errored when run alone;
#   * it hand-built filesystem paths ("engines/kasal/paths/flow/modules/..."),
#     so moving the source broke the loader;
#   * and when the loader raised, the restore step never ran, leaving a dummy
#     GroupContext bolted onto the real src.utils.user_context for the rest of
#     the xdist worker's life. That is what "GroupContext() takes no arguments"
#     was, 34 failures away in an unrelated suite.
#
# A plain import does the job.


# ---------------------------------------------------------------------------
# _resolve_tool_override helper tests
# ---------------------------------------------------------------------------
class TestResolveToolOverride:
    """Tests for the _resolve_tool_override module-level helper."""

    def _make_factory(self, tool_info=None):
        factory = MagicMock()
        factory.get_tool_info.return_value = tool_info
        return factory

    def test_empty_tool_configs_returns_none(self):
        factory = self._make_factory()
        assert _resolve_tool_override(factory, "35", {}) is None
        assert _resolve_tool_override(factory, "35", None) is None

    def test_direct_id_match(self):
        factory = self._make_factory()
        configs = {"35": {"spaceId": "abc123"}}
        result = _resolve_tool_override(factory, "35", configs)
        assert result == {"spaceId": "abc123"}
        # Should NOT call get_tool_info when direct match found
        factory.get_tool_info.assert_not_called()

    def test_direct_id_match_with_int(self):
        factory = self._make_factory()
        configs = {"35": {"spaceId": "abc123"}}
        result = _resolve_tool_override(factory, 35, configs)
        assert result == {"spaceId": "abc123"}

    def test_title_based_match(self):
        tool_info = MagicMock()
        tool_info.title = "GenieTool"
        factory = self._make_factory(tool_info)
        configs = {"GenieTool": {"spaceId": "space-xyz"}}
        result = _resolve_tool_override(factory, "35", configs)
        assert result == {"spaceId": "space-xyz"}
        factory.get_tool_info.assert_called_once_with("35")

    def test_no_match_returns_none(self):
        tool_info = MagicMock()
        tool_info.title = "SomeOtherTool"
        factory = self._make_factory(tool_info)
        configs = {"GenieTool": {"spaceId": "space-xyz"}}
        result = _resolve_tool_override(factory, "35", configs)
        assert result is None

    def test_tool_info_none_returns_none(self):
        factory = self._make_factory(None)
        configs = {"GenieTool": {"spaceId": "space-xyz"}}
        result = _resolve_tool_override(factory, "99", configs)
        assert result is None

    def test_tool_info_no_title_returns_none(self):
        tool_info = MagicMock(spec=[])  # no attributes at all
        factory = self._make_factory(tool_info)
        configs = {"GenieTool": {"spaceId": "space-xyz"}}
        result = _resolve_tool_override(factory, "35", configs)
        assert result is None


# ---------------------------------------------------------------------------
# TaskConfig._configure_task_tools tests
# ---------------------------------------------------------------------------
class TestTaskConfigToolOverride:
    """Test that _configure_task_tools passes tool_config_override to create_tool.

    _configure_task_tools creates a ToolFactory internally via a local import.
    The ``async with request_scoped_session()`` will fail in stubs, so the code
    falls back to ``ToolFactory(factory_config)`` → ``tool_factory.initialize()``.
    We make that fallback return our controlled factory mock by having
    ``ToolFactory(...)`` return the mock and its ``.initialize()`` succeed.
    """

    def _setup_factory_mock(self, tool_factory):
        """Patch sys.modules stubs so _configure_task_tools uses our tool_factory.

        Returns (tf_mod, db_mod, orig_tf, orig_rss) for cleanup.
        """
        tf_mod = sys.modules["src.services.tools.tool_factory"]
        db_mod = sys.modules["src.db.session"]
        orig_tf = getattr(tf_mod, "ToolFactory", None)
        orig_rss = getattr(db_mod, "request_scoped_session", None)

        # ToolFactory(factory_config) should return our tool_factory instance.
        # The .create() async classmethod path will fail (session mock isn't perfect),
        # so the except branch calls ToolFactory(factory_config) – a plain call.
        mock_tf_class = MagicMock(return_value=tool_factory)
        # Also set .create in case the happy path works in some environments
        mock_tf_class.create = AsyncMock(return_value=tool_factory)
        # tool_factory.initialize() must be async
        tool_factory.initialize = AsyncMock()

        tf_mod.ToolFactory = mock_tf_class
        # Ensure the async-with request_scoped_session() fails so we hit fallback
        db_mod.request_scoped_session = MagicMock(side_effect=Exception("stub"))

        return tf_mod, db_mod, orig_tf, orig_rss

    def _restore(self, tf_mod, db_mod, orig_tf, orig_rss):
        tf_mod.ToolFactory = orig_tf
        db_mod.request_scoped_session = orig_rss

    @pytest.mark.asyncio
    async def test_task_tools_pass_override_via_tool_configs(self):
        """When task_data has tools and tool_configs, create_tool gets the override."""
        fake_tool = MagicMock(name="fake_genie_tool")

        tool_factory = MagicMock()
        tool_factory.create_tool.return_value = fake_tool

        tool_info = MagicMock()
        tool_info.title = "GenieTool"
        tool_factory.get_tool_info.return_value = tool_info

        task_data = MagicMock()
        task_data.name = "Test Task"
        task_data.tools = ["35"]
        task_data.tool_configs = {"GenieTool": {"spaceId": "space-123"}}
        task_data.id = "1"

        agent = MagicMock()
        agent.tools = []

        refs = self._setup_factory_mock(tool_factory)
        try:
            await TaskConfig._configure_task_tools(task_data, agent, flow_data=None, group_context=None)
        finally:
            self._restore(*refs)

        tool_factory.create_tool.assert_called_once_with(
            "35", tool_config_override={"spaceId": "space-123"}
        )
        assert agent.tools == [fake_tool]

    @pytest.mark.asyncio
    async def test_task_tools_no_tool_configs(self):
        """When task_data has no tool_configs, create_tool gets override=None."""
        fake_tool = MagicMock(name="fake_tool")

        tool_factory = MagicMock()
        tool_factory.create_tool.return_value = fake_tool
        tool_factory.get_tool_info.return_value = None

        task_data = MagicMock()
        task_data.name = "Test Task"
        task_data.tools = ["10"]
        task_data.tool_configs = None
        task_data.id = "1"

        agent = MagicMock()
        agent.tools = []

        refs = self._setup_factory_mock(tool_factory)
        try:
            await TaskConfig._configure_task_tools(task_data, agent, flow_data=None, group_context=None)
        finally:
            self._restore(*refs)

        tool_factory.create_tool.assert_called_once_with(
            "10", tool_config_override=None
        )

    @pytest.mark.asyncio
    async def test_node_tools_pass_override(self):
        """When tools come from flow node data, they also get overrides."""
        fake_tool = MagicMock(name="fake_tool")

        tool_factory = MagicMock()
        tool_factory.create_tool.return_value = fake_tool

        tool_info = MagicMock()
        tool_info.title = "GenieTool"
        tool_factory.get_tool_info.return_value = tool_info

        task_data = MagicMock()
        task_data.name = "Node Task"
        task_data.tools = None  # no direct tools — triggers node lookup
        task_data.tool_configs = {"GenieTool": {"spaceId": "node-space"}}
        task_data.id = "42"

        agent = MagicMock()
        agent.tools = []

        flow_data = MagicMock()
        flow_data.nodes = [
            {
                "id": "task-42",
                "data": {"tools": ["35"]}
            }
        ]

        refs = self._setup_factory_mock(tool_factory)
        try:
            await TaskConfig._configure_task_tools(task_data, agent, flow_data=flow_data, group_context=None)
        finally:
            self._restore(*refs)

        tool_factory.create_tool.assert_called_once_with(
            "35", tool_config_override={"spaceId": "node-space"}
        )
