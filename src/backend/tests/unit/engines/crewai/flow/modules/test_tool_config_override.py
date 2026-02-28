"""Tests for tool_config_override propagation in flow path.

Covers:
- _resolve_tool_override helper (direct ID match, title-based match, no match, empty)
- TaskConfig._configure_task_tools passes tool_config_override to create_tool
- AgentConfig._create_tools_from_ids passes tool_config_override to create_tool
"""

import os
import sys
import types
import importlib
import importlib.util
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# ---------------------------------------------------------------------------
# Load our target modules directly from file, bypassing the __init__.py chain.
# ---------------------------------------------------------------------------
_BACKEND_SRC = os.path.join(
    os.path.dirname(__file__), os.pardir, os.pardir, os.pardir,
    os.pardir, os.pardir, os.pardir, "src"
)
_BACKEND_SRC = os.path.normpath(_BACKEND_SRC)


def _stub_module(name):
    """Create a stub in sys.modules so imports from within the loaded module work."""
    if name not in sys.modules:
        mod = types.ModuleType(name)
        mod.__path__ = []
        sys.modules[name] = mod
    return sys.modules[name]


def _load_from_file(module_name, filepath):
    """Load a Python file as module_name and register it in sys.modules."""
    spec = importlib.util.spec_from_file_location(module_name, filepath)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


# Ensure stubs for all imports that task_config.py and agent_config.py need
_stubs_needed = [
    "src", "src.core", "src.core.logger",
    "src.utils", "src.utils.user_context",
    "src.engines", "src.engines.crewai",
    "src.engines.crewai.tools", "src.engines.crewai.tools.tool_factory",
    "src.engines.crewai.flow", "src.engines.crewai.flow.modules",
    "src.engines.crewai.guardrails",
    "src.engines.crewai.guardrails.guardrail_factory",
    "src.engines.crewai.guardrails.guardrail_wrapper",
    "src.db", "src.db.session",
    "src.services", "src.services.api_keys_service",
    "src.services.mcp_service",
    "src.models", "src.models.agent",
    "src.core.llm_manager",
    "src.engines.crewai.tools.mcp_integration",
    "crewai", "crewai.flow", "crewai.flow.flow",
    "crewai.tasks", "crewai.tasks.llm_guardrail",
]
for _s in _stubs_needed:
    _stub_module(_s)

# Wire up expected attributes
_logger_mgr = MagicMock()
_logger_mgr.get_instance.return_value = MagicMock(flow=MagicMock())
sys.modules["src.core.logger"].LoggerManager = _logger_mgr
sys.modules["src.utils.user_context"].GroupContext = type("GroupContext", (), {})
sys.modules["crewai"].Task = MagicMock
sys.modules["crewai"].Agent = MagicMock
sys.modules["crewai"].LLM = MagicMock
sys.modules["src.engines.crewai.tools.tool_factory"].ToolFactory = MagicMock
sys.modules["src.db.session"].request_scoped_session = MagicMock
sys.modules["src.services.api_keys_service"].ApiKeysService = MagicMock

# Load the actual source files
_task_config_mod = _load_from_file(
    "src.engines.crewai.flow.modules.task_config",
    os.path.join(_BACKEND_SRC, "engines", "crewai", "flow", "modules", "task_config.py"),
)
_agent_config_mod = _load_from_file(
    "src.engines.crewai.flow.modules.agent_config",
    os.path.join(_BACKEND_SRC, "engines", "crewai", "flow", "modules", "agent_config.py"),
)

_resolve_tool_override = _task_config_mod._resolve_tool_override
TaskConfig = _task_config_mod.TaskConfig
AgentConfig = _agent_config_mod.AgentConfig


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
        tf_mod = sys.modules["src.engines.crewai.tools.tool_factory"]
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


# ---------------------------------------------------------------------------
# AgentConfig._create_tools_from_ids tests
# ---------------------------------------------------------------------------
class TestAgentConfigToolOverride:
    """Test that _create_tools_from_ids passes tool_config_override to create_tool."""

    @pytest.mark.asyncio
    async def test_create_tools_from_ids_with_tool_configs(self):
        """When tool_configs is provided, create_tool gets the matching override."""
        fake_tool = MagicMock(name="fake_genie_tool")

        tool_factory = MagicMock()
        tool_factory.create_tool.return_value = fake_tool

        tool_info = MagicMock()
        tool_info.title = "GenieTool"
        tool_factory.get_tool_info.return_value = tool_info

        tools = await AgentConfig._create_tools_from_ids(
            ["35"], tool_factory, "agent TestAgent",
            tool_configs={"GenieTool": {"spaceId": "agent-space"}}
        )

        assert tools == [fake_tool]
        tool_factory.create_tool.assert_called_once_with(
            "35", tool_config_override={"spaceId": "agent-space"}
        )

    @pytest.mark.asyncio
    async def test_create_tools_from_ids_without_tool_configs(self):
        """When tool_configs is None, create_tool gets override=None."""
        fake_tool = MagicMock(name="fake_tool")

        tool_factory = MagicMock()
        tool_factory.create_tool.return_value = fake_tool
        tool_factory.get_tool_info.return_value = None

        tools = await AgentConfig._create_tools_from_ids(
            ["10"], tool_factory, "agent TestAgent",
            tool_configs=None
        )

        assert tools == [fake_tool]
        tool_factory.create_tool.assert_called_once_with(
            "10", tool_config_override=None
        )

    @pytest.mark.asyncio
    async def test_create_tools_from_ids_multiple_tools(self):
        """Multiple tools get their own individual overrides."""
        genie_tool = MagicMock(name="genie")
        other_tool = MagicMock(name="other")

        tool_factory = MagicMock()
        tool_factory.create_tool.side_effect = [genie_tool, other_tool]

        genie_info = MagicMock()
        genie_info.title = "GenieTool"
        other_info = MagicMock()
        other_info.title = "WebSearch"

        tool_factory.get_tool_info.side_effect = [genie_info, other_info]

        tool_configs = {
            "GenieTool": {"spaceId": "my-space"},
            # WebSearch has no config override
        }

        tools = await AgentConfig._create_tools_from_ids(
            ["35", "12"], tool_factory, "agent Multi",
            tool_configs=tool_configs
        )

        assert tools == [genie_tool, other_tool]
        calls = tool_factory.create_tool.call_args_list
        assert calls[0].args == ("35",)
        assert calls[0].kwargs == {"tool_config_override": {"spaceId": "my-space"}}
        assert calls[1].args == ("12",)
        assert calls[1].kwargs == {"tool_config_override": None}
