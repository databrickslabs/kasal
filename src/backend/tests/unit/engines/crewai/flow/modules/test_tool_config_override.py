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
#
# IMPORTANT: We install temporary stubs in sys.modules so the source files
# can resolve their imports, then IMMEDIATELY clean up all stubs so we don't
# pollute the import cache for other test modules.  The extracted Python
# objects (_resolve_tool_override, TaskConfig, AgentConfig) survive because
# they're held by reference — they don't need the stubs to stay in sys.modules.
# ---------------------------------------------------------------------------
_BACKEND_SRC = os.path.join(
    os.path.dirname(__file__), os.pardir, os.pardir, os.pardir,
    os.pardir, os.pardir, os.pardir, "src"
)
_BACKEND_SRC = os.path.normpath(_BACKEND_SRC)


def _load_modules_isolated():
    """Load task_config and agent_config in an isolated sys.modules context.

    Returns (_resolve_tool_override, TaskConfig, AgentConfig).
    """
    stubs_needed = [
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
    loaded_modules = [
        "src.engines.crewai.flow.modules.task_config",
        "src.engines.crewai.flow.modules.agent_config",
    ]

    # 1. Snapshot existing entries
    saved = {k: sys.modules[k] for k in stubs_needed + loaded_modules if k in sys.modules}

    # 2. Install stubs (only for modules not already loaded)
    added = []
    for name in stubs_needed:
        if name not in sys.modules:
            mod = types.ModuleType(name)
            mod.__path__ = []
            sys.modules[name] = mod
            added.append(name)

    # 3. Wire up expected attributes on stubs.
    #    IMPORTANT: For modules that already existed in sys.modules (i.e. the
    #    real module was already imported by another test file), we save the
    #    original attribute value so we can restore it later — otherwise we'd
    #    permanently mutate the real module object.
    _attr_backups = []  # list of (module_obj, attr_name, old_value_or_sentinel)
    _SENTINEL = object()

    def _set_attr(module_key, attr_name, value):
        mod = sys.modules[module_key]
        old = getattr(mod, attr_name, _SENTINEL)
        _attr_backups.append((mod, attr_name, old))
        setattr(mod, attr_name, value)

    logger_mgr = MagicMock()
    logger_mgr.get_instance.return_value = MagicMock(flow=MagicMock())
    _set_attr("src.core.logger", "LoggerManager", logger_mgr)
    _set_attr("src.utils.user_context", "GroupContext", type("GroupContext", (), {}))
    _set_attr("crewai", "Task", MagicMock)
    _set_attr("crewai", "Agent", MagicMock)
    _set_attr("crewai", "LLM", MagicMock)
    _set_attr("src.engines.crewai.tools.tool_factory", "ToolFactory", MagicMock)
    _set_attr("src.db.session", "request_scoped_session", MagicMock)
    _set_attr("src.services.api_keys_service", "ApiKeysService", MagicMock)

    # 4. Load actual source files
    def _load(module_name, filepath):
        spec = importlib.util.spec_from_file_location(module_name, filepath)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = mod
        spec.loader.exec_module(mod)
        return mod

    task_mod = _load(
        "src.engines.crewai.flow.modules.task_config",
        os.path.join(_BACKEND_SRC, "engines", "crewai", "flow", "modules", "task_config.py"),
    )
    agent_mod = _load(
        "src.engines.crewai.flow.modules.agent_config",
        os.path.join(_BACKEND_SRC, "engines", "crewai", "flow", "modules", "agent_config.py"),
    )

    # 5. Extract the symbols we need
    resolve_fn = task_mod._resolve_tool_override
    task_cls = task_mod.TaskConfig
    agent_cls = agent_mod.AgentConfig

    # 6. Restore mutated attributes on pre-existing (real) modules FIRST,
    #    before touching sys.modules entries.
    for mod_obj, attr_name, old_val in reversed(_attr_backups):
        if old_val is _SENTINEL:
            # Attribute didn't exist before — remove it
            try:
                delattr(mod_obj, attr_name)
            except AttributeError:
                pass
        else:
            setattr(mod_obj, attr_name, old_val)

    # 7. Restore sys.modules — remove every stub we added
    for name in added + loaded_modules:
        if name in saved:
            sys.modules[name] = saved[name]
        else:
            sys.modules.pop(name, None)

    # Also restore any pre-existing modules we may have overwritten
    for name, orig in saved.items():
        sys.modules[name] = orig

    return resolve_fn, task_cls, agent_cls


_resolve_tool_override, TaskConfig, AgentConfig = _load_modules_isolated()


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
