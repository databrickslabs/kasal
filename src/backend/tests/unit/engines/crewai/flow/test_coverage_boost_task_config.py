"""
Coverage-boosting tests for task_config.py.

Targets uncovered lines:
 69-213, 229-313, 333-334, 343-346, 355-356, 365-370, 390-392, 399-401,
 410, 440-442, 449-451, 453-456
"""

import pytest
import uuid
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, AsyncMock, patch, Mock

from src.engines.crewai.flow.modules.task_adapter import TaskConfig, _resolve_tool_override


# ---------------------------------------------------------------------------
# _resolve_tool_override (module-level helper)
# ---------------------------------------------------------------------------

class TestResolveToolOverride:

    def test_no_tool_configs_returns_none(self):
        tf = MagicMock()
        assert _resolve_tool_override(tf, "1", None) is None
        assert _resolve_tool_override(tf, "1", {}) is None

    def test_direct_lookup_by_tool_id(self):
        tf = MagicMock()
        configs = {"1": {"key": "value"}}
        result = _resolve_tool_override(tf, "1", configs)
        assert result == {"key": "value"}

    def test_lookup_by_tool_title(self):
        tf = MagicMock()
        tool_info = MagicMock()
        tool_info.title = "MyTool"
        tf.get_tool_info.return_value = tool_info
        configs = {"MyTool": {"override": True}}
        result = _resolve_tool_override(tf, "42", configs)
        assert result == {"override": True}

    def test_lookup_by_title_not_found_in_configs(self):
        tf = MagicMock()
        tool_info = MagicMock()
        tool_info.title = "UnknownTool"
        tf.get_tool_info.return_value = tool_info
        configs = {"OtherTool": {"x": 1}}
        result = _resolve_tool_override(tf, "42", configs)
        assert result is None

    def test_tool_info_none(self):
        tf = MagicMock()
        tf.get_tool_info.return_value = None
        result = _resolve_tool_override(tf, "99", {"SomeTool": {}})
        assert result is None

    def test_tool_info_no_title(self):
        tf = MagicMock()
        tool_info = MagicMock(spec=[])  # no 'title' attribute
        tf.get_tool_info.return_value = tool_info
        result = _resolve_tool_override(tf, "99", {"SomeTool": {}})
        assert result is None


# ---------------------------------------------------------------------------
# TaskConfig.configure_task
# ---------------------------------------------------------------------------

def _make_task_data(**kwargs):
    defaults = dict(
        name="Test Task",
        description="Do something",
        expected_output="Something done",
        agent_id=None,
        tools=[],
        tool_configs={},
        async_execution=False,
        human_input=False,
        guardrail=None,
        config={},
        markdown=False,
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _make_agent():
    """Create a real CrewAI Agent that passes pydantic validation."""
    from crewai import Agent, LLM
    from unittest.mock import patch

    with patch("crewai.agent.Agent._setup_agent_executor"):
        with patch("crewai.utilities.rpm_controller.RPMController", MagicMock()):
            agent = MagicMock()
            agent.role = "Test Agent"
            agent.tools = []
            # Make it pass pydantic isinstance check by making it look like BaseAgent
            from crewai.agents.agent_builder.base_agent import BaseAgent
            agent.__class__ = BaseAgent
            return agent


def _make_real_task_with_mock_agent():
    """Create a mock task and agent that work for TaskConfig tests.

    We can't create real CrewAI Task/Agent objects without API keys,
    so we use mocks with the right attributes and class hierarchy.
    """
    from crewai.agents.agent_builder.base_agent import BaseAgent

    agent = MagicMock(spec=BaseAgent)
    agent.role = "Tester"
    agent.goal = "Test"
    agent.backstory = "A test agent"
    agent.tools = []
    agent.llm = MagicMock()
    agent.memory = False
    # Make it pass isinstance checks
    agent.__class__ = BaseAgent

    task = MagicMock()
    task.description = "Test task description"
    task.expected_output = "Expected output"
    task.agent = agent
    task.guardrail = None
    task.retry_on_fail = False
    task.callback = None
    task.async_execution = False
    task.human_input = False

    return task, agent


def _make_task_with_real_class(description="Test desc", agent=None):
    """Create a MagicMock Task but with real attribute-setting behavior."""
    from crewai.agents.agent_builder.base_agent import BaseAgent

    if agent is None:
        agent = MagicMock(spec=BaseAgent)
        agent.role = "Tester"
        agent.goal = "Test"
        agent.tools = []
        agent.__class__ = BaseAgent

    task = MagicMock()
    task.description = description
    task.expected_output = "output"
    task.agent = agent
    task.guardrail = None
    task.retry_on_fail = False
    task.callback = None
    task.async_execution = False
    task.human_input = False
    return task, agent


class TestConfigureTask:

    @pytest.mark.asyncio
    async def test_configure_task_no_task_data(self):
        result = await TaskConfig.configure_task(None)
        assert result is None

    @pytest.mark.asyncio
    async def test_configure_task_no_agent_returns_none(self):
        task_data = _make_task_data()

        with patch.object(TaskConfig, "_resolve_agent_for_task", new=AsyncMock(return_value=None)), \
             patch.object(TaskConfig, "_configure_task_tools", new=AsyncMock()):
            result = await TaskConfig.configure_task(task_data)
        assert result is None

    @pytest.mark.asyncio
    async def test_configure_task_success_basic(self):
        task_data = _make_task_data()
        task, agent = _make_real_task_with_mock_agent()

        with patch.object(TaskConfig, "_configure_task_tools", new=AsyncMock()), \
             patch("crewai.Task", return_value=task):
            result = await TaskConfig.configure_task(task_data, agent=agent)

        assert result is task

    @pytest.mark.asyncio
    async def test_configure_task_with_task_output_callback(self):
        task_data = _make_task_data()
        task, agent = _make_real_task_with_mock_agent()
        callback = MagicMock()

        with patch.object(TaskConfig, "_configure_task_tools", new=AsyncMock()), \
             patch("crewai.Task", return_value=task):
            result = await TaskConfig.configure_task(task_data, agent=agent, task_output_callback=callback)

        assert task.callback == callback

    @pytest.mark.asyncio
    async def test_configure_task_with_markdown(self):
        task_data = _make_task_data(markdown=True)
        task, agent = _make_real_task_with_mock_agent()

        captured_kwargs = {}

        def mock_task_ctor(**kwargs):
            captured_kwargs.update(kwargs)
            return task

        with patch.object(TaskConfig, "_configure_task_tools", new=AsyncMock()), \
             patch("crewai.Task", side_effect=mock_task_ctor):
            result = await TaskConfig.configure_task(task_data, agent=agent)

        # Unified to the crew path's wording ("markdown syntax").
        assert "markdown" in captured_kwargs.get("description", "").lower()

    @pytest.mark.asyncio
    async def test_configure_task_async_execution_set(self):
        task_data = _make_task_data(async_execution=True)
        task, agent = _make_real_task_with_mock_agent()

        captured_kwargs = {}

        def mock_task_ctor(**kwargs):
            captured_kwargs.update(kwargs)
            return task

        with patch.object(TaskConfig, "_configure_task_tools", new=AsyncMock()), \
             patch("crewai.Task", side_effect=mock_task_ctor):
            await TaskConfig.configure_task(task_data, agent=agent)

        assert captured_kwargs.get("async_execution") is True

    @pytest.mark.asyncio
    async def test_configure_task_human_input_set(self):
        task_data = _make_task_data(human_input=True)
        task, agent = _make_real_task_with_mock_agent()

        captured_kwargs = {}

        def mock_task_ctor(**kwargs):
            captured_kwargs.update(kwargs)
            return task

        with patch.object(TaskConfig, "_configure_task_tools", new=AsyncMock()), \
             patch("crewai.Task", side_effect=mock_task_ctor):
            await TaskConfig.configure_task(task_data, agent=agent)

        assert captured_kwargs.get("human_input") is True

    @pytest.mark.asyncio
    async def test_configure_task_guardrail_dict_config(self):
        """Guardrail config as dict is converted to JSON and factory is called."""
        task_data = _make_task_data(guardrail={"type": "company_count", "max": 5})
        task, agent = _make_real_task_with_mock_agent()

        captured_kwargs = {}

        def mock_task_ctor(**kwargs):
            captured_kwargs.update(kwargs)
            return task

        with patch.object(TaskConfig, "_configure_task_tools", new=AsyncMock()), \
             patch("crewai.Task", side_effect=mock_task_ctor), \
             patch("src.engines.crewai.guardrails.guardrail_factory.GuardrailFactory") as MockGF:
            MockGF.create_guardrail.return_value = MagicMock()

            await TaskConfig.configure_task(task_data, agent=agent)

        MockGF.create_guardrail.assert_called_once()
        assert "guardrail" in captured_kwargs
        assert captured_kwargs.get("retry_on_fail") is True

    @pytest.mark.asyncio
    async def test_configure_task_guardrail_self_reflection_stamps_run_model(self):
        """An LLM-backed code guardrail (self_reflection / prompt_injection_check)
        gets the run's model stamped into its config via resolve_guardrail_model
        (from the agent), so the LLM judge uses the SAME model as the run rather
        than the hardcoded default. Covers task_config.py:136-138."""
        task_data = _make_task_data(guardrail={"type": "self_reflection"})
        task, agent = _make_real_task_with_mock_agent()
        # Agent runs with this model (chat-input selection, top-down).
        agent.llm.model = "databricks/databricks-claude-sonnet-4-5"

        captured = {}

        def _capture(cfg):
            captured["cfg"] = cfg
            return MagicMock()

        with patch.object(TaskConfig, "_configure_task_tools", new=AsyncMock()), \
             patch("crewai.Task", return_value=task), \
             patch("src.engines.crewai.guardrails.guardrail_factory.GuardrailFactory") as MockGF, \
             patch("src.engines.crewai.guardrails.guardrail_wrapper.GuardrailWrapper", return_value=MagicMock()):
            MockGF.create_guardrail.side_effect = _capture

            await TaskConfig.configure_task(task_data, agent=agent)

        # Reached the factory as JSON with the resolved (prefix-stripped) model stamped.
        sent = json.loads(captured["cfg"])
        assert sent["type"] == "self_reflection"
        assert sent["llm_model"] == "databricks-claude-sonnet-4-5"

    @pytest.mark.asyncio
    async def test_configure_task_guardrail_non_llm_type_not_stamped(self):
        """A non-LLM code guardrail (e.g. company_count) must NOT get an llm_model
        stamped — only self_reflection/prompt_injection_check do. Guards the
        branch condition at task_config.py:136."""
        task_data = _make_task_data(guardrail={"type": "company_count", "max": 5})
        task, agent = _make_real_task_with_mock_agent()
        agent.llm.model = "databricks/databricks-claude-sonnet-4-5"

        captured = {}

        def _capture(cfg):
            captured["cfg"] = cfg
            return MagicMock()

        with patch.object(TaskConfig, "_configure_task_tools", new=AsyncMock()), \
             patch("crewai.Task", return_value=task), \
             patch("src.engines.crewai.guardrails.guardrail_factory.GuardrailFactory") as MockGF, \
             patch("src.engines.crewai.guardrails.guardrail_wrapper.GuardrailWrapper", return_value=MagicMock()):
            MockGF.create_guardrail.side_effect = _capture

            await TaskConfig.configure_task(task_data, agent=agent)

        sent = json.loads(captured["cfg"])
        assert "llm_model" not in sent

    @pytest.mark.asyncio
    async def test_configure_task_guardrail_factory_returns_none(self):
        task_data = _make_task_data(guardrail='{"type": "unknown"}')
        task, agent = _make_real_task_with_mock_agent()

        with patch.object(TaskConfig, "_configure_task_tools", new=AsyncMock()), \
             patch("crewai.Task", return_value=task), \
             patch("src.engines.crewai.guardrails.guardrail_factory.GuardrailFactory") as MockGF:
            MockGF.create_guardrail.return_value = None

            result = await TaskConfig.configure_task(task_data, agent=agent)

        assert result is task  # Task still created, just no guardrail applied

    @pytest.mark.asyncio
    async def test_configure_task_guardrail_exception_continues(self):
        task_data = _make_task_data(guardrail="bad_guardrail_string")
        task, agent = _make_real_task_with_mock_agent()

        with patch.object(TaskConfig, "_configure_task_tools", new=AsyncMock()), \
             patch("crewai.Task", return_value=task), \
             patch("src.engines.crewai.guardrails.guardrail_factory.GuardrailFactory", side_effect=ImportError("no module")):
            result = await TaskConfig.configure_task(task_data, agent=agent)

        # Should still return task even if guardrail setup fails
        assert result is task

    @pytest.mark.asyncio
    async def test_configure_task_llm_guardrail_dict_config(self):
        task_data = _make_task_data(
            config={"llm_guardrail": {"description": "Validate output", "llm_model": "databricks-claude"}}
        )
        task, agent = _make_real_task_with_mock_agent()

        mock_gc = MagicMock()
        mock_gc.primary_group_id = "test-group"

        captured_kwargs = {}

        def mock_task_ctor(**kwargs):
            captured_kwargs.update(kwargs)
            return task

        with patch.object(TaskConfig, "_configure_task_tools", new=AsyncMock()), \
             patch("crewai.Task", side_effect=mock_task_ctor), \
             patch("crewai.tasks.llm_guardrail.LLMGuardrail") as MockLLMG, \
             patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new_callable=AsyncMock, return_value=MagicMock()), \
             patch("src.utils.user_context.UserContext.get_group_context", return_value=mock_gc):
            MockLLMG.return_value = MagicMock()

            await TaskConfig.configure_task(task_data, agent=agent)

        # LLM guardrail should be applied (as a Task kwarg, with retry enabled)
        assert "guardrail" in captured_kwargs
        assert captured_kwargs.get("retry_on_fail") is True

    @pytest.mark.asyncio
    async def test_configure_task_llm_guardrail_object_config(self):
        """llm_guardrail config as an object (not dict)."""
        llm_guardrail_cfg = MagicMock()
        llm_guardrail_cfg.description = "Validate"
        llm_guardrail_cfg.llm_model = "databricks-model"

        task_data = _make_task_data(config={"llm_guardrail": llm_guardrail_cfg})
        task, agent = _make_real_task_with_mock_agent()

        mock_gc = MagicMock()
        mock_gc.primary_group_id = "test-group"

        with patch.object(TaskConfig, "_configure_task_tools", new=AsyncMock()), \
             patch("crewai.Task", return_value=task), \
             patch("crewai.tasks.llm_guardrail.LLMGuardrail") as MockLLMG, \
             patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new_callable=AsyncMock, return_value=MagicMock()), \
             patch("src.utils.user_context.UserContext.get_group_context", return_value=mock_gc):
            mock_llm_guardrail = MagicMock()
            MockLLMG.return_value = mock_llm_guardrail

            result = await TaskConfig.configure_task(task_data, agent=agent)

        assert result is task

    async def _run_guardrail_and_get_model(self, guardrail_cfg, agent_model):
        """Configure a task with the given guardrail config + agent model,
        returning the model name passed to LLMManager.configure_crewai_llm."""
        task_data = _make_task_data(config={"llm_guardrail": guardrail_cfg})
        task, agent = _make_real_task_with_mock_agent()
        agent.llm.model = agent_model

        mock_gc = MagicMock()
        mock_gc.primary_group_id = "test-group"
        mock_configure_llm = AsyncMock(return_value=MagicMock())

        with patch.object(TaskConfig, "_configure_task_tools", new=AsyncMock()), \
             patch("crewai.Task", return_value=task), \
             patch("crewai.tasks.llm_guardrail.LLMGuardrail") as MockLLMG, \
             patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new=mock_configure_llm), \
             patch("src.utils.user_context.UserContext.get_group_context", return_value=mock_gc):
            MockLLMG.return_value = MagicMock()
            await TaskConfig.configure_task(task_data, agent=agent)

        mock_configure_llm.assert_called_once()
        call_args = mock_configure_llm.call_args
        return call_args.args[0] if call_args.args else call_args.kwargs.get("model_name", "")

    @pytest.mark.asyncio
    async def test_configure_task_llm_guardrail_defaults_to_agent_model(self):
        """With no explicit guardrail model, the guardrail defaults to the model
        its AGENT runs with (the chat-input selection), prefix-stripped."""
        model = await self._run_guardrail_and_get_model(
            {"description": "Validate"},  # no llm_model -> inherit run/agent model
            agent_model="databricks/run-selected-model",
        )
        assert model == "run-selected-model"

    @pytest.mark.asyncio
    async def test_configure_task_llm_guardrail_explicit_model_wins(self):
        """An explicitly chosen guardrail model overrides the agent/run model."""
        model = await self._run_guardrail_and_get_model(
            {"description": "Validate", "llm_model": "databricks-claude-opus-4"},
            agent_model="databricks/run-selected-model",
        )
        assert model == "databricks-claude-opus-4"

    @pytest.mark.asyncio
    async def test_configure_task_llm_guardrail_augments_description(self):
        """Non-default guardrail description augments task description."""
        task_data = _make_task_data(
            config={"llm_guardrail": {
                "description": "Check for exactly 5 items",
                "llm_model": "databricks/claude"
            }}
        )
        task, agent = _make_real_task_with_mock_agent()

        mock_gc = MagicMock()
        mock_gc.primary_group_id = "test-group"

        captured_desc = {}

        def mock_task_ctor(**kwargs):
            captured_desc["description"] = kwargs.get("description", "")
            return task

        with patch.object(TaskConfig, "_configure_task_tools", new=AsyncMock()), \
             patch("crewai.Task", side_effect=mock_task_ctor), \
             patch("crewai.tasks.llm_guardrail.LLMGuardrail"), \
             patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new_callable=AsyncMock, return_value=MagicMock()), \
             patch("src.utils.user_context.UserContext.get_group_context", return_value=mock_gc):
            await TaskConfig.configure_task(task_data, agent=agent)

        # description should include VALIDATION REQUIREMENTS
        assert "VALIDATION REQUIREMENTS" in task.description or \
               "VALIDATION REQUIREMENTS" in captured_desc.get("description", "")

    @pytest.mark.asyncio
    async def test_configure_task_llm_guardrail_exception_continues(self):
        task_data = _make_task_data(
            config={"llm_guardrail": {"description": "validate", "llm_model": "m"}}
        )
        task, agent = _make_real_task_with_mock_agent()

        mock_gc = MagicMock()
        mock_gc.primary_group_id = "test-group"

        with patch.object(TaskConfig, "_configure_task_tools", new=AsyncMock()), \
             patch("crewai.Task", return_value=task), \
             patch("crewai.tasks.llm_guardrail.LLMGuardrail", side_effect=ImportError("no crewai")), \
             patch("src.core.llm_manager.LLMManager.configure_crewai_llm", new_callable=AsyncMock, return_value=MagicMock()), \
             patch("src.utils.user_context.UserContext.get_group_context", return_value=mock_gc):
            result = await TaskConfig.configure_task(task_data, agent=agent)

        assert result is task

    @pytest.mark.asyncio
    async def test_configure_task_exception_returns_none(self):
        task_data = _make_task_data()
        _, agent = _make_real_task_with_mock_agent()

        with patch.object(TaskConfig, "_configure_task_tools", new=AsyncMock(side_effect=RuntimeError("tools fail"))):
            result = await TaskConfig.configure_task(task_data, agent=agent)

        assert result is None


# ---------------------------------------------------------------------------
# _resolve_agent_for_task
# ---------------------------------------------------------------------------

class TestResolveAgentForTask:

    @pytest.mark.asyncio
    async def test_no_agent_id_and_no_flow_data(self):
        task_data = _make_task_data(agent_id=None)
        result = await TaskConfig._resolve_agent_for_task(task_data, None, None)
        assert result is None

    @pytest.mark.asyncio
    async def test_agent_found_via_repo(self):
        agent_id = uuid.uuid4()
        task_data = _make_task_data(agent_id=agent_id)
        agent_data = MagicMock()
        agent_data.id = agent_id

        mock_agent = MagicMock()
        agent_repo = MagicMock()
        agent_repo.get = AsyncMock(return_value=agent_data)

        with patch("src.engines.crewai.flow.modules.agent_adapter.AgentConfig.configure_agent_and_tools",
                   new=AsyncMock(return_value=mock_agent)):
            result = await TaskConfig._resolve_agent_for_task(
                task_data, None, {"agent": agent_repo}
            )

        assert result is mock_agent

    @pytest.mark.asyncio
    async def test_agent_not_found_in_repo_fallback_db(self):
        agent_id = uuid.uuid4()
        task_data = _make_task_data(agent_id=agent_id)
        mock_agent = MagicMock()

        agent_repo = MagicMock()
        agent_repo.get = AsyncMock(return_value=None)

        with patch("src.db.session.request_scoped_session") as MockSession, \
             patch("src.engines.crewai.flow.modules.agent_adapter.AgentConfig.configure_agent_and_tools",
                   new=AsyncMock(return_value=mock_agent)):
            mock_session_ctx = MagicMock()
            mock_session = AsyncMock()
            mock_result = MagicMock()
            mock_result.scalar_one_or_none = MagicMock(return_value=MagicMock())
            mock_session.execute = AsyncMock(return_value=mock_result)
            mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_ctx.__aexit__ = AsyncMock(return_value=False)
            MockSession.return_value = mock_session_ctx

            result = await TaskConfig._resolve_agent_for_task(
                task_data, None, {"agent": agent_repo}
            )

        assert result is mock_agent

    @pytest.mark.asyncio
    async def test_agent_configure_fails_returns_none(self):
        agent_id = uuid.uuid4()
        task_data = _make_task_data(agent_id=agent_id)
        agent_data = MagicMock()

        agent_repo = MagicMock()
        agent_repo.get = AsyncMock(return_value=agent_data)

        with patch("src.engines.crewai.flow.modules.agent_adapter.AgentConfig.configure_agent_and_tools",
                   new=AsyncMock(return_value=None)):
            result = await TaskConfig._resolve_agent_for_task(
                task_data, None, {"agent": agent_repo}
            )

        assert result is None

    @pytest.mark.asyncio
    async def test_agent_id_not_found_in_db_returns_none(self):
        agent_id = uuid.uuid4()
        task_data = _make_task_data(agent_id=agent_id)

        agent_repo = MagicMock()
        agent_repo.get = AsyncMock(return_value=None)

        with patch("src.db.session.request_scoped_session") as MockSession:
            mock_session_ctx = MagicMock()
            mock_session = AsyncMock()
            mock_result = MagicMock()
            mock_result.scalar_one_or_none = MagicMock(return_value=None)
            mock_session.execute = AsyncMock(return_value=mock_result)
            mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_ctx.__aexit__ = AsyncMock(return_value=False)
            MockSession.return_value = mock_session_ctx

            result = await TaskConfig._resolve_agent_for_task(
                task_data, None, {"agent": agent_repo}
            )

        assert result is None

    @pytest.mark.asyncio
    async def test_agent_db_error_continues_to_none(self):
        agent_id = uuid.uuid4()
        task_data = _make_task_data(agent_id=agent_id)

        agent_repo = MagicMock()
        agent_repo.get = AsyncMock(return_value=None)

        with patch("src.db.session.request_scoped_session") as MockSession:
            mock_session_ctx = MagicMock()
            mock_session_ctx.__aenter__ = AsyncMock(side_effect=Exception("db error"))
            mock_session_ctx.__aexit__ = AsyncMock(return_value=False)
            MockSession.return_value = mock_session_ctx

            result = await TaskConfig._resolve_agent_for_task(
                task_data, None, {"agent": agent_repo}
            )

        assert result is None


# ---------------------------------------------------------------------------
# _configure_task_tools
# ---------------------------------------------------------------------------

def _make_mock_session_ctx():
    """Helper to create a mock session context manager."""
    from contextlib import asynccontextmanager
    mock_session = AsyncMock()
    mock_session_ctx = MagicMock()
    mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_ctx.__aexit__ = AsyncMock(return_value=False)
    return mock_session_ctx, mock_session


class TestConfigureTaskTools:

    @pytest.mark.asyncio
    async def test_task_with_no_tools(self):
        task_data = _make_task_data(tools=None)
        _, agent = _make_real_task_with_mock_agent()
        agent.tools = []

        session_ctx, _ = _make_mock_session_ctx()

        with patch("src.db.session.request_scoped_session", return_value=session_ctx), \
             patch("src.services.api_keys_service.ApiKeysService"), \
             patch("src.engines.crewai.tools.tool_factory.ToolFactory") as MockTF:
            MockTF.create = AsyncMock(return_value=MagicMock())

            await TaskConfig._configure_task_tools(task_data, agent, None)

        # No tools assigned - agent.tools should remain unchanged
        assert agent.tools == []

    @pytest.mark.asyncio
    async def test_task_with_list_of_tools(self):
        task_data = _make_task_data(tools=["1", "2"])
        _, agent = _make_real_task_with_mock_agent()
        agent.tools = []

        mock_tool_factory = MagicMock()
        mock_tool = MagicMock()
        mock_tool_factory.create_tool.return_value = mock_tool
        mock_tool_factory.get_tool_info.return_value = None

        session_ctx, _ = _make_mock_session_ctx()

        with patch("src.db.session.request_scoped_session", return_value=session_ctx), \
             patch("src.services.api_keys_service.ApiKeysService"), \
             patch("src.engines.crewai.tools.tool_factory.ToolFactory") as MockTF:
            MockTF.create = AsyncMock(return_value=mock_tool_factory)

            await TaskConfig._configure_task_tools(task_data, agent, None)

        assert agent.tools == [mock_tool, mock_tool]

    @pytest.mark.asyncio
    async def test_task_with_json_tools_string(self):
        task_data = _make_task_data(tools='["tool-1"]')
        _, agent = _make_real_task_with_mock_agent()
        agent.tools = []

        mock_tool_factory = MagicMock()
        mock_tool_factory.create_tool.return_value = MagicMock()
        mock_tool_factory.get_tool_info.return_value = None

        session_ctx, _ = _make_mock_session_ctx()

        with patch("src.db.session.request_scoped_session", return_value=session_ctx), \
             patch("src.services.api_keys_service.ApiKeysService"), \
             patch("src.engines.crewai.tools.tool_factory.ToolFactory") as MockTF:
            MockTF.create = AsyncMock(return_value=mock_tool_factory)

            await TaskConfig._configure_task_tools(task_data, agent, None)

        # 1 tool assigned
        assert len(agent.tools) >= 1

    @pytest.mark.asyncio
    async def test_task_with_invalid_json_tools_string(self):
        task_data = _make_task_data(tools="not-json")
        _, agent = _make_real_task_with_mock_agent()

        session_ctx, _ = _make_mock_session_ctx()

        with patch("src.db.session.request_scoped_session", return_value=session_ctx), \
             patch("src.services.api_keys_service.ApiKeysService"), \
             patch("src.engines.crewai.tools.tool_factory.ToolFactory") as MockTF:
            MockTF.create = AsyncMock(return_value=MagicMock())

            # Should not raise
            await TaskConfig._configure_task_tools(task_data, agent, None)

    @pytest.mark.asyncio
    async def test_task_with_empty_tools_list(self):
        task_data = _make_task_data(tools=[])
        _, agent = _make_real_task_with_mock_agent()
        agent.tools = []

        session_ctx, _ = _make_mock_session_ctx()

        with patch("src.db.session.request_scoped_session", return_value=session_ctx), \
             patch("src.services.api_keys_service.ApiKeysService"), \
             patch("src.engines.crewai.tools.tool_factory.ToolFactory") as MockTF:
            MockTF.create = AsyncMock(return_value=MagicMock())

            await TaskConfig._configure_task_tools(task_data, agent, None)

        # Empty list → agent tools unchanged
        assert agent.tools == []

    @pytest.mark.asyncio
    async def test_task_tool_factory_create_fails_continues(self):
        task_data = _make_task_data(tools=["broken-tool"])
        _, agent = _make_real_task_with_mock_agent()
        agent.tools = []

        mock_tool_factory = MagicMock()
        mock_tool_factory.create_tool.side_effect = Exception("tool factory error")
        mock_tool_factory.get_tool_info.return_value = None

        session_ctx, _ = _make_mock_session_ctx()

        with patch("src.db.session.request_scoped_session", return_value=session_ctx), \
             patch("src.services.api_keys_service.ApiKeysService"), \
             patch("src.engines.crewai.tools.tool_factory.ToolFactory") as MockTF:
            MockTF.create = AsyncMock(return_value=mock_tool_factory)

            await TaskConfig._configure_task_tools(task_data, agent, None)

        # Tool failed but agent tools not changed from empty
        assert agent.tools == []

    @pytest.mark.asyncio
    async def test_task_tool_returns_none_warning(self):
        task_data = _make_task_data(tools=["null-tool"])
        _, agent = _make_real_task_with_mock_agent()
        agent.tools = []

        mock_tool_factory = MagicMock()
        mock_tool_factory.create_tool.return_value = None  # Factory returns None
        mock_tool_factory.get_tool_info.return_value = None

        session_ctx, _ = _make_mock_session_ctx()

        with patch("src.db.session.request_scoped_session", return_value=session_ctx), \
             patch("src.services.api_keys_service.ApiKeysService"), \
             patch("src.engines.crewai.tools.tool_factory.ToolFactory") as MockTF:
            MockTF.create = AsyncMock(return_value=mock_tool_factory)

            await TaskConfig._configure_task_tools(task_data, agent, None)

        # No tools added since factory returned None
        assert agent.tools == []

    @pytest.mark.asyncio
    async def test_task_with_group_context_creates_factory_with_group_id(self):
        task_data = _make_task_data(tools=[])
        _, agent = _make_real_task_with_mock_agent()
        group_ctx = MagicMock()
        group_ctx.primary_group_id = "group-123"

        session_ctx, _ = _make_mock_session_ctx()

        with patch("src.db.session.request_scoped_session", return_value=session_ctx), \
             patch("src.services.api_keys_service.ApiKeysService"), \
             patch("src.engines.crewai.tools.tool_factory.ToolFactory") as MockTF:
            mock_tf = AsyncMock()
            MockTF.create = AsyncMock(return_value=mock_tf)

            await TaskConfig._configure_task_tools(task_data, agent, None, group_context=group_ctx)

        # factory should have been called with group_id
        MockTF.create.assert_called_once()
        call_kw = MockTF.create.call_args.kwargs
        assert call_kw.get("config", {}).get("group_id") == "group-123"

    @pytest.mark.asyncio
    async def test_task_api_keys_service_fails_fallback(self):
        """When request_scoped_session fails for ApiKeys, falls back to basic factory."""
        task_data = _make_task_data(tools=["1"])
        _, agent = _make_real_task_with_mock_agent()
        agent.tools = []

        from contextlib import asynccontextmanager
        fail_ctx = MagicMock()
        fail_ctx.__aenter__ = AsyncMock(side_effect=Exception("session fail"))
        fail_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_factory = MagicMock()
        mock_factory.initialize = AsyncMock()
        mock_tool = MagicMock()
        mock_factory.create_tool.return_value = mock_tool
        mock_factory.get_tool_info.return_value = None

        with patch("src.db.session.request_scoped_session", return_value=fail_ctx), \
             patch("src.services.api_keys_service.ApiKeysService"), \
             patch("src.engines.crewai.tools.tool_factory.ToolFactory") as MockTF:
            MockTF.return_value = mock_factory

            await TaskConfig._configure_task_tools(task_data, agent, None)

        # Falls back to basic factory, 1 tool
        assert len(agent.tools) >= 1

    @pytest.mark.asyncio
    async def test_task_with_flow_nodes_for_tools(self):
        """When task has no tools, look in flow nodes."""
        task_id = uuid.uuid4()
        task_data = _make_task_data(tools=None, id=task_id)

        flow_data = SimpleNamespace(
            nodes=[
                {
                    "id": f"task-{task_id}",
                    "type": "taskNode",
                    "data": {"tools": ["node-tool-1"]}
                }
            ]
        )

        _, agent = _make_real_task_with_mock_agent()
        agent.tools = []
        mock_tool = MagicMock()
        mock_factory = MagicMock()
        mock_factory.create_tool.return_value = mock_tool
        mock_factory.get_tool_info.return_value = None

        session_ctx, _ = _make_mock_session_ctx()

        with patch("src.db.session.request_scoped_session", return_value=session_ctx), \
             patch("src.services.api_keys_service.ApiKeysService"), \
             patch("src.engines.crewai.tools.tool_factory.ToolFactory") as MockTF:
            MockTF.create = AsyncMock(return_value=mock_factory)

            await TaskConfig._configure_task_tools(task_data, agent, flow_data)

        assert mock_tool in agent.tools

    @pytest.mark.asyncio
    async def test_task_no_tools_no_flow_data_no_assign(self):
        """No tools in task, no flow data - no tools assigned, no error."""
        task_data = _make_task_data(tools=None)
        _, agent = _make_real_task_with_mock_agent()
        agent.tools = []

        session_ctx, _ = _make_mock_session_ctx()

        with patch("src.db.session.request_scoped_session", return_value=session_ctx), \
             patch("src.services.api_keys_service.ApiKeysService"), \
             patch("src.engines.crewai.tools.tool_factory.ToolFactory") as MockTF:
            MockTF.create = AsyncMock(return_value=MagicMock())

            await TaskConfig._configure_task_tools(task_data, agent, None)

        assert agent.tools == []
