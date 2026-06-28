"""
Coverage-boosting tests for the BODY of flow method closures in flow_methods.py.

These test the inner execution code of create_starting_point_crew_method and
create_listener_method by calling the wrapped method functions directly.
"""

import pytest
import uuid
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch, Mock

from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory, extract_final_answer


def _make_task(role="Agent", has_context=False, has_kasal_memory_disabled=False):
    """Create a minimal mock task."""
    agent = MagicMock()
    agent.role = role
    agent.tools = []
    agent.llm = MagicMock()
    agent.llm.model = "gpt-4o"
    agent.llm.max_tokens = None
    agent.llm.timeout = None
    agent._kasal_memory_disabled = has_kasal_memory_disabled
    agent.reasoning = False

    task = MagicMock()
    task.agent = agent
    task.description = "Test task " * 5  # >50 chars to test truncation
    task.expected_output = "Output"
    if has_context:
        task.context = [MagicMock()]  # Non-empty context list
    else:
        task.context = []
    return task


def _make_flow_instance(state=None):
    """Create a mock flow instance."""
    mock_flow = MagicMock()
    mock_flow.state = state if state is not None else {}
    return mock_flow


def _make_create_callbacks():
    return MagicMock(return_value=(MagicMock(), MagicMock()))


# ---------------------------------------------------------------------------
# Starting point method body tests
# ---------------------------------------------------------------------------

class TestStartingPointMethodBody:

    @pytest.mark.asyncio
    async def test_task_with_context_logs_dependency(self):
        """Covers line 222: task with context list logs dependency count."""
        task = _make_task(has_context=True)
        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name="starting_point_0",
            task_list=[task],
            crew_name="Test Crew",
            callbacks={"job_id": "job-1"},
            group_context=None,
            create_execution_callbacks=_make_create_callbacks(),
        )

        mock_flow = _make_flow_instance()

        with patch("src.engines.crewai.paths.flow.modules.flow_methods.Crew") as MockCrew, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.asyncio.wait_for") as mock_wait:
            crew_inst = MagicMock()
            MockCrew.return_value = crew_inst
            mock_result = MagicMock()
            mock_result.raw = "done"
            mock_wait.return_value = mock_result

            inner = method._meth
            result = await inner(mock_flow)

        assert result == "done"

    @pytest.mark.asyncio
    async def test_crew_memory_all_disabled(self):
        """Covers lines 264-266: all agents have memory disabled → crew_memory=False."""
        task = _make_task(has_kasal_memory_disabled=True)
        crew_data = MagicMock()
        crew_data.memory = None  # Not explicitly set
        crew_data.process = None
        crew_data.verbose = None
        crew_data.planning = False
        crew_data.reasoning = False

        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name="starting_point_0",
            task_list=[task],
            crew_name="Test Crew",
            callbacks={"job_id": "job-2"},
            group_context=None,
            create_execution_callbacks=_make_create_callbacks(),
            crew_data=crew_data,
        )

        mock_flow = _make_flow_instance()

        with patch("src.engines.crewai.paths.flow.modules.flow_methods.Crew") as MockCrew, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.asyncio.wait_for") as mock_wait:
            crew_inst = MagicMock()
            crew_kwargs_captured = {}

            def capture_kwargs(**kwargs):
                crew_kwargs_captured.update(kwargs)
                return crew_inst

            MockCrew.side_effect = capture_kwargs
            mock_result = MagicMock()
            mock_result.raw = "result"
            mock_wait.return_value = mock_result

            inner = method._meth
            result = await inner(mock_flow)

        assert crew_kwargs_captured.get("memory") is False

    @pytest.mark.asyncio
    async def test_crew_memory_explicitly_false(self):
        """Covers line 268-270: crew config explicitly sets memory=False."""
        task = _make_task()
        crew_data = MagicMock()
        crew_data.memory = False
        crew_data.process = None
        crew_data.verbose = None
        crew_data.planning = False
        crew_data.reasoning = False

        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name="starting_point_0",
            task_list=[task],
            crew_name="Test Crew",
            callbacks={"job_id": "job-3"},
            group_context=None,
            create_execution_callbacks=_make_create_callbacks(),
            crew_data=crew_data,
        )

        mock_flow = _make_flow_instance()

        with patch("src.engines.crewai.paths.flow.modules.flow_methods.Crew") as MockCrew, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.asyncio.wait_for") as mock_wait:
            crew_kwargs_captured = {}

            def capture_kwargs(**kwargs):
                crew_kwargs_captured.update(kwargs)
                return MagicMock()

            MockCrew.side_effect = capture_kwargs
            mock_result = MagicMock()
            mock_result.raw = "res"
            mock_wait.return_value = mock_result

            inner = method._meth
            await inner(mock_flow)

        assert crew_kwargs_captured.get("memory") is False

    @pytest.mark.asyncio
    async def test_crew_memory_from_config_true(self):
        """Covers line 270-272: crew config sets memory=True with at least one enabled agent."""
        # One agent enabled, one disabled - not all disabled, so crew_config=True branch
        task1 = _make_task(has_kasal_memory_disabled=False)  # enabled
        task2 = _make_task(role="Agent2", has_kasal_memory_disabled=True)  # disabled
        crew_data = MagicMock()
        crew_data.memory = True
        crew_data.process = None
        crew_data.verbose = None
        crew_data.planning = False
        crew_data.reasoning = False

        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name="starting_point_0",
            task_list=[task1, task2],
            crew_name="Test Crew",
            callbacks={"job_id": "job-4"},
            group_context=None,
            create_execution_callbacks=_make_create_callbacks(),
            crew_data=crew_data,
        )

        mock_flow = _make_flow_instance()

        with patch("src.engines.crewai.paths.flow.modules.flow_methods.Crew") as MockCrew, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.asyncio.wait_for") as mock_wait:
            crew_kwargs_captured = {}

            def capture_kwargs(**kwargs):
                crew_kwargs_captured.update(kwargs)
                return MagicMock()

            MockCrew.side_effect = capture_kwargs
            mock_result = MagicMock()
            mock_result.raw = "res"
            mock_wait.return_value = mock_result

            inner = method._meth
            await inner(mock_flow)

        # crew_memory_from_config=True with one enabled agent → memory wiring runs.
        # configure_flow_crew_memory now PROCESSES memory into a configured Memory
        # (or a graceful False) instead of leaving the bare True that made CrewAI
        # build its own ChromaDB+OpenAI Memory and fail with CHROMA_OPENAI_API_KEY.
        assert crew_kwargs_captured.get("memory") is not True

    @pytest.mark.asyncio
    async def test_attaches_trace_context_to_tools_and_memory(self):
        """Flow crews attach execution trace context (parity with the crew path)
        so tool/memory traces carry job_id + group attribution."""
        task = _make_task()
        crew_data = MagicMock()
        crew_data.memory = False  # skip memory wiring → isolate the trace attach
        crew_data.process = None
        crew_data.verbose = None
        crew_data.planning = False
        crew_data.reasoning = False

        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name="starting_point_0",
            task_list=[task],
            crew_name="Test Crew",
            callbacks={"job_id": "job-trace-1"},
            group_context=None,
            create_execution_callbacks=_make_create_callbacks(),
            crew_data=crew_data,
        )
        mock_flow = _make_flow_instance()

        with patch("src.engines.crewai.paths.flow.modules.flow_methods.Crew") as MockCrew, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.asyncio.wait_for") as mock_wait, \
             patch("src.engines.crewai.kernel.trace_context.attach_execution_trace_context") as mock_attach:
            MockCrew.return_value = MagicMock()
            mock_result = MagicMock()
            mock_result.raw = "res"
            mock_wait.return_value = mock_result

            await method._meth(mock_flow)

        # Flow delegates to the shared common entry point (the SAME one the crew
        # path uses), passing the crew + crew_kwargs and the group/job for attribution.
        mock_attach.assert_called_once()
        args, kwargs = mock_attach.call_args
        assert args[0] is MockCrew.return_value
        assert "group_id" in kwargs and "job_id" in kwargs

    @pytest.mark.asyncio
    async def test_crew_hierarchical_process(self):
        """Covers lines 282-283: hierarchical process type."""
        task = _make_task()
        crew_data = MagicMock()
        crew_data.memory = None
        crew_data.process = "hierarchical"
        crew_data.verbose = None
        crew_data.planning = False
        crew_data.reasoning = False

        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name="starting_point_0",
            task_list=[task],
            crew_name="Test Crew",
            callbacks={},
            group_context=None,
            create_execution_callbacks=_make_create_callbacks(),
            crew_data=crew_data,
        )

        mock_flow = _make_flow_instance()

        with patch("src.engines.crewai.paths.flow.modules.flow_methods.Crew") as MockCrew, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.asyncio.wait_for") as mock_wait, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.Process") as MockProcess:
            crew_kwargs_captured = {}

            def capture_kwargs(**kwargs):
                crew_kwargs_captured.update(kwargs)
                return MagicMock()

            MockCrew.side_effect = capture_kwargs
            mock_result = MagicMock()
            mock_result.raw = "res"
            mock_wait.return_value = mock_result

            inner = method._meth
            await inner(mock_flow)

        assert "process" in crew_kwargs_captured

    @pytest.mark.asyncio
    async def test_planning_with_planning_llm(self):
        """Covers lines 307-316: planning enabled with explicit planning_llm."""
        task = _make_task()
        crew_data = MagicMock()
        crew_data.memory = None
        crew_data.process = None
        crew_data.verbose = None
        crew_data.planning = True
        crew_data.planning_llm = "claude-3"
        crew_data.reasoning = False

        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name="starting_point_0",
            task_list=[task],
            crew_name="Test Crew",
            callbacks={},
            group_context=None,
            create_execution_callbacks=_make_create_callbacks(),
            crew_data=crew_data,
        )

        mock_flow = _make_flow_instance()

        with patch("src.engines.crewai.paths.flow.modules.flow_methods.Crew") as MockCrew, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.asyncio.wait_for") as mock_wait, \
             patch("src.core.llm_manager.LLMManager") as MockLLM:
            mock_llm = MagicMock()
            MockLLM.get_llm = AsyncMock(return_value=mock_llm)
            crew_kwargs_captured = {}

            def capture_kwargs(**kwargs):
                crew_kwargs_captured.update(kwargs)
                return MagicMock()

            MockCrew.side_effect = capture_kwargs
            mock_result = MagicMock()
            mock_result.raw = "res"
            mock_wait.return_value = mock_result

            inner = method._meth
            await inner(mock_flow)

        assert crew_kwargs_captured.get("planning") is True

    @pytest.mark.asyncio
    async def test_planning_fallback_to_agent_llm(self):
        """Covers line 317-320: planning enabled, no planning_llm, use agent's LLM."""
        task = _make_task()
        crew_data = MagicMock()
        crew_data.memory = None
        crew_data.process = None
        crew_data.verbose = None
        crew_data.planning = True
        crew_data.planning_llm = None  # No planning LLM
        crew_data.reasoning = False

        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name="starting_point_0",
            task_list=[task],
            crew_name="Test Crew",
            callbacks={},
            group_context=None,
            create_execution_callbacks=_make_create_callbacks(),
            crew_data=crew_data,
        )

        mock_flow = _make_flow_instance()

        with patch("src.engines.crewai.paths.flow.modules.flow_methods.Crew") as MockCrew, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.asyncio.wait_for") as mock_wait:
            crew_kwargs_captured = {}

            def capture_kwargs(**kwargs):
                crew_kwargs_captured.update(kwargs)
                return MagicMock()

            MockCrew.side_effect = capture_kwargs
            mock_result = MagicMock()
            mock_result.raw = "res"
            mock_wait.return_value = mock_result

            inner = method._meth
            await inner(mock_flow)

        # Should fall back to agent's LLM
        assert crew_kwargs_captured.get("planning") is True

    @pytest.mark.asyncio
    async def test_planning_llm_creation_fails(self):
        """Covers line 315-316: planning_llm creation fails, uses warning."""
        task = _make_task()
        crew_data = MagicMock()
        crew_data.memory = None
        crew_data.process = None
        crew_data.verbose = None
        crew_data.planning = True
        crew_data.planning_llm = "bad-model"
        crew_data.reasoning = False

        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name="starting_point_0",
            task_list=[task],
            crew_name="Test Crew",
            callbacks={},
            group_context=None,
            create_execution_callbacks=_make_create_callbacks(),
            crew_data=crew_data,
        )

        mock_flow = _make_flow_instance()

        with patch("src.engines.crewai.paths.flow.modules.flow_methods.Crew") as MockCrew, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.asyncio.wait_for") as mock_wait, \
             patch("src.core.llm_manager.LLMManager") as MockLLM:
            MockLLM.get_llm = AsyncMock(side_effect=Exception("LLM error"))
            crew_kwargs_captured = {}

            def capture_kwargs(**kwargs):
                crew_kwargs_captured.update(kwargs)
                return MagicMock()

            MockCrew.side_effect = capture_kwargs
            mock_result = MagicMock()
            mock_result.raw = "res"
            mock_wait.return_value = mock_result

            inner = method._meth
            await inner(mock_flow)

        # Should still work even if planning LLM creation fails
        assert crew_kwargs_captured.get("planning") is True

    @pytest.mark.asyncio
    async def test_reasoning_propagated_to_agents(self):
        """Covers lines 328-336: reasoning propagated to agents."""
        task = _make_task()
        task.agent.reasoning = False  # Not yet enabled
        crew_data = MagicMock()
        crew_data.memory = None
        crew_data.process = None
        crew_data.verbose = None
        crew_data.planning = False
        crew_data.reasoning = True

        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name="starting_point_0",
            task_list=[task],
            crew_name="Test Crew",
            callbacks={},
            group_context=None,
            create_execution_callbacks=_make_create_callbacks(),
            crew_data=crew_data,
        )

        mock_flow = _make_flow_instance()

        with patch("src.engines.crewai.paths.flow.modules.flow_methods.Crew") as MockCrew, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.asyncio.wait_for") as mock_wait:
            crew_kwargs_captured = {}

            def capture_kwargs(**kwargs):
                crew_kwargs_captured.update(kwargs)
                return MagicMock()

            MockCrew.side_effect = capture_kwargs
            mock_result = MagicMock()
            mock_result.raw = "res"
            mock_wait.return_value = mock_result

            inner = method._meth
            await inner(mock_flow)

        assert crew_kwargs_captured.get("reasoning") is True
        assert task.agent.reasoning is True

    @pytest.mark.asyncio
    async def test_embedder_configured_for_memory(self):
        """Covers lines 339-353: embedder configured when crew_memory is True."""
        task = _make_task()
        crew_data = MagicMock()
        crew_data.memory = True
        crew_data.process = None
        crew_data.verbose = None
        crew_data.planning = False
        crew_data.reasoning = False

        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name="starting_point_0",
            task_list=[task],
            crew_name="Test Crew",
            callbacks={"job_id": "job-emb"},
            group_context=None,
            create_execution_callbacks=_make_create_callbacks(),
            crew_data=crew_data,
            group_id="g1",
        )

        mock_flow = _make_flow_instance()

        with patch("src.engines.crewai.paths.flow.modules.flow_methods.Crew") as MockCrew, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.asyncio.wait_for") as mock_wait, \
             patch("src.engines.crewai.config.embedder_config_builder.EmbedderConfigBuilder") as MockEmb:
            mock_builder = MagicMock()
            mock_builder.configure_embedder = AsyncMock(return_value=({"memory": True, "embedder": {"provider": "test"}}, None, None))
            MockEmb.return_value = mock_builder

            crew_inst = MagicMock()
            MockCrew.return_value = crew_inst
            mock_result = MagicMock()
            mock_result.raw = "res"
            mock_wait.return_value = mock_result

            inner = method._meth
            await inner(mock_flow)

        # Should have called the embedder builder
        assert mock_builder.configure_embedder.called

    @pytest.mark.asyncio
    async def test_embedder_config_fails_continues(self):
        """Covers lines 350-353: embedder config fails, continues."""
        task = _make_task()
        crew_data = MagicMock()
        crew_data.memory = True
        crew_data.process = None
        crew_data.verbose = None
        crew_data.planning = False
        crew_data.reasoning = False

        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name="starting_point_0",
            task_list=[task],
            crew_name="Test Crew",
            callbacks={},
            group_context=None,
            create_execution_callbacks=_make_create_callbacks(),
            crew_data=crew_data,
        )

        mock_flow = _make_flow_instance()

        with patch("src.engines.crewai.paths.flow.modules.flow_methods.Crew") as MockCrew, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.asyncio.wait_for") as mock_wait, \
             patch("src.engines.crewai.config.embedder_config_builder.EmbedderConfigBuilder", side_effect=ImportError("no embedder")):
            crew_inst = MagicMock()
            MockCrew.return_value = crew_inst
            mock_result = MagicMock()
            mock_result.raw = "res"
            mock_wait.return_value = mock_result

            inner = method._meth
            result = await inner(mock_flow)

        # Should still work even if embedder fails
        assert result == "res"

    @pytest.mark.asyncio
    async def test_result_none_fallback(self):
        """Covers line 443: result is None → serializable_result = None."""
        task = _make_task()
        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name="starting_point_0",
            task_list=[task],
            crew_name="Test Crew",
            callbacks={},
            group_context=None,
            create_execution_callbacks=_make_create_callbacks(),
        )

        mock_flow = _make_flow_instance()

        with patch("src.engines.crewai.paths.flow.modules.flow_methods.Crew") as MockCrew, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.asyncio.wait_for") as mock_wait:
            crew_inst = MagicMock()
            MockCrew.return_value = crew_inst
            mock_wait.return_value = None  # Returns None

            inner = method._meth
            result = await inner(mock_flow)

        assert result is None

    @pytest.mark.asyncio
    async def test_result_not_raw_string_fallback(self):
        """Covers line 441-442: result has no raw but is not None → str(result)."""
        task = _make_task()
        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name="starting_point_0",
            task_list=[task],
            crew_name="Test Crew",
            callbacks={},
            group_context=None,
            create_execution_callbacks=_make_create_callbacks(),
        )

        mock_flow = _make_flow_instance()

        class NoRaw:
            def __str__(self):
                return "no raw result"

        with patch("src.engines.crewai.paths.flow.modules.flow_methods.Crew") as MockCrew, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.asyncio.wait_for") as mock_wait:
            crew_inst = MagicMock()
            MockCrew.return_value = crew_inst
            no_raw = NoRaw()
            mock_wait.return_value = no_raw

            inner = method._meth
            result = await inner(mock_flow)

        assert result == "no raw result"

    @pytest.mark.asyncio
    async def test_state_stored_in_dict(self):
        """Covers lines 450-453: result stored in state dict."""
        task = _make_task()
        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name="starting_point_0",
            task_list=[task],
            crew_name="My Crew",
            callbacks={},
            group_context=None,
            create_execution_callbacks=_make_create_callbacks(),
        )

        mock_flow = _make_flow_instance(state={})

        with patch("src.engines.crewai.paths.flow.modules.flow_methods.Crew") as MockCrew, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.asyncio.wait_for") as mock_wait:
            crew_inst = MagicMock()
            MockCrew.return_value = crew_inst
            mock_result = MagicMock()
            mock_result.raw = "stored result"
            mock_wait.return_value = mock_result

            inner = method._meth
            result = await inner(mock_flow)

        assert result == "stored result"
        assert mock_flow.state.get("starting_point_0") == "stored result"

    @pytest.mark.asyncio
    async def test_result_long_raw_diagnostic(self):
        """Covers lines 427-431: result.raw length >400 - logs first/last 200 chars."""
        task = _make_task()
        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name="starting_point_0",
            task_list=[task],
            crew_name="Test Crew",
            callbacks={},
            group_context=None,
            create_execution_callbacks=_make_create_callbacks(),
        )

        mock_flow = _make_flow_instance()
        long_result = "x" * 500  # >400 chars

        with patch("src.engines.crewai.paths.flow.modules.flow_methods.Crew") as MockCrew, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.asyncio.wait_for") as mock_wait:
            crew_inst = MagicMock()
            MockCrew.return_value = crew_inst
            mock_result = MagicMock()
            mock_result.raw = long_result
            mock_wait.return_value = mock_result

            inner = method._meth
            result = await inner(mock_flow)

        assert result == long_result


# ---------------------------------------------------------------------------
# Listener method body tests
# ---------------------------------------------------------------------------

class TestListenerMethodBody:

    @pytest.mark.asyncio
    async def test_listener_memory_all_disabled(self):
        """Covers lines 620-622: all listener agents have memory disabled."""
        task = _make_task(has_kasal_memory_disabled=True)
        crew_data = MagicMock()
        crew_data.memory = None
        crew_data.process = None
        crew_data.verbose = None
        crew_data.planning = False
        crew_data.reasoning = False

        method = FlowMethodFactory.create_listener_method(
            method_name="listener_0",
            listener_tasks=[task],
            method_condition="starting_point_0",
            condition_type="NONE",
            callbacks={"job_id": "job-1"},
            group_context=None,
            create_execution_callbacks=_make_create_callbacks(),
            crew_data=crew_data,
        )

        mock_flow = _make_flow_instance()

        with patch("src.engines.crewai.paths.flow.modules.flow_methods.Crew") as MockCrew, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.asyncio.wait_for") as mock_wait, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.Task") as MockTask:
            crew_kwargs_captured = {}

            def capture_kwargs(**kwargs):
                crew_kwargs_captured.update(kwargs)
                return MagicMock()

            MockCrew.side_effect = capture_kwargs
            MockTask.return_value = MagicMock(agent=task.agent, expected_output="out")
            mock_result = MagicMock()
            mock_result.raw = "listener result"
            mock_wait.return_value = mock_result

            inner = method._meth
            result = await inner(mock_flow)

        assert crew_kwargs_captured.get("memory") is False

    @pytest.mark.asyncio
    async def test_listener_with_previous_output(self):
        """Covers lines 540-558: listener gets previous output, context injected."""
        task = _make_task()
        method = FlowMethodFactory.create_listener_method(
            method_name="listener_0",
            listener_tasks=[task],
            method_condition="starting_point_0",
            condition_type="NONE",
            callbacks={"job_id": "job-2"},
            group_context=None,
            create_execution_callbacks=_make_create_callbacks(),
        )

        mock_flow = _make_flow_instance()

        with patch("src.engines.crewai.paths.flow.modules.flow_methods.Crew") as MockCrew, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.asyncio.wait_for") as mock_wait, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.Task") as MockTask:
            crew_inst = MagicMock()
            MockCrew.return_value = crew_inst
            runtime_task = MagicMock(agent=task.agent, expected_output="out")
            MockTask.return_value = runtime_task
            mock_result = MagicMock()
            mock_result.raw = "done"
            mock_wait.return_value = mock_result

            inner = method._meth
            # Pass previous output as results tuple
            result = await inner(mock_flow, "previous crew output")

        assert result == "done"

    @pytest.mark.asyncio
    async def test_listener_large_context_injection(self):
        """Covers lines 550-558: large previous output (>2K chars) - brief context."""
        task = _make_task()
        method = FlowMethodFactory.create_listener_method(
            method_name="listener_0",
            listener_tasks=[task],
            method_condition="starting_point_0",
            condition_type="NONE",
            callbacks={},
            group_context=None,
            create_execution_callbacks=_make_create_callbacks(),
        )

        mock_flow = _make_flow_instance()
        large_output = "x" * 3000  # >2000 chars

        with patch("src.engines.crewai.paths.flow.modules.flow_methods.Crew") as MockCrew, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.asyncio.wait_for") as mock_wait, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.Task") as MockTask:
            crew_inst = MagicMock()
            MockCrew.return_value = crew_inst
            MockTask.return_value = MagicMock(agent=task.agent, expected_output="out")
            mock_result = MagicMock()
            mock_result.raw = "done"
            mock_wait.return_value = mock_result

            inner = method._meth
            result = await inner(mock_flow, large_output)

        assert result == "done"

    @pytest.mark.asyncio
    async def test_listener_planning_with_llm(self):
        """Covers lines 665-677: planning in listener with planning_llm."""
        task = _make_task()
        crew_data = MagicMock()
        crew_data.memory = None
        crew_data.process = None
        crew_data.verbose = None
        crew_data.planning = True
        crew_data.planning_llm = "claude-3"
        crew_data.reasoning = False

        method = FlowMethodFactory.create_listener_method(
            method_name="listener_0",
            listener_tasks=[task],
            method_condition="starting_point_0",
            condition_type="NONE",
            callbacks={"job_id": "job-p"},
            group_context=None,
            create_execution_callbacks=_make_create_callbacks(),
            crew_data=crew_data,
        )

        mock_flow = _make_flow_instance()

        with patch("src.engines.crewai.paths.flow.modules.flow_methods.Crew") as MockCrew, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.asyncio.wait_for") as mock_wait, \
             patch("src.core.llm_manager.LLMManager") as MockLLM, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.Task") as MockTask:
            mock_llm = MagicMock()
            MockLLM.get_llm = AsyncMock(return_value=mock_llm)
            crew_kwargs_captured = {}

            def capture_kwargs(**kwargs):
                crew_kwargs_captured.update(kwargs)
                return MagicMock()

            MockCrew.side_effect = capture_kwargs
            MockTask.return_value = MagicMock(agent=task.agent, expected_output="out")
            mock_result = MagicMock()
            mock_result.raw = "res"
            mock_wait.return_value = mock_result

            inner = method._meth
            await inner(mock_flow)

        assert crew_kwargs_captured.get("planning") is True

    @pytest.mark.asyncio
    async def test_listener_reasoning(self):
        """Covers lines 683-691: reasoning for listener crew."""
        task = _make_task()
        task.agent.reasoning = False
        crew_data = MagicMock()
        crew_data.memory = None
        crew_data.process = None
        crew_data.verbose = None
        crew_data.planning = False
        crew_data.reasoning = True

        method = FlowMethodFactory.create_listener_method(
            method_name="listener_0",
            listener_tasks=[task],
            method_condition="starting_point_0",
            condition_type="NONE",
            callbacks={},
            group_context=None,
            create_execution_callbacks=_make_create_callbacks(),
            crew_data=crew_data,
        )

        mock_flow = _make_flow_instance()

        with patch("src.engines.crewai.paths.flow.modules.flow_methods.Crew") as MockCrew, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.asyncio.wait_for") as mock_wait, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.Task") as MockTask:
            crew_kwargs_captured = {}

            def capture_kwargs(**kwargs):
                crew_kwargs_captured.update(kwargs)
                return MagicMock()

            MockCrew.side_effect = capture_kwargs
            MockTask.return_value = MagicMock(agent=task.agent, expected_output="out")
            mock_result = MagicMock()
            mock_result.raw = "res"
            mock_wait.return_value = mock_result

            inner = method._meth
            await inner(mock_flow)

        assert crew_kwargs_captured.get("reasoning") is True
        assert task.agent.reasoning is True

    @pytest.mark.asyncio
    async def test_listener_json_tool_injection(self):
        """Covers lines 806-844: JSON previous output injected into tool _default_config."""
        task = _make_task()

        # Tool with _default_config
        mock_tool = MagicMock()
        mock_tool._default_config = {"join_key_map": None, "config_json": ""}

        task.agent.tools = [mock_tool]

        method = FlowMethodFactory.create_listener_method(
            method_name="listener_0",
            listener_tasks=[task],
            method_condition="starting_point_0",
            condition_type="NONE",
            callbacks={},
            group_context=None,
            create_execution_callbacks=_make_create_callbacks(),
        )

        mock_flow = _make_flow_instance()

        # Pipeline config JSON
        import json
        pipeline_config = json.dumps({
            "join_key_map": {"key": "val"},
            "enrichment_joins": [],
            "filter_sets": [],
            "measure_resolutions": {}
        })

        with patch("src.engines.crewai.paths.flow.modules.flow_methods.Crew") as MockCrew, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.asyncio.wait_for") as mock_wait, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.Task") as MockTask:
            crew_inst = MagicMock()
            MockCrew.return_value = crew_inst
            MockTask.return_value = MagicMock(agent=task.agent, expected_output="out")
            mock_result = MagicMock()
            mock_result.raw = "done"
            mock_wait.return_value = mock_result

            inner = method._meth
            result = await inner(mock_flow, pipeline_config)

        # config_json should have been injected
        assert mock_tool._default_config.get("config_json") == pipeline_config

    @pytest.mark.asyncio
    async def test_listener_state_lookup_for_injection(self):
        """Covers lines 778-803: listener checks state for latest output."""
        task = _make_task()
        method = FlowMethodFactory.create_listener_method(
            method_name="listener_0",
            listener_tasks=[task],
            method_condition="starting_point_0",
            condition_type="NONE",
            callbacks={},
            group_context=None,
            create_execution_callbacks=_make_create_callbacks(),
        )

        # State has a listener output that is larger than results
        state = {
            "listener_0": "newer and longer output from flow state " * 10
        }
        mock_flow = _make_flow_instance(state=state)

        with patch("src.engines.crewai.paths.flow.modules.flow_methods.Crew") as MockCrew, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.asyncio.wait_for") as mock_wait, \
             patch("src.engines.crewai.paths.flow.modules.flow_methods.Task") as MockTask:
            crew_inst = MagicMock()
            MockCrew.return_value = crew_inst
            MockTask.return_value = MagicMock(agent=task.agent, expected_output="out")
            mock_result = MagicMock()
            mock_result.raw = "done"
            mock_wait.return_value = mock_result

            inner = method._meth
            result = await inner(mock_flow, "short result")

        assert result == "done"
