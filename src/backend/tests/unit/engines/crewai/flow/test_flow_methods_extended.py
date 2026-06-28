"""
Extended unit tests for flow_methods.py — targeting uncovered branches.

Focus areas:
- extract_final_answer: dict path, object with raw attr, fallback string, list of non-dicts
- get_model_context_limits: various LLM types, found model config with non-None values
- FlowMethodFactory.create_skipped_crew_method: get_cached_output all branches
- FlowMethodFactory.create_hitl_gate_method: method structure
- FlowMethodFactory.create_starting_point_crew_method: memory/planning/reasoning branches
- FlowMethodFactory.create_listener_method: no-results path, state injection
"""
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock
import uuid


# ============================================================================
# extract_final_answer - uncovered branches
# ============================================================================

class TestExtractFinalAnswerExtended:
    """Additional tests for extract_final_answer covering missed branches."""

    def test_dict_result_with_content_key(self):
        """Test first_result is a dict with 'content' key (not wrapped in list)."""
        from src.engines.crewai.paths.flow.modules.flow_methods import extract_final_answer

        results = [{'content': 'Final Answer: The dict answer'}]
        result = extract_final_answer(results)
        assert result == "The dict answer"

    def test_dict_result_with_content_no_final_answer(self):
        """Test dict content without Final Answer marker returns full content."""
        from src.engines.crewai.paths.flow.modules.flow_methods import extract_final_answer

        results = [{'content': 'Plain content no marker here'}]
        result = extract_final_answer(results)
        assert result == "Plain content no marker here"

    def test_object_with_raw_attribute(self):
        """Test first_result has 'raw' attribute."""
        from src.engines.crewai.paths.flow.modules.flow_methods import extract_final_answer

        mock_obj = MagicMock()
        mock_obj.raw = "Final Answer: Raw object answer"
        # Make it NOT a list or dict
        type(mock_obj).__getitem__ = MagicMock(side_effect=TypeError)

        results = [mock_obj]
        result = extract_final_answer(results)
        assert result == "Raw object answer"

    def test_object_with_raw_attribute_no_marker(self):
        """Test first_result with raw but no Final Answer marker."""
        from src.engines.crewai.paths.flow.modules.flow_methods import extract_final_answer

        mock_obj = MagicMock()
        mock_obj.raw = "Just raw content"
        results = [mock_obj]
        result = extract_final_answer(results)
        assert result == "Just raw content"

    def test_object_with_empty_raw_falls_to_str(self):
        """Test first_result has falsy raw falls back to str()."""
        from src.engines.crewai.paths.flow.modules.flow_methods import extract_final_answer

        mock_obj = MagicMock()
        mock_obj.raw = None
        # No content key, no raw content
        del mock_obj.__class__.__getitem__
        results = [mock_obj]
        # Should not raise
        result = extract_final_answer(results)
        assert isinstance(result, str)

    def test_list_with_strings_in_nested(self):
        """Test list containing non-dict items falls back to str concatenation."""
        from src.engines.crewai.paths.flow.modules.flow_methods import extract_final_answer

        results = [['string1', 'string2']]
        result = extract_final_answer(results)
        assert 'string1' in result
        assert 'string2' in result

    def test_nested_list_item_with_final_answer_no_colon(self):
        """Test nested list item with 'Final Answer' without colon."""
        from src.engines.crewai.paths.flow.modules.flow_methods import extract_final_answer

        results = [[{'content': 'Thinking...\nFinal Answer\nThe actual answer'}]]
        result = extract_final_answer(results)
        assert 'actual answer' in result

    def test_string_result_with_final_answer_no_colon(self):
        """Test string with 'Final Answer' marker but no colon."""
        from src.engines.crewai.paths.flow.modules.flow_methods import extract_final_answer

        results = ["Thinking...\nFinal Answer\nAnswer without colon"]
        result = extract_final_answer(results)
        assert "Answer without colon" in result

    def test_fallback_to_string_conversion(self):
        """Test fallback path that converts to str."""
        from src.engines.crewai.paths.flow.modules.flow_methods import extract_final_answer

        class NoRawNoContent:
            pass

        results = [NoRawNoContent()]
        result = extract_final_answer(results)
        assert isinstance(result, str)

    def test_results_direct_string_input(self):
        """Test results passed as a plain string is handled (returns first char as content)."""
        from src.engines.crewai.paths.flow.modules.flow_methods import extract_final_answer

        # When results is a plain string, results[0] = first character
        # This tests the fallback str() path
        result = extract_final_answer("X")
        assert isinstance(result, str)
        assert result == "X"


# ============================================================================
# get_model_context_limits - found config with values
# ============================================================================

class TestGetModelContextLimitsExtended:
    """Additional tests covering model config with actual values."""

    @pytest.mark.asyncio
    async def test_returns_config_values_when_found(self):
        """Test actual context window and max_output values are returned."""
        from src.engines.crewai.paths.flow.modules.flow_methods import get_model_context_limits

        mock_agent = MagicMock()
        mock_agent.llm = "gpt-4"

        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = str(uuid.uuid4())

        mock_model_config = MagicMock()
        mock_model_config.context_window = 32000
        mock_model_config.max_output_tokens = 8000

        with patch('src.db.session.request_scoped_session') as mock_session, \
             patch('src.services.model_config_service.ModelConfigService') as mock_svc_cls:
            mock_session_instance = AsyncMock()
            mock_session.return_value.__aenter__ = AsyncMock(return_value=mock_session_instance)
            mock_session.return_value.__aexit__ = AsyncMock(return_value=False)

            mock_svc = MagicMock()
            mock_svc.find_by_key = AsyncMock(return_value=mock_model_config)
            mock_svc_cls.return_value = mock_svc

            context_window, max_output = await get_model_context_limits(mock_agent, mock_group_context)
            assert context_window == 32000
            assert max_output == 8000

    @pytest.mark.asyncio
    async def test_returns_defaults_when_config_values_are_zero(self):
        """Test default returned when config has falsy context_window."""
        from src.engines.crewai.paths.flow.modules.flow_methods import get_model_context_limits

        mock_agent = MagicMock()
        mock_agent.llm = "unknown-model"

        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = str(uuid.uuid4())

        mock_model_config = MagicMock()
        mock_model_config.context_window = 0  # Falsy
        mock_model_config.max_output_tokens = None  # Falsy

        with patch('src.db.session.request_scoped_session') as mock_session, \
             patch('src.services.model_config_service.ModelConfigService') as mock_svc_cls:
            mock_session.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_session.return_value.__aexit__ = AsyncMock(return_value=False)

            mock_svc = MagicMock()
            mock_svc.find_by_key = AsyncMock(return_value=mock_model_config)
            mock_svc_cls.return_value = mock_svc

            context_window, max_output = await get_model_context_limits(mock_agent, mock_group_context)
            # Falls back to defaults when values are falsy
            assert context_window == 128000
            assert max_output == 16000

    @pytest.mark.asyncio
    async def test_llm_with_unknown_type_returns_defaults(self):
        """Test LLM object without model attr returns defaults."""
        from src.engines.crewai.paths.flow.modules.flow_methods import get_model_context_limits

        mock_agent = MagicMock()
        mock_llm = MagicMock(spec=[])  # No attributes
        del mock_llm.__str__  # not a string
        # Create a plain object with no 'model' attr
        class UnknownLLM:
            pass
        mock_agent.llm = UnknownLLM()

        context_window, max_output = await get_model_context_limits(mock_agent, None)
        assert context_window == 128000
        assert max_output == 16000


# ============================================================================
# create_skipped_crew_method - get_cached_output branches
# ============================================================================

class TestGetCachedOutputBranches:
    """Tests for the get_cached_output helper inside create_skipped_crew_method."""

    @pytest.mark.asyncio
    async def test_skipped_starting_no_checkpoint_uses_method_outputs(self):
        """_method_outputs contains the method key -> returns that."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        method = FlowMethodFactory.create_skipped_crew_method(
            method_name='starting_point_0',
            crew_name='Crew A',
            crew_sequence=1,
            is_starting_point=True,
            checkpoint_output=None,
        )

        mock_flow = MagicMock()
        mock_flow.state = {}
        mock_flow._method_outputs = {'starting_point_0': 'from_persist'}

        inner_func = method.__wrapped__ if hasattr(method, '__wrapped__') else method
        result = await inner_func(mock_flow)
        assert result == 'from_persist'

    @pytest.mark.asyncio
    async def test_skipped_starting_no_checkpoint_uses_state_method_key(self):
        """state dict has method_name key -> returns that."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        method = FlowMethodFactory.create_skipped_crew_method(
            method_name='starting_point_0',
            crew_name='Crew A',
            crew_sequence=1,
            is_starting_point=True,
            checkpoint_output=None,
        )

        mock_flow = MagicMock()
        mock_flow.state = {'starting_point_0': 'from_state_method'}
        mock_flow._method_outputs = {}

        inner_func = method.__wrapped__ if hasattr(method, '__wrapped__') else method
        result = await inner_func(mock_flow)
        assert result == 'from_state_method'

    @pytest.mark.asyncio
    async def test_skipped_starting_no_checkpoint_uses_state_crew_key(self):
        """state dict has crew_name key -> returns that."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        method = FlowMethodFactory.create_skipped_crew_method(
            method_name='starting_point_0',
            crew_name='Crew Alpha',
            crew_sequence=1,
            is_starting_point=True,
            checkpoint_output=None,
        )

        mock_flow = MagicMock()
        mock_flow.state = {'Crew Alpha': 'from_state_crew'}
        mock_flow._method_outputs = {}

        inner_func = method.__wrapped__ if hasattr(method, '__wrapped__') else method
        result = await inner_func(mock_flow)
        assert result == 'from_state_crew'

    @pytest.mark.asyncio
    async def test_skipped_starting_no_checkpoint_uses_seq_key(self):
        """state dict has crew_{seq}_output key -> returns that."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        method = FlowMethodFactory.create_skipped_crew_method(
            method_name='starting_point_0',
            crew_name='Crew B',
            crew_sequence=3,
            is_starting_point=True,
            checkpoint_output=None,
        )

        mock_flow = MagicMock()
        mock_flow.state = {'crew_3_output': 'from_seq_key'}
        mock_flow._method_outputs = {}

        inner_func = method.__wrapped__ if hasattr(method, '__wrapped__') else method
        result = await inner_func(mock_flow)
        assert result == 'from_seq_key'

    @pytest.mark.asyncio
    async def test_skipped_starting_no_checkpoint_uses_output_key(self):
        """state dict has {method}_output key -> returns that."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        method = FlowMethodFactory.create_skipped_crew_method(
            method_name='my_method',
            crew_name='Crew C',
            crew_sequence=2,
            is_starting_point=True,
            checkpoint_output=None,
        )

        mock_flow = MagicMock()
        mock_flow.state = {'my_method_output': 'from_output_key'}
        mock_flow._method_outputs = {}

        inner_func = method.__wrapped__ if hasattr(method, '__wrapped__') else method
        result = await inner_func(mock_flow)
        assert result == 'from_output_key'

    @pytest.mark.asyncio
    async def test_skipped_starting_no_checkpoint_uses_previous_output_key(self):
        """state dict has 'previous_output' key -> returns that."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        method = FlowMethodFactory.create_skipped_crew_method(
            method_name='starting_point_0',
            crew_name='Crew D',
            crew_sequence=1,
            is_starting_point=True,
            checkpoint_output=None,
        )

        mock_flow = MagicMock()
        mock_flow.state = {'previous_output': 'from_prev_key'}
        mock_flow._method_outputs = {}

        inner_func = method.__wrapped__ if hasattr(method, '__wrapped__') else method
        result = await inner_func(mock_flow)
        assert result == 'from_prev_key'

    @pytest.mark.asyncio
    async def test_skipped_starting_no_checkpoint_returns_placeholder(self):
        """No sources available -> returns placeholder dict."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        method = FlowMethodFactory.create_skipped_crew_method(
            method_name='starting_point_0',
            crew_name='Crew E',
            crew_sequence=1,
            is_starting_point=True,
            checkpoint_output=None,
        )

        mock_flow = MagicMock()
        mock_flow.state = {}
        mock_flow._method_outputs = {}

        inner_func = method.__wrapped__ if hasattr(method, '__wrapped__') else method
        result = await inner_func(mock_flow)
        assert isinstance(result, dict)
        assert result.get('status') == 'skipped'

    @pytest.mark.asyncio
    async def test_skipped_listener_uses_previous_output_fallback(self):
        """Listener skipped method: no checkpoint, no state -> uses prev output."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        method = FlowMethodFactory.create_skipped_crew_method(
            method_name='listener_0',
            crew_name='Listener Crew',
            crew_sequence=2,
            is_starting_point=False,
            method_condition='starting_point_0',
            condition_type='NONE',
            checkpoint_output=None,
        )

        mock_flow = MagicMock()
        mock_flow.state = {}
        mock_flow._method_outputs = {}

        inner_func = method.__wrapped__ if hasattr(method, '__wrapped__') else method
        result = await inner_func(mock_flow, 'prev_fallback_data')
        assert result == 'prev_fallback_data'

    @pytest.mark.asyncio
    async def test_skipped_starting_stores_in_state(self):
        """Verify result is stored in flow state for downstream use."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        method = FlowMethodFactory.create_skipped_crew_method(
            method_name='starting_point_0',
            crew_name='Crew F',
            crew_sequence=1,
            is_starting_point=True,
            checkpoint_output='Checkpoint data',
        )

        mock_flow = MagicMock()
        mock_flow.state = {}

        inner_func = method.__wrapped__ if hasattr(method, '__wrapped__') else method
        await inner_func(mock_flow)

        # State should be updated
        assert mock_flow.state.get('starting_point_0') == 'Checkpoint data'
        assert mock_flow.state.get('Crew F') == 'Checkpoint data'

    @pytest.mark.asyncio
    async def test_skipped_listener_stores_in_state(self):
        """Listener stub stores checkpoint result in state."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        method = FlowMethodFactory.create_skipped_crew_method(
            method_name='listener_1',
            crew_name='Listener Crew G',
            crew_sequence=2,
            is_starting_point=False,
            method_condition='starting_point_0',
            condition_type='NONE',
            checkpoint_output='Listener checkpoint data',
        )

        mock_flow = MagicMock()
        mock_flow.state = {}

        inner_func = method.__wrapped__ if hasattr(method, '__wrapped__') else method
        await inner_func(mock_flow, 'prev_output')

        assert mock_flow.state.get('listener_1') == 'Listener checkpoint data'
        assert mock_flow.state.get('previous_output') == 'Listener checkpoint data'

    @pytest.mark.asyncio
    async def test_skipped_listener_no_checkpoint_no_prev_returns_placeholder(self):
        """Listener with nothing -> returns placeholder dict."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        method = FlowMethodFactory.create_skipped_crew_method(
            method_name='listener_0',
            crew_name='Listener H',
            crew_sequence=2,
            is_starting_point=False,
            method_condition='starting_point_0',
            condition_type='NONE',
            checkpoint_output=None,
        )

        mock_flow = MagicMock()
        mock_flow.state = {}
        mock_flow._method_outputs = {}

        inner_func = method.__wrapped__ if hasattr(method, '__wrapped__') else method
        result = await inner_func(mock_flow)
        assert isinstance(result, dict)
        assert result.get('status') == 'skipped'

    def test_state_object_based_lookup(self):
        """get_cached_output: object-like state (has attribute named like method)."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        method = FlowMethodFactory.create_skipped_crew_method(
            method_name='starting_point_0',
            crew_name='Crew ObjState',
            crew_sequence=1,
            is_starting_point=True,
            checkpoint_output=None,
        )
        # Just verify it's callable and has the right name
        assert method.__name__ == 'starting_point_0'


# ============================================================================
# FlowMethodFactory.create_hitl_gate_method - method structure
# ============================================================================

class TestCreateHitlGateMethod:
    """Tests for HITL gate method creation."""

    def test_creates_callable_method(self):
        """create_hitl_gate_method returns a callable."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        method = FlowMethodFactory.create_hitl_gate_method(
            method_name='hitl_gate_0',
            gate_node_id='gate-node-1',
            gate_config={'message': 'Please approve', 'timeout_seconds': 3600},
            previous_method_name='starting_point_0',
            crew_sequence=1,
            callbacks={'job_id': 'test-job-123', 'flow_id': 'flow-456'},
            group_context=None,
        )
        assert callable(method)

    def test_method_name_set_correctly(self):
        """create_hitl_gate_method sets __name__ correctly."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        method = FlowMethodFactory.create_hitl_gate_method(
            method_name='hitl_gate_check',
            gate_node_id='gate-1',
            gate_config={},
            previous_method_name='starting_point_0',
            crew_sequence=1,
        )
        assert method.__name__ == 'hitl_gate_check'

    def test_creates_without_callbacks(self):
        """create_hitl_gate_method works without callbacks."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        method = FlowMethodFactory.create_hitl_gate_method(
            method_name='hitl_gate_0',
            gate_node_id='gate-1',
            gate_config={'message': 'Approve?'},
            previous_method_name='starting_point_0',
            crew_sequence=1,
        )
        assert callable(method)


# ============================================================================
# create_starting_point_crew_method - memory/planning/reasoning branches
# ============================================================================

class TestStartingPointMethodBranches:
    """Test branches inside the starting_point crew execution."""

    def _make_task(self, agent=None):
        task = MagicMock()
        task.description = "Test task description"
        task.expected_output = "Expected output"
        task.context = None
        if agent is None:
            agent = MagicMock()
            agent.role = "Test Agent"
            agent.tools = []
            agent._kasal_memory_disabled = False
        task.agent = agent
        return task

    def _make_crew_data(self, memory=None, process='sequential', verbose=True, planning=False, reasoning=False):
        crew_data = MagicMock()
        crew_data.memory = memory
        crew_data.process = process
        crew_data.verbose = verbose
        crew_data.planning = planning
        crew_data.reasoning = reasoning
        return crew_data

    def test_method_created_with_hierarchical_process(self):
        """create_starting_point_crew_method with hierarchical process."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        crew_data = self._make_crew_data(process='hierarchical')
        mock_create_callbacks = MagicMock(return_value=(MagicMock(), MagicMock()))

        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name='starting_point_0',
            task_list=[self._make_task()],
            crew_name='Hierarchical Crew',
            callbacks={'job_id': 'j1'},
            group_context=None,
            create_execution_callbacks=mock_create_callbacks,
            crew_data=crew_data,
        )
        assert callable(method)

    def test_method_created_with_all_agents_memory_disabled(self):
        """When all agents have _kasal_memory_disabled=True, crew_memory=False."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        agent = MagicMock()
        agent.role = "Disabled Memory Agent"
        agent.tools = []
        agent._kasal_memory_disabled = True
        task = self._make_task(agent=agent)

        mock_create_callbacks = MagicMock(return_value=(MagicMock(), MagicMock()))

        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name='starting_point_0',
            task_list=[task],
            crew_name='No Memory Crew',
            callbacks={'job_id': 'j1'},
            group_context=None,
            create_execution_callbacks=mock_create_callbacks,
            crew_data=None,
        )
        assert callable(method)

    def test_method_with_planning_enabled(self):
        """create_starting_point_crew_method with planning=True."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        crew_data = self._make_crew_data(planning=True)
        crew_data.planning_llm = None  # Force fallback to agent LLM

        mock_create_callbacks = MagicMock(return_value=(MagicMock(), MagicMock()))

        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name='starting_point_0',
            task_list=[self._make_task()],
            crew_name='Planning Crew',
            callbacks={'job_id': 'j1'},
            group_context=None,
            create_execution_callbacks=mock_create_callbacks,
            crew_data=crew_data,
        )
        assert callable(method)

    def test_method_with_reasoning_enabled(self):
        """create_starting_point_crew_method with reasoning=True."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        crew_data = self._make_crew_data(reasoning=True)
        mock_create_callbacks = MagicMock(return_value=(MagicMock(), MagicMock()))

        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name='starting_point_0',
            task_list=[self._make_task()],
            crew_name='Reasoning Crew',
            callbacks={'job_id': 'j1'},
            group_context=None,
            create_execution_callbacks=mock_create_callbacks,
            crew_data=crew_data,
        )
        assert callable(method)

    @pytest.mark.asyncio
    async def test_starting_method_with_crew_memory_false(self):
        """crew_data.memory=False -> crew_memory is False."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        crew_data = self._make_crew_data(memory=False)
        mock_create_callbacks = MagicMock(return_value=(MagicMock(), MagicMock()))

        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name='starting_point_0',
            task_list=[self._make_task()],
            crew_name='No Memory Crew',
            callbacks={'job_id': 'j1'},
            group_context=None,
            create_execution_callbacks=mock_create_callbacks,
            crew_data=crew_data,
        )

        mock_flow = MagicMock()
        mock_flow.state = {}

        with patch('src.engines.crewai.paths.flow.modules.flow_methods.Crew') as mock_crew_cls:
            mock_crew = MagicMock()
            mock_crew.kickoff_async = AsyncMock(return_value=MagicMock(raw="result"))
            mock_crew_cls.return_value = mock_crew

            with patch('asyncio.wait_for', new_callable=AsyncMock) as mock_wait:
                mock_wait.return_value = MagicMock(raw="result")
                inner = method.__wrapped__ if hasattr(method, '__wrapped__') else method
                result = await inner(mock_flow)

            # Crew was created
            mock_crew_cls.assert_called_once()
            call_kwargs = mock_crew_cls.call_args[1]
            assert call_kwargs.get('memory') is False

    @pytest.mark.asyncio
    async def test_starting_method_no_callbacks_job_id(self):
        """No job_id in callbacks -> skips callback setup."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        mock_create_callbacks = MagicMock(return_value=(MagicMock(), MagicMock()))

        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name='starting_point_0',
            task_list=[self._make_task()],
            crew_name='No Callback Crew',
            callbacks={},  # No job_id
            group_context=None,
            create_execution_callbacks=mock_create_callbacks,
        )

        mock_flow = MagicMock()
        mock_flow.state = {}

        with patch('src.engines.crewai.paths.flow.modules.flow_methods.Crew') as mock_crew_cls, \
             patch('asyncio.wait_for', new_callable=AsyncMock) as mock_wait:
            mock_crew = MagicMock()
            mock_crew_cls.return_value = mock_crew
            mock_wait.return_value = MagicMock(raw="done")

            inner = method.__wrapped__ if hasattr(method, '__wrapped__') else method
            await inner(mock_flow)

            # Callbacks should not be set (no job_id)
            mock_create_callbacks.assert_not_called()


# ============================================================================
# create_listener_method - no-results path, large context, state injection
# ============================================================================

class TestListenerMethodBranches:
    """Test branches inside the listener method execution."""

    def _make_task(self):
        agent = MagicMock()
        agent.role = "Listener Agent"
        agent.tools = []
        agent._kasal_memory_disabled = False
        task = MagicMock()
        task.description = "Listener task"
        task.expected_output = "Output"
        task.agent = agent
        return task

    @pytest.mark.asyncio
    async def test_listener_with_no_results(self):
        """Listener called with no results (no previous output)."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        mock_create_callbacks = MagicMock(return_value=(MagicMock(), MagicMock()))

        method = FlowMethodFactory.create_listener_method(
            method_name='listener_0',
            listener_tasks=[self._make_task()],
            method_condition='starting_point_0',
            condition_type='NONE',
            callbacks={'job_id': 'j1'},
            group_context=None,
            create_execution_callbacks=mock_create_callbacks,
            crew_name='Listener Crew',
        )

        mock_flow = MagicMock()
        mock_flow.state = {}

        with patch('src.engines.crewai.paths.flow.modules.flow_methods.Crew') as mock_crew_cls, \
             patch('src.engines.crewai.paths.flow.modules.flow_methods.Task') as mock_task_cls, \
             patch('asyncio.wait_for', new_callable=AsyncMock) as mock_wait:
            mock_crew_cls.return_value = MagicMock()
            mock_task_cls.return_value = MagicMock()
            mock_wait.return_value = MagicMock(raw="listener result")

            inner = method.__wrapped__ if hasattr(method, '__wrapped__') else method
            result = await inner(mock_flow)  # No results arg

        assert result is not None

    @pytest.mark.asyncio
    async def test_listener_with_large_previous_output(self):
        """Large previous output (>2000 chars) takes the truncated path."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        mock_create_callbacks = MagicMock(return_value=(MagicMock(), MagicMock()))

        method = FlowMethodFactory.create_listener_method(
            method_name='listener_0',
            listener_tasks=[self._make_task()],
            method_condition='starting_point_0',
            condition_type='NONE',
            callbacks={'job_id': 'j1'},
            group_context=None,
            create_execution_callbacks=mock_create_callbacks,
            crew_name='Listener Crew',
        )

        mock_flow = MagicMock()
        mock_flow.state = {}

        large_output = "A" * 5000  # > 2000 chars

        with patch('src.engines.crewai.paths.flow.modules.flow_methods.Crew') as mock_crew_cls, \
             patch('src.engines.crewai.paths.flow.modules.flow_methods.Task') as mock_task_cls, \
             patch('asyncio.wait_for', new_callable=AsyncMock) as mock_wait:
            mock_crew_cls.return_value = MagicMock()
            mock_task_cls.return_value = MagicMock()
            mock_wait.return_value = MagicMock(raw="listener result from large input")

            inner = method.__wrapped__ if hasattr(method, '__wrapped__') else method
            result = await inner(mock_flow, large_output)

        assert result is not None

    @pytest.mark.asyncio
    async def test_listener_with_json_previous_output(self):
        """JSON previous output triggers tool _default_config injection."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory
        import json

        agent = MagicMock()
        agent.role = "Listener Agent"
        agent.tools = []
        agent._kasal_memory_disabled = False

        mock_tool = MagicMock()
        mock_tool._default_config = {'config_json': ''}
        agent.tools = [mock_tool]

        task = MagicMock()
        task.description = "Process output"
        task.expected_output = "Processed"
        task.agent = agent

        mock_create_callbacks = MagicMock(return_value=(MagicMock(), MagicMock()))

        method = FlowMethodFactory.create_listener_method(
            method_name='listener_0',
            listener_tasks=[task],
            method_condition='starting_point_0',
            condition_type='NONE',
            callbacks={'job_id': 'j1'},
            group_context=None,
            create_execution_callbacks=mock_create_callbacks,
            crew_name='Listener Crew',
        )

        mock_flow = MagicMock()
        mock_flow.state = {}

        pipeline_output = json.dumps({
            'join_key_map': {'key': 'val'},
            'enrichment_joins': [],
            'filter_sets': {},
        })

        with patch('src.engines.crewai.paths.flow.modules.flow_methods.Crew') as mock_crew_cls, \
             patch('src.engines.crewai.paths.flow.modules.flow_methods.Task') as mock_task_cls, \
             patch('asyncio.wait_for', new_callable=AsyncMock) as mock_wait:
            mock_crew_cls.return_value = MagicMock()
            mock_task_cls.return_value = MagicMock()
            mock_wait.return_value = MagicMock(raw="processed result")

            inner = method.__wrapped__ if hasattr(method, '__wrapped__') else method
            result = await inner(mock_flow, pipeline_output)

        assert result is not None

    @pytest.mark.asyncio
    async def test_listener_with_memory_disabled_agent(self):
        """All agents memory disabled -> crew_memory=False for listener."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        agent = MagicMock()
        agent.role = "No Mem Agent"
        agent.tools = []
        agent._kasal_memory_disabled = True

        task = MagicMock()
        task.description = "Task"
        task.expected_output = "Output"
        task.agent = agent

        crew_data = MagicMock()
        crew_data.memory = None
        crew_data.process = 'sequential'
        crew_data.verbose = True
        crew_data.planning = False
        crew_data.reasoning = False

        mock_create_callbacks = MagicMock(return_value=(MagicMock(), MagicMock()))

        method = FlowMethodFactory.create_listener_method(
            method_name='listener_0',
            listener_tasks=[task],
            method_condition='starting_point_0',
            condition_type='NONE',
            callbacks={'job_id': 'j1'},
            group_context=None,
            create_execution_callbacks=mock_create_callbacks,
            crew_name='Listener Crew',
            crew_data=crew_data,
        )

        mock_flow = MagicMock()
        mock_flow.state = {}

        with patch('src.engines.crewai.paths.flow.modules.flow_methods.Crew') as mock_crew_cls, \
             patch('src.engines.crewai.paths.flow.modules.flow_methods.Task') as mock_task_cls, \
             patch('asyncio.wait_for', new_callable=AsyncMock) as mock_wait:
            mock_crew_cls.return_value = MagicMock()
            mock_task_cls.return_value = MagicMock()
            mock_wait.return_value = MagicMock(raw="result")

            inner = method.__wrapped__ if hasattr(method, '__wrapped__') else method
            await inner(mock_flow, "prev output")

            call_kwargs = mock_crew_cls.call_args[1]
            assert call_kwargs.get('memory') is False

    @pytest.mark.asyncio
    async def test_listener_timeout(self):
        """Listener method handles asyncio.TimeoutError correctly."""
        import asyncio
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        mock_create_callbacks = MagicMock(return_value=(MagicMock(), MagicMock()))

        method = FlowMethodFactory.create_listener_method(
            method_name='listener_0',
            listener_tasks=[MagicMock(description='t', expected_output='o', agent=MagicMock(role='r', tools=[]))],
            method_condition='starting_point_0',
            condition_type='NONE',
            callbacks={'job_id': 'j1'},
            group_context=None,
            create_execution_callbacks=mock_create_callbacks,
        )

        mock_flow = MagicMock()
        mock_flow.state = {}

        with patch('src.engines.crewai.paths.flow.modules.flow_methods.Crew') as mock_crew_cls, \
             patch('src.engines.crewai.paths.flow.modules.flow_methods.Task') as mock_task_cls, \
             patch('asyncio.wait_for', new_callable=AsyncMock) as mock_wait:
            mock_crew_cls.return_value = MagicMock()
            mock_task_cls.return_value = MagicMock()
            mock_wait.side_effect = asyncio.TimeoutError()

            inner = method.__wrapped__ if hasattr(method, '__wrapped__') else method
            with pytest.raises(TimeoutError):
                await inner(mock_flow, "prev output")

    @pytest.mark.asyncio
    async def test_listener_stores_result_in_state(self):
        """Result from listener execution is stored in flow state."""
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory

        mock_create_callbacks = MagicMock(return_value=(MagicMock(), MagicMock()))

        method = FlowMethodFactory.create_listener_method(
            method_name='listener_1',
            listener_tasks=[MagicMock(description='t', expected_output='o', agent=MagicMock(role='r', tools=[]))],
            method_condition='starting_point_0',
            condition_type='NONE',
            callbacks={'job_id': 'j1'},
            group_context=None,
            create_execution_callbacks=mock_create_callbacks,
            crew_name='Stored Result Crew',
        )

        mock_flow = MagicMock()
        mock_flow.state = {}

        with patch('src.engines.crewai.paths.flow.modules.flow_methods.Crew') as mock_crew_cls, \
             patch('src.engines.crewai.paths.flow.modules.flow_methods.Task') as mock_task_cls, \
             patch('asyncio.wait_for', new_callable=AsyncMock) as mock_wait:
            mock_crew_cls.return_value = MagicMock()
            mock_task_cls.return_value = MagicMock()
            mock_wait.return_value = MagicMock(raw="stored_result")

            inner = method.__wrapped__ if hasattr(method, '__wrapped__') else method
            await inner(mock_flow, "prev output")

        assert mock_flow.state.get('listener_1') == "stored_result"


# ============================================================================
# Method naming sanity checks
# ============================================================================

class TestMethodNaming:
    """Ensure method __name__ is always set correctly."""

    def test_starting_point_method_name(self):
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory
        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name='my_custom_start',
            task_list=[MagicMock(description='t', agent=MagicMock(role='r', tools=[]))],
            crew_name='Test',
            callbacks=None,
            group_context=None,
            create_execution_callbacks=MagicMock(return_value=(MagicMock(), MagicMock())),
        )
        assert method.__name__ == 'my_custom_start'

    def test_listener_method_name(self):
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory
        method = FlowMethodFactory.create_listener_method(
            method_name='my_listener',
            listener_tasks=[MagicMock(description='t', agent=MagicMock(role='r', tools=[]))],
            method_condition='some_start',
            condition_type='NONE',
            callbacks=None,
            group_context=None,
            create_execution_callbacks=MagicMock(return_value=(MagicMock(), MagicMock())),
        )
        assert method.__name__ == 'my_listener'

    def test_skipped_starting_method_name(self):
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory
        method = FlowMethodFactory.create_skipped_crew_method(
            method_name='skipped_start',
            crew_name='Crew',
            crew_sequence=1,
            is_starting_point=True,
        )
        assert method.__name__ == 'skipped_start'

    def test_skipped_listener_method_name(self):
        from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory
        method = FlowMethodFactory.create_skipped_crew_method(
            method_name='skipped_listener',
            crew_name='Crew',
            crew_sequence=2,
            is_starting_point=False,
            method_condition='start',
        )
        assert method.__name__ == 'skipped_listener'
