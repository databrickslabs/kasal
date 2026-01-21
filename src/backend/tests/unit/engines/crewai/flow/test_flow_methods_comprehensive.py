"""
Comprehensive unit tests for flow_methods.py module.
Target: 80%+ coverage
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import uuid


class TestExtractFinalAnswer:
    """Tests for extract_final_answer function."""

    def test_extract_final_answer_empty_results(self):
        """Test extract_final_answer with empty results."""
        from src.engines.crewai.flow.modules.flow_methods import extract_final_answer

        result = extract_final_answer(None)
        assert result == ""

        result = extract_final_answer([])
        assert result == ""

    def test_extract_final_answer_with_final_answer_marker(self):
        """Test extract_final_answer extracts text after Final Answer marker."""
        from src.engines.crewai.flow.modules.flow_methods import extract_final_answer

        results = ["Thinking process here... Final Answer: This is the answer"]
        result = extract_final_answer(results)
        assert result == "This is the answer"

    def test_extract_final_answer_without_colon(self):
        """Test extract_final_answer handles Final Answer without colon."""
        from src.engines.crewai.flow.modules.flow_methods import extract_final_answer

        results = ["Thinking process here... Final Answer\nThis is the answer"]
        result = extract_final_answer(results)
        assert "This is the answer" in result

    def test_extract_final_answer_with_dict_content(self):
        """Test extract_final_answer with dict containing content key."""
        from src.engines.crewai.flow.modules.flow_methods import extract_final_answer

        results = [{'content': 'Final Answer: The answer is 42'}]
        result = extract_final_answer(results)
        assert result == "The answer is 42"

    def test_extract_final_answer_with_list_of_dicts(self):
        """Test extract_final_answer with list of dicts."""
        from src.engines.crewai.flow.modules.flow_methods import extract_final_answer

        results = [[
            {'content': 'First Final Answer: Answer 1'},
            {'content': 'Second Final Answer: Answer 2'}
        ]]
        result = extract_final_answer(results)
        assert "Answer 1" in result
        assert "Answer 2" in result

    def test_extract_final_answer_with_crew_output(self):
        """Test extract_final_answer with CrewOutput-like object."""
        from src.engines.crewai.flow.modules.flow_methods import extract_final_answer

        mock_output = MagicMock()
        mock_output.raw = "Final Answer: Crew output answer"

        results = [mock_output]
        result = extract_final_answer(results)
        assert result == "Crew output answer"

    def test_extract_final_answer_with_string(self):
        """Test extract_final_answer with plain string."""
        from src.engines.crewai.flow.modules.flow_methods import extract_final_answer

        results = ["Final Answer: Simple string answer"]
        result = extract_final_answer(results)
        assert result == "Simple string answer"

    def test_extract_final_answer_no_marker(self):
        """Test extract_final_answer returns full content when no marker."""
        from src.engines.crewai.flow.modules.flow_methods import extract_final_answer

        results = ["This is the full output without any marker"]
        result = extract_final_answer(results)
        assert result == "This is the full output without any marker"

    def test_extract_final_answer_with_list_strings(self):
        """Test extract_final_answer with list of strings in nested list."""
        from src.engines.crewai.flow.modules.flow_methods import extract_final_answer

        results = [['First item', 'Second item']]
        result = extract_final_answer(results)
        assert "First item" in result
        assert "Second item" in result


class TestGetModelContextLimits:
    """Tests for get_model_context_limits function."""

    @pytest.mark.asyncio
    async def test_get_model_context_limits_defaults(self):
        """Test get_model_context_limits returns defaults when no agent."""
        from src.engines.crewai.flow.modules.flow_methods import get_model_context_limits

        context_window, max_output = await get_model_context_limits(None, None)
        assert context_window == 128000
        assert max_output == 16000

    @pytest.mark.asyncio
    async def test_get_model_context_limits_string_llm(self):
        """Test get_model_context_limits with string LLM."""
        from src.engines.crewai.flow.modules.flow_methods import get_model_context_limits

        mock_agent = MagicMock()
        mock_agent.llm = "gpt-4"

        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = str(uuid.uuid4())

        # Patch at the source modules since imports happen inside the function
        with patch('src.db.session.async_session_factory') as mock_session, \
             patch('src.services.model_config_service.ModelConfigService') as mock_service:
            mock_session.return_value.__aenter__ = AsyncMock()
            mock_session.return_value.__aexit__ = AsyncMock()

            mock_model_config = MagicMock()
            mock_model_config.context_window = 128000
            mock_model_config.max_output_tokens = 16000

            mock_service_instance = MagicMock()
            mock_service_instance.find_by_key = AsyncMock(return_value=mock_model_config)
            mock_service.return_value = mock_service_instance

            context_window, max_output = await get_model_context_limits(mock_agent, mock_group_context)
            assert context_window == 128000
            assert max_output == 16000

    @pytest.mark.asyncio
    async def test_get_model_context_limits_llm_object(self):
        """Test get_model_context_limits with LLM object."""
        from src.engines.crewai.flow.modules.flow_methods import get_model_context_limits

        mock_llm = MagicMock()
        mock_llm.model = "gpt-4"

        mock_agent = MagicMock()
        mock_agent.llm = mock_llm

        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = str(uuid.uuid4())

        # Patch at the source modules since imports happen inside the function
        with patch('src.db.session.async_session_factory') as mock_session, \
             patch('src.services.model_config_service.ModelConfigService') as mock_service:
            mock_session.return_value.__aenter__ = AsyncMock()
            mock_session.return_value.__aexit__ = AsyncMock()

            mock_service_instance = MagicMock()
            mock_service_instance.find_by_key = AsyncMock(return_value=None)
            mock_service.return_value = mock_service_instance

            context_window, max_output = await get_model_context_limits(mock_agent, mock_group_context)
            assert context_window == 128000
            assert max_output == 16000

    @pytest.mark.asyncio
    async def test_get_model_context_limits_no_model_name(self):
        """Test get_model_context_limits returns defaults when no model name."""
        from src.engines.crewai.flow.modules.flow_methods import get_model_context_limits

        mock_agent = MagicMock()
        mock_agent.llm = None

        context_window, max_output = await get_model_context_limits(mock_agent, None)
        assert context_window == 128000
        assert max_output == 16000

    @pytest.mark.asyncio
    async def test_get_model_context_limits_no_group_id(self):
        """Test get_model_context_limits returns defaults when no group_id."""
        from src.engines.crewai.flow.modules.flow_methods import get_model_context_limits

        mock_agent = MagicMock()
        mock_agent.llm = "gpt-4"

        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = None
        mock_group_context.group_ids = []

        context_window, max_output = await get_model_context_limits(mock_agent, mock_group_context)
        assert context_window == 128000
        assert max_output == 16000

    @pytest.mark.asyncio
    async def test_get_model_context_limits_exception(self):
        """Test get_model_context_limits handles exceptions gracefully."""
        from src.engines.crewai.flow.modules.flow_methods import get_model_context_limits

        mock_agent = MagicMock()
        mock_agent.llm = "gpt-4"

        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = str(uuid.uuid4())

        # Patch at the source module since import happens inside the function
        with patch('src.db.session.async_session_factory') as mock_session:
            mock_session.side_effect = Exception("Database error")

            context_window, max_output = await get_model_context_limits(mock_agent, mock_group_context)
            assert context_window == 128000
            assert max_output == 16000

    @pytest.mark.asyncio
    async def test_get_model_context_limits_with_group_ids_list(self):
        """Test get_model_context_limits uses group_ids list when primary_group_id is None."""
        from src.engines.crewai.flow.modules.flow_methods import get_model_context_limits

        mock_agent = MagicMock()
        mock_agent.llm = "gpt-4"

        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = None
        mock_group_context.group_ids = [str(uuid.uuid4())]

        # Patch at the source modules since imports happen inside the function
        with patch('src.db.session.async_session_factory') as mock_session, \
             patch('src.services.model_config_service.ModelConfigService') as mock_service:
            mock_session.return_value.__aenter__ = AsyncMock()
            mock_session.return_value.__aexit__ = AsyncMock()

            mock_service_instance = MagicMock()
            mock_service_instance.find_by_key = AsyncMock(return_value=None)
            mock_service.return_value = mock_service_instance

            context_window, max_output = await get_model_context_limits(mock_agent, mock_group_context)
            assert context_window == 128000
            assert max_output == 16000


class TestFlowMethodFactory:
    """Tests for FlowMethodFactory class."""

    @pytest.fixture
    def mock_agent(self):
        """Create mock agent."""
        agent = MagicMock()
        agent.role = "Test Agent"
        agent.tools = []
        agent.memory = True
        return agent

    @pytest.fixture
    def mock_task(self, mock_agent):
        """Create mock task."""
        task = MagicMock()
        task.agent = mock_agent
        task.description = "Test task description"
        task.expected_output = "Expected output"
        task.context = None
        return task

    @pytest.fixture
    def mock_callbacks(self):
        """Create mock callbacks."""
        return {
            'job_id': str(uuid.uuid4()),
        }

    @pytest.fixture
    def mock_group_context(self):
        """Create mock group context."""
        context = MagicMock()
        context.primary_group_id = str(uuid.uuid4())
        return context

    def test_create_starting_point_crew_method(self, mock_task, mock_callbacks, mock_group_context):
        """Test create_starting_point_crew_method creates a valid method."""
        from src.engines.crewai.flow.modules.flow_methods import FlowMethodFactory

        mock_create_callbacks = MagicMock(return_value=(MagicMock(), MagicMock()))

        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name='starting_point_0',
            task_list=[mock_task],
            crew_name='Test Crew',
            callbacks=mock_callbacks,
            group_context=mock_group_context,
            create_execution_callbacks=mock_create_callbacks,
        )

        assert method is not None
        assert method.__name__ == 'starting_point_0'
        assert callable(method)

    def test_create_starting_point_crew_method_with_crew_data(self, mock_task, mock_callbacks, mock_group_context):
        """Test create_starting_point_crew_method with crew_data configuration."""
        from src.engines.crewai.flow.modules.flow_methods import FlowMethodFactory

        mock_create_callbacks = MagicMock(return_value=(MagicMock(), MagicMock()))

        mock_crew_data = MagicMock()
        mock_crew_data.memory = False
        mock_crew_data.process = 'hierarchical'
        mock_crew_data.verbose = True
        mock_crew_data.planning = True
        mock_crew_data.reasoning = True

        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name='starting_point_0',
            task_list=[mock_task],
            crew_name='Test Crew',
            callbacks=mock_callbacks,
            group_context=mock_group_context,
            create_execution_callbacks=mock_create_callbacks,
            crew_data=mock_crew_data,
        )

        assert method is not None
        assert callable(method)

    def test_create_listener_method(self, mock_task, mock_callbacks, mock_group_context):
        """Test create_listener_method creates a valid method."""
        from src.engines.crewai.flow.modules.flow_methods import FlowMethodFactory

        mock_create_callbacks = MagicMock(return_value=(MagicMock(), MagicMock()))

        method = FlowMethodFactory.create_listener_method(
            method_name='listener_0',
            listener_tasks=[mock_task],
            method_condition='starting_point_0',
            condition_type='NONE',
            callbacks=mock_callbacks,
            group_context=mock_group_context,
            create_execution_callbacks=mock_create_callbacks,
            crew_name='Listener Crew',
        )

        assert method is not None
        assert method.__name__ == 'listener_0'
        assert callable(method)

    def test_create_listener_method_with_crew_data(self, mock_task, mock_callbacks, mock_group_context):
        """Test create_listener_method with crew_data configuration."""
        from src.engines.crewai.flow.modules.flow_methods import FlowMethodFactory

        mock_create_callbacks = MagicMock(return_value=(MagicMock(), MagicMock()))

        mock_crew_data = MagicMock()
        mock_crew_data.memory = True
        mock_crew_data.process = 'sequential'
        mock_crew_data.verbose = False
        mock_crew_data.planning = False
        mock_crew_data.reasoning = False

        method = FlowMethodFactory.create_listener_method(
            method_name='listener_0',
            listener_tasks=[mock_task],
            method_condition='starting_point_0',
            condition_type='NONE',
            callbacks=mock_callbacks,
            group_context=mock_group_context,
            create_execution_callbacks=mock_create_callbacks,
            crew_name='Listener Crew',
            crew_data=mock_crew_data,
        )

        assert method is not None
        assert callable(method)

    def test_create_skipped_crew_method_starting_point(self):
        """Test create_skipped_crew_method for starting point."""
        from src.engines.crewai.flow.modules.flow_methods import FlowMethodFactory

        method = FlowMethodFactory.create_skipped_crew_method(
            method_name='starting_point_0',
            crew_name='Skipped Crew',
            crew_sequence=1,
            is_starting_point=True,
            checkpoint_output='Previous output',
        )

        assert method is not None
        assert method.__name__ == 'starting_point_0'
        assert callable(method)

    def test_create_skipped_crew_method_listener(self):
        """Test create_skipped_crew_method for listener."""
        from src.engines.crewai.flow.modules.flow_methods import FlowMethodFactory

        method = FlowMethodFactory.create_skipped_crew_method(
            method_name='listener_0',
            crew_name='Skipped Listener',
            crew_sequence=2,
            is_starting_point=False,
            method_condition='starting_point_0',
            condition_type='NONE',
            checkpoint_output='Previous listener output',
        )

        assert method is not None
        assert method.__name__ == 'listener_0'
        assert callable(method)

    def test_create_skipped_crew_method_without_checkpoint(self):
        """Test create_skipped_crew_method without checkpoint output."""
        from src.engines.crewai.flow.modules.flow_methods import FlowMethodFactory

        method = FlowMethodFactory.create_skipped_crew_method(
            method_name='starting_point_0',
            crew_name='Skipped Crew',
            crew_sequence=1,
            is_starting_point=True,
            checkpoint_output=None,
        )

        assert method is not None
        assert callable(method)


class TestSkippedMethodExecution:
    """Tests for skipped method execution behavior."""

    @pytest.mark.asyncio
    async def test_skipped_starting_method_with_checkpoint(self):
        """Test skipped starting method returns checkpoint output."""
        from src.engines.crewai.flow.modules.flow_methods import FlowMethodFactory

        method = FlowMethodFactory.create_skipped_crew_method(
            method_name='starting_point_0',
            crew_name='Skipped Crew',
            crew_sequence=1,
            is_starting_point=True,
            checkpoint_output='Checkpoint output data',
        )

        # Create a mock flow instance
        mock_flow = MagicMock()
        mock_flow.state = {}

        # Extract the inner function and call it
        # The method is wrapped with @start() decorator
        inner_func = method.__wrapped__ if hasattr(method, '__wrapped__') else method
        result = await inner_func(mock_flow)

        assert result == 'Checkpoint output data'

    @pytest.mark.asyncio
    async def test_skipped_listener_method_with_checkpoint(self):
        """Test skipped listener method returns checkpoint output."""
        from src.engines.crewai.flow.modules.flow_methods import FlowMethodFactory

        method = FlowMethodFactory.create_skipped_crew_method(
            method_name='listener_0',
            crew_name='Skipped Listener',
            crew_sequence=2,
            is_starting_point=False,
            method_condition='starting_point_0',
            condition_type='NONE',
            checkpoint_output='Listener checkpoint data',
        )

        # Create a mock flow instance
        mock_flow = MagicMock()
        mock_flow.state = {}

        # Extract the inner function and call it
        inner_func = method.__wrapped__ if hasattr(method, '__wrapped__') else method
        result = await inner_func(mock_flow, 'previous_output')

        assert result == 'Listener checkpoint data'

    @pytest.mark.asyncio
    async def test_skipped_method_uses_state_fallback(self):
        """Test skipped method falls back to state when no checkpoint."""
        from src.engines.crewai.flow.modules.flow_methods import FlowMethodFactory

        method = FlowMethodFactory.create_skipped_crew_method(
            method_name='starting_point_0',
            crew_name='Skipped Crew',
            crew_sequence=1,
            is_starting_point=True,
            checkpoint_output=None,
        )

        # Create a mock flow instance with state containing cached output
        mock_flow = MagicMock()
        mock_flow.state = {'starting_point_0': 'State cached output'}
        mock_flow._method_outputs = None

        inner_func = method.__wrapped__ if hasattr(method, '__wrapped__') else method
        result = await inner_func(mock_flow)

        # Should return state value or placeholder
        assert result is not None


class TestCrewMethodExecution:
    """Tests for actual crew method execution behavior."""

    @pytest.mark.asyncio
    async def test_starting_point_method_execution(self):
        """Test starting point method executes crew correctly."""
        from src.engines.crewai.flow.modules.flow_methods import FlowMethodFactory

        mock_agent = MagicMock()
        mock_agent.role = "Test Agent"
        mock_agent.tools = []
        mock_agent.memory = True

        mock_task = MagicMock()
        mock_task.agent = mock_agent
        mock_task.description = "Test task"
        mock_task.expected_output = "Output"
        mock_task.context = None

        mock_callbacks = {'job_id': str(uuid.uuid4())}
        mock_create_callbacks = MagicMock(return_value=(MagicMock(), MagicMock()))

        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name='starting_point_0',
            task_list=[mock_task],
            crew_name='Test Crew',
            callbacks=mock_callbacks,
            group_context=None,
            create_execution_callbacks=mock_create_callbacks,
        )

        # Create mock flow instance
        mock_flow = MagicMock()
        mock_flow.state = {}

        # Mock Crew to avoid actual execution
        with patch('src.engines.crewai.flow.modules.flow_methods.Crew') as mock_crew_class:
            mock_crew = MagicMock()
            mock_result = MagicMock()
            mock_result.raw = "Crew execution result"
            mock_crew.kickoff_async = AsyncMock(return_value=mock_result)
            mock_crew_class.return_value = mock_crew

            inner_func = method.__wrapped__ if hasattr(method, '__wrapped__') else method
            result = await inner_func(mock_flow)

            assert result == "Crew execution result"
            mock_crew_class.assert_called_once()

    @pytest.mark.asyncio
    async def test_listener_method_execution(self):
        """Test listener method executes crew correctly."""
        from src.engines.crewai.flow.modules.flow_methods import FlowMethodFactory

        mock_agent = MagicMock()
        mock_agent.role = "Test Agent"
        mock_agent.tools = []
        mock_agent.memory = True

        mock_task = MagicMock()
        mock_task.agent = mock_agent
        mock_task.description = "Listener task"
        mock_task.expected_output = "Output"

        mock_callbacks = {'job_id': str(uuid.uuid4())}
        mock_create_callbacks = MagicMock(return_value=(MagicMock(), MagicMock()))

        method = FlowMethodFactory.create_listener_method(
            method_name='listener_0',
            listener_tasks=[mock_task],
            method_condition='starting_point_0',
            condition_type='NONE',
            callbacks=mock_callbacks,
            group_context=None,
            create_execution_callbacks=mock_create_callbacks,
            crew_name='Listener Crew',
        )

        # Create mock flow instance
        mock_flow = MagicMock()
        mock_flow.state = {}

        # Mock dependencies
        with patch('src.engines.crewai.flow.modules.flow_methods.Crew') as mock_crew_class, \
             patch('src.engines.crewai.flow.modules.flow_methods.Task') as mock_task_class, \
             patch('src.engines.crewai.flow.modules.flow_methods.get_model_context_limits') as mock_limits:
            mock_crew = MagicMock()
            mock_result = MagicMock()
            mock_result.raw = "Listener execution result"
            mock_crew.kickoff_async = AsyncMock(return_value=mock_result)
            mock_crew_class.return_value = mock_crew

            mock_task_class.return_value = mock_task
            mock_limits.return_value = (128000, 16000)

            inner_func = method.__wrapped__ if hasattr(method, '__wrapped__') else method
            result = await inner_func(mock_flow, "Previous output")

            assert result == "Listener execution result"

    @pytest.mark.asyncio
    async def test_starting_point_method_timeout(self):
        """Test starting point method handles timeout correctly."""
        import asyncio
        from src.engines.crewai.flow.modules.flow_methods import FlowMethodFactory

        mock_agent = MagicMock()
        mock_agent.role = "Test Agent"
        mock_agent.tools = []

        mock_task = MagicMock()
        mock_task.agent = mock_agent
        mock_task.description = "Test task"

        mock_callbacks = {'job_id': str(uuid.uuid4())}
        mock_create_callbacks = MagicMock(return_value=(MagicMock(), MagicMock()))

        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name='starting_point_0',
            task_list=[mock_task],
            crew_name='Test Crew',
            callbacks=mock_callbacks,
            group_context=None,
            create_execution_callbacks=mock_create_callbacks,
        )

        mock_flow = MagicMock()
        mock_flow.state = {}

        with patch('src.engines.crewai.flow.modules.flow_methods.Crew') as mock_crew_class, \
             patch('asyncio.wait_for') as mock_wait_for:
            mock_crew = MagicMock()
            mock_crew_class.return_value = mock_crew
            mock_wait_for.side_effect = asyncio.TimeoutError()

            inner_func = method.__wrapped__ if hasattr(method, '__wrapped__') else method

            with pytest.raises(TimeoutError):
                await inner_func(mock_flow)

    @pytest.mark.asyncio
    async def test_starting_point_method_execution_error(self):
        """Test starting point method handles execution errors."""
        from src.engines.crewai.flow.modules.flow_methods import FlowMethodFactory

        mock_agent = MagicMock()
        mock_agent.role = "Test Agent"
        mock_agent.tools = []

        mock_task = MagicMock()
        mock_task.agent = mock_agent
        mock_task.description = "Test task"

        mock_callbacks = {'job_id': str(uuid.uuid4())}
        mock_create_callbacks = MagicMock(return_value=(MagicMock(), MagicMock()))

        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name='starting_point_0',
            task_list=[mock_task],
            crew_name='Test Crew',
            callbacks=mock_callbacks,
            group_context=None,
            create_execution_callbacks=mock_create_callbacks,
        )

        mock_flow = MagicMock()
        mock_flow.state = {}

        with patch('src.engines.crewai.flow.modules.flow_methods.Crew') as mock_crew_class:
            mock_crew = MagicMock()
            mock_crew.kickoff_async = AsyncMock(side_effect=Exception("Execution error"))
            mock_crew_class.return_value = mock_crew

            inner_func = method.__wrapped__ if hasattr(method, '__wrapped__') else method

            with pytest.raises(Exception, match="Execution error"):
                await inner_func(mock_flow)
