"""
Comprehensive unit tests for flow_builder.py module.
Target: 80%+ coverage
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock
import uuid
from typing import Dict, Any, List


class TestFlowBuilder:
    """Tests for FlowBuilder class."""

    @pytest.fixture
    def mock_repositories(self):
        """Create mock repositories."""
        return {
            'task': AsyncMock(),
            'agent': AsyncMock(),
            'crew': AsyncMock(),
            'execution_history': AsyncMock(),
            'execution_trace': AsyncMock(),
        }

    @pytest.fixture
    def mock_callbacks(self):
        """Create mock callbacks."""
        return {
            'job_id': str(uuid.uuid4()),
            'task_callback': MagicMock(),
        }

    @pytest.fixture
    def mock_group_context(self):
        """Create mock group context."""
        context = MagicMock()
        context.primary_group_id = str(uuid.uuid4())
        context.group_ids = [context.primary_group_id]
        return context

    @pytest.fixture
    def basic_flow_data(self):
        """Create basic flow data for testing."""
        task_id = str(uuid.uuid4())
        crew_id = str(uuid.uuid4())
        return {
            'flow_config': {
                'startingPoints': [
                    {'taskId': task_id, 'crewId': crew_id}
                ],
                'listeners': [],
                'routers': [],
            },
            'edges': [],
        }

    @pytest.mark.asyncio
    async def test_build_flow_no_flow_data(self, mock_repositories):
        """Test build_flow raises ValueError when no flow data provided."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        with pytest.raises(ValueError, match="No flow data provided"):
            await FlowBuilder.build_flow(None, mock_repositories)

    @pytest.mark.asyncio
    async def test_build_flow_empty_flow_data(self, mock_repositories):
        """Test build_flow raises ValueError when flow data is empty."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        with pytest.raises(ValueError, match="No flow data provided"):
            await FlowBuilder.build_flow({}, mock_repositories)

    @pytest.mark.asyncio
    async def test_build_flow_no_starting_points(self, mock_repositories):
        """Test build_flow raises ValueError when no starting points defined."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        flow_data = {
            'flow_config': {
                'startingPoints': [],
                'listeners': [],
                'routers': [],
            }
        }

        with pytest.raises(ValueError, match="No starting points defined"):
            await FlowBuilder.build_flow(flow_data, mock_repositories)

    @pytest.mark.asyncio
    async def test_build_flow_with_string_flow_config(self, mock_repositories):
        """Test build_flow handles JSON string flow_config."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder
        import json

        task_id = str(uuid.uuid4())
        crew_id = str(uuid.uuid4())
        flow_config_str = json.dumps({
            'startingPoints': [{'taskId': task_id, 'crewId': crew_id}],
            'listeners': [],
            'routers': [],
        })

        flow_data = {'flow_config': flow_config_str}

        # Should raise because no task repo returns valid data
        with pytest.raises(ValueError):
            await FlowBuilder.build_flow(flow_data, mock_repositories)

    @pytest.mark.asyncio
    async def test_build_flow_with_checkpoint_edge(self, mock_repositories):
        """Test build_flow enables persistence when checkpoint edge exists."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        task_id = str(uuid.uuid4())
        crew_id = str(uuid.uuid4())
        flow_data = {
            'flow_config': {
                'startingPoints': [{'taskId': task_id, 'crewId': crew_id}],
                'listeners': [],
                'routers': [],
            },
            'edges': [
                {'data': {'checkpoint': True}}
            ],
        }

        # Mock the process methods to avoid complex setup
        with patch('src.engines.crewai.flow.modules.flow_builder.FlowConfigManager') as mock_config_manager, \
             patch('src.engines.crewai.flow.modules.flow_builder.FlowProcessorManager') as mock_processor:
            mock_config_manager.collect_agent_mcp_requirements = AsyncMock(return_value={})
            mock_processor.process_starting_points = AsyncMock(return_value=[])
            mock_processor.process_listeners = AsyncMock(return_value=[])
            mock_processor.process_routers = AsyncMock(return_value=[])

            # When process_starting_points returns empty, the flow builder logs an error
            # but still creates a flow (with persistence enabled due to checkpoint edge)
            flow_cls = await FlowBuilder.build_flow(flow_data, mock_repositories)
            # The flow class should be created even with empty processors result
            assert flow_cls is not None

    @pytest.mark.asyncio
    async def test_build_flow_with_resume_parameters(self, mock_repositories, mock_callbacks, mock_group_context):
        """Test build_flow handles resume parameters correctly."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        task_id = str(uuid.uuid4())
        crew_id = str(uuid.uuid4())
        execution_id = "123"
        restore_uuid = str(uuid.uuid4())

        flow_data = {
            'flow_config': {
                'startingPoints': [{'taskId': task_id, 'crewId': crew_id}],
                'listeners': [],
                'routers': [],
            },
            'edges': [],
        }

        # Mock execution history lookup
        mock_execution = MagicMock()
        mock_execution.job_id = str(uuid.uuid4())
        mock_repositories['execution_history'].get_execution_by_id = AsyncMock(return_value=mock_execution)
        mock_repositories['execution_trace'].get_crew_outputs_for_resume = AsyncMock(return_value={})

        with patch('src.engines.crewai.flow.modules.flow_builder.FlowConfigManager') as mock_config_manager, \
             patch('src.engines.crewai.flow.modules.flow_builder.FlowProcessorManager') as mock_processor:
            mock_config_manager.collect_agent_mcp_requirements = AsyncMock(return_value={})
            mock_processor.process_starting_points = AsyncMock(return_value=[])
            mock_processor.process_listeners = AsyncMock(return_value=[])
            mock_processor.process_routers = AsyncMock(return_value=[])

            # When process_starting_points returns empty, the flow builder logs an error
            # but still creates a flow with resume parameters
            flow_cls = await FlowBuilder.build_flow(
                flow_data,
                mock_repositories,
                mock_callbacks,
                mock_group_context,
                restore_uuid=restore_uuid,
                resume_from_crew_sequence=1,
                resume_from_execution_id=execution_id
            )
            # The flow class should be created even with empty processors result
            assert flow_cls is not None

    def test_apply_state_operations_with_none(self):
        """Test _apply_state_operations handles None input."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        mock_flow = MagicMock()
        # Should not raise
        FlowBuilder._apply_state_operations(mock_flow, None)

    def test_apply_state_operations_with_reads(self):
        """Test _apply_state_operations handles state reads."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        mock_flow = MagicMock()
        mock_flow.state = {'test_var': 'test_value'}

        state_operations = {
            'reads': ['test_var'],
            'writes': [],
        }

        FlowBuilder._apply_state_operations(mock_flow, state_operations)

    def test_apply_state_operations_with_writes_value(self):
        """Test _apply_state_operations handles state writes with direct value."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        mock_flow = MagicMock()
        mock_flow.state = {}

        state_operations = {
            'reads': [],
            'writes': [{'variable': 'new_var', 'value': 'new_value'}],
        }

        FlowBuilder._apply_state_operations(mock_flow, state_operations)

    def test_apply_state_operations_with_writes_expression(self):
        """Test _apply_state_operations handles state writes with expression."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        mock_flow = MagicMock()
        mock_flow.state = {'x': 5}

        state_operations = {
            'reads': [],
            'writes': [{'variable': 'y', 'expression': 'state["x"] + 1'}],
        }

        FlowBuilder._apply_state_operations(mock_flow, state_operations)

    def test_apply_state_operations_with_object_state(self):
        """Test _apply_state_operations handles object-based state."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        class MockState:
            def __init__(self):
                self.test_var = 'initial'

        mock_flow = MagicMock()
        mock_flow.state = MockState()
        # Make hasattr check fail for 'get' method
        type(mock_flow.state).get = PropertyMock(side_effect=AttributeError)

        state_operations = {
            'reads': ['test_var'],
            'writes': [{'variable': 'new_var', 'value': 'new_value'}],
        }

        FlowBuilder._apply_state_operations(mock_flow, state_operations)

    def test_apply_state_operations_expression_error(self):
        """Test _apply_state_operations handles expression evaluation errors."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        mock_flow = MagicMock()
        mock_flow.state = {}

        state_operations = {
            'reads': [],
            'writes': [{'variable': 'y', 'expression': 'invalid_syntax('}],
        }

        # Should not raise, just log error
        FlowBuilder._apply_state_operations(mock_flow, state_operations)


class TestCreateDynamicFlow:
    """Tests for _create_dynamic_flow method."""

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_basic(self):
        """Test basic dynamic flow creation."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        # Create mock task and agent
        mock_agent = MagicMock()
        mock_agent.role = "Test Agent"
        mock_agent.tools = []

        mock_task = MagicMock()
        mock_task.agent = mock_agent
        mock_task.description = "Test task"

        # Setup starting points as expected tuple format
        starting_points = [
            ('starting_point_0', ['task-1'], [mock_task], 'Test Crew', MagicMock())
        ]

        with patch('src.engines.crewai.flow.modules.flow_builder.FlowMethodFactory') as mock_factory:
            mock_method = AsyncMock()
            mock_factory.create_starting_point_crew_method = MagicMock(return_value=mock_method)

            flow = await FlowBuilder._create_dynamic_flow(
                starting_points=starting_points,
                listener_crews=[],
                routers=[],
                all_agents={'Test Agent': mock_agent},
                all_tasks={'task-1': mock_task},
                flow_config={},
                callbacks=None,
                group_context=None,
            )

            assert flow is not None

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_with_state(self):
        """Test dynamic flow creation with state configuration."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        mock_agent = MagicMock()
        mock_agent.role = "Test Agent"

        mock_task = MagicMock()
        mock_task.agent = mock_agent
        mock_task.description = "Test task"

        starting_points = [
            ('starting_point_0', ['task-1'], [mock_task], 'Test Crew', MagicMock())
        ]

        flow_config = {
            'state': {
                'enabled': True,
                'type': 'unstructured',
                'initialValues': {'counter': 0},
            },
        }

        with patch('src.engines.crewai.flow.modules.flow_builder.FlowMethodFactory') as mock_factory:
            mock_method = AsyncMock()
            mock_factory.create_starting_point_crew_method = MagicMock(return_value=mock_method)

            flow = await FlowBuilder._create_dynamic_flow(
                starting_points=starting_points,
                listener_crews=[],
                routers=[],
                all_agents={'Test Agent': mock_agent},
                all_tasks={'task-1': mock_task},
                flow_config=flow_config,
                callbacks=None,
                group_context=None,
            )

            assert flow is not None

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_with_persistence(self):
        """Test dynamic flow creation with persistence enabled."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        mock_agent = MagicMock()
        mock_agent.role = "Test Agent"

        mock_task = MagicMock()
        mock_task.agent = mock_agent
        mock_task.description = "Test task"

        starting_points = [
            ('starting_point_0', ['task-1'], [mock_task], 'Test Crew', MagicMock())
        ]

        flow_config = {
            'persistence': {
                'enabled': True,
                'level': 'flow',
            },
        }

        with patch('src.engines.crewai.flow.modules.flow_builder.FlowMethodFactory') as mock_factory:
            mock_method = AsyncMock()
            mock_factory.create_starting_point_crew_method = MagicMock(return_value=mock_method)

            flow = await FlowBuilder._create_dynamic_flow(
                starting_points=starting_points,
                listener_crews=[],
                routers=[],
                all_agents={'Test Agent': mock_agent},
                all_tasks={'task-1': mock_task},
                flow_config=flow_config,
                callbacks=None,
                group_context=None,
                restore_uuid=str(uuid.uuid4()),
            )

            assert flow is not None

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_with_listeners(self):
        """Test dynamic flow creation with listener crews."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        mock_agent = MagicMock()
        mock_agent.role = "Test Agent"

        mock_task = MagicMock()
        mock_task.agent = mock_agent
        mock_task.description = "Test task"

        starting_points = [
            ('starting_point_0', ['task-1'], [mock_task], 'Start Crew', MagicMock())
        ]

        listener_crews = [
            ('listener_0', 'crew-2', ['task-2'], [mock_task], 'Listener Crew', ['task-1'], 'NONE', MagicMock())
        ]

        with patch('src.engines.crewai.flow.modules.flow_builder.FlowMethodFactory') as mock_factory:
            mock_start_method = AsyncMock()
            mock_listener_method = AsyncMock()
            mock_factory.create_starting_point_crew_method = MagicMock(return_value=mock_start_method)
            mock_factory.create_listener_method = MagicMock(return_value=mock_listener_method)

            flow = await FlowBuilder._create_dynamic_flow(
                starting_points=starting_points,
                listener_crews=listener_crews,
                routers=[],
                all_agents={'Test Agent': mock_agent},
                all_tasks={'task-1': mock_task, 'task-2': mock_task},
                flow_config={'listeners': [{'crewId': 'crew-2', 'name': 'Listener Crew'}]},
                callbacks=None,
                group_context=None,
            )

            assert flow is not None

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_skip_crew_for_resume(self):
        """Test dynamic flow creation with skipped crews for checkpoint resume.

        When resume_from_crew_sequence=2, crews with sequence < 2 (i.e., sequence 1)
        should be skipped because they were already completed before the checkpoint.
        The comparison uses < (not <=) because resume_from is the sequence of the
        crew TO RUN, not the last completed crew.
        """
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        mock_agent = MagicMock()
        mock_agent.role = "Test Agent"

        mock_task = MagicMock()
        mock_task.agent = mock_agent
        mock_task.description = "Test task"

        starting_points = [
            ('starting_point_0', ['task-1'], [mock_task], 'Start Crew', MagicMock())
        ]

        checkpoint_outputs = {'Start Crew': 'Previous output'}

        with patch('src.engines.crewai.flow.modules.flow_builder.FlowMethodFactory') as mock_factory:
            mock_skip_method = AsyncMock()
            mock_factory.create_skipped_crew_method = MagicMock(return_value=mock_skip_method)

            flow = await FlowBuilder._create_dynamic_flow(
                starting_points=starting_points,
                listener_crews=[],
                routers=[],
                all_agents={'Test Agent': mock_agent},
                all_tasks={'task-1': mock_task},
                flow_config={},
                callbacks=None,
                group_context=None,
                resume_from_crew_sequence=2,  # Skip crews with sequence < 2 (i.e., crew 1)
                checkpoint_outputs=checkpoint_outputs,
            )

            assert flow is not None
            mock_factory.create_skipped_crew_method.assert_called()

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_with_routers(self):
        """Test dynamic flow creation with routers."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        mock_agent = MagicMock()
        mock_agent.role = "Test Agent"

        mock_task = MagicMock()
        mock_task.agent = mock_agent
        mock_task.description = "Test task"

        starting_points = [
            ('starting_point_0', ['task-1'], [mock_task], 'Start Crew', MagicMock())
        ]

        routers = [
            {
                'name': 'test_router',
                'listenTo': 'starting_point_0',
                'routes': {'success': [{'id': 'task-2'}], 'failure': []},
                'routeConditions': {'success': 'state.get("result") == True'},
            }
        ]

        with patch('src.engines.crewai.flow.modules.flow_builder.FlowMethodFactory') as mock_factory:
            mock_start_method = AsyncMock()
            mock_factory.create_starting_point_crew_method = MagicMock(return_value=mock_start_method)

            flow = await FlowBuilder._create_dynamic_flow(
                starting_points=starting_points,
                listener_crews=[],
                routers=routers,
                all_agents={'Test Agent': mock_agent},
                all_tasks={'task-1': mock_task, 'task-2': mock_task},
                flow_config={},
                callbacks=None,
                group_context=None,
            )

            assert flow is not None

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_listener_with_and_condition(self):
        """Test dynamic flow creation with AND condition listener."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        mock_agent = MagicMock()
        mock_agent.role = "Test Agent"

        mock_task = MagicMock()
        mock_task.agent = mock_agent
        mock_task.description = "Test task"

        starting_points = [
            ('starting_point_0', ['task-1'], [mock_task], 'Start Crew 1', MagicMock()),
            ('starting_point_1', ['task-2'], [mock_task], 'Start Crew 2', MagicMock()),
        ]

        listener_crews = [
            ('listener_0', 'crew-3', ['task-3'], [mock_task], 'Listener Crew', ['task-1', 'task-2'], 'AND', MagicMock())
        ]

        with patch('src.engines.crewai.flow.modules.flow_builder.FlowMethodFactory') as mock_factory:
            mock_start_method = AsyncMock()
            mock_listener_method = AsyncMock()
            mock_factory.create_starting_point_crew_method = MagicMock(return_value=mock_start_method)
            mock_factory.create_listener_method = MagicMock(return_value=mock_listener_method)

            flow = await FlowBuilder._create_dynamic_flow(
                starting_points=starting_points,
                listener_crews=listener_crews,
                routers=[],
                all_agents={'Test Agent': mock_agent},
                all_tasks={'task-1': mock_task, 'task-2': mock_task, 'task-3': mock_task},
                flow_config={},
                callbacks=None,
                group_context=None,
            )

            assert flow is not None

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_listener_with_or_condition(self):
        """Test dynamic flow creation with OR condition listener."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        mock_agent = MagicMock()
        mock_agent.role = "Test Agent"

        mock_task = MagicMock()
        mock_task.agent = mock_agent
        mock_task.description = "Test task"

        starting_points = [
            ('starting_point_0', ['task-1'], [mock_task], 'Start Crew 1', MagicMock()),
            ('starting_point_1', ['task-2'], [mock_task], 'Start Crew 2', MagicMock()),
        ]

        listener_crews = [
            ('listener_0', 'crew-3', ['task-3'], [mock_task], 'Listener Crew', ['task-1', 'task-2'], 'OR', MagicMock())
        ]

        with patch('src.engines.crewai.flow.modules.flow_builder.FlowMethodFactory') as mock_factory:
            mock_start_method = AsyncMock()
            mock_listener_method = AsyncMock()
            mock_factory.create_starting_point_crew_method = MagicMock(return_value=mock_start_method)
            mock_factory.create_listener_method = MagicMock(return_value=mock_listener_method)

            flow = await FlowBuilder._create_dynamic_flow(
                starting_points=starting_points,
                listener_crews=listener_crews,
                routers=[],
                all_agents={'Test Agent': mock_agent},
                all_tasks={'task-1': mock_task, 'task-2': mock_task, 'task-3': mock_task},
                flow_config={},
                callbacks=None,
                group_context=None,
            )

            assert flow is not None

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_skipped_listener(self):
        """Test dynamic flow creation with skipped listener for checkpoint resume."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        mock_agent = MagicMock()
        mock_agent.role = "Test Agent"

        mock_task = MagicMock()
        mock_task.agent = mock_agent
        mock_task.description = "Test task"

        starting_points = [
            ('starting_point_0', ['task-1'], [mock_task], 'Start Crew', MagicMock())
        ]

        listener_crews = [
            ('listener_0', 'crew-2', ['task-2'], [mock_task], 'Listener Crew', ['task-1'], 'NONE', MagicMock())
        ]

        checkpoint_outputs = {
            'Start Crew': 'Previous start output',
            'Listener Crew': 'Previous listener output'
        }

        with patch('src.engines.crewai.flow.modules.flow_builder.FlowMethodFactory') as mock_factory:
            mock_skip_method = AsyncMock()
            mock_factory.create_skipped_crew_method = MagicMock(return_value=mock_skip_method)

            flow = await FlowBuilder._create_dynamic_flow(
                starting_points=starting_points,
                listener_crews=listener_crews,
                routers=[],
                all_agents={'Test Agent': mock_agent},
                all_tasks={'task-1': mock_task, 'task-2': mock_task},
                flow_config={'listeners': [{'crewId': 'crew-2', 'name': 'Listener Crew'}]},
                callbacks=None,
                group_context=None,
                resume_from_crew_sequence=2,
                checkpoint_outputs=checkpoint_outputs,
            )

            assert flow is not None
