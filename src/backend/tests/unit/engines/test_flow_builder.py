"""
Unit tests for CrewAI flow builder module.
"""
import pytest
import json
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from typing import Dict, Any

from src.engines.crewai.flow.modules.flow_builder import FlowBuilder


class TestFlowBuilder:
    """Test cases for FlowBuilder class."""

    @pytest.fixture
    def sample_flow_data(self):
        """Sample flow data for testing."""
        return {
            'flow_config': {
                'startingPoints': [
                    {
                        'crewName': 'test_crew',
                        'crewId': '1',
                        'taskName': 'test_task',
                        'taskId': '1'
                    }
                ],
                'listeners': [
                    {
                        'name': 'test_listener',
                        'crewId': '2',
                        'listenToTaskIds': ['1'],
                        'conditionType': 'NONE',
                        'tasks': [
                            {'id': '2'}
                        ]
                    }
                ]
            }
        }

    @pytest.fixture
    def sample_repositories(self):
        """Sample repositories for testing."""
        task_repo = Mock()
        agent_repo = Mock()
        return {
            'task': task_repo,
            'agent': agent_repo
        }

    @pytest.fixture
    def sample_callbacks(self):
        """Sample callbacks for testing."""
        streaming_callback = Mock()
        streaming_callback.execute = Mock()
        return {
            'streaming': streaming_callback
        }

    @pytest.fixture
    def mock_task_data(self):
        """Mock task data."""
        task = Mock()
        task.name = "Test Task"
        task.description = "Test description"
        task.expected_output = "Test output"
        task.agent_id = "1"
        task.id = "1"
        return task

    @pytest.fixture
    def mock_agent_data(self):
        """Mock agent data."""
        agent = Mock()
        agent.name = "Test Agent"
        agent.role = "Test Role"
        agent.id = "1"
        return agent

    @pytest.fixture
    def mock_configured_agent(self):
        """Mock configured CrewAI agent."""
        agent = Mock()
        agent.role = "Test Role"
        agent.tools = []
        return agent

    @pytest.fixture
    def mock_configured_task(self):
        """Mock configured CrewAI task."""
        task = Mock()
        task.description = "Test description"
        task.agent = Mock()
        task.agent.role = "Test Role"
        return task

    @pytest.mark.asyncio
    async def test_build_flow_success(self, sample_flow_data, sample_repositories, sample_callbacks,
                                    mock_task_data, mock_agent_data, mock_configured_agent, mock_configured_task):
        """Test successful flow building."""
        # Setup repository mocks
        sample_repositories['task'].find_by_id.return_value = mock_task_data
        sample_repositories['agent'].find_by_id.return_value = mock_agent_data

        with patch('src.engines.crewai.flow.modules.flow_builder.AgentConfig') as mock_agent_config, \
             patch('src.engines.crewai.flow.modules.flow_builder.TaskConfig') as mock_task_config:
            
            mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=mock_configured_agent)
            mock_task_config.configure_task = AsyncMock(return_value=mock_configured_task)

            result = await FlowBuilder.build_flow(sample_flow_data, sample_repositories, sample_callbacks)

            assert result is not None
            assert hasattr(result, '__class__')
            mock_agent_config.configure_agent_and_tools.assert_called()
            mock_task_config.configure_task.assert_called()

    @pytest.mark.asyncio
    async def test_build_flow_no_flow_data(self):
        """Test flow building with no flow data."""
        with pytest.raises(ValueError, match="No flow data provided"):
            await FlowBuilder.build_flow(None)

    @pytest.mark.asyncio
    async def test_build_flow_empty_flow_data(self):
        """Test flow building with empty flow data."""
        with pytest.raises(ValueError, match="No flow data provided"):
            await FlowBuilder.build_flow({})

    @pytest.mark.asyncio
    async def test_build_flow_no_starting_points(self):
        """Test flow building with no starting points."""
        flow_data = {'flow_config': {'listeners': []}}
        with pytest.raises(ValueError, match="No starting points defined in flow configuration"):
            await FlowBuilder.build_flow(flow_data)

    @pytest.mark.asyncio
    async def test_build_flow_string_flow_config(self, sample_repositories, sample_callbacks,
                                               mock_task_data, mock_agent_data, mock_configured_agent, mock_configured_task):
        """Test flow building with empty flow_config that gets parsed from string."""
        flow_config = {
            'startingPoints': [
                {
                    'crewName': 'test_crew',
                    'crewId': '1',
                    'taskName': 'test_task',
                    'taskId': '1'
                }
            ],
            'listeners': []
        }
        # Empty string triggers the string parsing logic since not flow_config will be True
        flow_data = {'flow_config': ''}

        # Setup repository mocks
        sample_repositories['task'].find_by_id.return_value = mock_task_data
        sample_repositories['agent'].find_by_id.return_value = mock_agent_data

        # Patch the json.loads to return our flow_config when called
        with patch('src.engines.crewai.flow.modules.flow_builder.AgentConfig') as mock_agent_config, \
             patch('src.engines.crewai.flow.modules.flow_builder.TaskConfig') as mock_task_config, \
             patch('json.loads') as mock_json_loads:
            
            mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=mock_configured_agent)
            mock_task_config.configure_task = AsyncMock(return_value=mock_configured_task)
            mock_json_loads.return_value = flow_config

            result = await FlowBuilder.build_flow(flow_data, sample_repositories, sample_callbacks)

            assert result is not None
            mock_json_loads.assert_called_once_with('')

    @pytest.mark.asyncio
    async def test_build_flow_invalid_json_config(self):
        """Test flow building with invalid JSON config."""
        flow_data = {'flow_config': 'invalid json'}
        
        with pytest.raises(ValueError, match="Failed to build flow"):
            await FlowBuilder.build_flow(flow_data)

    @pytest.mark.asyncio
    async def test_process_starting_points_success(self, sample_repositories, sample_callbacks,
                                                 mock_task_data, mock_agent_data, mock_configured_agent, mock_configured_task):
        """Test successful processing of starting points."""
        starting_points = [
            {
                'crewName': 'test_crew',
                'crewId': '1',
                'taskName': 'test_task',
                'taskId': '1'
            }
        ]
        all_agents = {}
        all_tasks = {}
        flow_data = {}

        # Setup repository mocks
        sample_repositories['task'].find_by_id.return_value = mock_task_data
        sample_repositories['agent'].find_by_id.return_value = mock_agent_data

        with patch('src.engines.crewai.flow.modules.flow_builder.AgentConfig') as mock_agent_config, \
             patch('src.engines.crewai.flow.modules.flow_builder.TaskConfig') as mock_task_config:
            
            mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=mock_configured_agent)
            mock_task_config.configure_task = AsyncMock(return_value=mock_configured_task)

            await FlowBuilder._process_starting_points(
                starting_points, all_agents, all_tasks, flow_data, sample_repositories, sample_callbacks
            )

            assert '1' in all_agents
            assert '1' in all_tasks

    @pytest.mark.asyncio
    async def test_process_starting_points_no_repositories(self, mock_task_data, mock_agent_data, 
                                                         mock_configured_agent, mock_configured_task):
        """Test processing starting points without repositories."""
        starting_points = [
            {
                'crewName': 'test_crew',
                'crewId': '1',
                'taskName': 'test_task',
                'taskId': '1'
            }
        ]
        all_agents = {}
        all_tasks = {}
        flow_data = {}

        with patch('src.repositories.task_repository.get_sync_task_repository') as mock_task_repo_factory, \
             patch('src.repositories.agent_repository.get_sync_agent_repository') as mock_agent_repo_factory, \
             patch('src.engines.crewai.flow.modules.flow_builder.AgentConfig') as mock_agent_config, \
             patch('src.engines.crewai.flow.modules.flow_builder.TaskConfig') as mock_task_config:
            
            mock_task_repo = Mock()
            mock_task_repo.find_by_id.return_value = mock_task_data
            mock_task_repo_factory.return_value = mock_task_repo
            
            mock_agent_repo = Mock()
            mock_agent_repo.find_by_id.return_value = mock_agent_data
            mock_agent_repo_factory.return_value = mock_agent_repo

            mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=mock_configured_agent)
            mock_task_config.configure_task = AsyncMock(return_value=mock_configured_task)

            await FlowBuilder._process_starting_points(
                starting_points, all_agents, all_tasks, flow_data, None, None
            )

            assert '1' in all_agents
            assert '1' in all_tasks

    @pytest.mark.asyncio
    async def test_process_starting_points_no_task_data(self, sample_repositories):
        """Test processing starting points when task data is not found."""
        starting_points = [
            {
                'crewName': 'test_crew',
                'crewId': '1',
                'taskName': 'test_task',
                'taskId': '1'
            }
        ]
        all_agents = {}
        all_tasks = {}
        flow_data = {}

        # Setup repository mocks to return None
        sample_repositories['task'].find_by_id.return_value = None

        await FlowBuilder._process_starting_points(
            starting_points, all_agents, all_tasks, flow_data, sample_repositories, None
        )

        # Should not add anything to agents or tasks
        assert len(all_agents) == 0
        assert len(all_tasks) == 0

    @pytest.mark.asyncio
    async def test_process_starting_points_no_agent_data(self, sample_repositories, mock_task_data):
        """Test processing starting points when agent data is not found."""
        starting_points = [
            {
                'crewName': 'test_crew',
                'crewId': '1',
                'taskName': 'test_task',
                'taskId': '1'
            }
        ]
        all_agents = {}
        all_tasks = {}
        flow_data = {}

        # Setup repository mocks
        sample_repositories['task'].find_by_id.return_value = mock_task_data
        sample_repositories['agent'].find_by_id.return_value = None

        await FlowBuilder._process_starting_points(
            starting_points, all_agents, all_tasks, flow_data, sample_repositories, None
        )

        # Should not add agent but task processing might still continue
        assert len(all_agents) == 0

    @pytest.mark.asyncio
    async def test_process_listeners_success(self, sample_repositories, sample_callbacks,
                                           mock_agent_data, mock_configured_agent, mock_configured_task):
        """Test successful processing of listeners."""
        # Create mock task data with specific ID for this test
        mock_task_data = Mock()
        mock_task_data.name = "Test Task"
        mock_task_data.description = "Test description"
        mock_task_data.expected_output = "Test output"
        mock_task_data.agent_id = "2"
        mock_task_data.id = "2"
        
        listeners = [
            {
                'name': 'test_listener',
                'crewId': '2',
                'listenToTaskIds': ['1'],
                'conditionType': 'NONE',
                'tasks': [
                    {'id': '2'}
                ]
            }
        ]
        all_agents = {}
        all_tasks = {}
        flow_data = {}

        # Setup repository mocks
        sample_repositories['task'].find_by_id.return_value = mock_task_data
        sample_repositories['agent'].find_by_id.return_value = mock_agent_data

        with patch('src.engines.crewai.flow.modules.flow_builder.AgentConfig') as mock_agent_config, \
             patch('src.engines.crewai.flow.modules.flow_builder.TaskConfig') as mock_task_config:
            
            mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=mock_configured_agent)
            mock_task_config.configure_task = AsyncMock(return_value=mock_configured_task)

            await FlowBuilder._process_listeners(
                listeners, all_agents, all_tasks, flow_data, sample_repositories, sample_callbacks
            )

            assert '2' in all_agents
            assert '2' in all_tasks

    @pytest.mark.asyncio
    async def test_process_listeners_no_repositories(self, mock_task_data, mock_agent_data, 
                                                   mock_configured_agent, mock_configured_task):
        """Test processing listeners without repositories."""
        listeners = [
            {
                'name': 'test_listener',
                'crewId': '2',
                'listenToTaskIds': ['1'],
                'conditionType': 'NONE',
                'tasks': [
                    {'id': '2'}
                ]
            }
        ]
        all_agents = {}
        all_tasks = {}
        flow_data = {}

        with patch('src.repositories.task_repository.get_sync_task_repository') as mock_task_repo_factory, \
             patch('src.repositories.agent_repository.get_sync_agent_repository') as mock_agent_repo_factory, \
             patch('src.engines.crewai.flow.modules.flow_builder.AgentConfig') as mock_agent_config, \
             patch('src.engines.crewai.flow.modules.flow_builder.TaskConfig') as mock_task_config:
            
            mock_task_repo = Mock()
            mock_task_repo.find_by_id.return_value = mock_task_data
            mock_task_repo_factory.return_value = mock_task_repo
            
            mock_agent_repo = Mock()
            mock_agent_repo.find_by_id.return_value = mock_agent_data
            mock_agent_repo_factory.return_value = mock_agent_repo

            mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=mock_configured_agent)
            mock_task_config.configure_task = AsyncMock(return_value=mock_configured_task)

            await FlowBuilder._process_listeners(
                listeners, all_agents, all_tasks, flow_data, None, None
            )

            assert '2' in all_agents

    @pytest.mark.asyncio
    async def test_process_listeners_empty_list(self):
        """Test processing empty listeners list."""
        listeners = []
        all_agents = {}
        all_tasks = {}
        flow_data = {}

        await FlowBuilder._process_listeners(
            listeners, all_agents, all_tasks, flow_data, None, None
        )

        # Should not add anything
        assert len(all_agents) == 0
        assert len(all_tasks) == 0

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_success(self, mock_configured_agent, mock_configured_task):
        """Test successful creation of dynamic flow."""
        starting_points = [
            {
                'crewName': 'test_crew',
                'crewId': '1',
                'taskName': 'test_task',
                'taskId': '1'
            }
        ]
        listeners = []
        all_agents = {'1': mock_configured_agent}
        all_tasks = {'1': mock_configured_task}

        with patch('src.engines.crewai.flow.modules.flow_builder.Crew') as mock_crew_class:
            mock_crew = Mock()
            mock_crew.kickoff.return_value = "result"
            mock_crew_class.return_value = mock_crew

            result = await FlowBuilder._create_dynamic_flow(
                starting_points, listeners, all_agents, all_tasks
            )

            assert result is not None
            assert hasattr(result, 'start_flow_0')

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_with_listeners(self, mock_configured_agent, mock_configured_task):
        """Test creation of dynamic flow with listeners."""
        starting_points = [
            {
                'crewName': 'test_crew',
                'crewId': '1',
                'taskName': 'test_task',
                'taskId': '1'
            }
        ]
        listeners = [
            {
                'name': 'test_listener',
                'crewId': '2',
                'listenToTaskIds': ['1'],
                'conditionType': 'NONE',
                'tasks': [
                    {'id': '2'}
                ]
            }
        ]
        all_agents = {'1': mock_configured_agent, '2': mock_configured_agent}
        all_tasks = {'1': mock_configured_task, '2': mock_configured_task}

        with patch('src.engines.crewai.flow.modules.flow_builder.Crew') as mock_crew_class, \
             patch('src.engines.crewai.flow.modules.flow_builder.listen') as mock_listen:
            
            mock_crew = Mock()
            mock_crew.kickoff.return_value = "result"
            mock_crew_class.return_value = mock_crew
            
            # Mock the listen decorator
            def mock_decorator(func):
                return func
            mock_listen.return_value = mock_decorator

            result = await FlowBuilder._create_dynamic_flow(
                starting_points, listeners, all_agents, all_tasks
            )

            assert result is not None
            assert hasattr(result, 'start_flow_0')

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_and_condition(self, mock_configured_agent, mock_configured_task):
        """Test creation of dynamic flow with AND condition."""
        starting_points = [
            {
                'crewName': 'test_crew',
                'crewId': '1',
                'taskName': 'test_task',
                'taskId': '1'
            }
        ]
        listeners = [
            {
                'name': 'test_listener',
                'crewId': '2',
                'listenToTaskIds': ['1'],
                'conditionType': 'AND',
                'tasks': [
                    {'id': '2'}
                ]
            }
        ]
        all_agents = {'1': mock_configured_agent, '2': mock_configured_agent}
        all_tasks = {'1': mock_configured_task, '2': mock_configured_task}

        with patch('src.engines.crewai.flow.modules.flow_builder.Crew') as mock_crew_class, \
             patch('src.engines.crewai.flow.modules.flow_builder.listen') as mock_listen, \
             patch('src.engines.crewai.flow.modules.flow_builder.and_') as mock_and:
            
            mock_crew = Mock()
            mock_crew.kickoff.return_value = "result"
            mock_crew_class.return_value = mock_crew
            
            # Mock the listen decorator and and_ function
            def mock_decorator(func):
                return func
            mock_listen.return_value = mock_decorator
            mock_and.return_value = Mock()

            result = await FlowBuilder._create_dynamic_flow(
                starting_points, listeners, all_agents, all_tasks
            )

            assert result is not None
            mock_and.assert_called()

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_or_condition(self, mock_configured_agent, mock_configured_task):
        """Test creation of dynamic flow with OR condition."""
        starting_points = [
            {
                'crewName': 'test_crew',
                'crewId': '1',
                'taskName': 'test_task',
                'taskId': '1'
            }
        ]
        listeners = [
            {
                'name': 'test_listener',
                'crewId': '2',
                'listenToTaskIds': ['1'],
                'conditionType': 'OR',
                'tasks': [
                    {'id': '2'}
                ]
            }
        ]
        all_agents = {'1': mock_configured_agent, '2': mock_configured_agent}
        all_tasks = {'1': mock_configured_task, '2': mock_configured_task}

        with patch('src.engines.crewai.flow.modules.flow_builder.Crew') as mock_crew_class, \
             patch('src.engines.crewai.flow.modules.flow_builder.listen') as mock_listen, \
             patch('src.engines.crewai.flow.modules.flow_builder.or_') as mock_or:
            
            mock_crew = Mock()
            mock_crew.kickoff.return_value = "result"
            mock_crew_class.return_value = mock_crew
            
            # Mock the listen decorator and or_ function
            def mock_decorator(func):
                return func
            mock_listen.return_value = mock_decorator
            mock_or.return_value = Mock()

            result = await FlowBuilder._create_dynamic_flow(
                starting_points, listeners, all_agents, all_tasks
            )

            assert result is not None
            mock_or.assert_called()

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_empty_starting_points(self):
        """Test creation of dynamic flow with empty starting points."""
        starting_points = []
        listeners = []
        all_agents = {}
        all_tasks = {}

        result = await FlowBuilder._create_dynamic_flow(
            starting_points, listeners, all_agents, all_tasks
        )

        assert result is not None
        # Should not have any start methods
        start_methods = [method for method in dir(result) if method.startswith('start_flow_')]
        assert len(start_methods) == 0

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_missing_task(self, mock_configured_agent):
        """Test creation of dynamic flow when task is missing from all_tasks."""
        starting_points = [
            {
                'crewName': 'test_crew',
                'crewId': '1',
                'taskName': 'test_task',
                'taskId': 'missing_task'
            }
        ]
        listeners = []
        all_agents = {'1': mock_configured_agent}
        all_tasks = {}  # Empty, so task won't be found

        result = await FlowBuilder._create_dynamic_flow(
            starting_points, listeners, all_agents, all_tasks
        )

        assert result is not None
        # Should not have any start methods since task is missing
        start_methods = [method for method in dir(result) if method.startswith('start_flow_')]
        assert len(start_methods) == 0

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_listener_missing_tasks(self, mock_configured_agent, mock_configured_task):
        """Test creation of dynamic flow when listener has missing tasks."""
        starting_points = [
            {
                'crewName': 'test_crew',
                'crewId': '1',
                'taskName': 'test_task',
                'taskId': '1'
            }
        ]
        listeners = [
            {
                'name': 'test_listener',
                'crewId': '2',
                'listenToTaskIds': ['1'],
                'conditionType': 'NONE',
                'tasks': [
                    {'id': 'missing_task'}  # This task ID doesn't exist in all_tasks
                ]
            }
        ]
        all_agents = {'1': mock_configured_agent, '2': mock_configured_agent}
        all_tasks = {'1': mock_configured_task}  # Only has task '1', not 'missing_task'

        result = await FlowBuilder._create_dynamic_flow(
            starting_points, listeners, all_agents, all_tasks
        )

        assert result is not None
        # Should have start method but no listener methods since listener tasks are missing
        assert hasattr(result, 'start_flow_0')

    @pytest.mark.asyncio
    async def test_id_string_conversion(self, sample_repositories, mock_agent_data, 
                                      mock_configured_agent, mock_configured_task):
        """Test that IDs are properly converted to strings."""
        # Create special mock task data for this test
        mock_task_data = Mock()
        mock_task_data.name = "Test Task"
        mock_task_data.description = "Test description"
        mock_task_data.expected_output = "Test output"
        mock_task_data.agent_id = "1"  # Task has agent_id "1", so it should use that instead of crew_id
        mock_task_data.id = "456"
        
        starting_points = [
            {
                'crewName': 'test_crew',
                'crewId': 123,  # Integer ID
                'taskName': 'test_task',
                'taskId': 456   # Integer ID
            }
        ]
        all_agents = {}
        all_tasks = {}
        flow_data = {}

        # Setup repository mocks
        sample_repositories['task'].find_by_id.return_value = mock_task_data
        sample_repositories['agent'].find_by_id.return_value = mock_agent_data

        with patch('src.engines.crewai.flow.modules.flow_builder.AgentConfig') as mock_agent_config, \
             patch('src.engines.crewai.flow.modules.flow_builder.TaskConfig') as mock_task_config:
            
            mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=mock_configured_agent)
            mock_task_config.configure_task = AsyncMock(return_value=mock_configured_task)

            await FlowBuilder._process_starting_points(
                starting_points, all_agents, all_tasks, flow_data, sample_repositories, None
            )

            # Check that repositories were called with string IDs
            sample_repositories['task'].find_by_id.assert_called_with('456')
            # Should use agent_id from task (which is "1") not crew_id (which is "123")
            sample_repositories['agent'].find_by_id.assert_called_with('1')

    @pytest.mark.asyncio
    async def test_agent_id_fallback_to_crew_id(self, sample_repositories, mock_configured_agent, mock_configured_task):
        """Test that agent_id falls back to crew_id when task has no agent_id."""
        mock_task_data = Mock()
        mock_task_data.name = "Test Task"
        mock_task_data.description = "Test description"
        mock_task_data.expected_output = "Test output"
        mock_task_data.agent_id = None  # No agent_id
        mock_task_data.id = "1"

        mock_agent_data = Mock()
        mock_agent_data.name = "Test Agent"
        mock_agent_data.role = "Test Role"
        mock_agent_data.id = "1"

        starting_points = [
            {
                'crewName': 'test_crew',
                'crewId': '1',
                'taskName': 'test_task',
                'taskId': '1'
            }
        ]
        all_agents = {}
        all_tasks = {}
        flow_data = {}

        # Setup repository mocks
        sample_repositories['task'].find_by_id.return_value = mock_task_data
        sample_repositories['agent'].find_by_id.return_value = mock_agent_data

        with patch('src.engines.crewai.flow.modules.flow_builder.AgentConfig') as mock_agent_config, \
             patch('src.engines.crewai.flow.modules.flow_builder.TaskConfig') as mock_task_config:
            
            mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=mock_configured_agent)
            mock_task_config.configure_task = AsyncMock(return_value=mock_configured_task)

            await FlowBuilder._process_starting_points(
                starting_points, all_agents, all_tasks, flow_data, sample_repositories, None
            )

            # Should use crew_id ('1') as agent_id
            sample_repositories['agent'].find_by_id.assert_called_with('1')
            assert '1' in all_agents

    @pytest.mark.asyncio
    async def test_build_flow_exception_handling(self, sample_repositories):
        """Test flow building exception handling."""
        flow_data = {
            'flow_config': {
                'startingPoints': [
                    {
                        'crewName': 'test_crew',
                        'crewId': '1',
                        'taskName': 'test_task',
                        'taskId': '1'
                    }
                ]
            }
        }

        # Make repository throw an exception
        sample_repositories['task'].find_by_id.side_effect = Exception("Database error")

        with pytest.raises(ValueError, match="Failed to build flow"):
            await FlowBuilder.build_flow(flow_data, sample_repositories)

    @pytest.mark.asyncio
    async def test_process_starting_points_no_task_id(self, sample_repositories):
        """Test processing starting points with no task_id."""
        starting_points = [
            {
                'crewName': 'test_crew',
                'crewId': '1',
                'taskName': 'test_task',
                'taskId': None  # No task_id
            }
        ]
        all_agents = {}
        all_tasks = {}
        flow_data = {}

        await FlowBuilder._process_starting_points(
            starting_points, all_agents, all_tasks, flow_data, sample_repositories, None
        )

        # Should not add anything
        assert len(all_agents) == 0
        assert len(all_tasks) == 0

    @pytest.mark.asyncio
    async def test_process_starting_points_no_crew_id(self, sample_repositories, mock_task_data, mock_configured_task):
        """Test processing starting points with no crew_id."""
        # Task with no agent_id and no crew_id fallback
        mock_task_data.agent_id = None
        
        starting_points = [
            {
                'crewName': 'test_crew',
                'crewId': None,  # No crew_id
                'taskName': 'test_task',
                'taskId': '1'
            }
        ]
        all_agents = {}
        all_tasks = {}
        flow_data = {}

        sample_repositories['task'].find_by_id.return_value = mock_task_data

        with patch('src.engines.crewai.flow.modules.flow_builder.TaskConfig') as mock_task_config:
            mock_task_config.configure_task = AsyncMock(return_value=mock_configured_task)

            await FlowBuilder._process_starting_points(
                starting_points, all_agents, all_tasks, flow_data, sample_repositories, None
            )

        # Should still add task even if no agent
        assert '1' in all_tasks

    @pytest.mark.asyncio
    async def test_process_starting_points_task_config_none(self, sample_repositories, mock_task_data, mock_agent_data, mock_configured_agent):
        """Test processing starting points when task config returns None."""
        starting_points = [
            {
                'crewName': 'test_crew',
                'crewId': '1',
                'taskName': 'test_task',
                'taskId': '1'
            }
        ]
        all_agents = {}
        all_tasks = {}
        flow_data = {}

        sample_repositories['task'].find_by_id.return_value = mock_task_data
        sample_repositories['agent'].find_by_id.return_value = mock_agent_data

        with patch('src.engines.crewai.flow.modules.flow_builder.AgentConfig') as mock_agent_config, \
             patch('src.engines.crewai.flow.modules.flow_builder.TaskConfig') as mock_task_config:
            
            mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=mock_configured_agent)
            mock_task_config.configure_task = AsyncMock(return_value=None)  # Returns None

            await FlowBuilder._process_starting_points(
                starting_points, all_agents, all_tasks, flow_data, sample_repositories, None
            )

            # Should add agent but not task since task config returned None
            assert '1' in all_agents
            assert '1' not in all_tasks

    @pytest.mark.asyncio
    async def test_process_starting_points_with_callbacks(self, sample_repositories, sample_callbacks, mock_task_data, mock_agent_data, mock_configured_agent, mock_configured_task):
        """Test processing starting points with streaming callbacks."""
        starting_points = [
            {
                'crewName': 'test_crew',
                'crewId': '1',
                'taskName': 'test_task',
                'taskId': '1'
            }
        ]
        all_agents = {}
        all_tasks = {}
        flow_data = {}

        sample_repositories['task'].find_by_id.return_value = mock_task_data
        sample_repositories['agent'].find_by_id.return_value = mock_agent_data

        with patch('src.engines.crewai.flow.modules.flow_builder.AgentConfig') as mock_agent_config, \
             patch('src.engines.crewai.flow.modules.flow_builder.TaskConfig') as mock_task_config:
            
            mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=mock_configured_agent)
            mock_task_config.configure_task = AsyncMock(return_value=mock_configured_task)

            await FlowBuilder._process_starting_points(
                starting_points, all_agents, all_tasks, flow_data, sample_repositories, sample_callbacks
            )

            # Should call TaskConfig.configure_task with the streaming callback
            mock_task_config.configure_task.assert_called_with(
                mock_task_data, mock_configured_agent, sample_callbacks['streaming'].execute, flow_data, sample_repositories
            )

    @pytest.mark.asyncio
    async def test_process_listeners_no_crew_id(self, sample_repositories):
        """Test processing listeners with no crew_id."""
        listeners = [
            {
                'name': 'test_listener',
                'crewId': None,  # No crew_id
                'listenToTaskIds': ['1'],
                'conditionType': 'NONE',
                'tasks': [
                    {'id': '2'}
                ]
            }
        ]
        all_agents = {}
        all_tasks = {}
        flow_data = {}

        await FlowBuilder._process_listeners(
            listeners, all_agents, all_tasks, flow_data, sample_repositories, None
        )

        # Should not add any agents since no crew_id
        assert len(all_agents) == 0

    @pytest.mark.asyncio
    async def test_process_listeners_no_tasks(self, sample_repositories, mock_agent_data, mock_configured_agent):
        """Test processing listeners with no tasks."""
        listeners = [
            {
                'name': 'test_listener',
                'crewId': '2',
                'listenToTaskIds': ['1'],
                'conditionType': 'NONE',
                'tasks': []  # No tasks
            }
        ]
        all_agents = {}
        all_tasks = {}
        flow_data = {}

        sample_repositories['agent'].find_by_id.return_value = mock_agent_data

        with patch('src.engines.crewai.flow.modules.flow_builder.AgentConfig') as mock_agent_config:
            mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=mock_configured_agent)

            await FlowBuilder._process_listeners(
                listeners, all_agents, all_tasks, flow_data, sample_repositories, None
            )

        # Should add agent but no tasks
        assert '2' in all_agents
        assert len(all_tasks) == 0

    @pytest.mark.asyncio
    async def test_process_listeners_task_not_found(self, sample_repositories, mock_agent_data, mock_configured_agent):
        """Test processing listeners when task is not found."""
        listeners = [
            {
                'name': 'test_listener',
                'crewId': '2',
                'listenToTaskIds': ['1'],
                'conditionType': 'NONE',
                'tasks': [
                    {'id': '2'}
                ]
            }
        ]
        all_agents = {}
        all_tasks = {}
        flow_data = {}

        sample_repositories['agent'].find_by_id.return_value = mock_agent_data
        sample_repositories['task'].find_by_id.return_value = None  # Task not found

        with patch('src.engines.crewai.flow.modules.flow_builder.AgentConfig') as mock_agent_config:
            mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=mock_configured_agent)

            await FlowBuilder._process_listeners(
                listeners, all_agents, all_tasks, flow_data, sample_repositories, None
            )

        # Should add agent but no tasks since task not found
        assert '2' in all_agents
        assert len(all_tasks) == 0

    @pytest.mark.asyncio
    async def test_process_listeners_no_agent_for_task(self, sample_repositories, mock_task_data):
        """Test processing listeners when no agent is available for task."""
        mock_task_data.agent_id = 'missing_agent'
        
        listeners = [
            {
                'name': 'test_listener',
                'crewId': '2',
                'listenToTaskIds': ['1'],
                'conditionType': 'NONE',
                'tasks': [
                    {'id': '2'}
                ]
            }
        ]
        all_agents = {}  # No agents available
        all_tasks = {}
        flow_data = {}

        sample_repositories['task'].find_by_id.return_value = mock_task_data

        await FlowBuilder._process_listeners(
            listeners, all_agents, all_tasks, flow_data, sample_repositories, None
        )

        # Should not add any tasks since no agent available
        assert len(all_tasks) == 0

    @pytest.mark.asyncio
    async def test_process_listeners_task_config_none(self, sample_repositories, mock_task_data, mock_agent_data, mock_configured_agent):
        """Test processing listeners when task config returns None."""
        listeners = [
            {
                'name': 'test_listener',
                'crewId': '2',
                'listenToTaskIds': ['1'],
                'conditionType': 'NONE',
                'tasks': [
                    {'id': '2'}
                ]
            }
        ]
        all_agents = {'2': mock_configured_agent}  # Agent already available
        all_tasks = {}
        flow_data = {}

        sample_repositories['task'].find_by_id.return_value = mock_task_data

        with patch('src.engines.crewai.flow.modules.flow_builder.TaskConfig') as mock_task_config:
            mock_task_config.configure_task = AsyncMock(return_value=None)  # Returns None

            await FlowBuilder._process_listeners(
                listeners, all_agents, all_tasks, flow_data, sample_repositories, None
            )

        # Should not add task since config returned None
        assert len(all_tasks) == 0

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_listener_no_listen_to_task_ids(self, mock_configured_agent, mock_configured_task):
        """Test creation of dynamic flow when listener has no listenToTaskIds."""
        starting_points = [
            {
                'crewName': 'test_crew',
                'crewId': '1',
                'taskName': 'test_task',
                'taskId': '1'
            }
        ]
        listeners = [
            {
                'name': 'test_listener',
                'crewId': '2',
                'listenToTaskIds': [],  # Empty list
                'conditionType': 'NONE',
                'tasks': [
                    {'id': '2'}
                ]
            }
        ]
        all_agents = {'1': mock_configured_agent, '2': mock_configured_agent}
        all_tasks = {'1': mock_configured_task, '2': mock_configured_task}

        with patch('src.engines.crewai.flow.modules.flow_builder.Crew') as mock_crew_class:
            mock_crew = Mock()
            mock_crew.kickoff.return_value = "result"
            mock_crew_class.return_value = mock_crew

            result = await FlowBuilder._create_dynamic_flow(
                starting_points, listeners, all_agents, all_tasks
            )

            assert result is not None
            # Should have start method but no listener methods since no listenToTaskIds
            assert hasattr(result, 'start_flow_0')

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_listener_task_not_in_all_tasks(self, mock_configured_agent, mock_configured_task):
        """Test creation of dynamic flow when listener task ID is not in all_tasks."""
        starting_points = [
            {
                'crewName': 'test_crew',
                'crewId': '1',
                'taskName': 'test_task',
                'taskId': '1'
            }
        ]
        listeners = [
            {
                'name': 'test_listener',
                'crewId': '2',
                'listenToTaskIds': ['missing_task'],  # Task not in all_tasks
                'conditionType': 'NONE',
                'tasks': [
                    {'id': '2'}
                ]
            }
        ]
        all_agents = {'1': mock_configured_agent, '2': mock_configured_agent}
        all_tasks = {'1': mock_configured_task, '2': mock_configured_task}  # missing_task not here

        with patch('src.engines.crewai.flow.modules.flow_builder.Crew') as mock_crew_class:
            mock_crew = Mock()
            mock_crew.kickoff.return_value = "result"
            mock_crew_class.return_value = mock_crew

            result = await FlowBuilder._create_dynamic_flow(
                starting_points, listeners, all_agents, all_tasks
            )

            assert result is not None
            # Should have start method but no listener methods since listen task not found
            assert hasattr(result, 'start_flow_0')

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_and_condition_single_task(self, mock_configured_agent, mock_configured_task):
        """Test creation of dynamic flow with AND condition but only one task."""
        starting_points = [
            {
                'crewName': 'test_crew',
                'crewId': '1',
                'taskName': 'test_task',
                'taskId': '1'
            }
        ]
        listeners = [
            {
                'name': 'test_listener',
                'crewId': '2',
                'listenToTaskIds': ['1'],  # Only one task
                'conditionType': 'AND',
                'tasks': [
                    {'id': '2'}
                ]
            }
        ]
        all_agents = {'1': mock_configured_agent, '2': mock_configured_agent}
        all_tasks = {'1': mock_configured_task, '2': mock_configured_task}

        with patch('src.engines.crewai.flow.modules.flow_builder.Crew') as mock_crew_class, \
             patch('src.engines.crewai.flow.modules.flow_builder.listen') as mock_listen, \
             patch('src.engines.crewai.flow.modules.flow_builder.and_') as mock_and:
            
            mock_crew = Mock()
            mock_crew.kickoff.return_value = "result"
            mock_crew_class.return_value = mock_crew
            
            def mock_decorator(func):
                return func
            mock_listen.return_value = mock_decorator
            mock_and.return_value = Mock()

            result = await FlowBuilder._create_dynamic_flow(
                starting_points, listeners, all_agents, all_tasks
            )

            assert result is not None

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_or_condition_single_task(self, mock_configured_agent, mock_configured_task):
        """Test creation of dynamic flow with OR condition but only one task."""
        starting_points = [
            {
                'crewName': 'test_crew',
                'crewId': '1',
                'taskName': 'test_task',
                'taskId': '1'
            }
        ]
        listeners = [
            {
                'name': 'test_listener',
                'crewId': '2',
                'listenToTaskIds': ['1'],  # Only one task
                'conditionType': 'OR',
                'tasks': [
                    {'id': '2'}
                ]
            }
        ]
        all_agents = {'1': mock_configured_agent, '2': mock_configured_agent}
        all_tasks = {'1': mock_configured_task, '2': mock_configured_task}

        with patch('src.engines.crewai.flow.modules.flow_builder.Crew') as mock_crew_class, \
             patch('src.engines.crewai.flow.modules.flow_builder.listen') as mock_listen, \
             patch('src.engines.crewai.flow.modules.flow_builder.or_') as mock_or:
            
            mock_crew = Mock()
            mock_crew.kickoff.return_value = "result"
            mock_crew_class.return_value = mock_crew
            
            def mock_decorator(func):
                return func
            mock_listen.return_value = mock_decorator
            mock_or.return_value = Mock()

            result = await FlowBuilder._create_dynamic_flow(
                starting_points, listeners, all_agents, all_tasks
            )

            assert result is not None

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_and_condition_no_matching_methods(self, mock_configured_agent, mock_configured_task):
        """Test creation of dynamic flow with AND condition but no matching start methods."""
        starting_points = [
            {
                'crewName': 'test_crew',
                'crewId': '1',
                'taskName': 'test_task',
                'taskId': '1'
            }
        ]
        listeners = [
            {
                'name': 'test_listener',
                'crewId': '2',
                'listenToTaskIds': ['999'],  # Task ID not in starting points
                'conditionType': 'AND',
                'tasks': [
                    {'id': '2'}
                ]
            }
        ]
        all_agents = {'1': mock_configured_agent, '2': mock_configured_agent}
        all_tasks = {'1': mock_configured_task, '2': mock_configured_task, '999': mock_configured_task}

        with patch('src.engines.crewai.flow.modules.flow_builder.Crew') as mock_crew_class, \
             patch('src.engines.crewai.flow.modules.flow_builder.listen') as mock_listen, \
             patch('src.engines.crewai.flow.modules.flow_builder.and_') as mock_and:
            
            mock_crew = Mock()
            mock_crew.kickoff.return_value = "result"
            mock_crew_class.return_value = mock_crew
            
            def mock_decorator(func):
                return func
            mock_listen.return_value = mock_decorator
            mock_and.return_value = Mock()

            result = await FlowBuilder._create_dynamic_flow(
                starting_points, listeners, all_agents, all_tasks
            )

            assert result is not None

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_agent_without_tools_log(self, mock_configured_task):
        """Test creation of dynamic flow logs when agent has no tools."""
        # Create agent without tools
        mock_agent_no_tools = Mock()
        mock_agent_no_tools.role = "Test Role"
        mock_agent_no_tools.tools = []  # No tools
        
        mock_configured_task.agent = mock_agent_no_tools
        
        starting_points = [
            {
                'crewName': 'test_crew',
                'crewId': '1',
                'taskName': 'test_task',
                'taskId': '1'
            }
        ]
        listeners = []
        all_agents = {'1': mock_agent_no_tools}
        all_tasks = {'1': mock_configured_task}

        with patch('src.engines.crewai.flow.modules.flow_builder.Crew') as mock_crew_class:
            mock_crew = Mock()
            mock_crew.kickoff.return_value = "result"
            mock_crew_class.return_value = mock_crew

            result = await FlowBuilder._create_dynamic_flow(
                starting_points, listeners, all_agents, all_tasks
            )

            assert result is not None
            assert hasattr(result, 'start_flow_0')

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_agent_no_tools_attribute(self, mock_configured_task):
        """Test creation of dynamic flow when agent has no tools attribute."""
        # Create agent without tools attribute
        mock_agent_no_tools_attr = Mock()
        mock_agent_no_tools_attr.role = "Test Role"
        # Don't set tools attribute at all
        if hasattr(mock_agent_no_tools_attr, 'tools'):
            delattr(mock_agent_no_tools_attr, 'tools')
        
        mock_configured_task.agent = mock_agent_no_tools_attr
        
        starting_points = [
            {
                'crewName': 'test_crew',
                'crewId': '1',
                'taskName': 'test_task',
                'taskId': '1'
            }
        ]
        listeners = []
        all_agents = {'1': mock_agent_no_tools_attr}
        all_tasks = {'1': mock_configured_task}

        with patch('src.engines.crewai.flow.modules.flow_builder.Crew') as mock_crew_class:
            mock_crew = Mock()
            mock_crew.kickoff.return_value = "result"
            mock_crew_class.return_value = mock_crew

            result = await FlowBuilder._create_dynamic_flow(
                starting_points, listeners, all_agents, all_tasks
            )

            assert result is not None
            assert hasattr(result, 'start_flow_0')

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_listener_agents_no_tools(self, mock_configured_agent, mock_configured_task):
        """Test creation of dynamic flow listener when agents have no tools."""
        # Create agents without tools
        mock_agent_no_tools = Mock()
        mock_agent_no_tools.role = "Test Role"
        mock_agent_no_tools.tools = []
        
        mock_task_with_no_tools_agent = Mock()
        mock_task_with_no_tools_agent.description = "Test description"
        mock_task_with_no_tools_agent.agent = mock_agent_no_tools
        
        starting_points = [
            {
                'crewName': 'test_crew',
                'crewId': '1',
                'taskName': 'test_task',
                'taskId': '1'
            }
        ]
        listeners = [
            {
                'name': 'test_listener',
                'crewId': '2',
                'listenToTaskIds': ['1'],
                'conditionType': 'NONE',
                'tasks': [
                    {'id': '2'}
                ]
            }
        ]
        all_agents = {'1': mock_configured_agent, '2': mock_agent_no_tools}
        all_tasks = {'1': mock_configured_task, '2': mock_task_with_no_tools_agent}

        with patch('src.engines.crewai.flow.modules.flow_builder.Crew') as mock_crew_class, \
             patch('src.engines.crewai.flow.modules.flow_builder.listen') as mock_listen:
            
            mock_crew = Mock()
            mock_crew.kickoff.return_value = "result"
            mock_crew_class.return_value = mock_crew
            
            def mock_decorator(func):
                return func
            mock_listen.return_value = mock_decorator

            result = await FlowBuilder._create_dynamic_flow(
                starting_points, listeners, all_agents, all_tasks
            )

            assert result is not None

    @pytest.mark.asyncio
    async def test_build_flow_empty_flow_config_fallback(self):
        """Test flow building with empty flow_config that can't be parsed as JSON."""
        flow_data = {'flow_config': ''}  # Empty string

        with pytest.raises(ValueError, match="Failed to build flow"):
            await FlowBuilder.build_flow(flow_data)

    @pytest.mark.asyncio
    async def test_process_listeners_agent_already_exists(self, sample_repositories, mock_configured_agent, mock_configured_task):
        """Test processing listeners when agent already exists in all_agents."""
        # Create mock task data with specific ID for this test
        mock_task_data = Mock()
        mock_task_data.name = "Test Task"
        mock_task_data.description = "Test description"
        mock_task_data.expected_output = "Test output"
        mock_task_data.agent_id = "2"
        mock_task_data.id = "2"
        
        listeners = [
            {
                'name': 'test_listener',
                'crewId': '2',
                'listenToTaskIds': ['1'],
                'conditionType': 'NONE',
                'tasks': [
                    {'id': '2'}
                ]
            }
        ]
        all_agents = {'2': mock_configured_agent}  # Agent already exists
        all_tasks = {}
        flow_data = {}

        sample_repositories['task'].find_by_id.return_value = mock_task_data

        with patch('src.engines.crewai.flow.modules.flow_builder.TaskConfig') as mock_task_config:
            mock_task_config.configure_task = AsyncMock(return_value=mock_configured_task)

            await FlowBuilder._process_listeners(
                listeners, all_agents, all_tasks, flow_data, sample_repositories, None
            )

        # Should use existing agent and add task
        assert '2' in all_agents
        assert '2' in all_tasks

    @pytest.mark.asyncio
    async def test_process_listeners_with_callbacks(self, sample_repositories, sample_callbacks, mock_configured_agent, mock_configured_task):
        """Test processing listeners with streaming callbacks."""
        # Create mock task data with specific ID for this test
        mock_task_data = Mock()
        mock_task_data.name = "Test Task"
        mock_task_data.description = "Test description"
        mock_task_data.expected_output = "Test output"
        mock_task_data.agent_id = "2"
        mock_task_data.id = "2"
        
        listeners = [
            {
                'name': 'test_listener',
                'crewId': '2',
                'listenToTaskIds': ['1'],
                'conditionType': 'NONE',
                'tasks': [
                    {'id': '2'}
                ]
            }
        ]
        all_agents = {'2': mock_configured_agent}  # Agent already exists
        all_tasks = {}
        flow_data = {}

        sample_repositories['task'].find_by_id.return_value = mock_task_data

        with patch('src.engines.crewai.flow.modules.flow_builder.TaskConfig') as mock_task_config:
            mock_task_config.configure_task = AsyncMock(return_value=mock_configured_task)

            await FlowBuilder._process_listeners(
                listeners, all_agents, all_tasks, flow_data, sample_repositories, sample_callbacks
            )

            # Should call TaskConfig.configure_task with the streaming callback
            mock_task_config.configure_task.assert_called_with(
                mock_task_data, mock_configured_agent, sample_callbacks['streaming'].execute, flow_data, sample_repositories
            )

    @pytest.mark.asyncio
    async def test_build_flow_with_empty_dict_flow_config(self):
        """Test flow building with flow_config set to empty dict (falsy)."""
        flow_data = {'flow_config': {}}

        with pytest.raises(ValueError, match="Failed to build flow"):
            await FlowBuilder.build_flow(flow_data)

    @pytest.mark.asyncio
    async def test_build_flow_callbacks_without_streaming_key(self, sample_flow_data, sample_repositories, mock_task_data, mock_agent_data, mock_configured_agent, mock_configured_task):
        """Test flow building with callbacks that don't have 'streaming' key."""
        callbacks_without_streaming = {'other_callback': Mock()}

        sample_repositories['task'].find_by_id.return_value = mock_task_data
        sample_repositories['agent'].find_by_id.return_value = mock_agent_data

        with patch('src.engines.crewai.flow.modules.flow_builder.AgentConfig') as mock_agent_config, \
             patch('src.engines.crewai.flow.modules.flow_builder.TaskConfig') as mock_task_config:
            
            mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=mock_configured_agent)
            mock_task_config.configure_task = AsyncMock(return_value=mock_configured_task)

            result = await FlowBuilder.build_flow(sample_flow_data, sample_repositories, callbacks_without_streaming)

            assert result is not None
            # Should call TaskConfig.configure_task with None as callback since 'streaming' key not found
            mock_task_config.configure_task.assert_called_with(
                mock_task_data, mock_configured_agent, None, sample_flow_data, sample_repositories
            )

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_multiple_listen_to_task_ids_and_condition(self, mock_configured_agent, mock_configured_task):
        """Test creation of dynamic flow with multiple listenToTaskIds and AND condition."""
        starting_points = [
            {
                'crewName': 'test_crew1',
                'crewId': '1',
                'taskName': 'test_task1',
                'taskId': '1'
            },
            {
                'crewName': 'test_crew2',
                'crewId': '2',
                'taskName': 'test_task2',
                'taskId': '2'
            }
        ]
        listeners = [
            {
                'name': 'test_listener',
                'crewId': '3',
                'listenToTaskIds': ['1', '2'],  # Multiple tasks
                'conditionType': 'AND',
                'tasks': [
                    {'id': '3'}
                ]
            }
        ]
        
        all_agents = {'1': mock_configured_agent, '2': mock_configured_agent, '3': mock_configured_agent}
        all_tasks = {'1': mock_configured_task, '2': mock_configured_task, '3': mock_configured_task}

        with patch('src.engines.crewai.flow.modules.flow_builder.Crew') as mock_crew_class, \
             patch('src.engines.crewai.flow.modules.flow_builder.listen') as mock_listen, \
             patch('src.engines.crewai.flow.modules.flow_builder.and_') as mock_and:
            
            mock_crew = Mock()
            mock_crew.kickoff.return_value = "result"
            mock_crew_class.return_value = mock_crew
            
            def mock_decorator(func):
                return func
            mock_listen.return_value = mock_decorator
            mock_and.return_value = Mock()

            result = await FlowBuilder._create_dynamic_flow(
                starting_points, listeners, all_agents, all_tasks
            )

            assert result is not None
            mock_and.assert_called()

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_multiple_listen_to_task_ids_or_condition(self, mock_configured_agent, mock_configured_task):
        """Test creation of dynamic flow with multiple listenToTaskIds and OR condition."""
        starting_points = [
            {
                'crewName': 'test_crew1',
                'crewId': '1',
                'taskName': 'test_task1',
                'taskId': '1'
            },
            {
                'crewName': 'test_crew2',
                'crewId': '2',
                'taskName': 'test_task2',
                'taskId': '2'
            }
        ]
        listeners = [
            {
                'name': 'test_listener',
                'crewId': '3',
                'listenToTaskIds': ['1', '2'],  # Multiple tasks
                'conditionType': 'OR',
                'tasks': [
                    {'id': '3'}
                ]
            }
        ]
        
        all_agents = {'1': mock_configured_agent, '2': mock_configured_agent, '3': mock_configured_agent}
        all_tasks = {'1': mock_configured_task, '2': mock_configured_task, '3': mock_configured_task}

        with patch('src.engines.crewai.flow.modules.flow_builder.Crew') as mock_crew_class, \
             patch('src.engines.crewai.flow.modules.flow_builder.listen') as mock_listen, \
             patch('src.engines.crewai.flow.modules.flow_builder.or_') as mock_or:
            
            mock_crew = Mock()
            mock_crew.kickoff.return_value = "result"
            mock_crew_class.return_value = mock_crew
            
            def mock_decorator(func):
                return func
            mock_listen.return_value = mock_decorator
            mock_or.return_value = Mock()

            result = await FlowBuilder._create_dynamic_flow(
                starting_points, listeners, all_agents, all_tasks
            )

            assert result is not None
            mock_or.assert_called()

    @pytest.mark.asyncio
    async def test_process_listeners_task_already_in_all_tasks(self, sample_repositories, mock_configured_agent):
        """Test processing listeners when task is already in all_tasks."""
        listeners = [
            {
                'name': 'test_listener',
                'crewId': '2',
                'listenToTaskIds': ['1'],
                'conditionType': 'NONE',
                'tasks': [
                    {'id': '2'}  # This task will already be in all_tasks
                ]
            }
        ]
        all_agents = {'2': mock_configured_agent}
        all_tasks = {'2': Mock()}  # Task already exists
        flow_data = {}

        await FlowBuilder._process_listeners(
            listeners, all_agents, all_tasks, flow_data, sample_repositories, None
        )

        # Should not try to process task since it's already in all_tasks
        sample_repositories['task'].find_by_id.assert_not_called()

    @pytest.mark.asyncio
    async def test_build_flow_with_flow_config_none_value(self):
        """Test flow building with flow_config set to None."""
        flow_data = {'flow_config': None}

        with pytest.raises(ValueError, match="Failed to build flow"):
            await FlowBuilder.build_flow(flow_data)

    @pytest.mark.asyncio
    async def test_create_dynamic_flow_listener_agents_unique_deduplication(self, mock_configured_agent, mock_configured_task):
        """Test creation of dynamic flow listener with agent deduplication in crew creation."""
        mock_agent1 = Mock()
        mock_agent1.role = "Test Role 1"
        mock_agent1.tools = []
        
        # Create tasks that share the same agent (should be deduplicated)
        mock_task1 = Mock()
        mock_task1.description = "Test description 1"
        mock_task1.agent = mock_agent1
        
        mock_task2 = Mock()
        mock_task2.description = "Test description 2"
        mock_task2.agent = mock_agent1  # Same agent as task1
        
        starting_points = [
            {
                'crewName': 'test_crew',
                'crewId': '1',
                'taskName': 'test_task',
                'taskId': '1'
            }
        ]
        listeners = [
            {
                'name': 'test_listener',
                'crewId': '2',
                'listenToTaskIds': ['1'],
                'conditionType': 'NONE',
                'tasks': [
                    {'id': '2'},
                    {'id': '3'}
                ]
            }
        ]
        
        all_agents = {'1': mock_agent1, '2': mock_agent1}
        all_tasks = {'1': mock_configured_task, '2': mock_task1, '3': mock_task2}  # Tasks 2 and 3 both use agent1

        with patch('src.engines.crewai.flow.modules.flow_builder.Crew') as mock_crew_class, \
             patch('src.engines.crewai.flow.modules.flow_builder.listen') as mock_listen:
            
            mock_crew = Mock()
            mock_crew.kickoff.return_value = "result"
            mock_crew_class.return_value = mock_crew
            
            def mock_decorator(func):
                return func
            mock_listen.return_value = mock_decorator

            result = await FlowBuilder._create_dynamic_flow(
                starting_points, listeners, all_agents, all_tasks
            )

            assert result is not None

    @pytest.mark.asyncio
    async def test_build_flow_no_flow_config_key_in_data(self):
        """Test flow building with no flow_config key."""
        flow_data = {'other_key': 'value'}

        with pytest.raises(ValueError, match="Failed to build flow"):
            await FlowBuilder.build_flow(flow_data)