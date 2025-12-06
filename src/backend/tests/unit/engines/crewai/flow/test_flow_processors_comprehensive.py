"""
Comprehensive unit tests for flow_processors.py module.
Target: 80%+ coverage
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import uuid


class TestFlowProcessorManager:
    """Tests for FlowProcessorManager class."""

    @pytest.fixture
    def mock_repositories(self):
        """Create mock repositories."""
        return {
            'task': AsyncMock(),
            'agent': AsyncMock(),
            'crew': AsyncMock(),
        }

    @pytest.fixture
    def mock_group_context(self):
        """Create mock group context."""
        context = MagicMock()
        context.primary_group_id = str(uuid.uuid4())
        return context

    @pytest.fixture
    def mock_callbacks(self):
        """Create mock callbacks."""
        return {
            'task_callback': MagicMock(),
        }


class TestProcessStartingPoints:
    """Tests for process_starting_points method."""

    @pytest.fixture
    def mock_repositories(self):
        """Create mock repositories."""
        return {
            'task': AsyncMock(),
            'agent': AsyncMock(),
            'crew': AsyncMock(),
        }

    @pytest.mark.asyncio
    async def test_process_starting_points_no_task_repo(self):
        """Test process_starting_points returns empty when no task repo."""
        from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager

        flow_config = {'startingPoints': [{'taskId': 'task-1', 'crewId': 'crew-1'}]}
        all_tasks = {}

        result = await FlowProcessorManager.process_starting_points(
            flow_config, all_tasks, None
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_process_starting_points_missing_task_id(self, mock_repositories):
        """Test process_starting_points skips entries without task_id."""
        from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager

        flow_config = {'startingPoints': [{'crewId': 'crew-1'}]}  # Missing taskId
        all_tasks = {}

        result = await FlowProcessorManager.process_starting_points(
            flow_config, all_tasks, mock_repositories
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_process_starting_points_missing_crew_id(self, mock_repositories):
        """Test process_starting_points skips entries without crew_id."""
        from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager

        flow_config = {'startingPoints': [{'taskId': 'task-1'}]}  # Missing crewId
        all_tasks = {}

        result = await FlowProcessorManager.process_starting_points(
            flow_config, all_tasks, mock_repositories
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_process_starting_points_crew_not_found(self, mock_repositories):
        """Test process_starting_points handles crew not found."""
        from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager

        flow_config = {'startingPoints': [{'taskId': 'task-1', 'crewId': 'crew-1'}]}
        all_tasks = {}

        mock_repositories['crew'].get = AsyncMock(return_value=None)

        result = await FlowProcessorManager.process_starting_points(
            flow_config, all_tasks, mock_repositories
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_process_starting_points_task_not_found(self, mock_repositories):
        """Test process_starting_points handles task not found."""
        from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager

        task_id = str(uuid.uuid4())
        crew_id = str(uuid.uuid4())

        flow_config = {'startingPoints': [{'taskId': task_id, 'crewId': crew_id}]}
        all_tasks = {}

        mock_crew = MagicMock()
        mock_crew.name = "Test Crew"
        mock_crew.nodes = []
        mock_crew.edges = []
        mock_repositories['crew'].get = AsyncMock(return_value=mock_crew)
        mock_repositories['task'].get = AsyncMock(return_value=None)

        result = await FlowProcessorManager.process_starting_points(
            flow_config, all_tasks, mock_repositories
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_process_starting_points_success(self, mock_repositories):
        """Test process_starting_points successfully processes starting points."""
        from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager

        task_id = str(uuid.uuid4())
        crew_id = str(uuid.uuid4())
        agent_id = str(uuid.uuid4())

        flow_config = {'startingPoints': [{'taskId': task_id, 'crewId': crew_id}]}
        all_tasks = {}

        # Create mock crew with nodes and edges
        mock_crew = MagicMock()
        mock_crew.name = "Test Crew"
        mock_crew.nodes = [
            {'id': f'taskNode-{task_id}', 'type': 'taskNode'},
            {'id': f'agentNode-{agent_id}', 'type': 'agentNode'},
        ]
        mock_crew.edges = [
            {'source': f'agentNode-{agent_id}', 'target': f'taskNode-{task_id}'},
        ]
        mock_crew.tool_configs = {}

        mock_task = MagicMock()
        mock_task.agent_id = agent_id
        mock_task.async_execution = False
        mock_task.tool_configs = {}

        mock_agent = MagicMock()
        mock_agent.role = "Test Agent"

        mock_repositories['crew'].get = AsyncMock(return_value=mock_crew)
        mock_repositories['task'].get = AsyncMock(return_value=mock_task)
        mock_repositories['agent'].get = AsyncMock(return_value=mock_agent)

        # Mock the AgentConfig and TaskConfig at their source modules (imports happen inside functions)
        with patch('src.engines.crewai.flow.modules.agent_config.AgentConfig') as mock_agent_config, \
             patch('src.engines.crewai.flow.modules.task_config.TaskConfig') as mock_task_config:
            mock_agent_obj = MagicMock()
            mock_agent_obj.role = "Test Agent"
            mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=mock_agent_obj)

            mock_task_obj = MagicMock()
            mock_task_obj.agent = mock_agent_obj
            mock_task_config.configure_task = AsyncMock(return_value=mock_task_obj)

            result = await FlowProcessorManager.process_starting_points(
                flow_config, all_tasks, mock_repositories
            )

            assert len(result) == 1
            assert result[0][0] == 'starting_point_0'

    @pytest.mark.asyncio
    async def test_process_starting_points_groups_by_crew(self, mock_repositories):
        """Test process_starting_points groups tasks by crew_id."""
        from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager

        task_id_1 = str(uuid.uuid4())
        task_id_2 = str(uuid.uuid4())
        crew_id = str(uuid.uuid4())
        agent_id = str(uuid.uuid4())

        flow_config = {
            'startingPoints': [
                {'taskId': task_id_1, 'crewId': crew_id},
                {'taskId': task_id_2, 'crewId': crew_id},  # Same crew
            ]
        }
        all_tasks = {}

        mock_crew = MagicMock()
        mock_crew.name = "Test Crew"
        mock_crew.nodes = [
            {'id': f'taskNode-{task_id_1}', 'type': 'taskNode'},
            {'id': f'taskNode-{task_id_2}', 'type': 'taskNode'},
            {'id': f'agentNode-{agent_id}', 'type': 'agentNode'},
        ]
        mock_crew.edges = [
            {'source': f'agentNode-{agent_id}', 'target': f'taskNode-{task_id_1}'},
            {'source': f'agentNode-{agent_id}', 'target': f'taskNode-{task_id_2}'},
        ]
        mock_crew.tool_configs = {}

        mock_task = MagicMock()
        mock_task.agent_id = agent_id
        mock_task.async_execution = False
        mock_task.tool_configs = {}

        mock_agent = MagicMock()
        mock_agent.role = "Test Agent"

        mock_repositories['crew'].get = AsyncMock(return_value=mock_crew)
        mock_repositories['task'].get = AsyncMock(return_value=mock_task)
        mock_repositories['agent'].get = AsyncMock(return_value=mock_agent)

        with patch('src.engines.crewai.flow.modules.agent_config.AgentConfig') as mock_agent_config, \
             patch('src.engines.crewai.flow.modules.task_config.TaskConfig') as mock_task_config, \
             patch('crewai.Task') as mock_crewai_task:
            mock_agent_obj = MagicMock()
            mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=mock_agent_obj)

            mock_task_obj = MagicMock()
            mock_task_obj.agent = mock_agent_obj
            mock_task_config.configure_task = AsyncMock(return_value=mock_task_obj)

            # Mock crewai.Task to return a MagicMock instead of trying pydantic validation
            mock_crewai_task.return_value = MagicMock()

            result = await FlowProcessorManager.process_starting_points(
                flow_config, all_tasks, mock_repositories
            )

            # Should be grouped into 1 crew
            assert len(result) == 1
            # Should have 3 tasks in the crew (2 original + 1 auto-completion task for parallel execution)
            assert len(result[0][1]) == 3

    @pytest.mark.asyncio
    async def test_process_starting_points_with_async_tasks(self, mock_repositories):
        """Test process_starting_points handles async tasks correctly."""
        from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager

        task_id_1 = str(uuid.uuid4())
        task_id_2 = str(uuid.uuid4())
        crew_id = str(uuid.uuid4())
        agent_id = str(uuid.uuid4())

        flow_config = {
            'startingPoints': [
                {'taskId': task_id_1, 'crewId': crew_id},
                {'taskId': task_id_2, 'crewId': crew_id},
            ]
        }
        all_tasks = {}

        mock_crew = MagicMock()
        mock_crew.name = "Test Crew"
        mock_crew.nodes = [
            {'id': f'taskNode-{task_id_1}', 'type': 'taskNode'},
            {'id': f'taskNode-{task_id_2}', 'type': 'taskNode'},
            {'id': f'agentNode-{agent_id}', 'type': 'agentNode'},
        ]
        mock_crew.edges = [
            {'source': f'agentNode-{agent_id}', 'target': f'taskNode-{task_id_1}'},
            {'source': f'agentNode-{agent_id}', 'target': f'taskNode-{task_id_2}'},
        ]
        mock_crew.tool_configs = {}

        # Create async tasks
        mock_task = MagicMock()
        mock_task.agent_id = agent_id
        mock_task.async_execution = True  # Async execution
        mock_task.tool_configs = {}

        mock_agent = MagicMock()
        mock_agent.role = "Test Agent"

        mock_repositories['crew'].get = AsyncMock(return_value=mock_crew)
        mock_repositories['task'].get = AsyncMock(return_value=mock_task)
        mock_repositories['agent'].get = AsyncMock(return_value=mock_agent)

        with patch('src.engines.crewai.flow.modules.agent_config.AgentConfig') as mock_agent_config, \
             patch('src.engines.crewai.flow.modules.task_config.TaskConfig') as mock_task_config, \
             patch('crewai.Task') as mock_crewai_task:
            mock_agent_obj = MagicMock()
            mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=mock_agent_obj)

            mock_task_obj = MagicMock()
            mock_task_obj.agent = mock_agent_obj
            mock_task_obj.async_execution = True
            mock_task_config.configure_task = AsyncMock(return_value=mock_task_obj)

            mock_completion_task = MagicMock()
            mock_crewai_task.return_value = mock_completion_task

            result = await FlowProcessorManager.process_starting_points(
                flow_config, all_tasks, mock_repositories
            )

            assert len(result) == 1


class TestProcessListeners:
    """Tests for process_listeners method."""

    @pytest.fixture
    def mock_repositories(self):
        """Create mock repositories."""
        return {
            'task': AsyncMock(),
            'agent': AsyncMock(),
            'crew': AsyncMock(),
        }

    @pytest.mark.asyncio
    async def test_process_listeners_no_task_repo(self):
        """Test process_listeners returns empty when no task repo."""
        from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager

        flow_config = {'listeners': [{'crewId': 'crew-1'}]}
        all_tasks = {}

        result = await FlowProcessorManager.process_listeners(
            flow_config, all_tasks, None
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_process_listeners_missing_crew_id(self, mock_repositories):
        """Test process_listeners skips entries without crew_id."""
        from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager

        flow_config = {'listeners': [{'tasks': []}]}  # Missing crewId
        all_tasks = {}

        result = await FlowProcessorManager.process_listeners(
            flow_config, all_tasks, mock_repositories
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_process_listeners_skips_router_type(self, mock_repositories):
        """Test process_listeners skips ROUTER condition type."""
        from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager

        flow_config = {
            'listeners': [
                {'crewId': 'crew-1', 'conditionType': 'ROUTER', 'tasks': []}
            ]
        }
        all_tasks = {}

        result = await FlowProcessorManager.process_listeners(
            flow_config, all_tasks, mock_repositories
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_process_listeners_crew_not_found(self, mock_repositories):
        """Test process_listeners handles crew not found."""
        from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager

        crew_id = str(uuid.uuid4())
        flow_config = {
            'listeners': [
                {'crewId': crew_id, 'tasks': [{'id': 'task-1'}], 'listenToTaskIds': ['task-0']}
            ]
        }
        all_tasks = {}

        mock_repositories['crew'].get = AsyncMock(return_value=None)

        result = await FlowProcessorManager.process_listeners(
            flow_config, all_tasks, mock_repositories
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_process_listeners_no_tasks(self, mock_repositories):
        """Test process_listeners handles no tasks in listener."""
        from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager

        crew_id = str(uuid.uuid4())
        flow_config = {
            'listeners': [
                {'crewId': crew_id, 'tasks': [], 'listenToTaskIds': ['task-0']}
            ]
        }
        all_tasks = {}

        result = await FlowProcessorManager.process_listeners(
            flow_config, all_tasks, mock_repositories
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_process_listeners_success(self, mock_repositories):
        """Test process_listeners successfully processes listeners."""
        from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager

        task_id = str(uuid.uuid4())
        crew_id = str(uuid.uuid4())
        agent_id = str(uuid.uuid4())

        flow_config = {
            'listeners': [
                {
                    'crewId': crew_id,
                    'tasks': [{'id': task_id}],
                    'listenToTaskIds': ['start-task'],
                    'conditionType': 'NONE',
                }
            ]
        }
        all_tasks = {}

        mock_crew = MagicMock()
        mock_crew.name = "Listener Crew"
        mock_crew.nodes = [
            {'id': f'taskNode-{task_id}', 'type': 'taskNode'},
            {'id': f'agentNode-{agent_id}', 'type': 'agentNode'},
        ]
        mock_crew.edges = [
            {'source': f'agentNode-{agent_id}', 'target': f'taskNode-{task_id}'},
        ]
        mock_crew.tool_configs = {}

        mock_task = MagicMock()
        mock_task.agent_id = agent_id
        mock_task.async_execution = False
        mock_task.tool_configs = {}

        mock_agent = MagicMock()
        mock_agent.role = "Test Agent"

        mock_repositories['crew'].get = AsyncMock(return_value=mock_crew)
        mock_repositories['task'].get = AsyncMock(return_value=mock_task)
        mock_repositories['agent'].get = AsyncMock(return_value=mock_agent)

        with patch('src.engines.crewai.flow.modules.agent_config.AgentConfig') as mock_agent_config, \
             patch('src.engines.crewai.flow.modules.task_config.TaskConfig') as mock_task_config:
            mock_agent_obj = MagicMock()
            mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=mock_agent_obj)

            mock_task_obj = MagicMock()
            mock_task_obj.agent = mock_agent_obj
            mock_task_config.configure_task = AsyncMock(return_value=mock_task_obj)

            result = await FlowProcessorManager.process_listeners(
                flow_config, all_tasks, mock_repositories
            )

            assert len(result) == 1
            assert result[0][0] == 'listener_0'

    @pytest.mark.asyncio
    async def test_process_listeners_groups_by_crew(self, mock_repositories):
        """Test process_listeners groups listeners by crew_id."""
        from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager

        task_id_1 = str(uuid.uuid4())
        task_id_2 = str(uuid.uuid4())
        crew_id = str(uuid.uuid4())
        agent_id = str(uuid.uuid4())

        flow_config = {
            'listeners': [
                {
                    'crewId': crew_id,
                    'tasks': [{'id': task_id_1}],
                    'listenToTaskIds': ['start-task'],
                    'conditionType': 'NONE',
                },
                {
                    'crewId': crew_id,  # Same crew
                    'tasks': [{'id': task_id_2}],
                    'listenToTaskIds': ['start-task'],
                    'conditionType': 'NONE',
                },
            ]
        }
        all_tasks = {}

        mock_crew = MagicMock()
        mock_crew.name = "Listener Crew"
        mock_crew.nodes = [
            {'id': f'taskNode-{task_id_1}', 'type': 'taskNode'},
            {'id': f'taskNode-{task_id_2}', 'type': 'taskNode'},
            {'id': f'agentNode-{agent_id}', 'type': 'agentNode'},
        ]
        mock_crew.edges = [
            {'source': f'agentNode-{agent_id}', 'target': f'taskNode-{task_id_1}'},
            {'source': f'agentNode-{agent_id}', 'target': f'taskNode-{task_id_2}'},
        ]
        mock_crew.tool_configs = {}

        mock_task = MagicMock()
        mock_task.agent_id = agent_id
        mock_task.async_execution = False
        mock_task.tool_configs = {}

        mock_agent = MagicMock()
        mock_agent.role = "Test Agent"

        mock_repositories['crew'].get = AsyncMock(return_value=mock_crew)
        mock_repositories['task'].get = AsyncMock(return_value=mock_task)
        mock_repositories['agent'].get = AsyncMock(return_value=mock_agent)

        with patch('src.engines.crewai.flow.modules.agent_config.AgentConfig') as mock_agent_config, \
             patch('src.engines.crewai.flow.modules.task_config.TaskConfig') as mock_task_config:
            mock_agent_obj = MagicMock()
            mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=mock_agent_obj)

            mock_task_obj = MagicMock()
            mock_task_obj.agent = mock_agent_obj
            mock_task_config.configure_task = AsyncMock(return_value=mock_task_obj)

            result = await FlowProcessorManager.process_listeners(
                flow_config, all_tasks, mock_repositories
            )

            # Should be grouped into 1 crew
            assert len(result) == 1
            # Should have 2 tasks
            assert len(result[0][2]) == 2

    @pytest.mark.asyncio
    async def test_process_listeners_auto_and_condition(self, mock_repositories):
        """Test process_listeners auto-sets AND condition for multiple listen targets."""
        from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager

        task_id = str(uuid.uuid4())
        crew_id = str(uuid.uuid4())
        agent_id = str(uuid.uuid4())

        flow_config = {
            'listeners': [
                {
                    'crewId': crew_id,
                    'tasks': [{'id': task_id}],
                    'listenToTaskIds': ['task-1', 'task-2'],  # Multiple listen targets
                    'conditionType': 'NONE',
                },
            ]
        }
        all_tasks = {}

        mock_crew = MagicMock()
        mock_crew.name = "Listener Crew"
        mock_crew.nodes = [
            {'id': f'taskNode-{task_id}', 'type': 'taskNode'},
            {'id': f'agentNode-{agent_id}', 'type': 'agentNode'},
        ]
        mock_crew.edges = [
            {'source': f'agentNode-{agent_id}', 'target': f'taskNode-{task_id}'},
        ]
        mock_crew.tool_configs = {}

        mock_task = MagicMock()
        mock_task.agent_id = agent_id
        mock_task.async_execution = False
        mock_task.tool_configs = {}

        mock_agent = MagicMock()
        mock_agent.role = "Test Agent"

        mock_repositories['crew'].get = AsyncMock(return_value=mock_crew)
        mock_repositories['task'].get = AsyncMock(return_value=mock_task)
        mock_repositories['agent'].get = AsyncMock(return_value=mock_agent)

        with patch('src.engines.crewai.flow.modules.agent_config.AgentConfig') as mock_agent_config, \
             patch('src.engines.crewai.flow.modules.task_config.TaskConfig') as mock_task_config:
            mock_agent_obj = MagicMock()
            mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=mock_agent_obj)

            mock_task_obj = MagicMock()
            mock_task_obj.agent = mock_agent_obj
            mock_task_config.configure_task = AsyncMock(return_value=mock_task_obj)

            result = await FlowProcessorManager.process_listeners(
                flow_config, all_tasks, mock_repositories
            )

            assert len(result) == 1
            # Should have AND condition type
            assert result[0][6] == 'AND'


class TestProcessRouters:
    """Tests for process_routers method."""

    @pytest.fixture
    def mock_repositories(self):
        """Create mock repositories."""
        return {
            'task': AsyncMock(),
            'agent': AsyncMock(),
            'crew': AsyncMock(),
        }

    @pytest.mark.asyncio
    async def test_process_routers_no_task_repo(self):
        """Test process_routers returns empty when no task repo."""
        from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager

        flow_config = {'routers': [{'name': 'router-1'}]}
        all_tasks = {}

        result = await FlowProcessorManager.process_routers(
            flow_config, all_tasks, None
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_process_routers_missing_listen_to(self, mock_repositories):
        """Test process_routers skips entries without listenTo."""
        from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager

        flow_config = {'routers': [{'name': 'router-1', 'routes': {}}]}  # Missing listenTo
        all_tasks = {}

        result = await FlowProcessorManager.process_routers(
            flow_config, all_tasks, mock_repositories
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_process_routers_empty_routes(self, mock_repositories):
        """Test process_routers handles empty routes."""
        from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager

        flow_config = {
            'routers': [
                {'name': 'router-1', 'listenTo': 'starting_point_0', 'routes': {}}
            ]
        }
        all_tasks = {}

        result = await FlowProcessorManager.process_routers(
            flow_config, all_tasks, mock_repositories
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_process_routers_route_no_crew_id(self, mock_repositories):
        """Test process_routers handles route without crew_id."""
        from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager

        task_id = str(uuid.uuid4())
        flow_config = {
            'routers': [
                {
                    'name': 'router-1',
                    'listenTo': 'starting_point_0',
                    'routes': {'success': [{'id': task_id}]},  # No crewId
                    'routeConditions': {},
                }
            ]
        }
        all_tasks = {}

        result = await FlowProcessorManager.process_routers(
            flow_config, all_tasks, mock_repositories
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_process_routers_crew_not_found(self, mock_repositories):
        """Test process_routers handles crew not found."""
        from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager

        task_id = str(uuid.uuid4())
        crew_id = str(uuid.uuid4())
        flow_config = {
            'routers': [
                {
                    'name': 'router-1',
                    'listenTo': 'starting_point_0',
                    'routes': {'success': [{'id': task_id, 'crewId': crew_id}]},
                    'routeConditions': {},
                }
            ]
        }
        all_tasks = {}

        mock_repositories['crew'].get = AsyncMock(return_value=None)

        result = await FlowProcessorManager.process_routers(
            flow_config, all_tasks, mock_repositories
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_process_routers_success(self, mock_repositories):
        """Test process_routers successfully processes routers."""
        from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager

        task_id = str(uuid.uuid4())
        crew_id = str(uuid.uuid4())
        agent_id = str(uuid.uuid4())

        flow_config = {
            'routers': [
                {
                    'name': 'router-1',
                    'listenTo': 'starting_point_0',
                    'routes': {'success': [{'id': task_id, 'crewId': crew_id}]},
                    'routeConditions': {'success': 'state.get("result") == True'},
                }
            ]
        }
        all_tasks = {}

        mock_crew = MagicMock()
        mock_crew.name = "Route Crew"
        mock_crew.nodes = [
            {'id': f'taskNode-{task_id}', 'type': 'taskNode'},
            {'id': f'agentNode-{agent_id}', 'type': 'agentNode'},
        ]
        mock_crew.edges = [
            {'source': f'agentNode-{agent_id}', 'target': f'taskNode-{task_id}'},
        ]
        mock_crew.tool_configs = {}

        mock_task = MagicMock()
        mock_task.agent_id = agent_id
        mock_task.tool_configs = {}

        mock_agent = MagicMock()
        mock_agent.role = "Test Agent"

        mock_repositories['crew'].get = AsyncMock(return_value=mock_crew)
        mock_repositories['task'].get = AsyncMock(return_value=mock_task)
        mock_repositories['agent'].get = AsyncMock(return_value=mock_agent)

        with patch('src.engines.crewai.flow.modules.agent_config.AgentConfig') as mock_agent_config, \
             patch('src.engines.crewai.flow.modules.task_config.TaskConfig') as mock_task_config:
            mock_agent_obj = MagicMock()
            mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=mock_agent_obj)

            mock_task_obj = MagicMock()
            mock_task_obj.agent = mock_agent_obj
            mock_task_config.configure_task = AsyncMock(return_value=mock_task_obj)

            result = await FlowProcessorManager.process_routers(
                flow_config, all_tasks, mock_repositories
            )

            assert len(result) == 1
            assert result[0][0] == 'router_0'

    @pytest.mark.asyncio
    async def test_process_routers_multiple_routes(self, mock_repositories):
        """Test process_routers handles multiple routes."""
        from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager

        task_id_1 = str(uuid.uuid4())
        task_id_2 = str(uuid.uuid4())
        crew_id = str(uuid.uuid4())
        agent_id = str(uuid.uuid4())

        flow_config = {
            'routers': [
                {
                    'name': 'router-1',
                    'listenTo': 'starting_point_0',
                    'routes': {
                        'success': [{'id': task_id_1, 'crewId': crew_id}],
                        'failure': [{'id': task_id_2, 'crewId': crew_id}],
                    },
                    'routeConditions': {
                        'success': 'state.get("result") == True',
                        'failure': 'state.get("result") == False',
                    },
                }
            ]
        }
        all_tasks = {}

        mock_crew = MagicMock()
        mock_crew.name = "Route Crew"
        mock_crew.nodes = [
            {'id': f'taskNode-{task_id_1}', 'type': 'taskNode'},
            {'id': f'taskNode-{task_id_2}', 'type': 'taskNode'},
            {'id': f'agentNode-{agent_id}', 'type': 'agentNode'},
        ]
        mock_crew.edges = [
            {'source': f'agentNode-{agent_id}', 'target': f'taskNode-{task_id_1}'},
            {'source': f'agentNode-{agent_id}', 'target': f'taskNode-{task_id_2}'},
        ]
        mock_crew.tool_configs = {}

        mock_task = MagicMock()
        mock_task.agent_id = agent_id
        mock_task.tool_configs = {}

        mock_agent = MagicMock()
        mock_agent.role = "Test Agent"

        mock_repositories['crew'].get = AsyncMock(return_value=mock_crew)
        mock_repositories['task'].get = AsyncMock(return_value=mock_task)
        mock_repositories['agent'].get = AsyncMock(return_value=mock_agent)

        with patch('src.engines.crewai.flow.modules.agent_config.AgentConfig') as mock_agent_config, \
             patch('src.engines.crewai.flow.modules.task_config.TaskConfig') as mock_task_config:
            mock_agent_obj = MagicMock()
            mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=mock_agent_obj)

            mock_task_obj = MagicMock()
            mock_task_obj.agent = mock_agent_obj
            mock_task_config.configure_task = AsyncMock(return_value=mock_task_obj)

            result = await FlowProcessorManager.process_routers(
                flow_config, all_tasks, mock_repositories
            )

            assert len(result) == 1
            # Should have 2 routes
            assert len(result[0][1]) == 2

    @pytest.mark.asyncio
    async def test_process_routers_exception_handling(self, mock_repositories):
        """Test process_routers handles exceptions gracefully."""
        from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager

        task_id = str(uuid.uuid4())
        crew_id = str(uuid.uuid4())

        flow_config = {
            'routers': [
                {
                    'name': 'router-1',
                    'listenTo': 'starting_point_0',
                    'routes': {'success': [{'id': task_id, 'crewId': crew_id}]},
                    'routeConditions': {},
                }
            ]
        }
        all_tasks = {}

        mock_repositories['crew'].get = AsyncMock(side_effect=Exception("Database error"))

        result = await FlowProcessorManager.process_routers(
            flow_config, all_tasks, mock_repositories
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_process_routers_task_not_found(self, mock_repositories):
        """Test process_routers handles task not found."""
        from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager

        task_id = str(uuid.uuid4())
        crew_id = str(uuid.uuid4())

        flow_config = {
            'routers': [
                {
                    'name': 'router-1',
                    'listenTo': 'starting_point_0',
                    'routes': {'success': [{'id': task_id, 'crewId': crew_id}]},
                    'routeConditions': {},
                }
            ]
        }
        all_tasks = {}

        mock_crew = MagicMock()
        mock_crew.name = "Route Crew"
        mock_crew.nodes = []
        mock_crew.edges = []
        mock_crew.tool_configs = {}

        mock_repositories['crew'].get = AsyncMock(return_value=mock_crew)
        mock_repositories['task'].get = AsyncMock(return_value=None)

        result = await FlowProcessorManager.process_routers(
            flow_config, all_tasks, mock_repositories
        )

        assert result == []
