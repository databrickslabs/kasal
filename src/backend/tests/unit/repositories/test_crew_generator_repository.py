"""
Unit tests for CrewGeneratorRepository.

Tests the functionality of crew generation repository including
agent creation, task creation, agent-task mapping, dependency resolution, and error handling.
"""
import pytest
import uuid
import json
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone
from typing import List, Dict, Any

from sqlalchemy.ext.asyncio import AsyncSession

from src.repositories.crew_generator_repository import CrewGeneratorRepository
from src.models.agent import Agent
from src.models.task import Task


# Mock agent model
class MockAgent:
    def __init__(self, id=None, name="Test Agent", role="Analyst", goal="Analyze data",
                 backstory="Expert analyst", llm="gpt-4", tools=None, allow_delegation=False,
                 verbose=False, max_iter=25, max_rpm=10, cache=True, allow_code_execution=False,
                 code_execution_mode="safe", max_retry_limit=2, use_system_prompt=True,
                 respect_context_window=True, function_calling_llm=None,
                 created_at=None, updated_at=None, group_id=None):
        self.id = id or str(uuid.uuid4())
        self.name = name
        self.role = role
        self.goal = goal
        self.backstory = backstory
        self.llm = llm
        self.tools = tools or []
        self.allow_delegation = allow_delegation
        self.verbose = verbose
        self.max_iter = max_iter
        self.max_rpm = max_rpm
        self.cache = cache
        self.allow_code_execution = allow_code_execution
        self.code_execution_mode = code_execution_mode
        self.max_retry_limit = max_retry_limit
        self.use_system_prompt = use_system_prompt
        self.respect_context_window = respect_context_window
        self.function_calling_llm = function_calling_llm
        self.created_at = created_at or datetime.now(timezone.utc)
        self.updated_at = updated_at or datetime.now(timezone.utc)
        self.group_id = group_id


# Mock task model
# Sentinel value to distinguish between None passed explicitly and default
_CONTEXT_DEFAULT = object()

class MockTask:
    def __init__(self, id=None, name="Test Task", description="Test task description",
                 agent_id=None, expected_output="Test output", tools=None, async_execution=False,
                 context=_CONTEXT_DEFAULT, output=None, human_input=False, markdown=False,
                 created_at=None, updated_at=None, group_id=None):
        self.id = id or str(uuid.uuid4())
        self.name = name
        self.description = description
        self.agent_id = agent_id
        self.expected_output = expected_output
        self.tools = tools or []
        self.async_execution = async_execution
        self.context = [] if context is _CONTEXT_DEFAULT else context
        self.output = output
        self.human_input = human_input
        self.markdown = markdown
        self.created_at = created_at or datetime.now(timezone.utc)
        self.updated_at = updated_at or datetime.now(timezone.utc)
        self.group_id = group_id


@pytest.fixture
def crew_generator_repository():
    """Create a crew generator repository."""
    return CrewGeneratorRepository()


@pytest.fixture
def sample_crew_dict():
    """Create sample crew data for testing."""
    return {
        "agents": [
            {
                "name": "Data Analyst",
                "role": "Senior Data Analyst",
                "goal": "Analyze complex datasets",
                "backstory": "Expert in data analysis with 10 years experience",
                "llm": "gpt-4",
                "tools": ["python", "sql"],
                "allow_delegation": True,
                "verbose": True
            },
            {
                "name": "Report Writer",
                "role": "Technical Writer",
                "goal": "Create comprehensive reports",
                "backstory": "Skilled technical writer",
                "llm": "gpt-3.5-turbo",
                "tools": ["markdown", "charts"]
            }
        ],
        "tasks": [
            {
                "name": "Data Analysis Task",
                "description": "Analyze the sales data",
                "agent": "Data Analyst",
                "expected_output": "Analysis report",
                "tools": ["python"],
                "_context_refs": []
            },
            {
                "name": "Report Generation Task",
                "description": "Generate final report",
                "agent": "Report Writer", 
                "expected_output": "Final report",
                "tools": ["markdown"],
                "_context_refs": ["Data Analysis Task"]
            }
        ]
    }


@pytest.fixture
def sample_agents_data():
    """Create sample agent data for testing."""
    return [
        {
            "name": "Researcher",
            "role": "Research Specialist",
            "goal": "Conduct thorough research",
            "backstory": "PhD in research methodology",
            "llm": "gpt-4",
            "tools": ["web_search", "academic_db"],
            "verbose": True,
            "max_iter": 30
        },
        {
            "name": "Writer",
            "role": "Content Creator",
            "goal": "Write engaging content",
            "backstory": "Professional writer with 5 years experience",
            "llm": "claude-3"
        }
    ]


@pytest.fixture
def sample_tasks_data():
    """Create sample task data for testing."""
    return [
        {
            "name": "Research Task",
            "description": "Research the topic thoroughly",
            "agent": "Researcher",
            "expected_output": "Research summary",
            "tools": ["web_search"],
            "_context_refs": []
        },
        {
            "name": "Writing Task",
            "description": "Write the final article",
            "agent": "Writer",
            "expected_output": "Article draft",
            "tools": ["markdown"],
            "_context_refs": ["Research Task"]
        }
    ]


class TestCrewGeneratorRepositoryInit:
    """Test cases for CrewGeneratorRepository initialization and factory."""
    
    def test_create_factory_method(self):
        """Test factory method creates repository instance."""
        repository = CrewGeneratorRepository.create_instance()
        assert isinstance(repository, CrewGeneratorRepository)
    
    def test_direct_initialization(self):
        """Test direct initialization."""
        repository = CrewGeneratorRepository()
        assert isinstance(repository, CrewGeneratorRepository)


class TestCrewGeneratorRepositorySafeGetAttr:
    """Test cases for _safe_get_attr utility method."""
    
    def test_safe_get_attr_dictionary(self, crew_generator_repository):
        """Test safe get attribute with dictionary."""
        data = {"name": "test", "value": 42}
        
        assert crew_generator_repository._safe_get_attr(data, "name") == "test"
        assert crew_generator_repository._safe_get_attr(data, "value") == 42
        assert crew_generator_repository._safe_get_attr(data, "missing") is None
        assert crew_generator_repository._safe_get_attr(data, "missing", "default") == "default"
    
    def test_safe_get_attr_object(self, crew_generator_repository):
        """Test safe get attribute with object."""
        class TestObj:
            name = "test"
            value = 42
        
        obj = TestObj()
        
        assert crew_generator_repository._safe_get_attr(obj, "name") == "test"
        assert crew_generator_repository._safe_get_attr(obj, "value") == 42
        assert crew_generator_repository._safe_get_attr(obj, "missing") is None
        assert crew_generator_repository._safe_get_attr(obj, "missing", "default") == "default"
    
    def test_safe_get_attr_dict_like_object(self, crew_generator_repository):
        """Test safe get attribute with dict-like object."""
        class DictLike:
            def get(self, key, default=None):
                data = {"name": "test", "value": 42}
                return data.get(key, default)
        
        obj = DictLike()
        
        assert crew_generator_repository._safe_get_attr(obj, "name") == "test"
        assert crew_generator_repository._safe_get_attr(obj, "value") == 42
        assert crew_generator_repository._safe_get_attr(obj, "missing") is None
    
    def test_safe_get_attr_invalid_object(self, crew_generator_repository):
        """Test safe get attribute with invalid object."""
        invalid_obj = "not an object"
        
        assert crew_generator_repository._safe_get_attr(invalid_obj, "anything") is None
        assert crew_generator_repository._safe_get_attr(invalid_obj, "anything", "default") == "default"


class TestCrewGeneratorRepositoryCreate:
    """Test cases for create method."""
    
    @pytest.mark.asyncio
    async def test_create_entity_success(self, crew_generator_repository):
        """Test successful entity creation."""
        mock_entity = MockAgent()
        
        with patch('src.repositories.crew_generator_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            result = await crew_generator_repository.create(mock_entity)
            
            assert result == mock_entity
            mock_session.add.assert_called_once_with(mock_entity)
            mock_session.commit.assert_called_once()
            mock_session.refresh.assert_called_once_with(mock_entity)
    
    @pytest.mark.asyncio
    async def test_create_entity_database_error(self, crew_generator_repository):
        """Test entity creation with database error."""
        mock_entity = MockAgent()
        
        with patch('src.repositories.crew_generator_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            mock_session.commit.side_effect = Exception("Database error")
            
            with patch('src.repositories.crew_generator_repository.logger') as mock_logger:
                with pytest.raises(Exception, match="Database error"):
                    await crew_generator_repository.create(mock_entity)
                
                mock_session.rollback.assert_called_once()
                mock_logger.error.assert_called()


class TestCrewGeneratorRepositoryUpdate:
    """Test cases for update method."""
    
    @pytest.mark.asyncio
    async def test_update_task_context_success(self, crew_generator_repository):
        """Test successful task context update."""
        task_id = "task-123"
        update_data = {"context": ["dep1", "dep2"]}
        mock_task = MockTask(id=task_id, context=[])
        
        with patch('src.repositories.crew_generator_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch('src.repositories.crew_generator_repository.TaskRepository') as mock_task_repo_class:
                mock_task_repo = AsyncMock()
                mock_task_repo_class.return_value = mock_task_repo
                mock_task_repo.get.return_value = mock_task
                
                result = await crew_generator_repository.update(task_id, update_data)
                
                assert result == mock_task
                assert mock_task.context == ["dep1", "dep2"]
                mock_session.commit.assert_called_once()
                mock_session.refresh.assert_called_once_with(mock_task)
    
    @pytest.mark.asyncio
    async def test_update_task_not_found(self, crew_generator_repository):
        """Test update when task not found."""
        task_id = "nonexistent"
        update_data = {"context": ["dep1"]}
        
        with patch('src.repositories.crew_generator_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch('src.repositories.crew_generator_repository.TaskRepository') as mock_task_repo_class:
                mock_task_repo = AsyncMock()
                mock_task_repo_class.return_value = mock_task_repo
                mock_task_repo.get.return_value = None
                
                with patch('src.repositories.crew_generator_repository.logger') as mock_logger:
                    result = await crew_generator_repository.update(task_id, update_data)
                    
                    assert result is None
                    mock_logger.error.assert_called()
    
    @pytest.mark.asyncio
    async def test_update_non_task_entity(self, crew_generator_repository):
        """Test update for non-task entity (not implemented)."""
        entity_id = "agent-123"
        update_data = {"name": "New Name"}
        
        with patch('src.repositories.crew_generator_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch('src.repositories.crew_generator_repository.logger') as mock_logger:
                result = await crew_generator_repository.update(entity_id, update_data)
                
                assert result is None
                mock_logger.error.assert_called()
    
    @pytest.mark.asyncio
    async def test_update_database_error(self, crew_generator_repository):
        """Test update with database error."""
        task_id = "task-123"
        update_data = {"context": ["dep1"]}
        
        with patch('src.repositories.crew_generator_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            mock_session.commit.side_effect = Exception("Update failed")
            
            with patch('src.repositories.crew_generator_repository.TaskRepository') as mock_task_repo_class:
                mock_task_repo = AsyncMock()
                mock_task_repo_class.return_value = mock_task_repo
                mock_task_repo.get.return_value = MockTask()
                
                with patch('src.repositories.crew_generator_repository.logger') as mock_logger:
                    with pytest.raises(Exception, match="Update failed"):
                        await crew_generator_repository.update(task_id, update_data)
                    
                    mock_session.rollback.assert_called_once()
                    mock_logger.error.assert_called()


class TestCrewGeneratorRepositoryCreateCrewEntities:
    """Test cases for create_crew_entities method."""
    
    @pytest.mark.asyncio
    async def test_create_crew_entities_success(self, crew_generator_repository, sample_crew_dict):
        """Test successful crew entities creation."""
        mock_agents = [
            MockAgent(id="agent-1", name="Data Analyst", group_id="group-123"),
            MockAgent(id="agent-2", name="Report Writer", group_id="group-123")
        ]
        mock_tasks = [
            MockTask(id="task-1", name="Data Analysis Task", agent_id="agent-1"),
            MockTask(id="task-2", name="Report Generation Task", agent_id="agent-2")
        ]
        
        with patch.object(crew_generator_repository, '_create_agents', return_value=mock_agents):
            with patch.object(crew_generator_repository, '_create_tasks', return_value=mock_tasks):
                with patch.object(crew_generator_repository, '_create_task_dependencies'):
                    result = await crew_generator_repository.create_crew_entities(sample_crew_dict)
                    
                    assert "agents" in result
                    assert "tasks" in result
                    assert len(result["agents"]) == 2
                    assert len(result["tasks"]) == 2
                    
                    # Check agent serialization
                    agent_dict = result["agents"][0]
                    assert agent_dict["id"] == "agent-1"
                    assert agent_dict["name"] == "Data Analyst"
                    assert "created_at" in agent_dict
                    
                    # Check task serialization
                    task_dict = result["tasks"][0]
                    assert task_dict["id"] == "task-1"
                    assert task_dict["name"] == "Data Analysis Task"
                    assert task_dict["agent_id"] == "agent-1"
    
    @pytest.mark.asyncio
    async def test_create_crew_entities_empty_data(self, crew_generator_repository):
        """Test crew creation with empty data."""
        empty_crew_dict = {"agents": [], "tasks": []}
        
        with patch.object(crew_generator_repository, '_create_agents', return_value=[]):
            with patch.object(crew_generator_repository, '_create_tasks', return_value=[]):
                with patch.object(crew_generator_repository, '_create_task_dependencies'):
                    result = await crew_generator_repository.create_crew_entities(empty_crew_dict)
                    
                    assert result["agents"] == []
                    assert result["tasks"] == []
    
    @pytest.mark.asyncio
    async def test_create_crew_entities_missing_keys(self, crew_generator_repository):
        """Test crew creation with missing agents/tasks keys."""
        incomplete_crew_dict = {}
        
        with patch.object(crew_generator_repository, '_create_agents', return_value=[]):
            with patch.object(crew_generator_repository, '_create_tasks', return_value=[]):
                with patch.object(crew_generator_repository, '_create_task_dependencies'):
                    result = await crew_generator_repository.create_crew_entities(incomplete_crew_dict)
                    
                    assert result["agents"] == []
                    assert result["tasks"] == []
    
    @pytest.mark.asyncio
    async def test_create_crew_entities_with_logging(self, crew_generator_repository, sample_crew_dict):
        """Test crew creation with proper logging."""
        mock_agents = [MockAgent(name="Test Agent", group_id="group-123")]
        mock_tasks = [MockTask(name="Test Task", group_id="group-123")]
        
        with patch.object(crew_generator_repository, '_create_agents', return_value=mock_agents):
            with patch.object(crew_generator_repository, '_create_tasks', return_value=mock_tasks):
                with patch.object(crew_generator_repository, '_create_task_dependencies'):
                    with patch('src.repositories.crew_generator_repository.logger') as mock_logger:
                        await crew_generator_repository.create_crew_entities(sample_crew_dict)
                        
                        # Verify logging was called
                        mock_logger.info.assert_called()


class TestCrewGeneratorRepositoryCreateAgents:
    """Test cases for _create_agents method."""
    
    @pytest.mark.asyncio
    async def test_create_agents_success(self, crew_generator_repository, sample_agents_data):
        """Test successful agent creation."""
        with patch.object(crew_generator_repository, 'create') as mock_create:
            with patch('src.repositories.crew_generator_repository.Agent') as mock_agent_class:
                # Create mock agents that will be returned
                mock_agents = [
                    MockAgent(name="Researcher", group_id="group-123"),
                    MockAgent(name="Writer", group_id="group-123")
                ]
                mock_agent_class.side_effect = mock_agents
                mock_create.side_effect = lambda agent: agent  # Return the agent as-is
                
                result = await crew_generator_repository._create_agents(sample_agents_data)
                
                assert len(result) == 2
                assert result[0].name == "Researcher"
                assert result[1].name == "Writer"
                assert mock_create.call_count == 2
    
    @pytest.mark.asyncio
    async def test_create_agents_with_defaults(self, crew_generator_repository):
        """Test agent creation with default values."""
        minimal_agent_data = [{"name": "Minimal Agent"}]
        
        with patch.object(crew_generator_repository, 'create') as mock_create:
            with patch('src.repositories.crew_generator_repository.Agent') as mock_agent_class:
                mock_agent = MockAgent(name="Minimal Agent", group_id="group-123")
                mock_agent_class.return_value = mock_agent
                mock_create.return_value = mock_agent
                
                result = await crew_generator_repository._create_agents(minimal_agent_data)
                
                assert len(result) == 1
                # Verify Agent was called with default values
                call_args = mock_agent_class.call_args[1]
                assert call_args["name"] == "Minimal Agent"
                assert call_args["allow_delegation"] is False
                assert call_args["verbose"] is False
                assert call_args["max_iter"] == 25
                assert call_args["cache"] is True
    
    @pytest.mark.asyncio
    async def test_create_agents_empty_list(self, crew_generator_repository):
        """Test agent creation with empty list."""
        result = await crew_generator_repository._create_agents([])
        assert result == []
    
    @pytest.mark.asyncio
    async def test_create_agents_creation_error(self, crew_generator_repository, sample_agents_data):
        """Test agent creation with creation error."""
        with patch.object(crew_generator_repository, 'create', side_effect=Exception("Creation failed")):
            with patch('src.repositories.crew_generator_repository.Agent') as mock_agent_class:
                mock_agent_class.return_value = MockAgent()
                
                with pytest.raises(Exception, match="Creation failed"):
                    await crew_generator_repository._create_agents(sample_agents_data)
    
    @pytest.mark.asyncio
    async def test_create_agents_with_logging(self, crew_generator_repository, sample_agents_data):
        """Test agent creation with proper logging."""
        with patch.object(crew_generator_repository, 'create') as mock_create:
            with patch('src.repositories.crew_generator_repository.Agent') as mock_agent_class:
                mock_agent = MockAgent()
                mock_agent_class.return_value = mock_agent
                mock_create.return_value = mock_agent
                
                with patch('src.repositories.crew_generator_repository.logger') as mock_logger:
                    await crew_generator_repository._create_agents(sample_agents_data)
                    
                    # Verify logging calls
                    mock_logger.info.assert_called()


class TestCrewGeneratorRepositoryCreateTasks:
    """Test cases for _create_tasks method."""
    
    @pytest.mark.asyncio
    async def test_create_tasks_success(self, crew_generator_repository, sample_tasks_data):
        """Test successful task creation with agent mapping."""
        agent_name_to_id = {
            "Researcher": "agent-1",
            "Writer": "agent-2"
        }
        
        with patch.object(crew_generator_repository, 'create') as mock_create:
            with patch('src.repositories.crew_generator_repository.Task') as mock_task_class:
                mock_tasks = [
                    MockTask(name="Research Task", agent_id="agent-1"),
                    MockTask(name="Writing Task", agent_id="agent-2")
                ]
                mock_task_class.side_effect = mock_tasks
                mock_create.side_effect = lambda task: task
                
                result = await crew_generator_repository._create_tasks(sample_tasks_data, agent_name_to_id)
                
                assert len(result) == 2
                assert result[0].name == "Research Task"
                assert result[0].agent_id == "agent-1"
                assert result[1].name == "Writing Task"
                assert result[1].agent_id == "agent-2"
    
    @pytest.mark.asyncio
    async def test_create_tasks_case_insensitive_agent_matching(self, crew_generator_repository):
        """Test task creation with case-insensitive agent matching."""
        tasks_data = [{"name": "Test Task", "agent": "researcher"}]  # lowercase
        agent_name_to_id = {"Researcher": "agent-1"}  # uppercase
        
        with patch.object(crew_generator_repository, 'create') as mock_create:
            with patch('src.repositories.crew_generator_repository.Task') as mock_task_class:
                mock_task = MockTask(agent_id="agent-1")
                mock_task_class.return_value = mock_task
                mock_create.return_value = mock_task
                
                result = await crew_generator_repository._create_tasks(tasks_data, agent_name_to_id)
                
                assert len(result) == 1
                assert result[0].agent_id == "agent-1"
    
    @pytest.mark.asyncio
    async def test_create_tasks_partial_agent_matching(self, crew_generator_repository):
        """Test task creation with partial agent name matching."""
        tasks_data = [{"name": "Test Task", "agent": "Data Analyst Expert"}]
        agent_name_to_id = {"Data Analyst": "agent-1", "Report Writer": "agent-2"}
        
        with patch.object(crew_generator_repository, 'create') as mock_create:
            with patch('src.repositories.crew_generator_repository.Task') as mock_task_class:
                mock_task = MockTask(agent_id="agent-1")
                mock_task_class.return_value = mock_task
                mock_create.return_value = mock_task
                
                with patch('src.repositories.crew_generator_repository.logger') as mock_logger:
                    result = await crew_generator_repository._create_tasks(tasks_data, agent_name_to_id)
                    
                    assert len(result) == 1
                    assert result[0].agent_id == "agent-1"
                    mock_logger.info.assert_called()
    
    @pytest.mark.asyncio
    async def test_create_tasks_round_robin_assignment(self, crew_generator_repository):
        """Test task creation with round-robin assignment when no agent specified."""
        tasks_data = [
            {"name": "Task 1"},  # No agent specified
            {"name": "Task 2"},  # No agent specified
            {"name": "Task 3"}   # No agent specified
        ]
        agent_name_to_id = {"Agent1": "agent-1", "Agent2": "agent-2"}
        
        with patch.object(crew_generator_repository, 'create') as mock_create:
            with patch('src.repositories.crew_generator_repository.Task') as mock_task_class:
                mock_tasks = [
                    MockTask(name="Task 1", agent_id="agent-1"),
                    MockTask(name="Task 2", agent_id="agent-2"),
                    MockTask(name="Task 3", agent_id="agent-1")  # Round-robin back to first
                ]
                mock_task_class.side_effect = mock_tasks
                mock_create.side_effect = lambda task: task
                
                result = await crew_generator_repository._create_tasks(tasks_data, agent_name_to_id)
                
                assert len(result) == 3
                assert result[0].agent_id == "agent-1"
                assert result[1].agent_id == "agent-2"
                assert result[2].agent_id == "agent-1"  # Round-robin
    
    @pytest.mark.asyncio
    async def test_create_tasks_no_agents_available(self, crew_generator_repository):
        """Test task creation when no agents are available."""
        tasks_data = [{"name": "Orphan Task"}]
        agent_name_to_id = {}
        
        with patch.object(crew_generator_repository, 'create') as mock_create:
            with patch('src.repositories.crew_generator_repository.Task') as mock_task_class:
                mock_task = MockTask(agent_id=None)
                mock_task_class.return_value = mock_task
                mock_create.return_value = mock_task
                
                with patch('src.repositories.crew_generator_repository.logger') as mock_logger:
                    result = await crew_generator_repository._create_tasks(tasks_data, agent_name_to_id)
                    
                    assert len(result) == 1
                    assert result[0].agent_id is None
                    mock_logger.warning.assert_called()
    
    @pytest.mark.asyncio
    async def test_create_tasks_agent_not_found_poor_match(self, crew_generator_repository):
        """Test task creation when agent name has poor match score."""
        tasks_data = [{"name": "Test Task", "agent": "Completely Different Name"}]
        agent_name_to_id = {"Data Analyst": "agent-1", "Report Writer": "agent-2"}
        
        with patch.object(crew_generator_repository, 'create') as mock_create:
            with patch('src.repositories.crew_generator_repository.Task') as mock_task_class:
                mock_task = MockTask(agent_id="agent-1")  # Should get round-robin assignment
                mock_task_class.return_value = mock_task
                mock_create.return_value = mock_task
                
                with patch('src.repositories.crew_generator_repository.logger') as mock_logger:
                    result = await crew_generator_repository._create_tasks(tasks_data, agent_name_to_id)
                    
                    assert len(result) == 1
                    # Should fall back to round-robin
                    mock_logger.warning.assert_called()
    
    @pytest.mark.asyncio
    async def test_create_tasks_assigned_agent_field(self, crew_generator_repository):
        """Test task creation using 'assigned_agent' field."""
        tasks_data = [{"name": "Test Task", "assigned_agent": "Researcher"}]
        agent_name_to_id = {"Researcher": "agent-1"}
        
        with patch.object(crew_generator_repository, 'create') as mock_create:
            with patch('src.repositories.crew_generator_repository.Task') as mock_task_class:
                mock_task = MockTask(agent_id="agent-1")
                mock_task_class.return_value = mock_task
                mock_create.return_value = mock_task
                
                result = await crew_generator_repository._create_tasks(tasks_data, agent_name_to_id)
                
                assert len(result) == 1
                assert result[0].agent_id == "agent-1"
    
    @pytest.mark.asyncio
    async def test_create_tasks_agent_substring_matching(self, crew_generator_repository):
        """Test task creation with agent substring matching."""
        tasks_data = [{"name": "Test Task", "agent": "Data"}]  # Partial name match
        agent_name_to_id = {"Senior Data Analyst": "agent-1", "Report Writer": "agent-2"}
        
        with patch.object(crew_generator_repository, 'create') as mock_create:
            with patch('src.repositories.crew_generator_repository.Task') as mock_task_class:
                mock_task = MockTask(agent_id="agent-1")
                mock_task_class.return_value = mock_task
                mock_create.return_value = mock_task
                
                with patch('src.repositories.crew_generator_repository.logger') as mock_logger:
                    result = await crew_generator_repository._create_tasks(tasks_data, agent_name_to_id)
                    
                    assert len(result) == 1
                    assert result[0].agent_id == "agent-1"  # Should match "Senior Data Analyst"
                    # Check that it found a best match (score calculation: "data" in "senior data analyst" gets 5 + "data" common word gets 3 = 8)
                    mock_logger.info.assert_any_call("Found best match for 'Data' -> 'Senior Data Analyst' with score 8: agent-1")

    @pytest.mark.asyncio
    async def test_create_tasks_with_defaults(self, crew_generator_repository):
        """Test task creation with default values."""
        tasks_data = [{"name": "Minimal Task"}]
        agent_name_to_id = {"Agent": "agent-1"}
        
        with patch.object(crew_generator_repository, 'create') as mock_create:
            with patch('src.repositories.crew_generator_repository.Task') as mock_task_class:
                mock_task = MockTask()
                mock_task_class.return_value = mock_task
                mock_create.return_value = mock_task
                
                result = await crew_generator_repository._create_tasks(tasks_data, agent_name_to_id)
                
                # Verify Task was called with default values
                call_args = mock_task_class.call_args[1]
                assert call_args["tools"] == []
                assert call_args["async_execution"] is False
                assert call_args["human_input"] is False
                assert call_args["markdown"] is False


class TestCrewGeneratorRepositoryCreateTaskDependencies:
    """Test cases for _create_task_dependencies method."""
    
    @pytest.mark.asyncio
    async def test_create_task_dependencies_success(self, crew_generator_repository):
        """Test successful task dependency creation."""
        created_tasks = [
            MockTask(id="task-1", name="Task 1", group_id="group-123"),
            MockTask(id="task-2", name="Task 2", group_id="group-123")
        ]
        tasks_data = [
            {"name": "Task 1", "_context_refs": []},
            {"name": "Task 2", "_context_refs": ["Task 1"]}
        ]
        
        with patch('src.repositories.crew_generator_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch('src.repositories.crew_generator_repository.TaskRepository'):
                await crew_generator_repository._create_task_dependencies(created_tasks, tasks_data)
                
                # Verify task 2 has dependency on task 1
                assert created_tasks[1].context == ["task-1"]
                mock_session.add_all.assert_called_once()
                mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_task_dependencies_no_references(self, crew_generator_repository):
        """Test dependency creation when tasks have no context references."""
        created_tasks = [
            MockTask(id="task-1", name="Independent Task", context=[])  # Already empty
        ]
        tasks_data = [
            {"name": "Independent Task", "_context_refs": []}
        ]
        
        with patch('src.repositories.crew_generator_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch('src.repositories.crew_generator_repository.TaskRepository'):
                with patch('src.repositories.crew_generator_repository.logger') as mock_logger:
                    await crew_generator_repository._create_task_dependencies(created_tasks, tasks_data)
                    
                    # Task should have empty context (no change needed)
                    assert created_tasks[0].context == []
                    # No updates needed since context was already []
                    mock_session.add_all.assert_not_called()
                    mock_session.commit.assert_not_called()
                    mock_logger.info.assert_any_call("No task context updates needed.")
    
    @pytest.mark.asyncio
    async def test_create_task_dependencies_self_dependency_skipped(self, crew_generator_repository):
        """Test that self-dependencies are skipped."""
        created_tasks = [
            MockTask(id="task-1", name="Self Referencing Task", group_id="group-123")
        ]
        tasks_data = [
            {"name": "Self Referencing Task", "_context_refs": ["Self Referencing Task"]}
        ]
        
        with patch('src.repositories.crew_generator_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch('src.repositories.crew_generator_repository.TaskRepository'):
                with patch('src.repositories.crew_generator_repository.logger') as mock_logger:
                    await crew_generator_repository._create_task_dependencies(created_tasks, tasks_data)
                    
                    # Self-dependency should be skipped, context should be empty
                    assert created_tasks[0].context == []
                    mock_logger.warning.assert_called()
    
    @pytest.mark.asyncio
    async def test_create_task_dependencies_unresolved_references(self, crew_generator_repository):
        """Test dependency creation with unresolved references."""
        created_tasks = [
            MockTask(id="task-1", name="Task 1", group_id="group-123")
        ]
        tasks_data = [
            {"name": "Task 1", "_context_refs": ["Nonexistent Task"]}
        ]
        
        with patch('src.repositories.crew_generator_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch('src.repositories.crew_generator_repository.TaskRepository'):
                with patch('src.repositories.crew_generator_repository.logger') as mock_logger:
                    await crew_generator_repository._create_task_dependencies(created_tasks, tasks_data)
                    
                    # Task should have empty context since reference couldn't be resolved
                    assert created_tasks[0].context == []
                    mock_logger.warning.assert_called()
    
    @pytest.mark.asyncio
    async def test_create_task_dependencies_duplicate_removal(self, crew_generator_repository):
        """Test that duplicate dependencies are removed."""
        created_tasks = [
            MockTask(id="task-1", name="Task 1", group_id="group-123"),
            MockTask(id="task-2", name="Task 2", group_id="group-123"),
            MockTask(id="task-3", name="Task 3", group_id="group-123")
        ]
        tasks_data = [
            {"name": "Task 1", "_context_refs": []},
            {"name": "Task 2", "_context_refs": []},
            {"name": "Task 3", "_context_refs": ["Task 1", "Task 2", "Task 1"]}  # Duplicate Task 1
        ]
        
        with patch('src.repositories.crew_generator_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch('src.repositories.crew_generator_repository.TaskRepository'):
                with patch('src.repositories.crew_generator_repository.logger') as mock_logger:
                    await crew_generator_repository._create_task_dependencies(created_tasks, tasks_data)
                    
                    # Task 3 should have only unique dependencies
                    assert len(created_tasks[2].context) == 2
                    assert "task-1" in created_tasks[2].context
                    assert "task-2" in created_tasks[2].context
                    mock_logger.info.assert_called()
    
    @pytest.mark.asyncio
    async def test_create_task_dependencies_task_not_found_in_db(self, crew_generator_repository):
        """Test dependency creation when task is not found in created tasks."""
        created_tasks = [
            MockTask(id="task-1", name="Task 1", group_id="group-123")
        ]
        tasks_data = [
            {"name": "Task 1", "_context_refs": []},
            {"name": "Orphan Task", "_context_refs": ["Task 1"]}  # Not in created_tasks
        ]
        
        with patch('src.repositories.crew_generator_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch('src.repositories.crew_generator_repository.TaskRepository'):
                with patch('src.repositories.crew_generator_repository.logger') as mock_logger:
                    await crew_generator_repository._create_task_dependencies(created_tasks, tasks_data)
                    
                    # Should log warning about orphan task not found
                    mock_logger.warning.assert_called()
    
    @pytest.mark.asyncio
    async def test_create_task_dependencies_database_error(self, crew_generator_repository):
        """Test dependency creation with database error."""
        created_tasks = [
            MockTask(id="task-1", name="Task 1", group_id="group-123"),
            MockTask(id="task-2", name="Task 2", group_id="group-123")
        ]
        tasks_data = [
            {"name": "Task 1", "_context_refs": []},
            {"name": "Task 2", "_context_refs": ["Task 1"]}  # This will trigger a dependency update
        ]
        
        with patch('src.repositories.crew_generator_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            mock_session.commit.side_effect = Exception("Commit failed")
            
            with patch('src.repositories.crew_generator_repository.TaskRepository'):
                with patch('src.repositories.crew_generator_repository.logger') as mock_logger:
                    with pytest.raises(Exception, match="Commit failed"):
                        await crew_generator_repository._create_task_dependencies(created_tasks, tasks_data)
                    
                    mock_session.rollback.assert_called_once()
                    mock_logger.error.assert_called()
    
    @pytest.mark.asyncio
    async def test_create_task_dependencies_no_updates_needed(self, crew_generator_repository):
        """Test dependency creation when no updates are needed."""
        created_tasks = [
            MockTask(id="task-1", name="Task 1", context=[])
        ]
        tasks_data = [
            {"name": "Task 1"}  # No _context_refs field
        ]
        
        with patch('src.repositories.crew_generator_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch('src.repositories.crew_generator_repository.TaskRepository'):
                with patch('src.repositories.crew_generator_repository.logger') as mock_logger:
                    await crew_generator_repository._create_task_dependencies(created_tasks, tasks_data)
                    
                    mock_session.add_all.assert_not_called()
                    mock_session.commit.assert_not_called()
                    # The method always logs this at the end regardless of updates
                    mock_logger.info.assert_any_call("No task context updates needed.")
                    mock_logger.info.assert_any_call("Finished processing task dependencies")


class TestCrewGeneratorRepositoryIntegration:
    """Integration test cases testing method interactions."""
    
    @pytest.mark.asyncio
    async def test_complete_crew_creation_workflow(self, crew_generator_repository):
        """Test complete workflow from crew dict to serialized entities."""
        crew_dict = {
            "agents": [
                {"name": "Analyst", "role": "Data Analyst", "goal": "Analyze data"}
            ],
            "tasks": [
                {"name": "Analysis", "agent": "Analyst", "description": "Analyze the data"}
            ]
        }
        
        with patch('src.repositories.crew_generator_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch('src.repositories.crew_generator_repository.Agent') as mock_agent_class:
                with patch('src.repositories.crew_generator_repository.Task') as mock_task_class:
                    with patch('src.repositories.crew_generator_repository.TaskRepository'):
                        mock_agent = MockAgent(id="agent-1", name="Analyst", group_id="group-123")
                        mock_task = MockTask(id="task-1", name="Analysis", agent_id="agent-1")
                        
                        mock_agent_class.return_value = mock_agent
                        mock_task_class.return_value = mock_task
                        
                        result = await crew_generator_repository.create_crew_entities(crew_dict)
                        
                        # Verify complete workflow
                        assert len(result["agents"]) == 1
                        assert len(result["tasks"]) == 1
                        assert result["agents"][0]["name"] == "Analyst"
                        assert result["tasks"][0]["name"] == "Analysis"
                        assert result["tasks"][0]["agent_id"] == "agent-1"
    
    @pytest.mark.asyncio
    async def test_agent_task_mapping_integration(self, crew_generator_repository):
        """Test that agent-task mapping works correctly throughout the workflow."""
        crew_dict = {
            "agents": [
                {"name": "Agent A", "role": "Role A"},
                {"name": "Agent B", "role": "Role B"}
            ],
            "tasks": [
                {"name": "Task 1", "agent": "Agent A"},
                {"name": "Task 2", "agent": "Agent B"},
                {"name": "Task 3", "agent": "agent a"}  # Case insensitive
            ]
        }
        
        with patch('src.repositories.crew_generator_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch('src.repositories.crew_generator_repository.Agent') as mock_agent_class:
                with patch('src.repositories.crew_generator_repository.Task') as mock_task_class:
                    with patch('src.repositories.crew_generator_repository.TaskRepository'):
                        mock_agents = [
                            MockAgent(id="agent-1", name="Agent A", group_id="group-123"),
                            MockAgent(id="agent-2", name="Agent B", group_id="group-123")
                        ]
                        mock_tasks = [
                            MockTask(id="task-1", name="Task 1", agent_id="agent-1"),
                            MockTask(id="task-2", name="Task 2", agent_id="agent-2"),
                            MockTask(id="task-3", name="Task 3", agent_id="agent-1")  # Case insensitive match
                        ]
                        
                        mock_agent_class.side_effect = mock_agents
                        mock_task_class.side_effect = mock_tasks
                        
                        result = await crew_generator_repository.create_crew_entities(crew_dict)
                        
                        # Verify correct agent assignments
                        tasks = result["tasks"]
                        assert tasks[0]["agent_id"] == "agent-1"  # Task 1 -> Agent A
                        assert tasks[1]["agent_id"] == "agent-2"  # Task 2 -> Agent B
                        assert tasks[2]["agent_id"] == "agent-1"  # Task 3 -> Agent A (case insensitive)


class TestCrewGeneratorRepositoryErrorHandling:
    """Test cases for error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_create_crew_entities_agent_creation_failure(self, crew_generator_repository, sample_crew_dict):
        """Test crew creation when agent creation fails."""
        with patch.object(crew_generator_repository, '_create_agents', side_effect=Exception("Agent creation failed")):
            with pytest.raises(Exception, match="Agent creation failed"):
                await crew_generator_repository.create_crew_entities(sample_crew_dict)
    
    @pytest.mark.asyncio
    async def test_create_crew_entities_task_creation_failure(self, crew_generator_repository, sample_crew_dict):
        """Test crew creation when task creation fails."""
        mock_agents = [MockAgent()]
        
        with patch.object(crew_generator_repository, '_create_agents', return_value=mock_agents):
            with patch.object(crew_generator_repository, '_create_tasks', side_effect=Exception("Task creation failed")):
                with pytest.raises(Exception, match="Task creation failed"):
                    await crew_generator_repository.create_crew_entities(sample_crew_dict)
    
    @pytest.mark.asyncio
    async def test_create_crew_entities_dependency_creation_failure(self, crew_generator_repository, sample_crew_dict):
        """Test crew creation when dependency creation fails."""
        mock_agents = [MockAgent()]
        mock_tasks = [MockTask()]
        
        with patch.object(crew_generator_repository, '_create_agents', return_value=mock_agents):
            with patch.object(crew_generator_repository, '_create_tasks', return_value=mock_tasks):
                with patch.object(crew_generator_repository, '_create_task_dependencies', side_effect=Exception("Dependency creation failed")):
                    with pytest.raises(Exception, match="Dependency creation failed"):
                        await crew_generator_repository.create_crew_entities(sample_crew_dict)


class TestCrewGeneratorRepositoryEdgeCases:
    """Test cases for edge cases and boundary conditions."""
    
    @pytest.mark.asyncio
    async def test_create_agents_with_all_fields(self, crew_generator_repository):
        """Test agent creation with all possible fields."""
        complex_agent_data = [{
            "name": "Complex Agent",
            "role": "Complex Role",
            "goal": "Complex Goal",
            "backstory": "Complex Backstory",
            "llm": "gpt-4-turbo",
            "tools": ["tool1", "tool2", "tool3"],
            "allow_delegation": True,
            "verbose": True,
            "max_iter": 50,
            "max_rpm": 20,
            "cache": False,
            "allow_code_execution": True,
            "code_execution_mode": "unsafe",
            "max_retry_limit": 5,
            "use_system_prompt": False,
            "respect_context_window": False,
            "function_calling_llm": "gpt-4"
        }]
        
        with patch.object(crew_generator_repository, 'create') as mock_create:
            with patch('src.repositories.crew_generator_repository.Agent') as mock_agent_class:
                mock_agent = MockAgent()
                mock_agent_class.return_value = mock_agent
                mock_create.return_value = mock_agent
                
                result = await crew_generator_repository._create_agents(complex_agent_data)
                
                # Verify all fields were passed correctly
                call_args = mock_agent_class.call_args[1]
                assert call_args["name"] == "Complex Agent"
                assert call_args["tools"] == ["tool1", "tool2", "tool3"]
                assert call_args["allow_delegation"] is True
                assert call_args["max_iter"] == 50
                assert call_args["cache"] is False
    
    @pytest.mark.asyncio
    async def test_create_tasks_with_all_fields(self, crew_generator_repository):
        """Test task creation with all possible fields."""
        complex_task_data = [{
            "name": "Complex Task",
            "description": "Complex Description",
            "agent": "Test Agent",
            "expected_output": "Complex Output",
            "tools": ["tool1", "tool2"],
            "async_execution": True,
            "output": "Actual Output",
            "human_input": True,
            "markdown": True,
            "_context_refs": ["Other Task"]
        }]
        agent_name_to_id = {"Test Agent": "agent-1"}
        
        with patch.object(crew_generator_repository, 'create') as mock_create:
            with patch('src.repositories.crew_generator_repository.Task') as mock_task_class:
                mock_task = MockTask()
                mock_task_class.return_value = mock_task
                mock_create.return_value = mock_task
                
                result = await crew_generator_repository._create_tasks(complex_task_data, agent_name_to_id)
                
                # Verify all fields were passed correctly
                call_args = mock_task_class.call_args[1]
                assert call_args["name"] == "Complex Task"
                assert call_args["description"] == "Complex Description"
                assert call_args["tools"] == ["tool1", "tool2"]
                assert call_args["async_execution"] is True
                assert call_args["human_input"] is True
                assert call_args["markdown"] is True
    
    @pytest.mark.asyncio
    async def test_safe_get_attr_with_none_object(self, crew_generator_repository):
        """Test _safe_get_attr with None object."""
        result = crew_generator_repository._safe_get_attr(None, "anything")
        assert result is None
        
        result = crew_generator_repository._safe_get_attr(None, "anything", "default")
        assert result == "default"
    
    @pytest.mark.asyncio 
    async def test_create_task_dependencies_ensure_empty_context_from_none(self, crew_generator_repository):
        """Test dependency creation when task context is None and needs to be ensured empty."""
        task = MockTask(id="task-1", name="Task 1", context=None, group_id="group-123")
        created_tasks = [task]
        tasks_data = [
            {"name": "Task 1", "_context_refs": []}  # Empty context refs with None context
        ]
        
        # Verify the task context is None before the call
        assert task.context is None
        
        with patch('src.repositories.crew_generator_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch('src.repositories.crew_generator_repository.TaskRepository'):
                with patch('src.repositories.crew_generator_repository.logger') as mock_logger:
                    await crew_generator_repository._create_task_dependencies(created_tasks, tasks_data)
                    
                    # Task context should be ensured to be empty and updated
                    assert created_tasks[0].context == []
                    mock_session.add_all.assert_called_once()
                    mock_session.commit.assert_called_once()
                    mock_logger.info.assert_any_call("Ensured context is empty for task 'Task 1' (ID: task-1)")
    
    @pytest.mark.asyncio
    async def test_create_task_dependencies_ensure_empty_context_from_non_empty(self, crew_generator_repository):
        """Test dependency creation when task context is non-empty and needs to be ensured empty."""
        created_tasks = [
            MockTask(id="task-1", name="Task 1", context=["old-dep"], group_id="group-123")
        ]
        tasks_data = [
            {"name": "Task 1"}  # No _context_refs field at all (safe_get_attr returns [])
        ]
        
        with patch('src.repositories.crew_generator_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch('src.repositories.crew_generator_repository.TaskRepository'):
                with patch('src.repositories.crew_generator_repository.logger') as mock_logger:
                    await crew_generator_repository._create_task_dependencies(created_tasks, tasks_data)
                    
                    # Task context should be ensured to be empty and updated
                    assert created_tasks[0].context == []
                    mock_session.add_all.assert_called_once()
                    mock_session.commit.assert_called_once()
                    mock_logger.info.assert_any_call("Ensured context is empty for task 'Task 1' (ID: task-1)")

    @pytest.mark.asyncio
    async def test_create_task_dependencies_with_complex_refs(self, crew_generator_repository):
        """Test dependency creation with complex reference patterns."""
        created_tasks = [
            MockTask(id="task-1", name="Task A", group_id="group-123"),
            MockTask(id="task-2", name="Task B", group_id="group-123"),
            MockTask(id="task-3", name="Task C", group_id="group-123"),
            MockTask(id="task-4", name="Task D", group_id="group-123")
        ]
        tasks_data = [
            {"name": "Task A", "_context_refs": []},
            {"name": "Task B", "_context_refs": ["Task A"]},
            {"name": "Task C", "_context_refs": ["Task A", "Task B"]},
            {"name": "Task D", "_context_refs": ["Task C", "Nonexistent Task", "Task A"]}
        ]
        
        with patch('src.repositories.crew_generator_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch('src.repositories.crew_generator_repository.TaskRepository'):
                await crew_generator_repository._create_task_dependencies(created_tasks, tasks_data)
                
                # Verify complex dependency resolution
                assert created_tasks[0].context == []  # Task A: no deps
                assert created_tasks[1].context == ["task-1"]  # Task B: depends on A
                assert len(created_tasks[2].context) == 2  # Task C: depends on A, B
                assert "task-1" in created_tasks[2].context
                assert "task-2" in created_tasks[2].context
                # Task D: depends on C, A (nonexistent ignored)
                assert len(created_tasks[3].context) == 2
                assert "task-3" in created_tasks[3].context
                assert "task-1" in created_tasks[3].context