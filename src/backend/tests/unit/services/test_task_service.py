"""
Unit tests for TaskService.

Tests the functionality of task management service including
CRUD operations with group isolation.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from typing import Optional, Dict, Any

from sqlalchemy.ext.asyncio import AsyncSession
from src.services.task_service import TaskService
from src.models.task import Task
from src.repositories.task_repository import TaskRepository
from src.schemas.task import TaskCreate, TaskUpdate
from src.utils.user_context import GroupContext


# Mock task model
class MockTask:
    def __init__(self, id="task-123", name="Test Task", description="Test Description",
                 expected_output="Test Output", agent_id="agent-123",
                 group_id="group-123", created_by_email="test@example.com"):
        self.id = id
        self.name = name
        self.description = description
        self.expected_output = expected_output
        self.agent_id = agent_id
        self.group_id = group_id
        self.created_by_email = created_by_email
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()


@pytest.fixture
def mock_session():
    """Create a mock database session."""
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.scalars = AsyncMock()
    return session


@pytest.fixture
def mock_repository():
    """Create a mock task repository."""
    repository = AsyncMock(spec=TaskRepository)
    return repository


@pytest.fixture
def task_service(mock_session, mock_repository):
    """Create a task service with mocked dependencies."""
    with patch('src.services.task_service.TaskRepository', return_value=mock_repository):
        service = TaskService(session=mock_session)
        service.repository = mock_repository
        return service


@pytest.fixture
def sample_task_create():
    """Create a sample task creation schema."""
    return TaskCreate(
        name="New Task",
        description="Implement new feature",
        expected_output="Working feature implementation"
    )


@pytest.fixture
def sample_task_create_with_agent():
    """Create a sample task creation schema with agent assignment."""
    return TaskCreate(
        name="Task with Agent",
        description="Task assigned to agent",
        expected_output="Expected output",
        agent_id="agent-456"
    )


@pytest.fixture
def sample_task_update():
    """Create a sample task update schema."""
    return TaskUpdate(
        name="Updated Task",
        description="Updated description",
        expected_output="Updated output"
    )


@pytest.fixture
def sample_group_context():
    """Create a sample group context."""
    return GroupContext(
        group_ids=["group-123", "group-456"],
        group_email="test@example.com",
        email_domain="example.com",
        user_id="user-123"
    )


class TestTaskServiceInit:
    """Test cases for TaskService initialization."""
    
    def test_init_with_defaults(self, mock_session):
        """Test initialization with default parameters."""
        service = TaskService(session=mock_session)
        
        assert service.session == mock_session
        assert service.repository_class == TaskRepository
        assert service.model_class == Task
        assert isinstance(service.repository, TaskRepository)
    
    def test_init_with_custom_classes(self, mock_session):
        """Test initialization with custom repository and model classes."""
        mock_repo_class = MagicMock()
        mock_model_class = MagicMock()
        
        service = TaskService(
            session=mock_session,
            repository_class=mock_repo_class,
            model_class=mock_model_class
        )
        
        assert service.repository_class == mock_repo_class
        assert service.model_class == mock_model_class
        mock_repo_class.assert_called_once_with(mock_session)


class TestTaskServiceGet:
    """Test cases for get method."""
    
    @pytest.mark.asyncio
    async def test_get_success(self, task_service, mock_repository):
        """Test successful task retrieval."""
        task = MockTask()
        mock_repository.get.return_value = task
        
        result = await task_service.get("task-123")
        
        assert result == task
        mock_repository.get.assert_called_once_with("task-123")
    
    @pytest.mark.asyncio
    async def test_get_not_found(self, task_service, mock_repository):
        """Test get when task is not found."""
        mock_repository.get.return_value = None
        
        result = await task_service.get("non-existent")
        
        assert result is None
        mock_repository.get.assert_called_once_with("non-existent")


class TestTaskServiceCreate:
    """Test cases for create method."""
    
    @pytest.mark.asyncio
    async def test_create_success(self, task_service, mock_repository, sample_task_create):
        """Test successful task creation."""
        created_task = MockTask(
            name=sample_task_create.name,
            description=sample_task_create.description
        )
        mock_repository.create.return_value = created_task
        
        result = await task_service.create(sample_task_create)
        
        assert result == created_task
        mock_repository.create.assert_called_once()
        call_args = mock_repository.create.call_args[0][0]
        assert call_args["name"] == "New Task"
        assert call_args["description"] == "Implement new feature"
    
    @pytest.mark.asyncio
    async def test_create_with_empty_agent_id(self, task_service, mock_repository):
        """Test creation with empty agent_id converts to None."""
        task_data = TaskCreate(
            name="Task",
            description="Description",
            expected_output="Expected output",
            agent_id=""  # Empty string should convert to None
        )
        created_task = MockTask(name="Task", agent_id=None)
        mock_repository.create.return_value = created_task
        
        result = await task_service.create(task_data)
        
        assert result == created_task
        call_args = mock_repository.create.call_args[0][0]
        assert call_args["agent_id"] is None
    
    @pytest.mark.asyncio
    async def test_create_with_agent_id(self, task_service, mock_repository, sample_task_create_with_agent):
        """Test creation with valid agent_id."""
        created_task = MockTask(agent_id="agent-456")
        mock_repository.create.return_value = created_task
        
        result = await task_service.create(sample_task_create_with_agent)
        
        assert result == created_task
        call_args = mock_repository.create.call_args[0][0]
        assert call_args["agent_id"] == "agent-456"


class TestTaskServiceFindByName:
    """Test cases for find_by_name method."""
    
    @pytest.mark.asyncio
    async def test_find_by_name_success(self, task_service, mock_repository):
        """Test successful find by name."""
        task = MockTask(name="Specific Task")
        mock_repository.find_by_name.return_value = task
        
        result = await task_service.find_by_name("Specific Task")
        
        assert result == task
        mock_repository.find_by_name.assert_called_once_with("Specific Task")
    
    @pytest.mark.asyncio
    async def test_find_by_name_not_found(self, task_service, mock_repository):
        """Test find by name when task doesn't exist."""
        mock_repository.find_by_name.return_value = None
        
        result = await task_service.find_by_name("Non-existent Task")
        
        assert result is None
        mock_repository.find_by_name.assert_called_once_with("Non-existent Task")


class TestTaskServiceFindByAgentId:
    """Test cases for find_by_agent_id method."""
    
    @pytest.mark.asyncio
    async def test_find_by_agent_id_success(self, task_service, mock_repository):
        """Test successful find tasks by agent ID."""
        tasks = [
            MockTask(id="task-1", agent_id="agent-123"),
            MockTask(id="task-2", agent_id="agent-123")
        ]
        mock_repository.find_by_agent_id.return_value = tasks
        
        result = await task_service.find_by_agent_id("agent-123")
        
        assert result == tasks
        assert len(result) == 2
        mock_repository.find_by_agent_id.assert_called_once_with("agent-123")
    
    @pytest.mark.asyncio
    async def test_find_by_agent_id_empty(self, task_service, mock_repository):
        """Test find by agent ID when no tasks exist."""
        mock_repository.find_by_agent_id.return_value = []
        
        result = await task_service.find_by_agent_id("agent-no-tasks")
        
        assert result == []
        mock_repository.find_by_agent_id.assert_called_once_with("agent-no-tasks")


class TestTaskServiceFindAll:
    """Test cases for find_all method."""
    
    @pytest.mark.asyncio
    async def test_find_all_success(self, task_service, mock_repository):
        """Test successful find all tasks."""
        tasks = [
            MockTask(id="task-1", name="Task 1"),
            MockTask(id="task-2", name="Task 2"),
            MockTask(id="task-3", name="Task 3")
        ]
        mock_repository.find_all.return_value = tasks
        
        result = await task_service.find_all()
        
        assert result == tasks
        assert len(result) == 3
        mock_repository.find_all.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_all_empty(self, task_service, mock_repository):
        """Test find all when no tasks exist."""
        mock_repository.find_all.return_value = []
        
        result = await task_service.find_all()
        
        assert result == []
        mock_repository.find_all.assert_called_once()


class TestTaskServiceUpdateWithPartialData:
    """Test cases for update_with_partial_data method."""
    
    @pytest.mark.asyncio
    async def test_update_with_partial_data_success(self, task_service, mock_repository, sample_task_update):
        """Test successful partial update."""
        updated_task = MockTask(
            name=sample_task_update.name,
            description=sample_task_update.description
        )
        mock_repository.update.return_value = updated_task
        
        result = await task_service.update_with_partial_data("task-123", sample_task_update)
        
        assert result == updated_task
        mock_repository.update.assert_called_once()
        call_args = mock_repository.update.call_args[0]
        assert call_args[0] == "task-123"
        assert "name" in call_args[1]
        assert "description" in call_args[1]
    
    @pytest.mark.asyncio
    async def test_update_with_partial_data_no_fields(self, task_service, mock_repository):
        """Test update with no fields set (all None)."""
        empty_update = TaskUpdate()
        existing_task = MockTask()
        mock_repository.get.return_value = existing_task
        
        result = await task_service.update_with_partial_data("task-123", empty_update)
        
        assert result == existing_task
        mock_repository.update.assert_not_called()
        mock_repository.get.assert_called_once_with("task-123")
    
    @pytest.mark.asyncio
    async def test_update_with_partial_data_empty_agent_id(self, task_service, mock_repository):
        """Test update with empty agent_id converts to None."""
        update_data = TaskUpdate(agent_id="")
        updated_task = MockTask(agent_id=None)
        mock_repository.update.return_value = updated_task
        
        result = await task_service.update_with_partial_data("task-123", update_data)
        
        assert result == updated_task
        call_args = mock_repository.update.call_args[0]
        assert call_args[1]["agent_id"] is None
    
    @pytest.mark.asyncio
    async def test_update_with_partial_data_not_found(self, task_service, mock_repository, sample_task_update):
        """Test update when task is not found."""
        mock_repository.update.return_value = None
        
        result = await task_service.update_with_partial_data("non-existent", sample_task_update)
        
        assert result is None
        mock_repository.update.assert_called_once()


class TestTaskServiceUpdateFull:
    """Test cases for update_full method."""
    
    @pytest.mark.asyncio
    async def test_update_full_success(self, task_service, mock_repository):
        """Test successful full update."""
        update_data = {
            "name": "Fully Updated Task",
            "description": "Fully updated description",
            "expected_output": "Fully updated output",
            "agent_id": "agent-789"
        }
        updated_task = MockTask(name="Fully Updated Task")
        mock_repository.update.return_value = updated_task
        
        result = await task_service.update_full("task-123", update_data)
        
        assert result == updated_task
        mock_repository.update.assert_called_once_with("task-123", update_data)
    
    @pytest.mark.asyncio
    async def test_update_full_empty_agent_id(self, task_service, mock_repository):
        """Test full update with empty agent_id converts to None."""
        update_data = {
            "name": "Task",
            "agent_id": ""
        }
        updated_task = MockTask(agent_id=None)
        mock_repository.update.return_value = updated_task
        
        result = await task_service.update_full("task-123", update_data)
        
        assert result == updated_task
        # Verify the update_data was modified in place
        assert update_data["agent_id"] is None
        mock_repository.update.assert_called_once_with("task-123", update_data)
    
    @pytest.mark.asyncio
    async def test_update_full_not_found(self, task_service, mock_repository):
        """Test full update when task is not found."""
        update_data = {"name": "Updated"}
        mock_repository.update.return_value = None
        
        result = await task_service.update_full("non-existent", update_data)
        
        assert result is None
        mock_repository.update.assert_called_once()


class TestTaskServiceDelete:
    """Test cases for delete method."""
    
    @pytest.mark.asyncio
    async def test_delete_success(self, task_service, mock_repository):
        """Test successful task deletion."""
        mock_repository.delete.return_value = True
        
        result = await task_service.delete("task-123")
        
        assert result is True
        mock_repository.delete.assert_called_once_with("task-123")
    
    @pytest.mark.asyncio
    async def test_delete_not_found(self, task_service, mock_repository):
        """Test delete when task is not found."""
        mock_repository.delete.return_value = False
        
        result = await task_service.delete("non-existent")
        
        assert result is False
        mock_repository.delete.assert_called_once_with("non-existent")


class TestTaskServiceDeleteAll:
    """Test cases for delete_all method."""
    
    @pytest.mark.asyncio
    async def test_delete_all_success(self, task_service, mock_repository):
        """Test successful delete all tasks."""
        mock_repository.delete_all.return_value = None
        
        await task_service.delete_all()
        
        mock_repository.delete_all.assert_called_once()


class TestTaskServiceCreateWithGroup:
    """Test cases for create_with_group method."""
    
    @pytest.mark.asyncio
    async def test_create_with_group_success(self, task_service, mock_repository, 
                                            sample_task_create, sample_group_context):
        """Test successful task creation with group context."""
        created_task = MockTask(
            name=sample_task_create.name,
            group_id=sample_group_context.primary_group_id,
            created_by_email=sample_group_context.group_email
        )
        mock_repository.create.return_value = created_task
        
        result = await task_service.create_with_group(sample_task_create, sample_group_context)
        
        assert result == created_task
        mock_repository.create.assert_called_once()
        call_args = mock_repository.create.call_args[0][0]
        assert call_args["group_id"] == "group-123"  # Should use primary_group_id property
        assert call_args["created_by_email"] == "test@example.com"
        assert call_args["name"] == "New Task"
    
    @pytest.mark.asyncio
    async def test_create_with_group_empty_agent_id(self, task_service, mock_repository, sample_group_context):
        """Test creation with group and empty agent_id."""
        task_data = TaskCreate(
            name="Task with Empty Agent",
            description="Description",
            expected_output="Expected output",
            agent_id=""
        )
        created_task = MockTask(name="Task with Empty Agent", agent_id=None)
        mock_repository.create.return_value = created_task
        
        result = await task_service.create_with_group(task_data, sample_group_context)
        
        assert result == created_task
        call_args = mock_repository.create.call_args[0][0]
        assert call_args["agent_id"] is None
        assert call_args["group_id"] == "group-123"
    
    @pytest.mark.asyncio
    async def test_create_with_group_all_fields(self, task_service, mock_repository, sample_group_context):
        """Test creation with group including all optional fields."""
        full_task_data = TaskCreate(
            name="Full Task",
            description="Full Description",
            expected_output="Full Output",
            agent_id="agent-999",
            context=["task-1", "task-2"],
            tools=["tool1", "tool2"],
            async_execution=True
        )
        created_task = MockTask(name="Full Task")
        mock_repository.create.return_value = created_task
        
        result = await task_service.create_with_group(full_task_data, sample_group_context)
        
        assert result == created_task
        call_args = mock_repository.create.call_args[0][0]
        assert call_args["tools"] == ["tool1", "tool2"]
        assert call_args["context"] == ["task-1", "task-2"]
        assert call_args["async_execution"] is True


class TestTaskServiceFindByGroup:
    """Test cases for find_by_group method."""
    
    @pytest.mark.asyncio
    async def test_find_by_group_success(self, task_service, mock_session, sample_group_context):
        """Test successful find tasks by group."""
        tasks = [
            MockTask(id="task-1", group_id="group-123"),
            MockTask(id="task-2", group_id="group-123"),
            MockTask(id="task-3", group_id="group-456")
        ]
        
        # Mock the session execute and scalars
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = tasks
        mock_session.execute.return_value = mock_result
        
        result = await task_service.find_by_group(sample_group_context)
        
        assert len(result) == 3
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_by_group_empty_group_context(self, task_service, sample_group_context):
        """Test find by group with empty group IDs."""
        empty_context = GroupContext(
            group_ids=[],
            group_email="test@example.com",
            email_domain="example.com",
            user_id="user-123"
        )
        
        result = await task_service.find_by_group(empty_context)
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_find_by_group_no_tasks(self, task_service, mock_session, sample_group_context):
        """Test find by group when no tasks exist for the group."""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result
        
        result = await task_service.find_by_group(sample_group_context)
        
        assert result == []
        mock_session.execute.assert_called_once()


class TestTaskServiceFactoryMethod:
    """Test cases for factory method."""
    
    # The create class method is shadowed by the instance create method from BaseService
    # so we cannot test it directly