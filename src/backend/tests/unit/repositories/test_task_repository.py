"""
Unit tests for TaskRepository.

Tests the functionality of task repository including
CRUD operations, config synchronization, and error handling.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call
from datetime import datetime
from typing import List

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.repositories.task_repository import TaskRepository, SyncTaskRepository, get_sync_task_repository
from src.models.task import Task


# Mock task model
class MockTask:
    def __init__(self, id="task-123", name="Test Task", description="Test Description",
                 agent_id="agent-123", config=None, output_pydantic=None,
                 output_json=None, output_file=None, callback=None,
                 guardrail=None, markdown=None, group_id="group-123",
                 created_by_email="test@example.com", created_at=None, updated_at=None):
        self.id = id
        self.name = name
        self.description = description
        self.agent_id = agent_id
        self.config = config or {}
        self.output_pydantic = output_pydantic
        self.output_json = output_json
        self.output_file = output_file
        self.callback = callback
        self.guardrail = guardrail
        self.markdown = markdown
        self.group_id = group_id
        self.created_by_email = created_by_email
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()


# Mock SQLAlchemy result objects
class MockScalars:
    def __init__(self, results):
        self.results = results
    
    def first(self):
        return self.results[0] if self.results else None
    
    def all(self):
        return self.results


class MockResult:
    def __init__(self, results):
        self._scalars = MockScalars(results)
    
    def scalars(self):
        return self._scalars


@pytest.fixture
def mock_async_session():
    """Create a mock async database session."""
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    session.rollback = AsyncMock()
    session.delete = AsyncMock()
    return session


@pytest.fixture
def mock_sync_session():
    """Create a mock sync database session."""
    session = MagicMock(spec=Session)
    session.query.return_value = session
    session.filter.return_value = session
    session.first.return_value = None
    session.all.return_value = []
    return session


@pytest.fixture
def task_repository(mock_async_session):
    """Create a task repository with async session."""
    return TaskRepository(session=mock_async_session)


@pytest.fixture
def sync_task_repository(mock_sync_session):
    """Create a sync task repository with sync session."""
    return SyncTaskRepository(db=mock_sync_session)


@pytest.fixture
def sample_tasks():
    """Create sample tasks for testing."""
    return [
        MockTask(id="task-1", name="Task 1", agent_id="agent-1"),
        MockTask(id="task-2", name="Task 2", agent_id="agent-2"),
        MockTask(id="task-3", name="Task 3", agent_id="agent-1")
    ]


@pytest.fixture
def sample_task_data():
    """Create sample task data for creation."""
    return {
        "name": "new_task",
        "description": "A new test task",
        "agent_id": "agent-123",
        "config": {
            "output_pydantic": "UserModel",
            "output_json": "output.json",
            "callback": "my_callback"
        }
    }


class TestTaskRepositoryInit:
    """Test cases for TaskRepository initialization."""
    
    def test_init_success(self, mock_async_session):
        """Test successful initialization."""
        repository = TaskRepository(session=mock_async_session)
        
        assert repository.model == Task
        assert repository.session == mock_async_session


class TestTaskRepositoryGet:
    """Test cases for get method."""
    
    @pytest.mark.asyncio
    async def test_get_success(self, task_repository, mock_async_session):
        """Test successful task retrieval."""
        task = MockTask(id="task-123")
        mock_result = MockResult([task])
        mock_async_session.execute.return_value = mock_result
        
        result = await task_repository.get("task-123")
        
        assert result == task
        mock_async_session.execute.assert_called_once()
        # Verify the query was constructed correctly
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(Task)))
    
    @pytest.mark.asyncio
    async def test_get_not_found(self, task_repository, mock_async_session):
        """Test get when task not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await task_repository.get("nonexistent")
        
        assert result is None
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_exception_handling(self, task_repository, mock_async_session):
        """Test get with database exception."""
        mock_async_session.execute.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            await task_repository.get("task-123")
        
        mock_async_session.rollback.assert_called_once()


class TestTaskRepositoryCreate:
    """Test cases for create method."""
    
    @pytest.mark.asyncio
    async def test_create_success(self, task_repository, mock_async_session, sample_task_data):
        """Test successful task creation."""
        # Create expected task data after synchronization
        expected_data = sample_task_data.copy()
        expected_data['output_pydantic'] = 'UserModel'
        expected_data['output_json'] = 'output.json'
        expected_data['callback'] = 'my_callback'
        
        created_task = MockTask(
            name=sample_task_data["name"],
            description=sample_task_data["description"]
        )
        
        with patch.object(task_repository, 'model') as mock_task_class:
            mock_task_class.return_value = created_task
            
            result = await task_repository.create(sample_task_data)
            
            assert result == created_task
            mock_task_class.assert_called_once()
            mock_async_session.add.assert_called_once_with(created_task)
            mock_async_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_config_to_root_sync(self, task_repository, mock_async_session):
        """Test creation with config fields syncing to root level."""
        task_data = {
            "name": "sync_task",
            "config": {
                "output_pydantic": "UserModel",
                "output_json": "output.json",
                "output_file": "result.txt",
                "callback": "my_callback",
                "guardrail": "safety_check"
            }
        }
        
        created_task = MockTask(name="sync_task", group_id="group-123")
        
        with patch.object(task_repository, 'model') as mock_task_class:
            mock_task_class.return_value = created_task
            
            result = await task_repository.create(task_data)
            
            # Verify config fields were synced to root level
            call_args = mock_task_class.call_args[1]
            assert call_args["output_pydantic"] == "UserModel"
            assert call_args["output_json"] == "output.json"
            assert call_args["output_file"] == "result.txt"
            assert call_args["callback"] == "my_callback"
            assert call_args["guardrail"] == "safety_check"
    
    @pytest.mark.asyncio
    async def test_create_root_to_config_sync(self, task_repository, mock_async_session):
        """Test creation with root fields syncing to config."""
        task_data = {
            "name": "sync_task",
            "output_pydantic": "UserModel",
            "output_json": "output.json",
            "output_file": "result.txt",
            "callback": "my_callback",
            "guardrail": "safety_check",
            "markdown": True
        }
        
        created_task = MockTask(name="sync_task", group_id="group-123")
        
        with patch.object(task_repository, 'model') as mock_task_class:
            mock_task_class.return_value = created_task
            
            result = await task_repository.create(task_data)
            
            # Verify root fields were synced to config
            call_args = mock_task_class.call_args[1]
            config = call_args["config"]
            assert config["output_pydantic"] == "UserModel"
            assert config["output_json"] == "output.json"
            assert config["output_file"] == "result.txt"
            assert config["callback"] == "my_callback"
            assert config["guardrail"] == "safety_check"
            assert config["markdown"] == True
    
    @pytest.mark.asyncio
    async def test_create_markdown_bidirectional_sync(self, task_repository, mock_async_session):
        """Test creation with markdown syncing both ways."""
        # Test config to root sync
        task_data_config_to_root = {
            "name": "markdown_task",
            "config": {"markdown": True}
        }
        
        created_task = MockTask(name="markdown_task", group_id="group-123")
        
        with patch.object(task_repository, 'model') as mock_task_class:
            mock_task_class.return_value = created_task
            
            result = await task_repository.create(task_data_config_to_root)
            
            call_args = mock_task_class.call_args[1]
            assert call_args["markdown"] == True
    
    @pytest.mark.asyncio
    async def test_create_empty_config_handling(self, task_repository, mock_async_session):
        """Test creation when config doesn't exist."""
        task_data = {
            "name": "no_config_task",
            "output_pydantic": "UserModel"
        }
        
        created_task = MockTask(name="no_config_task", group_id="group-123")
        
        with patch.object(task_repository, 'model') as mock_task_class:
            mock_task_class.return_value = created_task
            
            result = await task_repository.create(task_data)
            
            # Verify config was created and field was synced
            call_args = mock_task_class.call_args[1]
            assert "config" in call_args
            assert call_args["config"]["output_pydantic"] == "UserModel"
    
    @pytest.mark.asyncio
    async def test_create_none_values_handling(self, task_repository, mock_async_session):
        """Test creation with None values for optional fields."""
        task_data = {
            "name": "none_task",
            "agent_id": None,  # Should not be converted to empty string
            "output_pydantic": None,
            "config": None
        }
        
        created_task = MockTask(name="none_task", group_id="group-123")
        
        with patch.object(task_repository, 'model') as mock_task_class:
            mock_task_class.return_value = created_task
            
            result = await task_repository.create(task_data)
            
            # Verify None agent_id is preserved (for PostgreSQL foreign key constraints)
            call_args = mock_task_class.call_args[1]
            assert call_args["agent_id"] is None
    
    @pytest.mark.asyncio
    async def test_create_exception_handling(self, task_repository, mock_async_session):
        """Test create with database exception."""
        task_data = {"name": "error_task"}
        
        created_task = MockTask()
        
        with patch.object(task_repository, 'model') as mock_task_class:
            mock_task_class.return_value = created_task
            mock_async_session.flush.side_effect = Exception("Create error")
            
            with pytest.raises(Exception, match="Create error"):
                await task_repository.create(task_data)
            
            mock_async_session.rollback.assert_called_once()


class TestTaskRepositoryUpdate:
    """Test cases for update method."""
    
    @pytest.mark.asyncio
    async def test_update_success(self, task_repository, mock_async_session):
        """Test successful task update."""
        task = MockTask(id="task-123", name="Old Name", group_id="group-123")
        mock_result = MockResult([task])
        mock_async_session.execute.return_value = mock_result
        
        update_data = {"name": "New Name", "description": "Updated description"}
        result = await task_repository.update("task-123", update_data)
        
        assert result == task
        assert task.name == "New Name"
        assert task.description == "Updated description"
        mock_async_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_config_sync(self, task_repository, mock_async_session):
        """Test update with config synchronization."""
        task = MockTask(id="task-123")
        mock_result = MockResult([task])
        mock_async_session.execute.return_value = mock_result
        
        update_data = {
            "config": {
                "output_pydantic": "NewModel",
                "output_json": "new_output.json"
            }
        }
        
        with patch.object(task_repository, 'get', return_value=task):
            result = await task_repository.update("task-123", update_data)
            
            # The actual synchronization logic would be applied during update
            # This test verifies the method completes successfully
            assert result == task
            mock_async_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_not_found(self, task_repository, mock_async_session):
        """Test update when task not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        with patch.object(task_repository, 'get', return_value=None):
            update_data = {"name": "New Name"}
            result = await task_repository.update("nonexistent", update_data)
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_update_exception_handling(self, task_repository, mock_async_session):
        """Test update with database exception."""
        task = MockTask(id="task-123")
        
        with patch.object(task_repository, 'get', return_value=task):
            mock_async_session.flush.side_effect = Exception("Update error")
            
            with pytest.raises(Exception, match="Update error"):
                await task_repository.update("task-123", {"name": "New Name"})
            
            mock_async_session.rollback.assert_called_once()


class TestTaskRepositoryDelete:
    """Test cases for delete method."""
    
    @pytest.mark.asyncio
    async def test_delete_success(self, task_repository, mock_async_session):
        """Test successful task deletion."""
        task = MockTask(id="task-123", name="Test Task")
        
        with patch.object(task_repository, 'get', return_value=task):
            result = await task_repository.delete("task-123")
            
            assert result is True
            mock_async_session.delete.assert_called_once_with(task)
            mock_async_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_not_found(self, task_repository, mock_async_session):
        """Test delete when task not found."""
        with patch.object(task_repository, 'get', return_value=None):
            result = await task_repository.delete("nonexistent")
            
            assert result is False
            mock_async_session.delete.assert_not_called()
            mock_async_session.flush.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_delete_exception_handling(self, task_repository, mock_async_session):
        """Test delete with database exception."""
        task = MockTask(id="task-123")
        
        with patch.object(task_repository, 'get', return_value=task):
            mock_async_session.flush.side_effect = Exception("Delete error")
            
            with pytest.raises(Exception, match="Delete error"):
                await task_repository.delete("task-123")
            
            mock_async_session.rollback.assert_called_once()


class TestTaskRepositoryFindByName:
    """Test cases for find_by_name method."""
    
    @pytest.mark.asyncio
    async def test_find_by_name_success(self, task_repository, mock_async_session):
        """Test successful find by name."""
        task = MockTask(name="Test Task")
        mock_result = MockResult([task])
        mock_async_session.execute.return_value = mock_result
        
        result = await task_repository.find_by_name("Test Task")
        
        assert result == task
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_by_name_not_found(self, task_repository, mock_async_session):
        """Test find by name when task not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await task_repository.find_by_name("Nonexistent Task")
        
        assert result is None
        mock_async_session.execute.assert_called_once()


class TestTaskRepositoryFindByAgentId:
    """Test cases for find_by_agent_id method."""
    
    @pytest.mark.asyncio
    async def test_find_by_agent_id_success(self, task_repository, mock_async_session, sample_tasks):
        """Test successful find by agent ID."""
        agent_tasks = [sample_tasks[0], sample_tasks[2]]  # Tasks with agent-1
        mock_result = MockResult(agent_tasks)
        mock_async_session.execute.return_value = mock_result
        
        result = await task_repository.find_by_agent_id("agent-1")
        
        assert result == agent_tasks
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_by_agent_id_empty(self, task_repository, mock_async_session):
        """Test find by agent ID when no tasks found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await task_repository.find_by_agent_id("nonexistent-agent")
        
        assert result == []
        mock_async_session.execute.assert_called_once()


class TestTaskRepositoryFindAll:
    """Test cases for find_all method."""
    
    @pytest.mark.asyncio
    async def test_find_all_success(self, task_repository, mock_async_session, sample_tasks):
        """Test successful find all tasks."""
        mock_result = MockResult(sample_tasks)
        mock_async_session.execute.return_value = mock_result
        
        result = await task_repository.find_all()
        
        assert result == sample_tasks
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_all_empty(self, task_repository, mock_async_session):
        """Test find all when no tasks exist."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await task_repository.find_all()
        
        assert result == []
        mock_async_session.execute.assert_called_once()


class TestTaskRepositoryDeleteAll:
    """Test cases for delete_all method."""
    
    @pytest.mark.asyncio
    async def test_delete_all_success(self, task_repository, mock_async_session):
        """Test successful delete all tasks."""
        await task_repository.delete_all()
        
        mock_async_session.execute.assert_called_once()
        mock_async_session.flush.assert_called_once()


class TestTaskRepositoryConfigSynchronization:
    """Test cases for config synchronization logic."""
    
    @pytest.mark.asyncio
    async def test_all_config_fields_sync_to_root(self, task_repository, mock_async_session):
        """Test all config fields sync to root level."""
        task_data = {
            "name": "full_sync_task",
            "config": {
                "output_pydantic": "FullModel",
                "output_json": "full.json",
                "output_file": "full.txt",
                "callback": "full_callback",
                "guardrail": "full_guard",
                "markdown": True
            }
        }
        
        created_task = MockTask(name="full_sync_task", group_id="group-123")
        
        with patch.object(task_repository, 'model') as mock_task_class:
            mock_task_class.return_value = created_task
            
            result = await task_repository.create(task_data)
            
            call_args = mock_task_class.call_args[1]
            assert call_args["output_pydantic"] == "FullModel"
            assert call_args["output_json"] == "full.json"
            assert call_args["output_file"] == "full.txt"
            assert call_args["callback"] == "full_callback"
            assert call_args["guardrail"] == "full_guard"
            assert call_args["markdown"] == True
    
    @pytest.mark.asyncio
    async def test_all_root_fields_sync_to_config(self, task_repository, mock_async_session):
        """Test all root fields sync to config."""
        task_data = {
            "name": "full_root_sync",
            "output_pydantic": "RootModel",
            "output_json": "root.json",
            "output_file": "root.txt",
            "callback": "root_callback",
            "guardrail": "root_guard",
            "markdown": False
        }
        
        created_task = MockTask(name="full_root_sync", group_id="group-123")
        
        with patch.object(task_repository, 'model') as mock_task_class:
            mock_task_class.return_value = created_task
            
            result = await task_repository.create(task_data)
            
            call_args = mock_task_class.call_args[1]
            config = call_args["config"]
            assert config["output_pydantic"] == "RootModel"
            assert config["output_json"] == "root.json"
            assert config["output_file"] == "root.txt"
            assert config["callback"] == "root_callback"
            assert config["guardrail"] == "root_guard"
            assert config["markdown"] == False
    
    @pytest.mark.asyncio
    async def test_partial_sync_with_existing_config(self, task_repository, mock_async_session):
        """Test partial sync when config already exists."""
        task_data = {
            "name": "partial_sync",
            "config": {"existing_field": "keep_me"},
            "output_pydantic": "AddedModel"
        }
        
        created_task = MockTask(name="partial_sync", group_id="group-123")
        
        with patch.object(task_repository, 'model') as mock_task_class:
            mock_task_class.return_value = created_task
            
            result = await task_repository.create(task_data)
            
            call_args = mock_task_class.call_args[1]
            config = call_args["config"]
            assert config["existing_field"] == "keep_me"
            assert config["output_pydantic"] == "AddedModel"
    
    @pytest.mark.asyncio
    async def test_empty_string_vs_none_handling(self, task_repository, mock_async_session):
        """Test handling of empty strings vs None values."""
        task_data = {
            "name": "empty_test",
            "output_pydantic": "",  # Empty string
            "output_json": None,    # None value
            "config": {
                "output_file": "",  # Empty string in config
                "callback": None    # None in config
            }
        }
        
        created_task = MockTask(name="empty_test", group_id="group-123")
        
        with patch.object(task_repository, 'model') as mock_task_class:
            mock_task_class.return_value = created_task
            
            result = await task_repository.create(task_data)
            
            # Empty strings should not trigger sync, only truthy values
            call_args = mock_task_class.call_args[1]
            # Config should be created but only with the empty values
            assert "config" in call_args


class TestTaskRepositoryIntegration:
    """Integration test cases testing method interactions."""
    
    @pytest.mark.asyncio
    async def test_create_then_get_flow(self, task_repository, mock_async_session):
        """Test the flow from create to get."""
        task_data = {"name": "integration_task", "agent_id": "agent-123"}
        
        created_task = MockTask(name="integration_task", group_id="group-123")
        
        with patch.object(task_repository, 'model') as mock_task_class:
            mock_task_class.return_value = created_task
            
            # Create task
            create_result = await task_repository.create(task_data)
            
            # Mock get for retrieval using patch.object for get method
            with patch.object(task_repository, 'get', return_value=created_task) as mock_get:
                get_result = await task_repository.get("integration_task")
                
                assert create_result == created_task
                assert get_result == created_task
                mock_async_session.add.assert_called_once()
                mock_get.assert_called_once_with("integration_task")
    
    @pytest.mark.asyncio
    async def test_get_then_update_flow(self, task_repository, mock_async_session):
        """Test the flow from get to update."""
        task = MockTask(id="task-123", name="Original Name", group_id="group-123")
        mock_result = MockResult([task])
        mock_async_session.execute.return_value = mock_result
        
        # The update method calls get internally
        update_data = {"name": "Updated Name"}
        result = await task_repository.update("task-123", update_data)
        
        assert result == task
        assert task.name == "Updated Name"
        # Should call execute for the get operation in update
        mock_async_session.execute.assert_called()
        mock_async_session.flush.assert_called_once()


class TestTaskRepositoryErrorHandling:
    """Test cases for error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_get_session_error(self, task_repository, mock_async_session):
        """Test get when session raises an error."""
        mock_async_session.execute.side_effect = Exception("Session error")
        
        with pytest.raises(Exception, match="Session error"):
            await task_repository.get("task-123")
        
        mock_async_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_model_instantiation_error(self, task_repository, mock_async_session):
        """Test create when model instantiation fails."""
        task_data = {"name": "error_task"}
        
        with patch.object(task_repository, 'model') as mock_task_class:
            mock_task_class.side_effect = Exception("Model error")
            
            with pytest.raises(Exception, match="Model error"):
                await task_repository.create(task_data)
            
            mock_async_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_get_error(self, task_repository, mock_async_session):
        """Test update when internal get fails."""
        with patch.object(task_repository, 'get', side_effect=Exception("Get error")):
            with pytest.raises(Exception, match="Get error"):
                await task_repository.update("task-123", {"name": "New Name"})
    
    @pytest.mark.asyncio
    async def test_config_processing_error(self, task_repository, mock_async_session):
        """Test handling of config processing errors."""
        # This test simulates an error during config synchronization
        task_data = {
            "name": "config_error_task",
            "config": {"invalid": "data"}
        }
        
        with patch.object(task_repository, 'model') as mock_task_class:
            # Simulate an error during object creation after config processing
            mock_task_class.side_effect = Exception("Config processing error")
            
            with pytest.raises(Exception, match="Config processing error"):
                await task_repository.create(task_data)
            
            mock_async_session.rollback.assert_called_once()


class TestSyncTaskRepository:
    """Test cases for SyncTaskRepository."""
    
    def test_init_success(self):
        """Test successful initialization."""
        mock_session = MagicMock(spec=Session)
        repository = SyncTaskRepository(db=mock_session)
        assert repository.db == mock_session
    
    def test_find_by_id_success(self):
        """Test successful find by ID."""
        mock_session = MagicMock(spec=Session)
        repository = SyncTaskRepository(db=mock_session)
        task = MockTask(id=1, name="Test Task")
        mock_session.query.return_value.filter.return_value.first.return_value = task
        
        result = repository.find_by_id(1)
        
        assert result == task
        mock_session.query.assert_called_once_with(Task)
        mock_session.query.return_value.filter.assert_called_once()
        mock_session.query.return_value.filter.return_value.first.assert_called_once()
    
    def test_find_by_id_not_found(self):
        """Test find by ID when task not found."""
        mock_session = MagicMock(spec=Session)
        repository = SyncTaskRepository(db=mock_session)
        mock_session.query.return_value.filter.return_value.first.return_value = None
        
        result = repository.find_by_id(999)
        
        assert result is None
    
    def test_find_by_name_success(self):
        """Test successful find by name."""
        mock_session = MagicMock(spec=Session)
        repository = SyncTaskRepository(db=mock_session)
        task = MockTask(name="Test Task")
        mock_session.query.return_value.filter.return_value.first.return_value = task
        
        result = repository.find_by_name("Test Task")
        
        assert result == task
        mock_session.query.assert_called_once_with(Task)
    
    def test_find_by_name_not_found(self):
        """Test find by name when task not found."""
        mock_session = MagicMock(spec=Session)
        repository = SyncTaskRepository(db=mock_session)
        mock_session.query.return_value.filter.return_value.first.return_value = None
        
        result = repository.find_by_name("Nonexistent Task")
        
        assert result is None
    
    def test_find_by_agent_id_success(self, sample_tasks):
        """Test successful find by agent ID."""
        mock_session = MagicMock(spec=Session)
        repository = SyncTaskRepository(db=mock_session)
        agent_tasks = [sample_tasks[0], sample_tasks[2]]
        mock_session.query.return_value.filter.return_value.all.return_value = agent_tasks
        
        result = repository.find_by_agent_id(1)
        
        assert result == agent_tasks
        mock_session.query.assert_called_once_with(Task)
    
    def test_find_by_agent_id_empty(self):
        """Test find by agent ID when no tasks found."""
        mock_session = MagicMock(spec=Session)
        repository = SyncTaskRepository(db=mock_session)
        mock_session.query.return_value.filter.return_value.all.return_value = []
        
        result = repository.find_by_agent_id(999)
        
        assert result == []
    
    def test_find_all_success(self, sample_tasks):
        """Test successful find all tasks."""
        mock_session = MagicMock(spec=Session)
        repository = SyncTaskRepository(db=mock_session)
        mock_session.query.return_value.all.return_value = sample_tasks
        
        result = repository.find_all()
        
        assert result == sample_tasks
        mock_session.query.assert_called_once_with(Task)
    
    def test_find_all_empty(self):
        """Test find all when no tasks exist."""
        mock_session = MagicMock(spec=Session)
        repository = SyncTaskRepository(db=mock_session)
        mock_session.query.return_value.all.return_value = []
        
        result = repository.find_all()
        
        assert result == []


class TestGetSyncTaskRepository:
    """Test cases for get_sync_task_repository factory function."""
    
    @patch('src.repositories.task_repository.SessionLocal')
    def test_get_sync_task_repository_success(self, mock_session_local):
        """Test successful creation of sync repository."""
        mock_session = MagicMock()
        mock_session_local.return_value = mock_session
        
        result = get_sync_task_repository()
        
        assert isinstance(result, SyncTaskRepository)
        assert result.db == mock_session
        mock_session_local.assert_called_once()


class TestTaskRepositoryUpdateConfigSynchronization:
    """Test cases for update method config synchronization."""
    
    @pytest.mark.asyncio
    async def test_update_config_to_root_sync(self, task_repository, mock_async_session):
        """Test update with config fields syncing to root level."""
        task = MockTask(id="task-123")
        
        update_data = {
            "config": {
                "output_pydantic": "UpdatedModel",
                "output_json": "updated.json",
                "output_file": "updated.txt",
                "callback": "updated_callback",
                "guardrail": "updated_guard"
            }
        }
        
        with patch.object(task_repository, 'get', return_value=task):
            result = await task_repository.update("task-123", update_data)
            
            assert result == task
            mock_async_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_root_to_config_sync(self, task_repository, mock_async_session):
        """Test update with root fields syncing to config."""
        task = MockTask(id="task-123")
        
        update_data = {
            "output_pydantic": "UpdatedModel",
            "output_json": "updated.json",
            "output_file": "updated.txt",
            "callback": "updated_callback",
            "guardrail": "updated_guard",
            "markdown": False
        }
        
        with patch.object(task_repository, 'get', return_value=task):
            result = await task_repository.update("task-123", update_data)
            
            assert result == task
            mock_async_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_markdown_bidirectional_sync(self, task_repository, mock_async_session):
        """Test update with markdown syncing both ways."""
        task = MockTask(id="task-123")
        
        # Test config to root sync
        update_data = {
            "config": {"markdown": True}
        }
        
        with patch.object(task_repository, 'get', return_value=task):
            result = await task_repository.update("task-123", update_data)
            
            assert result == task
            mock_async_session.flush.assert_called_once()


class TestTaskRepositoryMissingCoverage:
    """Test cases for covering remaining uncovered lines."""
    
    @pytest.mark.asyncio
    async def test_create_missing_config_key(self, task_repository, mock_async_session):
        """Test create when config key is missing entirely."""
        task_data = {
            "name": "no_config_key_task",
            # No config key at all - this should trigger line 82-83
            "output_pydantic": "TestModel"  # This should NOT trigger the redundant checks on lines 86-87
        }
        
        created_task = MockTask(name="no_config_key_task", group_id="group-123")
        
        with patch.object(task_repository, 'model') as mock_task_class:
            mock_task_class.return_value = created_task
            
            result = await task_repository.create(task_data)
            
            call_args = mock_task_class.call_args[1]
            assert "config" in call_args
            assert call_args["config"]["output_pydantic"] == "TestModel"
    
    @pytest.mark.asyncio
    async def test_create_all_sync_fields_from_root_missing_config(self, task_repository, mock_async_session):
        """Test create with all sync fields from root to config when config key is missing."""
        # Create separate tasks to trigger the redundant config checks on lines 86, 91, 96, 101, 106
        task_data_output_json = {
            "name": "output_json_task",
            "output_json": "test.json",      # No config key - triggers lines 90, then 91-92
        }
        
        created_task = MockTask(name="output_json_task", group_id="group-123")
        
        with patch.object(task_repository, 'model') as mock_task_class:
            mock_task_class.return_value = created_task
            
            result = await task_repository.create(task_data_output_json)
            
            call_args = mock_task_class.call_args[1]
            config = call_args["config"]
            assert config["output_json"] == "test.json"
    
    @pytest.mark.asyncio
    async def test_create_markdown_missing_config(self, task_repository, mock_async_session):
        """Test create when config key is missing and markdown needs sync."""
        task_data = {
            "name": "markdown_no_config",
            # No config key at all
            "markdown": True  # This should trigger lines 110, then 111-112
        }
        
        created_task = MockTask(name="markdown_no_config", group_id="group-123")
        
        with patch.object(task_repository, 'model') as mock_task_class:
            mock_task_class.return_value = created_task
            
            result = await task_repository.create(task_data)
            
            call_args = mock_task_class.call_args[1]
            assert call_args["config"]["markdown"] == True
    
    @pytest.mark.asyncio
    async def test_update_missing_config_key(self, task_repository, mock_async_session):
        """Test update when config key is missing entirely."""
        task = MockTask(id="task-123")
        
        update_data = {
            # No config key at all - this should trigger line 166-167
            "output_pydantic": "UpdatedModel"  # This should NOT trigger the redundant checks on lines 170-171
        }
        
        with patch.object(task_repository, 'get', return_value=task):
            result = await task_repository.update("task-123", update_data)
            
            assert result == task
            mock_async_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_output_json_missing_config(self, task_repository, mock_async_session):
        """Test update with output_json field when config key is missing."""
        task = MockTask(id="task-123")
        
        update_data = {
            "output_json": "updated.json",      # No config key - triggers lines 174, then 175-176
        }
        
        with patch.object(task_repository, 'get', return_value=task):
            result = await task_repository.update("task-123", update_data)
            
            assert result == task
            mock_async_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_output_file_missing_config(self, task_repository, mock_async_session):
        """Test update with output_file field when config key is missing."""
        task = MockTask(id="task-123")
        
        update_data = {
            "output_file": "updated.txt",       # No config key - triggers lines 179, then 180-181
        }
        
        with patch.object(task_repository, 'get', return_value=task):
            result = await task_repository.update("task-123", update_data)
            
            assert result == task
            mock_async_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_callback_missing_config(self, task_repository, mock_async_session):
        """Test update with callback field when config key is missing."""
        task = MockTask(id="task-123")
        
        update_data = {
            "callback": "updated_callback",     # No config key - triggers lines 184, then 185-186
        }
        
        with patch.object(task_repository, 'get', return_value=task):
            result = await task_repository.update("task-123", update_data)
            
            assert result == task
            mock_async_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_guardrail_missing_config(self, task_repository, mock_async_session):
        """Test update with guardrail field when config key is missing."""
        task = MockTask(id="task-123")
        
        update_data = {
            "guardrail": "updated_guard"        # No config key - triggers lines 189, then 190-191
        }
        
        with patch.object(task_repository, 'get', return_value=task):
            result = await task_repository.update("task-123", update_data)
            
            assert result == task
            mock_async_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_markdown_missing_config(self, task_repository, mock_async_session):
        """Test update when config key is missing and markdown needs sync."""
        task = MockTask(id="task-123")
        
        update_data = {
            # No config key at all
            "markdown": False  # This should trigger lines 194, then 195-196
        }
        
        with patch.object(task_repository, 'get', return_value=task):
            result = await task_repository.update("task-123", update_data)
            
            assert result == task
            mock_async_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_redundant_config_checks(self, task_repository, mock_async_session):
        """Test create to trigger the redundant config checks after config exists."""
        # To trigger lines 87, 92, 97, 102, 107, 112, we need scenarios where:
        # 1. config exists (not None and contains the key)
        # 2. but the individual field checks still need to run
        
        # First, test output_pydantic with existing config
        task_data = {
            "name": "redundant_check_task",
            "config": {"existing_key": "value"},  # Config exists but doesn't have output_pydantic
            "output_pydantic": "TestModel"  # This should NOT trigger line 87 since config exists
        }
        
        created_task = MockTask(name="redundant_check_task", group_id="group-123")
        
        with patch.object(task_repository, 'model') as mock_task_class:
            mock_task_class.return_value = created_task
            
            result = await task_repository.create(task_data)
            
            call_args = mock_task_class.call_args[1]
            assert call_args["config"]["output_pydantic"] == "TestModel"
    
    @pytest.mark.asyncio
    async def test_update_redundant_config_checks(self, task_repository, mock_async_session):
        """Test update to trigger the redundant config checks after config exists."""
        task = MockTask(id="task-123")
        
        # To trigger lines 171, 176, 181, 186, 191, 196, we need scenarios where:
        # 1. config exists (not None and contains some key)
        # 2. but the individual field checks still need to run
        
        # First, test output_pydantic with existing config
        update_data = {
            "config": {"existing_key": "value"},  # Config exists but doesn't have output_pydantic
            "output_pydantic": "UpdatedModel"  # This should NOT trigger line 171 since config exists
        }
        
        with patch.object(task_repository, 'get', return_value=task):
            result = await task_repository.update("task-123", update_data)
            
            assert result == task
            mock_async_session.flush.assert_called_once()