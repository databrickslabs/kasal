"""
Unit tests for TaskService.

Tests the functionality of the task service including
creating, updating, deleting, and managing tasks.
"""
import pytest
import uuid
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any, List

from src.services.task_service import TaskService
from src.schemas.task import TaskCreate, TaskUpdate
from src.models.task import Task
from src.core.unit_of_work import UnitOfWork


@pytest.fixture
def mock_uow():
    """Create a mock unit of work."""
    uow = MagicMock(spec=UnitOfWork)
    uow.session = AsyncMock()
    uow.commit = AsyncMock()
    uow.rollback = AsyncMock()
    return uow


@pytest.fixture
def mock_task_repository():
    """Create a mock task repository."""
    repo = AsyncMock()
    
    # Create mock task objects
    mock_task = MagicMock(spec=Task)
    mock_task.id = uuid.uuid4()
    mock_task.name = "Test Task"
    mock_task.description = "Test task description"
    mock_task.agent = "researcher"
    mock_task.expected_output = "A comprehensive report"
    mock_task.context = ["previous_task"]
    mock_task.tools = ["web_search"]
    mock_task.async_execution = False
    mock_task.created_at = datetime.now(UTC)
    mock_task.updated_at = datetime.now(UTC)
    mock_task.is_active = True
    
    # Setup repository method returns
    repo.get.return_value = mock_task
    repo.list.return_value = [mock_task]
    repo.create.return_value = mock_task
    repo.update.return_value = mock_task
    repo.delete.return_value = True
    repo.get_by_name.return_value = mock_task
    repo.search.return_value = [mock_task]
    
    return repo


@pytest.fixture
def task_create_data():
    """Create test data for task creation."""
    return TaskCreate(
        name="Test Task",
        description="Conduct thorough research on the given topic",
        agent="researcher",
        expected_output="A comprehensive research report with findings",
        context=["previous_research"],
        tools=["web_search", "document_analyzer"],
        async_execution=False,
        guardrails=["data_quality_check"]
    )


@pytest.fixture
def task_update_data():
    """Create test data for task updates."""
    return TaskUpdate(
        name="Updated Task",
        description="Updated task description",
        expected_output="Updated expected output"
    )


class TestTaskService:
    """Test cases for TaskService."""
    
    @pytest.mark.asyncio
    async def test_create_task_success(self, mock_uow, mock_task_repository, task_create_data):
        """Test successful task creation."""
        with patch("src.services.task_service.TaskRepository", return_value=mock_task_repository):
            service = TaskService(mock_uow)
            
            result = await service.create(task_create_data)
            
            assert result is not None
            assert result.name == "Test Task"
            assert result.description == "Test task description"
            mock_task_repository.create.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_task_validation_error(self, mock_uow, mock_task_repository):
        """Test task creation with invalid data."""
        with patch("src.services.task_service.TaskRepository", return_value=mock_task_repository):
            service = TaskService(mock_uow)
            
            # Test with invalid data (empty description)
            invalid_data = TaskCreate(
                name="Test Task",
                description="",  # Empty description should fail validation
                agent="researcher",
                expected_output="Output",
                context=[],
                tools=[],
                async_execution=False
            )
            
            mock_task_repository.create.side_effect = ValueError("Description cannot be empty")
            
            with pytest.raises(ValueError, match="Description cannot be empty"):
                await service.create(invalid_data)
            
            mock_uow.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_task_by_id(self, mock_uow, mock_task_repository):
        """Test getting a task by ID."""
        task_id = uuid.uuid4()
        
        with patch("src.services.task_service.TaskRepository", return_value=mock_task_repository):
            service = TaskService(mock_uow)
            
            result = await service.get(task_id)
            
            assert result is not None
            assert result.name == "Test Task"
            mock_task_repository.get.assert_called_once_with(task_id)
    
    @pytest.mark.asyncio
    async def test_get_task_not_found(self, mock_uow, mock_task_repository):
        """Test getting a non-existent task."""
        task_id = uuid.uuid4()
        mock_task_repository.get.return_value = None
        
        with patch("src.services.task_service.TaskRepository", return_value=mock_task_repository):
            service = TaskService(mock_uow)
            
            result = await service.get(task_id)
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_update_task_success(self, mock_uow, mock_task_repository, task_update_data):
        """Test successful task update."""
        task_id = uuid.uuid4()
        
        with patch("src.services.task_service.TaskRepository", return_value=mock_task_repository):
            service = TaskService(mock_uow)
            
            result = await service.update(task_id, task_update_data)
            
            assert result is not None
            mock_task_repository.update.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_task_success(self, mock_uow, mock_task_repository):
        """Test successful task deletion."""
        task_id = uuid.uuid4()
        
        with patch("src.services.task_service.TaskRepository", return_value=mock_task_repository):
            service = TaskService(mock_uow)
            
            result = await service.delete(task_id)
            
            assert result is True
            mock_task_repository.delete.assert_called_once_with(task_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_tasks(self, mock_uow, mock_task_repository):
        """Test listing all tasks."""
        with patch("src.services.task_service.TaskRepository", return_value=mock_task_repository):
            service = TaskService(mock_uow)
            
            result = await service.list()
            
            assert len(result) == 1
            assert result[0].name == "Test Task"
            mock_task_repository.list.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_validate_task_dependencies(self, mock_uow):
        """Test validation of task dependencies."""
        service = TaskService(mock_uow)
        
        # Test valid dependencies
        valid_context = ["task1", "task2"]
        available_tasks = ["task1", "task2", "task3"]
        
        service._validate_dependencies(valid_context, available_tasks)  # Should not raise
        
        # Test invalid dependencies (missing task)
        invalid_context = ["task1", "task4"]  # task4 doesn't exist
        
        with pytest.raises(ValueError, match="Task dependency not found"):
            service._validate_dependencies(invalid_context, available_tasks)
    
    @pytest.mark.asyncio
    async def test_validate_assigned_agent(self, mock_uow):
        """Test validation of assigned agent."""
        service = TaskService(mock_uow)
        
        # Test valid agent
        valid_agent = "researcher"
        available_agents = ["researcher", "writer", "analyst"]
        
        service._validate_agent(valid_agent, available_agents)  # Should not raise
        
        # Test invalid agent
        invalid_agent = "nonexistent_agent"
        
        with pytest.raises(ValueError, match="Agent not found"):
            service._validate_agent(invalid_agent, available_agents)
    
    @pytest.mark.asyncio
    async def test_validate_task_tools(self, mock_uow):
        """Test validation of task tools."""
        service = TaskService(mock_uow)
        
        # Test valid tools
        valid_tools = ["web_search", "document_analyzer"]
        available_tools = ["web_search", "document_analyzer", "calculator"]
        
        service._validate_tools(valid_tools, available_tools)  # Should not raise
        
        # Test invalid tools
        invalid_tools = ["nonexistent_tool"]
        
        with pytest.raises(ValueError, match="Tool not found"):
            service._validate_tools(invalid_tools, available_tools)
    
    @pytest.mark.asyncio
    async def test_validate_guardrails(self, mock_uow):
        """Test validation of task guardrails."""
        service = TaskService(mock_uow)
        
        # Test valid guardrails
        valid_guardrails = ["data_quality_check", "output_validation"]
        available_guardrails = ["data_quality_check", "output_validation", "safety_check"]
        
        service._validate_guardrails(valid_guardrails, available_guardrails)  # Should not raise
        
        # Test invalid guardrails
        invalid_guardrails = ["nonexistent_guardrail"]
        
        with pytest.raises(ValueError, match="Guardrail not found"):
            service._validate_guardrails(invalid_guardrails, available_guardrails)
    
    @pytest.mark.asyncio
    async def test_task_execution_order(self, mock_uow, mock_task_repository):
        """Test task execution order calculation."""
        with patch("src.services.task_service.TaskRepository", return_value=mock_task_repository):
            service = TaskService(mock_uow)
            
            # Mock multiple tasks with dependencies
            task1 = MagicMock()
            task1.name = "task1"
            task1.context = []
            
            task2 = MagicMock()
            task2.name = "task2"
            task2.context = ["task1"]
            
            task3 = MagicMock()
            task3.name = "task3"
            task3.context = ["task1", "task2"]
            
            tasks = [task3, task1, task2]  # Intentionally out of order
            
            execution_order = service._calculate_execution_order(tasks)
            
            # Should return tasks in dependency order
            assert execution_order[0].name == "task1"
            assert execution_order[1].name == "task2"
            assert execution_order[2].name == "task3"
    
    @pytest.mark.asyncio
    async def test_detect_circular_dependencies(self, mock_uow):
        """Test detection of circular dependencies."""
        service = TaskService(mock_uow)
        
        # Create tasks with circular dependency
        task1 = MagicMock()
        task1.name = "task1"
        task1.context = ["task2"]
        
        task2 = MagicMock()
        task2.name = "task2"
        task2.context = ["task1"]
        
        tasks = [task1, task2]
        
        with pytest.raises(ValueError, match="Circular dependency detected"):
            service._calculate_execution_order(tasks)
    
    @pytest.mark.asyncio
    async def test_task_clone(self, mock_uow, mock_task_repository):
        """Test task cloning functionality."""
        task_id = uuid.uuid4()
        new_name = "Cloned Task"
        
        with patch("src.services.task_service.TaskRepository", return_value=mock_task_repository):
            service = TaskService(mock_uow)
            
            cloned_task = await service.clone_task(task_id, new_name)
            
            assert cloned_task is not None
            mock_task_repository.get.assert_called_once_with(task_id)
            mock_task_repository.create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_task_metrics(self, mock_uow, mock_task_repository):
        """Test task metrics collection."""
        with patch("src.services.task_service.TaskRepository", return_value=mock_task_repository):
            service = TaskService(mock_uow)
            
            metrics = await service.get_task_metrics()
            
            assert "total_tasks" in metrics
            assert "active_tasks" in metrics
            assert "tasks_by_agent" in metrics
            assert metrics["total_tasks"] >= 0
            assert metrics["active_tasks"] >= 0
    
    @pytest.mark.asyncio
    async def test_task_execution_status_tracking(self, mock_uow, mock_task_repository):
        """Test task execution status tracking."""
        task_id = uuid.uuid4()
        execution_status = {
            "status": "completed",
            "execution_time": 120.5,
            "output": "Task completed successfully"
        }
        
        with patch("src.services.task_service.TaskRepository", return_value=mock_task_repository):
            service = TaskService(mock_uow)
            
            result = await service.update_execution_status(task_id, execution_status)
            
            assert result is not None
            mock_task_repository.update.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_task_template_creation(self, mock_uow, mock_task_repository):
        """Test creating task template."""
        task_id = uuid.uuid4()
        
        with patch("src.services.task_service.TaskRepository", return_value=mock_task_repository):
            service = TaskService(mock_uow)
            
            template = await service.create_template(task_id)
            
            assert template is not None
            assert "name" in template
            assert "description" in template
            assert "agent" in template
            mock_task_repository.get.assert_called_once_with(task_id)