import pytest
from unittest.mock import Mock, patch, AsyncMock
from typing import Dict, Any, Optional, List, Union
import uuid
from datetime import datetime

# Test flow runner service - based on actual code inspection

from src.engines.crewai.flow.flow_runner_service import FlowRunnerService


class TestFlowRunnerServiceInit:
    """Test FlowRunnerService initialization"""

    def test_flow_runner_service_init_with_db_session(self):
        """Test FlowRunnerService __init__ with database session"""
        mock_db = Mock()

        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService') as mock_flow_exec_service:
            with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository') as mock_flow_repo:
                with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository') as mock_task_repo:
                    with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository') as mock_agent_repo:
                        with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository') as mock_tool_repo:
                            with patch('src.engines.crewai.flow.flow_runner_service.CrewRepository') as mock_crew_repo:
                                mock_flow_exec_instance = Mock()
                                mock_flow_instance = Mock()
                                mock_task_instance = Mock()
                                mock_agent_instance = Mock()
                                mock_tool_instance = Mock()
                                mock_crew_instance = Mock()

                                mock_flow_exec_service.return_value = mock_flow_exec_instance
                                mock_flow_repo.return_value = mock_flow_instance
                                mock_task_repo.return_value = mock_task_instance
                                mock_agent_repo.return_value = mock_agent_instance
                                mock_tool_repo.return_value = mock_tool_instance
                                mock_crew_repo.return_value = mock_crew_instance

                                service = FlowRunnerService(mock_db)

                                assert service.db == mock_db
                                assert service.flow_execution_service == mock_flow_exec_instance
                                assert service.flow_repo == mock_flow_instance
                                assert service.task_repo == mock_task_instance
                                assert service.agent_repo == mock_agent_instance
                                assert service.tool_repo == mock_tool_instance
                                assert service.crew_repo == mock_crew_instance

    def test_flow_runner_service_init_creates_repositories(self):
        """Test FlowRunnerService __init__ creates all repository instances"""
        mock_db = Mock()

        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService') as mock_flow_exec_service:
            with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository') as mock_flow_repo:
                with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository') as mock_task_repo:
                    with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository') as mock_agent_repo:
                        with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository') as mock_tool_repo:
                            with patch('src.engines.crewai.flow.flow_runner_service.CrewRepository') as mock_crew_repo:
                                service = FlowRunnerService(mock_db)

                                # Verify all repositories were created with the database session
                                mock_flow_exec_service.assert_called_once_with(mock_db)
                                mock_flow_repo.assert_called_once_with(mock_db)
                                mock_task_repo.assert_called_once_with(mock_db)
                                mock_agent_repo.assert_called_once_with(mock_db)
                                mock_tool_repo.assert_called_once_with(mock_db)
                                mock_crew_repo.assert_called_once_with(mock_db)

    def test_flow_runner_service_init_stores_attributes(self):
        """Test FlowRunnerService __init__ stores all attributes correctly"""
        mock_db = Mock()

        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService'):
            with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository'):
                with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository'):
                    with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository'):
                        with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository'):
                            with patch('src.engines.crewai.flow.flow_runner_service.CrewRepository'):
                                service = FlowRunnerService(mock_db)

                                # Check all attributes are stored
                                assert hasattr(service, 'db')
                                assert hasattr(service, 'flow_execution_service')
                                assert hasattr(service, 'flow_repo')
                                assert hasattr(service, 'task_repo')
                                assert hasattr(service, 'agent_repo')
                                assert hasattr(service, 'tool_repo')
                                assert hasattr(service, 'crew_repo')

                                assert service.db == mock_db


class TestFlowRunnerServiceCreateFlowExecution:
    """Test FlowRunnerService create_flow_execution method"""

    @pytest.fixture
    def service(self):
        """Create a service with mocked dependencies"""
        mock_db = Mock()
        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService') as mock_exec_service:
            with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository'):
                with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository'):
                    with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository'):
                        with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository'):
                            with patch('src.engines.crewai.flow.flow_runner_service.CrewRepository'):
                                svc = FlowRunnerService(mock_db)
                                # Mock the create_execution method
                                mock_execution = Mock()
                                mock_execution.id = 1
                                mock_execution.flow_id = uuid.uuid4()
                                mock_execution.status = "pending"
                                svc.flow_execution_service.create_execution = AsyncMock(return_value=mock_execution)
                                return svc

    @pytest.mark.asyncio
    async def test_create_flow_execution_with_uuid_flow_id(self, service):
        """Test create_flow_execution with UUID flow_id"""
        flow_id = uuid.uuid4()
        job_id = "test-job-id"
        config = {"test": "config"}

        result = await service.create_flow_execution(flow_id, job_id, config)

        assert isinstance(result, dict)
        assert result["success"] is True
        assert "execution_id" in result
        assert "job_id" in result
        assert result["job_id"] == job_id

    @pytest.mark.asyncio
    async def test_create_flow_execution_with_valid_uuid_string(self, service):
        """Test create_flow_execution with valid UUID string"""
        flow_id = str(uuid.uuid4())  # Valid UUID string
        job_id = "test-job-id"

        result = await service.create_flow_execution(flow_id, job_id)

        assert isinstance(result, dict)
        assert result["success"] is True
        assert "execution_id" in result
        assert "job_id" in result

    @pytest.mark.asyncio
    async def test_create_flow_execution_with_none_config(self, service):
        """Test create_flow_execution with None config and valid UUID"""
        flow_id = uuid.uuid4()
        job_id = "test-job-id"
        config = None

        result = await service.create_flow_execution(flow_id, job_id, config)

        assert isinstance(result, dict)
        assert result["success"] is True
        assert "execution_id" in result

    @pytest.mark.asyncio
    async def test_create_flow_execution_calls_service_create(self, service):
        """Test create_flow_execution calls service create method"""
        flow_id = uuid.uuid4()
        job_id = "test-job-id"

        result = await service.create_flow_execution(flow_id, job_id)

        # Should call the service create method
        service.flow_execution_service.create_execution.assert_called_once()
        assert isinstance(result, dict)
        assert "execution_id" in result

    @pytest.mark.asyncio
    async def test_create_flow_execution_handles_exception(self):
        """Test create_flow_execution handles exceptions"""
        mock_db = Mock()
        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService') as mock_exec_service:
            with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository'):
                with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository'):
                    with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository'):
                        with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository'):
                            with patch('src.engines.crewai.flow.flow_runner_service.CrewRepository'):
                                svc = FlowRunnerService(mock_db)
                                svc.flow_execution_service.create_execution = AsyncMock(side_effect=Exception("Test error"))

                                result = await svc.create_flow_execution(uuid.uuid4(), "job-id")

                                assert result["success"] is False
                                assert "error" in result


class TestFlowRunnerServiceConstants:
    """Test FlowRunnerService constants and module-level attributes"""

    def test_logger_initialization(self):
        """Test logger is properly initialized"""
        from src.engines.crewai.flow.flow_runner_service import logger

        assert logger is not None
        assert hasattr(logger, 'info')
        assert hasattr(logger, 'error')
        assert hasattr(logger, 'warning')

    def test_required_imports(self):
        """Test that required imports are available"""
        from src.engines.crewai.flow import flow_runner_service

        # Check module has the expected structure
        assert hasattr(flow_runner_service, 'FlowRunnerService')
        assert hasattr(flow_runner_service, 'logger')

    def test_schema_imports(self):
        """Test schema imports"""
        from src.engines.crewai.flow.flow_runner_service import (
            FlowExecutionCreate, FlowExecutionUpdate, FlowNodeExecutionCreate,
            FlowNodeExecutionUpdate, FlowExecutionStatus
        )

        assert FlowExecutionCreate is not None
        assert FlowExecutionUpdate is not None
        assert FlowNodeExecutionCreate is not None
        assert FlowNodeExecutionUpdate is not None
        assert FlowExecutionStatus is not None

    def test_service_imports(self):
        """Test service imports"""
        from src.engines.crewai.flow.flow_runner_service import (
            FlowExecutionService, FlowRepository,
            TaskRepository, AgentRepository, ToolRepository
        )

        assert FlowExecutionService is not None
        assert FlowRepository is not None
        assert TaskRepository is not None
        assert AgentRepository is not None
        assert ToolRepository is not None


class TestFlowRunnerServiceMethodSignatures:
    """Test FlowRunnerService method signatures and basic structure"""

    @pytest.fixture
    def service(self):
        """Create a service with mocked dependencies"""
        mock_db = Mock()
        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService'):
            with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository'):
                with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository'):
                    with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository'):
                        with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository'):
                            with patch('src.engines.crewai.flow.flow_runner_service.CrewRepository'):
                                return FlowRunnerService(mock_db)

    def test_create_flow_execution_method_exists(self, service):
        """Test create_flow_execution method exists and is async"""
        assert hasattr(service, 'create_flow_execution')
        assert callable(service.create_flow_execution)
        # Check if it's a coroutine function (async)
        import inspect
        assert inspect.iscoroutinefunction(service.create_flow_execution)

    def test_all_required_methods_exist(self, service):
        """Test that all required methods exist"""
        required_methods = ['create_flow_execution']

        for method_name in required_methods:
            assert hasattr(service, method_name)
            assert callable(getattr(service, method_name))


class TestFlowRunnerServiceAttributes:
    """Test FlowRunnerService attribute access and properties"""

    @pytest.fixture
    def mock_db(self):
        return Mock()

    @pytest.fixture
    def service(self, mock_db):
        """Create a service with mocked dependencies"""
        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService'):
            with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository'):
                with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository'):
                    with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository'):
                        with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository'):
                            with patch('src.engines.crewai.flow.flow_runner_service.CrewRepository'):
                                return FlowRunnerService(mock_db)

    def test_service_has_required_attributes(self, service):
        """Test that service has all required attributes after initialization"""
        required_attributes = [
            'db', 'flow_execution_service', 'flow_repo',
            'task_repo', 'agent_repo', 'tool_repo', 'crew_repo'
        ]

        for attr_name in required_attributes:
            assert hasattr(service, attr_name)
            assert getattr(service, attr_name) is not None

    def test_service_db_storage(self, mock_db, service):
        """Test service stores database session correctly"""
        assert service.db == mock_db

    def test_service_repositories_are_separate(self, service):
        """Test that repositories are separate instances"""
        repositories = [
            service.flow_execution_service, service.flow_repo,
            service.task_repo, service.agent_repo,
            service.tool_repo, service.crew_repo
        ]

        # All repositories should be different objects
        for i, repo1 in enumerate(repositories):
            for j, repo2 in enumerate(repositories):
                if i != j:
                    assert repo1 is not repo2
