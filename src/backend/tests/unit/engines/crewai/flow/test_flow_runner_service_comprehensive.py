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
        
        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionRepository') as mock_flow_exec_repo:
            with patch('src.engines.crewai.flow.flow_runner_service.FlowNodeExecutionRepository') as mock_node_exec_repo:
                with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository') as mock_flow_repo:
                    with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository') as mock_task_repo:
                        with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository') as mock_agent_repo:
                            with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository') as mock_tool_repo:
                                mock_flow_exec_instance = Mock()
                                mock_node_exec_instance = Mock()
                                mock_flow_instance = Mock()
                                mock_task_instance = Mock()
                                mock_agent_instance = Mock()
                                mock_tool_instance = Mock()
                                
                                mock_flow_exec_repo.return_value = mock_flow_exec_instance
                                mock_node_exec_repo.return_value = mock_node_exec_instance
                                mock_flow_repo.return_value = mock_flow_instance
                                mock_task_repo.return_value = mock_task_instance
                                mock_agent_repo.return_value = mock_agent_instance
                                mock_tool_repo.return_value = mock_tool_instance
                                
                                service = FlowRunnerService(mock_db)
                                
                                assert service.db == mock_db
                                assert service.flow_execution_repo == mock_flow_exec_instance
                                assert service.node_execution_repo == mock_node_exec_instance
                                assert service.flow_repo == mock_flow_instance
                                assert service.task_repo == mock_task_instance
                                assert service.agent_repo == mock_agent_instance
                                assert service.tool_repo == mock_tool_instance

    def test_flow_runner_service_init_creates_repositories(self):
        """Test FlowRunnerService __init__ creates all repository instances"""
        mock_db = Mock()
        
        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionRepository') as mock_flow_exec_repo:
            with patch('src.engines.crewai.flow.flow_runner_service.FlowNodeExecutionRepository') as mock_node_exec_repo:
                with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository') as mock_flow_repo:
                    with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository') as mock_task_repo:
                        with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository') as mock_agent_repo:
                            with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository') as mock_tool_repo:
                                service = FlowRunnerService(mock_db)
                                
                                # Verify all repositories were created with the database session
                                mock_flow_exec_repo.assert_called_once_with(mock_db)
                                mock_node_exec_repo.assert_called_once_with(mock_db)
                                mock_flow_repo.assert_called_once_with(mock_db)
                                mock_task_repo.assert_called_once_with(mock_db)
                                mock_agent_repo.assert_called_once_with(mock_db)
                                mock_tool_repo.assert_called_once_with(mock_db)

    def test_flow_runner_service_init_stores_attributes(self):
        """Test FlowRunnerService __init__ stores all attributes correctly"""
        mock_db = Mock()
        
        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionRepository'):
            with patch('src.engines.crewai.flow.flow_runner_service.FlowNodeExecutionRepository'):
                with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository'):
                    with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository'):
                        with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository'):
                            with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository'):
                                service = FlowRunnerService(mock_db)
                                
                                # Check all attributes are stored
                                assert hasattr(service, 'db')
                                assert hasattr(service, 'flow_execution_repo')
                                assert hasattr(service, 'node_execution_repo')
                                assert hasattr(service, 'flow_repo')
                                assert hasattr(service, 'task_repo')
                                assert hasattr(service, 'agent_repo')
                                assert hasattr(service, 'tool_repo')
                                
                                assert service.db == mock_db
                                assert service.flow_execution_repo is not None
                                assert service.node_execution_repo is not None
                                assert service.flow_repo is not None
                                assert service.task_repo is not None
                                assert service.agent_repo is not None
                                assert service.tool_repo is not None


class TestFlowRunnerServiceCreateFlowExecution:
    """Test FlowRunnerService create_flow_execution method"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_db = Mock()
        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionRepository'):
            with patch('src.engines.crewai.flow.flow_runner_service.FlowNodeExecutionRepository'):
                with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository'):
                    with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository'):
                        with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository'):
                            with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository'):
                                self.service = FlowRunnerService(self.mock_db)

    def test_create_flow_execution_with_uuid_flow_id(self):
        """Test create_flow_execution with UUID flow_id"""
        flow_id = uuid.uuid4()
        job_id = "test-job-id"
        config = {"test": "config"}

        result = self.service.create_flow_execution(flow_id, job_id, config)

        assert isinstance(result, dict)
        assert "execution_id" in result
        assert "flow_id" in result
        assert "job_id" in result
        assert result["flow_id"] == flow_id  # UUID object, not string
        assert result["job_id"] == job_id

    def test_create_flow_execution_with_string_flow_id(self):
        """Test create_flow_execution with invalid string flow_id"""
        flow_id = "test-flow-id"  # Invalid UUID format
        job_id = "test-job-id"
        config = {"test": "config"}

        result = self.service.create_flow_execution(flow_id, job_id, config)

        assert isinstance(result, dict)
        assert "success" in result
        assert result["success"] is False
        assert "error" in result
        assert "Invalid UUID format" in result["error"]
        assert result["flow_id"] == flow_id
        assert result["job_id"] == job_id

    def test_create_flow_execution_with_valid_uuid_string(self):
        """Test create_flow_execution with valid UUID string"""
        flow_id = str(uuid.uuid4())  # Valid UUID string
        job_id = "test-job-id"

        result = self.service.create_flow_execution(flow_id, job_id)

        assert isinstance(result, dict)
        assert "execution_id" in result
        assert "flow_id" in result
        assert "job_id" in result
        assert result["flow_id"] == uuid.UUID(flow_id)  # Converted to UUID
        assert result["job_id"] == job_id

    def test_create_flow_execution_with_none_config(self):
        """Test create_flow_execution with None config and valid UUID"""
        flow_id = uuid.uuid4()
        job_id = "test-job-id"
        config = None

        result = self.service.create_flow_execution(flow_id, job_id, config)

        assert isinstance(result, dict)
        assert "execution_id" in result
        assert "flow_id" in result
        assert "job_id" in result
        assert result["flow_id"] == flow_id
        assert result["job_id"] == job_id

    def test_create_flow_execution_calls_repository_create(self):
        """Test create_flow_execution calls repository create method"""
        flow_id = uuid.uuid4()
        job_id = "test-job-id"

        result = self.service.create_flow_execution(flow_id, job_id)

        # Should call the repository create method
        self.service.flow_execution_repo.create.assert_called()
        assert isinstance(result, dict)
        assert "execution_id" in result
        assert "success" in result


class TestFlowRunnerServiceGetFlowExecution:
    """Test FlowRunnerService get_flow_execution method"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_db = Mock()
        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionRepository'):
            with patch('src.engines.crewai.flow.flow_runner_service.FlowNodeExecutionRepository'):
                with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository'):
                    with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository'):
                        with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository'):
                            with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository'):
                                self.service = FlowRunnerService(self.mock_db)

    def test_get_flow_execution_basic(self):
        """Test get_flow_execution with basic execution_id"""
        execution_id = 123

        result = self.service.get_flow_execution(execution_id)

        assert isinstance(result, dict)
        # Should return some basic structure - actual implementation returns "execution" or "error"
        assert "execution" in result or "error" in result
        assert "success" in result

    def test_get_flow_execution_different_ids(self):
        """Test get_flow_execution with different execution IDs"""
        execution_ids = [1, 123, 999, 0]
        
        for execution_id in execution_ids:
            result = self.service.get_flow_execution(execution_id)
            assert isinstance(result, dict)

    def test_get_flow_execution_negative_id(self):
        """Test get_flow_execution with negative execution_id"""
        execution_id = -1
        
        result = self.service.get_flow_execution(execution_id)
        
        assert isinstance(result, dict)


class TestFlowRunnerServiceGetFlowExecutionsByFlow:
    """Test FlowRunnerService get_flow_executions_by_flow method"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_db = Mock()
        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionRepository'):
            with patch('src.engines.crewai.flow.flow_runner_service.FlowNodeExecutionRepository'):
                with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository'):
                    with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository'):
                        with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository'):
                            with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository'):
                                self.service = FlowRunnerService(self.mock_db)

    def test_get_flow_executions_by_flow_with_uuid(self):
        """Test get_flow_executions_by_flow with UUID flow_id"""
        flow_id = uuid.uuid4()
        
        result = self.service.get_flow_executions_by_flow(flow_id)
        
        assert isinstance(result, dict)
        # Should return some basic structure
        assert "executions" in result or "error" in result or "flow_id" in result

    def test_get_flow_executions_by_flow_with_string(self):
        """Test get_flow_executions_by_flow with string flow_id"""
        flow_id = "test-flow-id"
        
        result = self.service.get_flow_executions_by_flow(flow_id)
        
        assert isinstance(result, dict)

    def test_get_flow_executions_by_flow_different_ids(self):
        """Test get_flow_executions_by_flow with different flow IDs"""
        flow_ids = ["flow1", "flow2", str(uuid.uuid4()), ""]
        
        for flow_id in flow_ids:
            result = self.service.get_flow_executions_by_flow(flow_id)
            assert isinstance(result, dict)


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
        from src.engines.crewai.flow.flow_runner_service import (
            os, logging, asyncio, uuid, datetime
        )
        
        assert os is not None
        assert logging is not None
        assert asyncio is not None
        assert uuid is not None
        assert datetime is not None

    def test_typing_imports(self):
        """Test typing imports"""
        from src.engines.crewai.flow.flow_runner_service import (
            Dict, List, Optional, Any, Union
        )
        
        assert Dict is not None
        assert List is not None
        assert Optional is not None
        assert Any is not None
        assert Union is not None

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

    def test_repository_imports(self):
        """Test repository imports"""
        from src.engines.crewai.flow.flow_runner_service import (
            FlowExecutionRepository, FlowNodeExecutionRepository, FlowRepository,
            TaskRepository, AgentRepository, ToolRepository
        )
        
        assert FlowExecutionRepository is not None
        assert FlowNodeExecutionRepository is not None
        assert FlowRepository is not None
        assert TaskRepository is not None
        assert AgentRepository is not None
        assert ToolRepository is not None

    def test_service_imports(self):
        """Test service imports"""
        from src.engines.crewai.flow.flow_runner_service import (
            LoggerManager, async_session_factory, ApiKeysService, BackendFlow
        )
        
        assert LoggerManager is not None
        assert async_session_factory is not None
        assert ApiKeysService is not None
        assert BackendFlow is not None


class TestFlowRunnerServiceMethodSignatures:
    """Test FlowRunnerService method signatures and basic structure"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_db = Mock()
        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionRepository'):
            with patch('src.engines.crewai.flow.flow_runner_service.FlowNodeExecutionRepository'):
                with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository'):
                    with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository'):
                        with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository'):
                            with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository'):
                                self.service = FlowRunnerService(self.mock_db)

    def test_run_flow_method_exists(self):
        """Test run_flow method exists and is async"""
        assert hasattr(self.service, 'run_flow')
        assert callable(self.service.run_flow)
        # Check if it's a coroutine function (async)
        import inspect
        assert inspect.iscoroutinefunction(self.service.run_flow)

    def test_private_run_dynamic_flow_method_exists(self):
        """Test _run_dynamic_flow method exists and is async"""
        assert hasattr(self.service, '_run_dynamic_flow')
        assert callable(self.service._run_dynamic_flow)
        import inspect
        assert inspect.iscoroutinefunction(self.service._run_dynamic_flow)

    def test_private_run_flow_execution_method_exists(self):
        """Test _run_flow_execution method exists and is async"""
        assert hasattr(self.service, '_run_flow_execution')
        assert callable(self.service._run_flow_execution)
        import inspect
        assert inspect.iscoroutinefunction(self.service._run_flow_execution)

    def test_private_create_flow_from_config_method_exists(self):
        """Test _create_flow_from_config method exists"""
        assert hasattr(self.service, '_create_flow_from_config')
        assert callable(self.service._create_flow_from_config)
        import inspect
        # This should be a regular method, not async
        assert not inspect.iscoroutinefunction(self.service._create_flow_from_config)

    def test_all_required_methods_exist(self):
        """Test that all required methods exist"""
        required_methods = [
            'create_flow_execution', 'run_flow', 'get_flow_execution',
            'get_flow_executions_by_flow', '_run_dynamic_flow',
            '_run_flow_execution', '_create_flow_from_config'
        ]
        
        for method_name in required_methods:
            assert hasattr(self.service, method_name)
            assert callable(getattr(self.service, method_name))


class TestFlowRunnerServiceAttributes:
    """Test FlowRunnerService attribute access and properties"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_db = Mock()
        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionRepository'):
            with patch('src.engines.crewai.flow.flow_runner_service.FlowNodeExecutionRepository'):
                with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository'):
                    with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository'):
                        with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository'):
                            with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository'):
                                self.service = FlowRunnerService(self.mock_db)

    def test_service_has_required_attributes(self):
        """Test that service has all required attributes after initialization"""
        required_attributes = [
            'db', 'flow_execution_repo', 'node_execution_repo',
            'flow_repo', 'task_repo', 'agent_repo', 'tool_repo'
        ]
        
        for attr_name in required_attributes:
            assert hasattr(self.service, attr_name)
            assert getattr(self.service, attr_name) is not None

    def test_service_db_storage(self):
        """Test service stores database session correctly"""
        assert self.service.db == self.mock_db
        
        # Test with different session
        new_mock_db = Mock()
        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionRepository'):
            with patch('src.engines.crewai.flow.flow_runner_service.FlowNodeExecutionRepository'):
                with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository'):
                    with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository'):
                        with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository'):
                            with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository'):
                                new_service = FlowRunnerService(new_mock_db)
                                assert new_service.db == new_mock_db
                                assert new_service.db != self.mock_db

    def test_service_repositories_are_separate(self):
        """Test that repositories are separate instances"""
        repositories = [
            self.service.flow_execution_repo, self.service.node_execution_repo,
            self.service.flow_repo, self.service.task_repo,
            self.service.agent_repo, self.service.tool_repo
        ]
        
        # All repositories should be different objects
        for i, repo1 in enumerate(repositories):
            for j, repo2 in enumerate(repositories):
                if i != j:
                    assert repo1 is not repo2
