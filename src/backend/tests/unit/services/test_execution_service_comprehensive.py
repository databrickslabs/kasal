import pytest
from unittest.mock import Mock, patch, AsyncMock
from typing import Dict, Any, Optional, List
import uuid
import json
from datetime import datetime

# Test execution service - based on actual code inspection

from src.services.execution_service import ExecutionService


class TestExecutionServiceInit:
    """Test ExecutionService initialization"""

    def test_execution_service_init_with_session(self):
        """Test ExecutionService __init__ with session parameter"""
        mock_session = Mock()

        with patch('src.services.execution_service.ExecutionNameService') as mock_name_service:
            with patch('src.services.execution_service.CrewAIExecutionService') as mock_crew_service:
                mock_name_instance = Mock()
                mock_crew_instance = Mock()
                mock_name_service.create.return_value = mock_name_instance
                mock_crew_service.return_value = mock_crew_instance

                service = ExecutionService(mock_session)

                assert service.session == mock_session
                assert service.execution_name_service == mock_name_instance
                assert service.crewai_execution_service == mock_crew_instance

    def test_execution_service_init_without_session(self):
        """Test ExecutionService __init__ without session parameter"""
        with patch('src.services.execution_service.ExecutionNameService') as mock_name_service:
            with patch('src.services.execution_service.CrewAIExecutionService') as mock_crew_service:
                mock_name_instance = Mock()
                mock_crew_instance = Mock()
                mock_name_service.create.return_value = mock_name_instance
                mock_crew_service.return_value = mock_crew_instance

                service = ExecutionService()

                assert service.session is None
                assert service.execution_name_service == mock_name_instance
                assert service.crewai_execution_service == mock_crew_instance

    def test_execution_service_init_creates_services(self):
        """Test ExecutionService __init__ creates service instances"""
        mock_session = Mock()

        with patch('src.services.execution_service.ExecutionNameService') as mock_name_service:
            with patch('src.services.execution_service.CrewAIExecutionService') as mock_crew_service:
                service = ExecutionService(mock_session)

                # Verify services were created properly
                mock_name_service.create.assert_called_once_with(mock_session)
                mock_crew_service.assert_called_once()

    def test_execution_service_init_class_attributes(self):
        """Test ExecutionService __init__ uses class attributes"""
        with patch('src.services.execution_service.ExecutionNameService'):
            with patch('src.services.execution_service.CrewAIExecutionService'):
                service = ExecutionService()

                # Should have access to class-level attributes
                assert hasattr(ExecutionService, 'executions')
                assert hasattr(ExecutionService, '_thread_pool')
                assert isinstance(ExecutionService.executions, dict)
                assert ExecutionService._thread_pool is not None


class TestExecutionServiceStaticMethods:
    """Test ExecutionService static methods"""

    def test_create_execution_id(self):
        """Test create_execution_id static method"""
        result = ExecutionService.create_execution_id()
        
        assert isinstance(result, str)
        assert len(result) > 0
        # Should be a valid UUID format
        try:
            uuid.UUID(result)
            assert True
        except ValueError:
            assert False, "Generated execution ID is not a valid UUID"

    def test_create_execution_id_unique(self):
        """Test create_execution_id generates unique IDs"""
        id1 = ExecutionService.create_execution_id()
        id2 = ExecutionService.create_execution_id()
        
        assert id1 != id2

    def test_create_execution_id_multiple_calls(self):
        """Test create_execution_id with multiple calls"""
        ids = set()
        for _ in range(10):
            execution_id = ExecutionService.create_execution_id()
            assert execution_id not in ids
            ids.add(execution_id)

    def test_get_execution_static(self):
        """Test get_execution static method"""
        execution_id = "test-execution-id"
        
        # Should return None for non-existent execution
        result = ExecutionService.get_execution(execution_id)
        
        assert result is None

    def test_get_execution_static_with_none(self):
        """Test get_execution static method with None"""
        result = ExecutionService.get_execution(None)
        
        assert result is None

    def test_get_execution_static_with_empty_string(self):
        """Test get_execution static method with empty string"""
        result = ExecutionService.get_execution("")
        
        assert result is None


class TestExecutionServiceAddExecutionToMemory:
    """Test ExecutionService add_execution_to_memory static method"""

    def test_add_execution_to_memory_basic(self):
        """Test add_execution_to_memory with basic parameters"""
        execution_id = "test-execution-id"
        status = "running"
        run_name = "Test Run"

        # Should not raise an exception
        ExecutionService.add_execution_to_memory(execution_id, status, run_name)

        # Should be able to retrieve it
        result = ExecutionService.get_execution(execution_id)
        assert result is not None
        assert result["status"] == status
        assert result["run_name"] == run_name

    def test_add_execution_to_memory_with_optional_params(self):
        """Test add_execution_to_memory with optional parameters"""
        execution_id = "test-execution-id-2"
        status = "completed"
        run_name = "Test Run 2"
        group_id = 123
        group_email = "test@example.com"

        ExecutionService.add_execution_to_memory(
            execution_id, status, run_name, group_id=group_id, group_email=group_email
        )

        result = ExecutionService.get_execution(execution_id)
        assert result is not None
        assert result["status"] == status
        assert result["run_name"] == run_name
        assert result["group_id"] == group_id
        assert result["group_email"] == group_email

    def test_add_execution_to_memory_overwrite(self):
        """Test add_execution_to_memory overwrites existing execution"""
        execution_id = "test-execution-id-3"

        # Add first execution
        ExecutionService.add_execution_to_memory(execution_id, "running", "First Run")
        result1 = ExecutionService.get_execution(execution_id)
        assert result1["run_name"] == "First Run"

        # Overwrite with second execution
        ExecutionService.add_execution_to_memory(execution_id, "completed", "Second Run")
        result2 = ExecutionService.get_execution(execution_id)
        assert result2["run_name"] == "Second Run"
        assert result2["status"] == "completed"


class TestExecutionServiceSanitizeForDatabase:
    """Test ExecutionService sanitize_for_database static method"""

    def test_sanitize_for_database_basic(self):
        """Test sanitize_for_database with basic data"""
        data = {
            "string_field": "test_value",
            "int_field": 42,
            "bool_field": True,
            "list_field": [1, 2, 3],
            "dict_field": {"nested": "value"}
        }
        
        result = ExecutionService.sanitize_for_database(data)
        
        assert isinstance(result, dict)
        assert result["string_field"] == "test_value"
        assert result["int_field"] == 42
        assert result["bool_field"] is True
        assert result["list_field"] == [1, 2, 3]
        assert result["dict_field"] == {"nested": "value"}

    def test_sanitize_for_database_with_none_values(self):
        """Test sanitize_for_database with None values"""
        data = {
            "field1": "value1",
            "field2": None,
            "field3": "value3"
        }
        
        result = ExecutionService.sanitize_for_database(data)
        
        assert isinstance(result, dict)
        assert result["field1"] == "value1"
        assert result["field2"] is None
        assert result["field3"] == "value3"

    def test_sanitize_for_database_with_datetime(self):
        """Test sanitize_for_database with datetime objects"""
        now = datetime.now()
        data = {
            "timestamp": now,
            "other_field": "value"
        }
        
        result = ExecutionService.sanitize_for_database(data)
        
        assert isinstance(result, dict)
        assert result["other_field"] == "value"
        # datetime should be converted to string
        assert isinstance(result["timestamp"], str)

    def test_sanitize_for_database_with_uuid(self):
        """Test sanitize_for_database with UUID objects"""
        test_uuid = uuid.uuid4()
        data = {
            "id": test_uuid,
            "name": "test"
        }
        
        result = ExecutionService.sanitize_for_database(data)
        
        assert isinstance(result, dict)
        assert result["name"] == "test"
        # UUID should be converted to string
        assert isinstance(result["id"], str)
        assert result["id"] == str(test_uuid)

    def test_sanitize_for_database_nested_objects(self):
        """Test sanitize_for_database with nested objects"""
        test_uuid = uuid.uuid4()
        now = datetime.now()
        data = {
            "nested": {
                "uuid_field": test_uuid,
                "datetime_field": now,
                "string_field": "value"
            },
            "list_with_objects": [
                {"uuid": test_uuid},
                {"datetime": now}
            ]
        }
        
        result = ExecutionService.sanitize_for_database(data)
        
        assert isinstance(result, dict)
        assert isinstance(result["nested"]["uuid_field"], str)
        assert isinstance(result["nested"]["datetime_field"], str)
        assert result["nested"]["string_field"] == "value"
        
        assert isinstance(result["list_with_objects"][0]["uuid"], str)
        assert isinstance(result["list_with_objects"][1]["datetime"], str)

    def test_sanitize_for_database_empty_dict(self):
        """Test sanitize_for_database with empty dictionary"""
        data = {}
        
        result = ExecutionService.sanitize_for_database(data)
        
        assert isinstance(result, dict)
        assert len(result) == 0

    def test_sanitize_for_database_preserves_json_serializable(self):
        """Test sanitize_for_database preserves JSON serializable data"""
        data = {
            "string": "test",
            "number": 42,
            "boolean": True,
            "null": None,
            "array": [1, 2, 3],
            "object": {"key": "value"}
        }
        
        result = ExecutionService.sanitize_for_database(data)
        
        # Should be JSON serializable
        json_str = json.dumps(result)
        assert isinstance(json_str, str)
        
        # Should preserve all values
        assert result == data


class TestExecutionServiceConstants:
    """Test ExecutionService constants and module-level attributes"""

    def test_logger_initialization(self):
        """Test logger is properly initialized"""
        from src.services.execution_service import logger
        
        assert logger is not None
        assert hasattr(logger, 'info')
        assert hasattr(logger, 'error')
        assert hasattr(logger, 'warning')

    def test_required_imports(self):
        """Test that required imports are available"""
        from src.services.execution_service import (
            logging, sys, traceback, json, os, uuid, concurrent, asyncio
        )
        
        assert logging is not None
        assert sys is not None
        assert traceback is not None
        assert json is not None
        assert os is not None
        assert uuid is not None
        assert concurrent is not None
        assert asyncio is not None

    def test_schema_imports(self):
        """Test schema imports"""
        from src.services.execution_service import (
            ExecutionStatus, CrewConfig, ExecutionNameGenerationRequest, ExecutionCreateResponse
        )
        
        assert ExecutionStatus is not None
        assert CrewConfig is not None
        assert ExecutionNameGenerationRequest is not None
        assert ExecutionCreateResponse is not None

    def test_service_imports(self):
        """Test service imports"""
        from src.services.execution_service import (
            CrewAIExecutionService, ExecutionStatusService, ExecutionNameService
        )
        
        assert CrewAIExecutionService is not None
        assert ExecutionStatusService is not None
        assert ExecutionNameService is not None

    def test_utils_imports(self):
        """Test utils imports"""
        from src.services.execution_service import (
            run_in_thread_with_loop, create_and_run_loop, GroupContext
        )
        
        assert run_in_thread_with_loop is not None
        assert create_and_run_loop is not None
        assert GroupContext is not None


class TestExecutionServiceAttributes:
    """Test ExecutionService attribute access and properties"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = Mock()
        with patch('src.services.execution_service.ExecutionNameService'):
            with patch('src.services.execution_service.CrewAIExecutionService'):
                self.service = ExecutionService(self.mock_session)

    def test_service_has_required_attributes(self):
        """Test that service has all required attributes after initialization"""
        # Check all required attributes exist
        assert hasattr(self.service, 'session')
        assert hasattr(self.service, 'execution_name_service')
        assert hasattr(self.service, 'crewai_execution_service')

        # Check attribute values
        assert self.service.session == self.mock_session
        assert self.service.execution_name_service is not None
        assert self.service.crewai_execution_service is not None

    def test_service_session_storage(self):
        """Test service stores session correctly"""
        assert self.service.session == self.mock_session

        # Test with different session
        new_mock_session = Mock()
        with patch('src.services.execution_service.ExecutionNameService'):
            with patch('src.services.execution_service.CrewAIExecutionService'):
                new_service = ExecutionService(new_mock_session)
                assert new_service.session == new_mock_session
                assert new_service.session != self.mock_session

    def test_service_services_are_separate(self):
        """Test that services are separate instances"""
        assert self.service.execution_name_service is not self.service.crewai_execution_service
        assert self.service.execution_name_service is not None
        assert self.service.crewai_execution_service is not None
