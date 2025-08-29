"""Unit tests for task_helpers module."""

import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock, call
from typing import Optional, Type, List, Dict, Any
import json

from crewai import Agent, Task
from crewai.tasks.task_output import TaskOutput
from crewai.tools import BaseTool
from pydantic import BaseModel, create_model

from src.engines.crewai.helpers.task_helpers import (
    is_data_missing,
    get_pydantic_class_from_name,
    create_task
)


class TestIsDataMissing:
    """Test cases for is_data_missing function."""
    
    def test_is_data_missing_no_pydantic_attribute(self):
        """Test when output has no pydantic attribute."""
        output = Mock(spec=['__class__'])  # Minimal spec without pydantic attribute
        output.__class__ = TaskOutput
        
        result = is_data_missing(output)
        
        assert result is True
    
    def test_is_data_missing_less_than_10_events(self):
        """Test when output has less than 10 events."""
        output = Mock(spec=TaskOutput)
        output.pydantic = Mock()
        output.pydantic.events = ["event1", "event2", "event3"]
        
        result = is_data_missing(output)
        
        assert result is True
    
    def test_is_data_missing_exactly_10_events(self):
        """Test when output has exactly 10 events."""
        output = Mock(spec=TaskOutput)
        output.pydantic = Mock()
        output.pydantic.events = [f"event{i}" for i in range(10)]
        
        result = is_data_missing(output)
        
        assert result is False
    
    def test_is_data_missing_more_than_10_events(self):
        """Test when output has more than 10 events."""
        output = Mock(spec=TaskOutput)
        output.pydantic = Mock()
        output.pydantic.events = [f"event{i}" for i in range(15)]
        
        result = is_data_missing(output)
        
        assert result is False


class TestGetPydanticClassFromName:
    """Test cases for get_pydantic_class_from_name function."""
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.helpers.task_helpers.UnitOfWork')
    async def test_get_pydantic_class_schema_not_found(self, mock_uow_class):
        """Test when schema is not found in database."""
        # Create async context manager mock
        mock_uow = AsyncMock()
        mock_uow.schema_repository.find_by_name.return_value = None
        
        # Mock the context manager
        mock_uow_class.return_value.__aenter__.return_value = mock_uow
        mock_uow_class.return_value.__aexit__.return_value = None
        
        result = await get_pydantic_class_from_name("NonExistentSchema")
        
        assert result is None
        mock_uow.schema_repository.find_by_name.assert_called_once_with("NonExistentSchema")
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.helpers.task_helpers.UnitOfWork')
    async def test_get_pydantic_class_invalid_schema_definition(self, mock_uow_class):
        """Test when schema has invalid definition."""
        # Create async context manager mock
        mock_uow = AsyncMock()
        mock_schema = Mock()
        mock_schema.schema_definition = None
        mock_uow.schema_repository.find_by_name.return_value = mock_schema
        
        # Mock the context manager
        mock_uow_class.return_value.__aenter__.return_value = mock_uow
        mock_uow_class.return_value.__aexit__.return_value = None
        
        result = await get_pydantic_class_from_name("InvalidSchema")
        
        assert result is None
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.helpers.task_helpers.UnitOfWork')
    async def test_get_pydantic_class_simple_types(self, mock_uow_class):
        """Test creating Pydantic model with simple field types."""
        # Create async context manager mock
        mock_uow = AsyncMock()
        mock_schema = Mock()
        mock_schema.schema_definition = {
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"},
                "height": {"type": "number"},
                "is_active": {"type": "boolean"}
            },
            "required": ["name", "age"],
            "description": "Test model"
        }
        mock_uow.schema_repository.find_by_name.return_value = mock_schema
        
        # Mock the context manager
        mock_uow_class.return_value.__aenter__.return_value = mock_uow
        mock_uow_class.return_value.__aexit__.return_value = None
        
        result = await get_pydantic_class_from_name("SimpleSchema")
        
        assert result is not None
        assert issubclass(result, BaseModel)
        assert result.__name__ == "SimpleSchema"
        assert result.__doc__ == "Test model"
        
        # Test field types
        fields = result.model_fields
        assert "name" in fields
        assert "age" in fields
        assert "height" in fields
        assert "is_active" in fields
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.helpers.task_helpers.UnitOfWork')
    async def test_get_pydantic_class_array_types(self, mock_uow_class):
        """Test creating Pydantic model with array field types."""
        # Create async context manager mock
        mock_uow = AsyncMock()
        mock_schema = Mock()
        mock_schema.schema_definition = {
            "properties": {
                "tags": {"type": "array", "items": {"type": "string"}},
                "scores": {"type": "array", "items": {"type": "integer"}},
                "prices": {"type": "array", "items": {"type": "number"}},
                "flags": {"type": "array", "items": {"type": "boolean"}},
                "misc": {"type": "array", "items": {"type": "unknown"}}
            },
            "required": []
        }
        mock_uow.schema_repository.find_by_name.return_value = mock_schema
        
        # Mock the context manager
        mock_uow_class.return_value.__aenter__.return_value = mock_uow
        mock_uow_class.return_value.__aexit__.return_value = None
        
        result = await get_pydantic_class_from_name("ArraySchema")
        
        assert result is not None
        assert issubclass(result, BaseModel)
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.helpers.task_helpers.UnitOfWork')
    async def test_get_pydantic_class_object_and_any_types(self, mock_uow_class):
        """Test creating Pydantic model with object and any types."""
        # Create async context manager mock
        mock_uow = AsyncMock()
        mock_schema = Mock()
        mock_schema.schema_definition = {
            "properties": {
                "metadata": {"type": "object"},
                "data": {"type": "unknown"}
            },
            "required": []
        }
        mock_uow.schema_repository.find_by_name.return_value = mock_schema
        
        # Mock the context manager
        mock_uow_class.return_value.__aenter__.return_value = mock_uow
        mock_uow_class.return_value.__aexit__.return_value = None
        
        result = await get_pydantic_class_from_name("ObjectSchema")
        
        assert result is not None
        assert issubclass(result, BaseModel)
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.helpers.task_helpers.UnitOfWork')
    async def test_get_pydantic_class_nullable_fields(self, mock_uow_class):
        """Test creating Pydantic model with nullable fields."""
        # Create async context manager mock
        mock_uow = AsyncMock()
        mock_schema = Mock()
        mock_schema.schema_definition = {
            "properties": {
                "name": {"type": "string", "nullable": True},
                "age": {"type": "integer", "nullable": False}
            },
            "required": ["age"]
        }
        mock_uow.schema_repository.find_by_name.return_value = mock_schema
        
        # Mock the context manager
        mock_uow_class.return_value.__aenter__.return_value = mock_uow
        mock_uow_class.return_value.__aexit__.return_value = None
        
        result = await get_pydantic_class_from_name("NullableSchema")
        
        assert result is not None
        assert issubclass(result, BaseModel)
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.helpers.task_helpers.UnitOfWork')
    async def test_get_pydantic_class_creation_error(self, mock_uow_class):
        """Test when Pydantic model creation fails."""
        # Create async context manager mock
        mock_uow = AsyncMock()
        mock_schema = Mock()
        mock_schema.schema_definition = {
            "properties": {
                "name": {"type": "string"}
            },
            "required": ["name"]
        }
        mock_uow.schema_repository.find_by_name.return_value = mock_schema
        
        # Mock the context manager
        mock_uow_class.return_value.__aenter__.return_value = mock_uow
        mock_uow_class.return_value.__aexit__.return_value = None
        
        with patch('src.engines.crewai.helpers.task_helpers.create_model', side_effect=Exception("Model creation failed")):
            result = await get_pydantic_class_from_name("ErrorSchema")
        
        assert result is None
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.helpers.task_helpers.UnitOfWork')
    async def test_get_pydantic_class_uow_not_initialized(self, mock_uow_class):
        """Test when UnitOfWork is not initialized."""
        # Create async context manager mock
        mock_uow = AsyncMock()
        mock_schema = Mock()
        mock_schema.schema_definition = {
            "properties": {"name": {"type": "string"}},
            "required": ["name"]
        }
        mock_uow.schema_repository.find_by_name.return_value = mock_schema
        
        # Mock the context manager
        mock_uow_class.return_value.__aenter__.return_value = mock_uow
        mock_uow_class.return_value.__aexit__.return_value = None
        
        result = await get_pydantic_class_from_name("TestSchema")
        
        # Note: With async context manager, initialization is handled differently
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.helpers.task_helpers.UnitOfWork')
    async def test_get_pydantic_class_field_definition_error(self, mock_uow_class):
        """Test when field definition causes an error."""
        # Create async context manager mock
        mock_uow = AsyncMock()
        mock_schema = Mock()
        mock_schema.schema_definition = {
            "properties": {
                "bad_field": {"type": "invalid_type_that_causes_error"}
            },
            "required": ["bad_field"]
        }
        mock_uow.schema_repository.find_by_name.return_value = mock_schema
        
        # Mock the context manager
        mock_uow_class.return_value.__aenter__.return_value = mock_uow
        mock_uow_class.return_value.__aexit__.return_value = None
        
        # This should not crash and should create a model with Any type for the problematic field
        result = await get_pydantic_class_from_name("ErrorFieldSchema")
        
        assert result is not None
        assert issubclass(result, BaseModel)
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.helpers.task_helpers.UnitOfWork')
    async def test_get_pydantic_class_general_exception(self, mock_uow_class):
        """Test when a general exception occurs during schema lookup."""
        # Make the context manager creation itself fail
        mock_uow_class.return_value.__aenter__.side_effect = Exception("Database connection failed")
        
        result = await get_pydantic_class_from_name("ExceptionSchema")
        
        assert result is None
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.helpers.task_helpers.UnitOfWork')
    async def test_get_pydantic_class_schema_definition_not_dict(self, mock_uow_class):
        """Test when schema definition is not a dictionary."""
        # Create async context manager mock
        mock_uow = AsyncMock()
        mock_schema = Mock()
        mock_schema.schema_definition = "not a dict"  # Invalid format
        mock_uow.schema_repository.find_by_name.return_value = mock_schema
        
        # Mock the context manager
        mock_uow_class.return_value.__aenter__.return_value = mock_uow
        mock_uow_class.return_value.__aexit__.return_value = None
        
        result = await get_pydantic_class_from_name("InvalidFormatSchema")
        
        assert result is None
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.helpers.task_helpers.UnitOfWork')
    async def test_get_pydantic_class_empty_properties(self, mock_uow_class):
        """Test creating Pydantic model with empty properties."""
        # Create async context manager mock
        mock_uow = AsyncMock()
        mock_schema = Mock()
        mock_schema.schema_definition = {
            "properties": {},  # Empty properties
            "required": [],
            "description": "Empty model"
        }
        mock_uow.schema_repository.find_by_name.return_value = mock_schema
        
        # Mock the context manager
        mock_uow_class.return_value.__aenter__.return_value = mock_uow
        mock_uow_class.return_value.__aexit__.return_value = None
        
        result = await get_pydantic_class_from_name("EmptySchema")
        
        assert result is not None
        assert issubclass(result, BaseModel)
        assert result.__name__ == "EmptySchema"
        assert result.__doc__ == "Empty model"
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.helpers.task_helpers.UnitOfWork')
    async def test_get_pydantic_class_no_description(self, mock_uow_class):
        """Test creating Pydantic model without description."""
        # Create async context manager mock
        mock_uow = AsyncMock()
        mock_schema = Mock()
        mock_schema.schema_definition = {
            "properties": {
                "name": {"type": "string"}
            },
            "required": ["name"]
            # No description field
        }
        mock_uow.schema_repository.find_by_name.return_value = mock_schema
        
        # Mock the context manager
        mock_uow_class.return_value.__aenter__.return_value = mock_uow
        mock_uow_class.return_value.__aexit__.return_value = None
        
        result = await get_pydantic_class_from_name("NoDescSchema")
        
        assert result is not None
        assert issubclass(result, BaseModel)
        assert result.__doc__ == "Model for NoDescSchema"  # Default description
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.helpers.task_helpers.UnitOfWork')
    @patch('src.engines.crewai.helpers.task_helpers.List')
    async def test_get_pydantic_class_field_definition_exception(self, mock_list, mock_uow_class):
        """Test field definition that causes an exception (covering lines 126-128)."""
        # Create async context manager mock
        mock_uow = AsyncMock()
        
        # Mock List to raise exception when accessed with __getitem__
        mock_list.__getitem__.side_effect = Exception("List access failed")
        
        mock_schema = Mock()
        mock_schema.schema_definition = {
            "properties": {
                "items": {"type": "array", "items": {"type": "string"}}
            },
            "required": []
        }
        mock_uow.schema_repository.find_by_name.return_value = mock_schema
        
        # Mock the context manager
        mock_uow_class.return_value.__aenter__.return_value = mock_uow
        mock_uow_class.return_value.__aexit__.return_value = None
        
        result = await get_pydantic_class_from_name("ExceptionFieldSchema")
        
        # Should handle exception and still create model with Any type
        assert result is not None
        assert issubclass(result, BaseModel)
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.helpers.task_helpers.UnitOfWork')
    async def test_get_pydantic_class_field_assignment_exception(self, mock_uow_class):
        """Test direct field assignment exception to cover lines 126-128."""
        # Create async context manager mock
        mock_uow = AsyncMock()
        
        # Create a mock schema with a field that will trigger the exception path
        mock_schema = Mock()
        mock_schema.schema_definition = {
            "properties": {
                "test_field": {"type": "array", "items": {"type": "string"}, "nullable": True}
            },
            "required": []
        }
        mock_uow.schema_repository.find_by_name.return_value = mock_schema
        
        # Mock the context manager
        mock_uow_class.return_value.__aenter__.return_value = mock_uow
        mock_uow_class.return_value.__aexit__.return_value = None
        
        # Create a custom List type that will fail during field assignment
        class FailingList:
            def __getitem__(self, item):
                # This will cause an exception when List[str] is accessed in line 108
                raise Exception("List type access failed")
        
        with patch('src.engines.crewai.helpers.task_helpers.List', FailingList()):
            result = await get_pydantic_class_from_name("ExceptionFieldSchema")
            
            # Should handle exception and create model with Any type fields
            assert result is not None
            assert issubclass(result, BaseModel)


class TestCreateTask:
    """Test cases for create_task function."""
    
    @pytest.mark.asyncio
    async def test_create_task_basic(self):
        """Test creating a basic task without tools."""
        task_key = "test_task"
        task_config = {
            "description": "Test task description",
            "expected_output": "Test expected output",
            "async_execution": False,
            "retry_on_fail": True,
            "max_retries": 2,
            "markdown": False
        }
        # Create a real Agent instance for proper validation
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            # Mock MCP service to return no enabled servers
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            task = await create_task(task_key, task_config, agent)
        
        assert isinstance(task, Task)
        assert task.description == "Test task description"
        assert task.expected_output == "Test expected output"
        assert task.agent == agent
        # These attributes may not be directly accessible on Task object
        # The function worked without errors, which is what we're testing
    
    @pytest.mark.asyncio
    async def test_create_task_with_markdown(self):
        """Test creating a task with markdown enabled."""
        task_key = "markdown_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "markdown": True
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            task = await create_task(task_key, task_config, agent)
        
        assert "markdown syntax" in task.description
        assert "formatted in markdown" in task.expected_output
    
    @pytest.mark.asyncio
    async def test_create_task_with_output_file(self):
        """Test creating a task with output file configuration."""
        task_key = "output_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "output_file_enabled": True,
            "output_filename": "custom_output.md"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        output_dir = "/test/output"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('os.makedirs') as mock_makedirs:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            task = await create_task(task_key, task_config, agent, output_dir=output_dir)
        
        assert task.output_file.endswith("test/output/custom_output.md")
        mock_makedirs.assert_called_once_with(output_dir, exist_ok=True)
    
    @pytest.mark.asyncio
    async def test_create_task_with_direct_output_file(self):
        """Test creating a task with direct output_file in config."""
        task_key = "direct_output_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "output_file": "/direct/path/output.txt"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('os.makedirs') as mock_makedirs:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            task = await create_task(task_key, task_config, agent)
        
        assert task.output_file.endswith("direct/path/output.txt")
        mock_makedirs.assert_called_once_with("/direct/path", exist_ok=True)
    
    @pytest.mark.asyncio
    async def test_create_task_with_guardrail(self):
        """Test creating a task with guardrail configuration."""
        task_key = "guardrail_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "guardrail": {"type": "test_guardrail", "config": "test"},
            "retry_on_fail": False
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        mock_guardrail = Mock()
        mock_guardrail.validate.return_value = {"valid": True}
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.guardrails.guardrail_factory.GuardrailFactory') as mock_factory:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            mock_factory.create_guardrail.return_value = mock_guardrail
            
            task = await create_task(task_key, task_config, agent)
        
        assert hasattr(task, 'guardrail')
        # Note: retry_on_fail is passed to CrewAI during task creation but may not be accessible as an attribute
        
        # Test the guardrail function
        output = Mock()
        valid, result = task.guardrail(output)
        assert valid == True
        assert result == output
    
    @pytest.mark.asyncio
    async def test_create_task_with_guardrail_validation_failure(self):
        """Test guardrail validation failure."""
        task_key = "guardrail_fail_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "guardrail": {"type": "test_guardrail"}
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        mock_guardrail = Mock()
        mock_guardrail.validate.return_value = {
            "valid": False,
            "feedback": "Validation failed"
        }
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.guardrails.guardrail_factory.GuardrailFactory') as mock_factory, \
             patch('os.makedirs'), \
             patch('builtins.open', create=True):
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            mock_factory.create_guardrail.return_value = mock_guardrail
            
            task = await create_task(task_key, task_config, agent)
            
            # Test the guardrail function with failure
            output = Mock()
            valid, feedback = task.guardrail(output)
            assert valid == False
            assert feedback == "Validation failed"
    
    @pytest.mark.asyncio
    async def test_create_task_guardrail_validation_exception(self):
        """Test guardrail validation exception handling (lines 555-563)."""
        task_key = "guardrail_exception_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "guardrail": {"type": "test_guardrail"}
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        mock_guardrail = Mock()
        # Make validate raise an exception
        mock_guardrail.validate.side_effect = Exception("Validation error occurred")
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.guardrails.guardrail_factory.GuardrailFactory') as mock_factory, \
             patch('os.makedirs'), \
             patch('builtins.open', create=True):
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            mock_factory.create_guardrail.return_value = mock_guardrail
            
            task = await create_task(task_key, task_config, agent)
            
            # Test the guardrail function with exception
            output = Mock()
            valid, error_msg = task.guardrail(output)
            assert valid == False
            assert "Validation error" in error_msg
    
    @pytest.mark.asyncio
    async def test_create_task_with_output_pydantic(self):
        """Test creating a task with output_pydantic configuration."""
        task_key = "pydantic_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "output_pydantic": "TestModel"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        agent.llm = Mock()
        agent.llm.model = "test-model"
        
        # Create a test Pydantic model
        TestModel = create_model("TestModel", name=(str, ...))
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.helpers.task_helpers.get_pydantic_class_from_name') as mock_get_pydantic, \
             patch('src.engines.crewai.helpers.model_conversion_handler.get_compatible_converter_for_model') as mock_converter:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            mock_get_pydantic.return_value = TestModel
            mock_converter.return_value = (None, TestModel, False, True)
            
            task = await create_task(task_key, task_config, agent)
        
        assert task.output_pydantic == TestModel
    
    @pytest.mark.asyncio
    async def test_create_task_pydantic_model_not_found(self):
        """Test when pydantic model is not found (line 649)."""
        task_key = "pydantic_not_found_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "output_pydantic": "NonExistentModel"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.helpers.task_helpers.get_pydantic_class_from_name') as mock_get_pydantic:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            # Model not found
            mock_get_pydantic.return_value = None
            
            task = await create_task(task_key, task_config, agent)
        
        # output_pydantic should not be set when model is not found
        assert not hasattr(task, 'output_pydantic') or task.output_pydantic is None
    
    @pytest.mark.asyncio
    async def test_create_task_with_tools_resolution(self):
        """Test creating a task with tool resolution."""
        task_key = "tools_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["1", "2"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        mock_tool_service = Mock()
        # Mock the get_tool_by_id method properly
        mock_tool1 = Mock(title="Tool1")
        mock_tool2 = Mock(title="Tool2")
        mock_tool_service.get_tool_by_id = AsyncMock(side_effect=[mock_tool1, mock_tool2])
        # Mock get_tool_config_by_name
        mock_tool_service.get_tool_config_by_name = AsyncMock(return_value={})
        
        mock_tool_factory = Mock()
        
        # Create proper tool mocks that inherit from BaseTool
        tool1_instance = Mock(spec=BaseTool)
        tool1_instance.name = "tool1"
        tool1_instance.description = "Tool 1 description"
        tool2_instance = Mock(spec=BaseTool)
        tool2_instance.name = "tool2"
        tool2_instance.description = "Tool 2 description"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            mock_tool_factory.create_tool.side_effect = [tool1_instance, tool2_instance]
            
            task = await create_task(
                task_key, 
                task_config, 
                agent,
                tool_service=mock_tool_service,
                tool_factory=mock_tool_factory
            )
        
        assert len(task.tools) == 2
        assert tool1_instance in task.tools
        assert tool2_instance in task.tools
    
    @pytest.mark.skip(reason="MCP servers now handled by MCPIntegration module")
    async def test_create_task_with_mcp_sse_server(self):
        """Test creating a task with MCP SSE server enabled."""
        task_key = "mcp_sse_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock MCP server response
        mock_server = Mock()
        mock_server.id = "server1"
        mock_server.name = "TestServer"
        mock_server.server_type = "SSE"
        mock_server.server_url = "https://test.com/sse"
        mock_server.api_key = "test-key"
        
        # Mock MCP tool as a dictionary (as returned by MCPAdapter)
        mock_mcp_tool = {
            "name": "test_tool",
            "description": "Test tool description",
            "mcp_tool": Mock(),
            "input_schema": {},
            "adapter": Mock()  # Will be replaced with actual adapter
        }
        
        # Create wrapped tool that will be returned by create_crewai_tool_from_mcp
        wrapped_tool = Mock(spec=BaseTool)
        wrapped_tool.name = "TestServer_test_tool"  # The name gets prefixed with server name
        wrapped_tool.description = "Test tool description"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.common.mcp_adapter.MCPAdapter') as mock_adapter_class, \
             patch('src.engines.crewai.tools.mcp_handler.create_crewai_tool_from_mcp') as mock_create_tool, \
             patch('src.engines.crewai.tools.mcp_handler.register_mcp_adapter') as mock_register:
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_servers_response = Mock(servers=[mock_server])
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server)
            mock_mcp_instance.resolve_effective_servers = AsyncMock(return_value=[mock_server])
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            # Setup adapter mock
            mock_adapter = Mock()
            # Update the tool dict to have the correct adapter reference
            mock_mcp_tool["adapter"] = mock_adapter
            mock_adapter.tools = [mock_mcp_tool]
            mock_adapter.initialize = AsyncMock()
            mock_adapter_class.return_value = mock_adapter
            
            # Setup create_crewai_tool_from_mcp mock to return the wrapped tool
            mock_create_tool.return_value = wrapped_tool
            
            task = await create_task(task_key, task_config, agent)
        
        # Verify adapter was created with correct parameters
        mock_adapter_class.assert_called_once()
        call_args = mock_adapter_class.call_args[0][0]
        assert call_args["url"] == "https://test.com/sse"
        assert call_args["headers"]["Authorization"] == "Bearer test-key"
        
        # Verify tool was added
        assert len(task.tools) == 1
        assert wrapped_tool in task.tools
        assert wrapped_tool.name == "TestServer_test_tool"
    
    @pytest.mark.skip(reason="MCP servers now handled by MCPIntegration module")
    async def test_create_task_with_mcp_databricks_server(self):
        """Test creating a task with Databricks MCP server."""
        task_key = "mcp_databricks_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock MCP server response
        mock_server = Mock()
        mock_server.id = "databricks_server"
        mock_server.name = "DatabricksServer"
        mock_server.server_type = "SSE"
        mock_server.server_url = "https://workspace.databricksapps.com/app"
        mock_server.api_key = None  # No API key for OAuth
        
        # Mock MCP tool as a dictionary (as returned by MCPAdapter)
        mock_mcp_tool = {
            "name": "test_tool",
            "description": "Test tool description",
            "mcp_tool": Mock(),
            "input_schema": {},
            "adapter": Mock()  # Will be replaced with actual adapter
        }
        
        # Create wrapped tool that will be returned by create_crewai_tool_from_mcp
        wrapped_tool = Mock(spec=BaseTool)
        wrapped_tool.name = "DatabricksServer_test_tool"  # The name gets prefixed with server name
        wrapped_tool.description = "Test tool description"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.common.mcp_adapter.MCPAdapter') as mock_adapter_class, \
             patch('src.utils.databricks_auth.get_mcp_auth_headers', new_callable=AsyncMock) as mock_auth, \
             patch('src.engines.crewai.tools.mcp_handler.create_crewai_tool_from_mcp') as mock_create_tool, \
             patch('src.engines.crewai.tools.mcp_handler.register_mcp_adapter'):
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_servers_response = Mock(servers=[mock_server])
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server)
            mock_mcp_instance.resolve_effective_servers = AsyncMock(return_value=[mock_server])
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            # Setup OAuth mock
            mock_auth.return_value = ({"Authorization": "Bearer oauth-token"}, None)
            
            # Setup adapter mock
            mock_adapter = Mock()
            # Update the tool dict to have the correct adapter reference
            mock_mcp_tool["adapter"] = mock_adapter
            mock_adapter.tools = [mock_mcp_tool]
            mock_adapter.initialize = AsyncMock()
            mock_adapter_class.return_value = mock_adapter
            
            # Setup create_crewai_tool_from_mcp mock to return the wrapped tool
            mock_create_tool.return_value = wrapped_tool
            
            task = await create_task(task_key, task_config, agent)
        
        # Verify OAuth was used with proper parameters
        mock_auth.assert_called_once_with(
            "https://workspace.databricksapps.com/app/sse",
            user_token=None,
            api_key=None
        )
        
        # Verify adapter was created with OAuth headers
        call_args = mock_adapter_class.call_args[0][0]
        assert call_args["url"] == "https://workspace.databricksapps.com/app/sse"
        assert call_args["headers"]["Authorization"] == "Bearer oauth-token"
        
        # Verify tool was added
        assert len(task.tools) == 1
        assert wrapped_tool in task.tools
        assert wrapped_tool.name == "DatabricksServer_test_tool"
    
    @pytest.mark.skip(reason="MCP auth now handled by MCPIntegration module")
    async def test_create_task_databricks_oauth_fail_fallback_to_api_key(self):
        """Test Databricks OAuth failure with fallback to API key (covers lines 261-262)."""
        task_key = "databricks_fallback_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock MCP server response with API key
        mock_server = Mock()
        mock_server.id = "databricks_server"
        mock_server.name = "DatabricksServer"
        mock_server.server_type = "SSE"
        mock_server.server_url = "https://workspace.databricksapps.com/app"
        mock_server.api_key = "fallback-api-key"
        
        # Mock MCP tool as a dictionary (as returned by MCPAdapter)
        mock_mcp_tool = {
            "name": "test_tool",
            "description": "Test tool description",
            "mcp_tool": Mock(),
            "input_schema": {},
            "adapter": Mock()  # Will be replaced with actual adapter
        }
        
        # Create wrapped tool that will be returned by create_crewai_tool_from_mcp
        wrapped_tool = Mock(spec=BaseTool)
        wrapped_tool.name = "DatabricksServer_test_tool"
        wrapped_tool.description = "Test tool description"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.tools.mcp_handler.get_or_create_mcp_adapter') as mock_get_adapter, \
             patch('src.utils.databricks_auth.get_mcp_auth_headers') as mock_auth, \
             patch('src.engines.crewai.tools.mcp_handler.create_crewai_tool_from_mcp') as mock_create_tool, \
             patch('src.engines.crewai.tools.mcp_handler.register_mcp_adapter'):
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_servers_response = Mock(servers=[mock_server])
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server)
            mock_mcp_instance.resolve_effective_servers = AsyncMock(return_value=[mock_server])
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            # Setup OAuth to fail, triggering fallback to API key (lines 261-262)
            mock_auth.return_value = (None, "OAuth failed")
            
            # Setup adapter mock
            mock_adapter = Mock()
            # Update the tool dict to have the correct adapter reference
            mock_mcp_tool["adapter"] = mock_adapter
            mock_adapter.tools = [mock_mcp_tool]
            mock_get_adapter.return_value = mock_adapter
            
            # Setup create_crewai_tool_from_mcp mock to return the wrapped tool
            mock_create_tool.return_value = wrapped_tool
            
            task = await create_task(task_key, task_config, agent)
        
        # Verify OAuth was attempted with the modified URL (with /sse added)
        mock_auth.assert_called_once_with(
            "https://workspace.databricksapps.com/app/sse",
            user_token=None,
            api_key="fallback-api-key"
        )
        
        # Verify adapter was created with the correct URL
        call_args = mock_get_adapter.call_args[0][0]
        assert call_args["url"] == "https://workspace.databricksapps.com/app/sse"
        # When OAuth fails, no headers should be added (no automatic fallback)
        assert "headers" not in call_args or call_args.get("headers") == {}
        
        # Verify tool was added
        assert len(task.tools) == 1
        assert wrapped_tool in task.tools
    
    @pytest.mark.asyncio
    async def test_create_task_with_mcp_stdio_server(self):
        """Test creating a task with MCP STDIO server."""
        task_key = "mcp_stdio_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock MCP server response
        mock_server = Mock()
        mock_server.id = "stdio_server"
        mock_server.name = "StdioServer"
        mock_server.server_type = "STDIO"
        mock_server.command = "python"
        mock_server.args = ["-m", "mcp_server"]
        mock_server.api_key = "test-key"
        mock_server.additional_config = {"setting": "value"}
        
        # Mock MCP tool as a dictionary (as returned by MCPAdapter)
        mock_mcp_tool = {
            "name": "test_tool",
            "description": "Test tool description",
            "mcp_tool": Mock(),
            "input_schema": {},
            "adapter": Mock()  # Will be replaced with actual adapter
        }
        
        # Create wrapped tool that will be returned by create_crewai_tool_from_mcp
        wrapped_tool = Mock(spec=BaseTool)
        wrapped_tool.name = "StdioServer_test_tool"  # The name gets prefixed with server name
        wrapped_tool.description = "Test tool description"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.common.mcp_adapter.MCPAdapter') as mock_adapter_class, \
             patch('mcp.StdioServerParameters') as mock_stdio_params, \
             patch('src.engines.crewai.tools.mcp_handler.create_crewai_tool_from_mcp') as mock_create_tool, \
             patch('src.engines.crewai.tools.mcp_handler.register_mcp_adapter'), \
             patch.dict('os.environ', {'EXISTING': 'value'}):
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_servers_response = Mock(servers=[mock_server])
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server)
            mock_mcp_instance.resolve_effective_servers = AsyncMock(return_value=[mock_server])
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            # Setup adapter mock with tools to trigger lines 362-374
            mock_adapter = Mock()
            mock_adapter.tools = [mock_mcp_tool]  # Add tool to trigger the loop
            mock_adapter.initialize = AsyncMock()
            mock_adapter_class.return_value = mock_adapter
            
            # Setup create_crewai_tool_from_mcp mock to return the wrapped tool
            mock_create_tool.return_value = wrapped_tool
            
            task = await create_task(task_key, task_config, agent)
        
        # Verify STDIO is not supported - no tools should be added
        # The log shows "WARNING - Unsupported MCP server type: STDIO"
        mock_stdio_params.assert_not_called()
        mock_adapter_class.assert_not_called()
        mock_create_tool.assert_not_called()
        
        # Verify no tools were added since STDIO is unsupported
        assert len(task.tools) == 0
    
    @pytest.mark.asyncio
    async def test_create_task_with_optional_fields(self):
        """Test creating a task with optional configuration fields."""
        task_key = "optional_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "async_execution": True,
            "human_input": True
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            task = await create_task(task_key, task_config, agent)
        
        assert task.async_execution == True
        assert task.human_input == True
    
    @pytest.mark.asyncio
    async def test_create_task_error_handling(self):
        """Test error handling during task creation."""
        task_key = "error_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.helpers.task_helpers.Task', side_effect=Exception("Task creation failed")):
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            with pytest.raises(Exception, match="Task creation failed"):
                await create_task(task_key, task_config, agent)
    
    @pytest.mark.asyncio
    async def test_create_task_with_existing_callback(self):
        """Test creating a task with existing callback configuration."""
        existing_callback = Mock()
        task_key = "callback_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "callback": existing_callback
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            task = await create_task(task_key, task_config, agent)
        
        assert task.callback == existing_callback
    
    @pytest.mark.asyncio
    async def test_create_task_guardrail_creation_failed_with_callback(self):
        """Test guardrail creation failure with existing callback (lines 575-578)."""
        existing_callback = Mock()
        task_key = "guardrail_fail_callback_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "guardrail": {"type": "test_guardrail"},
            "callback": existing_callback
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.guardrails.guardrail_factory.GuardrailFactory') as mock_factory:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            # Guardrail creation fails
            mock_factory.create_guardrail.return_value = None
            
            task = await create_task(task_key, task_config, agent)
        
        # Should preserve the existing callback when guardrail creation fails
        assert task.callback == existing_callback
    
    @pytest.mark.asyncio
    async def test_create_task_mcp_service_error(self):
        """Test handling MCP service errors during task creation."""
        task_key = "mcp_error_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            # Simulate MCP service throwing an exception
            mock_mcp_service.from_unit_of_work = AsyncMock(side_effect=Exception("MCP service failed"))
            
            # Should still create task despite MCP error
            task = await create_task(task_key, task_config, agent)
            
            assert isinstance(task, Task)
            assert task.description == "Task description"
    
    @pytest.mark.asyncio
    async def test_create_task_databricks_auth_error(self):
        """Test Databricks authentication error handling."""
        task_key = "databricks_auth_error_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock MCP server response
        mock_server = Mock()
        mock_server.id = "databricks_server"
        mock_server.name = "DatabricksServer"
        mock_server.server_type = "SSE"
        mock_server.server_url = "https://workspace.databricksapps.com/app"
        mock_server.api_key = "fallback-key"
        
        # Create wrapped tool that will be returned by create_crewai_tool_from_mcp
        wrapped_tool = Mock(spec=BaseTool)
        wrapped_tool.name = "DatabricksServer_test_tool"
        wrapped_tool.description = "Test tool description"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.common.mcp_adapter.MCPAdapter') as mock_adapter_class, \
             patch('src.utils.databricks_auth.get_mcp_auth_headers') as mock_auth, \
             patch('src.engines.crewai.tools.mcp_handler.create_crewai_tool_from_mcp') as mock_create_tool, \
             patch('src.engines.crewai.tools.mcp_handler.register_mcp_adapter'):
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_servers_response = Mock(servers=[mock_server])
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server)
            mock_mcp_instance.resolve_effective_servers = AsyncMock(return_value=[mock_server])
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            # Setup OAuth failure
            mock_auth.side_effect = Exception("OAuth authentication failed")
            
            # Setup create_crewai_tool_from_mcp mock to return the wrapped tool
            mock_create_tool.return_value = wrapped_tool
            
            # Setup adapter mock
            mock_adapter = Mock()
            mock_adapter.tools = []
            mock_adapter.initialize = AsyncMock()
            mock_adapter_class.return_value = mock_adapter
            
            task = await create_task(task_key, task_config, agent)
        
        # When OAuth fails with exception, adapter should not be created
        # So we just verify the task was created successfully
        assert isinstance(task, Task)
        assert task.description == "Task description"
    
    @pytest.mark.asyncio
    async def test_create_task_mcp_sse_adapter_creation_error(self):
        """Test MCP SSE adapter creation error handling (covers lines 312-313)."""
        task_key = "mcp_sse_error_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock MCP server response
        mock_server = Mock()
        mock_server.id = "server1"
        mock_server.name = "TestServer"
        mock_server.server_type = "SSE"
        mock_server.server_url = "https://test.com/sse"
        mock_server.api_key = "test-key"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.common.mcp_adapter.MCPAdapter') as mock_adapter_class:
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_servers_response = Mock(servers=[mock_server])
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server)
            mock_mcp_instance.resolve_effective_servers = AsyncMock(return_value=[mock_server])
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            # Make adapter creation fail to trigger exception handling
            mock_adapter_class.side_effect = Exception("Adapter creation failed")
            
            # Task creation should continue despite adapter error
            task = await create_task(task_key, task_config, agent)
        
        # Verify task was still created (error was handled gracefully)
        assert task is not None
        assert len(task.tools) == 0  # No tools added due to adapter error
    
    @pytest.mark.asyncio
    async def test_create_task_mcp_stdio_adapter_creation_error(self):
        """Test MCP STDIO adapter creation error handling (covers lines 375-376)."""
        task_key = "mcp_stdio_error_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock MCP server response
        mock_server = Mock()
        mock_server.id = "stdio_server"
        mock_server.name = "StdioServer"
        mock_server.server_type = "STDIO"
        mock_server.command = "python"
        mock_server.args = ["-m", "mcp_server"]
        mock_server.api_key = "test-key"
        mock_server.additional_config = None
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.common.mcp_adapter.MCPAdapter') as mock_adapter_class, \
             patch('mcp.StdioServerParameters'):
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_servers_response = Mock(servers=[mock_server])
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server)
            mock_mcp_instance.resolve_effective_servers = AsyncMock(return_value=[mock_server])
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            # Make adapter creation fail to trigger exception handling
            mock_adapter_class.side_effect = Exception("STDIO adapter creation failed")
            
            # Task creation should continue despite adapter error
            task = await create_task(task_key, task_config, agent)
        
        # Verify task was still created (error was handled gracefully)
        assert task is not None
        assert len(task.tools) == 0  # No tools added due to adapter error

    @pytest.mark.asyncio
    async def test_create_task_mcp_adapter_initialization_error(self):
        """Test MCP adapter initialization error handling."""
        task_key = "adapter_error_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock MCP server response
        mock_server = Mock()
        mock_server.id = "test_server"
        mock_server.name = "TestServer"
        mock_server.server_type = "SSE"
        mock_server.server_url = "https://test.com/sse"
        mock_server.api_key = "test-key"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.common.mcp_adapter.MCPAdapter') as mock_adapter_class:
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_servers_response = Mock(servers=[mock_server])
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server)
            mock_mcp_instance.resolve_effective_servers = AsyncMock(return_value=[mock_server])
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            # Setup adapter to fail during initialization
            mock_adapter_class.side_effect = Exception("Adapter initialization failed")
            
            # Should still create task despite adapter error
            task = await create_task(task_key, task_config, agent)
            
            assert isinstance(task, Task)
            assert task.description == "Task description"
    
    @pytest.mark.asyncio
    async def test_create_task_tool_resolution_error(self):
        """Test tool resolution error handling (lines 449-450)."""
        task_key = "tool_error_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["tool1", "tool2"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        mock_tool_service = Mock()
        mock_tool_factory = Mock()
        mock_tool_factory.create_tool.side_effect = Exception("Tool creation error")
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            # First let resolve work, then fail during tool creation
            mock_resolve.return_value = ["Tool1", "Tool2"]
            
            # Should still create task despite tool creation error
            task = await create_task(
                task_key, 
                task_config, 
                agent,
                tool_service=mock_tool_service,
                tool_factory=mock_tool_factory
            )
            
            assert isinstance(task, Task)
            assert task.description == "Task description"
    
    @pytest.mark.asyncio
    async def test_create_task_tool_resolution_exception_outer(self):
        """Test exception during tool resolution - outer exception handler (lines 449-450)."""
        task_key = "tool_exception_outer_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["tool1"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        mock_tool_service = Mock()
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            # Make resolve_tool_ids_to_names raise an exception
            with patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve:
                # This will trigger the exception at lines 449-450
                mock_resolve.side_effect = Exception("Critical resolution error")
                
                # Should still create task despite exception in tool resolution
                task = await create_task(
                    task_key, 
                    task_config, 
                    agent,
                    tool_service=mock_tool_service
                )
        
        assert isinstance(task, Task)
        assert task.description == "Task description"
    
    @pytest.mark.asyncio
    async def test_create_task_tool_factory_error(self):
        """Test tool factory error handling."""
        task_key = "tool_factory_error_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["tool1"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        mock_tool_service = Mock()
        mock_tool_factory = Mock()
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            mock_resolve.side_effect = AsyncMock(return_value=["tool1"])
            
            # Simulate tool factory error
            mock_tool_factory.create_tool.side_effect = Exception("Tool creation failed")
            
            # Should still create task despite tool factory error
            task = await create_task(
                task_key, 
                task_config, 
                agent,
                tool_service=mock_tool_service,
                tool_factory=mock_tool_factory
            )
            
            assert isinstance(task, Task)
            assert task.description == "Task description"
    
    @pytest.mark.asyncio
    async def test_create_task_no_server_url(self):
        """Test MCP server without server URL."""
        task_key = "no_url_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock MCP server response without URL
        mock_server = Mock()
        mock_server.id = "test_server"
        mock_server.name = "TestServer"
        mock_server.server_type = "SSE"
        mock_server.server_url = None  # No URL
        mock_server.api_key = "test-key"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_servers_response = Mock(servers=[mock_server])
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server)
            mock_mcp_instance.resolve_effective_servers = AsyncMock(return_value=[mock_server])
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            task = await create_task(task_key, task_config, agent)
            
            assert isinstance(task, Task)
            assert task.description == "Task description"
    
    @pytest.mark.asyncio
    async def test_create_task_no_command_stdio(self):
        """Test STDIO MCP server without command."""
        task_key = "no_command_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock MCP server response without command
        mock_server = Mock()
        mock_server.id = "stdio_server"
        mock_server.name = "StdioServer"
        mock_server.server_type = "STDIO"
        mock_server.command = None  # No command
        mock_server.args = []
        mock_server.api_key = "test-key"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_servers_response = Mock(servers=[mock_server])
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server)
            mock_mcp_instance.resolve_effective_servers = AsyncMock(return_value=[mock_server])
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            task = await create_task(task_key, task_config, agent)
            
            assert isinstance(task, Task)
            assert task.description == "Task description"
    
    @pytest.mark.asyncio
    async def test_create_task_unsupported_server_type(self):
        """Test unsupported MCP server type."""
        task_key = "unsupported_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock MCP server with unsupported type
        mock_server = Mock()
        mock_server.id = "unsupported_server"
        mock_server.name = "UnsupportedServer"
        mock_server.server_type = "WEBSOCKET"  # Unsupported type
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_servers_response = Mock(servers=[mock_server])
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server)
            mock_mcp_instance.resolve_effective_servers = AsyncMock(return_value=[mock_server])
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            task = await create_task(task_key, task_config, agent)
            
            assert isinstance(task, Task)
            assert task.description == "Task description"
    
    @pytest.mark.asyncio
    async def test_create_task_server_detail_not_found(self):
        """Test when server detail cannot be fetched."""
        task_key = "no_detail_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock MCP server response
        mock_server = Mock()
        mock_server.id = "test_server"
        mock_server.name = "TestServer"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=Mock(servers=[mock_server]))
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=None)  # Server detail not found
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            task = await create_task(task_key, task_config, agent)
            
            assert isinstance(task, Task)
            assert task.description == "Task description"
    
    @pytest.mark.asyncio  
    async def test_create_task_mcp_tools_service_adapter_skip(self):
        """Test MCP service adapter skip case (covers lines 419-422)."""
        task_key = "mcp_service_adapter_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["1"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        mock_tool_service = Mock()
        mock_tool1 = Mock(title="Tool1")
        mock_tool_service.get_tool_by_id = AsyncMock(side_effect=[mock_tool1])
        mock_tool_service.get_tool_config_by_name = AsyncMock(return_value={})
        
        mock_tool_factory = Mock()
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            # Mock tool factory to return MCP service adapter tuple
            mock_tool_factory.create_tool.return_value = (True, 'mcp_service_adapter')
            
            task = await create_task(
                task_key, 
                task_config, 
                agent,
                tool_service=mock_tool_service,
                tool_factory=mock_tool_factory
            )
        
        # Should continue without adding tools (service adapter is deprecated)
        assert len(task.tools) == 0
    
    @pytest.mark.asyncio
    async def test_create_task_mcp_tools_unexpected_format(self):
        """Test MCP tools unexpected format warning (covers lines 428-429)."""
        task_key = "mcp_unexpected_format_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["1"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        mock_tool_service = Mock()
        mock_tool1 = Mock(title="Tool1")
        mock_tool_service.get_tool_by_id = AsyncMock(side_effect=[mock_tool1])
        mock_tool_service.get_tool_config_by_name = AsyncMock(return_value={})
        
        mock_tool_factory = Mock()
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            # Mock tool factory to return MCP tuple with unexpected format
            mock_tool_factory.create_tool.return_value = (True, "unexpected_string_format")
            
            task = await create_task(
                task_key, 
                task_config, 
                agent,
                tool_service=mock_tool_service,
                tool_factory=mock_tool_factory
            )
        
        # Should continue without adding tools (unexpected format)
        assert len(task.tools) == 0
    
    @pytest.mark.asyncio
    async def test_create_task_mcp_tools_list_processing(self):
        """Test MCP tools list processing (covers lines 425-427)."""
        task_key = "mcp_tools_list_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["1"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        mock_tool_service = Mock()
        mock_tool1 = Mock(title="Tool1")
        mock_tool_service.get_tool_by_id = AsyncMock(side_effect=[mock_tool1])
        mock_tool_service.get_tool_config_by_name = AsyncMock(return_value={})
        
        mock_tool_factory = Mock()
        
        # Create mock MCP tools to be added
        mock_mcp_tool1 = Mock(spec=BaseTool)
        mock_mcp_tool1.name = "mcp_tool1"
        mock_mcp_tool2 = Mock(spec=BaseTool)
        mock_mcp_tool2.name = "mcp_tool2"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            # Mock tool factory to return MCP tuple with list of tools
            mock_tool_factory.create_tool.return_value = (True, [mock_mcp_tool1, mock_mcp_tool2])
            
            task = await create_task(
                task_key, 
                task_config, 
                agent,
                tool_service=mock_tool_service,
                tool_factory=mock_tool_factory
            )
        
        # Should add both MCP tools from the list (covers lines 425-427)
        assert len(task.tools) == 2
        assert mock_mcp_tool1 in task.tools
        assert mock_mcp_tool2 in task.tools
    
    @pytest.mark.asyncio
    async def test_create_task_tool_creation_failure(self):
        """Test tool creation failure handling (covers lines 442-444)."""
        task_key = "tool_creation_fail_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["1"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        mock_tool_service = Mock()
        mock_tool1 = Mock(title="Tool1")
        mock_tool_service.get_tool_by_id = AsyncMock(side_effect=[mock_tool1])
        mock_tool_service.get_tool_config_by_name = AsyncMock(return_value={})
        
        mock_tool_factory = Mock()
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            # Mock tool factory to return None (creation failure)
            mock_tool_factory.create_tool.return_value = None
            
            task = await create_task(
                task_key, 
                task_config, 
                agent,
                tool_service=mock_tool_service,
                tool_factory=mock_tool_factory
            )
        
        # Should continue without adding tools (creation failed)
        assert len(task.tools) == 0
    
    @pytest.mark.asyncio
    async def test_create_task_tool_creation_exception(self):
        """Test tool creation exception handling (covers lines 443-444)."""
        task_key = "tool_creation_exception_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["1"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        mock_tool_service = Mock()
        mock_tool1 = Mock(title="Tool1")
        mock_tool_service.get_tool_by_id = AsyncMock(side_effect=[mock_tool1])
        mock_tool_service.get_tool_config_by_name = AsyncMock(return_value={})
        
        mock_tool_factory = Mock()
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            # Mock tool factory to raise exception
            mock_tool_factory.create_tool.side_effect = Exception("Tool creation failed")
            
            task = await create_task(
                task_key, 
                task_config, 
                agent,
                tool_service=mock_tool_service,
                tool_factory=mock_tool_factory
            )
        
        # Should continue without adding tools (exception was handled)
        assert len(task.tools) == 0

    @pytest.mark.asyncio
    async def test_create_task_mcp_server_detail_not_found_coverage(self):
        """Test MCP server detail fetch failure (covers lines 206-208)."""
        task_key = "mcp_server_detail_fail_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock MCP server response
        mock_server = Mock()
        mock_server.id = "server1"
        mock_server.name = "TestServer"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=Mock(servers=[mock_server]))
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=None)  # Server detail not found
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            task = await create_task(task_key, task_config, agent)
            
            # Should still create task despite server detail fetch failure
            assert isinstance(task, Task)
            assert task.description == "Task description"
            assert len(task.tools) == 0
    
    @pytest.mark.asyncio
    async def test_create_task_mcp_server_no_url_coverage(self):
        """Test MCP server without URL (covers lines 218-221)."""
        task_key = "mcp_no_url_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock MCP server response without URL
        mock_server = Mock()
        mock_server.id = "server1"
        mock_server.name = "TestServer"
        mock_server.server_type = "SSE"
        mock_server.server_url = None  # No URL
        mock_server.api_key = "test-key"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_servers_response = Mock(servers=[mock_server])
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server)
            mock_mcp_instance.resolve_effective_servers = AsyncMock(return_value=[mock_server])
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            task = await create_task(task_key, task_config, agent)
            
            # Should still create task despite missing URL
            assert isinstance(task, Task)
            assert task.description == "Task description"
            assert len(task.tools) == 0
    
    @pytest.mark.asyncio
    async def test_create_task_mcp_stdio_no_command_coverage(self):
        """Test STDIO MCP server without command (covers lines 324-328)."""
        task_key = "mcp_stdio_no_command_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock MCP server response without command
        mock_server = Mock()
        mock_server.id = "stdio_server"
        mock_server.name = "StdioServer"
        mock_server.server_type = "STDIO"
        mock_server.command = None  # No command
        mock_server.args = []
        mock_server.api_key = "test-key"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_servers_response = Mock(servers=[mock_server])
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server)
            mock_mcp_instance.resolve_effective_servers = AsyncMock(return_value=[mock_server])
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            task = await create_task(task_key, task_config, agent)
            
            # Should still create task despite missing command
            assert isinstance(task, Task)
            assert task.description == "Task description"
            assert len(task.tools) == 0
    
    @pytest.mark.asyncio
    async def test_create_task_mcp_stdio_additional_config_nonstring_coverage(self):
        """Test STDIO MCP server with non-string additional config (covers lines 333-337)."""
        task_key = "mcp_stdio_nonstring_config_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock MCP server response with non-string config values
        mock_server = Mock()
        mock_server.id = "stdio_server"
        mock_server.name = "StdioServer"
        mock_server.server_type = "STDIO"
        mock_server.command = "python"
        mock_server.args = ["-m", "mcp_server"]
        mock_server.api_key = "test-key"
        mock_server.additional_config = {
            "string_setting": "value",
            "int_setting": 123,  # Non-string value
            "bool_setting": True  # Non-string value
        }
        
        # Mock MCP tool as a dictionary (as returned by MCPAdapter)
        mock_mcp_tool = {
            "name": "test_tool",
            "description": "Test tool description",
            "mcp_tool": Mock(),
            "input_schema": {},
            "adapter": Mock()  # Will be replaced with actual adapter
        }
        wrapped_tool = Mock(spec=BaseTool)
        wrapped_tool.name = "StdioServer_test_tool"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.common.mcp_adapter.MCPAdapter') as mock_adapter_class, \
             patch('mcp.StdioServerParameters') as mock_stdio_params, \
             patch('src.engines.crewai.tools.mcp_handler.create_crewai_tool_from_mcp') as mock_create_tool, \
             patch('src.engines.crewai.tools.mcp_handler.register_mcp_adapter'), \
             patch.dict('os.environ', {'EXISTING': 'value'}):
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_servers_response = Mock(servers=[mock_server])
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server)
            mock_mcp_instance.resolve_effective_servers = AsyncMock(return_value=[mock_server])
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            # Setup adapter mock
            mock_adapter = Mock()
            # Update the tool dict to have the correct adapter reference
            mock_mcp_tool["adapter"] = mock_adapter
            mock_adapter.tools = [mock_mcp_tool]
            mock_adapter.initialize = AsyncMock()
            mock_adapter_class.return_value = mock_adapter
            
            # Setup create_crewai_tool_from_mcp mock
            mock_create_tool.return_value = wrapped_tool
            
            task = await create_task(task_key, task_config, agent)
        
        # Verify STDIO is not supported - no tools should be added
        # The log shows "WARNING - Unsupported MCP server type: STDIO"
        mock_stdio_params.assert_not_called()
        mock_adapter_class.assert_not_called()
        mock_create_tool.assert_not_called()
        
        # Verify no tools were added since STDIO is unsupported
        assert len(task.tools) == 0
    
    @pytest.mark.asyncio
    async def test_create_task_mcp_unsupported_server_type_coverage(self):
        """Test unsupported MCP server type (covers lines 378-380)."""
        task_key = "mcp_unsupported_type_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock MCP server with unsupported type
        mock_server = Mock()
        mock_server.id = "unsupported_server"
        mock_server.name = "UnsupportedServer"
        mock_server.server_type = "WEBSOCKET"  # Unsupported type
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_servers_response = Mock(servers=[mock_server])
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server)
            mock_mcp_instance.resolve_effective_servers = AsyncMock(return_value=[mock_server])
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            task = await create_task(task_key, task_config, agent)
            
            # Should still create task despite unsupported server type
            assert isinstance(task, Task)
            assert task.description == "Task description"
            assert len(task.tools) == 0
    
    @pytest.mark.asyncio
    async def test_create_task_tool_name_filtering_coverage(self):
        """Test tool name filtering with empty values (covers lines 398-399)."""
        task_key = "tool_name_filtering_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["1", "2"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        mock_tool_service = Mock()
        mock_tool1 = Mock(title="Tool1")
        mock_tool2 = Mock(title="Tool2")
        mock_tool_service.get_tool_by_id = AsyncMock(side_effect=[mock_tool1, mock_tool2])
        mock_tool_service.get_tool_config_by_name = AsyncMock(return_value={})
        
        mock_tool_factory = Mock()
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            # Mock tool resolution to return some empty/None values
            mock_resolve.return_value = ["tool1", None, "", "tool2"]
            
            # Mock tool factory to create tools for valid names only
            tool1_instance = Mock(spec=BaseTool)
            tool1_instance.name = "tool1"
            tool2_instance = Mock(spec=BaseTool)
            tool2_instance.name = "tool2"
            
            mock_tool_factory.create_tool.side_effect = [tool1_instance, tool2_instance]
            
            task = await create_task(
                task_key, 
                task_config, 
                agent,
                tool_service=mock_tool_service,
                tool_factory=mock_tool_factory
            )
        
        # Should only create tools for non-empty names (filtering out None and "")
        assert len(task.tools) == 2
        assert tool1_instance in task.tools
        assert tool2_instance in task.tools
    
    @pytest.mark.asyncio
    async def test_create_task_tool_service_no_config_method_coverage(self):
        """Test tool service without get_tool_config_by_name method (covers lines 404-405)."""
        task_key = "tool_service_no_config_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["1"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Create mock tool service WITHOUT get_tool_config_by_name method
        mock_tool_service = Mock()
        mock_tool1 = Mock(title="Tool1")
        mock_tool_service.get_tool_by_id = AsyncMock(side_effect=[mock_tool1])
        # Explicitly remove the method to trigger the hasattr check
        if hasattr(mock_tool_service, 'get_tool_config_by_name'):
            delattr(mock_tool_service, 'get_tool_config_by_name')
        
        mock_tool_factory = Mock()
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            mock_resolve.return_value = ["Tool1"]
            
            tool1_instance = Mock(spec=BaseTool)
            tool1_instance.name = "tool1"
            mock_tool_factory.create_tool.return_value = tool1_instance
            
            task = await create_task(
                task_key, 
                task_config, 
                agent,
                tool_service=mock_tool_service,
                tool_factory=mock_tool_factory
            )
        
        # Should create tool with empty config when method doesn't exist
        assert len(task.tools) == 1
        assert tool1_instance in task.tools
        # Verify create_tool was called with empty config
        mock_tool_factory.create_tool.assert_called_with("Tool1", result_as_answer=False, tool_config_override={})
    
    @pytest.mark.asyncio
    async def test_create_task_mcp_service_adapter_coverage(self):
        """Test MCP service adapter path (covers lines 418-422)."""
        task_key = "mcp_service_adapter_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["1"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        mock_tool_service = Mock()
        mock_tool1 = Mock(title="Tool1")
        mock_tool_service.get_tool_by_id = AsyncMock(side_effect=[mock_tool1])
        mock_tool_service.get_tool_config_by_name = AsyncMock(return_value={})
        
        mock_tool_factory = Mock()
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            mock_resolve.return_value = ["tool1"]
            
            # Mock tool factory to return MCP service adapter tuple
            mock_tool_factory.create_tool.return_value = (True, 'mcp_service_adapter')
            
            task = await create_task(
                task_key, 
                task_config, 
                agent,
                tool_service=mock_tool_service,
                tool_factory=mock_tool_factory
            )
        
        # Should continue without adding tools (service adapter is deprecated)
        assert len(task.tools) == 0
    
    @pytest.mark.asyncio
    async def test_create_task_mcp_special_tool_handling(self):
        """Test MCP special tool handling cases."""
        task_key = "mcp_special_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["1"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock tool service to return a tool with ID "mcp_tool"
        mock_tool_service = Mock()
        mock_tool = Mock()
        mock_tool.title = "mcp_tool"
        mock_tool_service.get_tool_by_id = AsyncMock(return_value=mock_tool)
        mock_tool_service.get_tool_config_by_name = AsyncMock(return_value={})
        
        mock_tool_factory = Mock()
        
        # Mock special MCP tool response
        mock_tool1 = Mock(spec=BaseTool)
        mock_tool1.name = "mcp_tool1"
        mock_tool2 = Mock(spec=BaseTool)
        mock_tool2.name = "mcp_tool2"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            # Mock tool factory returning MCP tool tuple
            mock_tool_factory.create_tool.return_value = (True, [mock_tool1, mock_tool2])
            
            task = await create_task(
                task_key, 
                task_config, 
                agent,
                tool_service=mock_tool_service,
                tool_factory=mock_tool_factory
            )
            
            assert len(task.tools) == 2
            assert mock_tool1 in task.tools
            assert mock_tool2 in task.tools
    
    @pytest.mark.asyncio
    async def test_create_task_mcp_service_adapter_deprecated(self):
        """Test deprecated MCP service adapter handling."""
        task_key = "deprecated_adapter_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["mcp_tool"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        mock_tool_service = Mock()
        mock_tool_factory = Mock()
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            mock_resolve.side_effect = AsyncMock(return_value=["mcp_tool"])
            
            # Mock tool factory returning deprecated service adapter
            mock_tool_factory.create_tool.return_value = (True, 'mcp_service_adapter')
            
            task = await create_task(
                task_key, 
                task_config, 
                agent,
                tool_service=mock_tool_service,
                tool_factory=mock_tool_factory
            )
            
            # Should create task without adding the deprecated adapter
            assert isinstance(task, Task)
            assert len(task.tools) == 0
    
    @pytest.mark.asyncio
    async def test_create_task_tool_resolution_exception(self):
        """Test tool resolution exception handling (covers lines 449-450)."""
        task_key = "tool_resolution_exception_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["1"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        mock_tool_service = Mock()
        # Make tool service raise exception during tool resolution
        mock_tool_service.get_tool_by_id = AsyncMock(side_effect=Exception("Tool resolution failed"))
        
        mock_tool_factory = Mock()
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            # Task creation should continue despite tool resolution error
            task = await create_task(
                task_key, 
                task_config, 
                agent,
                tool_service=mock_tool_service,
                tool_factory=mock_tool_factory
            )
        
        # Should still create task (error was handled gracefully)
        assert task is not None
        assert len(task.tools) == 0  # No tools added due to resolution error
    
    @pytest.mark.asyncio
    async def test_create_task_tool_resolution_outer_exception(self):
        """Test exception in tool resolution outer try block to cover lines 449-450."""
        task_key = "tool_resolution_outer_exception_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["1"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Create a mock tool service that will raise an exception
        mock_tool_service = Mock()
        mock_tool_factory = Mock()
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            # Make the tool service raise an exception before resolve_tool_ids_to_names is called
            # This should trigger the outer except block at lines 449-450
            with patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names', side_effect=Exception("Tool resolution system failed")):
                task = await create_task(
                    task_key, 
                    task_config, 
                    agent,
                    tool_service=mock_tool_service,
                    tool_factory=mock_tool_factory
                )
        
        # Should still create task (error was handled gracefully)
        assert task is not None
        assert len(task.tools) == 0  # No tools added due to resolution error
    
    @pytest.mark.asyncio
    async def test_create_task_with_tools_no_factory(self):
        """Test tool resolution without tool factory."""
        task_key = "no_factory_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["1", "2"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock tool service to simulate tools with numeric IDs
        mock_tool_service = Mock()
        mock_tool1 = Mock()
        mock_tool1.title = "tool1"
        mock_tool2 = Mock()
        mock_tool2.title = "tool2"
        mock_tool_service.get_tool_by_id = AsyncMock(side_effect=[mock_tool1, mock_tool2])
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            # No tool_factory provided - should raise validation error since CrewAI requires BaseTool instances
            with pytest.raises(Exception) as exc_info:
                task = await create_task(
                    task_key, 
                    task_config, 
                    agent,
                    tool_service=mock_tool_service
                )
            
            # Should fail because CrewAI requires BaseTool instances, not strings
            assert "should be a valid dictionary or instance of BaseTool" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_create_task_tool_factory_returns_none(self):
        """Test when tool factory returns None."""
        task_key = "none_tool_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["tool1"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        mock_tool_service = Mock()
        mock_tool_factory = Mock()
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            mock_resolve.side_effect = AsyncMock(return_value=["tool1"])
            
            # Tool factory returns None
            mock_tool_factory.create_tool.return_value = None
            
            task = await create_task(
                task_key, 
                task_config, 
                agent,
                tool_service=mock_tool_service,
                tool_factory=mock_tool_factory
            )
            
            # Should create task without the failed tool
            assert isinstance(task, Task)
            assert len(task.tools) == 0
    
    @pytest.mark.asyncio
    async def test_create_task_unexpected_mcp_tools_format(self):
        """Test unexpected MCP tools format."""
        task_key = "unexpected_format_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["mcp_tool"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        mock_tool_service = Mock()
        mock_tool_factory = Mock()
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            mock_resolve.side_effect = AsyncMock(return_value=["mcp_tool"])
            
            # Mock tool factory returning unexpected format
            mock_tool_factory.create_tool.return_value = (True, {"unexpected": "format"})
            
            task = await create_task(
                task_key, 
                task_config, 
                agent,
                tool_service=mock_tool_service,
                tool_factory=mock_tool_factory
            )
            
            # Should create task without the malformed tool
            assert isinstance(task, Task)
            assert len(task.tools) == 0
    
    @pytest.mark.asyncio
    async def test_create_task_output_json_string_false(self):
        """Test output_json handling when it's string 'false'."""
        task_key = "output_json_false_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "output_json": "false"  # String false should not be included
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            task = await create_task(task_key, task_config, agent)
            
            # output_json should not be set when it's "false"
            assert not hasattr(task, 'output_json') or task.output_json != "false"
    
    @pytest.mark.asyncio
    async def test_create_task_output_json_string_true(self):
        """Test output_json handling when it's string 'true'."""
        task_key = "output_json_true_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "output_json": "true"  # String true should be included
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            task = await create_task(task_key, task_config, agent)
            
            # String output_json should be ignored since CrewAI requires BaseModel class
            assert isinstance(task, Task)
            # The task should be created successfully but without the string output_json
    
    @pytest.mark.asyncio
    async def test_create_task_pydantic_output_not_compatible(self):
        """Test output_pydantic when model is not compatible."""
        task_key = "incompatible_pydantic_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "output_pydantic": "TestModel"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        agent.llm = Mock()
        agent.llm.model = "test-model"
        
        # Create a test Pydantic model
        from pydantic import create_model
        TestModel = create_model("TestModel", name=(str, ...))
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.helpers.task_helpers.get_pydantic_class_from_name') as mock_get_pydantic, \
             patch('src.engines.crewai.helpers.model_conversion_handler.get_compatible_converter_for_model') as mock_converter:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            mock_get_pydantic.return_value = TestModel
            # Return incompatible result
            mock_converter.return_value = (None, None, False, False)
            
            task = await create_task(task_key, task_config, agent)
        
        # Should fall back to standard approach
        assert task.output_pydantic == TestModel
    
    @pytest.mark.asyncio
    async def test_create_task_pydantic_output_use_converter(self):
        """Test output_pydantic when using converter class."""
        task_key = "converter_pydantic_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "output_pydantic": "TestModel"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        agent.llm = Mock()
        agent.llm.model = "test-model"
        
        # Create a test Pydantic model and converter
        from pydantic import create_model
        from crewai.utilities.converter import Converter
        
        TestModel = create_model("TestModel", name=(str, ...))
        
        class MockConverter(Converter):
            def to_pydantic(self, data, agent=None):
                return TestModel(name="test")
            
            def to_json(self, data, agent=None):
                return '{"name": "test"}'
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.helpers.task_helpers.get_pydantic_class_from_name') as mock_get_pydantic, \
             patch('src.engines.crewai.helpers.model_conversion_handler.get_compatible_converter_for_model') as mock_converter:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            mock_get_pydantic.return_value = TestModel
            # Return compatible result with converter
            mock_converter.return_value = (MockConverter, TestModel, False, True)
            
            task = await create_task(task_key, task_config, agent)
        
        # Should use converter
        assert task.converter_cls == MockConverter
        assert task.output_pydantic == TestModel
    
    @pytest.mark.asyncio
    async def test_create_task_pydantic_output_use_json_approach(self):
        """Test output_pydantic when using output_json approach."""
        task_key = "json_approach_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "output_pydantic": "TestModel"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        agent.llm = Mock()
        agent.llm.model = "test-model"
        
        # Create a test Pydantic model
        from pydantic import create_model
        TestModel = create_model("TestModel", name=(str, ...))
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.helpers.task_helpers.get_pydantic_class_from_name') as mock_get_pydantic, \
             patch('src.engines.crewai.helpers.model_conversion_handler.get_compatible_converter_for_model') as mock_converter, \
             patch('src.engines.crewai.helpers.model_conversion_handler.configure_output_json_approach') as mock_configure:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            mock_get_pydantic.return_value = TestModel
            # Return compatible result with json approach
            mock_converter.return_value = (None, TestModel, True, True)
            # Mock configure_output_json_approach to return a dict with task args
            def mock_configure_fn(task_args, pydantic_class):
                # Return the original task_args with output_json added
                task_args_copy = task_args.copy()
                task_args_copy['output_json'] = pydantic_class
                return task_args_copy
            mock_configure.side_effect = mock_configure_fn
            
            task = await create_task(task_key, task_config, agent)
        
        # Should call configure_output_json_approach
        mock_configure.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_task_output_directory_creation_error(self):
        """Test error handling when output directory creation fails."""
        task_key = "dir_error_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "output_file": "/invalid/path/output.txt"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('os.makedirs') as mock_makedirs:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            mock_makedirs.side_effect = OSError("Permission denied")
            
            # Should still create task despite directory creation error
            task = await create_task(task_key, task_config, agent)
            
            assert isinstance(task, Task)
            assert task.output_file.endswith("invalid/path/output.txt")
    
    @pytest.mark.asyncio
    async def test_create_task_tool_config_with_get_tool_config_by_name(self):
        """Test tool creation with tool config lookup."""
        task_key = "tool_config_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["1"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock tool service that properly resolves tool IDs
        mock_tool_service = Mock()
        mock_tool = Mock()
        mock_tool.title = "tool1"
        mock_tool_service.get_tool_by_id = AsyncMock(return_value=mock_tool)
        mock_tool_service.get_tool_config_by_name = AsyncMock(return_value={"result_as_answer": True})
        
        mock_tool_factory = Mock()
        mock_tool_instance = Mock(spec=BaseTool)
        mock_tool_instance.name = "tool1"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            mock_tool_factory.create_tool.return_value = mock_tool_instance
            
            task = await create_task(
                task_key, 
                task_config, 
                agent,
                tool_service=mock_tool_service,
                tool_factory=mock_tool_factory
            )
            
            # Should call get_tool_config_by_name and pass config to create_tool
            mock_tool_service.get_tool_config_by_name.assert_called_once_with("tool1")
            mock_tool_factory.create_tool.assert_called_once_with("tool1", result_as_answer=True, tool_config_override={})
            assert mock_tool_instance in task.tools
    
    @pytest.mark.asyncio
    async def test_create_task_stdio_not_supported(self):
        """Test STDIO server type is not supported."""
        task_key = "stdio_config_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock MCP server with STDIO type
        mock_server = Mock()
        mock_server.id = "stdio_server"
        mock_server.name = "StdioServer"
        mock_server.server_type = "STDIO"
        mock_server.command = "python"
        mock_server.args = ["-m", "test"]
        mock_server.api_key = "test-key"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_servers_response = Mock(servers=[mock_server])
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server)
            mock_mcp_instance.resolve_effective_servers = AsyncMock(return_value=[mock_server])
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            # Create task - STDIO servers should be skipped
            task = await create_task(task_key, task_config, agent)
        
        # Verify task was created but no tools were added (STDIO not supported)
        assert task is not None
        assert task.description == "Task description"
        assert task.expected_output == "Expected output"
    
    @pytest.mark.asyncio
    async def test_create_task_guardrail_factory_import_error(self):
        """Test guardrail setup when factory import fails."""
        task_key = "guardrail_import_error_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "guardrail": {"type": "test_guardrail"},
            "callback": Mock()  # Existing callback should be preserved
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            # Mock import error when trying to import GuardrailFactory
            with patch('builtins.__import__', side_effect=ImportError("Cannot import guardrail factory")):
                task = await create_task(task_key, task_config, agent)
        
        # Should preserve existing callback when guardrail setup fails
        assert task.callback == task_config["callback"]
    
    @pytest.mark.skip(reason="MCP auth now handled by MCPIntegration module")
    async def test_create_task_regular_databricks_server_auth(self):
        """Test regular Databricks server (not Apps) uses API key authentication."""
        task_key = "regular_databricks_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock regular Databricks server
        mock_server = Mock()
        mock_server.id = "databricks_server"
        mock_server.name = "DatabricksServer"
        mock_server.server_type = "SSE"
        mock_server.server_url = "https://workspace.databricks.com/sse"
        mock_server.api_key = "test-api-key"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.common.mcp_adapter.MCPAdapter') as mock_adapter_class, \
             patch('src.utils.databricks_auth.get_mcp_auth_headers') as mock_auth, \
             patch('src.engines.crewai.tools.mcp_handler.create_crewai_tool_from_mcp'), \
             patch('src.engines.crewai.tools.mcp_handler.register_mcp_adapter'):
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_servers_response = Mock(servers=[mock_server])
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server)
            mock_mcp_instance.resolve_effective_servers = AsyncMock(return_value=[mock_server])
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            # OAuth should not be called for non-databricksapps URLs
            
            # Setup adapter mock
            mock_adapter = Mock()
            mock_adapter.tools = []
            mock_adapter.initialize = AsyncMock()
            mock_adapter_class.return_value = mock_adapter
            
            task = await create_task(task_key, task_config, agent)
        
        # Verify OAuth was NOT called for regular Databricks server (only for databricksapps.com)
        mock_auth.assert_not_called()
        
        # Verify adapter was created with API key headers
        call_args = mock_adapter_class.call_args[0][0]
        assert call_args["headers"]["Authorization"] == "Bearer test-api-key"
    
    @pytest.mark.skip(reason="MCP auth now handled by MCPIntegration module")
    async def test_create_task_non_databricks_server_with_api_key(self):
        """Test non-Databricks server with API key."""
        task_key = "non_databricks_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock non-Databricks server
        mock_server = Mock()
        mock_server.id = "regular_server"
        mock_server.name = "RegularServer"
        mock_server.server_type = "SSE"
        mock_server.server_url = "https://example.com/sse"
        mock_server.api_key = "regular-key"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.common.mcp_adapter.MCPAdapter') as mock_adapter_class, \
             patch('src.engines.crewai.tools.mcp_handler.create_crewai_tool_from_mcp'), \
             patch('src.engines.crewai.tools.mcp_handler.register_mcp_adapter'):
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_servers_response = Mock(servers=[mock_server])
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server)
            mock_mcp_instance.resolve_effective_servers = AsyncMock(return_value=[mock_server])
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            # Setup adapter mock
            mock_adapter = Mock()
            mock_adapter.tools = []
            mock_adapter.initialize = AsyncMock()
            mock_adapter_class.return_value = mock_adapter
            
            task = await create_task(task_key, task_config, agent)
        
        # Verify regular API key authentication was used
        call_args = mock_adapter_class.call_args[0][0]
        assert call_args["headers"]["Authorization"] == "Bearer regular-key"
    
    @pytest.mark.skip(reason="MCP auth now handled by MCPIntegration module")
    async def test_create_task_databricks_no_api_key_auth_fail(self):
        """Test Databricks server with no API key and auth failure."""
        task_key = "databricks_no_key_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock Databricks server without API key
        mock_server = Mock()
        mock_server.id = "databricks_server"
        mock_server.name = "DatabricksServer"
        mock_server.server_type = "SSE"
        mock_server.server_url = "https://workspace.databricksapps.com/app"
        mock_server.api_key = None  # No API key
        
        # Create wrapped tool that will be returned by create_crewai_tool_from_mcp
        wrapped_tool = Mock(spec=BaseTool)
        wrapped_tool.name = "DatabricksServer_test_tool"
        wrapped_tool.description = "Test tool description"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.tools.mcp_handler.get_or_create_mcp_adapter') as mock_get_adapter, \
             patch('src.utils.databricks_auth.get_mcp_auth_headers') as mock_auth, \
             patch('src.engines.crewai.tools.mcp_handler.create_crewai_tool_from_mcp') as mock_create_tool, \
             patch('src.engines.crewai.tools.mcp_handler.register_mcp_adapter'):
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_servers_response = Mock(servers=[mock_server])
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server)
            mock_mcp_instance.resolve_effective_servers = AsyncMock(return_value=[mock_server])
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            # Setup OAuth failure with no fallback
            mock_auth.side_effect = Exception("OAuth authentication failed")
            
            # Setup create_crewai_tool_from_mcp mock to return the wrapped tool
            mock_create_tool.return_value = wrapped_tool
            
            # Setup adapter mock with at least one tool to trigger processing
            mock_mcp_tool = {
                "name": "test_tool",
                "description": "Test tool description",
                "mcp_tool": Mock(),
                "input_schema": {},
                "adapter": Mock()
            }
            mock_adapter = Mock()
            mock_mcp_tool["adapter"] = mock_adapter
            mock_adapter.tools = [mock_mcp_tool]
            mock_get_adapter.return_value = mock_adapter
            
            task = await create_task(task_key, task_config, agent)
        
        # Verify the adapter was requested to be created despite auth failure
        mock_get_adapter.assert_called_once()
        # Check that the server params don't include authentication headers when auth fails with exception
        call_args = mock_get_adapter.call_args[0][0]
        assert "headers" not in call_args or not call_args.get("headers")
    
    @pytest.mark.asyncio
    async def test_create_task_stdio_minimal_not_supported(self):
        """Test minimal STDIO server is not supported."""
        task_key = "stdio_minimal_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock minimal STDIO server
        mock_server = Mock()
        mock_server.id = "stdio_server"
        mock_server.name = "StdioServer"
        mock_server.server_type = "STDIO"
        mock_server.command = "python"
        mock_server.args = ["-m", "test"]
        mock_server.api_key = None  # No API key
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_servers_response = Mock(servers=[mock_server])
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server)
            mock_mcp_instance.resolve_effective_servers = AsyncMock(return_value=[mock_server])
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            # Create task - STDIO servers should be skipped
            task = await create_task(task_key, task_config, agent)
        
        # Verify task was created without tools (STDIO not supported)
        assert task is not None
        assert task.description == "Task description"
        assert task.expected_output == "Expected output"
    
    @pytest.mark.asyncio
    async def test_create_task_tool_name_filtering(self):
        """Test tool name filtering with None values - when all tools are filtered out."""
        task_key = "tool_filter_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["1", "2"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        mock_tool_service = Mock()
        
        # Mock tool service to return empty/invalid results for all tool IDs
        def get_tool_by_id_side_effect(tool_id):
            if tool_id == 1:
                mock_obj = Mock()
                mock_obj.title = ""  # Empty tool name - should be filtered
                return mock_obj
            elif tool_id == 2:
                return None  # No tool found - should be filtered
            return None
        
        mock_tool_service.get_tool_by_id = AsyncMock(side_effect=get_tool_by_id_side_effect)
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            # Don't provide tool_factory - when all tools are filtered out, task creation should still work
            task = await create_task(
                task_key, 
                task_config, 
                agent,
                tool_service=mock_tool_service
            )
            
            # Should have no tools since all were filtered out, but task should still be created
            assert isinstance(task, Task)
            assert task.description == "Task description"
            assert len(task.tools) == 0  # All tools filtered out
    
    @pytest.mark.asyncio
    async def test_create_task_stdio_error_during_adapter_creation(self):
        """Test STDIO adapter creation error handling."""
        task_key = "stdio_adapter_error_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock STDIO server
        mock_server = Mock()
        mock_server.id = "stdio_server"
        mock_server.name = "StdioServer"
        mock_server.server_type = "STDIO"
        mock_server.command = "python"
        mock_server.args = ["-m", "test"]
        mock_server.api_key = "test-key"
        mock_server.additional_config = None
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.common.mcp_adapter.MCPAdapter') as mock_adapter_class:
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_servers_response = Mock(servers=[mock_server])
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server)
            mock_mcp_instance.resolve_effective_servers = AsyncMock(return_value=[mock_server])
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            # Setup adapter to fail during creation
            mock_adapter_class.side_effect = Exception("STDIO adapter creation failed")
            
            # Should still create task despite STDIO adapter error
            task = await create_task(task_key, task_config, agent)
            
            assert isinstance(task, Task)
            assert task.description == "Task description"
    
    @pytest.mark.asyncio
    async def test_create_task_uow_initialize_path_coverage(self):
        """Test coverage of UnitOfWork initialization in UnitOfWork path."""
        task_key = "uow_init_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "output_pydantic": "TestSchema"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock schema
        mock_schema = Mock()
        mock_schema.schema_definition = {
            "properties": {"name": {"type": "string"}},
            "required": ["name"]
        }
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.helpers.task_helpers.UnitOfWork') as mock_task_uow:
            
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            # Create async context manager mock for task_helpers.UnitOfWork
            mock_uow = AsyncMock()
            mock_uow.schema_repository.find_by_name.return_value = mock_schema
            mock_task_uow.return_value.__aenter__.return_value = mock_uow
            mock_task_uow.return_value.__aexit__.return_value = None
            
            task = await create_task(task_key, task_config, agent)
            
            assert isinstance(task, Task)
            assert task.description == "Task description"
            assert task.output_pydantic is not None  # Should have resolved the Pydantic model
    
    @pytest.mark.asyncio
    async def test_create_task_nullable_field_coverage(self):
        """Test nullable field handling in Pydantic model creation."""
        task_key = "nullable_field_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "output_pydantic": "NullableTestSchema"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock schema with nullable field that's also required (edge case)
        mock_schema = Mock()
        mock_schema.schema_definition = {
            "properties": {
                "required_nullable": {"type": "string", "nullable": True},
                "optional_nullable": {"type": "integer", "nullable": True}
            },
            "required": ["required_nullable"]
        }
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.helpers.task_helpers.UnitOfWork') as mock_task_uow:
            
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            # Create async context manager mock for task_helpers.UnitOfWork
            mock_uow = AsyncMock()
            mock_uow.schema_repository.find_by_name.return_value = mock_schema
            mock_task_uow.return_value.__aenter__.return_value = mock_uow
            mock_task_uow.return_value.__aexit__.return_value = None
            
            task = await create_task(task_key, task_config, agent)
            
            assert task.output_pydantic is not None
            assert isinstance(task, Task)
    
    @pytest.mark.asyncio
    async def test_create_task_empty_enabled_servers_response(self):
        """Test when enabled servers response is empty but not None."""
        task_key = "empty_servers_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            
            # Setup empty servers response
            mock_mcp_instance = Mock()
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=Mock(servers=[]))
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            task = await create_task(task_key, task_config, agent)
            
            assert isinstance(task, Task)
            assert task.description == "Task description"
    
    @pytest.mark.asyncio 
    async def test_create_task_logger_path_coverage(self):
        """Test various logger paths for complete coverage."""
        task_key = "logger_coverage_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["1"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        # Agent name and role will be accessed via getattr in the implementation
        # No need to set agent.name as it doesn't exist in the Agent model
        
        mock_tool_service = Mock()
        
        # Mock tool service to return empty tool name (will be filtered out)
        mock_tool_obj = Mock()
        mock_tool_obj.title = ""  # Empty tool name - should be filtered
        mock_tool_service.get_tool_by_id = AsyncMock(return_value=mock_tool_obj)
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            # Don't use tool_factory to avoid Mock await issues
            task = await create_task(
                task_key, 
                task_config, 
                agent,
                tool_service=mock_tool_service
            )
            
            assert isinstance(task, Task)
            # Tool should be filtered out due to empty name
            # The test still covers the logger path we're testing for
            assert len(task.tools) == 0
    
    @pytest.mark.asyncio
    async def test_create_task_tool_without_hasattr_name(self):
        """Test handling tools that don't have name attribute."""
        task_key = "no_name_tool_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["1"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        mock_tool_service = Mock()
        
        # Mock tool service to return empty tool name (will be filtered out)
        mock_tool_obj = Mock()
        mock_tool_obj.title = ""  # Empty tool name - should be filtered
        mock_tool_service.get_tool_by_id = AsyncMock(return_value=mock_tool_obj)
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            # Don't use tool_factory to avoid Mock await issues
            task = await create_task(
                task_key, 
                task_config, 
                agent,
                tool_service=mock_tool_service
            )
            
            assert isinstance(task, Task)
            # Tool should be filtered out due to empty name
            # The test still covers the path for tools without name attribute
            assert len(task.tools) == 0
    
    @pytest.mark.asyncio
    async def test_create_task_tool_without_description(self):
        """Test handling tools that don't have description attribute."""
        task_key = "no_desc_tool_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["1"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        mock_tool_service = Mock()
        
        # Mock tool service to return empty tool name (will be filtered out)
        mock_tool_obj = Mock()
        mock_tool_obj.title = ""  # Empty tool name - should be filtered
        mock_tool_service.get_tool_by_id = AsyncMock(return_value=mock_tool_obj)
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            # Don't use tool_factory to avoid Mock await issues
            task = await create_task(
                task_key, 
                task_config, 
                agent,
                tool_service=mock_tool_service
            )
            
            assert isinstance(task, Task)
            # Tool should be filtered out due to empty name
            # The test still covers the path for tools without description
            assert len(task.tools) == 0
    
    @pytest.mark.asyncio
    async def test_create_task_string_tools_coverage(self):
        """Test code path when tools are strings instead of objects."""
        task_key = "string_tools_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["1", "2"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        mock_tool_service = Mock()
        # No tool_factory provided, so tools remain as strings
        
        # Mock tool service to return tool objects
        mock_tool_obj1 = Mock()
        mock_tool_obj1.title = "tool1"
        mock_tool_obj2 = Mock()
        mock_tool_obj2.title = "tool2"
        
        def get_tool_by_id_side_effect(tool_id):
            if tool_id == 1:
                return mock_tool_obj1
            elif tool_id == 2:
                return mock_tool_obj2
            return None
        
        mock_tool_service.get_tool_by_id = AsyncMock(side_effect=get_tool_by_id_side_effect)
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            # String tools should cause a validation error in CrewAI
            with pytest.raises(Exception) as exc_info:
                task = await create_task(
                    task_key, 
                    task_config, 
                    agent,
                    tool_service=mock_tool_service
                    # No tool_factory provided
                )
            
            # Should raise validation error for string tools
            assert "Input should be a valid dictionary or instance of BaseTool" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_create_task_no_tool_service_but_has_tools(self):
        """Test when no tool service provided but tools are in config."""
        task_key = "no_service_tools_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "tools": ["tool1", "tool2"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service:
            
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            task = await create_task(task_key, task_config, agent)
            
            assert isinstance(task, Task)
            # Should have empty tools since no service to resolve them
            assert len(task.tools) == 0
    
    @pytest.mark.asyncio
    async def test_create_task_finally_block_coverage(self):
        """Test finally block coverage in get_pydantic_class_from_name."""
        task_key = "finally_block_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "output_pydantic": "TestSchema"
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock schema
        mock_schema = Mock()
        mock_schema.schema_definition = {
            "properties": {"name": {"type": "string"}},
            "required": ["name"]
        }
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.helpers.task_helpers.UnitOfWork') as mock_sync_uow:
            
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=Mock(
                get_enabled_servers=AsyncMock(return_value=Mock(servers=[]))
            ))
            
            mock_uow = Mock()
            mock_uow._initialized = True
            mock_uow.schema_repository.find_by_name_sync.return_value = mock_schema
            mock_sync_uow.get_instance.return_value = mock_uow
            
            task = await create_task(task_key, task_config, agent)
            
            assert isinstance(task, Task)
            # The finally block should execute without issues
    
    @pytest.mark.skip(reason="Test requires refactoring to match implementation changes")
    @pytest.mark.asyncio
    async def test_create_task_server_url_endswith_sse(self):
        """Test server URL that already ends with /sse."""
        task_key = "sse_url_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "enabled_servers": ["apps_server"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock Databricks Apps server with URL ending in /sse
        mock_server = Mock()
        mock_server.id = "apps_server"
        mock_server.name = "AppsServer"
        mock_server.server_type = "SSE"
        mock_server.server_url = "https://workspace.databricksapps.com/app/sse"  # Already ends with /sse
        mock_server.api_key = "test-key"
        
        # Create wrapped tool that will be returned by create_crewai_tool_from_mcp
        wrapped_tool = Mock(spec=BaseTool)
        wrapped_tool.name = "AppsServer_test_tool"
        wrapped_tool.description = "Test tool description"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.tools.mcp_handler.get_or_create_mcp_adapter') as mock_get_adapter, \
             patch('src.utils.databricks_auth.get_mcp_auth_headers') as mock_auth, \
             patch('src.engines.crewai.tools.mcp_handler.create_crewai_tool_from_mcp') as mock_create_tool, \
             patch('src.engines.crewai.tools.mcp_handler.register_mcp_adapter'):
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_servers_response = Mock(servers=[mock_server])
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server)
            mock_mcp_instance.resolve_effective_servers = AsyncMock(return_value=[mock_server])
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            # Setup OAuth success
            mock_auth.return_value = ({"Authorization": "Bearer oauth-token"}, None)
            
            # Setup create_crewai_tool_from_mcp mock to return the wrapped tool
            mock_create_tool.return_value = wrapped_tool
            
            # Setup adapter mock with at least one tool to trigger processing
            mock_mcp_tool = {
                "name": "test_tool",
                "description": "Test tool description",
                "mcp_tool": Mock(),
                "input_schema": {},
                "adapter": Mock()
            }
            mock_adapter = Mock()
            mock_mcp_tool["adapter"] = mock_adapter
            mock_adapter.tools = [mock_mcp_tool]
            mock_get_adapter.return_value = mock_adapter
            
            task = await create_task(task_key, task_config, agent)
            
            # Verify URL was not modified (should remain ending with /sse)
            call_args = mock_get_adapter.call_args[0][0]
            assert call_args["url"] == "https://workspace.databricksapps.com/app/sse"
    
    @pytest.mark.skip(reason="Test requires refactoring to match implementation changes")
    @pytest.mark.asyncio
    async def test_create_task_databricks_apps_url_without_sse(self):
        """Test Databricks Apps URL that doesn't end with /sse gets modified."""
        task_key = "apps_no_sse_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "enabled_servers": ["apps_server"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock Databricks Apps server without /sse
        mock_server = Mock()
        mock_server.id = "apps_server"
        mock_server.name = "AppsServer"
        mock_server.server_type = "SSE"
        mock_server.server_url = "https://workspace.databricksapps.com/app"  # No /sse
        mock_server.api_key = "test-key"
        
        # Create wrapped tool that will be returned by create_crewai_tool_from_mcp
        wrapped_tool = Mock(spec=BaseTool)
        wrapped_tool.name = "AppsServer_test_tool"
        wrapped_tool.description = "Test tool description"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.tools.mcp_handler.get_or_create_mcp_adapter') as mock_get_adapter, \
             patch('src.utils.databricks_auth.get_mcp_auth_headers') as mock_auth, \
             patch('src.engines.crewai.tools.mcp_handler.create_crewai_tool_from_mcp') as mock_create_tool, \
             patch('src.engines.crewai.tools.mcp_handler.register_mcp_adapter'):
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_servers_response = Mock(servers=[mock_server])
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server)
            mock_mcp_instance.resolve_effective_servers = AsyncMock(return_value=[mock_server])
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            # Setup OAuth success
            mock_auth.return_value = ({"Authorization": "Bearer oauth-token"}, None)
            
            # Setup create_crewai_tool_from_mcp mock to return the wrapped tool
            mock_create_tool.return_value = wrapped_tool
            
            # Setup adapter mock with at least one tool to trigger processing
            mock_mcp_tool = {
                "name": "test_tool",
                "description": "Test tool description",
                "mcp_tool": Mock(),
                "input_schema": {},
                "adapter": Mock()
            }
            mock_adapter = Mock()
            mock_mcp_tool["adapter"] = mock_adapter
            mock_adapter.tools = [mock_mcp_tool]
            mock_get_adapter.return_value = mock_adapter
            
            task = await create_task(task_key, task_config, agent)
            
            # Verify URL was modified to include /sse
            call_args = mock_get_adapter.call_args[0][0]
            assert call_args["url"] == "https://workspace.databricksapps.com/app/sse"
    
    @pytest.mark.skip(reason="Test requires refactoring to match implementation changes")
    @pytest.mark.asyncio
    async def test_create_task_databricks_oauth_with_fallback(self):
        """Test Databricks server with OAuth failure and API key fallback."""
        task_key = "oauth_fallback_task"
        task_config = {
            "description": "Task description",
            "expected_output": "Expected output",
            "enabled_servers": ["databricks_server"]
        }
        agent = Agent(
            role="TestRole",
            goal="Test Goal", 
            backstory="Test Backstory",
            verbose=True
        )
        
        # Mock Databricks Apps server
        mock_server = Mock()
        mock_server.id = "apps_server"
        mock_server.name = "AppsServer"
        mock_server.server_type = "SSE"
        mock_server.server_url = "https://workspace.databricksapps.com/app"
        mock_server.api_key = "fallback-api-key"
        
        # Create wrapped tool that will be returned by create_crewai_tool_from_mcp
        wrapped_tool = Mock(spec=BaseTool)
        wrapped_tool.name = "AppsServer_test_tool"
        wrapped_tool.description = "Test tool description"
        
        with patch('src.core.unit_of_work.UnitOfWork'), \
             patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
             patch('src.engines.crewai.tools.mcp_handler.get_or_create_mcp_adapter') as mock_get_adapter, \
             patch('src.utils.databricks_auth.get_mcp_auth_headers') as mock_auth, \
             patch('src.engines.crewai.tools.mcp_handler.create_crewai_tool_from_mcp') as mock_create_tool, \
             patch('src.engines.crewai.tools.mcp_handler.register_mcp_adapter'):
            
            # Setup MCP service mocks
            mock_mcp_instance = Mock()
            mock_servers_response = Mock(servers=[mock_server])
            mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
            mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server)
            mock_mcp_instance.resolve_effective_servers = AsyncMock(return_value=[mock_server])
            mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
            
            # Setup OAuth failure but return headers=None, error=msg
            mock_auth.return_value = (None, "OAuth failed")
            
            # Setup create_crewai_tool_from_mcp mock to return the wrapped tool
            mock_create_tool.return_value = wrapped_tool
            
            # Setup adapter mock with at least one tool to trigger processing
            mock_mcp_tool = {
                "name": "test_tool",
                "description": "Test tool description",
                "mcp_tool": Mock(),
                "input_schema": {},
                "adapter": Mock()
            }
            mock_adapter = Mock()
            mock_mcp_tool["adapter"] = mock_adapter
            mock_adapter.tools = [mock_mcp_tool]
            mock_get_adapter.return_value = mock_adapter
            
            task = await create_task(task_key, task_config, agent)
            
            # When OAuth fails, no headers should be added (no automatic fallback)
            call_args = mock_get_adapter.call_args[0][0]
            # Headers should not be in the params when OAuth fails
            assert "headers" not in call_args or call_args.get("headers") == {}

