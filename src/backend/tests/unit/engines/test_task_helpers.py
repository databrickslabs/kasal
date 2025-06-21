import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call, mock_open
import json
import asyncio
from typing import Dict, Any, List

from src.engines.crewai.helpers.task_helpers import (
    is_data_missing,
    get_pydantic_class_from_name,
    create_task
)


class TestIsDataMissing:
    """Test the is_data_missing function"""
    
    def test_is_data_missing_with_task_output(self):
        """Test with TaskOutput that has pydantic model"""
        mock_output = MagicMock()
        mock_output.pydantic = MagicMock()
        mock_output.pydantic.events = [1, 2, 3]  # Less than 10
        
        result = is_data_missing(mock_output)
        assert result is True
    
    def test_is_data_missing_with_sufficient_events(self):
        """Test with TaskOutput that has sufficient events"""
        mock_output = MagicMock()
        mock_output.pydantic = MagicMock()
        mock_output.pydantic.events = list(range(15))  # More than 10
        
        result = is_data_missing(mock_output)
        assert result is False
    
    def test_is_data_missing_no_pydantic(self):
        """Test with output that has no pydantic attribute"""
        mock_output = MagicMock()
        del mock_output.pydantic  # Remove pydantic attribute
        
        result = is_data_missing(mock_output)
        assert result is True


class TestGetPydanticClassFromName:
    """Test the get_pydantic_class_from_name function"""
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.helpers.task_helpers.UnitOfWork')
    async def test_get_pydantic_class_from_name_success(self, mock_uow_class):
        """Test successful class retrieval"""
        # Setup mock schema
        mock_schema = MagicMock()
        mock_schema.schema_definition = {
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"}
            },
            "required": ["name"]
        }
        
        # Setup UoW mock with async context manager
        mock_uow = AsyncMock()
        mock_uow_class.return_value.__aenter__.return_value = mock_uow
        mock_uow_class.return_value.__aexit__.return_value = None
        mock_uow.schema_repository.find_by_name.return_value = mock_schema
        
        result = await get_pydantic_class_from_name("TestSchema")
        
        assert result is not None
        assert result.__name__ == "TestSchema"
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.helpers.task_helpers.UnitOfWork')
    async def test_get_pydantic_class_from_name_not_found(self, mock_uow_class):
        """Test when schema is not found"""
        # Setup UoW mock with async context manager
        mock_uow = AsyncMock()
        mock_uow_class.return_value.__aenter__.return_value = mock_uow
        mock_uow_class.return_value.__aexit__.return_value = None
        mock_uow.schema_repository.find_by_name.return_value = None
        
        result = await get_pydantic_class_from_name("NonExistentSchema")
        
        assert result is None
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.helpers.task_helpers.UnitOfWork')
    async def test_get_pydantic_class_from_name_with_schema(self, mock_uow_class):
        """Test when UnitOfWork returns a valid schema"""
        # Setup schema mock
        mock_schema = MagicMock()
        mock_schema.schema_definition = {
            "properties": {
                "name": {"type": "string"}
            },
            "required": ["name"]
        }
        
        # Setup UoW mock with async context manager
        mock_uow = AsyncMock()
        mock_uow_class.return_value.__aenter__.return_value = mock_uow
        mock_uow_class.return_value.__aexit__.return_value = None
        mock_uow.schema_repository.find_by_name.return_value = mock_schema
        
        result = await get_pydantic_class_from_name("TestSchema")
        
        assert result is not None
        assert result.__name__ == "TestSchema"
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.helpers.task_helpers.UnitOfWork')
    async def test_get_pydantic_class_from_name_invalid_schema_definition(self, mock_uow_class):
        """Test when schema definition is invalid"""
        # Setup schema with invalid definition
        mock_schema = MagicMock()
        mock_schema.schema_definition = "invalid"
        
        # Setup UoW mock with async context manager
        mock_uow = AsyncMock()
        mock_uow_class.return_value.__aenter__.return_value = mock_uow
        mock_uow_class.return_value.__aexit__.return_value = None
        mock_uow.schema_repository.find_by_name.return_value = mock_schema
        
        result = await get_pydantic_class_from_name("TestSchema")
        
        assert result is None
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.helpers.task_helpers.UnitOfWork')
    async def test_get_pydantic_class_from_name_none_schema_definition(self, mock_uow_class):
        """Test when schema definition is None"""
        # Setup schema with None definition
        mock_schema = MagicMock()
        mock_schema.schema_definition = None
        
        # Setup UoW mock with async context manager
        mock_uow = AsyncMock()
        mock_uow_class.return_value.__aenter__.return_value = mock_uow
        mock_uow_class.return_value.__aexit__.return_value = None
        mock_uow.schema_repository.find_by_name.return_value = mock_schema
        
        result = await get_pydantic_class_from_name("TestSchema")
        
        assert result is None
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.helpers.task_helpers.UnitOfWork')
    async def test_get_pydantic_class_from_name_complex_types(self, mock_uow_class):
        """Test schema with complex field types"""
        # Setup schema with complex types
        mock_schema = MagicMock()
        mock_schema.schema_definition = {
            "properties": {
                "numbers": {
                    "type": "array",
                    "items": {"type": "integer"}
                },
                "data": {
                    "type": "object"
                },
                "flags": {
                    "type": "array",
                    "items": {"type": "boolean"}
                },
                "scores": {
                    "type": "array",
                    "items": {"type": "number"}
                },
                "tags": {
                    "type": "array",
                    "items": {"type": "unknown"}
                },
                "unknown_field": {
                    "type": "unknown"
                },
                "nullable_field": {
                    "type": "string",
                    "nullable": True
                }
            },
            "required": ["numbers"]
        }
        
        # Setup UoW mock with async context manager
        mock_uow = AsyncMock()
        mock_uow_class.return_value.__aenter__.return_value = mock_uow
        mock_uow_class.return_value.__aexit__.return_value = None
        mock_uow.schema_repository.find_by_name.return_value = mock_schema
        
        result = await get_pydantic_class_from_name("ComplexSchema")
        
        assert result is not None
        assert result.__name__ == "ComplexSchema"
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.helpers.task_helpers.UnitOfWork')
    async def test_get_pydantic_class_from_name_field_error(self, mock_uow_class):
        """Test when field definition causes an error"""
        # Setup schema with problematic field
        mock_schema = MagicMock()
        mock_schema.schema_definition = {
            "properties": {
                "problematic_field": {
                    "type": "string"
                }
            },
            "required": ["problematic_field"]
        }
        
        # Setup UoW mock with async context manager
        mock_uow = AsyncMock()
        mock_uow_class.return_value.__aenter__.return_value = mock_uow
        mock_uow_class.return_value.__aexit__.return_value = None
        mock_uow.schema_repository.find_by_name.return_value = mock_schema
        
        # Mock create_model to raise an exception
        with patch('src.engines.crewai.helpers.task_helpers.create_model') as mock_create:
            mock_create.side_effect = Exception("Field error")
            
            result = await get_pydantic_class_from_name("ProblematicSchema")
            
            assert result is None
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.helpers.task_helpers.UnitOfWork')
    async def test_get_pydantic_class_from_name_general_exception(self, mock_uow_class):
        """Test when a general exception occurs"""
        # Setup UoW mock to raise exception when entering context
        mock_uow_class.return_value.__aenter__.side_effect = Exception("Database error")
        
        result = await get_pydantic_class_from_name("TestSchema")
        
        assert result is None


class TestCreateTask:
    """Test the create_task function"""
    
    @pytest.fixture
    def mock_task_config(self):
        """Mock task configuration"""
        return {
            "description": "Test task description",
            "expected_output": "Expected output format"
        }
    
    @pytest.fixture
    def mock_agent(self):
        """Mock agent"""
        mock_agent = MagicMock()
        mock_agent.role = "Test Agent"
        mock_agent.name = "test_agent"
        return mock_agent
    
    @pytest.fixture
    def mock_tools_list(self):
        """Mock tools list"""
        mock_tool1 = MagicMock()
        mock_tool1.name = "tool1"
        mock_tool2 = MagicMock()
        mock_tool2.name = "tool2"
        return [mock_tool1, mock_tool2]
    
    @pytest.mark.asyncio
    async def test_create_task_basic(self, mock_task_config, mock_agent, mock_tools_list):
        """Test basic task creation"""
        with patch('src.engines.crewai.helpers.task_helpers.Task') as mock_task_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow, \
             patch('src.services.mcp_service.MCPService') as mock_mcp:
            
            # Setup UoW to return empty MCP servers
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            mock_mcp_service = AsyncMock()
            mock_mcp.from_unit_of_work.return_value = mock_mcp_service
            mock_mcp_service.get_enabled_servers.return_value = None
            
            mock_task_instance = MagicMock()
            mock_task_class.return_value = mock_task_instance
            
            result = await create_task(
                task_key="test_task",
                task_config=mock_task_config,
                agent=mock_agent,
                tools=mock_tools_list
            )
            
            assert result == mock_task_instance
            mock_task_class.assert_called_once()
            
            # Verify the task was created with correct parameters
            call_args = mock_task_class.call_args
            assert call_args[1]["description"] == "Test task description"
            assert call_args[1]["expected_output"] == "Expected output format"
            assert call_args[1]["agent"] == mock_agent
    
    @pytest.mark.asyncio
    async def test_create_task_with_output_pydantic(self, mock_agent, mock_tools_list):
        """Test task creation with Pydantic output"""
        task_config = {
            "description": "Test task",
            "expected_output": "Pydantic output",
            "output_pydantic": "TestOutputModel"
        }
        
        mock_pydantic_class = MagicMock()
        mock_pydantic_class.__name__ = "TestOutputModel"
        
        with patch('src.engines.crewai.helpers.task_helpers.Task') as mock_task_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow, \
             patch('src.services.mcp_service.MCPService') as mock_mcp, \
             patch('src.engines.crewai.helpers.task_helpers.get_pydantic_class_from_name') as mock_get_class:
            
            # Setup UoW to return empty MCP servers
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            mock_mcp_service = AsyncMock()
            mock_mcp.from_unit_of_work.return_value = mock_mcp_service
            mock_mcp_service.get_enabled_servers.return_value = None
            
            # Mock async function to return a coroutine
            async def mock_get_pydantic_class(name):
                return mock_pydantic_class
            mock_get_class.side_effect = mock_get_pydantic_class
            mock_task_instance = MagicMock()
            mock_task_class.return_value = mock_task_instance
            
            result = await create_task(
                task_key="test_task",
                task_config=task_config,
                agent=mock_agent,
                tools=mock_tools_list
            )
            
            assert result == mock_task_instance
            mock_get_class.assert_called_once_with("TestOutputModel")
            
            # Verify output_pydantic was set
            call_args = mock_task_class.call_args
            assert call_args[1]["output_pydantic"] == mock_pydantic_class
    
    
    @pytest.mark.asyncio
    async def test_create_task_with_output_dir(self, mock_agent, mock_tools_list):
        """Test task creation with output directory"""
        task_config = {
            "description": "Test task",
            "expected_output": "Test output",
            "output_file_enabled": True,
            "output_filename": "test_output.md"
        }
        
        with patch('src.engines.crewai.helpers.task_helpers.Task') as mock_task_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow, \
             patch('src.services.mcp_service.MCPService') as mock_mcp, \
             patch('os.makedirs') as mock_makedirs:
            
            # Setup UoW to return empty MCP servers
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            mock_mcp_service = AsyncMock()
            mock_mcp.from_unit_of_work.return_value = mock_mcp_service
            mock_mcp_service.get_enabled_servers.return_value = None
            
            mock_task_instance = MagicMock()
            mock_task_class.return_value = mock_task_instance
            
            result = await create_task(
                task_key="test_task",
                task_config=task_config,
                agent=mock_agent,
                tools=mock_tools_list,
                output_dir="./test_output"
            )
            
            assert result == mock_task_instance
            mock_makedirs.assert_called_once_with("./test_output", exist_ok=True)
            
            # Verify output_file was set
            call_args = mock_task_class.call_args
            assert "output_file" in call_args[1]
    
    @pytest.mark.asyncio
    async def test_create_task_with_output_file_in_config(self, mock_agent, mock_tools_list):
        """Test task creation with output_file in task config"""
        task_config = {
            "description": "Test task",
            "expected_output": "Test output",
            "output_file": "/some/path/output.md"
        }
        
        with patch('src.engines.crewai.helpers.task_helpers.Task') as mock_task_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow, \
             patch('src.services.mcp_service.MCPService') as mock_mcp, \
             patch('os.makedirs') as mock_makedirs:
            
            # Setup UoW to return empty MCP servers
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            mock_mcp_service = AsyncMock()
            mock_mcp.from_unit_of_work.return_value = mock_mcp_service
            mock_mcp_service.get_enabled_servers.return_value = None
            
            mock_task_instance = MagicMock()
            mock_task_class.return_value = mock_task_instance
            
            result = await create_task(
                task_key="test_task",
                task_config=task_config,
                agent=mock_agent,
                tools=mock_tools_list
            )
            
            assert result == mock_task_instance
            mock_makedirs.assert_called_once_with("/some/path", exist_ok=True)
            
            # Verify output_file was set
            call_args = mock_task_class.call_args
            assert call_args[1]["output_file"] == "/some/path/output.md"
    
    @pytest.mark.asyncio
    async def test_create_task_output_file_makedirs_failure(self, mock_agent, mock_tools_list):
        """Test task creation when output directory creation fails"""
        task_config = {
            "description": "Test task",
            "expected_output": "Test output",
            "output_file": "/readonly/output.md"
        }
        
        with patch('src.engines.crewai.helpers.task_helpers.Task') as mock_task_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow, \
             patch('src.services.mcp_service.MCPService') as mock_mcp, \
             patch('os.makedirs') as mock_makedirs:
            
            # Setup UoW to return empty MCP servers
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            mock_mcp_service = AsyncMock()
            mock_mcp.from_unit_of_work.return_value = mock_mcp_service
            mock_mcp_service.get_enabled_servers.return_value = None
            
            # Make makedirs raise an exception
            mock_makedirs.side_effect = OSError("Permission denied")
            
            mock_task_instance = MagicMock()
            mock_task_class.return_value = mock_task_instance
            
            result = await create_task(
                task_key="test_task",
                task_config=task_config,
                agent=mock_agent,
                tools=mock_tools_list
            )
            
            assert result == mock_task_instance
            # Should still continue despite makedirs failure
            call_args = mock_task_class.call_args
            assert call_args[1]["output_file"] == "/readonly/output.md"
    
    @pytest.mark.asyncio
    async def test_create_task_with_markdown_enabled(self, mock_agent, mock_tools_list):
        """Test task creation with markdown enabled"""
        task_config = {
            "description": "Test task",
            "expected_output": "Test output",
            "markdown": True
        }
        
        with patch('src.engines.crewai.helpers.task_helpers.Task') as mock_task_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow, \
             patch('src.services.mcp_service.MCPService') as mock_mcp:
            
            # Setup UoW to return empty MCP servers
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            mock_mcp_service = AsyncMock()
            mock_mcp.from_unit_of_work.return_value = mock_mcp_service
            mock_mcp_service.get_enabled_servers.return_value = None
            
            mock_task_instance = MagicMock()
            mock_task_class.return_value = mock_task_instance
            
            result = await create_task(
                task_key="test_task",
                task_config=task_config,
                agent=mock_agent,
                tools=mock_tools_list
            )
            
            assert result == mock_task_instance
            
            # Verify markdown instructions were added
            call_args = mock_task_class.call_args
            assert "Please format your response using markdown syntax." in call_args[1]["description"]
            assert "Your response should be formatted in markdown." in call_args[1]["expected_output"]
    
    @pytest.mark.asyncio
    async def test_create_task_with_optional_fields(self, mock_agent, mock_tools_list):
        """Test task creation with various optional fields"""
        task_config = {
            "description": "Test task",
            "expected_output": "Test output",
            "async_execution": True,
            "context": ["context1", "context2"],
            "human_input": True,
            "converter_cls": "SomeConverter",
            "output_json": "schema.json"
        }
        
        with patch('src.engines.crewai.helpers.task_helpers.Task') as mock_task_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow, \
             patch('src.services.mcp_service.MCPService') as mock_mcp:
            
            # Setup UoW to return empty MCP servers
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            mock_mcp_service = AsyncMock()
            mock_mcp.from_unit_of_work.return_value = mock_mcp_service
            mock_mcp_service.get_enabled_servers.return_value = None
            
            mock_task_instance = MagicMock()
            mock_task_class.return_value = mock_task_instance
            
            result = await create_task(
                task_key="test_task",
                task_config=task_config,
                agent=mock_agent,
                tools=mock_tools_list
            )
            
            assert result == mock_task_instance
            
            # Verify optional fields were set
            call_args = mock_task_class.call_args
            assert call_args[1]["async_execution"] is True
            assert call_args[1]["context"] == ["context1", "context2"]
            assert call_args[1]["human_input"] is True
            assert call_args[1]["converter_cls"] == "SomeConverter"
            # output_json as string is filtered out by the implementation to avoid CrewAI validation errors
            assert "output_json" not in call_args[1]
    
    @pytest.mark.asyncio
    async def test_create_task_output_json_false_string(self, mock_agent, mock_tools_list):
        """Test task creation with output_json set to string 'false'"""
        task_config = {
            "description": "Test task",
            "expected_output": "Test output",
            "output_json": "false"
        }
        
        with patch('src.engines.crewai.helpers.task_helpers.Task') as mock_task_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow, \
             patch('src.services.mcp_service.MCPService') as mock_mcp:
            
            # Setup UoW to return empty MCP servers
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            mock_mcp_service = AsyncMock()
            mock_mcp.from_unit_of_work.return_value = mock_mcp_service
            mock_mcp_service.get_enabled_servers.return_value = None
            
            mock_task_instance = MagicMock()
            mock_task_class.return_value = mock_task_instance
            
            result = await create_task(
                task_key="test_task",
                task_config=task_config,
                agent=mock_agent,
                tools=mock_tools_list
            )
            
            assert result == mock_task_instance
            
            # Verify output_json was NOT set (filtered out)
            call_args = mock_task_class.call_args
            assert "output_json" not in call_args[1]
    
    @pytest.mark.asyncio
    async def test_create_task_with_mcp_servers_empty_list(self, mock_agent, mock_tools_list):
        """Test task creation with enabled MCP servers but empty list"""
        task_config = {
            "description": "Test task",
            "expected_output": "Test output"
        }
        
        with patch('src.engines.crewai.helpers.task_helpers.Task') as mock_task_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow, \
             patch('src.services.mcp_service.MCPService') as mock_mcp:
            
            # Setup UoW
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            # Setup MCP service with empty servers list
            mock_mcp_service = AsyncMock()
            mock_mcp.from_unit_of_work.return_value = mock_mcp_service
            
            mock_response = MagicMock()
            mock_response.servers = []
            mock_mcp_service.get_enabled_servers.return_value = mock_response
            
            mock_task_instance = MagicMock()
            mock_task_class.return_value = mock_task_instance
            
            result = await create_task(
                task_key="test_task",
                task_config=task_config,
                agent=mock_agent,
                tools=mock_tools_list
            )
            
            assert result == mock_task_instance
    
    @pytest.mark.asyncio
    async def test_create_task_with_guardrail_validation_success(self, mock_agent, mock_tools_list):
        """Test guardrail validation function with successful validation"""
        task_config = {
            "description": "Test task",
            "expected_output": "Test output",
            "guardrail": {"type": "test"}
        }
        
        with patch('src.engines.crewai.helpers.task_helpers.Task') as mock_task_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow, \
             patch('src.services.mcp_service.MCPService') as mock_mcp, \
             patch('src.engines.crewai.guardrails.guardrail_factory.GuardrailFactory') as mock_factory, \
             patch('src.core.logger.LoggerManager') as mock_logger_manager:
            
            # Setup UoW to return empty MCP servers
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            mock_mcp_service = AsyncMock()
            mock_mcp.from_unit_of_work.return_value = mock_mcp_service
            mock_mcp_service.get_enabled_servers.return_value = None
            
            # Setup logger manager
            mock_logger_instance = MagicMock()
            mock_logger_instance._log_dir = "/tmp/logs"
            mock_logger_manager.get_instance.return_value = mock_logger_instance
            
            # Setup guardrail that validates successfully
            mock_guardrail = MagicMock()
            mock_guardrail.validate.return_value = {"valid": True, "feedback": "Good output"}
            mock_factory.create_guardrail.return_value = mock_guardrail
            
            mock_task_instance = MagicMock()
            mock_task_class.return_value = mock_task_instance
            
            with patch('builtins.open', mock_open()) as mock_file:
                result = await create_task(
                    task_key="test_task",
                    task_config=task_config,
                    agent=mock_agent,
                    tools=mock_tools_list
                )
            
            assert result == mock_task_instance
            
            # Test the guardrail validation function
            call_args = mock_task_class.call_args
            guardrail_func = call_args[1]["guardrail"]
            
            # Test successful validation
            validation_result = guardrail_func("test output")
            assert validation_result == (True, "test output")
    
    @pytest.mark.asyncio
    async def test_create_task_with_guardrail_validation_failure(self, mock_agent, mock_tools_list):
        """Test guardrail validation function with failed validation"""
        task_config = {
            "description": "Test task",
            "expected_output": "Test output",
            "guardrail": {"type": "test"}
        }
        
        with patch('src.engines.crewai.helpers.task_helpers.Task') as mock_task_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow, \
             patch('src.services.mcp_service.MCPService') as mock_mcp, \
             patch('src.engines.crewai.guardrails.guardrail_factory.GuardrailFactory') as mock_factory, \
             patch('src.core.logger.LoggerManager') as mock_logger_manager:
            
            # Setup UoW to return empty MCP servers
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            mock_mcp_service = AsyncMock()
            mock_mcp.from_unit_of_work.return_value = mock_mcp_service
            mock_mcp_service.get_enabled_servers.return_value = None
            
            # Setup logger manager
            mock_logger_instance = MagicMock()
            mock_logger_instance._log_dir = "/tmp/logs"
            mock_logger_manager.get_instance.return_value = mock_logger_instance
            
            # Setup guardrail that validates unsuccessfully
            mock_guardrail = MagicMock()
            mock_guardrail.validate.return_value = {"valid": False, "feedback": "Output failed validation"}
            mock_factory.create_guardrail.return_value = mock_guardrail
            
            mock_task_instance = MagicMock()
            mock_task_class.return_value = mock_task_instance
            
            with patch('builtins.open', mock_open()) as mock_file:
                result = await create_task(
                    task_key="test_task",
                    task_config=task_config,
                    agent=mock_agent,
                    tools=mock_tools_list
                )
            
            assert result == mock_task_instance
            
            # Test the guardrail validation function
            call_args = mock_task_class.call_args
            guardrail_func = call_args[1]["guardrail"]
            
            # Test failed validation
            validation_result = guardrail_func("bad output")
            assert validation_result == (False, "Output failed validation")
    
    @pytest.mark.asyncio
    async def test_create_task_with_guardrail_validation_exception(self, mock_agent, mock_tools_list):
        """Test guardrail validation function with exception during validation"""
        task_config = {
            "description": "Test task",
            "expected_output": "Test output",
            "guardrail": {"type": "test"}
        }
        
        with patch('src.engines.crewai.helpers.task_helpers.Task') as mock_task_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow, \
             patch('src.services.mcp_service.MCPService') as mock_mcp, \
             patch('src.engines.crewai.guardrails.guardrail_factory.GuardrailFactory') as mock_factory, \
             patch('src.core.logger.LoggerManager') as mock_logger_manager:
            
            # Setup UoW to return empty MCP servers
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            mock_mcp_service = AsyncMock()
            mock_mcp.from_unit_of_work.return_value = mock_mcp_service
            mock_mcp_service.get_enabled_servers.return_value = None
            
            # Setup logger manager
            mock_logger_instance = MagicMock()
            mock_logger_instance._log_dir = "/tmp/logs"
            mock_logger_manager.get_instance.return_value = mock_logger_instance
            
            # Setup guardrail that raises exception
            mock_guardrail = MagicMock()
            mock_guardrail.validate.side_effect = Exception("Validation error")
            mock_factory.create_guardrail.return_value = mock_guardrail
            
            mock_task_instance = MagicMock()
            mock_task_class.return_value = mock_task_instance
            
            with patch('builtins.open', mock_open()) as mock_file:
                result = await create_task(
                    task_key="test_task",
                    task_config=task_config,
                    agent=mock_agent,
                    tools=mock_tools_list
                )
            
            assert result == mock_task_instance
            
            # Test the guardrail validation function
            call_args = mock_task_class.call_args
            guardrail_func = call_args[1]["guardrail"]
            
            # Test exception during validation
            validation_result = guardrail_func("test output")
            assert validation_result[0] is False
            assert "Validation error: Validation error" in validation_result[1]
    
    @pytest.mark.asyncio
    async def test_create_task_tool_factory_mcp_tuple_success(self, mock_agent, mock_tools_list):
        """Test task creation with tool factory returning MCP tuple"""
        task_config = {
            "description": "Test task",
            "expected_output": "Test output",
            "tools": [1]
        }
        
        mock_tool_service = AsyncMock()
        mock_tool_factory = MagicMock()
        
        with patch('src.engines.crewai.helpers.task_helpers.Task') as mock_task_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow, \
             patch('src.services.mcp_service.MCPService') as mock_mcp, \
             patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve:
            
            # Setup UoW to return empty MCP servers
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            mock_mcp_service = AsyncMock()
            mock_mcp.from_unit_of_work.return_value = mock_mcp_service
            mock_mcp_service.get_enabled_servers.return_value = None
            
            mock_resolve.return_value = ["mcp_tool"]
            
            # Setup tool factory to return MCP tuple with list of tools
            mock_mcp_tool1 = MagicMock()
            mock_mcp_tool2 = MagicMock()
            mock_tool_factory.create_tool.return_value = (True, [mock_mcp_tool1, mock_mcp_tool2])
            
            mock_task_instance = MagicMock()
            mock_task_class.return_value = mock_task_instance
            
            result = await create_task(
                task_key="test_task",
                task_config=task_config,
                agent=mock_agent,
                tools=mock_tools_list,
                tool_service=mock_tool_service,
                tool_factory=mock_tool_factory
            )
            
            assert result == mock_task_instance
            # Verify MCP tools were added
            call_args = mock_task_class.call_args
            task_tools = call_args[1]["tools"]
            # Should have original tools plus 2 MCP tools
            assert len(task_tools) >= 2
    
    @pytest.mark.asyncio
    async def test_create_task_tool_factory_mcp_service_adapter_deprecated(self, mock_agent, mock_tools_list):
        """Test task creation with deprecated mcp_service_adapter"""
        task_config = {
            "description": "Test task",
            "expected_output": "Test output",
            "tools": [1]
        }
        
        mock_tool_service = AsyncMock()
        mock_tool_factory = MagicMock()
        
        with patch('src.engines.crewai.helpers.task_helpers.Task') as mock_task_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow, \
             patch('src.services.mcp_service.MCPService') as mock_mcp, \
             patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve:
            
            # Setup UoW to return empty MCP servers
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            mock_mcp_service = AsyncMock()
            mock_mcp.from_unit_of_work.return_value = mock_mcp_service
            mock_mcp_service.get_enabled_servers.return_value = None
            
            mock_resolve.return_value = ["mcp_tool"]
            
            # Setup tool factory to return deprecated mcp_service_adapter
            mock_tool_factory.create_tool.return_value = (True, 'mcp_service_adapter')
            
            mock_task_instance = MagicMock()
            mock_task_class.return_value = mock_task_instance
            
            result = await create_task(
                task_key="test_task",
                task_config=task_config,
                agent=mock_agent,
                tools=mock_tools_list,
                tool_service=mock_tool_service,
                tool_factory=mock_tool_factory
            )
            
            assert result == mock_task_instance
    
    @pytest.mark.asyncio
    async def test_create_task_tool_factory_mcp_unexpected_format(self, mock_agent, mock_tools_list):
        """Test task creation with unexpected MCP tools format"""
        task_config = {
            "description": "Test task",
            "expected_output": "Test output",
            "tools": [1]
        }
        
        mock_tool_service = AsyncMock()
        mock_tool_factory = MagicMock()
        
        with patch('src.engines.crewai.helpers.task_helpers.Task') as mock_task_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow, \
             patch('src.services.mcp_service.MCPService') as mock_mcp, \
             patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve:
            
            # Setup UoW to return empty MCP servers
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            mock_mcp_service = AsyncMock()
            mock_mcp.from_unit_of_work.return_value = mock_mcp_service
            mock_mcp_service.get_enabled_servers.return_value = None
            
            mock_resolve.return_value = ["mcp_tool"]
            
            # Setup tool factory to return unexpected format
            mock_tool_factory.create_tool.return_value = (True, "unexpected_string")
            
            mock_task_instance = MagicMock()
            mock_task_class.return_value = mock_task_instance
            
            result = await create_task(
                task_key="test_task",
                task_config=task_config,
                agent=mock_agent,
                tools=mock_tools_list,
                tool_service=mock_tool_service,
                tool_factory=mock_tool_factory
            )
            
            assert result == mock_task_instance
    
    @pytest.mark.asyncio
    async def test_create_task_tool_factory_none_result(self, mock_agent, mock_tools_list):
        """Test task creation when tool factory returns None"""
        task_config = {
            "description": "Test task",
            "expected_output": "Test output",
            "tools": [1]
        }
        
        mock_tool_service = AsyncMock()
        mock_tool_factory = MagicMock()
        
        with patch('src.engines.crewai.helpers.task_helpers.Task') as mock_task_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow, \
             patch('src.services.mcp_service.MCPService') as mock_mcp, \
             patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve:
            
            # Setup UoW to return empty MCP servers
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            mock_mcp_service = AsyncMock()
            mock_mcp.from_unit_of_work.return_value = mock_mcp_service
            mock_mcp_service.get_enabled_servers.return_value = None
            
            mock_resolve.return_value = ["test_tool"]
            
            # Setup tool factory to return None
            mock_tool_factory.create_tool.return_value = None
            
            mock_task_instance = MagicMock()
            mock_task_class.return_value = mock_task_instance
            
            result = await create_task(
                task_key="test_task",
                task_config=task_config,
                agent=mock_agent,
                tools=mock_tools_list,
                tool_service=mock_tool_service,
                tool_factory=mock_tool_factory
            )
            
            assert result == mock_task_instance
    
    @pytest.mark.asyncio
    async def test_create_task_tool_factory_exception(self, mock_agent, mock_tools_list):
        """Test task creation when tool factory raises exception"""
        task_config = {
            "description": "Test task",
            "expected_output": "Test output",
            "tools": [1]
        }
        
        mock_tool_service = AsyncMock()
        mock_tool_factory = MagicMock()
        
        with patch('src.engines.crewai.helpers.task_helpers.Task') as mock_task_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow, \
             patch('src.services.mcp_service.MCPService') as mock_mcp, \
             patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve:
            
            # Setup UoW to return empty MCP servers
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            mock_mcp_service = AsyncMock()
            mock_mcp.from_unit_of_work.return_value = mock_mcp_service
            mock_mcp_service.get_enabled_servers.return_value = None
            
            mock_resolve.return_value = ["test_tool"]
            
            # Setup tool factory to raise exception
            mock_tool_factory.create_tool.side_effect = Exception("Tool creation failed")
            
            mock_task_instance = MagicMock()
            mock_task_class.return_value = mock_task_instance
            
            result = await create_task(
                task_key="test_task",
                task_config=task_config,
                agent=mock_agent,
                tools=mock_tools_list,
                tool_service=mock_tool_service,
                tool_factory=mock_tool_factory
            )
            
            assert result == mock_task_instance
    
    @pytest.mark.asyncio
    async def test_create_task_no_tool_factory_with_names(self, mock_agent, mock_tools_list):
        """Test task creation without tool factory but with tool names"""
        task_config = {
            "description": "Test task",
            "expected_output": "Test output",
            "tools": [1]
        }
        
        mock_tool_service = AsyncMock()
        
        with patch('src.engines.crewai.helpers.task_helpers.Task') as mock_task_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow, \
             patch('src.services.mcp_service.MCPService') as mock_mcp, \
             patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve:
            
            # Setup UoW to return empty MCP servers
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            mock_mcp_service = AsyncMock()
            mock_mcp.from_unit_of_work.return_value = mock_mcp_service
            mock_mcp_service.get_enabled_servers.return_value = None
            
            mock_resolve.return_value = ["tool1", "tool2"]
            
            mock_task_instance = MagicMock()
            mock_task_class.return_value = mock_task_instance
            
            result = await create_task(
                task_key="test_task",
                task_config=task_config,
                agent=mock_agent,
                tools=mock_tools_list,
                tool_service=mock_tool_service
                # No tool_factory provided
            )
            
            assert result == mock_task_instance
    
    @pytest.mark.asyncio
    async def test_create_task_tool_resolution_exception(self, mock_agent, mock_tools_list):
        """Test task creation when tool resolution raises exception"""
        task_config = {
            "description": "Test task",
            "expected_output": "Test output",
            "tools": [1]
        }
        
        mock_tool_service = AsyncMock()
        mock_tool_factory = MagicMock()
        
        with patch('src.engines.crewai.helpers.task_helpers.Task') as mock_task_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow, \
             patch('src.services.mcp_service.MCPService') as mock_mcp, \
             patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve:
            
            # Setup UoW to return empty MCP servers
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            mock_mcp_service = AsyncMock()
            mock_mcp.from_unit_of_work.return_value = mock_mcp_service
            mock_mcp_service.get_enabled_servers.return_value = None
            
            # Setup resolve to raise exception
            mock_resolve.side_effect = Exception("Tool resolution failed")
            
            mock_task_instance = MagicMock()
            mock_task_class.return_value = mock_task_instance
            
            result = await create_task(
                task_key="test_task",
                task_config=task_config,
                agent=mock_agent,
                tools=mock_tools_list,
                tool_service=mock_tool_service,
                tool_factory=mock_tool_factory
            )
            
            assert result == mock_task_instance
    
    @pytest.mark.asyncio
    async def test_create_task_mcp_exception(self, mock_agent, mock_tools_list):
        """Test task creation when MCP setup raises exception"""
        task_config = {
            "description": "Test task",
            "expected_output": "Test output"
        }
        
        with patch('src.engines.crewai.helpers.task_helpers.Task') as mock_task_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow, \
             patch('src.services.mcp_service.MCPService') as mock_mcp:
            
            # Setup UoW to raise exception
            mock_uow.side_effect = Exception("MCP setup failed")
            
            mock_task_instance = MagicMock()
            mock_task_class.return_value = mock_task_instance
            
            result = await create_task(
                task_key="test_task",
                task_config=task_config,
                agent=mock_agent,
                tools=mock_tools_list
            )
            
            assert result == mock_task_instance
    
    @pytest.mark.asyncio
    async def test_create_task_with_output_pydantic_model_conversion(self, mock_agent, mock_tools_list):
        """Test task creation with output_pydantic and model conversion"""
        task_config = {
            "description": "Test task",
            "expected_output": "Test output",
            "output_pydantic": "TestModel"
        }
        
        with patch('src.engines.crewai.helpers.task_helpers.Task') as mock_task_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow, \
             patch('src.services.mcp_service.MCPService') as mock_mcp, \
             patch('src.engines.crewai.helpers.task_helpers.get_pydantic_class_from_name') as mock_get_class, \
             patch('src.engines.crewai.helpers.model_conversion_handler.get_compatible_converter_for_model') as mock_get_converter, \
             patch('src.engines.crewai.helpers.model_conversion_handler.configure_output_json_approach') as mock_configure_json:
            
            # Setup UoW to return empty MCP servers
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            mock_mcp_service = AsyncMock()
            mock_mcp.from_unit_of_work.return_value = mock_mcp_service
            mock_mcp_service.get_enabled_servers.return_value = None
            
            # Setup pydantic class
            mock_pydantic_class = MagicMock()
            mock_pydantic_class.__name__ = "TestModel"
            # Mock async function to return a coroutine
            async def mock_get_pydantic_class(name):
                return mock_pydantic_class
            mock_get_class.side_effect = mock_get_pydantic_class
            
            # Setup model conversion to use output_json approach
            mock_converter_cls = MagicMock()
            mock_get_converter.return_value = (mock_converter_cls, mock_pydantic_class, True, True)
            
            mock_configured_args = {"test": "configured"}
            mock_configure_json.return_value = mock_configured_args
            
            mock_task_instance = MagicMock()
            mock_task_class.return_value = mock_task_instance
            
            result = await create_task(
                task_key="test_task",
                task_config=task_config,
                agent=mock_agent,
                tools=mock_tools_list
            )
            
            assert result == mock_task_instance
            mock_configure_json.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_task_with_output_pydantic_custom_converter(self, mock_agent, mock_tools_list):
        """Test task creation with output_pydantic and custom converter"""
        task_config = {
            "description": "Test task",
            "expected_output": "Test output",
            "output_pydantic": "TestModel"
        }
        
        with patch('src.engines.crewai.helpers.task_helpers.Task') as mock_task_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow, \
             patch('src.services.mcp_service.MCPService') as mock_mcp, \
             patch('src.engines.crewai.helpers.task_helpers.get_pydantic_class_from_name') as mock_get_class, \
             patch('src.engines.crewai.helpers.model_conversion_handler.get_compatible_converter_for_model') as mock_get_converter:
            
            # Setup UoW to return empty MCP servers
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            mock_mcp_service = AsyncMock()
            mock_mcp.from_unit_of_work.return_value = mock_mcp_service
            mock_mcp_service.get_enabled_servers.return_value = None
            
            # Setup pydantic class
            mock_pydantic_class = MagicMock()
            mock_pydantic_class.__name__ = "TestModel"
            # Mock async function to return a coroutine
            async def mock_get_pydantic_class(name):
                return mock_pydantic_class
            mock_get_class.side_effect = mock_get_pydantic_class
            
            # Setup model conversion to use custom converter
            mock_converter_cls = MagicMock()
            mock_converter_cls.__name__ = "CustomConverter"
            mock_get_converter.return_value = (mock_converter_cls, mock_pydantic_class, False, True)
            
            mock_task_instance = MagicMock()
            mock_task_class.return_value = mock_task_instance
            
            result = await create_task(
                task_key="test_task",
                task_config=task_config,
                agent=mock_agent,
                tools=mock_tools_list
            )
            
            assert result == mock_task_instance
            call_args = mock_task_class.call_args
            assert call_args[1]["converter_cls"] == mock_converter_cls
            assert call_args[1]["output_pydantic"] == mock_pydantic_class
    
    @pytest.mark.asyncio
    async def test_create_task_with_output_pydantic_incompatible(self, mock_agent, mock_tools_list):
        """Test task creation with output_pydantic but incompatible model"""
        task_config = {
            "description": "Test task",
            "expected_output": "Test output",
            "output_pydantic": "TestModel"
        }
        
        with patch('src.engines.crewai.helpers.task_helpers.Task') as mock_task_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow, \
             patch('src.services.mcp_service.MCPService') as mock_mcp, \
             patch('src.engines.crewai.helpers.task_helpers.get_pydantic_class_from_name') as mock_get_class, \
             patch('src.engines.crewai.helpers.model_conversion_handler.get_compatible_converter_for_model') as mock_get_converter:
            
            # Setup UoW to return empty MCP servers
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            
            mock_mcp_service = AsyncMock()
            mock_mcp.from_unit_of_work.return_value = mock_mcp_service
            mock_mcp_service.get_enabled_servers.return_value = None
            
            # Setup pydantic class
            mock_pydantic_class = MagicMock()
            mock_pydantic_class.__name__ = "TestModel"
            # Mock async function to return a coroutine
            async def mock_get_pydantic_class(name):
                return mock_pydantic_class
            mock_get_class.side_effect = mock_get_pydantic_class
            
            # Setup model conversion to be incompatible
            mock_get_converter.return_value = (None, None, False, False)
            
            mock_task_instance = MagicMock()
            mock_task_class.return_value = mock_task_instance
            
            result = await create_task(
                task_key="test_task",
                task_config=task_config,
                agent=mock_agent,
                tools=mock_tools_list
            )
            
            assert result == mock_task_instance
            call_args = mock_task_class.call_args
            assert call_args[1]["output_pydantic"] == mock_pydantic_class