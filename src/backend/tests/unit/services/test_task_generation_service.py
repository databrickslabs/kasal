"""
Unit tests for TaskGenerationService.

Tests the functionality of AI-powered task generation service including
LLM integration, prompt template usage, and response processing.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call
import json
import os
from typing import Dict, Any, Optional

from src.services.task_generation_service import TaskGenerationService
from src.schemas.task_generation import TaskGenerationRequest, TaskGenerationResponse, Agent, AdvancedConfig
from src.schemas.task import TaskCreate, TaskConfig
from src.services.log_service import LLMLogService
from src.utils.user_context import GroupContext


# Mock data
MOCK_TEMPLATE_CONTENT = """
You are an AI assistant that creates task specifications in JSON format.
Create a task with the following structure:
{
    "name": "Task Name",
    "description": "Detailed task description",
    "expected_output": "What the task should produce",
    "tools": ["tool1", "tool2"],
    "advanced_config": {
        "async_execution": false,
        "context": [],
        "output_json": null,
        "output_pydantic": null,
        "output_file": null,
        "human_input": false,
        "markdown": false,
        "retry_on_fail": true,
        "max_retries": 3,
        "timeout": null,
        "priority": 1,
        "dependencies": [],
        "retry_delay": 0,
        "allow_delegation": false,
        "llm": "model_name"
    }
}
"""

MOCK_LLM_RESPONSE = {
    "choices": [{
        "message": {
            "content": json.dumps({
                "name": "Test Task",
                "description": "A test task for validation",
                "expected_output": "Test results",
                "tools": [{"name": "tool1"}, {"name": "tool2"}],
                "advanced_config": {
                    "async_execution": False,
                    "context": [],
                    "output_json": None,
                    "output_pydantic": None,
                    "output_file": None,
                    "human_input": False,
                    "markdown": False,
                    "retry_on_fail": True,
                    "max_retries": 3,
                    "timeout": None,
                    "priority": 1,
                    "dependencies": [],
                    "retry_delay": 0,
                    "allow_delegation": False,
                    "llm": "test-model"
                }
            })
        }
    }]
}

MOCK_AGENT = Agent(
    name="Test Agent",
    role="Data Analyst",
    goal="Analyze data",
    backstory="An experienced data analyst"
)


@pytest.fixture
def mock_log_service():
    """Create a mock LLM log service."""
    return AsyncMock(spec=LLMLogService)


@pytest.fixture
def task_generation_service(mock_log_service):
    """Create a task generation service with mocked dependencies."""
    return TaskGenerationService(log_service=mock_log_service)


@pytest.fixture
def sample_request():
    """Create a sample task generation request."""
    return TaskGenerationRequest(
        text="Create a task to analyze sales data",
        model="test-model"
    )


@pytest.fixture
def sample_request_with_agent():
    """Create a sample task generation request with agent."""
    return TaskGenerationRequest(
        text="Create a task to analyze sales data",
        model="test-model",
        agent=MOCK_AGENT
    )


@pytest.fixture
def group_context():
    """Create a sample group context."""
    return GroupContext(
        group_ids=["test-group"],
        user_id="test-user"
    )


class TestTaskGenerationService:
    """Test cases for TaskGenerationService."""

    def test_init(self, mock_log_service):
        """Test service initialization."""
        service = TaskGenerationService(log_service=mock_log_service)
        assert service.log_service == mock_log_service

    @patch('src.services.task_generation_service.LLMLogService')
    def test_create_factory_method(self, mock_llm_log_service_class):
        """Test the create factory method."""
        mock_log_service = AsyncMock()
        mock_llm_log_service_class.create.return_value = mock_log_service
        
        service = TaskGenerationService.create()
        
        assert isinstance(service, TaskGenerationService)
        assert service.log_service == mock_log_service
        mock_llm_log_service_class.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_log_llm_interaction_success(self, task_generation_service, group_context):
        """Test successful LLM interaction logging."""
        await task_generation_service._log_llm_interaction(
            endpoint="test-endpoint",
            prompt="test prompt",
            response="test response",
            model="test-model",
            status="success",
            group_context=group_context
        )
        
        task_generation_service.log_service.create_log.assert_called_once_with(
            endpoint="test-endpoint",
            prompt="test prompt",
            response="test response",
            model="test-model",
            status="success",
            error_message=None,
            group_context=group_context
        )

    @pytest.mark.asyncio
    async def test_log_llm_interaction_with_error(self, task_generation_service, group_context):
        """Test LLM interaction logging with error."""
        await task_generation_service._log_llm_interaction(
            endpoint="test-endpoint",
            prompt="test prompt",
            response="test response",
            model="test-model",
            status="error",
            error_message="Test error",
            group_context=group_context
        )
        
        task_generation_service.log_service.create_log.assert_called_once_with(
            endpoint="test-endpoint",
            prompt="test prompt",
            response="test response",
            model="test-model",
            status="error",
            error_message="Test error",
            group_context=group_context
        )

    @pytest.mark.asyncio
    async def test_log_llm_interaction_logging_failure(self, task_generation_service, group_context):
        """Test LLM interaction logging when logging itself fails."""
        # Mock log service to raise an exception
        task_generation_service.log_service.create_log.side_effect = Exception("Log service error")
        
        # This should not raise an exception
        await task_generation_service._log_llm_interaction(
            endpoint="test-endpoint",
            prompt="test prompt",
            response="test response",
            model="test-model",
            group_context=group_context
        )

    @pytest.mark.asyncio
    @patch('src.services.task_generation_service.TemplateService')
    @patch('src.services.task_generation_service.LLMManager')
    @patch('src.services.task_generation_service.litellm')
    async def test_generate_task_success(self, mock_litellm, mock_llm_manager, 
                                       mock_template_service, task_generation_service, 
                                       sample_request):
        """Test successful task generation."""
        # Mock template service
        mock_template_service.get_template_content = AsyncMock(return_value=MOCK_TEMPLATE_CONTENT)
        
        # Mock LLM manager
        mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "test-model"})
        
        # Mock litellm response
        mock_litellm.acompletion = AsyncMock(return_value=MOCK_LLM_RESPONSE)
        
        result = await task_generation_service.generate_task(sample_request)
        
        assert isinstance(result, TaskGenerationResponse)
        assert result.name == "Test Task"
        assert result.description == "A test task for validation"
        assert result.expected_output == "Test results"
        assert result.tools == [{"name": "tool1"}, {"name": "tool2"}]
        
        # Verify template service was called
        mock_template_service.get_template_content.assert_called_once_with("generate_task")
        
        # Verify LLM manager was called
        mock_llm_manager.configure_litellm.assert_called_once_with("test-model")
        
        # Verify litellm was called
        mock_litellm.acompletion.assert_called_once()

    @pytest.mark.asyncio
    @patch('src.services.task_generation_service.TemplateService')
    async def test_generate_task_no_template(self, mock_template_service, 
                                           task_generation_service, sample_request):
        """Test task generation when template is not found."""
        mock_template_service.get_template_content = AsyncMock(return_value=None)
        
        with pytest.raises(ValueError, match="Required prompt template 'generate_task' not found"):
            await task_generation_service.generate_task(sample_request)

    @pytest.mark.asyncio
    @patch('src.services.task_generation_service.TemplateService')
    @patch('src.services.task_generation_service.LLMManager')
    @patch('src.services.task_generation_service.litellm')
    async def test_generate_task_with_agent(self, mock_litellm, mock_llm_manager, 
                                          mock_template_service, task_generation_service, 
                                          sample_request_with_agent):
        """Test task generation with agent context."""
        mock_template_service.get_template_content = AsyncMock(return_value=MOCK_TEMPLATE_CONTENT)
        mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "test-model"})
        mock_litellm.acompletion = AsyncMock(return_value=MOCK_LLM_RESPONSE)
        
        result = await task_generation_service.generate_task(sample_request_with_agent)
        
        assert isinstance(result, TaskGenerationResponse)
        # Verify agent context was added to prompt
        call_args = mock_litellm.acompletion.call_args
        messages = call_args[1]['messages']
        system_message = messages[0]['content']
        assert "Test Agent" in system_message
        assert "Data Analyst" in system_message

    @pytest.mark.asyncio
    @patch('src.services.task_generation_service.TemplateService')
    @patch('src.services.task_generation_service.LLMManager')
    @patch('src.services.task_generation_service.litellm')
    async def test_generate_task_with_default_model(self, mock_litellm, mock_llm_manager, 
                                                  mock_template_service, task_generation_service):
        """Test task generation with default model."""
        mock_template_service.get_template_content = AsyncMock(return_value=MOCK_TEMPLATE_CONTENT)
        mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "default-model"})
        mock_litellm.acompletion = AsyncMock(return_value=MOCK_LLM_RESPONSE)
        
        # Request without model
        request = TaskGenerationRequest(text="Test task")
        
        with patch.dict(os.environ, {'TASK_MODEL': 'env-model'}):
            result = await task_generation_service.generate_task(request)
        
        assert isinstance(result, TaskGenerationResponse)
        mock_llm_manager.configure_litellm.assert_called_once_with("env-model")

    @pytest.mark.asyncio
    @patch('src.services.task_generation_service.TemplateService')
    @patch('src.services.task_generation_service.LLMManager')
    @patch('src.services.task_generation_service.litellm')
    async def test_generate_task_llm_error(self, mock_litellm, mock_llm_manager, 
                                         mock_template_service, task_generation_service, 
                                         sample_request):
        """Test task generation when LLM call fails."""
        mock_template_service.get_template_content = AsyncMock(return_value=MOCK_TEMPLATE_CONTENT)
        mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "test-model"})
        mock_litellm.acompletion = AsyncMock(side_effect=Exception("LLM error"))
        
        with pytest.raises(ValueError, match="Error generating completion: LLM error"):
            await task_generation_service.generate_task(sample_request)

    @pytest.mark.asyncio
    @patch('src.services.task_generation_service.TemplateService')
    @patch('src.services.task_generation_service.LLMManager')
    @patch('src.services.task_generation_service.litellm')
    async def test_generate_task_empty_content(self, mock_litellm, mock_llm_manager, 
                                             mock_template_service, task_generation_service, 
                                             sample_request):
        """Test task generation when LLM returns empty content."""
        mock_template_service.get_template_content = AsyncMock(return_value=MOCK_TEMPLATE_CONTENT)
        mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "test-model"})
        
        # Mock empty response
        empty_response = {
            "choices": [{
                "message": {
                    "content": ""
                }
            }]
        }
        mock_litellm.acompletion = AsyncMock(return_value=empty_response)
        
        with pytest.raises(ValueError, match="Empty content received from LLM"):
            await task_generation_service.generate_task(sample_request)

    @pytest.mark.asyncio
    @patch('src.services.task_generation_service.TemplateService')
    @patch('src.services.task_generation_service.LLMManager')
    @patch('src.services.task_generation_service.litellm')
    async def test_generate_task_with_code_block(self, mock_litellm, mock_llm_manager, 
                                               mock_template_service, task_generation_service, 
                                               sample_request):
        """Test task generation when response contains code blocks."""
        mock_template_service.get_template_content = AsyncMock(return_value=MOCK_TEMPLATE_CONTENT)
        mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "test-model"})
        
        # Mock response with code block
        code_block_response = {
            "choices": [{
                "message": {
                    "content": f"```json\n{json.dumps({'name': 'Test Task', 'description': 'Test', 'expected_output': 'Output', 'tools': []})}\n```"
                }
            }]
        }
        mock_litellm.acompletion = AsyncMock(return_value=code_block_response)
        
        result = await task_generation_service.generate_task(sample_request)
        
        assert isinstance(result, TaskGenerationResponse)
        assert result.name == "Test Task"

    @pytest.mark.asyncio
    @patch('src.services.task_generation_service.TemplateService')
    @patch('src.services.task_generation_service.LLMManager')
    @patch('src.services.task_generation_service.litellm')
    @patch('src.services.task_generation_service.robust_json_parser')
    async def test_generate_task_json_parsing_error(self, mock_parser, mock_litellm, 
                                                  mock_llm_manager, mock_template_service, 
                                                  task_generation_service, sample_request):
        """Test task generation when JSON parsing fails."""
        mock_template_service.get_template_content = AsyncMock(return_value=MOCK_TEMPLATE_CONTENT)
        mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "test-model"})
        mock_litellm.acompletion = AsyncMock(return_value=MOCK_LLM_RESPONSE)
        mock_parser.side_effect = ValueError("JSON parsing failed")
        
        with pytest.raises(ValueError, match="Could not parse response as JSON"):
            await task_generation_service.generate_task(sample_request)

    @pytest.mark.asyncio
    @patch('src.services.task_generation_service.TemplateService')
    @patch('src.services.task_generation_service.LLMManager')
    @patch('src.services.task_generation_service.litellm')
    @patch('src.services.task_generation_service.robust_json_parser')
    async def test_generate_task_missing_required_fields(self, mock_parser, mock_litellm, 
                                                       mock_llm_manager, mock_template_service, 
                                                       task_generation_service, sample_request):
        """Test task generation when required fields are missing."""
        mock_template_service.get_template_content = AsyncMock(return_value=MOCK_TEMPLATE_CONTENT)
        mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "test-model"})
        mock_litellm.acompletion = AsyncMock(return_value=MOCK_LLM_RESPONSE)
        mock_parser.return_value = {"name": "Test Task"}  # Missing required fields
        
        with pytest.raises(ValueError, match="Missing required field"):
            await task_generation_service.generate_task(sample_request)

    @pytest.mark.asyncio
    @patch('src.services.task_generation_service.TemplateService')
    @patch('src.services.task_generation_service.LLMManager')
    @patch('src.services.task_generation_service.litellm')
    @patch('src.services.task_generation_service.robust_json_parser')
    async def test_generate_task_advanced_config_fixes(self, mock_parser, mock_litellm, 
                                                     mock_llm_manager, mock_template_service, 
                                                     task_generation_service, sample_request):
        """Test task generation with advanced config fixes."""
        mock_template_service.get_template_content = AsyncMock(return_value=MOCK_TEMPLATE_CONTENT)
        mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "test-model"})
        mock_litellm.acompletion = AsyncMock(return_value=MOCK_LLM_RESPONSE)
        
        # Mock parsed response with problematic advanced_config
        problematic_setup = {
            "name": "Test Task",
            "description": "Test description",
            "expected_output": "Test output",
            "tools": [],
            "advanced_config": {
                "output_json": True,  # Boolean instead of dict/None
                "output_pydantic": False,  # Boolean instead of string/None
                "context": "not_a_list",  # String instead of list
                "dependencies": "not_a_list"  # String instead of list
            }
        }
        mock_parser.return_value = problematic_setup
        
        result = await task_generation_service.generate_task(sample_request)
        
        assert isinstance(result, TaskGenerationResponse)
        assert result.advanced_config.output_json is None
        assert result.advanced_config.output_pydantic is None
        assert result.advanced_config.context == []
        assert result.advanced_config.dependencies == []

    @pytest.mark.asyncio
    @patch('src.services.task_generation_service.TemplateService')
    @patch('src.services.task_generation_service.LLMManager')
    @patch('src.services.task_generation_service.litellm')
    @patch('src.services.task_generation_service.robust_json_parser')
    async def test_generate_task_with_json_string_output(self, mock_parser, mock_litellm, 
                                                       mock_llm_manager, mock_template_service, 
                                                       task_generation_service, sample_request):
        """Test task generation with JSON string in output_json."""
        mock_template_service.get_template_content = AsyncMock(return_value=MOCK_TEMPLATE_CONTENT)
        mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "test-model"})
        mock_litellm.acompletion = AsyncMock(return_value=MOCK_LLM_RESPONSE)
        
        # Mock parsed response with JSON string in output_json
        setup_with_json_string = {
            "name": "Test Task",
            "description": "Test description",
            "expected_output": "Test output",
            "tools": [],
            "advanced_config": {
                "output_json": '{"key": "value"}'  # JSON string
            }
        }
        mock_parser.return_value = setup_with_json_string
        
        result = await task_generation_service.generate_task(sample_request)
        
        assert isinstance(result, TaskGenerationResponse)
        assert result.advanced_config.output_json == {"key": "value"}

    @pytest.mark.asyncio
    @patch('src.services.task_generation_service.TemplateService')
    @patch('src.services.task_generation_service.LLMManager')
    @patch('src.services.task_generation_service.litellm')
    @patch('src.services.task_generation_service.robust_json_parser')
    async def test_generate_task_with_invalid_json_string(self, mock_parser, mock_litellm, 
                                                        mock_llm_manager, mock_template_service, 
                                                        task_generation_service, sample_request):
        """Test task generation with invalid JSON string in output_json."""
        mock_template_service.get_template_content = AsyncMock(return_value=MOCK_TEMPLATE_CONTENT)
        mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "test-model"})
        mock_litellm.acompletion = AsyncMock(return_value=MOCK_LLM_RESPONSE)
        
        # Mock parsed response with invalid JSON string
        setup_with_invalid_json = {
            "name": "Test Task",
            "description": "Test description",
            "expected_output": "Test output",
            "tools": [],
            "advanced_config": {
                "output_json": "invalid json string"
            }
        }
        mock_parser.return_value = setup_with_invalid_json
        
        result = await task_generation_service.generate_task(sample_request)
        
        assert isinstance(result, TaskGenerationResponse)
        assert result.advanced_config.output_json is None

    @pytest.mark.asyncio
    @patch('src.services.task_generation_service.TemplateService')
    @patch('src.services.task_generation_service.LLMManager')
    @patch('src.services.task_generation_service.litellm')
    @patch('src.services.task_generation_service.robust_json_parser')
    async def test_generate_task_with_markdown_enabled(self, mock_parser, mock_litellm, 
                                                     mock_llm_manager, mock_template_service, 
                                                     task_generation_service, sample_request):
        """Test task generation with markdown enabled."""
        mock_template_service.get_template_content = AsyncMock(return_value=MOCK_TEMPLATE_CONTENT)
        mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "test-model"})
        mock_litellm.acompletion = AsyncMock(return_value=MOCK_LLM_RESPONSE)
        
        # Mock parsed response with markdown enabled
        setup_with_markdown = {
            "name": "Test Task",
            "description": "Test description",
            "expected_output": "Test output",
            "tools": [],
            "advanced_config": {
                "markdown": True
            }
        }
        mock_parser.return_value = setup_with_markdown
        
        result = await task_generation_service.generate_task(sample_request)
        
        assert isinstance(result, TaskGenerationResponse)
        assert "Markdown" in result.description
        assert "Markdown" in result.expected_output

    @pytest.mark.asyncio
    async def test_generate_and_save_task(self, task_generation_service, sample_request, group_context):
        """Test the generate_and_save_task method."""
        # Mock the generate_task method
        mock_response = TaskGenerationResponse(
            name="Test Task",
            description="Test description",
            expected_output="Test output",
            tools=[],
            advanced_config=AdvancedConfig()
        )
        
        with patch.object(task_generation_service, 'generate_task', return_value=mock_response):
            result = await task_generation_service.generate_and_save_task(sample_request, group_context)
        
        assert isinstance(result, dict)
        assert result['name'] == "Test Task"
        assert result['description'] == "Test description"

    @pytest.mark.asyncio
    async def test_generate_and_save_task_logging_error(self, task_generation_service, sample_request, group_context):
        """Test generate_and_save_task when logging fails."""
        mock_response = TaskGenerationResponse(
            name="Test Task",
            description="Test description",
            expected_output="Test output",
            tools=[],
            advanced_config=AdvancedConfig()
        )
        
        # Mock generate_task to succeed but logging to fail
        task_generation_service.log_service.create_log.side_effect = Exception("Logging error")
        
        with patch.object(task_generation_service, 'generate_task', return_value=mock_response):
            result = await task_generation_service.generate_and_save_task(sample_request, group_context)
        
        # Should still return the result despite logging error
        assert isinstance(result, dict)
        assert result['name'] == "Test Task"

    def test_convert_to_task_create(self, task_generation_service):
        """Test conversion from TaskGenerationResponse to TaskCreate."""
        response = TaskGenerationResponse(
            name="Test Task",
            description="Test description",
            expected_output="Test output",
            tools=[{"name": "tool1"}, {"name": "tool2"}],
            advanced_config=AdvancedConfig(
                async_execution=True,
                context=["context1"],
                output_json={"key": "value"},
                output_pydantic="TestModel",
                output_file="output.txt",
                human_input=True,
                markdown=True,
                callback="callback_func"
            )
        )
        
        result = task_generation_service.convert_to_task_create(response)
        
        assert isinstance(result, TaskCreate)
        assert result.name == "Test Task"
        assert result.description == "Test description"
        assert result.expected_output == "Test output"
        assert result.tools == ["tool1", "tool2"]
        assert result.async_execution is True
        assert result.context == ["context1"]
        assert result.output_json == '{"key": "value"}'
        assert result.output_pydantic == "TestModel"
        assert result.output_file == "output.txt"
        assert result.human_input is True
        assert result.markdown is True
        assert result.callback == "callback_func"

    def test_convert_to_task_create_with_none_output_json(self, task_generation_service):
        """Test conversion when output_json is None."""
        response = TaskGenerationResponse(
            name="Test Task",
            description="Test description",
            expected_output="Test output",
            tools=[],
            advanced_config=AdvancedConfig(
                output_json=None
            )
        )
        
        result = task_generation_service.convert_to_task_create(response)
        
        assert isinstance(result, TaskCreate)
        assert result.output_json is None

    @pytest.mark.asyncio
    @patch('src.services.task_generation_service.TemplateService')
    @patch('src.services.task_generation_service.LLMManager')
    @patch('src.services.task_generation_service.litellm')
    @patch('src.services.task_generation_service.robust_json_parser')
    async def test_generate_task_missing_tools(self, mock_parser, mock_litellm, 
                                             mock_llm_manager, mock_template_service, 
                                             task_generation_service, sample_request):
        """Test task generation when tools are missing from response."""
        mock_template_service.get_template_content = AsyncMock(return_value=MOCK_TEMPLATE_CONTENT)
        mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "test-model"})
        mock_litellm.acompletion = AsyncMock(return_value=MOCK_LLM_RESPONSE)
        
        # Mock parsed response without tools
        setup_without_tools = {
            "name": "Test Task",
            "description": "Test description",
            "expected_output": "Test output"
            # Missing tools and advanced_config
        }
        mock_parser.return_value = setup_without_tools
        
        result = await task_generation_service.generate_task(sample_request)
        
        assert isinstance(result, TaskGenerationResponse)
        assert result.tools == []  # Should be set to empty array

    @pytest.mark.asyncio
    async def test_generate_and_save_task_with_logging_exception(self, task_generation_service, sample_request, group_context):
        """Test generate_and_save_task when logging interaction fails."""
        mock_response = TaskGenerationResponse(
            name="Test Task",
            description="Test description",
            expected_output="Test output",
            tools=[],
            advanced_config=AdvancedConfig()
        )
        
        # Mock the generate_task to return a response
        with patch.object(task_generation_service, 'generate_task', return_value=mock_response):
            # Mock the _log_llm_interaction to raise an exception (to cover line 328)
            with patch.object(task_generation_service, '_log_llm_interaction', side_effect=Exception("Logging failed")):
                result = await task_generation_service.generate_and_save_task(sample_request, group_context)
        
        # Should still return the result despite logging error
        assert isinstance(result, dict)
        assert result['name'] == "Test Task"

    def test_convert_to_task_create_with_string_tools(self, task_generation_service):
        """Test conversion with string tools (to cover line 376-377)."""
        # Create a response with dict tools first
        response = TaskGenerationResponse(
            name="Test Task",
            description="Test description",
            expected_output="Test output",
            tools=[{"name": "tool1"}, {"name": "tool2"}],
            advanced_config=AdvancedConfig()
        )
        
        # Manually modify the tools to include strings to test the conversion logic
        response.tools = ["tool1", "tool2"]  # Direct assignment to bypass validation
        
        result = task_generation_service.convert_to_task_create(response)
        
        assert isinstance(result, TaskCreate)
        assert result.tools == ["tool1", "tool2"]

    def test_convert_to_task_create_with_mixed_tools(self, task_generation_service):
        """Test conversion with mixed dict and string tools."""
        # Create a response with dict tools first
        response = TaskGenerationResponse(
            name="Test Task",
            description="Test description",
            expected_output="Test output",
            tools=[{"name": "tool1"}, {"name": "tool2"}],
            advanced_config=AdvancedConfig()
        )
        
        # Manually modify the tools to include mixed format to test the conversion logic
        response.tools = [{"name": "tool1"}, "tool2", {"name": "tool3"}]  # Direct assignment
        
        result = task_generation_service.convert_to_task_create(response)
        
        assert isinstance(result, TaskCreate)
        assert result.tools == ["tool1", "tool2", "tool3"]