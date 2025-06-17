"""
Unit tests for ExecutionNameService.

Tests the functionality of execution name generation including
LLM-based name generation, error handling, logging, and fallback behavior.
"""
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.execution_name_service import ExecutionNameService  
from src.schemas.execution import ExecutionNameGenerationRequest, ExecutionNameGenerationResponse


class MockLLMResponse:
    """Mock LLM response for testing."""
    
    def __init__(self, content="Data Analysis Task"):
        self.choices = [{"message": {"content": content}}]
    
    def __getitem__(self, key):
        if key == "choices":
            return self.choices
        raise KeyError(key)


@pytest.fixture
def mock_log_service():
    """Create a mock LLMLogService."""
    return AsyncMock()


@pytest.fixture
def execution_name_service(mock_log_service):
    """Create an ExecutionNameService with mocked dependencies."""
    return ExecutionNameService(log_service=mock_log_service)


@pytest.fixture  
def sample_request():
    """Create a sample ExecutionNameGenerationRequest."""
    return ExecutionNameGenerationRequest(
        agents_yaml={
            "agent1": {
                "role": "Data Analyst",
                "goal": "Analyze customer data", 
                "backstory": "Expert in data analysis"
            }
        },
        tasks_yaml={
            "task1": {
                "description": "Analyze customer behavior patterns",
                "agent": "Data Analyst",
                "expected_output": "Analysis report"
            }
        },
        model="gpt-3.5-turbo"
    )


class TestExecutionNameService:
    """Test cases for ExecutionNameService."""
    
    def test_execution_name_service_initialization(self, mock_log_service):
        """Test ExecutionNameService initialization."""
        service = ExecutionNameService(log_service=mock_log_service)
        
        assert service.log_service == mock_log_service
    
    @patch('src.services.execution_name_service.LLMLogService')
    def test_create_factory_method(self, mock_log_service_class):
        """Test the create factory method."""
        mock_log_instance = MagicMock()
        mock_log_service_class.create.return_value = mock_log_instance
        
        service = ExecutionNameService.create()
        
        assert isinstance(service, ExecutionNameService)
        assert service.log_service == mock_log_instance
        mock_log_service_class.create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_log_llm_interaction_success(self, execution_name_service, mock_log_service):
        """Test successful LLM interaction logging."""
        await execution_name_service._log_llm_interaction(
            endpoint="test-endpoint",
            prompt="test prompt",
            response="test response", 
            model="gpt-3.5-turbo"
        )
        
        mock_log_service.create_log.assert_called_once_with(
            endpoint="test-endpoint",
            prompt="test prompt",
            response="test response",
            model="gpt-3.5-turbo",
            status='success'
        )
    
    @pytest.mark.asyncio
    async def test_log_llm_interaction_failure(self, execution_name_service, mock_log_service):
        """Test LLM interaction logging with service failure."""
        mock_log_service.create_log.side_effect = Exception("Database error")
        
        # Should not raise exception, just log error
        await execution_name_service._log_llm_interaction(
            endpoint="test-endpoint",
            prompt="test prompt", 
            response="test response",
            model="gpt-3.5-turbo"
        )
        
        mock_log_service.create_log.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.services.execution_name_service.TemplateService')
    @patch('src.services.execution_name_service.LLMManager')
    @patch('litellm.acompletion')
    async def test_generate_execution_name_success(self, mock_acompletion, mock_llm_manager, 
                                                  mock_template_service, execution_name_service, 
                                                  mock_log_service, sample_request):
        """Test successful execution name generation."""
        # Mock template service
        mock_template_service.get_template_content = AsyncMock(return_value="Generate a descriptive name")
        
        # Mock LLM manager
        mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "gpt-3.5-turbo"})
        
        # Mock LLM response
        mock_response = MockLLMResponse("Customer Data Analysis")
        mock_acompletion.return_value = mock_response
        
        result = await execution_name_service.generate_execution_name(sample_request)
        
        assert isinstance(result, ExecutionNameGenerationResponse)
        assert result.name == "Customer Data Analysis"
        
        # Verify template service was called
        mock_template_service.get_template_content.assert_called_once_with(
            "generate_job_name", 
            "Generate a 2-4 word descriptive name for this execution based on the agents and tasks."
        )
        
        # Verify LLM manager was called
        mock_llm_manager.configure_litellm.assert_called_once_with("gpt-3.5-turbo")
        
        # Verify LLM completion was called
        mock_acompletion.assert_called_once()
        args = mock_acompletion.call_args
        assert args[1]["model"] == "gpt-3.5-turbo"
        assert args[1]["temperature"] == 0.7
        assert args[1]["max_tokens"] == 20
        assert len(args[1]["messages"]) == 2
        
        # Verify logging was attempted
        mock_log_service.create_log.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.services.execution_name_service.TemplateService')
    @patch('src.services.execution_name_service.LLMManager')
    @patch('litellm.acompletion')
    async def test_generate_execution_name_strips_quotes(self, mock_acompletion, mock_llm_manager,
                                                        mock_template_service, execution_name_service, 
                                                        mock_log_service, sample_request):
        """Test that generated names have quotes stripped."""
        # Mock template service
        mock_template_service.get_template_content = AsyncMock(return_value="Generate a descriptive name")
        
        # Mock LLM manager  
        mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "gpt-3.5-turbo"})
        
        # Mock LLM response with quotes
        mock_response = MockLLMResponse('"Data Analysis Task"')
        mock_acompletion.return_value = mock_response
        
        result = await execution_name_service.generate_execution_name(sample_request)
        
        assert result.name == "Data Analysis Task"
    
    @pytest.mark.asyncio
    @patch('src.services.execution_name_service.TemplateService')
    @patch('src.services.execution_name_service.LLMManager')
    @patch('litellm.acompletion')
    async def test_generate_execution_name_strips_single_quotes(self, mock_acompletion, mock_llm_manager,
                                                               mock_template_service, execution_name_service,
                                                               mock_log_service, sample_request):
        """Test that generated names have single quotes stripped."""
        # Mock template service
        mock_template_service.get_template_content = AsyncMock(return_value="Generate a descriptive name")
        
        # Mock LLM manager
        mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "gpt-3.5-turbo"})
        
        # Mock LLM response with single quotes
        mock_response = MockLLMResponse("'Data Analysis Task'")
        mock_acompletion.return_value = mock_response
        
        result = await execution_name_service.generate_execution_name(sample_request)
        
        assert result.name == "Data Analysis Task"
    
    @pytest.mark.asyncio
    @patch('src.services.execution_name_service.TemplateService')
    @patch('src.services.execution_name_service.LLMManager')
    @patch('litellm.acompletion')
    async def test_generate_execution_name_logging_failure_does_not_fail_request(self, mock_acompletion, 
                                                                                mock_llm_manager, mock_template_service,
                                                                                execution_name_service, mock_log_service, 
                                                                                sample_request):
        """Test that logging failure doesn't fail the name generation request."""
        # Mock template service
        mock_template_service.get_template_content = AsyncMock(return_value="Generate a descriptive name")
        
        # Mock LLM manager
        mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "gpt-3.5-turbo"})
        
        # Mock LLM response
        mock_response = MockLLMResponse("Data Analysis Task")
        mock_acompletion.return_value = mock_response
        
        # Mock logging to fail
        mock_log_service.create_log.side_effect = Exception("Logging failed")
        
        # Should still succeed
        result = await execution_name_service.generate_execution_name(sample_request)
        
        assert isinstance(result, ExecutionNameGenerationResponse)
        assert result.name == "Data Analysis Task"
    
    @pytest.mark.asyncio
    @patch('src.services.execution_name_service.TemplateService')
    async def test_generate_execution_name_template_service_failure_fallback(self, mock_template_service,
                                                                            execution_name_service,
                                                                            sample_request):
        """Test fallback behavior when template service fails."""
        # Mock template service to fail
        mock_template_service.get_template_content.side_effect = Exception("Template service failed")
        
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = "20230601_143000"
            
            result = await execution_name_service.generate_execution_name(sample_request)
            
            assert isinstance(result, ExecutionNameGenerationResponse)
            assert result.name == "Execution-20230601_143000"
    
    @pytest.mark.asyncio
    @patch('src.services.execution_name_service.TemplateService')
    @patch('src.services.execution_name_service.LLMManager')
    async def test_generate_execution_name_llm_manager_failure_fallback(self, mock_llm_manager, 
                                                                       mock_template_service,
                                                                       execution_name_service,
                                                                       sample_request):
        """Test fallback behavior when LLM manager fails."""
        # Mock template service
        mock_template_service.get_template_content = AsyncMock(return_value="Generate a descriptive name")
        
        # Mock LLM manager to fail
        mock_llm_manager.configure_litellm.side_effect = Exception("LLM manager failed")
        
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = "20230601_143030"
            
            result = await execution_name_service.generate_execution_name(sample_request)
            
            assert isinstance(result, ExecutionNameGenerationResponse)
            assert result.name == "Execution-20230601_143030"
    
    @pytest.mark.asyncio
    @patch('src.services.execution_name_service.TemplateService')
    @patch('src.services.execution_name_service.LLMManager')
    @patch('litellm.acompletion')
    async def test_generate_execution_name_llm_completion_failure_fallback(self, mock_acompletion, 
                                                                          mock_llm_manager, mock_template_service,
                                                                          execution_name_service, sample_request):
        """Test fallback behavior when LLM completion fails."""
        # Mock template service
        mock_template_service.get_template_content = AsyncMock(return_value="Generate a descriptive name")
        
        # Mock LLM manager
        mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "gpt-3.5-turbo"})
        
        # Mock LLM completion to fail
        mock_acompletion.side_effect = Exception("LLM completion failed")
        
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = "20230601_143100"
            
            result = await execution_name_service.generate_execution_name(sample_request)
            
            assert isinstance(result, ExecutionNameGenerationResponse)
            assert result.name == "Execution-20230601_143100"
    
    @pytest.mark.asyncio
    @patch('src.services.execution_name_service.TemplateService')
    @patch('src.services.execution_name_service.LLMManager')
    @patch('litellm.acompletion')
    async def test_generate_execution_name_prompt_construction(self, mock_acompletion, mock_llm_manager,
                                                              mock_template_service, execution_name_service,
                                                              sample_request):
        """Test that the prompt is constructed correctly."""
        # Mock template service
        system_message = "Custom system message for name generation"
        mock_template_service.get_template_content = AsyncMock(return_value=system_message)
        
        # Mock LLM manager
        mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "gpt-3.5-turbo"})
        
        # Mock LLM response
        mock_response = MockLLMResponse("Test Name")
        mock_acompletion.return_value = mock_response
        
        await execution_name_service.generate_execution_name(sample_request)
        
        # Verify the messages structure
        call_args = mock_acompletion.call_args[1]
        messages = call_args["messages"]
        
        assert len(messages) == 2
        assert messages[0]["role"] == "system"
        assert messages[0]["content"] == system_message
        assert messages[1]["role"] == "user"
        
        # Verify the user prompt contains the YAML content
        user_prompt = messages[1]["content"]
        assert "Agents:" in user_prompt
        assert "Tasks:" in user_prompt
        assert "Data Analyst" in user_prompt
        assert "Analyze customer behavior patterns" in user_prompt
    
    @pytest.mark.asyncio
    async def test_generate_execution_name_different_models(self, execution_name_service, mock_log_service):  
        """Test name generation with different model configurations."""
        with patch('src.services.execution_name_service.TemplateService') as mock_template_service, \
             patch('src.services.execution_name_service.LLMManager') as mock_llm_manager, \
             patch('litellm.acompletion') as mock_acompletion:
            
            # Mock services
            mock_template_service.get_template_content = AsyncMock(return_value="Generate name")
            mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "gpt-4"})
            mock_response = MockLLMResponse("GPT-4 Generated Name")
            mock_acompletion.return_value = mock_response
            
            # Test with different model
            request = ExecutionNameGenerationRequest(
                agents_yaml={},
                tasks_yaml={}, 
                model="gpt-4"
            )
            
            result = await execution_name_service.generate_execution_name(request)
            
            assert result.name == "GPT-4 Generated Name"
            mock_llm_manager.configure_litellm.assert_called_once_with("gpt-4")
    
    @pytest.mark.asyncio
    async def test_generate_execution_name_empty_yaml_content(self, execution_name_service):
        """Test name generation with empty YAML content."""
        with patch('src.services.execution_name_service.TemplateService') as mock_template_service, \
             patch('src.services.execution_name_service.LLMManager') as mock_llm_manager, \
             patch('litellm.acompletion') as mock_acompletion:
            
            # Mock services
            mock_template_service.get_template_content = AsyncMock(return_value="Generate name")
            mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "gpt-3.5-turbo"})
            mock_response = MockLLMResponse("Empty Config Task")
            mock_acompletion.return_value = mock_response
            
            # Test with empty YAML
            request = ExecutionNameGenerationRequest(
                agents_yaml={},
                tasks_yaml={},
                model="gpt-3.5-turbo"
            )
            
            result = await execution_name_service.generate_execution_name(request)
            
            assert result.name == "Empty Config Task"
    
    @pytest.mark.asyncio
    async def test_generate_execution_name_whitespace_handling(self, execution_name_service):
        """Test that whitespace in generated names is handled correctly."""
        with patch('src.services.execution_name_service.TemplateService') as mock_template_service, \
             patch('src.services.execution_name_service.LLMManager') as mock_llm_manager, \
             patch('litellm.acompletion') as mock_acompletion:
            
            # Mock services
            mock_template_service.get_template_content = AsyncMock(return_value="Generate name")
            mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "gpt-3.5-turbo"})
            
            # Mock response with leading/trailing whitespace
            mock_response = MockLLMResponse("  Whitespace Task  ")
            mock_acompletion.return_value = mock_response
            
            request = ExecutionNameGenerationRequest(
                agents_yaml={},
                tasks_yaml={},
                model="gpt-3.5-turbo"
            )
            
            result = await execution_name_service.generate_execution_name(request)
            
            assert result.name == "Whitespace Task"
    
    @pytest.mark.asyncio
    @patch('src.services.execution_name_service.logger')
    async def test_generate_execution_name_error_logging(self, mock_logger, execution_name_service):
        """Test that errors are properly logged."""
        with patch('src.services.execution_name_service.TemplateService') as mock_template_service:
            # Mock template service to fail
            mock_template_service.get_template_content.side_effect = Exception("Test error")
            
            request = ExecutionNameGenerationRequest(
                agents_yaml={},
                tasks_yaml={},
                model="gpt-3.5-turbo"
            )
            
            with patch('datetime.datetime') as mock_datetime:
                mock_datetime.now.return_value.strftime.return_value = "20230601_150000"
                
                result = await execution_name_service.generate_execution_name(request)
                
                # Should log error
                assert mock_logger.error.call_count >= 2  # Error message and traceback
                
                # Should return fallback name
                assert result.name == "Execution-20230601_150000"
    
    def test_service_attributes(self, execution_name_service, mock_log_service):
        """Test that service has correct attributes."""
        assert hasattr(execution_name_service, 'log_service')
        assert execution_name_service.log_service == mock_log_service
    
    @pytest.mark.asyncio
    async def test_log_llm_interaction_endpoint_parameter(self, execution_name_service, mock_log_service):
        """Test _log_llm_interaction with different endpoint values.""" 
        endpoints = ["generate-name", "custom-endpoint", "test-api"]
        
        for endpoint in endpoints:
            await execution_name_service._log_llm_interaction(
                endpoint=endpoint,
                prompt="test prompt",
                response="test response",
                model="gpt-3.5-turbo"
            )
        
        # Should have been called for each endpoint
        assert mock_log_service.create_log.call_count == len(endpoints)
        
        # Check that correct endpoints were used
        calls = mock_log_service.create_log.call_args_list
        for i, endpoint in enumerate(endpoints):
            assert calls[i][1]['endpoint'] == endpoint