"""
Unit tests for TemplateGenerationService.

Tests the functionality of template generation including LLM interaction,
template parsing, error handling, logging, and validation.
"""
import pytest
import json
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.template_generation_service import TemplateGenerationService
from src.schemas.template_generation import TemplateGenerationRequest, TemplateGenerationResponse


class MockLLMResponse:
    """Mock LLM response for testing."""
    
    def __init__(self, content):
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
def template_generation_service(mock_log_service):
    """Create a TemplateGenerationService with mocked dependencies."""
    return TemplateGenerationService(log_service=mock_log_service)


@pytest.fixture
def sample_request():
    """Create a sample TemplateGenerationRequest."""
    return TemplateGenerationRequest(
        role="Data Analyst",
        goal="Analyze customer data to find insights",
        backstory="Expert in data analysis with 5 years experience",
        model="gpt-3.5-turbo"
    )


@pytest.fixture
def sample_valid_templates():
    """Create sample valid template content."""
    return {
        "system_template": "You are a data analyst expert.",
        "prompt_template": "Analyze the following data: {data}",
        "response_template": "Based on my analysis: {analysis}"
    }


class TestTemplateGenerationService:
    """Test cases for TemplateGenerationService."""
    
    def test_template_generation_service_initialization(self, mock_log_service):
        """Test TemplateGenerationService initialization."""
        service = TemplateGenerationService(log_service=mock_log_service)
        
        assert service.log_service == mock_log_service
    
    @patch('src.services.template_generation_service.LLMLogService')
    def test_create_factory_method(self, mock_log_service_class):
        """Test the create factory method."""
        mock_log_instance = MagicMock()
        mock_log_service_class.create.return_value = mock_log_instance
        
        service = TemplateGenerationService.create()
        
        assert isinstance(service, TemplateGenerationService)
        assert service.log_service == mock_log_instance
        mock_log_service_class.create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_log_llm_interaction_success(self, template_generation_service, mock_log_service):
        """Test successful LLM interaction logging."""
        await template_generation_service._log_llm_interaction(
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
            status='success',
            error_message=None
        )
    
    @pytest.mark.asyncio
    async def test_log_llm_interaction_with_error(self, template_generation_service, mock_log_service):
        """Test LLM interaction logging with error status."""
        await template_generation_service._log_llm_interaction(
            endpoint="test-endpoint",
            prompt="test prompt",
            response="error response",
            model="gpt-3.5-turbo",
            status="error",
            error_message="Test error"
        )
        
        mock_log_service.create_log.assert_called_once_with(
            endpoint="test-endpoint",
            prompt="test prompt",
            response="error response",
            model="gpt-3.5-turbo",
            status='error',
            error_message="Test error"
        )
    
    @pytest.mark.asyncio
    async def test_log_llm_interaction_service_failure(self, template_generation_service, mock_log_service):
        """Test LLM interaction logging when log service fails."""
        mock_log_service.create_log.side_effect = Exception("Log service error")
        
        # Should not raise exception, just log error
        await template_generation_service._log_llm_interaction(
            endpoint="test-endpoint",
            prompt="test prompt",
            response="test response",
            model="gpt-3.5-turbo"
        )
        
        mock_log_service.create_log.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_generate_templates_success(self, template_generation_service, mock_log_service, 
                                            sample_request, sample_valid_templates):
        """Test successful template generation with ModelConfigService."""
        # Mock dependencies
        with patch('src.services.template_generation_service.UnitOfWork') as mock_uow, \
             patch('src.services.template_generation_service.ModelConfigService') as mock_model_service, \
             patch('src.services.template_generation_service.TemplateService') as mock_template_service, \
             patch('src.services.template_generation_service.LLMManager') as mock_llm_manager, \
             patch('src.services.template_generation_service.litellm') as mock_litellm, \
             patch('src.services.template_generation_service.robust_json_parser') as mock_parser:
            
            # Setup mocks for ModelConfigService
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            mock_model_service_instance = AsyncMock()
            mock_model_service.from_unit_of_work = AsyncMock(return_value=mock_model_service_instance)
            mock_model_service_instance.get_model_config = AsyncMock(return_value={"name": "gpt-3.5-turbo"})
            
            mock_template_service.get_template_content = AsyncMock(return_value="System template from DB")
            mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "gpt-3.5-turbo"})
            
            mock_response = MockLLMResponse(json.dumps(sample_valid_templates))
            mock_litellm.acompletion = AsyncMock(return_value=mock_response)
            mock_parser.return_value = sample_valid_templates
            
            result = await template_generation_service.generate_templates(sample_request)
            
            assert isinstance(result, TemplateGenerationResponse)
            assert result.system_template == sample_valid_templates["system_template"]
            assert result.prompt_template == sample_valid_templates["prompt_template"]
            assert result.response_template == sample_valid_templates["response_template"]
            
            # Verify ModelConfigService was used correctly
            mock_model_service.from_unit_of_work.assert_called_once_with(mock_uow_instance)
            mock_model_service_instance.get_model_config.assert_called_once_with("gpt-3.5-turbo")
            
            # Verify LLM was called correctly
            mock_litellm.acompletion.assert_called_once()
            call_kwargs = mock_litellm.acompletion.call_args.kwargs
            assert call_kwargs["temperature"] == 0.7
            assert call_kwargs["max_tokens"] == 4000
            assert len(call_kwargs["messages"]) == 2
            
            # Verify logging was called
            mock_log_service.create_log.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_generate_templates_model_not_found(self, template_generation_service, sample_request):
        """Test template generation when model is not found in database."""
        with patch('src.services.template_generation_service.UnitOfWork') as mock_uow, \
             patch('src.services.template_generation_service.ModelConfigService') as mock_model_service:
            
            # Setup mocks for ModelConfigService returning None
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            mock_model_service_instance = AsyncMock()
            mock_model_service.from_unit_of_work = AsyncMock(return_value=mock_model_service_instance)
            mock_model_service_instance.get_model_config = AsyncMock(return_value=None)
            
            with pytest.raises(ValueError, match="Model gpt-3.5-turbo not found in the database"):
                await template_generation_service.generate_templates(sample_request)

    @pytest.mark.asyncio
    async def test_generate_templates_no_prompt_template(self, template_generation_service, sample_request):
        """Test template generation when no prompt template is found."""
        with patch('src.services.template_generation_service.UnitOfWork') as mock_uow, \
             patch('src.services.template_generation_service.ModelConfigService') as mock_model_service, \
             patch('src.services.template_generation_service.TemplateService') as mock_template_service:
            
            # Setup mocks for ModelConfigService
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            mock_model_service_instance = AsyncMock()
            mock_model_service.from_unit_of_work = AsyncMock(return_value=mock_model_service_instance)
            mock_model_service_instance.get_model_config = AsyncMock(return_value={"name": "gpt-3.5-turbo"})
            
            mock_template_service.get_template_content = AsyncMock(return_value=None)
            
            with pytest.raises(ValueError, match="Required prompt template 'generate_templates' not found"):
                await template_generation_service.generate_templates(sample_request)
    
    @pytest.mark.asyncio
    async def test_generate_templates_llm_error(self, template_generation_service, mock_log_service, sample_request):
        """Test template generation when LLM completion fails."""
        with patch('src.services.template_generation_service.UnitOfWork') as mock_uow, \
             patch('src.services.template_generation_service.ModelConfigService') as mock_model_service, \
             patch('src.services.template_generation_service.TemplateService') as mock_template_service, \
             patch('src.services.template_generation_service.LLMManager') as mock_llm_manager, \
             patch('src.services.template_generation_service.litellm') as mock_litellm:
            
            # Setup mocks for ModelConfigService
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            mock_model_service_instance = AsyncMock()
            mock_model_service.from_unit_of_work = AsyncMock(return_value=mock_model_service_instance)
            mock_model_service_instance.get_model_config = AsyncMock(return_value={"name": "gpt-3.5-turbo"})
            
            mock_template_service.get_template_content = AsyncMock(return_value="System template")
            mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "gpt-3.5-turbo"})
            mock_litellm.acompletion = AsyncMock(side_effect=Exception("LLM error"))
            
            with pytest.raises(ValueError, match="Failed to generate templates"):
                await template_generation_service.generate_templates(sample_request)
            
            # Verify error was logged
            assert mock_log_service.create_log.call_count == 1
            call_kwargs = mock_log_service.create_log.call_args.kwargs
            assert call_kwargs["status"] == "error"
    
    @pytest.mark.asyncio
    async def test_generate_templates_json_parse_error(self, template_generation_service, sample_request):
        """Test template generation when JSON parsing fails."""
        with patch('src.services.template_generation_service.UnitOfWork') as mock_uow, \
             patch('src.services.template_generation_service.ModelConfigService') as mock_model_service, \
             patch('src.services.template_generation_service.TemplateService') as mock_template_service, \
             patch('src.services.template_generation_service.LLMManager') as mock_llm_manager, \
             patch('src.services.template_generation_service.litellm') as mock_litellm, \
             patch('src.services.template_generation_service.robust_json_parser') as mock_parser:
            
            # Setup mocks for ModelConfigService
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            mock_model_service_instance = AsyncMock()
            mock_model_service.from_unit_of_work = AsyncMock(return_value=mock_model_service_instance)
            mock_model_service_instance.get_model_config = AsyncMock(return_value={"name": "gpt-3.5-turbo"})
            mock_template_service.get_template_content = AsyncMock(return_value="System template")
            mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "gpt-3.5-turbo"})
            
            mock_response = MockLLMResponse("invalid json")
            mock_litellm.acompletion = AsyncMock(return_value=mock_response)
            mock_parser.side_effect = json.JSONDecodeError("Invalid JSON", "doc", 0)
            
            with pytest.raises(ValueError, match="Failed to parse AI response as JSON"):
                await template_generation_service.generate_templates(sample_request)
    
    @pytest.mark.asyncio
    async def test_generate_templates_missing_fields(self, template_generation_service, sample_request):
        """Test template generation when response is missing required fields."""
        incomplete_templates = {
            "system_template": "Complete system template",
            "prompt_template": "",  # Empty required field
            "response_template": "Complete response template"
        }
        
        with patch('src.services.template_generation_service.UnitOfWork') as mock_uow, \
             patch('src.services.template_generation_service.ModelConfigService') as mock_model_service, \
             patch('src.services.template_generation_service.TemplateService') as mock_template_service, \
             patch('src.services.template_generation_service.LLMManager') as mock_llm_manager, \
             patch('src.services.template_generation_service.litellm') as mock_litellm, \
             patch('src.services.template_generation_service.robust_json_parser') as mock_parser:
            
            # Setup mocks for ModelConfigService
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            mock_model_service_instance = AsyncMock()
            mock_model_service.from_unit_of_work = AsyncMock(return_value=mock_model_service_instance)
            mock_model_service_instance.get_model_config = AsyncMock(return_value={"name": "gpt-3.5-turbo"})
            mock_template_service.get_template_content = AsyncMock(return_value="System template")
            mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "gpt-3.5-turbo"})
            
            mock_response = MockLLMResponse(json.dumps(incomplete_templates))
            mock_litellm.acompletion = AsyncMock(return_value=mock_response)
            mock_parser.return_value = incomplete_templates
            
            with pytest.raises(ValueError, match="Missing or empty required field: prompt_template"):
                await template_generation_service.generate_templates(sample_request)
    
    @pytest.mark.asyncio
    async def test_generate_templates_field_normalization(self, template_generation_service, sample_request):
        """Test template generation with different field name formats."""
        # Test with capitalized field names
        capitalized_templates = {
            "System Template": "System template content",
            "Prompt Template": "Prompt template content", 
            "Response Template": "Response template content"
        }
        
        with patch('src.services.template_generation_service.UnitOfWork') as mock_uow, \
             patch('src.services.template_generation_service.ModelConfigService') as mock_model_service, \
             patch('src.services.template_generation_service.TemplateService') as mock_template_service, \
             patch('src.services.template_generation_service.LLMManager') as mock_llm_manager, \
             patch('src.services.template_generation_service.litellm') as mock_litellm, \
             patch('src.services.template_generation_service.robust_json_parser') as mock_parser:
            
            # Setup mocks for ModelConfigService
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            mock_model_service_instance = AsyncMock()
            mock_model_service.from_unit_of_work = AsyncMock(return_value=mock_model_service_instance)
            mock_model_service_instance.get_model_config = AsyncMock(return_value={"name": "gpt-3.5-turbo"})
            mock_template_service.get_template_content = AsyncMock(return_value="System template")
            mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "gpt-3.5-turbo"})
            
            mock_response = MockLLMResponse(json.dumps(capitalized_templates))
            mock_litellm.acompletion = AsyncMock(return_value=mock_response)
            mock_parser.return_value = capitalized_templates
            
            result = await template_generation_service.generate_templates(sample_request)
            
            assert result.system_template == "System template content"
            assert result.prompt_template == "Prompt template content"
            assert result.response_template == "Response template content"
    
    @pytest.mark.asyncio
    async def test_generate_templates_underscore_field_names(self, template_generation_service, sample_request):
        """Test template generation with underscore field name formats."""
        underscore_templates = {
            "System_Template": "System template content",
            "Prompt_Template": "Prompt template content",
            "Response_Template": "Response template content"
        }
        
        with patch('src.services.template_generation_service.UnitOfWork') as mock_uow, \
             patch('src.services.template_generation_service.ModelConfigService') as mock_model_service, \
             patch('src.services.template_generation_service.TemplateService') as mock_template_service, \
             patch('src.services.template_generation_service.LLMManager') as mock_llm_manager, \
             patch('src.services.template_generation_service.litellm') as mock_litellm, \
             patch('src.services.template_generation_service.robust_json_parser') as mock_parser:
            
            # Setup mocks for ModelConfigService
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            mock_model_service_instance = AsyncMock()
            mock_model_service.from_unit_of_work = AsyncMock(return_value=mock_model_service_instance)
            mock_model_service_instance.get_model_config = AsyncMock(return_value={"name": "gpt-3.5-turbo"})
            mock_template_service.get_template_content = AsyncMock(return_value="System template")
            mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "gpt-3.5-turbo"})
            
            mock_response = MockLLMResponse(json.dumps(underscore_templates))
            mock_litellm.acompletion = AsyncMock(return_value=mock_response)
            mock_parser.return_value = underscore_templates
            
            result = await template_generation_service.generate_templates(sample_request)
            
            assert result.system_template == "System template content"
            assert result.prompt_template == "Prompt template content"
            assert result.response_template == "Response template content"
    
    @pytest.mark.asyncio
    async def test_generate_templates_prompt_construction(self, template_generation_service, sample_request):
        """Test that the user prompt is constructed correctly."""
        with patch('src.services.template_generation_service.UnitOfWork') as mock_uow, \
             patch('src.services.template_generation_service.ModelConfigService') as mock_model_service, \
             patch('src.services.template_generation_service.TemplateService') as mock_template_service, \
             patch('src.services.template_generation_service.LLMManager') as mock_llm_manager, \
             patch('src.services.template_generation_service.litellm') as mock_litellm, \
             patch('src.services.template_generation_service.robust_json_parser') as mock_parser:
            
            # Setup mocks for ModelConfigService
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            mock_model_service_instance = AsyncMock()
            mock_model_service.from_unit_of_work = AsyncMock(return_value=mock_model_service_instance)
            mock_model_service_instance.get_model_config = AsyncMock(return_value={"name": "gpt-3.5-turbo"})
            system_message = "Custom system template"
            mock_template_service.get_template_content = AsyncMock(return_value=system_message)
            mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "gpt-3.5-turbo"})
            
            valid_templates = {"system_template": "sys", "prompt_template": "prompt", "response_template": "resp"}
            mock_response = MockLLMResponse(json.dumps(valid_templates))
            mock_litellm.acompletion = AsyncMock(return_value=mock_response)
            mock_parser.return_value = valid_templates
            
            await template_generation_service.generate_templates(sample_request)
            
            # Verify the messages structure
            call_kwargs = mock_litellm.acompletion.call_args.kwargs
            messages = call_kwargs["messages"]
            
            assert len(messages) == 2
            assert messages[0]["role"] == "system"
            assert messages[0]["content"] == system_message
            assert messages[1]["role"] == "user"
            
            user_content = messages[1]["content"]
            assert "Data Analyst" in user_content
            assert "Analyze customer data to find insights" in user_content
            assert "Expert in data analysis with 5 years experience" in user_content
    
    @pytest.mark.asyncio
    async def test_generate_templates_model_configuration(self, template_generation_service, sample_request):
        """Test different model configurations."""
        with patch('src.services.template_generation_service.UnitOfWork') as mock_uow, \
             patch('src.services.template_generation_service.ModelConfigService') as mock_model_service, \
             patch('src.services.template_generation_service.TemplateService') as mock_template_service, \
             patch('src.services.template_generation_service.LLMManager') as mock_llm_manager, \
             patch('src.services.template_generation_service.litellm') as mock_litellm, \
             patch('src.services.template_generation_service.robust_json_parser') as mock_parser:
            
            # Setup mocks for ModelConfigService
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            mock_model_service_instance = AsyncMock()
            mock_model_service.from_unit_of_work = AsyncMock(return_value=mock_model_service_instance)
            
            # Test with different model configuration
            mock_model_service_instance.get_model_config = AsyncMock(return_value={"name": "gpt-4"})
            mock_template_service.get_template_content = AsyncMock(return_value="System template")
            mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "gpt-4", "api_key": "test"})
            
            valid_templates = {"system_template": "sys", "prompt_template": "prompt", "response_template": "resp"}
            mock_response = MockLLMResponse(json.dumps(valid_templates))
            mock_litellm.acompletion = AsyncMock(return_value=mock_response)
            mock_parser.return_value = valid_templates
            
            result = await template_generation_service.generate_templates(sample_request)
            
            # Verify model config was called with correct model
            mock_model_service_instance.get_model_config.assert_called_with("gpt-3.5-turbo")
            mock_llm_manager.configure_litellm.assert_called_once_with("gpt-4")
            
            assert isinstance(result, TemplateGenerationResponse)
    
    @pytest.mark.asyncio
    @patch('src.services.template_generation_service.logger')
    async def test_logging_throughout_process(self, mock_logger, template_generation_service, sample_request, sample_valid_templates):
        """Test that appropriate logging occurs throughout the process."""
        with patch('src.services.template_generation_service.UnitOfWork') as mock_uow, \
             patch('src.services.template_generation_service.ModelConfigService') as mock_model_service, \
             patch('src.services.template_generation_service.TemplateService') as mock_template_service, \
             patch('src.services.template_generation_service.LLMManager') as mock_llm_manager, \
             patch('src.services.template_generation_service.litellm') as mock_litellm, \
             patch('src.services.template_generation_service.robust_json_parser') as mock_parser:
            
            # Setup mocks for ModelConfigService
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            mock_model_service_instance = AsyncMock()
            mock_model_service.from_unit_of_work = AsyncMock(return_value=mock_model_service_instance)
            mock_model_service_instance.get_model_config = AsyncMock(return_value={"name": "gpt-3.5-turbo"})
            mock_template_service.get_template_content = AsyncMock(return_value="System template")
            mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "gpt-3.5-turbo"})
            
            mock_response = MockLLMResponse(json.dumps(sample_valid_templates))
            mock_litellm.acompletion = AsyncMock(return_value=mock_response)
            mock_parser.return_value = sample_valid_templates
            
            await template_generation_service.generate_templates(sample_request)
            
            # Verify logging calls
            mock_logger.info.assert_any_call("Using model for template generation: gpt-3.5-turbo")
            mock_logger.info.assert_any_call("Using prompt template for generate_templates from database") 
            mock_logger.info.assert_any_call("Generated templates successfully")
    
    @pytest.mark.asyncio
    @patch('src.services.template_generation_service.logger')
    async def test_error_logging(self, mock_logger, template_generation_service, sample_request):
        """Test that errors are properly logged."""
        with patch('src.services.template_generation_service.UnitOfWork') as mock_uow, \
             patch('src.services.template_generation_service.ModelConfigService') as mock_model_service:
            
            # Setup mock to throw exception
            mock_uow.side_effect = Exception("Model config error")
            
            with pytest.raises(Exception):
                await template_generation_service.generate_templates(sample_request)
            
            mock_logger.error.assert_called_with("Error generating templates: Model config error")
    
    def test_service_attributes(self, template_generation_service, mock_log_service):
        """Test that service has correct attributes."""
        assert hasattr(template_generation_service, 'log_service')
        assert template_generation_service.log_service == mock_log_service
    
    @pytest.mark.asyncio
    async def test_multiple_field_name_fallbacks(self, template_generation_service, sample_request):
        """Test field name normalization with mixed formats."""
        mixed_templates = {
            "system_template": "System content",  # Normal format
            "Prompt Template": "Prompt content",  # Space format
            "Response_Template": "Response content"  # Underscore format
        }
        
        with patch('src.services.template_generation_service.UnitOfWork') as mock_uow, \
             patch('src.services.template_generation_service.ModelConfigService') as mock_model_service, \
             patch('src.services.template_generation_service.TemplateService') as mock_template_service, \
             patch('src.services.template_generation_service.LLMManager') as mock_llm_manager, \
             patch('src.services.template_generation_service.litellm') as mock_litellm, \
             patch('src.services.template_generation_service.robust_json_parser') as mock_parser:
            
            # Setup mocks for ModelConfigService
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            mock_model_service_instance = AsyncMock()
            mock_model_service.from_unit_of_work = AsyncMock(return_value=mock_model_service_instance)
            mock_model_service_instance.get_model_config = AsyncMock(return_value={"name": "gpt-3.5-turbo"})
            mock_template_service.get_template_content = AsyncMock(return_value="System template")
            mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "gpt-3.5-turbo"})
            
            mock_response = MockLLMResponse(json.dumps(mixed_templates))
            mock_litellm.acompletion = AsyncMock(return_value=mock_response)
            mock_parser.return_value = mixed_templates
            
            result = await template_generation_service.generate_templates(sample_request)
            
            assert result.system_template == "System content"
            assert result.prompt_template == "Prompt content"
            assert result.response_template == "Response content"