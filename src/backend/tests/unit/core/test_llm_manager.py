"""
Comprehensive test suite for src.core.llm_manager module.

Tests both LiteLLMFileLogger and LLMManager classes with full coverage
of all functionality including edge cases and error handling.
"""
import pytest
import os
import sys
import tempfile
import asyncio
import time
import json
import logging
from unittest.mock import patch, MagicMock, AsyncMock, Mock, PropertyMock
from datetime import datetime, timedelta

from src.core.llm_manager import LiteLLMFileLogger, LLMManager
from src.schemas.model_provider import ModelProvider


@pytest.fixture(autouse=True)
def reset_modules_and_circuit_breaker():
    """Reset modules and circuit breaker state."""
    # Clear databricks_auth module
    if 'src.utils.databricks_auth' in sys.modules:
        del sys.modules['src.utils.databricks_auth']
    
    # Reset circuit breaker
    original_failures = LLMManager._embedding_failures.copy()
    LLMManager._embedding_failures.clear()
    
    yield
    
    LLMManager._embedding_failures = original_failures.copy()


@pytest.fixture
def temp_log_file():
    """Create temporary log file for testing."""
    temp_dir = tempfile.mkdtemp()
    log_file = os.path.join(temp_dir, "test_llm.log")
    yield log_file
    import shutil
    shutil.rmtree(temp_dir, ignore_errors=True)


def create_mock_uow():
    """Helper to create properly configured UnitOfWork mock."""
    mock_uow = AsyncMock()
    mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
    mock_uow.__aexit__ = AsyncMock(return_value=None)
    return mock_uow


class AsyncContextResponse:
    def __init__(self, status=200, json_data=None, text_data=None):
        self.status = status
        self._json_data = json_data or {}
        self._text_data = text_data or ""
        
    async def json(self):
        return self._json_data
        
    async def text(self):
        return self._text_data
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class AsyncContextSession:
    def __init__(self, response):
        self.response = response
        
    def post(self, *args, **kwargs):
        return self.response
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class TestComplete100PercentCoverage:
    """Complete test suite for 100% coverage."""
    
    # LiteLLMFileLogger Tests
    def test_logger_initialization_with_custom_path(self, temp_log_file):
        """Test logger initialization with custom file path."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        assert logger.file_path == temp_log_file
        assert logger.logger is not None
        assert logger.logger.name == "litellm_file_logger"
        assert logger.logger.level == logging.DEBUG

    def test_logger_initialization_default_path(self):
        """Test logger initialization with default path."""
        logger = LiteLLMFileLogger()
        assert logger.file_path is not None
        assert logger.logger is not None

    def test_directory_creation(self):
        """Test automatic directory creation - lines 47-49."""
        import tempfile
        import shutil
        
        temp_base = tempfile.mkdtemp()
        try:
            non_existent_dir = os.path.join(temp_base, "nested", "deep", "path")
            log_file = os.path.join(non_existent_dir, "test.log")
            
            assert not os.path.exists(non_existent_dir)
            logger = LiteLLMFileLogger(file_path=log_file)
            assert os.path.exists(non_existent_dir)
            assert logger.file_path == log_file
        finally:
            shutil.rmtree(temp_base, ignore_errors=True)

    def test_log_pre_api_call_normal(self, temp_log_file):
        """Test normal log_pre_api_call operation."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        model = "gpt-3.5-turbo"
        messages = [{"role": "user", "content": "test"}]
        kwargs = {"temperature": 0.7, "max_tokens": 100}
        logger.log_pre_api_call(model, messages, kwargs)

    def test_log_pre_api_call_with_exception(self, temp_log_file):
        """Test log_pre_api_call with exception handling - lines 67-68."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        
        class BadKwargs:
            def items(self):
                raise Exception("Test exception")
        
        logger.log_pre_api_call("test-model", [], BadKwargs())

    def test_log_post_api_call_normal(self, temp_log_file):
        """Test normal log_post_api_call operation."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        kwargs = {"model": "gpt-3.5-turbo"}
        response_obj = {"choices": [{"message": {"content": "test response"}}]}
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        logger.log_post_api_call(kwargs, response_obj, start_time, end_time)

    def test_log_post_api_call_with_no_message_content(self, temp_log_file):
        """Test log_post_api_call with choices that don't have message.content - line 90."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        kwargs = {"model": "test"}
        response_obj = {"choices": [{"index": 0, "finish_reason": "stop"}]}
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        logger.log_post_api_call(kwargs, response_obj, start_time, end_time)

    def test_log_post_api_call_with_response_obj_none(self, temp_log_file):
        """Test log_post_api_call with None response_obj."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        kwargs = {"model": "test"}
        response_obj = None
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        logger.log_post_api_call(kwargs, response_obj, start_time, end_time)

    def test_log_post_api_call_choices_exception(self, temp_log_file):
        """Test log_post_api_call with choices exception - lines 91-92."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        kwargs = {"model": "test"}
        response_obj = {"choices": "not a list"}
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        logger.log_post_api_call(kwargs, response_obj, start_time, end_time)

    def test_log_post_api_call_general_exception(self, temp_log_file):
        """Test log_post_api_call with general exception - lines 93-94."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        
        class BadEndTime:
            def __sub__(self, other):
                raise Exception("Time calculation error")
        
        kwargs = {"model": "test"}
        response_obj = {"choices": []}
        start_time = datetime.now()
        end_time = BadEndTime()
        logger.log_post_api_call(kwargs, response_obj, start_time, end_time)

    def test_log_success_event_normal(self, temp_log_file):
        """Test normal log_success_event operation."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        kwargs = {"model": "gpt-3.5-turbo", "messages": [{"role": "user", "content": "test"}]}
        response_obj = {
            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
            "choices": [{"message": {"content": "test response"}}]
        }
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        
        with patch('litellm.completion_cost', return_value=0.001):
            logger.log_success_event(kwargs, response_obj, start_time, end_time)

    def test_log_success_event_cost_exception(self, temp_log_file):
        """Test log_success_event with cost calculation exception - lines 125-126."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        kwargs = {"model": "gpt-3.5-turbo", "messages": [{"role": "user", "content": "test"}]}
        response_obj = {
            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
            "choices": [{"message": {"content": "test response"}}]
        }
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        
        with patch('litellm.completion_cost', side_effect=Exception("Cost error")):
            logger.log_success_event(kwargs, response_obj, start_time, end_time)

    def test_log_success_event_with_exception(self, temp_log_file):
        """Test log_success_event with exception - lines 127-128."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        kwargs = {"model": "test"}
        response_obj = None
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        logger.log_success_event(kwargs, response_obj, start_time, end_time)

    def test_log_success_event_usage_exception(self, temp_log_file):
        """Test log_success_event with usage exception - lines 129-130."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        
        class BadUsage:
            def __getitem__(self, key):
                raise Exception("Bad usage")
        
        kwargs = {"model": "test"}
        response_obj = {"usage": BadUsage()}
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        logger.log_success_event(kwargs, response_obj, start_time, end_time)

    def test_log_failure_event_normal(self, temp_log_file):
        """Test normal log_failure_event operation."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        kwargs = {"model": "test", "exception": Exception("test error")}
        response_obj = "error"
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        logger.log_failure_event(kwargs, response_obj, start_time, end_time)

    def test_log_failure_event_with_exception(self, temp_log_file):
        """Test log_failure_event with exception - lines 149-150."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        kwargs = {"model": "test"}
        response_obj = None
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        logger.log_failure_event(kwargs, response_obj, start_time, end_time)

    def test_log_failure_event_str_exception(self, temp_log_file):
        """Test failure logging exception - lines 150-152."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        
        class BadException:
            def __str__(self):
                raise Exception("Can't convert to string")
        
        kwargs = {"model": "test", "exception": BadException()}
        response_obj = "error"
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        logger.log_failure_event(kwargs, response_obj, start_time, end_time)

    @pytest.mark.asyncio
    async def test_async_log_pre_api_call(self, temp_log_file):
        """Test async_log_pre_api_call method."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        model = "gpt-4"
        messages = [{"role": "user", "content": "async test"}]
        kwargs = {"temperature": 0.5}
        await logger.async_log_pre_api_call(model, messages, kwargs)

    @pytest.mark.asyncio
    async def test_async_log_pre_api_call_exception(self, temp_log_file):
        """Test async pre API call exception - lines 162-163."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        with patch.object(logger, 'log_pre_api_call', side_effect=Exception("Sync error")):
            await logger.async_log_pre_api_call("model", [], {})

    @pytest.mark.asyncio
    async def test_async_log_post_api_call(self, temp_log_file):
        """Test async_log_post_api_call method."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        kwargs = {"model": "gpt-4"}
        response_obj = {"choices": [{"message": {"content": "async response"}}]}
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=2)
        await logger.async_log_post_api_call(kwargs, response_obj, start_time, end_time)

    @pytest.mark.asyncio
    async def test_async_log_post_api_call_exception(self, temp_log_file):
        """Test async post API call exception - lines 185-189."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        with patch.object(logger, 'log_post_api_call', side_effect=Exception("Sync error")):
            await logger.async_log_post_api_call({}, {}, datetime.now(), datetime.now())

    @pytest.mark.asyncio
    async def test_async_log_success_event(self, temp_log_file):
        """Test async_log_success_event method."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        kwargs = {"model": "gpt-4", "messages": [{"role": "user", "content": "async test"}]}
        response_obj = {
            "usage": {"prompt_tokens": 15, "completion_tokens": 10, "total_tokens": 25},
            "choices": [{"message": {"content": "async success response"}}]
        }
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1.5)
        
        with patch('litellm.completion_cost', return_value=0.002):
            await logger.async_log_success_event(kwargs, response_obj, start_time, end_time)

    @pytest.mark.asyncio
    async def test_async_log_success_event_exception(self, temp_log_file):
        """Test async success event exception - lines 220-225."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        with patch('asyncio.create_task', side_effect=Exception("Task error")):
            await logger.async_log_success_event({}, {}, datetime.now(), datetime.now())

    @pytest.mark.asyncio
    async def test_async_log_failure_event(self, temp_log_file):
        """Test async_log_failure_event method."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        kwargs = {"model": "gpt-4", "exception": Exception("async error")}
        response_obj = "async error"
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        await logger.async_log_failure_event(kwargs, response_obj, start_time, end_time)

    @pytest.mark.asyncio
    async def test_async_log_failure_event_exception(self, temp_log_file):
        """Test async failure event exception - lines 245-247."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        with patch.object(logger, 'log_failure_event', side_effect=Exception("Sync error")):
            await logger.async_log_failure_event({}, {}, datetime.now(), datetime.now())

    # LLMManager Tests
    @pytest.mark.asyncio
    async def test_configure_litellm_openai(self):
        """Test configure_litellm for OpenAI provider."""
        mock_config = {"provider": ModelProvider.OPENAI, "name": "gpt-3.5-turbo"}
        
        with patch('src.core.llm_manager.UnitOfWork'):
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
                    mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                    mock_api_keys.return_value = "test-api-key"
                    
                    result = await LLMManager.configure_litellm("test-model")
                    assert result["model"] == "gpt-3.5-turbo"
                    assert result["api_key"] == "test-api-key"

    @pytest.mark.asyncio
    async def test_configure_litellm_anthropic(self):
        """Test configure_litellm for Anthropic provider."""
        mock_config = {"provider": ModelProvider.ANTHROPIC, "name": "claude-3-sonnet"}
        
        with patch('src.core.llm_manager.UnitOfWork'):
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
                    mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                    mock_api_keys.return_value = "test-anthropic-key"
                    
                    result = await LLMManager.configure_litellm("test-model")
                    assert result["model"] == "claude-3-sonnet"
                    assert result["api_key"] == "test-anthropic-key"

    @pytest.mark.asyncio
    async def test_configure_litellm_deepseek(self):
        """Test configure_litellm for DeepSeek provider."""
        mock_config = {"provider": ModelProvider.DEEPSEEK, "name": "deepseek-chat"}
        
        with patch('src.core.llm_manager.UnitOfWork'):
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
                    mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                    mock_api_keys.return_value = "deepseek-key"
                    
                    result = await LLMManager.configure_litellm("test-model")
                    assert "deepseek/" in result["model"]

    @pytest.mark.asyncio
    async def test_configure_litellm_deepseek_already_prefixed(self):
        """Test DeepSeek when already prefixed - line 313."""
        mock_config = {"provider": ModelProvider.DEEPSEEK, "name": "deepseek/deepseek-chat"}
        
        with patch('src.core.llm_manager.UnitOfWork'):
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
                    mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                    mock_api_keys.return_value = "deepseek-key"
                    
                    result = await LLMManager.configure_litellm("test-model")
                    assert result["model"] == "deepseek/deepseek-chat"

    @pytest.mark.asyncio
    async def test_configure_litellm_ollama(self):
        """Test configure_litellm for Ollama provider."""
        mock_config = {"provider": ModelProvider.OLLAMA, "name": "llama-3-8b"}
        
        with patch('src.core.llm_manager.UnitOfWork'):
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                
                result = await LLMManager.configure_litellm("test-model")
                assert "ollama/" in result["model"]

    @pytest.mark.asyncio
    async def test_configure_litellm_ollama_hyphen_replacement(self):
        """Test configure_litellm for Ollama with hyphen replacement."""
        mock_config = {"provider": ModelProvider.OLLAMA, "name": "llama-3-8b-instruct"}
        
        with patch('src.core.llm_manager.UnitOfWork'):
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                
                result = await LLMManager.configure_litellm("test-model")
                assert "ollama/llama:3:8b:instruct" in result["model"]

    @pytest.mark.asyncio
    async def test_configure_litellm_gemini(self):
        """Test configure_litellm for Gemini provider."""
        mock_config = {"provider": ModelProvider.GEMINI, "name": "gemini-pro"}
        
        with patch('src.core.llm_manager.UnitOfWork'):
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
                    mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                    mock_api_keys.return_value = "gemini-key"
                    
                    result = await LLMManager.configure_litellm("test-model")
                    assert result["model"] == "gemini/gemini-pro"
                    assert result["api_key"] == "gemini-key"

    @pytest.mark.asyncio
    async def test_configure_litellm_unknown_provider(self):
        """Test unknown provider - line 421."""
        mock_config = {"provider": "UNKNOWN", "name": "unknown-model"}
        
        with patch('src.core.llm_manager.UnitOfWork'):
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                
                result = await LLMManager.configure_litellm("test-model")
                assert result["model"] == "unknown-model"

    @pytest.mark.asyncio
    async def test_configure_litellm_databricks_oauth_path(self):
        """Test configure_litellm for Databricks with OAuth authentication."""
        mock_config = {"provider": ModelProvider.DATABRICKS, "name": "databricks-model"}
        
        mock_databricks_auth = MagicMock()
        mock_databricks_auth.is_databricks_apps_environment = MagicMock(return_value=True)
        mock_databricks_auth.setup_environment_variables = MagicMock()
        
        with patch.dict('sys.modules', {'src.utils.databricks_auth': mock_databricks_auth}):
            with patch('src.core.llm_manager.UnitOfWork'):
                with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                    mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                    
                    result = await LLMManager.configure_litellm("test-model")
                    mock_databricks_auth.is_databricks_apps_environment.assert_called_once()
                    mock_databricks_auth.setup_environment_variables.assert_called_once()

    @pytest.mark.asyncio
    async def test_configure_litellm_databricks_pat_path(self):
        """Test configure_litellm for Databricks with PAT authentication."""
        mock_config = {"provider": ModelProvider.DATABRICKS, "name": "databricks-model"}
        
        mock_databricks_auth = MagicMock()
        mock_databricks_auth.is_databricks_apps_environment = MagicMock(return_value=False)
        
        with patch.dict('sys.modules', {'src.utils.databricks_auth': mock_databricks_auth}):
            with patch('src.core.llm_manager.UnitOfWork'):
                with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                    with patch('src.core.llm_manager.ApiKeysService.get_api_key_value') as mock_api_key:
                        mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                        mock_api_key.return_value = "test-pat-token"
                        
                        result = await LLMManager.configure_litellm("test-model")
                        mock_api_key.assert_called_with(key_name="DATABRICKS_TOKEN")
                        assert result["api_key"] == "test-pat-token"

    @pytest.mark.asyncio
    async def test_configure_litellm_databricks_no_token_warning(self):
        """Test configure_litellm for Databricks with no token."""
        mock_config = {"provider": ModelProvider.DATABRICKS, "name": "databricks-model"}
        
        mock_databricks_auth = MagicMock()
        mock_databricks_auth.is_databricks_apps_environment = MagicMock(return_value=False)
        
        with patch.dict('sys.modules', {'src.utils.databricks_auth': mock_databricks_auth}):
            with patch('src.core.llm_manager.UnitOfWork'):
                with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                    with patch('src.core.llm_manager.ApiKeysService.get_api_key_value') as mock_api_key:
                        mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                        mock_api_key.return_value = None
                        
                        with patch('src.core.llm_manager.logger.warning') as mock_warning:
                            result = await LLMManager.configure_litellm("test-model")
                            mock_warning.assert_called_with("No Databricks token found and not in Databricks Apps environment")

    @pytest.mark.asyncio
    async def test_line_357_databricks_api_key_fallback(self):
        """Test line 357 - DATABRICKS_API_KEY fallback."""
        if 'src.utils.databricks_auth' in sys.modules:
            del sys.modules['src.utils.databricks_auth']
        
        fake_module = type(sys)('fake_databricks_auth')
        sys.modules['src.utils.databricks_auth'] = fake_module
        
        def raise_import_error(*args, **kwargs):
            raise ImportError("Module not available")
        
        fake_module.__getattr__ = raise_import_error
        
        try:
            mock_config = {"provider": ModelProvider.DATABRICKS, "name": "databricks-model"}
            
            api_key_calls = []
            
            async def mock_get_api_key_value(key_name):
                api_key_calls.append(key_name)
                if key_name == "DATABRICKS_TOKEN":
                    return None
                elif key_name == "DATABRICKS_API_KEY":
                    return "LINE_357_TOKEN"
                return None
            
            with patch('src.core.llm_manager.UnitOfWork') as mock_uow:
                with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                    with patch('src.core.llm_manager.ApiKeysService.get_api_key_value', side_effect=mock_get_api_key_value):
                        mock_uow.return_value = create_mock_uow()
                        mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                        
                        result = await LLMManager.configure_litellm("test-model")
                        
                        assert "DATABRICKS_TOKEN" in api_key_calls
                        assert "DATABRICKS_API_KEY" in api_key_calls
                        assert result["api_key"] == "LINE_357_TOKEN"
        finally:
            if 'src.utils.databricks_auth' in sys.modules:
                del sys.modules['src.utils.databricks_auth']

    @pytest.mark.asyncio
    async def test_configure_litellm_databricks_workspace_url_handling(self):
        """Test Databricks workspace URL handling."""
        if 'src.utils.databricks_auth' in sys.modules:
            del sys.modules['src.utils.databricks_auth']
        
        mock_config = {"provider": ModelProvider.DATABRICKS, "name": "databricks-model"}
        
        with patch('src.core.llm_manager.UnitOfWork'):
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                with patch('src.core.llm_manager.ApiKeysService.get_api_key_value') as mock_api_key:
                    mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                    mock_api_key.return_value = "test-token"
                    
                    with patch.dict(os.environ, {'DATABRICKS_HOST': 'test-workspace.databricks.com'}):
                        result = await LLMManager.configure_litellm("test-model")
                        assert result["api_base"] == "https://test-workspace.databricks.com/serving-endpoints"

    @pytest.mark.asyncio
    async def test_configure_litellm_databricks_workspace_url_with_https(self):
        """Test Databricks workspace URL when already has https://."""
        if 'src.utils.databricks_auth' in sys.modules:
            del sys.modules['src.utils.databricks_auth']
        
        mock_config = {"provider": ModelProvider.DATABRICKS, "name": "databricks-model"}
        
        with patch('src.core.llm_manager.UnitOfWork'):
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                with patch('src.core.llm_manager.ApiKeysService.get_api_key_value') as mock_api_key:
                    mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                    mock_api_key.return_value = "test-token"
                    
                    with patch.dict(os.environ, {'DATABRICKS_HOST': 'https://test-workspace.databricks.com'}):
                        result = await LLMManager.configure_litellm("test-model")
                        assert result["api_base"] == "https://test-workspace.databricks.com/serving-endpoints"

    @pytest.mark.asyncio
    async def test_configure_litellm_databricks_database_config(self):
        """Test Databricks with database configuration."""
        if 'src.utils.databricks_auth' in sys.modules:
            del sys.modules['src.utils.databricks_auth']
        
        mock_config = {"provider": ModelProvider.DATABRICKS, "name": "databricks-model"}
        
        with patch('src.core.llm_manager.UnitOfWork') as mock_uow:
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                with patch('src.core.llm_manager.ApiKeysService.get_api_key_value') as mock_api_key:
                    with patch('src.services.databricks_service.DatabricksService.from_unit_of_work') as mock_db_service:
                        mock_uow.return_value = create_mock_uow()
                        mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                        mock_api_key.return_value = "test-token"
                        
                        mock_databricks_config = MagicMock()
                        mock_databricks_config.workspace_url = "https://db-workspace.databricks.com"
                        mock_db_service.return_value.get_databricks_config = AsyncMock(return_value=mock_databricks_config)
                        
                        with patch.dict(os.environ, {}, clear=True):
                            result = await LLMManager.configure_litellm("test-model")
                            assert result["api_base"] == "https://db-workspace.databricks.com/serving-endpoints"

    @pytest.mark.asyncio
    async def test_configure_litellm_databricks_database_none(self):
        """Test Databricks with None database config - line 383."""
        if 'src.utils.databricks_auth' in sys.modules:
            del sys.modules['src.utils.databricks_auth']
        
        mock_config = {"provider": ModelProvider.DATABRICKS, "name": "databricks-model"}
        
        with patch('src.core.llm_manager.UnitOfWork') as mock_uow:
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                with patch('src.core.llm_manager.ApiKeysService.get_api_key_value') as mock_api_key:
                    with patch('src.services.databricks_service.DatabricksService.from_unit_of_work') as mock_db_service:
                        mock_uow.return_value = create_mock_uow()
                        mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                        mock_api_key.return_value = "test-token"
                        mock_db_service.return_value.get_databricks_config = AsyncMock(return_value=None)
                        
                        with patch.dict(os.environ, {}, clear=True):
                            result = await LLMManager.configure_litellm("test-model")
                            # Should continue without error

    @pytest.mark.asyncio
    async def test_configure_litellm_databricks_database_exception(self):
        """Test Databricks with database exception - lines 388-395."""
        if 'src.utils.databricks_auth' in sys.modules:
            del sys.modules['src.utils.databricks_auth']
        
        mock_config = {"provider": ModelProvider.DATABRICKS, "name": "databricks-model"}
        
        with patch('src.core.llm_manager.UnitOfWork') as mock_uow:
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                with patch('src.core.llm_manager.ApiKeysService.get_api_key_value') as mock_api_key:
                    with patch('src.services.databricks_service.DatabricksService.from_unit_of_work') as mock_db_service:
                        mock_uow.return_value = create_mock_uow()
                        mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                        mock_api_key.return_value = "test-token"
                        mock_db_service.return_value.get_databricks_config = AsyncMock(side_effect=Exception("DB Error"))
                        
                        with patch.dict(os.environ, {}, clear=True):
                            with patch('src.core.llm_manager.logger.error') as mock_error:
                                result = await LLMManager.configure_litellm("test-model")
                                mock_error.assert_called()

    @pytest.mark.asyncio
    async def test_configure_litellm_model_not_found(self):
        """Test model not found exception - lines 591-592."""
        with patch('src.core.llm_manager.UnitOfWork') as mock_uow:
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                mock_uow.return_value = create_mock_uow()
                mock_service.return_value.get_model_config = AsyncMock(return_value=None)
                
                with pytest.raises(ValueError, match="Model unknown-model not found in the database"):
                    await LLMManager.configure_litellm("unknown-model")

    # Configure CrewAI LLM Tests
    @pytest.mark.asyncio
    async def test_configure_crewai_llm_openai(self):
        """Test configure_crewai_llm for OpenAI."""
        mock_config = {"provider": ModelProvider.OPENAI, "name": "gpt-4"}
        
        with patch('src.core.llm_manager.UnitOfWork'):
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
                    with patch('crewai.LLM') as mock_llm_class:
                        mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                        mock_api_keys.return_value = "test-key"
                        mock_llm_instance = MagicMock()
                        mock_llm_class.return_value = mock_llm_instance
                        
                        result = await LLMManager.configure_crewai_llm("test-model")
                        assert result is not None

    @pytest.mark.asyncio
    async def test_configure_crewai_llm_databricks_oauth(self):
        """Test configure_crewai_llm Databricks OAuth - lines 485-488."""
        mock_config = {"provider": ModelProvider.DATABRICKS, "name": "databricks-model"}
        
        mock_databricks_auth = MagicMock()
        mock_databricks_auth.is_databricks_apps_environment = MagicMock(return_value=True)
        mock_databricks_auth.setup_environment_variables = MagicMock()
        
        with patch.dict('sys.modules', {'src.utils.databricks_auth': mock_databricks_auth}):
            with patch('src.core.llm_manager.UnitOfWork') as mock_uow:
                with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                    with patch('crewai.LLM') as mock_llm_class:
                        mock_uow.return_value = create_mock_uow()
                        mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                        mock_llm_class.return_value = MagicMock()
                        
                        result = await LLMManager.configure_crewai_llm("test-model")
                        mock_databricks_auth.is_databricks_apps_environment.assert_called_once()
                        mock_databricks_auth.setup_environment_variables.assert_called_once()

    @pytest.mark.asyncio
    async def test_lines_493_495_crewai_import_error(self):
        """Test lines 493-495 - ImportError in configure_crewai_llm."""
        if 'src.utils.databricks_auth' in sys.modules:
            del sys.modules['src.utils.databricks_auth']
        
        fake_module = type(sys)('fake_databricks_auth')
        sys.modules['src.utils.databricks_auth'] = fake_module
        
        def raise_import_error(*args, **kwargs):
            raise ImportError("Module not available")
        
        fake_module.__getattr__ = raise_import_error
        
        try:
            mock_config = {"provider": ModelProvider.DATABRICKS, "name": "databricks-model"}
            
            api_calls = []
            
            async def mock_get_provider_api_key(provider):
                api_calls.append(provider)
                return "LINE_495_TOKEN"
            
            with patch('src.core.llm_manager.UnitOfWork') as mock_uow:
                with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                    with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key', side_effect=mock_get_provider_api_key):
                        with patch('crewai.LLM') as mock_llm_class:
                            mock_uow.return_value = create_mock_uow()
                            mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                            mock_llm_class.return_value = MagicMock()
                            
                            with patch.dict(os.environ, {'DATABRICKS_HOST': 'test-workspace.databricks.com'}):
                                result = await LLMManager.configure_crewai_llm("test-model")
                                
                                assert "DATABRICKS" in api_calls
                                assert result is not None
        finally:
            if 'src.utils.databricks_auth' in sys.modules:
                del sys.modules['src.utils.databricks_auth']

    @pytest.mark.asyncio
    async def test_configure_crewai_llm_databricks_database_config(self):
        """Test Databricks database config in configure_crewai_llm - lines 507-523."""
        mock_config = {"provider": ModelProvider.DATABRICKS, "name": "databricks-model"}
        
        mock_databricks_auth = MagicMock()
        mock_databricks_auth.is_databricks_apps_environment = MagicMock(return_value=False)
        
        with patch.dict('sys.modules', {'src.utils.databricks_auth': mock_databricks_auth}):
            with patch('src.core.llm_manager.UnitOfWork') as mock_uow:
                with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                    with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_key:
                        with patch('src.services.databricks_service.DatabricksService.from_unit_of_work') as mock_db_service:
                            with patch('crewai.LLM') as mock_llm_class:
                                mock_uow.return_value = create_mock_uow()
                                mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                                mock_api_key.return_value = "test-token"
                                
                                mock_db_config = MagicMock()
                                mock_db_config.workspace_url = "https://db-workspace.databricks.com"
                                mock_db_service.return_value.get_databricks_config = AsyncMock(return_value=mock_db_config)
                                mock_llm_class.return_value = MagicMock()
                                
                                with patch.dict(os.environ, {}, clear=True):
                                    result = await LLMManager.configure_crewai_llm("test-model")
                                    assert result is not None

    @pytest.mark.asyncio
    async def test_configure_crewai_llm_databricks_database_none(self):
        """Test Databricks database returns None in CrewAI - line 519."""
        mock_config = {"provider": ModelProvider.DATABRICKS, "name": "databricks-model"}
        
        mock_databricks_auth = MagicMock()
        mock_databricks_auth.is_databricks_apps_environment = MagicMock(return_value=False)
        
        with patch.dict('sys.modules', {'src.utils.databricks_auth': mock_databricks_auth}):
            with patch('src.core.llm_manager.UnitOfWork') as mock_uow:
                with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                    with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_key:
                        with patch('src.services.databricks_service.DatabricksService.from_unit_of_work') as mock_db_service:
                            with patch('crewai.LLM') as mock_llm_class:
                                mock_uow.return_value = create_mock_uow()
                                mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                                mock_api_key.return_value = "test-token"
                                mock_db_service.return_value.get_databricks_config = AsyncMock(return_value=None)
                                mock_llm_class.return_value = MagicMock()
                                
                                with patch.dict(os.environ, {}, clear=True):
                                    result = await LLMManager.configure_crewai_llm("test-model")
                                    assert result is not None

    @pytest.mark.asyncio
    async def test_configure_crewai_llm_deepseek_already_prefixed(self):
        """Test DeepSeek already prefixed in CrewAI - line 554."""
        mock_config = {"provider": ModelProvider.DEEPSEEK, "name": "deepseek/deepseek-chat"}
        
        with patch('src.core.llm_manager.UnitOfWork'):
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
                    with patch('crewai.LLM') as mock_llm_class:
                        mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                        mock_api_keys.return_value = "test-key"
                        mock_llm_class.return_value = MagicMock()
                        
                        result = await LLMManager.configure_crewai_llm("test-model")
                        assert result is not None

    @pytest.mark.asyncio
    async def test_configure_crewai_llm_ollama_hyphen(self):
        """Test Ollama hyphen replacement in CrewAI - lines 559-560."""
        mock_config = {"provider": ModelProvider.OLLAMA, "name": "llama-3-8b-instruct"}
        
        with patch('src.core.llm_manager.UnitOfWork'):
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                with patch('crewai.LLM') as mock_llm_class:
                    mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                    mock_llm_class.return_value = MagicMock()
                    
                    result = await LLMManager.configure_crewai_llm("test-model")
                    assert result is not None

    @pytest.mark.asyncio
    async def test_configure_crewai_llm_model_not_found(self):
        """Test model not found in configure_crewai_llm."""
        with patch('src.core.llm_manager.UnitOfWork') as mock_uow:
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                mock_uow.return_value = create_mock_uow()
                mock_service.return_value.get_model_config = AsyncMock(return_value=None)
                
                with pytest.raises(ValueError, match="Model test-model not found in the database"):
                    await LLMManager.configure_crewai_llm("test-model")

    # Embedding Tests
    @pytest.mark.asyncio
    async def test_get_embedding_openai_success(self):
        """Test get_embedding with OpenAI provider success."""
        with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
            with patch('os.environ.get', return_value=None):
                with patch('litellm.aembedding') as mock_embedding:
                    mock_api_keys.return_value = "test-openai-key"
                    
                    mock_response = {
                        "data": [{"embedding": [0.1, 0.2, 0.3]}]
                    }
                    mock_embedding.return_value = mock_response
                    
                    result = await LLMManager.get_embedding("test text")
                    assert result == [0.1, 0.2, 0.3]

    @pytest.mark.asyncio
    async def test_get_embedding_with_custom_model(self):
        """Test embedding with custom model - lines 647-651."""
        with patch('src.core.llm_manager.UnitOfWork') as mock_uow:
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
                    with patch('os.environ.get', return_value=None):
                        with patch('litellm.aembedding') as mock_embedding:
                            mock_uow.return_value = create_mock_uow()
                            mock_service.return_value.get_model_config = AsyncMock(return_value={
                                "provider": ModelProvider.OPENAI,
                                "name": "text-embedding-3-small"
                            })
                            mock_api_keys.return_value = "test-key"
                            
                            mock_response = {
                                "data": [{"embedding": [0.1, 0.2, 0.3]}]
                            }
                            mock_embedding.return_value = mock_response
                            
                            result = await LLMManager.get_embedding("test text", "custom-model")
                            assert result == [0.1, 0.2, 0.3]

    @pytest.mark.asyncio
    async def test_get_embedding_databricks_oauth(self):
        """Test Databricks embedding with OAuth - lines 658-660, 667, 681."""
        embedder_config = {"provider": "databricks", "config": {"model": "test-embedding"}}
        
        mock_databricks_auth = MagicMock()
        mock_databricks_auth.is_databricks_apps_environment = MagicMock(return_value=True)
        mock_databricks_auth.get_databricks_auth_headers = AsyncMock(return_value=({"Authorization": "Bearer oauth-token"}, None))
        
        with patch.dict('sys.modules', {'src.utils.databricks_auth': mock_databricks_auth}):
            success_response = AsyncContextResponse(status=200, json_data={"data": [{"embedding": [0.1, 0.2]}]})
            session = AsyncContextSession(success_response)
            
            with patch("aiohttp.ClientSession") as mock_session_class:
                mock_session_class.return_value = session
                
                with patch.dict(os.environ, {"DATABRICKS_HOST": "https://workspace.databricks.com"}):
                    result = await LLMManager.get_embedding("test text", embedder_config=embedder_config)
                    assert result == [0.1, 0.2]
                    mock_databricks_auth.is_databricks_apps_environment.assert_called()
                    mock_databricks_auth.get_databricks_auth_headers.assert_called()

    @pytest.mark.asyncio
    async def test_get_embedding_databricks_workspace_url_formatting(self):
        """Test Databricks embedding workspace URL formatting - lines 705-707."""
        embedder_config = {"provider": "databricks", "config": {"model": "test-embedding"}}
        
        with patch("src.core.llm_manager.ApiKeysService.get_provider_api_key") as mock_api_keys:
            with patch("aiohttp.ClientSession") as mock_session_class:
                mock_api_keys.return_value = "test-token"
                
                success_response = AsyncContextResponse(status=200, json_data={"data": [{"embedding": [0.3, 0.4]}]})
                session = AsyncContextSession(success_response)
                mock_session_class.return_value = session
                
                if 'src.utils.databricks_auth' in sys.modules:
                    del sys.modules['src.utils.databricks_auth']
                
                with patch.dict(os.environ, {"DATABRICKS_HOST": "workspace.databricks.com"}):
                    result = await LLMManager.get_embedding("test text", embedder_config=embedder_config)
                    assert result == [0.3, 0.4]

    @pytest.mark.asyncio
    async def test_get_embedding_databricks_success(self):
        """Test Databricks embedding success - lines 728-729."""
        success_response = AsyncContextResponse(
            status=200,
            json_data={"data": [{"embedding": [0.7, 0.8, 0.9]}]}
        )
        
        session = AsyncContextSession(success_response)
        embedder_config = {"provider": "databricks", "config": {"model": "test-embedding-model"}}
        
        with patch("src.core.llm_manager.ApiKeysService.get_provider_api_key") as mock_api_keys:
            with patch("aiohttp.ClientSession") as mock_session_class:
                mock_api_keys.return_value = "databricks-token"
                mock_session_class.return_value = session
                
                if 'src.utils.databricks_auth' in sys.modules:
                    del sys.modules['src.utils.databricks_auth']
                
                with patch.dict(os.environ, {"DATABRICKS_HOST": "https://workspace.databricks.com"}):
                    result = await LLMManager.get_embedding("test text", embedder_config=embedder_config)
                    assert result == [0.7, 0.8, 0.9]

    @pytest.mark.asyncio
    async def test_get_embedding_databricks_error_response(self):
        """Test Databricks embedding error response - lines 731-733."""
        error_response = AsyncContextResponse(
            status=400,
            text_data="ERROR TEXT FOR LINES 731-733"
        )
        
        session = AsyncContextSession(error_response)
        embedder_config = {"provider": "databricks", "config": {"model": "test-embedding-model"}}
        
        with patch("src.core.llm_manager.ApiKeysService.get_provider_api_key") as mock_api_keys:
            with patch("aiohttp.ClientSession") as mock_session_class:
                mock_api_keys.return_value = "databricks-token"
                mock_session_class.return_value = session
                
                if 'src.utils.databricks_auth' in sys.modules:
                    del sys.modules['src.utils.databricks_auth']
                
                with patch.dict(os.environ, {"DATABRICKS_HOST": "https://workspace.databricks.com"}):
                    with patch('src.core.llm_manager.logger.error') as mock_error:
                        result = await LLMManager.get_embedding("test text", embedder_config=embedder_config)
                        assert result is None
                        mock_error.assert_called()

    @pytest.mark.asyncio
    async def test_get_embedding_litellm_exception(self):
        """Test litellm.embedding exception - lines 735-737."""
        with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
            with patch('litellm.aembedding') as mock_embedding:
                mock_api_keys.return_value = "test-key"
                mock_embedding.side_effect = Exception("Embedding API error")
                
                result = await LLMManager.get_embedding("test text")
                assert result is None

    @pytest.mark.asyncio
    async def test_get_embedding_databricks_request_exception(self):
        """Test Databricks embedding request exception - lines 741-747."""
        embedder_config = {"provider": "databricks", "config": {"model": "test-embedding-model"}}
        
        with patch("src.core.llm_manager.ApiKeysService.get_provider_api_key") as mock_api_keys:
            with patch("aiohttp.ClientSession") as mock_session_class:
                mock_api_keys.return_value = "test-token"
                mock_session_class.side_effect = Exception("Connection error")
                
                if 'src.utils.databricks_auth' in sys.modules:
                    del sys.modules['src.utils.databricks_auth']
                
                with patch.dict(os.environ, {"DATABRICKS_HOST": "https://workspace.databricks.com"}):
                    result = await LLMManager.get_embedding("test text", embedder_config=embedder_config)
                    assert result is None

    @pytest.mark.asyncio
    async def test_get_embedding_unknown_provider(self):
        """Test unknown provider - lines 755-765."""
        unknown_config = {"provider": "unknown", "config": {"model": "unknown-model"}}
        
        with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
            mock_api_keys.return_value = None  # No API key
            
            with patch('src.core.llm_manager.logger.warning') as mock_warning:
                result = await LLMManager.get_embedding("test text", embedder_config=unknown_config)
                assert result is None
                mock_warning.assert_called_with("No OpenAI API key found for creating embeddings")

    @pytest.mark.asyncio
    async def test_get_embedding_general_exception(self):
        """Test general embedding exception - lines 811-824."""
        # This config will cause an exception when accessing embedder_config.get() on a non-dict
        bad_config = "not a dict"  # This will cause AttributeError when .get() is called
        
        with patch('src.core.llm_manager.logger.error') as mock_error:
            result = await LLMManager.get_embedding("test text", embedder_config=bad_config)
            assert result is None
            # Should log the actual exception
            mock_error.assert_called()
            error_call_args = mock_error.call_args[0][0]
            assert "Error creating embedding:" in error_call_args

    @pytest.mark.asyncio
    async def test_get_embedding_circuit_breaker(self):
        """Test circuit breaker functionality - lines 787-801."""
        # Reset circuit breaker
        LLMManager._embedding_failures.clear()
        
        with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
            with patch('litellm.aembedding') as mock_embedding:
                mock_api_keys.return_value = "test-key"
                mock_embedding.side_effect = Exception("API Error")
                
                # Trigger multiple failures
                for _ in range(3):
                    result = await LLMManager.get_embedding("test text")
                    assert result is None
                
                # Verify circuit breaker state
                assert LLMManager._embedding_failures['openai']['count'] == 3
                
                # Now test that circuit breaker prevents calls
                result = await LLMManager.get_embedding("test text")
                assert result is None

    @pytest.mark.asyncio
    async def test_get_embedding_circuit_breaker_reset(self):
        """Test circuit breaker resets after timeout."""
        # Simulate old failures that should be reset
        LLMManager._embedding_failures['openai'] = {
            'count': 5,
            'last_failure': time.time() - 301  # More than 5 minutes ago
        }

        with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
            with patch('os.environ.get', return_value=None):
                with patch('litellm.aembedding') as mock_embedding:
                    mock_api_keys.return_value = "test-openai-key"
                    
                    mock_response = {
                        "data": [{"embedding": [0.4, 0.5, 0.6]}]
                    }
                    mock_embedding.return_value = mock_response
                    
                    result = await LLMManager.get_embedding("test text")
                    assert result == [0.4, 0.5, 0.6]
                    # Circuit breaker should be reset
                    assert LLMManager._embedding_failures['openai']['count'] == 0

    @pytest.mark.asyncio
    async def test_get_embedding_data_extraction(self):
        """Test embedding data extraction - line 814."""
        with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
            with patch('os.environ.get', return_value=None):
                with patch('litellm.aembedding') as mock_embedding:
                    mock_api_keys.return_value = "test-key"
                    
                    # Create response with multiple embeddings
                    mock_response = {
                        "data": [
                            {"embedding": [0.1, 0.2]},
                            {"embedding": [0.3, 0.4]}
                        ]
                    }
                    mock_embedding.return_value = mock_response
                    
                    result = await LLMManager.get_embedding("test text")
                    # Should return first embedding
                    assert result == [0.1, 0.2]

    @pytest.mark.asyncio
    async def test_get_embedding_databricks_missing_credentials(self):
        """Test Databricks embedding with missing credentials - lines 689-690."""
        embedder_config = {"provider": "databricks", "config": {"model": "test-embedding-model"}}
        
        with patch("src.core.llm_manager.ApiKeysService.get_provider_api_key") as mock_api_keys:
            mock_api_keys.return_value = None  # No API key
            
            if 'src.utils.databricks_auth' in sys.modules:
                del sys.modules['src.utils.databricks_auth']
            
            with patch.dict(os.environ, {}, clear=True):
                with patch('src.core.llm_manager.logger.warning') as mock_warning:
                    result = await LLMManager.get_embedding("test text", embedder_config=embedder_config)
                    assert result is None
                    mock_warning.assert_called()

    @pytest.mark.asyncio
    async def test_line_317_deepseek_api_warning(self):
        """Test line 317 - DeepSeek no API key warning."""
        mock_config = {"provider": ModelProvider.DEEPSEEK, "name": "deepseek-chat"}
        
        with patch('src.core.llm_manager.UnitOfWork'):
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
                    mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                    mock_api_keys.return_value = None  # No API key
                    
                    with patch('src.core.llm_manager.logger.warning') as mock_warning:
                        result = await LLMManager.configure_litellm("test-model")
                        mock_warning.assert_called_with(f"No API key found for provider: {ModelProvider.DEEPSEEK}")

    @pytest.mark.asyncio
    async def test_line_425_gemini_warning(self):
        """Test line 425 - Gemini no API key warning."""
        mock_config = {"provider": ModelProvider.GEMINI, "name": "gemini-pro"}
        
        with patch('src.core.llm_manager.UnitOfWork'):
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
                    mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                    mock_api_keys.return_value = None  # No API key
                    
                    with patch('src.core.llm_manager.logger.warning') as mock_warning:
                        result = await LLMManager.configure_litellm("test-model")
                        mock_warning.assert_called_with(f"No API key found for provider: {ModelProvider.GEMINI}")

    @pytest.mark.asyncio
    async def test_lines_750_756_ollama_embedding(self):
        """Test lines 750-756 - Ollama embedding provider."""
        embedder_config = {"provider": "ollama", "config": {"model": "nomic-embed-text"}}
        
        with patch('litellm.aembedding') as mock_embedding:
            mock_response = {
                "data": [{"embedding": [0.5, 0.6, 0.7]}]
            }
            mock_embedding.return_value = mock_response
            
            result = await LLMManager.get_embedding("test text", embedder_config=embedder_config)
            assert result == [0.5, 0.6, 0.7]
            
            # Check that ollama prefix was added
            mock_embedding.assert_called_with(
                model="ollama/nomic-embed-text",
                input="test text",
                api_base="http://localhost:11434"
            )

    @pytest.mark.asyncio
    async def test_lines_764_774_google_embedding(self):
        """Test lines 764-774 - Google embedding provider."""
        embedder_config = {"provider": "google", "config": {"model": "text-embedding-004"}}
        
        with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
            with patch('litellm.aembedding') as mock_embedding:
                mock_api_keys.return_value = "google-api-key"
                mock_response = {
                    "data": [{"embedding": [0.8, 0.9, 1.0]}]
                }
                mock_embedding.return_value = mock_response
                
                result = await LLMManager.get_embedding("test text", embedder_config=embedder_config)
                assert result == [0.8, 0.9, 1.0]
                
                # Check that gemini prefix was added
                mock_embedding.assert_called_with(
                    model="gemini/text-embedding-004",
                    input="test text",
                    api_key="google-api-key"
                )

    @pytest.mark.asyncio
    async def test_lines_766_768_google_no_api_key(self):
        """Test lines 766-768 - Google embedding no API key."""
        embedder_config = {"provider": "google", "config": {"model": "text-embedding-004"}}
        
        with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
            mock_api_keys.return_value = None  # No API key
            
            with patch('src.core.llm_manager.logger.warning') as mock_warning:
                result = await LLMManager.get_embedding("test text", embedder_config=embedder_config)
                assert result is None
                mock_warning.assert_called_with("No Google API key found for creating embeddings")

    @pytest.mark.asyncio
    async def test_lines_804_810_embedding_response_failure(self):
        """Test lines 804-810 - Failed to get embedding from response."""
        with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
            with patch('litellm.aembedding') as mock_embedding:
                mock_api_keys.return_value = "test-key"
                # Return response without data
                mock_response = {"error": "No embeddings returned"}
                mock_embedding.return_value = mock_response
                
                with patch('src.core.llm_manager.logger.warning') as mock_warning:
                    result = await LLMManager.get_embedding("test text")
                    assert result is None
                    mock_warning.assert_called_with("Failed to get embedding from response")

    @pytest.mark.asyncio
    async def test_get_embedding_databricks_database_exception(self):
        """Test Databricks embedding with database exception - lines 684-685."""
        embedder_config = {"provider": "databricks", "config": {"model": "test-embedding-model"}}
        
        with patch("src.core.llm_manager.UnitOfWork") as mock_uow:
            with patch("src.services.databricks_service.DatabricksService.from_unit_of_work") as mock_db_service:
                with patch("src.core.llm_manager.ApiKeysService.get_provider_api_key") as mock_api_keys:
                    mock_uow.return_value = create_mock_uow()
                    mock_db_service.return_value.get_databricks_config = AsyncMock(side_effect=Exception("Database error"))
                    mock_api_keys.return_value = "test-token"
                    
                    if 'src.utils.databricks_auth' in sys.modules:
                        del sys.modules['src.utils.databricks_auth']
                    
                    with patch.dict(os.environ, {}, clear=True):
                        with patch('src.core.llm_manager.logger.error') as mock_error:
                            result = await LLMManager.get_embedding("test text", embedder_config=embedder_config)
                            mock_error.assert_called()

    @pytest.mark.asyncio
    async def test_lines_125_126_log_success_event_usage_error(self, temp_log_file):
        """Test lines 125-126 - usage key errors."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        kwargs = {"model": "gpt-3.5-turbo", "messages": [{"role": "user", "content": "test"}]}
        response_obj = {
            "usage": {"total_tokens": 15},  # Missing prompt_tokens and completion_tokens
            "choices": [{"message": {"content": "test response"}}]
        }
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        
        with patch('litellm.completion_cost', return_value=0.001):
            logger.log_success_event(kwargs, response_obj, start_time, end_time)

    def test_lines_129_130_log_success_no_usage(self, temp_log_file):
        """Test lines 129-130 - no usage data."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        kwargs = {"model": "test"}
        response_obj = {}  # No usage key
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        logger.log_success_event(kwargs, response_obj, start_time, end_time)

    @pytest.mark.asyncio
    async def test_lines_185_189_async_log_post_choices_error(self, temp_log_file):
        """Test lines 185-189 - async log post API call choices error."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        kwargs = {"model": "test"}
        
        class BadChoice:
            def __getitem__(self, key):
                raise Exception("Bad choice")
        
        response_obj = {"choices": [BadChoice()]}
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        
        with patch.object(logger.logger, 'error') as mock_error:
            await logger.async_log_post_api_call(kwargs, response_obj, start_time, end_time)
            mock_error.assert_called()

    @pytest.mark.asyncio
    async def test_lines_387_databricks_workspace_none(self):
        """Test line 387 - Databricks workspace URL is None in config."""
        mock_config = {"provider": ModelProvider.DATABRICKS, "name": "databricks-model"}
        
        if 'src.utils.databricks_auth' in sys.modules:
            del sys.modules['src.utils.databricks_auth']
        
        with patch('src.core.llm_manager.UnitOfWork') as mock_uow:
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                with patch('src.core.llm_manager.ApiKeysService.get_api_key_value') as mock_api_key:
                    with patch('src.services.databricks_service.DatabricksService.from_unit_of_work') as mock_db_service:
                        mock_uow.return_value = create_mock_uow()
                        mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                        mock_api_key.return_value = "test-token"
                        
                        # Mock database config with None workspace_url
                        mock_db_config = MagicMock()
                        mock_db_config.workspace_url = None
                        mock_db_service.return_value.get_databricks_config = AsyncMock(return_value=mock_db_config)
                        
                        with patch.dict(os.environ, {}, clear=True):
                            result = await LLMManager.configure_litellm("test-model")
                            # Should continue without setting api_base

    @pytest.mark.asyncio
    async def test_lines_477_478_crewai_anthropic(self):
        """Test lines 477-478 - CrewAI Anthropic provider."""
        mock_config = {"provider": ModelProvider.ANTHROPIC, "name": "claude-3-sonnet"}
        
        with patch('src.core.llm_manager.UnitOfWork'):
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
                    with patch('crewai.LLM') as mock_llm_class:
                        mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                        mock_api_keys.return_value = "anthropic-key"
                        mock_llm_instance = MagicMock()
                        mock_llm_class.return_value = mock_llm_instance
                        
                        result = await LLMManager.configure_crewai_llm("test-model")
                        assert result is not None

    @pytest.mark.asyncio
    async def test_lines_527_crewai_database_config_none(self):
        """Test line 527 - CrewAI Databricks database config workspace_url is None."""
        mock_config = {"provider": ModelProvider.DATABRICKS, "name": "databricks-model"}
        
        mock_databricks_auth = MagicMock()
        mock_databricks_auth.is_databricks_apps_environment = MagicMock(return_value=False)
        
        with patch.dict('sys.modules', {'src.utils.databricks_auth': mock_databricks_auth}):
            with patch('src.core.llm_manager.UnitOfWork') as mock_uow:
                with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                    with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_key:
                        with patch('src.services.databricks_service.DatabricksService.from_unit_of_work') as mock_db_service:
                            with patch('crewai.LLM') as mock_llm_class:
                                mock_uow.return_value = create_mock_uow()
                                mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                                mock_api_key.return_value = "test-token"
                                
                                # Mock database config with None workspace_url
                                mock_db_config = MagicMock()
                                mock_db_config.workspace_url = None
                                mock_db_service.return_value.get_databricks_config = AsyncMock(return_value=mock_db_config)
                                mock_llm_class.return_value = MagicMock()
                                
                                with patch.dict(os.environ, {}, clear=True):
                                    result = await LLMManager.configure_crewai_llm("test-model")
                                    assert result is not None

    @pytest.mark.asyncio
    async def test_lines_550_568_crewai_gemini(self):
        """Test lines 550-568 - CrewAI Gemini provider."""
        mock_config = {"provider": ModelProvider.GEMINI, "name": "gemini-1.5-pro"}
        
        with patch('src.core.llm_manager.UnitOfWork'):
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
                    with patch('crewai.LLM') as mock_llm_class:
                        mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                        mock_api_keys.return_value = "gemini-key"
                        mock_llm_instance = MagicMock()
                        mock_llm_class.return_value = mock_llm_instance
                        
                        with patch.dict(os.environ, {}, clear=True):
                            result = await LLMManager.configure_crewai_llm("test-model")
                            assert result is not None
                            # Check that environment variables were set
                            assert os.environ.get("GEMINI_API_KEY") == "gemini-key"
                            assert os.environ.get("GOOGLE_API_KEY") == "gemini-key"

    @pytest.mark.asyncio 
    async def test_lines_690_databricks_workspace_db(self):
        """Test line 690 - Databricks embedding workspace URL from database."""
        embedder_config = {"provider": "databricks", "config": {"model": "test-embedding-model"}}
        
        with patch("src.core.llm_manager.UnitOfWork") as mock_uow:
            with patch("src.services.databricks_service.DatabricksService.from_unit_of_work") as mock_db_service:
                with patch("src.core.llm_manager.ApiKeysService.get_provider_api_key") as mock_api_keys:
                    with patch("aiohttp.ClientSession") as mock_session_class:
                        mock_uow.return_value = create_mock_uow()
                        
                        # Mock database config with workspace URL
                        mock_db_config = MagicMock()
                        mock_db_config.workspace_url = "db-workspace.databricks.com"
                        mock_db_service.return_value.get_databricks_config = AsyncMock(return_value=mock_db_config)
                        
                        mock_api_keys.return_value = "test-token"
                        
                        success_response = AsyncContextResponse(status=200, json_data={"data": [{"embedding": [0.1, 0.2]}]})
                        session = AsyncContextSession(success_response)
                        mock_session_class.return_value = session
                        
                        if 'src.utils.databricks_auth' in sys.modules:
                            del sys.modules['src.utils.databricks_auth']
                        
                        with patch.dict(os.environ, {}, clear=True):  # No DATABRICKS_HOST
                            result = await LLMManager.get_embedding("test text", embedder_config=embedder_config)
                            assert result == [0.1, 0.2]

    @pytest.mark.asyncio
    async def test_lines_245_247_async_failure_event_exception(self, temp_log_file):
        """Test lines 245-247 - async failure event exception."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        
        class BadTime:
            def __sub__(self, other):
                raise Exception("Time error")
        
        kwargs = {"model": "test", "exception": Exception("test error")}
        response_obj = "error"
        start_time = datetime.now()
        end_time = BadTime()
        
        with patch.object(logger.logger, 'error') as mock_error:
            await logger.async_log_failure_event(kwargs, response_obj, start_time, end_time)
            mock_error.assert_called()

    def test_line_150_null_response_obj(self, temp_log_file):
        """Test line 150 - None response_obj."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        kwargs = {"model": "test"}
        response_obj = None  # This will test the response_obj condition
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        logger.log_failure_event(kwargs, response_obj, start_time, end_time)

    @pytest.mark.asyncio
    async def test_lines_599_600_get_llm(self):
        """Test lines 599-600 - get_llm method."""
        # This just calls configure_crewai_llm, so test that it works
        mock_config = {"provider": ModelProvider.OPENAI, "name": "gpt-4"}
        
        with patch('src.core.llm_manager.UnitOfWork'):
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
                    with patch('crewai.LLM') as mock_llm_class:
                        mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                        mock_api_keys.return_value = "test-key"
                        mock_llm_instance = MagicMock()
                        mock_llm_class.return_value = mock_llm_instance
                        
                        result = await LLMManager.get_llm("test-model")
                        assert result is not None

    @pytest.mark.asyncio
    async def test_lines_657_658_databricks_oauth_error(self):
        """Test lines 657-658 - Databricks OAuth header error."""
        embedder_config = {"provider": "databricks", "config": {"model": "test-embedding"}}
        
        mock_databricks_auth = MagicMock()
        mock_databricks_auth.is_databricks_apps_environment = MagicMock(return_value=True)
        mock_databricks_auth.get_databricks_auth_headers = AsyncMock(return_value=(None, "Auth error"))
        
        with patch.dict('sys.modules', {'src.utils.databricks_auth': mock_databricks_auth}):
            with patch('src.core.llm_manager.logger.error') as mock_error:
                result = await LLMManager.get_embedding("test text", embedder_config=embedder_config)
                assert result is None
                mock_error.assert_called()

    @pytest.mark.asyncio
    async def test_lines_666_669_databricks_import_error_embedding(self):
        """Test lines 666-669 - Databricks embedding import error."""
        embedder_config = {"provider": "databricks", "config": {"model": "test-embedding"}}
        
        if 'src.utils.databricks_auth' in sys.modules:
            del sys.modules['src.utils.databricks_auth']
        
        # Create a fake module that raises ImportError
        fake_module = type(sys)('fake_databricks_auth')
        sys.modules['src.utils.databricks_auth'] = fake_module
        
        def raise_import_error(*args, **kwargs):
            raise ImportError("Module not available")
        
        fake_module.__getattr__ = raise_import_error
        
        try:
            with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
                with patch('src.core.llm_manager.logger.warning') as mock_warning:
                    mock_api_keys.return_value = "test-token"
                    
                    with patch("aiohttp.ClientSession") as mock_session_class:
                        success_response = AsyncContextResponse(status=200, json_data={"data": [{"embedding": [0.3, 0.4]}]})
                        session = AsyncContextSession(success_response)
                        mock_session_class.return_value = session
                        
                        with patch.dict(os.environ, {"DATABRICKS_HOST": "https://workspace.databricks.com"}):
                            result = await LLMManager.get_embedding("test text", embedder_config=embedder_config)
                            assert result == [0.3, 0.4]
                            mock_warning.assert_called_with("Enhanced Databricks auth not available for embeddings, using legacy PAT")
        finally:
            if 'src.utils.databricks_auth' in sys.modules:
                del sys.modules['src.utils.databricks_auth']

    @pytest.mark.asyncio
    async def test_lines_737_738_databricks_json_error(self):
        """Test lines 737-738 - Databricks embedding JSON error."""
        embedder_config = {"provider": "databricks", "config": {"model": "test-embedding"}}
        
        class BadResponse:
            status = 200
            async def json(self):
                raise Exception("JSON decode error")
            async def text(self):
                return "Invalid JSON"
            async def __aenter__(self):
                return self
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass
        
        class BadSession:
            def post(self, *args, **kwargs):
                return BadResponse()
            async def __aenter__(self):
                return self
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass
        
        with patch("src.core.llm_manager.ApiKeysService.get_provider_api_key") as mock_api_keys:
            with patch("aiohttp.ClientSession") as mock_session_class:
                mock_api_keys.return_value = "test-token"
                mock_session_class.return_value = BadSession()
                
                if 'src.utils.databricks_auth' in sys.modules:
                    del sys.modules['src.utils.databricks_auth']
                
                with patch.dict(os.environ, {"DATABRICKS_HOST": "https://workspace.databricks.com"}):
                    with patch('src.core.llm_manager.logger.error') as mock_error:
                        result = await LLMManager.get_embedding("test text", embedder_config=embedder_config)
                        assert result is None
                        # Should have at least one error logged
                        assert mock_error.call_count >= 1

    def test_line_91_92_choices_iteration_error(self, temp_log_file):
        """Test lines 91-92 - choices iteration error."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        kwargs = {"model": "test"}
        
        class BadChoices:
            def __iter__(self):
                raise Exception("Iteration error")
        
        response_obj = {"choices": BadChoices()}
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        logger.log_post_api_call(kwargs, response_obj, start_time, end_time)

    def test_lines_125_126_usage_key_error(self, temp_log_file):
        """Test lines 125-126 - usage key access error."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        
        class BadUsage:
            def __getitem__(self, key):
                if key == "prompt_tokens":
                    raise KeyError("Missing prompt_tokens")
                return 10
            
            def get(self, key, default=None):
                if key == "prompt_tokens":
                    raise KeyError("Missing prompt_tokens")
                return 10
        
        kwargs = {"model": "test"}
        response_obj = {"usage": BadUsage()}
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        
        with patch('litellm.completion_cost', return_value=0.001):
            logger.log_success_event(kwargs, response_obj, start_time, end_time)

    @pytest.mark.asyncio
    async def test_lines_162_163_async_pre_call_error(self, temp_log_file):
        """Test lines 162-163 - async pre call error."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        
        class BadKwargs:
            def items(self):
                raise Exception("Items error")
        
        with patch.object(logger.logger, 'error') as mock_error:
            await logger.async_log_pre_api_call("model", [], BadKwargs())
            mock_error.assert_called()

    @pytest.mark.asyncio
    async def test_line_185_async_post_choices_single_error(self, temp_log_file):
        """Test line 185 - single choice processing error in async."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        kwargs = {"model": "test"}
        
        class BadChoice:
            def get(self, key, default=None):
                if key == "message":
                    raise Exception("Message error")
                return default
        
        response_obj = {"choices": [BadChoice()]}
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        
        await logger.async_log_post_api_call(kwargs, response_obj, start_time, end_time)

    @pytest.mark.asyncio
    async def test_lines_188_189_async_post_general_error(self, temp_log_file):
        """Test lines 188-189 - general error in async post call."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        
        class BadEndTime:
            def __sub__(self, other):
                raise Exception("Time calculation error")
        
        kwargs = {"model": "test"}
        response_obj = {}
        start_time = datetime.now()
        end_time = BadEndTime()
        
        with patch.object(logger.logger, 'error') as mock_error:
            await logger.async_log_post_api_call(kwargs, response_obj, start_time, end_time)
            mock_error.assert_called()

    @pytest.mark.asyncio
    async def test_lines_220_221_async_success_choices_error(self, temp_log_file):
        """Test lines 220-221 - choices error in async success event."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        
        class BadChoices:
            def __iter__(self):
                raise Exception("Choices iteration error")
        
        kwargs = {"model": "test", "messages": []}
        response_obj = {"usage": {"prompt_tokens": 5}, "choices": BadChoices()}
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        
        with patch('litellm.completion_cost', return_value=0.001):
            with patch.object(logger.logger, 'error') as mock_error:
                await logger.async_log_success_event(kwargs, response_obj, start_time, end_time)
                mock_error.assert_called()

    @pytest.mark.asyncio
    async def test_lines_224_225_async_success_cost_warning(self, temp_log_file):
        """Test lines 224-225 - cost calculation warning in async success."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        kwargs = {"model": "test"}
        response_obj = {"usage": {"prompt_tokens": 5}}
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        
        with patch('litellm.completion_cost', side_effect=Exception("Cost error")):
            with patch.object(logger.logger, 'warning') as mock_warning:
                await logger.async_log_success_event(kwargs, response_obj, start_time, end_time)
                mock_warning.assert_called()

    @pytest.mark.asyncio
    async def test_line_245_async_failure_duration_error(self, temp_log_file):
        """Test line 245 - duration calculation error in async failure."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        
        class BadStart:
            def __sub__(self, other):
                raise Exception("Duration error")
        
        kwargs = {"model": "test"}
        response_obj = "error"
        start_time = BadStart()
        end_time = datetime.now()
        
        with patch.object(logger.logger, 'error') as mock_error:
            await logger.async_log_failure_event(kwargs, response_obj, start_time, end_time)
            mock_error.assert_called()

    @pytest.mark.asyncio
    async def test_lines_567_568_crewai_gemini_no_api_key(self):
        """Test lines 567-568 - CrewAI Gemini with no API key."""
        mock_config = {"provider": ModelProvider.GEMINI, "name": "gemini-1.5-pro"}
        
        with patch('src.core.llm_manager.UnitOfWork'):
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
                    with patch('crewai.LLM') as mock_llm_class:
                        mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                        mock_api_keys.return_value = None  # No API key
                        mock_llm_instance = MagicMock()
                        mock_llm_class.return_value = mock_llm_instance
                        
                        result = await LLMManager.configure_crewai_llm("test-model")
                        assert result is not None
                        # Should still create LLM even without API key

    def test_lines_125_126_success_event_choices_content_error(self, temp_log_file):
        """Test lines 125-126 - error logging response content in success event."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        
        class BadChoice:
            def __getitem__(self, key):
                if key == "message":
                    return {"content": "test"}
                raise Exception("Bad choice access")
        
        kwargs = {"model": "test", "messages": []}
        response_obj = {"usage": {"prompt_tokens": 5}, "choices": [BadChoice()]}
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        
        with patch('litellm.completion_cost', return_value=0.001):
            logger.log_success_event(kwargs, response_obj, start_time, end_time)

    def test_lines_129_130_success_event_general_error(self, temp_log_file):
        """Test lines 129-130 - general error in log_success_event."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        
        class BadEndTime:
            def __sub__(self, other):
                raise Exception("Time error")
        
        kwargs = {"model": "test"}
        response_obj = {}
        start_time = datetime.now()
        end_time = BadEndTime()
        
        logger.log_success_event(kwargs, response_obj, start_time, end_time)

    def test_line_150_log_failure_none_exception(self, temp_log_file):
        """Test line 150 - log_failure_event with None exception."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        kwargs = {"model": "test", "exception": None}
        response_obj = "error"
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        logger.log_failure_event(kwargs, response_obj, start_time, end_time)

    @pytest.mark.asyncio
    async def test_line_185_actual_line_coverage(self, temp_log_file):
        """Test line 185 - actual single line coverage."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        kwargs = {"model": "test"}
        response_obj = {"choices": [{"message": {"content": "test"}}]}
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        
        await logger.async_log_post_api_call(kwargs, response_obj, start_time, end_time)

    @pytest.mark.asyncio
    async def test_lines_224_225_actual_warning(self, temp_log_file):
        """Test lines 224-225 - actual usage calculation warning."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        
        class BadUsage:
            def get(self, key, default=None):
                raise Exception("Usage error")
        
        kwargs = {"model": "test"}
        response_obj = {"usage": BadUsage()}
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        
        with patch.object(logger.logger, 'warning') as mock_warning:
            await logger.async_log_success_event(kwargs, response_obj, start_time, end_time)
            mock_warning.assert_called()

    @pytest.mark.asyncio
    async def test_line_245_actual_line(self, temp_log_file):
        """Test line 245 - actual async failure duration calculation."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        kwargs = {"model": "test", "exception": Exception("test")}
        response_obj = "error"
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        
        await logger.async_log_failure_event(kwargs, response_obj, start_time, end_time)

    @pytest.mark.asyncio
    async def test_lines_395_399_databricks_enhanced_auth_fallback(self):
        """Test lines 395-399 - databricks enhanced auth debug logging."""
        mock_config = {"provider": ModelProvider.DATABRICKS, "name": "databricks-model"}
        
        if 'src.utils.databricks_auth' in sys.modules:
            del sys.modules['src.utils.databricks_auth']
        
        with patch('src.core.llm_manager.UnitOfWork') as mock_uow:
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                with patch('src.core.llm_manager.ApiKeysService.get_api_key_value') as mock_api_key:
                    with patch('src.services.databricks_service.DatabricksService.from_unit_of_work') as mock_db_service:
                        mock_uow.return_value = create_mock_uow()
                        mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                        mock_api_key.return_value = "test-token"
                        
                        mock_databricks_config = MagicMock()
                        mock_databricks_config.workspace_url = None
                        mock_db_service.return_value.get_databricks_config = AsyncMock(return_value=mock_databricks_config)
                        
                        # Mock the enhanced auth module to trigger the debug path
                        fake_module = MagicMock()
                        fake_module._databricks_auth = MagicMock()
                        fake_module._databricks_auth._workspace_host = "https://enhanced.databricks.com"
                        
                        with patch.dict('sys.modules', {'src.utils.databricks_auth': fake_module}):
                            result = await LLMManager.configure_litellm("test-model")
                            assert result is not None

    @pytest.mark.asyncio
    async def test_lines_737_738_databricks_embedding_json_decode_error(self):
        """Test lines 737-738 - specific JSON decode error in databricks embedding."""
        embedder_config = {"provider": "databricks", "config": {"model": "test-embedding"}}
        
        class JsonErrorResponse:
            status = 200
            async def json(self):
                import json
                raise json.JSONDecodeError("Expecting value", "", 0)
            async def text(self):
                return "Invalid JSON response"
            async def __aenter__(self):
                return self
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass
        
        class JsonErrorSession:
            def post(self, *args, **kwargs):
                return JsonErrorResponse()
            async def __aenter__(self):
                return self
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass
        
        with patch("src.core.llm_manager.ApiKeysService.get_provider_api_key") as mock_api_keys:
            with patch("aiohttp.ClientSession") as mock_session_class:
                mock_api_keys.return_value = "test-token"
                mock_session_class.return_value = JsonErrorSession()
                
                if 'src.utils.databricks_auth' in sys.modules:
                    del sys.modules['src.utils.databricks_auth']
                
                with patch.dict(os.environ, {"DATABRICKS_HOST": "https://workspace.databricks.com"}):
                    with patch('src.core.llm_manager.logger.error') as mock_error:
                        result = await LLMManager.get_embedding("test text", embedder_config=embedder_config)
                        assert result is None
                        # Should have logged the JSON decode error specifically
                        assert mock_error.call_count >= 1

    def test_line_150_traceback_exception_logging(self, temp_log_file):
        """Test line 150 - traceback_exception logging in log_failure_event."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        kwargs = {"model": "test", "traceback_exception": "Sample traceback string"}
        response_obj = "error"
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        
        with patch.object(logger.logger, 'error') as mock_error:
            logger.log_failure_event(kwargs, response_obj, start_time, end_time)
            # Should log the traceback
            calls = [str(call) for call in mock_error.call_args_list]
            assert any("Traceback: Sample traceback string" in call for call in calls)

    @pytest.mark.asyncio
    async def test_line_185_choice_without_message_content(self, temp_log_file):
        """Test line 185 - choice without message.content in async post call."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        kwargs = {"model": "test"}
        # Choice that doesn't have message.content
        choice_without_content = {"index": 0, "finish_reason": "stop"}
        response_obj = {"choices": [choice_without_content]}
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        
        await logger.async_log_post_api_call(kwargs, response_obj, start_time, end_time)

    @pytest.mark.asyncio
    async def test_lines_224_225_general_async_success_error(self, temp_log_file):
        """Test lines 224-225 - general error in async_log_success_event."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        
        # Force an error in the main try block by making duration calculation fail
        class BadEndTime:
            def __sub__(self, other):
                raise Exception("Duration calculation failed")
        
        kwargs = {"model": "test"}
        response_obj = {}
        start_time = datetime.now()
        end_time = BadEndTime()
        
        with patch.object(logger.logger, 'error') as mock_error:
            await logger.async_log_success_event(kwargs, response_obj, start_time, end_time)
            calls = [str(call) for call in mock_error.call_args_list]
            assert any("Error in async_log_success_event" in call for call in calls)

    @pytest.mark.asyncio
    async def test_line_245_traceback_in_async_failure(self, temp_log_file):
        """Test line 245 - traceback logging in async_log_failure_event."""
        logger = LiteLLMFileLogger(file_path=temp_log_file)
        kwargs = {"model": "test", "traceback_exception": "Async traceback details"}
        response_obj = "error"
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        
        with patch.object(logger.logger, 'error') as mock_error:
            await logger.async_log_failure_event(kwargs, response_obj, start_time, end_time)
            calls = [str(call) for call in mock_error.call_args_list]
            assert any("Traceback: Async traceback details" in call for call in calls)

    @pytest.mark.asyncio
    async def test_line_387_workspace_url_https_prepend(self):
        """Test line 387 - prepending https to workspace URL from database."""
        mock_config = {"provider": ModelProvider.DATABRICKS, "name": "databricks-model"}
        
        if 'src.utils.databricks_auth' in sys.modules:
            del sys.modules['src.utils.databricks_auth']
        
        with patch('src.core.llm_manager.UnitOfWork') as mock_uow:
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                with patch('src.core.llm_manager.ApiKeysService.get_api_key_value') as mock_api_key:
                    with patch('src.services.databricks_service.DatabricksService.from_unit_of_work') as mock_db_service:
                        mock_uow.return_value = create_mock_uow()
                        mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                        mock_api_key.return_value = "test-token"
                        
                        # Mock database config with workspace URL WITHOUT https
                        mock_db_config = MagicMock()
                        mock_db_config.workspace_url = "workspace.databricks.com"  # No https prefix
                        mock_db_service.return_value.get_databricks_config = AsyncMock(return_value=mock_db_config)
                        
                        with patch.dict(os.environ, {}, clear=True):
                            with patch('src.core.llm_manager.logger.info') as mock_info:
                                result = await LLMManager.configure_litellm("test-model")
                                # Should have prepended https
                                calls = [str(call) for call in mock_info.call_args_list]
                                assert any("https://workspace.databricks.com" in call for call in calls)

    @pytest.mark.asyncio
    async def test_lines_398_399_enhanced_auth_exception_and_debug(self):
        """Test lines 398-399 - enhanced auth exception handling and debug logging."""
        mock_config = {"provider": ModelProvider.DATABRICKS, "name": "databricks-model"}
        
        if 'src.utils.databricks_auth' in sys.modules:
            del sys.modules['src.utils.databricks_auth']
        
        with patch('src.core.llm_manager.UnitOfWork') as mock_uow:
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                with patch('src.core.llm_manager.ApiKeysService.get_api_key_value') as mock_api_key:
                    with patch('src.services.databricks_service.DatabricksService.from_unit_of_work') as mock_db_service:
                        mock_uow.return_value = create_mock_uow()
                        mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                        mock_api_key.return_value = "test-token"
                        
                        mock_databricks_config = MagicMock()
                        mock_databricks_config.workspace_url = None
                        mock_db_service.return_value.get_databricks_config = AsyncMock(return_value=mock_databricks_config)
                        
                        # Mock the enhanced auth module with a _databricks_auth that raises an exception
                        fake_module = MagicMock()
                        fake_module._databricks_auth = MagicMock()
                        # Make the _workspace_host property access raise an exception
                        type(fake_module._databricks_auth)._workspace_host = PropertyMock(side_effect=Exception("Enhanced auth error"))
                        
                        with patch.dict('sys.modules', {'src.utils.databricks_auth': fake_module}):
                            with patch('src.core.llm_manager.logger.debug') as mock_debug:
                                result = await LLMManager.configure_litellm("test-model")
                                calls = [str(call) for call in mock_debug.call_args_list]
                                assert any("Could not get workspace URL from enhanced auth" in call for call in calls)

    @pytest.mark.asyncio
    async def test_line_527_crewai_databricks_workspace_https_prepend(self):
        """Test line 527 - prepending https in CrewAI Databricks config."""
        mock_config = {"provider": ModelProvider.DATABRICKS, "name": "databricks-model"}
        
        mock_databricks_auth = MagicMock()
        mock_databricks_auth.is_databricks_apps_environment = MagicMock(return_value=False)
        
        with patch.dict('sys.modules', {'src.utils.databricks_auth': mock_databricks_auth}):
            with patch('src.core.llm_manager.UnitOfWork') as mock_uow:
                with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                    with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_key:
                        with patch('src.services.databricks_service.DatabricksService.from_unit_of_work') as mock_db_service:
                            with patch('crewai.LLM') as mock_llm_class:
                                mock_uow.return_value = create_mock_uow()
                                mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                                mock_api_key.return_value = "test-token"
                                
                                # Mock database config with workspace URL WITHOUT https
                                mock_db_config = MagicMock()
                                mock_db_config.workspace_url = "crewai-workspace.databricks.com"
                                mock_db_service.return_value.get_databricks_config = AsyncMock(return_value=mock_db_config)
                                mock_llm_class.return_value = MagicMock()
                                
                                with patch.dict(os.environ, {}, clear=True):
                                    result = await LLMManager.configure_crewai_llm("test-model")
                                    assert result is not None

    @pytest.mark.asyncio
    async def test_lines_567_568_crewai_gemini_no_api_key_path(self):
        """Test lines 567-568 - CrewAI Gemini without API key path."""
        mock_config = {"provider": ModelProvider.GEMINI, "name": "gemini-1.5-pro"}
        
        with patch('src.core.llm_manager.UnitOfWork'):
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
                    with patch('crewai.LLM') as mock_llm_class:
                        mock_service.return_value.get_model_config = AsyncMock(return_value=mock_config)
                        mock_api_keys.return_value = None  # No API key - this should trigger lines 567-568
                        mock_llm_instance = MagicMock()
                        mock_llm_class.return_value = mock_llm_instance
                        
                        with patch.dict(os.environ, {}, clear=True):
                            result = await LLMManager.configure_crewai_llm("test-model")
                            assert result is not None
                            # Should not set environment variables when no API key
                            assert os.environ.get("GEMINI_API_KEY") is None

    @pytest.mark.asyncio
    async def test_lines_737_738_databricks_json_parse_error_path(self):
        """Test lines 737-738 - specific JSON parsing error path in databricks embedding."""
        embedder_config = {"provider": "databricks", "config": {"model": "test-embedding"}}
        
        class JsonParseErrorResponse:
            status = 200
            async def json(self):
                # Force a specific JSON parsing error to trigger the exact exception handling
                raise ValueError("JSON parse error")
            async def text(self):
                return "Response text for error"
            async def __aenter__(self):
                return self
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass
        
        class JsonParseErrorSession:
            def post(self, *args, **kwargs):
                return JsonParseErrorResponse()
            async def __aenter__(self):
                return self
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass
        
        with patch("src.core.llm_manager.ApiKeysService.get_provider_api_key") as mock_api_keys:
            with patch("aiohttp.ClientSession") as mock_session_class:
                mock_api_keys.return_value = "test-token"
                mock_session_class.return_value = JsonParseErrorSession()
                
                if 'src.utils.databricks_auth' in sys.modules:
                    del sys.modules['src.utils.databricks_auth']
                
                with patch.dict(os.environ, {"DATABRICKS_HOST": "https://workspace.databricks.com"}):
                    with patch('src.core.llm_manager.logger.error') as mock_error:
                        result = await LLMManager.get_embedding("test text", embedder_config=embedder_config)
                        assert result is None
                        # Should have logged both the JSON parse error and the general error
                        calls = [str(call) for call in mock_error.call_args_list]
                        assert any("Error parsing" in call or "JSON" in call for call in calls)

    @pytest.mark.asyncio
    async def test_lines_567_568_default_provider_fallback(self):
        """Test lines 567-568 - default provider fallback warning for unknown providers."""
        mock_uow = create_mock_uow()
        embedder_config = {
            "model_config_id": "test-config-id",
            "provider": "unknown_provider",
            "model": "test-model",
            "api_key": "test-key"
        }
        
        with patch('src.core.llm_manager.UnitOfWork', return_value=mock_uow):
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                mock_service.return_value.get_model_config.return_value = {
                    "name": "test-model",
                    "provider": "unknown_provider",
                    "api_base": "https://api.test.com"
                }
                
                with patch('src.core.llm_manager.logger.warning') as mock_warning:
                    with patch('crewai.LLM') as mock_llm:
                        result = await LLMManager.configure_crewai_llm("test-model")
                        
                        # Should log warning about using default model name format
                        mock_warning.assert_called_with("Using default model name format for provider: unknown_provider")
                        assert result is not None

    @pytest.mark.asyncio
    async def test_lines_737_738_databricks_no_embedding_data(self):
        """Test lines 737-738 - Databricks response without embedding data."""
        # Response with data but empty array
        class NoEmbeddingDataSession:
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                pass
            def post(self, *args, **kwargs):
                return NoEmbeddingDataResponse()
        
        class NoEmbeddingDataResponse:
            status = 200
            async def json(self):
                return {"data": []}  # Empty data array
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                pass
        
        embedder_config = {
            "model_config_id": "test-config-id",
            "provider": "databricks",
            "model": "bge-large-en"
        }
        
        mock_uow = create_mock_uow()
        with patch('src.core.llm_manager.UnitOfWork', return_value=mock_uow):
            with patch('src.core.llm_manager.ModelConfigService.from_unit_of_work') as mock_service:
                mock_service.return_value.get_model_config.return_value = {
                    "model_name": "bge-large-en",
                    "provider": "databricks"
                }
                
                with patch('src.core.llm_manager.ApiKeysService.get_provider_api_key') as mock_api_keys:
                    with patch('aiohttp.ClientSession', NoEmbeddingDataSession):
                        mock_api_keys.return_value = "test-token"
                        
                        if 'src.utils.databricks_auth' in sys.modules:
                            del sys.modules['src.utils.databricks_auth']
                        
                        with patch.dict(os.environ, {"DATABRICKS_HOST": "https://workspace.databricks.com"}):
                            with patch('src.core.llm_manager.logger.warning') as mock_warning:
                                result = await LLMManager.get_embedding("test text", embedder_config=embedder_config)
                                assert result is None
                                mock_warning.assert_called_with("No embedding data found in Databricks response")