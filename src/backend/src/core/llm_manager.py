"""
LLM Manager for handling model configuration and LLM interactions.

This module provides a centralized manager for configuring and interacting with
different LLM providers through litellm.
"""

import logging
import os
import json
from typing import Dict, Any, List, Optional
import time

from crewai import LLM
from src.schemas.model_provider import ModelProvider
from src.services.model_config_service import ModelConfigService
from src.services.api_keys_service import ApiKeysService
from src.core.unit_of_work import UnitOfWork
import litellm
import pathlib
from litellm.integrations.custom_logger import CustomLogger

# Get the absolute path to the logs directory
log_dir = os.environ.get("LOG_DIR", str(pathlib.Path(__file__).parent.parent.parent / "logs"))
log_file_path = os.path.join(log_dir, "llm.log")

# Configure LiteLLM for better compatibility with providers
os.environ["LITELLM_LOG"] = "DEBUG"  # For debugging (replaces deprecated litellm.set_verbose)
os.environ["LITELLM_LOG_FILE"] = log_file_path  # Configure LiteLLM to write logs to file

# Configure standard Python logger to also write to the llm.log file
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# Check if handlers already exist to avoid duplicates
if not logger.handlers:
    file_handler = logging.FileHandler(log_file_path)
    formatter = logging.Formatter('%(asctime)s - %(process)d - %(filename)s-%(funcName)s:%(lineno)d - %(levelname)s: %(message)s', 
                                 datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

# Create a custom file logger for LiteLLM
class LiteLLMFileLogger(CustomLogger):
    def __init__(self, file_path=None):
        self.file_path = file_path or log_file_path
        # Ensure the directory exists
        log_dir_path = os.path.dirname(self.file_path)
        if log_dir_path and not os.path.exists(log_dir_path):
            os.makedirs(log_dir_path, exist_ok=True)
        # Set up a file logger
        self.logger = logging.getLogger("litellm_file_logger")
        self.logger.setLevel(logging.DEBUG)
        # Remove existing handlers to avoid duplicates
        self.logger.handlers = []
        file_handler = logging.FileHandler(self.file_path)
        formatter = logging.Formatter('[LiteLLM] %(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
    
    def log_pre_api_call(self, model, messages, kwargs):
        try:
            self.logger.info(f"Pre-API Call - Model: {model}")
            self.logger.info(f"Messages: {json.dumps(messages, indent=2)}")
            # Log all kwargs except messages which we've already logged
            kwargs_to_log = {k: v for k, v in kwargs.items() if k != 'messages'}
            self.logger.info(f"Parameters: {json.dumps(kwargs_to_log, default=str, indent=2)}")
        except Exception as e:
            self.logger.error(f"Error in log_pre_api_call: {str(e)}")
    
    def log_post_api_call(self, kwargs, response_obj, start_time, end_time):
        try:
            duration = end_time - start_time
            duration_seconds = duration.total_seconds()
            self.logger.info(f"Post-API Call - Duration: {duration_seconds:.2f}s")
            # Log the full response object
            if response_obj:
                self.logger.info("Response:")
                # Log response metadata
                response_meta = {k: v for k, v in response_obj.items() if k != 'choices'}
                self.logger.info(f"Metadata: {json.dumps(response_meta, default=str, indent=2)}")
                
                # Log full response content
                if 'choices' in response_obj:
                    try:
                        for i, choice in enumerate(response_obj['choices']):
                            if 'message' in choice and 'content' in choice['message']:
                                content = choice['message']['content']
                                self.logger.info(f"Choice {i} content:\n{content}")
                            else:
                                self.logger.info(f"Choice {i}: {json.dumps(choice, default=str, indent=2)}")
                    except Exception as e:
                        self.logger.error(f"Error logging choices: {str(e)}")
        except Exception as e:
            self.logger.error(f"Error in log_post_api_call: {str(e)}")
    
    def log_success_event(self, kwargs, response_obj, start_time, end_time):
        try:
            duration = end_time - start_time
            duration_seconds = duration.total_seconds()
            model = kwargs.get('model', 'unknown')
            self.logger.info(f"Success - Model: {model}, Duration: {duration_seconds:.2f}s")
            
            # Calculate tokens and cost if available
            try:
                usage = response_obj.get('usage', {})
                prompt_tokens = usage.get('prompt_tokens', 0)
                completion_tokens = usage.get('completion_tokens', 0)
                total_tokens = usage.get('total_tokens', 0)
                
                cost = litellm.completion_cost(completion_response=response_obj)
                
                self.logger.info(f"Tokens - Prompt: {prompt_tokens}, Completion: {completion_tokens}, Total: {total_tokens}, Cost: ${cost:.6f}")
                
                # Log request messages again for convenience
                if 'messages' in kwargs:
                    self.logger.info(f"Request messages: {json.dumps(kwargs['messages'], indent=2)}")
                
                # Log complete response content
                if 'choices' in response_obj:
                    try:
                        for i, choice in enumerate(response_obj['choices']):
                            if 'message' in choice and 'content' in choice['message']:
                                content = choice['message']['content']
                                self.logger.info(f"Response content (choice {i}):\n{content}")
                    except Exception as e:
                        self.logger.error(f"Error logging response content: {str(e)}")
            except Exception as e:
                self.logger.warning(f"Could not calculate token usage: {str(e)}")
        except Exception as e:
            self.logger.error(f"Error in log_success_event: {str(e)}")
    
    def log_failure_event(self, kwargs, response_obj, start_time, end_time):
        try:
            duration = end_time - start_time
            duration_seconds = duration.total_seconds()
            model = kwargs.get('model', 'unknown')
            error_msg = str(response_obj) if response_obj else "Unknown error"
            
            self.logger.error(f"Failure - Model: {model}, Duration: {duration_seconds:.2f}s")
            self.logger.error(f"Error: {error_msg}")
            
            # Log exception details if available
            exception = kwargs.get('exception', None)
            if exception:
                self.logger.error(f"Exception: {str(exception)}")
                
            # Traceback if available
            traceback = kwargs.get('traceback_exception', None)
            if traceback:
                self.logger.error(f"Traceback: {str(traceback)}")
        except Exception as e:
            self.logger.error(f"Error in log_failure_event: {str(e)}")
    
    # Async versions of callback methods for async operations
    async def async_log_pre_api_call(self, model, messages, kwargs):
        try:
            self.logger.info(f"Pre-API Call - Model: {model}")
            self.logger.info(f"Messages: {json.dumps(messages, indent=2)}")
            # Log all kwargs except messages which we've already logged
            kwargs_to_log = {k: v for k, v in kwargs.items() if k != 'messages'}
            self.logger.info(f"Parameters: {json.dumps(kwargs_to_log, default=str, indent=2)}")
        except Exception as e:
            self.logger.error(f"Error in async_log_pre_api_call: {str(e)}")
    
    async def async_log_post_api_call(self, kwargs, response_obj, start_time, end_time):
        try:
            duration = end_time - start_time
            duration_seconds = duration.total_seconds()
            self.logger.info(f"Post-API Call - Duration: {duration_seconds:.2f}s")
            # Log the full response object
            if response_obj:
                self.logger.info("Response:")
                # Log response metadata
                response_meta = {k: v for k, v in response_obj.items() if k != 'choices'}
                self.logger.info(f"Metadata: {json.dumps(response_meta, default=str, indent=2)}")
                
                # Log full response content
                if 'choices' in response_obj:
                    try:
                        for i, choice in enumerate(response_obj['choices']):
                            if 'message' in choice and 'content' in choice['message']:
                                content = choice['message']['content']
                                self.logger.info(f"Choice {i} content:\n{content}")
                            else:
                                self.logger.info(f"Choice {i}: {json.dumps(choice, default=str, indent=2)}")
                    except Exception as e:
                        self.logger.error(f"Error logging choices: {str(e)}")
        except Exception as e:
            self.logger.error(f"Error in async_log_post_api_call: {str(e)}")
            
    async def async_log_success_event(self, kwargs, response_obj, start_time, end_time):
        try:
            duration = end_time - start_time
            duration_seconds = duration.total_seconds()
            model = kwargs.get('model', 'unknown')
            self.logger.info(f"Success - Model: {model}, Duration: {duration_seconds:.2f}s")
            
            # Calculate tokens and cost if available
            try:
                usage = response_obj.get('usage', {})
                prompt_tokens = usage.get('prompt_tokens', 0)
                completion_tokens = usage.get('completion_tokens', 0)
                total_tokens = usage.get('total_tokens', 0)
                
                cost = litellm.completion_cost(completion_response=response_obj)
                
                self.logger.info(f"Tokens - Prompt: {prompt_tokens}, Completion: {completion_tokens}, Total: {total_tokens}, Cost: ${cost:.6f}")
                
                # Log request messages again for convenience
                if 'messages' in kwargs:
                    self.logger.info(f"Request messages: {json.dumps(kwargs['messages'], indent=2)}")
                
                # Log complete response content
                if 'choices' in response_obj:
                    try:
                        for i, choice in enumerate(response_obj['choices']):
                            if 'message' in choice and 'content' in choice['message']:
                                content = choice['message']['content']
                                self.logger.info(f"Response content (choice {i}):\n{content}")
                    except Exception as e:
                        self.logger.error(f"Error logging response content: {str(e)}")
            except Exception as e:
                self.logger.warning(f"Could not calculate token usage: {str(e)}")
        except Exception as e:
            self.logger.error(f"Error in async_log_success_event: {str(e)}")
    
    async def async_log_failure_event(self, kwargs, response_obj, start_time, end_time):
        try:
            duration = end_time - start_time
            duration_seconds = duration.total_seconds()
            model = kwargs.get('model', 'unknown')
            error_msg = str(response_obj) if response_obj else "Unknown error"
            
            self.logger.error(f"Failure - Model: {model}, Duration: {duration_seconds:.2f}s")
            self.logger.error(f"Error: {error_msg}")
            
            # Log exception details if available
            exception = kwargs.get('exception', None)
            if exception:
                self.logger.error(f"Exception: {str(exception)}")
                
            # Traceback if available
            traceback = kwargs.get('traceback_exception', None)
            if traceback:
                self.logger.error(f"Traceback: {str(traceback)}")
        except Exception as e:
            self.logger.error(f"Error in async_log_failure_event: {str(e)}")

# Create logger instance
litellm_file_logger = LiteLLMFileLogger()

# Set up other litellm configuration
litellm.modify_params = True  # This helps with Anthropic API compatibility
litellm.num_retries = 5  # Global retries setting
litellm.retry_on = ["429", "timeout", "rate_limit_error"]  # Retry on these error types

# Add the file logger to litellm callbacks
litellm.success_callback = [litellm_file_logger]
litellm.failure_callback = [litellm_file_logger]

# Configure logging
logger.info(f"Configured LiteLLM to write logs to: {log_file_path}")

# Export functions for external use
__all__ = ['LLMManager']

class LLMManager:
    """Manager for LLM configurations and interactions."""
    
    # Circuit breaker for embeddings to prevent repeated failures
    _embedding_failures = {}  # Track failures by provider
    _embedding_failure_threshold = 3  # Number of failures before circuit opens
    _circuit_reset_time = 300  # Reset circuit after 5 minutes
    
    @staticmethod
    async def configure_litellm(model: str) -> Dict[str, Any]:
        """
        Configure litellm for the specified model.
        
        Args:
            model: Model identifier to configure
            
        Returns:
            Dict[str, Any]: Model configuration parameters for litellm
            
        Raises:
            ValueError: If model configuration is not found
            Exception: For other configuration errors
        """
        # Get model configuration from database using ModelConfigService
        async with UnitOfWork() as uow:
            model_config_service = await ModelConfigService.from_unit_of_work(uow)
            model_config_dict = await model_config_service.get_model_config(model)
            
        # Check if model configuration was found
        if not model_config_dict:
            raise ValueError(f"Model {model} not found in the database")
            
        # Extract provider and other configuration details
        provider = model_config_dict["provider"]
        model_name = model_config_dict["name"]
        
        logger.info(f"Using provider: {provider} for model: {model}")
        
        # Set up model parameters for litellm
        model_params = {
            "model": model_name
        }
        
        # Get API key for the provider using ApiKeysService
        if provider in [ModelProvider.OPENAI, ModelProvider.ANTHROPIC, ModelProvider.DEEPSEEK]:
            # Get API key using the provider name
            api_key = await ApiKeysService.get_provider_api_key(provider)
            if api_key:
                model_params["api_key"] = api_key
            else:
                logger.warning(f"No API key found for provider: {provider}")
        
        # Handle provider-specific configurations
        if provider == ModelProvider.DEEPSEEK:
            model_params["api_base"] = os.getenv("DEEPSEEK_ENDPOINT", "https://api.deepseek.com")
            if "deepseek/" not in model_params["model"]:
                model_params["model"] = f"deepseek/{model_params['model']}"
        elif provider == ModelProvider.OLLAMA:
            model_params["api_base"] = os.getenv("OLLAMA_API_BASE", "http://localhost:11434")
            # Normalize model name: replace hyphen with colon for Ollama models
            normalized_model_name = model_name
            if "-" in normalized_model_name:
                normalized_model_name = normalized_model_name.replace("-", ":")
            prefixed_model = f"ollama/{normalized_model_name}"
            model_params["model"] = prefixed_model
        elif provider == ModelProvider.DATABRICKS:
            # Use enhanced Databricks authentication system
            try:
                from src.utils.databricks_auth import is_databricks_apps_environment, setup_environment_variables
                
                # Check if running in Databricks Apps environment
                if is_databricks_apps_environment():
                    logger.info("Using Databricks Apps OAuth authentication for model service")
                    # Environment variables will be set up automatically by the enhanced auth system
                    setup_environment_variables()
                    # Don't set api_key - let OAuth handle authentication
                else:
                    # Only use API key service when NOT in Databricks Apps context
                    token = await ApiKeysService.get_api_key_value(key_name="DATABRICKS_TOKEN")
                    if not token:
                        token = await ApiKeysService.get_api_key_value(key_name="DATABRICKS_API_KEY")
                    
                    if token:
                        model_params["api_key"] = token
                        # Set environment variables to prevent reading from .databrickscfg
                        os.environ["DATABRICKS_TOKEN"] = token
                    else:
                        logger.warning("No Databricks token found and not in Databricks Apps environment")
                        
            except ImportError:
                logger.warning("Enhanced Databricks auth not available, using legacy PAT authentication")
                # Fallback to legacy PAT authentication
                token = await ApiKeysService.get_api_key_value(key_name="DATABRICKS_TOKEN")
                if not token:
                    token = await ApiKeysService.get_api_key_value(key_name="DATABRICKS_API_KEY")
                
                if token:
                    model_params["api_key"] = token
                    os.environ["DATABRICKS_TOKEN"] = token
                
            # Get workspace URL from environment first, then database configuration
            workspace_url = os.getenv("DATABRICKS_HOST", "")
            if workspace_url:
                # Ensure proper format
                if not workspace_url.startswith('https://'):
                    workspace_url = f"https://{workspace_url}"
                model_params["api_base"] = f"{workspace_url.rstrip('/')}/serving-endpoints"
                logger.info(f"Using Databricks workspace URL from environment: {workspace_url}")
            else:
                # Fallback to database configuration or endpoint env var
                model_params["api_base"] = os.getenv("DATABRICKS_ENDPOINT", "")
                if not model_params["api_base"]:
                    from src.services.databricks_service import DatabricksService
                    try:
                        async with UnitOfWork() as uow:
                            databricks_service = await DatabricksService.from_unit_of_work(uow)
                            config = await databricks_service.get_databricks_config()
                            if config and config.workspace_url:
                                workspace_url = config.workspace_url.rstrip('/')
                                if not workspace_url.startswith('https://'):
                                    workspace_url = f"https://{workspace_url}"
                                model_params["api_base"] = f"{workspace_url}/serving-endpoints"
                                logger.info(f"Using workspace URL from database: {workspace_url}")
                            else:
                                # Try to get from enhanced auth system
                                try:
                                    from src.utils.databricks_auth import _databricks_auth
                                    if hasattr(_databricks_auth, '_workspace_host') and _databricks_auth._workspace_host:
                                        workspace_url = _databricks_auth._workspace_host
                                        model_params["api_base"] = f"{workspace_url}/serving-endpoints"
                                        logger.info(f"Using workspace URL from enhanced auth: {workspace_url}")
                                except Exception as e:
                                    logger.debug(f"Could not get workspace URL from enhanced auth: {e}")
                    except Exception as e:
                        logger.error(f"Error getting Databricks workspace URL: {e}")
            
            # For Databricks with LiteLLM, we need the databricks/ prefix for provider identification
            if not model_params["model"].startswith("databricks/"):
                model_params["model"] = f"databricks/{model_params['model']}"
            
            # Debug logging
            logger.info(f"Databricks model params: model={model_params.get('model')}, api_base={model_params.get('api_base')}, has_api_key={bool(model_params.get('api_key'))}")
        elif provider == ModelProvider.GEMINI:
            # For Gemini, get the API key
            api_key = await ApiKeysService.get_provider_api_key(provider)
            # Set in environment variables for better compatibility with various libraries
            if api_key:
                model_params["api_key"] = api_key
                os.environ["GEMINI_API_KEY"] = api_key
                os.environ["GOOGLE_API_KEY"] = api_key
                
                # Set configuration for better tool/function handling with Instructor
                os.environ["INSTRUCTOR_MODEL_NAME"] = "gemini"
                
                # Configure compatibility mode for Pydantic schema conversion
                if "LITELLM_GEMINI_PYDANTIC_COMPAT" not in os.environ:
                    os.environ["LITELLM_GEMINI_PYDANTIC_COMPAT"] = "true"
            else:
                logger.warning(f"No API key found for provider: {provider}")
                
            # Configure the model with the proper prefix for direct Google AI API
            # NOT using Vertex AI which requires application default credentials
            model_params["model"] = f"gemini/{model_name}"
        
        return model_params

    @staticmethod
    async def configure_crewai_llm(model_name: str) -> LLM:
        """
        Create and configure a CrewAI LLM instance with the correct provider prefix.
        
        Args:
            model_name: The model identifier to configure
            
        Returns:
            LLM: Configured CrewAI LLM instance
            
        Raises:
            ValueError: If model configuration is not found
            Exception: For other configuration errors
        """
        # Get model configuration using ModelConfigService
        async with UnitOfWork() as uow:
            model_config_service = await ModelConfigService.from_unit_of_work(uow)
            model_config_dict = await model_config_service.get_model_config(model_name)
        
        # Check if model configuration was found
        if not model_config_dict:
            raise ValueError(f"Model {model_name} not found in the database")
        
        # Extract provider and model name
        provider = model_config_dict["provider"]
        model_name_value = model_config_dict["name"]
        
        logger.info(f"Configuring CrewAI LLM with provider: {provider}, model: {model_name}")
        
        # Get API key for the provider using ApiKeysService
        api_key = None
        api_base = None
        
        # Set the correct provider prefix based on provider
        if provider == ModelProvider.DEEPSEEK:
            api_key = await ApiKeysService.get_provider_api_key(provider)
            api_base = os.getenv("DEEPSEEK_ENDPOINT", "https://api.deepseek.com")
            prefixed_model = f"deepseek/{model_name_value}"
        elif provider == ModelProvider.OPENAI:
            api_key = await ApiKeysService.get_provider_api_key(provider)
            # OpenAI doesn't need a prefix
            prefixed_model = model_name_value
        elif provider == ModelProvider.ANTHROPIC:
            api_key = await ApiKeysService.get_provider_api_key(provider)
            prefixed_model = f"anthropic/{model_name_value}"
        elif provider == ModelProvider.OLLAMA:
            api_base = os.getenv("OLLAMA_API_BASE", "http://localhost:11434")
            # Normalize model name: replace hyphen with colon for Ollama models
            normalized_model_name = model_name_value
            if "-" in normalized_model_name:
                normalized_model_name = normalized_model_name.replace("-", ":")
            prefixed_model = f"ollama/{normalized_model_name}"
        elif provider == ModelProvider.DATABRICKS:
            # Use enhanced Databricks authentication for CrewAI LLM
            try:
                from src.utils.databricks_auth import is_databricks_apps_environment, setup_environment_variables
                
                # Check if running in Databricks Apps environment
                if is_databricks_apps_environment():
                    logger.info("Using Databricks Apps OAuth authentication for CrewAI LLM")
                    # Setup environment variables for LiteLLM compatibility
                    setup_environment_variables()
                    api_key = None  # OAuth will be handled by environment variables
                else:
                    # Only use API key service when NOT in Databricks Apps context
                    api_key = await ApiKeysService.get_provider_api_key("DATABRICKS")
                    
            except ImportError:
                logger.warning("Enhanced Databricks auth not available for CrewAI LLM, using legacy PAT")
                api_key = await ApiKeysService.get_provider_api_key("DATABRICKS")
                
            # Get workspace URL from environment first, then database
            workspace_url = os.getenv("DATABRICKS_HOST", "")
            if workspace_url:
                # Ensure proper format
                if not workspace_url.startswith('https://'):
                    workspace_url = f"https://{workspace_url}"
                api_base = f"{workspace_url.rstrip('/')}/serving-endpoints"
                logger.info(f"Using Databricks workspace URL from environment for CrewAI: {workspace_url}")
            else:
                # Fallback to DATABRICKS_ENDPOINT or database
                api_base = os.getenv("DATABRICKS_ENDPOINT", "")
                
                # Try to get workspace URL from database if not set
                if not api_base:
                    try:
                        from src.services.databricks_service import DatabricksService
                        async with UnitOfWork() as uow:
                            databricks_service = await DatabricksService.from_unit_of_work(uow)
                            config = await databricks_service.get_databricks_config()
                            if config and config.workspace_url:
                                workspace_url = config.workspace_url.rstrip('/')
                                if not workspace_url.startswith('https://'):
                                    workspace_url = f"https://{workspace_url}"
                                api_base = f"{workspace_url}/serving-endpoints"
                                logger.info(f"Using workspace URL from database for CrewAI: {workspace_url}")
                    except Exception as e:
                        logger.error(f"Error getting Databricks workspace URL for CrewAI: {e}")
            
            prefixed_model = f"databricks/{model_name_value}"
            
            # Ensure the model string explicitly includes the provider for CrewAI/LiteLLM compatibility
            llm_params = {
                "model": prefixed_model,
                # Add built-in retry capability
                "timeout": 120,  # Longer timeout to prevent premature failures
            }
            
            # Add API key and base URL if available
            if api_key:
                llm_params["api_key"] = api_key
            if api_base:
                llm_params["api_base"] = api_base
            
            # Add max_output_tokens if defined in model config
            if "max_output_tokens" in model_config_dict and model_config_dict["max_output_tokens"]:
                llm_params["max_tokens"] = model_config_dict["max_output_tokens"]
                logger.info(f"Setting max_tokens to {model_config_dict['max_output_tokens']} for model {prefixed_model}")
                
            logger.info(f"Creating CrewAI LLM with model: {prefixed_model}, has_api_key: {bool(api_key)}, api_base: {api_base}")
            return LLM(**llm_params)
        elif provider == ModelProvider.GEMINI:
            api_key = await ApiKeysService.get_provider_api_key(provider)
            # Set in environment variables for better compatibility with various libraries
            if api_key:
                os.environ["GEMINI_API_KEY"] = api_key
                os.environ["GOOGLE_API_KEY"] = api_key
                
                # Set configuration for better tool/function handling with Instructor
                os.environ["INSTRUCTOR_MODEL_NAME"] = "gemini"
                
                # Configure compatibility mode for Pydantic schema conversion
                if "LITELLM_GEMINI_PYDANTIC_COMPAT" not in os.environ:
                    os.environ["LITELLM_GEMINI_PYDANTIC_COMPAT"] = "true"
                    
            prefixed_model = f"gemini/{model_name_value}"
        else:
            # Default fallback for other providers - use LiteLLM provider prefixing convention
            logger.warning(f"Using default model name format for provider: {provider}")
            prefixed_model = f"{provider.lower()}/{model_name_value}" if provider else model_name_value
        
        # Configure LLM parameters (for all providers except Databricks which returns early)
        llm_params = {
            "model": prefixed_model,
            # Add built-in retry capability
            "timeout": 120,  # Longer timeout to prevent premature failures
        }
        
        # Add API key and base URL if available
        if api_key:
            llm_params["api_key"] = api_key
        if api_base:
            llm_params["api_base"] = api_base
        
        # Add max_output_tokens if defined in model config
        if "max_output_tokens" in model_config_dict and model_config_dict["max_output_tokens"]:
            llm_params["max_tokens"] = model_config_dict["max_output_tokens"]
            logger.info(f"Setting max_tokens to {model_config_dict['max_output_tokens']} for model {prefixed_model}")
        
        # Create and return the CrewAI LLM
        logger.info(f"Creating CrewAI LLM with model: {prefixed_model}")
        return LLM(**llm_params)

    @staticmethod
    async def get_llm(model_name: str) -> LLM:
        """
        Create a CrewAI LLM instance for the specified model.
        
        Args:
            model_name: The model identifier to configure
            
        Returns:
            LLM: CrewAI LLM instance
        """
        # Get standard LLM configuration
        llm = await LLMManager.configure_crewai_llm(model_name)
        return llm

    @staticmethod
    async def get_embedding(text: str, model: str = "databricks-gte-large-en", embedder_config: Optional[Dict[str, Any]] = None) -> Optional[List[float]]:
        """
        Get an embedding vector for the given text using configurable embedder.
        
        Args:
            text: The text to create an embedding for
            model: The embedding model to use (can be overridden by embedder_config)
            embedder_config: Optional embedder configuration with provider and model settings
            
        Returns:
            List[float]: The embedding vector or None if creation fails
        """
        provider = 'databricks'  # Default provider
        try:
            # Determine provider and model from embedder_config or defaults
            if embedder_config:
                provider = embedder_config.get('provider', 'databricks')
                config = embedder_config.get('config', {})
                embedding_model = config.get('model', model)
            else:
                provider = 'databricks'
                embedding_model = model
            
            # Check circuit breaker for this provider
            current_time = time.time()
            if provider in LLMManager._embedding_failures:
                failure_info = LLMManager._embedding_failures[provider]
                failure_count = failure_info.get('count', 0)
                last_failure_time = failure_info.get('last_failure', 0)
                
                # If circuit is open, check if it should be reset
                if failure_count >= LLMManager._embedding_failure_threshold:
                    if current_time - last_failure_time < LLMManager._circuit_reset_time:
                        logger.warning(f"Circuit breaker OPEN for {provider} embeddings. Failing fast.")
                        return None
                    else:
                        # Reset circuit after timeout
                        logger.info(f"Resetting circuit breaker for {provider} embeddings")
                        LLMManager._embedding_failures[provider] = {'count': 0, 'last_failure': 0}
            
            logger.info(f"Creating embedding using provider: {provider}, model: {embedding_model}")
            
            # Handle different embedding providers
            if provider == 'databricks' or 'databricks' in embedding_model:
                # Use enhanced Databricks authentication for embeddings - follow GenieTool pattern
                try:
                    from src.utils.databricks_auth import is_databricks_apps_environment, get_databricks_auth_headers
                    
                    # First try: OBO authentication if available
                    logger.info("Attempting enhanced Databricks authentication for embeddings")
                    headers_result, error = await get_databricks_auth_headers()
                    if headers_result and not error:
                        logger.info("Using enhanced Databricks authentication (OAuth/OBO) for embeddings")
                        headers = headers_result
                        api_key = None  # OAuth handled by headers
                    else:
                        logger.info(f"Enhanced auth failed ({error}), falling back to API key service")
                        # Second try: API key from service
                        api_key = await ApiKeysService.get_provider_api_key("DATABRICKS")
                        if api_key:
                            logger.info("Using API key from service for embeddings")
                            headers = None
                        else:
                            # Third try: Client credentials from environment
                            client_id = os.getenv("DATABRICKS_CLIENT_ID")
                            client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
                            if client_id and client_secret:
                                logger.info("Using client credentials for embeddings")
                                # Let the enhanced auth handle client credentials
                                headers_result, error = await get_databricks_auth_headers()
                                if headers_result and not error:
                                    headers = headers_result
                                    api_key = None
                                else:
                                    # Fourth try: Environment variable DATABRICKS_TOKEN
                                    api_key = os.getenv("DATABRICKS_TOKEN") or os.getenv("DATABRICKS_API_KEY")
                                    if api_key:
                                        logger.info("Using DATABRICKS_TOKEN from environment for embeddings")
                                        headers = None
                                    else:
                                        logger.error("No Databricks authentication method available")
                                        return None
                            else:
                                # Fourth try: Environment variable DATABRICKS_TOKEN
                                api_key = os.getenv("DATABRICKS_TOKEN") or os.getenv("DATABRICKS_API_KEY")
                                if api_key:
                                    logger.info("Using DATABRICKS_TOKEN from environment for embeddings")
                                    headers = None
                                else:
                                    logger.error("No Databricks authentication method available")
                                    return None
                        
                except ImportError:
                    logger.warning("Enhanced Databricks auth not available for embeddings, using fallback methods")
                    # Try API key service first
                    api_key = await ApiKeysService.get_provider_api_key("DATABRICKS")
                    if not api_key:
                        # Fall back to environment variable
                        api_key = os.getenv("DATABRICKS_TOKEN") or os.getenv("DATABRICKS_API_KEY")
                        if api_key:
                            logger.info("Using DATABRICKS_TOKEN from environment for embeddings (no enhanced auth)")
                    headers = None
                
                # Get workspace URL from environment first, then database
                workspace_url = os.getenv("DATABRICKS_HOST", "")
                if workspace_url:
                    # Ensure proper format
                    if not workspace_url.startswith('https://'):
                        workspace_url = f"https://{workspace_url}"
                    api_base = f"{workspace_url.rstrip('/')}/serving-endpoints"
                    logger.info(f"Using Databricks workspace URL from environment for embeddings: {workspace_url}")
                else:
                    # Fallback to database configuration
                    api_base = None
                    from src.services.databricks_service import DatabricksService
                    try:
                        async with UnitOfWork() as uow:
                            databricks_service = await DatabricksService.from_unit_of_work(uow)
                            config = await databricks_service.get_databricks_config()
                            if config and config.workspace_url:
                                workspace_url = config.workspace_url.rstrip('/')
                                if not workspace_url.startswith('https://'):
                                    workspace_url = f"https://{workspace_url}"
                                api_base = f"{workspace_url}/serving-endpoints"
                                logger.info(f"Using workspace URL from database for embeddings: {workspace_url}")
                    except Exception as e:
                        logger.error(f"Error getting Databricks workspace URL for embeddings: {e}")
                
                # Check if we have either OAuth headers or API key + base URL
                if not ((headers and api_base) or (api_key and api_base)):
                    logger.warning(f"Missing Databricks credentials - OAuth headers: {bool(headers)}, API key: {bool(api_key)}, API base: {bool(api_base)}")
                    return None
                
                # Ensure model has databricks prefix for litellm
                if not embedding_model.startswith('databricks/'):
                    embedding_model = f"databricks/{embedding_model}"
                
                # Use direct HTTP request to avoid config file issues
                import aiohttp
                
                try:
                    # Construct the direct API endpoint
                    endpoint_url = f"{api_base}/{embedding_model.replace('databricks/', '')}/invocations"
                    
                    # Use OAuth headers if available, otherwise fall back to API key
                    if headers:
                        request_headers = headers.copy()
                        if "Content-Type" not in request_headers:
                            request_headers["Content-Type"] = "application/json"
                    else:
                        request_headers = {
                            "Authorization": f"Bearer {api_key}",
                            "Content-Type": "application/json"
                        }
                    
                    payload = {
                        "input": [text] if isinstance(text, str) else text
                    }
                    
                    async with aiohttp.ClientSession() as session:
                        async with session.post(endpoint_url, headers=request_headers, json=payload) as response:
                            if response.status == 200:
                                result = await response.json()
                                # Databricks embedding API returns embeddings in 'data' field
                                if 'data' in result and len(result['data']) > 0:
                                    embedding = result['data'][0].get('embedding', result['data'][0])
                                    logger.info(f"Successfully created embedding with {len(embedding)} dimensions using direct Databricks API")
                                    return embedding
                                else:
                                    logger.warning("No embedding data found in Databricks response")
                                    return None
                            else:
                                error_text = await response.text()
                                logger.error(f"Databricks embedding API error {response.status}: {error_text}")
                                return None
                                
                except Exception as e:
                    logger.error(f"Error calling Databricks embedding API directly: {str(e)}")
                    return None
                
            elif provider == 'ollama':
                # Use Ollama for embeddings
                api_base = os.getenv("OLLAMA_API_BASE", "http://localhost:11434")
                
                # Ensure model has ollama prefix
                if not embedding_model.startswith('ollama/'):
                    embedding_model = f"ollama/{embedding_model}"
                
                response = await litellm.aembedding(
                    model=embedding_model,
                    input=text,
                    api_base=api_base
                )
                
            elif provider == 'google':
                # Use Google AI for embeddings
                api_key = await ApiKeysService.get_provider_api_key(ModelProvider.GEMINI)
                
                if not api_key:
                    logger.warning("No Google API key found for creating embeddings")
                    return None
                
                # Ensure model has gemini prefix for embeddings
                if not embedding_model.startswith('gemini/'):
                    embedding_model = f"gemini/{embedding_model}"
                
                response = await litellm.aembedding(
                    model=embedding_model,
                    input=text,
                    api_key=api_key
                )
                
            else:
                # Default to OpenAI for embeddings
                api_key = await ApiKeysService.get_provider_api_key(ModelProvider.OPENAI)
                
                if not api_key:
                    logger.warning("No OpenAI API key found for creating embeddings")
                    return None
                    
                # Create the embedding using litellm
                response = await litellm.aembedding(
                    model=embedding_model,
                    input=text,
                    api_key=api_key
                )
            
            # Extract the embedding vector
            if response and "data" in response and len(response["data"]) > 0:
                embedding = response["data"][0]["embedding"]
                logger.info(f"Successfully created embedding with {len(embedding)} dimensions using {provider}")
                # Reset failure count on success
                if provider in LLMManager._embedding_failures:
                    LLMManager._embedding_failures[provider] = {'count': 0, 'last_failure': 0}
                return embedding
            else:
                logger.warning("Failed to get embedding from response")
                # Track failure
                if provider not in LLMManager._embedding_failures:
                    LLMManager._embedding_failures[provider] = {'count': 0, 'last_failure': 0}
                LLMManager._embedding_failures[provider]['count'] += 1
                LLMManager._embedding_failures[provider]['last_failure'] = time.time()
                return None
                
        except Exception as e:
            logger.error(f"Error creating embedding: {str(e)}")
            # Track failure for circuit breaker
            if provider not in LLMManager._embedding_failures:
                LLMManager._embedding_failures[provider] = {'count': 0, 'last_failure': 0}
            LLMManager._embedding_failures[provider]['count'] += 1
            LLMManager._embedding_failures[provider]['last_failure'] = time.time()
            
            # Log circuit breaker status
            failure_count = LLMManager._embedding_failures[provider]['count']
            if failure_count >= LLMManager._embedding_failure_threshold:
                logger.error(f"Circuit breaker tripped for {provider} embeddings after {failure_count} failures")
            
            return None
