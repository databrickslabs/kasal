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
    def __init__(self):
        self.file_path = log_file_path
        # Set up a file logger
        self.logger = logging.getLogger("litellm_file_logger")
        self.logger.setLevel(logging.DEBUG)
        # Check if handlers already exist to avoid duplicates
        if not self.logger.handlers:
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
            # For Databricks, we need to get a token, not an API key
            token = await ApiKeysService.get_provider_api_key("DATABRICKS")
            if token:
                model_params["api_key"] = token
            model_params["api_base"] = os.getenv("DATABRICKS_ENDPOINT", "")
            if "databricks/" not in model_params["model"]:
                model_params["model"] = f"databricks/{model_params['model']}"
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
            api_key = await ApiKeysService.get_provider_api_key("DATABRICKS")
            api_base = os.getenv("DATABRICKS_ENDPOINT", "")
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
                
            logger.info(f"Creating CrewAI LLM with model: {prefixed_model}")
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
    async def get_embedding(text: str, model: str = "text-embedding-ada-002", embedder_config: Optional[Dict[str, Any]] = None) -> Optional[List[float]]:
        """
        Get an embedding vector for the given text using configurable embedder.
        
        Args:
            text: The text to create an embedding for
            model: The embedding model to use (can be overridden by embedder_config)
            embedder_config: Optional embedder configuration with provider and model settings
            
        Returns:
            List[float]: The embedding vector or None if creation fails
        """
        try:
            # Determine provider and model from embedder_config or defaults
            if embedder_config:
                provider = embedder_config.get('provider', 'openai')
                config = embedder_config.get('config', {})
                embedding_model = config.get('model', model)
            else:
                provider = 'openai'
                embedding_model = model
            
            logger.info(f"Creating embedding using provider: {provider}, model: {embedding_model}")
            
            # Handle different embedding providers
            if provider == 'databricks' or 'databricks' in embedding_model:
                # Use Databricks for embeddings
                api_key = await ApiKeysService.get_provider_api_key("DATABRICKS")
                api_base = os.getenv("DATABRICKS_ENDPOINT", "")
                
                if not api_key:
                    logger.warning("No Databricks API key found for creating embeddings")
                    return None
                
                # Ensure model has databricks prefix for litellm
                if not embedding_model.startswith('databricks/'):
                    if embedding_model == 'databricks-gte-large-en' or 'databricks' in embedding_model:
                        # Keep the model name as is for Databricks models
                        pass
                    else:
                        embedding_model = f"databricks/{embedding_model}"
                
                # Create the embedding using litellm with Databricks
                response = await litellm.aembedding(
                    model=embedding_model,
                    input=text,
                    api_key=api_key,
                    api_base=api_base
                )
                
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
                return embedding
            else:
                logger.warning("Failed to get embedding from response")
                return None
                
        except Exception as e:
            logger.error(f"Error creating embedding: {str(e)}")
            return None
