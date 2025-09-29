"""
LLM Manager for handling model configuration and LLM interactions.

This module provides a centralized manager for configuring and interacting with
different LLM providers through litellm.
"""

import logging
import os
import json
from typing import Dict, Any, List, Optional, Union
import time

from crewai import LLM
from src.schemas.model_provider import ModelProvider
from src.utils.databricks_url_utils import DatabricksURLUtils
from src.services.model_config_service import ModelConfigService
from src.services.api_keys_service import ApiKeysService
from src.core.unit_of_work import UnitOfWork
import pathlib

# CRITICAL: Import and apply model handlers BEFORE importing litellm
# This ensures the monkey patches are applied to handle model-specific responses
from src.core.llm_handlers.databricks_gpt_oss_handler import DatabricksGPTOSSHandler, DatabricksGPTOSSLLM

# Now import litellm after the monkey patch has been applied
import litellm
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

# Import LoggerManager for documentation embedding logger
from src.core.logger import LoggerManager
embedding_logger = LoggerManager.get_instance().documentation_embedding

# Set drop_params to True to automatically drop unsupported parameters
# This is especially useful for GPT-5 and other new models that may have different parameter support
# Note: With litellm 1.75.8+, GPT-5 is natively supported
litellm.drop_params = True
logger.info("Set litellm.drop_params=True to handle unsupported parameters gracefully")
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

# Configure MLflow integration for Databricks observability
_mlflow_configured = False
# Default to disabled globally; we enable per-workspace dynamically
_use_mlflow = False

def _configure_databricks_mlflow():
    """Configure MLflow using existing Databricks authentication system"""
    global _mlflow_configured

    if _mlflow_configured is True:
        return

    try:
        import mlflow
        from src.utils.databricks_auth import setup_environment_variables

        # Use existing authentication system - this sets DATABRICKS_HOST and DATABRICKS_TOKEN
        # using the same OBO/PAT logic as other Databricks services
        if setup_environment_variables():
            workspace_host = os.getenv("DATABRICKS_HOST", "")
            if workspace_host:
                # MLflow will automatically use DATABRICKS_HOST and DATABRICKS_TOKEN environment variables
                tracking_uri = "databricks"  # Simple form - uses environment variables
                mlflow.set_tracking_uri(tracking_uri)

                # Set up experiment for LLM operations
                experiment_name = os.getenv("MLFLOW_EXPERIMENT_NAME", "/Shared/kasal-llm-operations")
                try:
                    exp = mlflow.set_experiment(experiment_name)
                    logger.info(f"MLflow experiment set to: {experiment_name} (ID: {getattr(exp, 'experiment_id', 'unknown')})")
                    # Explicitly configure MLflow 3.x tracing destination and enable it
                    tracing_ok = False
                    try:
                        from mlflow.tracing.destination import Databricks as _Dest
                        mlflow.tracing.set_destination(_Dest(experiment_id=str(getattr(exp, "experiment_id", ""))))
                        mlflow.tracing.enable()
                        logger.info("MLflow tracing destination set and tracing enabled")
                        tracing_ok = True
                    except Exception as te:
                        logger.warning(f"Could not configure MLflow tracing destination: {te}")
                        tracing_ok = False
                except Exception as e:
                    logger.warning(f"Could not set MLflow experiment '{experiment_name}': {e}")
                    # Continue with default experiment
                    tracing_ok = False

                # Enable comprehensive MLflow autolog for both LiteLLM and CrewAI
                # This dual approach ensures maximum coverage of LLM calls

                # 1. Enable LiteLLM autolog (captures underlying LLM calls)
                try:
                    mlflow.litellm.autolog(log_traces=tracing_ok)
                    logger.info(f"✅ MLflow LiteLLM autolog enabled (log_traces={tracing_ok})")
                except Exception as e:
                    logger.warning(f"Failed to enable MLflow LiteLLM autolog: {e}")

                # 2. Enable CrewAI autolog (captures CrewAI workflow structure)
                try:
                    mlflow.crewai.autolog()
                    logger.info("✅ MLflow CrewAI autolog enabled")
                except AttributeError:
                    logger.warning("⚠️ MLflow CrewAI autolog not available (older MLflow version or integration issues)")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to enable CrewAI autolog: {e}")

                # Note: CrewAI uses LiteLLM internally, so LiteLLM autolog should capture
                # the underlying calls even when using CrewAI's LLM wrapper

                _mlflow_configured = True
                logger.info(f"Databricks MLflow configured successfully using existing authentication")

            else:
                logger.info("No Databricks workspace available for MLflow")
        else:
            logger.info("Databricks authentication not available for MLflow")

    except ImportError:
        logger.info("MLflow not available - install with 'pip install mlflow' for Databricks observability")
    except Exception as e:
        logger.warning(f"Failed to configure Databricks MLflow: {e}")

# Configure litellm callbacks to file logger by default
litellm.success_callback = [litellm_file_logger]
litellm.failure_callback = [litellm_file_logger]
logger.info("Using file-based logging for LLM observability")

# Configure logging
logger.info(f"Configured LiteLLM to write logs to: {log_file_path}")

# Enhanced LLM Wrapper for MLflow Integration
class MLflowTrackedLLM:
    """
    Wrapper around CrewAI LLM that ensures all calls are tracked in MLflow.
    This addresses the issue where CrewAI's LLM wrapper doesn't trigger MLflow autolog.
    """

    def __init__(self, llm_instance, model_name: str):
        self.llm = llm_instance
        self.model_name = model_name
        self._ensure_mlflow_configured()

    def _ensure_mlflow_configured(self):
        """Ensure MLflow is configured when LLM is used"""
        global _mlflow_configured
        if not _mlflow_configured:
            _configure_databricks_mlflow()

    def _log_llm_call(self, method_name: str, input_data, output_data, duration: float):
        """Manually log LLM call to MLflow if autolog didn't capture it"""
        try:
            import mlflow
            import time

            # Only log if we have an active MLflow session
            if _mlflow_configured and _use_mlflow:
                # Try to get the current run, or create one if needed
                run = mlflow.active_run()
                if not run:
                    # Start a run for this LLM call
                    mlflow.start_run(run_name=f"crewai_llm_{self.model_name}")
                    run = mlflow.active_run()

                if run:
                    # Log the LLM call details
                    mlflow.log_metric(f"llm_call_duration_{method_name}", duration)
                    mlflow.log_param(f"llm_model", self.model_name)
                    mlflow.log_param(f"llm_method", method_name)

                    # Log input/output lengths for tracking
                    if isinstance(input_data, str):
                        mlflow.log_metric(f"input_length_{method_name}", len(input_data))
                    if isinstance(output_data, str):
                        mlflow.log_metric(f"output_length_{method_name}", len(output_data))
                    elif hasattr(output_data, 'content'):
                        mlflow.log_metric(f"output_length_{method_name}", len(str(output_data.content)))

                    logger.info(f"✅ MLflow logged CrewAI LLM call: {method_name} for {self.model_name}")

        except Exception as e:
            logger.warning(f"Failed to log LLM call to MLflow: {e}")

    def invoke(self, input_data, **kwargs):
        """Tracked version of LLM invoke"""
        import time
        start_time = time.time()

        try:
            result = self.llm.invoke(input_data, **kwargs)
            duration = time.time() - start_time
            self._log_llm_call("invoke", input_data, result, duration)
            return result
        except Exception as e:
            duration = time.time() - start_time
            self._log_llm_call("invoke_error", input_data, str(e), duration)
            raise

    async def ainvoke(self, input_data, **kwargs):
        """Tracked version of LLM ainvoke"""
        import time
        start_time = time.time()

        try:
            result = await self.llm.ainvoke(input_data, **kwargs)
            duration = time.time() - start_time
            self._log_llm_call("ainvoke", input_data, result, duration)
            return result
        except Exception as e:
            duration = time.time() - start_time
            self._log_llm_call("ainvoke_error", input_data, str(e), duration)
            raise

    def __call__(self, input_data, **kwargs):
        """Tracked version of LLM __call__"""
        import time
        start_time = time.time()

        try:
            result = self.llm(input_data, **kwargs)
            duration = time.time() - start_time
            self._log_llm_call("call", input_data, result, duration)
            return result
        except Exception as e:
            duration = time.time() - start_time
            self._log_llm_call("call_error", input_data, str(e), duration)
            raise

    def __getattr__(self, name):
        """Delegate all other attributes to the wrapped LLM"""
        return getattr(self.llm, name)

# Export functions for external use
__all__ = ['LLMManager', 'DatabricksGPTOSSHandler', 'DatabricksGPTOSSLLM', 'MLflowTrackedLLM']

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
        from src.db.session import async_session_factory

        async with async_session_factory() as session:
            model_config_service = ModelConfigService(session)
            model_config_dict = await model_config_service.get_model_config(model)
            
        # Check if model configuration was found
        if not model_config_dict:
            raise ValueError(f"Model {model} not found in the database")
            
        # Extract provider and other configuration details
        provider = model_config_dict["provider"]
        model_name = model_config_dict["name"]
        
        logger.info(f"Using provider: {provider} for model: {model}")
        
        # Set up model parameters for litellm
        # Use longer timeout for GPT-5 models as they take more time to respond
        timeout_value = 300 if (provider == ModelProvider.OPENAI and "gpt-5" in model_name.lower()) else 120
        
        model_params = {
            "model": model_name,
            "timeout": timeout_value  # Extended timeout for GPT-5 (300s), standard for others (120s)
        }
        
        # GPT-5 doesn't support certain parameters - remove them
        if provider == ModelProvider.OPENAI and "gpt-5" in model_name.lower():
            # Set drop_params for this specific call
            model_params["drop_params"] = True
            # Also specify additional params to drop that litellm might not know about
            model_params["additional_drop_params"] = ["stop", "presence_penalty", "frequency_penalty", "logit_bias"]
            logger.info(f"Enabled drop_params and additional_drop_params for GPT-5 model: {model_name}")
        
        if timeout_value == 300:
            logger.info(f"Using extended timeout of {timeout_value}s for GPT-5 model in litellm: {model_name}")
        
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
                # Use centralized URL utility for consistent handling
                model_params["api_base"] = DatabricksURLUtils.construct_serving_endpoints_url(workspace_url)
                logger.info(f"Using Databricks workspace URL from environment: {workspace_url}")
            else:
                # Fallback to database configuration or endpoint env var
                model_params["api_base"] = os.getenv("DATABRICKS_ENDPOINT", "")
                if not model_params["api_base"]:
                    from src.services.databricks_service import DatabricksService
                    from src.db.session import async_session_factory
                    try:
                        async with async_session_factory() as session:
                            databricks_service = DatabricksService(session)
                            config = await databricks_service.get_databricks_config()
                            if config and config.workspace_url:
                                workspace_url = config.workspace_url
                                # Use centralized URL utility for consistent handling
                                model_params["api_base"] = DatabricksURLUtils.construct_serving_endpoints_url(workspace_url)
                                logger.info(f"Using workspace URL from database: {workspace_url}")
                            else:
                                # Try to get from enhanced auth system
                                try:
                                    from src.utils.databricks_auth import _databricks_auth
                                    if hasattr(_databricks_auth, '_workspace_host') and _databricks_auth._workspace_host:
                                        workspace_url = _databricks_auth._workspace_host
                                        # Use centralized URL utility for consistent handling
                                        model_params["api_base"] = DatabricksURLUtils.construct_serving_endpoints_url(workspace_url)
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
    async def configure_crewai_llm(model_name: str, temperature: Optional[float] = None) -> LLM:
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
        from src.db.session import async_session_factory
        async with async_session_factory() as session:
            model_config_service = ModelConfigService(session)
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
                # Use centralized URL utility for consistent handling
                api_base = DatabricksURLUtils.construct_serving_endpoints_url(workspace_url)
                logger.info(f"Using Databricks workspace URL from environment for CrewAI: {workspace_url}")
            else:
                # Fallback to DATABRICKS_ENDPOINT or database
                api_base = os.getenv("DATABRICKS_ENDPOINT", "")
                
                # Try to get workspace URL from database if not set
                if not api_base:
                    try:
                        from src.services.databricks_service import DatabricksService
                        from src.db.session import async_session_factory
                        async with async_session_factory() as session:
                            databricks_service = DatabricksService(session)
                            config = await databricks_service.get_databricks_config()
                            if config and config.workspace_url:
                                workspace_url = config.workspace_url
                                # Use centralized URL utility for consistent handling
                                api_base = DatabricksURLUtils.construct_serving_endpoints_url(workspace_url)
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
            
            # Add temperature if specified
            if temperature is not None:
                llm_params["temperature"] = temperature
                logger.info(f"Setting temperature to {temperature} for model {prefixed_model}")
            
            # Add API key and base URL if available
            if api_key:
                llm_params["api_key"] = api_key
            if api_base:
                llm_params["api_base"] = api_base
            
            # Add max_output_tokens if defined in model config
            if "max_output_tokens" in model_config_dict and model_config_dict["max_output_tokens"]:
                # GPT-5 and newer OpenAI models use max_completion_tokens instead of max_tokens
                # Since this is inside Databricks provider block, this won't apply to GPT-5
                llm_params["max_tokens"] = model_config_dict["max_output_tokens"]
                logger.info(f"Setting max_tokens to {model_config_dict['max_output_tokens']} for model {prefixed_model}")
                
            logger.info(f"Creating CrewAI LLM with model: {prefixed_model}, has_api_key: {bool(api_key)}, api_base: {api_base}")
            
            # Use custom wrapper for GPT-OSS models
            if DatabricksGPTOSSHandler.is_gpt_oss_model(model_name_value):
                logger.info(f"Using DatabricksGPTOSSLLM wrapper for GPT-OSS model: {model_name_value}")
                return DatabricksGPTOSSLLM(**llm_params)
            else:
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
        # Use longer timeout for GPT-5 models as they take more time to respond
        timeout_value = 300 if (provider == ModelProvider.OPENAI and "gpt-5" in model_name_value.lower()) else 120
        
        llm_params = {
            "model": prefixed_model,
            # Add built-in retry capability
            "timeout": timeout_value,  # Longer timeout for GPT-5 (300s), standard for others (120s)
        }
        
        # Add temperature if specified
        if temperature is not None:
            llm_params["temperature"] = temperature
            logger.info(f"Setting temperature to {temperature} for model {prefixed_model}")
        
        # GPT-5 doesn't support certain parameters - enable drop_params
        if provider == ModelProvider.OPENAI and "gpt-5" in model_name_value.lower():
            # CrewAI's LLM will pass this to litellm
            llm_params["drop_params"] = True
            # Also specify additional params to drop that litellm might not know about
            llm_params["additional_drop_params"] = ["stop", "presence_penalty", "frequency_penalty", "logit_bias"]
            logger.info(f"Enabled drop_params and additional_drop_params for GPT-5 CrewAI model: {model_name_value}")
        
        if timeout_value == 300:
            logger.info(f"Using extended timeout of {timeout_value}s for GPT-5 model: {model_name_value}")
        
        # Add API key and base URL if available
        if api_key:
            llm_params["api_key"] = api_key
        if api_base:
            llm_params["api_base"] = api_base
        
        # Add max_output_tokens if defined in model config
        if "max_output_tokens" in model_config_dict and model_config_dict["max_output_tokens"]:
            # litellm 1.75.8+ handles GPT-5 max_completion_tokens automatically
            llm_params["max_tokens"] = model_config_dict["max_output_tokens"]
            logger.info(f"Setting max_tokens to {model_config_dict['max_output_tokens']} for model {prefixed_model}")
        
        # Create and return the CrewAI LLM
        # litellm 1.75.8+ handles GPT-5 natively, no need for custom wrapper
        logger.info(f"Creating CrewAI LLM with model: {prefixed_model}")
        return LLM(**llm_params)

    @staticmethod
    async def get_llm(model_name: str, temperature: Optional[float] = None):
        """
        Create a CrewAI LLM instance for the specified model.
        If MLflow is enabled for the current workspace (group), wrap with MLflow tracking.
        """
        # Get standard LLM configuration
        llm = await LLMManager.configure_crewai_llm(model_name, temperature)

        # Determine if MLflow is enabled for this group
        try:
            from src.core.user_context import UserContext
            from src.db.session import async_session_factory
            from src.services.mlflow_service import MLflowService

            group_ctx = UserContext.get_group_context()
            group_id = getattr(group_ctx, "primary_group_id", None) if group_ctx else None

            enabled = False
            async with async_session_factory() as db:
                svc = MLflowService(db, group_id=group_id)
                enabled = await svc.is_enabled()

            if enabled:
                _configure_databricks_mlflow()
                tracked_llm = MLflowTrackedLLM(llm, model_name)
                logger.info(f"🎯 Created MLflow-tracked LLM for model: {model_name}")
                return tracked_llm
            else:
                logger.info("MLflow disabled for this workspace; returning plain LLM")
                return llm
        except Exception as e:
            logger.warning(f"Could not determine MLflow status; returning plain LLM. Error: {e}")
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
                        embedding_logger.warning(f"Circuit breaker OPEN for {provider} embeddings. Failing fast.")
                        return None
                    else:
                        # Reset circuit after timeout
                        embedding_logger.info(f"Resetting circuit breaker for {provider} embeddings")
                        LLMManager._embedding_failures[provider] = {'count': 0, 'last_failure': 0}
            
            embedding_logger.info(f"Creating embedding using provider: {provider}, model: {embedding_model}")
            
            # Handle different embedding providers
            if provider == 'databricks' or 'databricks' in embedding_model:
                # Use enhanced Databricks authentication for embeddings - follow GenieTool pattern
                try:
                    from src.utils.databricks_auth import is_databricks_apps_environment, get_databricks_auth_headers
                    
                    # First try: OBO authentication if available
                    embedding_logger.info("Attempting enhanced Databricks authentication for embeddings")
                    headers_result, error = await get_databricks_auth_headers()
                    if headers_result and not error:
                        embedding_logger.info("Using enhanced Databricks authentication (OAuth/OBO) for embeddings")
                        headers = headers_result
                        api_key = None  # OAuth handled by headers
                    else:
                        logger.info(f"Enhanced auth failed ({error}), falling back to API key service")
                        # Second try: API key from service
                        api_key = await ApiKeysService.get_provider_api_key("DATABRICKS")
                        if api_key:
                            embedding_logger.info("Using API key from service for embeddings")
                            headers = None
                        else:
                            # Third try: Client credentials from environment
                            client_id = os.getenv("DATABRICKS_CLIENT_ID")
                            client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
                            if client_id and client_secret:
                                embedding_logger.info("Using client credentials for embeddings")
                                # Let the enhanced auth handle client credentials
                                headers_result, error = await get_databricks_auth_headers()
                                if headers_result and not error:
                                    headers = headers_result
                                    api_key = None
                                else:
                                    # Fourth try: Environment variable DATABRICKS_TOKEN
                                    api_key = os.getenv("DATABRICKS_TOKEN") or os.getenv("DATABRICKS_API_KEY")
                                    if api_key:
                                        embedding_logger.info("Using DATABRICKS_TOKEN from environment for embeddings")
                                        headers = None
                                    else:
                                        logger.error("No Databricks authentication method available")
                                        return None
                            else:
                                # Fourth try: Environment variable DATABRICKS_TOKEN
                                api_key = os.getenv("DATABRICKS_TOKEN") or os.getenv("DATABRICKS_API_KEY")
                                if api_key:
                                    embedding_logger.info("Using DATABRICKS_TOKEN from environment for embeddings")
                                    headers = None
                                else:
                                    logger.error("No Databricks authentication method available")
                                    return None
                        
                except ImportError:
                    embedding_logger.warning("Enhanced Databricks auth not available for embeddings, using fallback methods")
                    # Try API key service first
                    api_key = await ApiKeysService.get_provider_api_key("DATABRICKS")
                    if not api_key:
                        # Fall back to environment variable
                        api_key = os.getenv("DATABRICKS_TOKEN") or os.getenv("DATABRICKS_API_KEY")
                        if api_key:
                            embedding_logger.info("Using DATABRICKS_TOKEN from environment for embeddings (no enhanced auth)")
                    headers = None
                
                # Get workspace URL from environment first, then database
                workspace_url = os.getenv("DATABRICKS_HOST", "")
                if workspace_url:
                    # Use centralized URL utility for consistent handling
                    api_base = DatabricksURLUtils.construct_serving_endpoints_url(workspace_url)
                    embedding_logger.info(f"Using Databricks workspace URL from environment for embeddings: {workspace_url}")
                else:
                    # Fallback to database configuration
                    api_base = None
                    from src.services.databricks_service import DatabricksService
                    from src.db.session import async_session_factory
                    try:
                        async with async_session_factory() as session:
                            databricks_service = DatabricksService(session)
                            config = await databricks_service.get_databricks_config()
                            if config and config.workspace_url:
                                workspace_url = config.workspace_url
                                # Use centralized URL utility for consistent handling
                                api_base = DatabricksURLUtils.construct_serving_endpoints_url(workspace_url)
                                embedding_logger.info(f"Using workspace URL from database for embeddings: {workspace_url}")
                    except Exception as e:
                        embedding_logger.error(f"Error getting Databricks workspace URL for embeddings: {e}")
                
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
                    # Construct the direct API endpoint using centralized utility
                    # Extract workspace URL from api_base (which contains /serving-endpoints)
                    workspace_url = DatabricksURLUtils.extract_workspace_from_endpoint(api_base)
                    endpoint_url = DatabricksURLUtils.construct_model_invocation_url(workspace_url, embedding_model)
                    
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
                    
                    timeout = aiohttp.ClientTimeout(total=float(os.getenv("EMBEDDING_HTTP_TIMEOUT_SECONDS", "30")))
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        async with session.post(endpoint_url, headers=request_headers, json=payload, timeout=timeout) as response:
                            if response.status == 200:
                                result = await response.json()
                                # Databricks embedding API returns embeddings in 'data' field
                                if 'data' in result and len(result['data']) > 0:
                                    embedding = result['data'][0].get('embedding', result['data'][0])
                                    embedding_logger.info(f"Successfully created embedding with {len(embedding)} dimensions using direct Databricks API")
                                    return embedding
                                else:
                                    embedding_logger.warning("No embedding data found in Databricks response")
                                    return None
                            else:
                                error_text = await response.text()
                                embedding_logger.error(f"Databricks embedding API error {response.status}: {error_text}")
                                return None
                                
                except Exception as e:
                    embedding_logger.error(f"Error calling Databricks embedding API directly: {str(e)}")
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
                    api_base=api_base,
                    timeout=float(os.getenv("EMBEDDING_TIMEOUT_SECONDS","60"))
                )
                
            elif provider == 'google':
                # Use Google AI for embeddings
                api_key = await ApiKeysService.get_provider_api_key(ModelProvider.GEMINI)
                
                if not api_key:
                    embedding_logger.warning("No Google API key found for creating embeddings")
                    return None
                
                # Ensure model has gemini prefix for embeddings
                if not embedding_model.startswith('gemini/'):
                    embedding_model = f"gemini/{embedding_model}"
                
                response = await litellm.aembedding(
                    model=embedding_model,
                    input=text,
                    api_key=api_key,
                    timeout=float(os.getenv("EMBEDDING_TIMEOUT_SECONDS","60"))
                )
                
            else:
                # Default to OpenAI for embeddings
                api_key = await ApiKeysService.get_provider_api_key(ModelProvider.OPENAI)
                
                if not api_key:
                    embedding_logger.warning("No OpenAI API key found for creating embeddings")
                    return None
                    
                # Create the embedding using litellm
                response = await litellm.aembedding(
                    model=embedding_model,
                    input=text,
                    api_key=api_key,
                    timeout=float(os.getenv("EMBEDDING_TIMEOUT_SECONDS","60"))
                )
            
            # Extract the embedding vector
            if response and "data" in response and len(response["data"]) > 0:
                embedding = response["data"][0]["embedding"]
                embedding_logger.info(f"Successfully created embedding with {len(embedding)} dimensions using {provider}")
                # Reset failure count on success
                if provider in LLMManager._embedding_failures:
                    LLMManager._embedding_failures[provider] = {'count': 0, 'last_failure': 0}
                return embedding
            else:
                embedding_logger.warning("Failed to get embedding from response")
                # Track failure
                if provider not in LLMManager._embedding_failures:
                    LLMManager._embedding_failures[provider] = {'count': 0, 'last_failure': 0}
                LLMManager._embedding_failures[provider]['count'] += 1
                LLMManager._embedding_failures[provider]['last_failure'] = time.time()
                return None
                
        except Exception as e:
            embedding_logger.error(f"Error creating embedding: {str(e)}")
            # Track failure for circuit breaker
            if provider not in LLMManager._embedding_failures:
                LLMManager._embedding_failures[provider] = {'count': 0, 'last_failure': 0}
            LLMManager._embedding_failures[provider]['count'] += 1
            LLMManager._embedding_failures[provider]['last_failure'] = time.time()
            
            # Log circuit breaker status
            failure_count = LLMManager._embedding_failures[provider]['count']
            if failure_count >= LLMManager._embedding_failure_threshold:
                embedding_logger.error(f"Circuit breaker tripped for {provider} embeddings after {failure_count} failures")
            
            return None
