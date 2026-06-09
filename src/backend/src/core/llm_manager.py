"""
LLM Manager for handling model configuration and LLM interactions.

This module provides a centralized manager for configuring and interacting with
different LLM providers through CrewAI's LLM class.

All LLM calls are routed through two main entry points:
- ``LLMManager.completion()`` — async helper for standalone calls (intent
  detection, generation services, etc.)
- ``LLMManager.configure_crewai_llm()`` / ``LLMManager.get_llm()`` — returns
  a configured CrewAI ``LLM`` instance for crew execution

litellm remains as a transitive dependency (used internally by CrewAI) but is
**not** called directly from application services.
"""

import asyncio
import logging
import os
import json
from typing import Dict, Any, List, Optional, Union, Tuple
import time

import litellm
from litellm import CustomLogger

from crewai import LLM
from src.schemas.model_provider import ModelProvider
from src.utils.databricks_url_utils import DatabricksURLUtils
from src.services.model_config_service import ModelConfigService
from src.services.api_keys_service import ApiKeysService
from src.core.unit_of_work import UnitOfWork
import pathlib

# Import custom model handlers (applied early for monkey patches)
# This ensures the monkey patches are applied to handle model-specific responses
from src.core.llm_handlers.databricks_gpt_oss_handler import DatabricksGPTOSSHandler, DatabricksRetryLLM
# Make CrewAI cognitive-memory models tolerate stringified-JSON metadata that
# Databricks/Bedrock models return (avoids the "1 validation error for
# MemoryAnalysis" retry spam). Import for its module-level patch side effect.
import src.core.llm_handlers.crewai_memory_patch  # noqa: F401


# Get the absolute path to the logs directory
log_dir = os.environ.get("LOG_DIR", str(pathlib.Path(__file__).parent.parent.parent / "logs"))
log_file_path = os.path.join(log_dir, "llm.log")

# Configure standard Python logger to also write to the llm.log file
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Import LoggerManager for documentation embedding logger
from src.core.logger import LoggerManager
embedding_logger = LoggerManager.get_instance().documentation_embedding

# Set drop_params to True to automatically drop unsupported parameters
# This is especially useful for GPT-5 and other new models that may have different parameter support
# Note: With litellm 1.75.8+, GPT-5 is natively supported

# Module-level token for subprocess callback fallback (contextvars don't propagate to callback threads)
_subprocess_user_token: Optional[str] = None

def set_subprocess_user_token(token: str) -> None:
    """Set token for LiteLLM callback fallback in subprocess mode."""
    global _subprocess_user_token
    _subprocess_user_token = token

litellm.drop_params = True
logger.info("Set litellm.drop_params=True to handle unsupported parameters gracefully")

# Register Databricks model context windows with CrewAI
# This is CRITICAL for CrewAI's respect_context_window to work correctly.
# CrewAI has a hardcoded LLM_CONTEXT_WINDOW_SIZES dictionary that it uses to determine
# when to trigger automatic summarization. Without entries for Databricks models,
# it falls back to DEFAULT_CONTEXT_WINDOW_SIZE (8192 tokens) which is incorrect.
# This causes CrewAI to not summarize when needed, leading to empty responses from
# models like Qwen that silently fail when context is too large.
try:
    from crewai.llm import LLM_CONTEXT_WINDOW_SIZES
    from src.seeds.model_configs import MODEL_CONFIGS

    registered_count = 0
    for model_name, config in MODEL_CONFIGS.items():
        if config.get('provider') == 'databricks':
            full_model_name = f"databricks/{model_name}"
            context_window = config.get('context_window', 128000)
            LLM_CONTEXT_WINDOW_SIZES[full_model_name] = context_window
            registered_count += 1
            logger.debug(f"Registered {full_model_name} with context_window={context_window} in CrewAI")

    logger.info(f"Registered {registered_count} Databricks models with CrewAI for context window management")
except Exception as reg_err:
    logger.warning(f"Could not register Databricks models with CrewAI: {reg_err}")
# Check if handlers already exist to avoid duplicates
if not logger.handlers:
    file_handler = logging.FileHandler(log_file_path)
    formatter = logging.Formatter('%(asctime)s - %(process)d - %(filename)s-%(funcName)s:%(lineno)d - %(levelname)s: %(message)s', 
                                 datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


logger.info(f"LLM operations log file: {log_file_path}")


class LiteLLMFileLogger(CustomLogger):
    """Logs LiteLLM calls to the llm.log file using the module logger."""

    def log_success_event(self, kwargs, response_obj, start_time, end_time):
        model = kwargs.get("model", "unknown")
        duration = (end_time - start_time).total_seconds() if hasattr(end_time - start_time, "total_seconds") else 0
        usage = {}
        if hasattr(response_obj, "usage") and response_obj.usage:
            usage = {
                "prompt_tokens": getattr(response_obj.usage, "prompt_tokens", 0),
                "completion_tokens": getattr(response_obj.usage, "completion_tokens", 0),
                "total_tokens": getattr(response_obj.usage, "total_tokens", 0),
            }
        logger.info(f"LLM success: model={model}, duration={duration:.2f}s, usage={usage}")

    def log_failure_event(self, kwargs, response_obj, start_time, end_time):
        model = kwargs.get("model", "unknown")
        exception = kwargs.get("exception", "unknown error")
        logger.error(f"LLM failure: model={model}, error={exception}")

    async def async_log_success_event(self, kwargs, response_obj, start_time, end_time):
        self.log_success_event(kwargs, response_obj, start_time, end_time)

    async def async_log_failure_event(self, kwargs, response_obj, start_time, end_time):
        self.log_failure_event(kwargs, response_obj, start_time, end_time)


# Create logger instance
litellm_file_logger = LiteLLMFileLogger()

# Dedicated callback for token telemetry to Databricks logfood
class LiteLLMTokenTelemetryLogger(CustomLogger):
    """
    Sends token usage telemetry to Databricks logfood.
    Uses get_auth_context for unified authentication.
    """
    
    def __init__(self):
        # Use module logger (already configured with handlers)
        self.logger = logger
    
    def _should_send(self, kwargs: Dict[str, Any], response_obj: Any) -> Tuple[bool, Optional[Dict], Optional[str], Optional[str]]:
        """Check if telemetry should be sent. Returns (should_send, usage, model, product_context)."""
        
        usage = response_obj.get('usage', {})
        if not usage:
            self.logger.debug(f"[TokenTelemetry] No usage in response - type={type(response_obj).__name__}")
            return False, None, None, None
        
        model = kwargs.get('model', 'unknown')
        
        # Extract product context from User-Agent (e.g., "kasal_agent/0.1.0" -> "agent")
        extra_headers = kwargs.get('extra_headers', {})
        user_agent = extra_headers.get('User-Agent', '')
        if '_' in user_agent and '/' in user_agent:
            product_context = user_agent.split('_')[1].split('/')[0]
        else:
            product_context = "llm"
        
        return True, usage, model, product_context
    
    def log_success_event(self, kwargs, response_obj, start_time, end_time):
        """Sync callback."""
        model = kwargs.get('model', 'unknown')
        usage = response_obj.get('usage', {}) if hasattr(response_obj, 'get') else {}
        
        should_send, usage, model, product_context = self._should_send(kwargs, response_obj)
        if not should_send:
            self.logger.debug(f"[TokenTelemetry] _should_send returned False")
            return
        
        msg = f"[TokenTelemetry] Sending: model={model}, context={product_context}, tokens={usage.get('total_tokens', 0)}"
        self.logger.info(msg)
        
        try:
            import asyncio
            from src.utils.telemetry import send_logfood_telemetry
            from src.utils.user_context import UserContext
            
            # Get user token from context (set during request via contextvars)
            # Falls back to module-level token for subprocess callback threads
            user_token = UserContext.get_user_token() or _subprocess_user_token
            
            # Use skip_db_auth=True to avoid opening database sessions during callbacks,
            # which can cause SQLAlchemy session conflicts with ongoing transactions
            coro = send_logfood_telemetry(
                usage=usage, model=model, product_context=product_context, 
                user_token=user_token, skip_db_auth=True
            )
            
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(coro)
            except RuntimeError:
                asyncio.run(coro)
        except Exception as e:
            self.logger.debug(f"Token telemetry failed: {e}")
    
    async def async_log_success_event(self, kwargs, response_obj, start_time, end_time):
        """Async callback."""
        model = kwargs.get('model', 'unknown')
        usage = response_obj.get('usage', {}) if hasattr(response_obj, 'get') else {}
        
        should_send, usage, model, product_context = self._should_send(kwargs, response_obj)
        if not should_send:
            self.logger.debug(f"[TokenTelemetry] _should_send returned False")
            return
        
        msg = f"[TokenTelemetry] Sending: model={model}, context={product_context}, tokens={usage.get('total_tokens', 0)}"
        self.logger.info(msg)
        
        try:
            from src.utils.telemetry import send_logfood_telemetry
            from src.utils.user_context import UserContext
            
            # Get user token from context (set during request via contextvars)
            # Falls back to module-level token for subprocess callback threads
            user_token = UserContext.get_user_token() or _subprocess_user_token
            
            # Use skip_db_auth=True to avoid opening database sessions during callbacks,
            # which can cause SQLAlchemy session conflicts with ongoing transactions
            await send_logfood_telemetry(
                usage=usage, model=model, product_context=product_context,
                user_token=user_token, skip_db_auth=True
            )
        except Exception as e:
            self.logger.debug(f"Token telemetry failed: {e}")


# Create telemetry logger instance
litellm_token_telemetry_logger = LiteLLMTokenTelemetryLogger()

# Register callbacks with LiteLLM
litellm.callbacks = [litellm_token_telemetry_logger]

# Set up other litellm configuration
litellm.modify_params = True  # This helps with Anthropic API compatibility
litellm.num_retries = 5  # Global retries setting
litellm.retry_on = ["429", "timeout", "rate_limit_error"]  # Retry on these error types


def _configure_litellm_caching() -> None:
    """Enable LiteLLM response caching based on environment settings.

    Caches completions/embeddings to cut latency and cost on repeated identical
    calls. Backend and TTL are env-configurable (see ``Settings``); defaults to
    on-disk ("disk") so the cache persists across the subprocess-per-execution
    model and is shared between the API process and crew subprocesses (cross-run
    hits). Failures degrade gracefully — caching is best-effort and must never
    break an LLM call.
    """
    from src.config.settings import settings

    if not settings.LITELLM_CACHE_ENABLED:
        logger.info("LiteLLM caching disabled (LITELLM_CACHE_ENABLED=false)")
        return

    cache_type = (settings.LITELLM_CACHE_TYPE or "local").lower()
    ttl = settings.LITELLM_CACHE_TTL

    try:
        if cache_type == "redis":
            host = settings.LITELLM_CACHE_REDIS_HOST
            if not host:
                logger.warning(
                    "LITELLM_CACHE_TYPE=redis but LITELLM_CACHE_REDIS_HOST is not set; "
                    "falling back to in-memory ('local') cache"
                )
                cache_type = "local"
            else:
                litellm.enable_cache(
                    type="redis",
                    host=host,
                    port=settings.LITELLM_CACHE_REDIS_PORT,
                    password=settings.LITELLM_CACHE_REDIS_PASSWORD,
                    ttl=ttl,
                )
                logger.info(f"LiteLLM Redis cache enabled (host={host}, ttl={ttl}s)")
                return

        if cache_type == "disk":
            # Disk cache persists across the subprocess-per-execution model and is
            # shared between the API process and crew subprocesses, so identical
            # calls hit across runs. Use a controlled dir (default under logs)
            # instead of litellm's ".litellm_cache" in the current directory.
            disk_dir = settings.LITELLM_CACHE_DIR or os.path.join(log_dir, "llm_cache")
            litellm.enable_cache(type="disk", disk_cache_dir=disk_dir, ttl=ttl)
            logger.info(f"LiteLLM disk cache enabled (dir={disk_dir}, ttl={ttl}s)")
            return

        litellm.enable_cache(type=cache_type, ttl=ttl)
        logger.info(f"LiteLLM cache enabled (type={cache_type}, ttl={ttl}s)")
    except Exception as e:
        logger.warning(f"Failed to configure LiteLLM caching ({cache_type}): {e}")


_configure_litellm_caching()

# Configure MLflow integration for Databricks observability
_mlflow_configured = False
# Default to disabled globally; we enable per-workspace dynamically
_use_mlflow = False

def _configure_databricks_mlflow():
    """
    Configure MLflow using unified Databricks authentication.

    MLflow is configured ONCE at application startup with service-level credentials.
    MLflow does NOT support OBO (On-Behalf-Of) authentication because it uses
    environment variables which are process-wide and would cause race conditions.

    This is acceptable because MLflow is for observability/tracking, not user-specific
    data access operations.
    """
    global _mlflow_configured

    if _mlflow_configured is True:
        return

    try:
        import mlflow
        import asyncio
        from src.utils.databricks_auth import get_auth_context

        # Get service-level authentication context (NO OBO - PAT/SPN only)
        # This is configured ONCE at startup to avoid race conditions
        # Pass user_token=None to skip OBO and use PAT/SPN
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Already in async context, need to run in executor
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, get_auth_context(user_token=None))
                    auth = future.result()
            else:
                # Not in event loop, can use asyncio.run
                auth = asyncio.run(get_auth_context(user_token=None))
        except RuntimeError:
            # No event loop, use asyncio.run
            auth = asyncio.run(get_auth_context(user_token=None))

        if auth and auth.workspace_url:
            # Set environment variables ONCE at startup for MLflow
            # MLflow ONLY supports environment variable authentication
            os.environ["DATABRICKS_HOST"] = auth.workspace_url
            os.environ["DATABRICKS_TOKEN"] = auth.token
            logger.info(f"MLflow configured with {auth.auth_method} authentication")

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
            logger.info(f"Databricks MLflow configured successfully with {auth.auth_method} authentication")

        else:
            logger.info("No Databricks workspace available for MLflow")

    except ImportError:
        logger.info("MLflow not available - install with 'pip install mlflow' for Databricks observability")
    except Exception as e:
        logger.warning(f"Failed to configure Databricks MLflow: {e}")

# Configure litellm callbacks to file logger and telemetry logger
litellm.success_callback = [litellm_file_logger, litellm_token_telemetry_logger]
litellm.failure_callback = [litellm_file_logger]
logger.info("Using file-based logging and token telemetry for LLM observability")

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
__all__ = ['LLMManager', 'DatabricksGPTOSSHandler', 'DatabricksRetryLLM']


def _is_http_400(exc: Exception) -> bool:
    """Check if an exception represents an HTTP 400 error."""
    # litellm raises BadRequestError (subclass of openai.BadRequestError)
    exc_name = type(exc).__name__
    if exc_name in ("BadRequestError",):
        return True
    # Also check status_code attribute (litellm exceptions carry it)
    if getattr(exc, "status_code", None) == 400:
        return True
    # Fallback: check string representation
    exc_str = str(exc)
    if "400" in exc_str and ("bad request" in exc_str.lower() or "BadRequest" in exc_str):
        return True
    return False


class LLMManager:
    """Manager for LLM configurations and interactions."""

    # Circuit breaker for embeddings to prevent repeated failures
    _embedding_failures = {}  # Track failures by provider
    _embedding_failure_threshold = 3  # Number of failures before circuit opens
    _circuit_reset_time = 300  # Reset circuit after 5 minutes

    @staticmethod
    def _get_group_id_from_context(required: bool = True) -> Optional[str]:
        """
        Get group_id from UserContext for multi-tenant isolation.

        Args:
            required: If True, raises ValueError when group_id is not available.
                     If False, returns None when group_id is not available.

        Returns:
            group_id string if available, None if not available and not required

        Raises:
            ValueError: If group_id is not available and required=True
        """
        from src.utils.user_context import UserContext
        try:
            group_context = UserContext.get_group_context()
            if group_context and hasattr(group_context, 'primary_group_id'):
                group_id = group_context.primary_group_id
                if group_id:
                    return group_id
        except Exception as e:
            logger.warning(f"Could not get group_id from UserContext: {e}")

        # If group_id is required, raise error
        if required:
            logger.error("Cannot retrieve API keys: no group_id available (multi-tenant isolation required)")
            raise ValueError("group_id is required for API key operations (multi-tenant isolation)")

        # Otherwise return None
        return None

    @staticmethod
    async def completion(
        messages: List[Dict[str, str]],
        model: str,
        temperature: float = 0.7,
        max_tokens: int = 4000,
        extra_headers: Optional[Dict[str, str]] = None,
        fallback_drop_system_on_400: bool = False,
    ) -> str:
        """
        Unified async completion method that routes through CrewAI's LLM class.

        Args:
            messages: List of message dicts with 'role' and 'content' keys
            model: Model identifier (e.g. 'databricks-llama-4-maverick')
            temperature: Sampling temperature (default 0.7)
            max_tokens: Maximum tokens in response (default 4000)
            extra_headers: Optional extra HTTP headers (e.g. User-Agent for telemetry)
            fallback_drop_system_on_400: If True and the call raises an HTTP 400,
                retry once with system messages removed (user messages only).
                Handles models that reject system+user dual-message payloads.

        Returns:
            str: The LLM response content string

        Raises:
            ValueError: If model configuration is not found or group_id is unavailable
            Exception: For LLM call errors
        """
        group_id = LLMManager._get_group_id_from_context(required=True)
        llm = await LLMManager.configure_crewai_llm(model, group_id, temperature)
        llm.max_tokens = max_tokens
        if extra_headers:
            # Pass extra_headers to the underlying litellm call via LLM extra_headers param
            llm.extra_headers = extra_headers

        # Use sync call() in a thread to ensure custom wrappers
        # (e.g. DatabricksRetryLLM) are invoked correctly.
        # The async acall() bypasses those overrides.
        start_time = time.time()
        try:
            result = await asyncio.to_thread(llm.call, messages)
            duration = time.time() - start_time
            logger.info(f"LLM completion: model={model}, duration={duration:.2f}s, response_length={len(result) if result else 0}")
            return result
        except Exception as e:
            duration = time.time() - start_time
            # On HTTP 400 with fallback enabled, retry without system messages
            if fallback_drop_system_on_400 and _is_http_400(e):
                user_only = [m for m in messages if m.get("role") != "system"]
                if user_only and len(user_only) < len(messages):
                    logger.warning(
                        f"LLM completion got 400, retrying without system message: model={model}"
                    )
                    try:
                        result = await asyncio.to_thread(llm.call, user_only)
                        fallback_duration = time.time() - start_time
                        logger.info(
                            f"LLM completion (user-only fallback): model={model}, "
                            f"duration={fallback_duration:.2f}s, response_length={len(result) if result else 0}"
                        )
                        return result
                    except Exception as retry_err:
                        logger.error(f"LLM completion user-only fallback also failed: {retry_err}")
                        raise retry_err
            logger.error(f"LLM completion failed: model={model}, duration={duration:.2f}s, error={e}")
            raise

    @staticmethod
    async def configure_crewai_llm(model_name: str, group_id: str, temperature: Optional[float] = None) -> LLM:
        """
        Create and configure a CrewAI LLM instance with the correct provider prefix.

        Args:
            model_name: The model identifier to configure
            group_id: Group ID for multi-tenant isolation (REQUIRED)
            temperature: Optional temperature override

        Returns:
            LLM: Configured CrewAI LLM instance

        Raises:
            ValueError: If model configuration is not found or group_id is not provided
            Exception: For other configuration errors
        """
        # SECURITY: Validate group_id is provided
        if not group_id:
            raise ValueError("group_id is REQUIRED for configure_crewai_llm (multi-tenant isolation)")

        # Get model configuration using ModelConfigService
        from src.db.session import request_scoped_session

        async with request_scoped_session() as session:
            model_config_service = ModelConfigService(session, group_id=group_id)
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
        # Note: group_id is already passed as parameter to this function
        if provider == ModelProvider.DEEPSEEK:
            api_key = await ApiKeysService.get_provider_api_key(provider, group_id=group_id)
            api_base = os.getenv("DEEPSEEK_ENDPOINT", "https://api.deepseek.com")
            prefixed_model = f"deepseek/{model_name_value}"
        elif provider == ModelProvider.OPENAI:
            api_key = await ApiKeysService.get_provider_api_key(provider, group_id=group_id)
            # OpenAI doesn't need a prefix
            prefixed_model = model_name_value
        elif provider == ModelProvider.ANTHROPIC:
            api_key = await ApiKeysService.get_provider_api_key(provider, group_id=group_id)
            prefixed_model = f"anthropic/{model_name_value}"
        elif provider == ModelProvider.OLLAMA:
            api_base = os.getenv("OLLAMA_API_BASE", "http://localhost:11434")
            # Normalize model name: replace hyphen with colon for Ollama models
            normalized_model_name = model_name_value
            if "-" in normalized_model_name:
                normalized_model_name = normalized_model_name.replace("-", ":")
            prefixed_model = f"ollama/{normalized_model_name}"
        elif provider == ModelProvider.DATABRICKS:
            # Use unified Databricks authentication for CrewAI LLM (thread-safe)
            try:
                from src.utils.databricks_auth import get_auth_context
                from src.utils.user_context import UserContext

                # Get user token from context for OBO authentication
                user_token = UserContext.get_user_token()

                # Get authentication context (OBO → PAT → Service Principal)
                auth = await get_auth_context(user_token=user_token, group_id=group_id)
                if auth:
                    # Pass authentication directly to CrewAI LLM (thread-safe)
                    api_key = auth.token
                    # Routes to /serving-endpoints or /ai-gateway/mlflow/v1 based on the
                    # AI Gateway toggle; LiteLLM appends /chat/completions either way.
                    api_base = DatabricksURLUtils.construct_llm_base_url(auth.workspace_url)
                    logger.info(f"Using Databricks {auth.auth_method} authentication for CrewAI LLM")
                else:
                    logger.warning("No Databricks authentication available for CrewAI LLM")
                    api_key = None

            except ImportError:
                # SECURITY: databricks_auth module is required - no fallback allowed
                logger.error("Unified Databricks auth module not available for CrewAI LLM")
                raise ImportError("databricks_auth module is required for Databricks authentication")
            
            prefixed_model = f"databricks/{model_name_value}"
            is_gpt5 = "gpt-5" in model_name_value.lower() or "gpt5" in model_name_value.lower()
            # Newer frontier models (GPT-5, Claude Opus 4.7+) reject `temperature`.
            from src.utils.model_config import model_rejects_temperature
            rejects_temperature = model_rejects_temperature(model_name_value)

            # Ensure the model string explicitly includes the provider for CrewAI compatibility
            # GPT-5 reasoning models need longer timeout (300s) — they can take 2-4 min on complex prompts
            # Standard Databricks models: 240s (server-side limit is 297s)
            llm_params = {
                "model": prefixed_model,
                "timeout": 300 if is_gpt5 else 297,
            }

            # GPT-5 reasoning models on Databricks reject stop, temperature, and other params.
            # litellm DatabricksConfig lists them as "supported" so drop_params=True won't help.
            # CrewAI has a built-in retry that catches the stop error, but pre-setting
            # additional_drop_params avoids the wasted first-call failure on every LLM call.
            # See: https://community.crewai.com/t/gpt5-crewai-issues/6829
            if is_gpt5:
                llm_params["additional_drop_params"] = ["stop", "temperature", "presence_penalty", "frequency_penalty", "logit_bias"]
                logger.info(f"Databricks GPT-5 model: {model_name_value} — additional_drop_params and 300s timeout set")
            elif rejects_temperature:
                # e.g. Claude Opus 4.8 — endpoint 400s on `temperature`.
                llm_params["additional_drop_params"] = ["temperature"]
                logger.info(f"Databricks model {model_name_value} rejects temperature — dropping it")

            # Add temperature only for models that accept it.
            if temperature is not None and not rejects_temperature:
                llm_params["temperature"] = temperature
                logger.info(f"Setting temperature to {temperature} for model {prefixed_model}")

            # Add API key and base URL if available
            if api_key:
                llm_params["api_key"] = api_key
            if api_base:
                llm_params["api_base"] = api_base

            # Add User-Agent header for Databricks API attribution
            # Using extra_headers instead of user_agent param (which Databricks rejects in body)
            from src.utils.telemetry import get_user_agent_header, KasalProduct
            llm_params["extra_headers"] = get_user_agent_header(KasalProduct.AGENT)

            # Add max_output_tokens if defined in model config
            if "max_output_tokens" in model_config_dict and model_config_dict["max_output_tokens"]:
                if is_gpt5:
                    # GPT-5 requires max_completion_tokens (litellm Databricks transformer
                    # rewrites it to max_tokens which GPT-5 rejects — litellm#13719)
                    llm_params["max_completion_tokens"] = model_config_dict["max_output_tokens"]
                    logger.info(f"Setting max_completion_tokens to {model_config_dict['max_output_tokens']} for Databricks GPT-5 model {prefixed_model}")
                else:
                    llm_params["max_tokens"] = model_config_dict["max_output_tokens"]
                    logger.info(f"Setting max_tokens to {model_config_dict['max_output_tokens']} for model {prefixed_model}")

            logger.info(f"Creating CrewAI LLM with model: {prefixed_model}, has_api_key: {bool(api_key)}, api_base: {api_base}")

            # gpt-5-3-codex ONLY supports the Responses API on Databricks.
            # DatabricksCodexCompletion extends OpenAICompletion with:
            #  - phase preservation (prevents early stopping / skipped tool calls)
            #  - stop-word suppression (GPT-5 reasoning rejects 'stop')
            #  - diagnostic logging for tool-calling debugging
            if "gpt-5-3-codex" in model_name_value.lower():
                from src.core.llm_handlers.databricks_codex_handler import DatabricksCodexCompletion
                # The Responses API is served under a DIFFERENT base path than chat:
                # /ai-gateway/openai/v1 (gateway) or /serving-endpoints (otherwise).
                # `api_base` here is the CHAT base (/ai-gateway/mlflow/v1 when the
                # gateway is on), which has no /responses route — using it yields a
                # 404 "Supervisor API is not enabled". Build the Responses base instead.
                responses_workspace = DatabricksURLUtils.extract_workspace_from_endpoint(api_base)
                responses_base_url = DatabricksURLUtils.construct_responses_base_url(responses_workspace)
                logger.info(f"Using DatabricksCodexCompletion for Responses API model: {model_name_value} (base_url={responses_base_url})")
                return DatabricksCodexCompletion(
                    model=model_name_value,
                    api_key=api_key,
                    base_url=responses_base_url,
                    timeout=300,
                    max_tokens=llm_params.get("max_completion_tokens") or llm_params.get("max_tokens"),
                )

            # Use DatabricksRetryLLM for all other Databricks models (GPT-OSS, Llama, Claude, etc.)
            # Provides retry logic for empty responses, rate limits, and message sanitization.
            # GPT-OSS Harmony response format is handled by the monkey patch in DatabricksGPTOSSHandler.
            logger.info(f"Using DatabricksRetryLLM wrapper for Databricks model: {model_name_value}")
            return DatabricksRetryLLM(**llm_params)
        elif provider == ModelProvider.GEMINI:
            # SECURITY: Use group_id parameter for multi-tenant isolation
            api_key = await ApiKeysService.get_provider_api_key(provider, group_id=group_id)
            # Set in environment variables for better compatibility with various libraries
            if api_key:
                os.environ["GEMINI_API_KEY"] = api_key
                os.environ["GOOGLE_API_KEY"] = api_key
            else:
                logger.warning(f"No API key found for Gemini with group_id: {group_id}")
                
                # Set configuration for better tool/function handling with Instructor
                os.environ["INSTRUCTOR_MODEL_NAME"] = "gemini"

            prefixed_model = f"gemini/{model_name_value}"
        else:
            # Default fallback for other providers
            logger.warning(f"Using default model name format for provider: {provider}")
            prefixed_model = f"{provider.lower()}/{model_name_value}" if provider else model_name_value
        
        # Configure LLM parameters (for all providers except Databricks which returns early)
        # Use longer timeout for GPT-5 models as they take more time to respond
        timeout_value = 300 if (provider == ModelProvider.OPENAI and "gpt-5" in model_name_value.lower()) else 300
        
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
            llm_params["drop_params"] = True
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
            # GPT-5 and newer OpenAI reasoning models use max_completion_tokens instead of max_tokens
            if provider == ModelProvider.OPENAI and "gpt-5" in model_name_value.lower():
                llm_params["max_completion_tokens"] = model_config_dict["max_output_tokens"]
                logger.info(f"Setting max_completion_tokens to {model_config_dict['max_output_tokens']} for GPT-5 model {prefixed_model}")
            else:
                llm_params["max_tokens"] = model_config_dict["max_output_tokens"]
                logger.info(f"Setting max_tokens to {model_config_dict['max_output_tokens']} for model {prefixed_model}")
        
        # Create and return the CrewAI LLM
        # GPT-5 models: CrewAI's drop_params=True (default) handles unsupported params,
        # and max_completion_tokens is already set above instead of max_tokens.
        if provider == ModelProvider.OPENAI and "gpt-5" in model_name_value.lower():
            # Drop params that GPT-5 reasoning models don't support
            llm_params["additional_drop_params"] = ["stop", "temperature"]
            logger.info(f"Creating CrewAI LLM for GPT-5 model: {prefixed_model} (with additional_drop_params)")
        logger.info(f"Creating CrewAI LLM with model: {prefixed_model}")
        return LLM(**llm_params)

    @staticmethod
    async def get_llm(model_name: str, temperature: Optional[float] = None):
        """
        Create a CrewAI LLM instance for the specified model.

        MLflow/tracing is handled by the OTEL service (otel_tracing/mlflow_setup.py)
        at the execution subprocess level, not per-LLM instance.
        """
        # CRITICAL: Get group_id from UserContext FIRST for multi-tenant isolation
        from src.utils.user_context import UserContext
        group_ctx = UserContext.get_group_context()
        group_id = getattr(group_ctx, "primary_group_id", None) if group_ctx else None

        if not group_id:
            logger.error("No group_id found in UserContext for LLM creation")
            raise ValueError("group_id is REQUIRED for get_llm (multi-tenant isolation)")

        return await LLMManager.configure_crewai_llm(model_name, group_id, temperature)

    @staticmethod
    async def get_embeddings(
        texts: List[str],
        model: str = "databricks-gte-large-en",
        embedder_config: Optional[Dict[str, Any]] = None,
        batch_size: Optional[int] = None,
    ) -> List[Optional[List[float]]]:
        """
        Get embedding vectors for many texts efficiently.

        Resolves Databricks auth ONCE and sends texts in batched requests, instead
        of one auth lookup + one HTTP round-trip per text. Returns a list aligned
        with ``texts`` (None for any text that failed). For non-Databricks
        providers, falls back to sequential ``get_embedding`` calls.
        """
        if not texts:
            return []

        provider = 'databricks'
        embedding_model = model
        if embedder_config:
            provider = embedder_config.get('provider', 'databricks')
            embedding_model = embedder_config.get('config', {}).get('model', model)

        # Only the Databricks serving endpoint supports the batched payload here;
        # other providers fall back to the existing per-text path.
        if not (provider == 'databricks' or 'databricks' in embedding_model):
            return [
                await LLMManager.get_embedding(t, model=model, embedder_config=embedder_config)
                for t in texts
            ]

        if batch_size is None:
            batch_size = int(os.getenv("EMBEDDING_BATCH_SIZE", "32"))

        try:
            from src.utils.databricks_auth import get_auth_context
            from src.utils.user_context import UserContext

            # Resolve auth ONCE for the whole file (this is the per-chunk cost we
            # are eliminating — each get_auth_context() opens a DB session).
            user_token = UserContext.get_user_token()
            emb_group_id = LLMManager._get_group_id_from_context(required=False)
            auth = await get_auth_context(user_token=user_token, group_id=emb_group_id)
            if not auth:
                embedding_logger.warning("No Databricks auth available for batch embeddings")
                return [None] * len(texts)

            if auth.auth_method in ("OBO", "OAuth"):
                request_headers = auth.get_headers().copy()
                request_headers.setdefault("Content-Type", "application/json")
            else:
                request_headers = {
                    "Authorization": f"Bearer {auth.token}",
                    "Content-Type": "application/json",
                }

            # AI Gateway on  -> /ai-gateway/mlflow/v1/embeddings (model in body)
            # AI Gateway off -> /serving-endpoints/<model>/invocations (model in path)
            endpoint_url, body_model = DatabricksURLUtils.construct_embeddings_url(
                auth.workspace_url, embedding_model
            )

            import aiohttp
            timeout = aiohttp.ClientTimeout(
                total=float(os.getenv("EMBEDDING_HTTP_TIMEOUT_SECONDS", "60"))
            )
            results: List[Optional[List[float]]] = []
            async with aiohttp.ClientSession(timeout=timeout) as session:
                for start in range(0, len(texts), batch_size):
                    batch = texts[start:start + batch_size]
                    payload = {"input": batch}
                    if body_model:
                        payload["model"] = body_model
                    try:
                        async with session.post(
                            endpoint_url, headers=request_headers, json=payload, timeout=timeout
                        ) as response:
                            if response.status == 200:
                                result = await response.json()
                                data = result.get('data', [])
                                try:
                                    data = sorted(data, key=lambda d: d.get('index', 0))
                                except Exception:
                                    pass
                                if len(data) == len(batch):
                                    results.extend([d.get('embedding', d) for d in data])
                                else:
                                    embedding_logger.warning(
                                        f"Batch embedding size mismatch: got {len(data)} for {len(batch)}"
                                    )
                                    results.extend(
                                        [data[i].get('embedding') if i < len(data) else None
                                         for i in range(len(batch))]
                                    )
                            else:
                                error_text = await response.text()
                                embedding_logger.error(
                                    f"Batch embedding API error {response.status}: {error_text}"
                                )
                                results.extend([None] * len(batch))
                    except Exception as batch_err:
                        embedding_logger.error(f"Batch embedding request failed: {batch_err}")
                        results.extend([None] * len(batch))
            return results

        except Exception as e:
            embedding_logger.error(f"Error in batch embeddings: {e}")
            return [None] * len(texts)

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
                # Use unified Databricks authentication for embeddings
                try:
                    from src.utils.databricks_auth import get_auth_context
                    from src.utils.user_context import UserContext

                    # Get user token from context for OBO authentication
                    user_token = UserContext.get_user_token()

                    # Use unified authentication (OBO → OAuth → PAT)
                    embedding_logger.info("Attempting unified Databricks authentication for embeddings")
                    emb_group_id = LLMManager._get_group_id_from_context(required=False)
                    auth = await get_auth_context(user_token=user_token, group_id=emb_group_id)
                    if auth:
                        embedding_logger.info(f"Using Databricks {auth.auth_method} authentication for embeddings")
                        # For OAuth/OBO, use headers approach
                        if auth.auth_method in ["OBO", "OAuth"]:
                            headers = auth.get_headers()
                            api_key = None
                        else:
                            # For PAT, use API key approach
                            api_key = auth.token
                            headers = None
                        api_base = DatabricksURLUtils.construct_llm_base_url(auth.workspace_url)
                    else:
                        embedding_logger.warning("No Databricks authentication available for embeddings")
                        return None

                except ImportError:
                    # SECURITY: databricks_auth module is required - no fallback allowed
                    embedding_logger.error("Unified Databricks auth module not available for embeddings")
                    raise ImportError("databricks_auth module is required for Databricks authentication")
                
                # Check if we have either OAuth headers or API key + base URL
                if not ((headers and api_base) or (api_key and api_base)):
                    logger.warning(f"Missing Databricks credentials - OAuth headers: {bool(headers)}, API key: {bool(api_key)}, API base: {bool(api_base)}")
                    return None
                
                # Ensure model has databricks prefix
                if not embedding_model.startswith('databricks/'):
                    embedding_model = f"databricks/{embedding_model}"
                
                # Use direct HTTP request to avoid config file issues
                import aiohttp
                
                try:
                    # Construct the direct API endpoint using centralized utility.
                    # AI Gateway on  -> /ai-gateway/mlflow/v1/embeddings (model in body)
                    # AI Gateway off -> /serving-endpoints/<model>/invocations (model in path)
                    workspace_url = DatabricksURLUtils.extract_workspace_from_endpoint(api_base)
                    endpoint_url, body_model = DatabricksURLUtils.construct_embeddings_url(workspace_url, embedding_model)

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
                    if body_model:
                        payload["model"] = body_model

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
                            elif response.status == 401:
                                # Token expired, try to refresh and retry once
                                embedding_logger.warning("Received 401 error, attempting to refresh token and retry")
                                try:
                                    # Refresh token and get new headers
                                    headers_result, error = await get_databricks_auth_headers()
                                    if headers_result and not error:
                                        # Update request headers with refreshed token
                                        if headers_result:
                                            request_headers = headers_result.copy()
                                            if "Content-Type" not in request_headers:
                                                request_headers["Content-Type"] = "application/json"

                                        # Retry the request with new token
                                        async with session.post(endpoint_url, headers=request_headers, json=payload, timeout=timeout) as retry_response:
                                            if retry_response.status == 200:
                                                result = await retry_response.json()
                                                if 'data' in result and len(result['data']) > 0:
                                                    embedding = result['data'][0].get('embedding', result['data'][0])
                                                    embedding_logger.info(f"Successfully created embedding after token refresh")
                                                    return embedding
                                                else:
                                                    embedding_logger.warning("No embedding data found in Databricks response after retry")
                                                    return None
                                            else:
                                                error_text = await retry_response.text()
                                                embedding_logger.error(f"Databricks embedding API error after retry {retry_response.status}: {error_text}")
                                                return None
                                    else:
                                        embedding_logger.error(f"Failed to refresh token: {error}")
                                        return None
                                except Exception as refresh_error:
                                    embedding_logger.error(f"Error refreshing token: {refresh_error}")
                                    return None
                            else:
                                error_text = await response.text()
                                embedding_logger.error(f"Databricks embedding API error {response.status}: {error_text}")
                                return None

                except Exception as e:
                    embedding_logger.error(f"Error calling Databricks embedding API directly: {str(e)}")
                    return None
                
            elif provider == 'ollama':
                # Use Ollama for embeddings via direct HTTP
                import aiohttp
                api_base = os.getenv("OLLAMA_API_BASE", "http://localhost:11434")
                # Strip ollama/ prefix if present for the raw API call
                raw_model = embedding_model.removeprefix("ollama/")

                timeout_val = aiohttp.ClientTimeout(total=float(os.getenv("EMBEDDING_TIMEOUT_SECONDS", "60")))
                async with aiohttp.ClientSession(timeout=timeout_val) as http_session:
                    async with http_session.post(
                        f"{api_base}/api/embed",
                        json={"model": raw_model, "input": text},
                    ) as resp:
                        if resp.status != 200:
                            error_text = await resp.text()
                            embedding_logger.error(f"Ollama embedding API error {resp.status}: {error_text}")
                            return None
                        result = await resp.json()
                        embeddings_list = result.get("embeddings", [])
                        if embeddings_list:
                            embedding = embeddings_list[0]
                            embedding_logger.info(f"Successfully created embedding with {len(embedding)} dimensions using Ollama")
                            if provider in LLMManager._embedding_failures:
                                LLMManager._embedding_failures[provider] = {'count': 0, 'last_failure': 0}
                            return embedding
                        embedding_logger.warning("No embedding data in Ollama response")
                        return None

            elif provider == 'google':
                # Use Google AI for embeddings via direct HTTP
                import aiohttp
                group_id = LLMManager._get_group_id_from_context()
                api_key = await ApiKeysService.get_provider_api_key(ModelProvider.GEMINI, group_id=group_id)

                if not api_key:
                    embedding_logger.warning("No Google API key found for creating embeddings")
                    return None

                # Strip gemini/ prefix if present
                raw_model = embedding_model.removeprefix("gemini/")
                url = f"https://generativelanguage.googleapis.com/v1beta/models/{raw_model}:embedContent?key={api_key}"
                payload = {"model": f"models/{raw_model}", "content": {"parts": [{"text": text}]}}

                timeout_val = aiohttp.ClientTimeout(total=float(os.getenv("EMBEDDING_TIMEOUT_SECONDS", "60")))
                async with aiohttp.ClientSession(timeout=timeout_val) as http_session:
                    async with http_session.post(url, json=payload) as resp:
                        if resp.status != 200:
                            error_text = await resp.text()
                            embedding_logger.error(f"Google embedding API error {resp.status}: {error_text}")
                            return None
                        result = await resp.json()
                        embedding_data = result.get("embedding", {})
                        values = embedding_data.get("values", [])
                        if values:
                            embedding_logger.info(f"Successfully created embedding with {len(values)} dimensions using Google")
                            if provider in LLMManager._embedding_failures:
                                LLMManager._embedding_failures[provider] = {'count': 0, 'last_failure': 0}
                            return values
                        embedding_logger.warning("No embedding data in Google response")
                        return None

            else:
                # Default to OpenAI for embeddings via direct HTTP
                import aiohttp
                group_id = LLMManager._get_group_id_from_context()
                api_key = await ApiKeysService.get_provider_api_key(ModelProvider.OPENAI, group_id=group_id)

                if not api_key:
                    embedding_logger.warning(f"No OpenAI API key found for creating embeddings with group_id: {group_id}")
                    return None

                url = "https://api.openai.com/v1/embeddings"
                headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
                payload = {"model": embedding_model, "input": text}

                timeout_val = aiohttp.ClientTimeout(total=float(os.getenv("EMBEDDING_TIMEOUT_SECONDS", "60")))
                async with aiohttp.ClientSession(timeout=timeout_val) as http_session:
                    async with http_session.post(url, headers=headers, json=payload) as resp:
                        if resp.status != 200:
                            error_text = await resp.text()
                            embedding_logger.error(f"OpenAI embedding API error {resp.status}: {error_text}")
                            return None
                        result = await resp.json()
                        data = result.get("data", [])
                        if data:
                            embedding = data[0].get("embedding", [])
                            embedding_logger.info(f"Successfully created embedding with {len(embedding)} dimensions using OpenAI")
                            if provider in LLMManager._embedding_failures:
                                LLMManager._embedding_failures[provider] = {'count': 0, 'last_failure': 0}
                            return embedding
                        embedding_logger.warning("No embedding data in OpenAI response")
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

