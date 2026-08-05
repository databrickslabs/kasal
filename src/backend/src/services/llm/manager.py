"""
LLM Manager — the public facade for LLM work in kasal.

Turns a model KEY from the catalogue into a configured ``kasal_engine`` LLM:
database lookup, per-tenant credentials (OBO -> PAT -> SPN), endpoint URLs and
the parameter rules a given endpoint imposes. Entry points:

- ``LLMManager.completion()`` — standalone calls (intent detection, generation
  services, ...); 38+ call sites, so this signature stays stable.
- ``LLMManager.configure_kasal_llm()`` / ``get_llm()`` — a configured ``LLM``
  for crew, flow and chat execution.
- ``LLMManager.get_embedding(s)()`` — thin delegates to
  ``src/core/llm/embeddings.py``.

Layering is documented in ``src/core/llm/__init__.py``. The rule that matters
here: **litellm is not on the LLM path**. The engine drives endpoints with the
OpenAI SDK, so litellm globals and patches reach only ``completion_with_usage``,
and every parameter set when building an LLM IS sent — there is no drop-params
safety net behind this module.
"""

import asyncio
import concurrent.futures
import contextvars
import functools
import logging
import os
import time
from contextlib import nullcontext
from typing import Any, Dict, List, Optional, Tuple, Union

import litellm
from litellm import CustomLogger

from src.core.llm.transport import LLM
from src.core.logger import LoggerManager
from src.schemas.model_provider import ModelProvider

# Dedicated executor for blocking LLM calls. ``asyncio.to_thread`` shares the
# loop's DEFAULT ThreadPoolExecutor (max ~min(32, cpu+4) workers) with every
# other to_thread user in the process, and Databricks LLM calls run with ~300s
# timeouts — a burst of slow calls saturated that pool and queued ALL
# concurrent chat users (plus every other to_thread caller) behind it. A
# dedicated, larger pool caps LLM concurrency without starving anyone else.
_LLM_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=int(os.getenv("KASAL_LLM_MAX_CONCURRENCY", "64")),
    thread_name_prefix="llm-call",
)


async def _run_llm_blocking(func, /, *args, **kwargs):
    """Run a blocking LLM call on the dedicated executor.

    Mirrors ``asyncio.to_thread`` semantics (contextvars propagate, so ambient
    group/user context inside callbacks keeps working) but on ``_LLM_EXECUTOR``.
    """
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    call = functools.partial(ctx.run, func, *args, **kwargs)
    return await loop.run_in_executor(_LLM_EXECUTOR, call)


# Endpoint-specific LLM subclasses. This import no longer carries side effects:
# the module used to apply two litellm monkey patches at import time, both of
# which the engine bypasses (it speaks to endpoints with the OpenAI SDK).
from src.services.llm.handlers.databricks_retry_llm import DatabricksRetryLLM
from src.services.llm.handlers.vllm import VLLMFunctionCallingLLM
from src.services.llm.params import resolve as resolve_llm_params
from src.services.settings.api_keys import ApiKeysService
from src.services.settings.models import ModelConfigService
from src.utils.databricks_url_utils import DatabricksURLUtils

# The former crewai_memory_patch / crewai_instructor_patch side-effect
# imports are gone: kasal_engine's analyze models are tolerant of
# stringified-JSON metadata by design, and InternalInstructor accepts
# per-call api_key/base_url natively.


# The log directory comes from LoggerManager, which owns that decision. This
# used to recompute it as Path(__file__).parent.parent.parent / "logs" — a
# depth-counting path that was correct only while this module lived at
# src/core/. When it moved to src/services/llm/ the same expression started
# resolving to backend/src/logs, so LLM logs and the litellm disk cache were
# written INTO the source tree, in production as well as in tests.
log_dir = str(LoggerManager.get_instance().log_dir)
log_file_path = os.path.join(log_dir, "llm.log")

# Configure standard Python logger to also write to the llm.log file
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def _refused_params(
    model_config_dict: Dict[str, Any], served_model: Optional[str]
) -> List[str]:
    """Parameter names to strip before the request is built.

    The union of two sources, and both are needed:

    * ``ModelConfig.unsupported_params`` — the hand-declared list. It is the
      escape hatch for an endpoint nobody has measured, and for a workspace whose
      serving config differs from ours.
    * ``core.llm.model_capabilities`` — MEASURED refusals. This is what makes the
      filter real: the column was empty for all 63 seeded models, so the filter
      existed and stripped nothing, and `temperature` went to claude-opus-5 as a
      400 in production.

    Union rather than either/or so a declaration can only ever ADD to what we
    know, never quietly cancel a measured refusal.

    Matched on the SERVED model name with the catalogue key as a fallback: the key
    is a Kasal alias and can differ from what the endpoint runs.
    """
    from src.core.llm.model_capabilities import refused_params

    declared = model_config_dict.get("unsupported_params") or []
    measured = refused_params(served_model) or refused_params(
        model_config_dict.get("key")
    )
    return sorted({str(name) for name in declared} | set(measured))


# The subprocess OBO token fallback lives in core/llm/subprocess_token.py — it is
# process state that usage telemetry reads, and telemetry must not import this
# module to get at it. Re-exported here for the existing call sites.
from src.core.llm.subprocess_token import set_subprocess_user_token

# Register Databricks model context windows with CrewAI
# This is CRITICAL for CrewAI's respect_context_window to work correctly.
# CrewAI has a hardcoded LLM_CONTEXT_WINDOW_SIZES dictionary that it uses to determine
# when to trigger automatic summarization. Without entries for Databricks models,
# it falls back to DEFAULT_CONTEXT_WINDOW_SIZE (8192 tokens) which is incorrect.
# This causes CrewAI to not summarize when needed, leading to empty responses from
# models like Qwen that silently fail when context is too large.
try:
    from src.core.llm.transport import LLM_CONTEXT_WINDOW_SIZES
    from src.seeds.model_configs import MODEL_CONFIGS

    # The litellm model-id prefix each provider is called with — must match the
    # `prefixed_model` built in configure_crewai_llm, or the lookup misses.
    # "openai/" for vllm/airllm/kimi is a ROUTING prefix, not a claim about who
    # made the model: those endpoints are OpenAI-compatible, so litellm reaches
    # them through its openai client with an explicit api_base (Moonshot for
    # kimi, the self-hosted box for vllm). OpenAI itself takes no prefix.
    _PROVIDER_PREFIXES = {
        "databricks": "databricks/",
        "vllm": "openai/",
        "airllm": "openai/",
        "kimi": "openai/",
        "openai": "",
        "anthropic": "anthropic/",
        "gemini": "gemini/",
        "deepseek": "deepseek/",
        "ollama": "ollama/",
    }

    registered_count = 0
    for model_name, config in MODEL_CONFIGS.items():
        provider = config.get("provider")
        context_window = config.get("context_window", 128000)
        # EVERY seeded model is registered, not just the Databricks/self-hosted
        # ones. An unregistered model falls back to DEFAULT_CONTEXT_WINDOW_SIZE
        # (8192 → 6963 after the 0.85 derate), so CrewAI's respect_context_window
        # and our max_tokens clamp would compact a 1M-token model at ~7k. The
        # engine's built-in table happened to cover the older catalogue (gpt-4o,
        # gemini-2.0-flash, deepseek-chat), which hid the gap; their replacements
        # — gpt-5.6, claude-5, gemini-3.x, deepseek-v4 — predate no table at all.
        if provider not in _PROVIDER_PREFIXES:
            logger.debug(
                f"No litellm prefix known for provider {provider!r}; skipping {model_name}"
            )
            continue
        prefix = _PROVIDER_PREFIXES[provider]
        # Register the bare name too: an agent config may carry it unprefixed,
        # and the lookup is by exact key.
        keys = [f"{prefix}{model_name}", model_name] if prefix else [model_name]
        # Ollama ids are normalized hyphen→colon before the call.
        if provider == "ollama" and "-" in model_name:
            keys.append(f"{prefix}{model_name.replace('-', ':')}")
        for key in keys:
            LLM_CONTEXT_WINDOW_SIZES[key] = context_window
        registered_count += 1
        logger.debug(
            f"Registered {keys} with context_window={context_window} in CrewAI"
        )

    logger.info(
        f"Registered {registered_count} models with CrewAI for context window management"
    )
except Exception as reg_err:
    logger.warning(f"Could not register Databricks models with CrewAI: {reg_err}")


# _context_window_for() read the same registry the engine reads, for the output
# clamp that now lives in OpenAICompletion._raw_context_window. The registration
# loop above is what this module still owns: teaching the engine the windows of
# models only kasal's catalogue knows about.


# Which errors mean "context window overflow" is owned by src/core/llm/
# context_limits.py, which extends the engine's CONTEXT_LIMIT_ERRORS once with
# every phrasing kasal's endpoints emit (vLLM, Anthropic-on-Databricks, ...).
# Importing it applies the extension; DatabricksRetryLLM matches against the same
# list rather than a private copy.
from src.core.llm import context_limits as _context_limits  # noqa: F401

# Check if handlers already exist to avoid duplicates
if not logger.handlers:
    file_handler = logging.FileHandler(log_file_path)
    formatter = logging.Formatter(
        "%(asctime)s - %(process)d - %(filename)s-%(funcName)s:%(lineno)d - %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


logger.info(f"LLM operations log file: {log_file_path}")


class LiteLLMFileLogger(CustomLogger):
    """Logs LiteLLM calls to the llm.log file using the module logger."""

    def log_success_event(self, kwargs, response_obj, start_time, end_time):
        model = kwargs.get("model", "unknown")
        duration = (
            (end_time - start_time).total_seconds()
            if hasattr(end_time - start_time, "total_seconds")
            else 0
        )
        usage = {}
        if hasattr(response_obj, "usage") and response_obj.usage:
            usage = {
                "prompt_tokens": getattr(response_obj.usage, "prompt_tokens", 0),
                "completion_tokens": getattr(
                    response_obj.usage, "completion_tokens", 0
                ),
                "total_tokens": getattr(response_obj.usage, "total_tokens", 0),
            }
        logger.info(
            f"LLM success: model={model}, duration={duration:.2f}s, usage={usage}"
        )

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

# Token telemetry used to be a litellm CustomLogger registered here on
# litellm.callbacks / success_callback. litellm is no longer on the path of
# any crew, flow or chat call, so it silently stopped reporting. It now
# listens on the engine's LLMCallCompletedEvent, which carries the same usage
# dict the engine already counts — see src/core/llm/usage_telemetry.py.
from src.core.llm.usage_telemetry import register_usage_telemetry

register_usage_telemetry()

# litellm globals. SCOPE: these reach exactly ONE call path —
# LLMManager.completion_with_usage, which calls litellm.completion directly for
# Anthropic prompt caching (it needs structured content blocks and the usage
# block back). Every other LLM call in kasal goes through the engine, which never
# imports litellm's completion path, so nothing here affects crew, flow or chat
# runs. `drop_params = True` was also set here and is gone: it only ever
# described litellm's behaviour, but its presence made it look as though
# unsupported params were being filtered everywhere — which is precisely the
# assumption that let temperature reach a GPT-5 endpoint and 400.
litellm.modify_params = True  # Anthropic API compatibility
litellm.num_retries = 5
litellm.retry_on = ["429", "timeout", "rate_limit_error"]


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
            try:
                litellm.enable_cache(type="disk", disk_cache_dir=disk_dir, ttl=ttl)
                logger.info(f"LiteLLM disk cache enabled (dir={disk_dir}, ttl={ttl}s)")
                return
            except Exception as disk_err:
                # Disk caching needs LiteLLM's optional dependency (litellm[caching],
                # i.e. the `diskcache` package). When it's absent, fall back to the
                # in-memory cache so callers still get caching (just without the
                # cross-subprocess persistence) instead of NO cache at all. Install
                # litellm[caching] to restore persistent disk caching.
                logger.info(
                    f"LiteLLM disk cache unavailable ({disk_err}); falling back to "
                    "in-memory cache. Install litellm[caching] for persistent disk caching."
                )
                cache_type = "local"

        litellm.enable_cache(type=cache_type, ttl=ttl)
        logger.info(f"LiteLLM cache enabled (type={cache_type}, ttl={ttl}s)")
    except Exception as e:
        logger.warning(f"Failed to configure LiteLLM caching ({cache_type}): {e}")


_configure_litellm_caching()

# MLflow configuration and the MLflowTrackedLLM wrapper used to live here.
# Both are gone: MLflow setup is owned by
# src/services/otel_tracing/mlflow_setup.py, which configures it in the
# EXECUTION SUBPROCESS with the right experiment, UC trace location and
# autologs. Two copies meant two places racing to set DATABRICKS_HOST /
# DATABRICKS_TOKEN and to call mlflow.set_experiment. The wrapper had no
# callers at all — get_llm returns the built LLM directly.

# File logging for the litellm path (completion_with_usage only — see the scope
# note above). Token telemetry is NOT registered here any more: it listens on the
# engine's event bus so it covers every call, not just this one.
litellm.success_callback = [litellm_file_logger]
litellm.failure_callback = [litellm_file_logger]

# Configure logging
logger.info(f"Configured LiteLLM to write logs to: {log_file_path}")

# Export functions for external use
__all__ = ["LLMManager", "DatabricksRetryLLM"]


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
    if "400" in exc_str and (
        "bad request" in exc_str.lower() or "BadRequest" in exc_str
    ):
        return True
    return False


class LLMManager:
    """The public facade for LLM work in kasal.

    Resolves a model KEY from the catalogue to a configured ``kasal_engine`` LLM
    (credentials, endpoint, per-endpoint parameter rules) and exposes the handful
    of entry points the rest of the codebase uses — ``completion`` alone has 38
    call sites. Implementation detail lives in ``src/core/llm/``; this class stays
    stable so callers do not have to track it.

    The embeddings circuit-breaker state that used to be declared here moved with
    the code to ``src/core/llm/embeddings.py``.
    """

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
            if group_context and hasattr(group_context, "primary_group_id"):
                group_id = group_context.primary_group_id
                if group_id:
                    return group_id
        except Exception as e:
            logger.warning(f"Could not get group_id from UserContext: {e}")

        # If group_id is required, raise error
        if required:
            logger.error(
                "Cannot retrieve API keys: no group_id available (multi-tenant isolation required)"
            )
            raise ValueError(
                "group_id is required for API key operations (multi-tenant isolation)"
            )

        # Otherwise return None
        return None

    @staticmethod
    async def completion(
        messages: List[Dict[str, str]],
        model: str,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        extra_headers: Optional[Dict[str, str]] = None,
        fallback_drop_system_on_400: bool = False,
        with_served_model: bool = False,
        response_format: Optional[Any] = None,
    ) -> Union[str, Tuple[str, str]]:
        """
        Unified async completion method that routes through CrewAI's LLM class.

        Args:
            messages: List of message dicts with 'role' and 'content' keys
            model: Model identifier (e.g. 'databricks-llama-4-maverick')
            temperature: Sampling temperature (default 0.7)
            max_tokens: Maximum tokens in response. None (default) inherits the
                model config's max_output_tokens already applied by
                configure_kasal_llm; a last-resort 4000 cap applies only when
                neither the caller nor the model config sets a budget.
            extra_headers: Optional extra HTTP headers (e.g. User-Agent for telemetry)
            fallback_drop_system_on_400: If True and the call raises an HTTP 400,
                retry once with system messages removed (user messages only).
                Handles models that reject system+user dual-message payloads.
            with_served_model: Return ``(content, served_model_key)`` instead of
                just the content. ``model`` is only what was ASKED for — a
                Databricks model on a deployment with no workspace resolves to a
                stand-in — so callers that record which model answered (the
                llmlog rows behind the LLM Logs table) need the resolved key.
                Opt-in so the 38+ existing call sites keep their str return.

        Returns:
            str: The LLM response content string, or ``(content, served_model)``
            when ``with_served_model`` is set.

        Raises:
            ValueError: If model configuration is not found or group_id is unavailable
            Exception: For LLM call errors
        """
        group_id = LLMManager._get_group_id_from_context(required=True)
        llm = await LLMManager.configure_kasal_llm(model, group_id, temperature)
        if response_format is not None:
            # Constrains the REQUEST: the transport turns a pydantic model into a
            # {"type": "json_schema", …} param, so a required field cannot be
            # omitted from the reply. Deliberately NOT passed as ``call``'s
            # ``response_model``, which would coerce the reply into the model —
            # this method's own logging and its 38+ callers expect a str, and the
            # callers that want structure already parse the JSON themselves.
            llm.response_format = response_format
        # The model that actually serves this call — configure_kasal_llm may have
        # substituted one. Reporting the requested name is what made the logs
        # claim a Databricks model ran on a workspace where no Databricks
        # endpoint is reachable at all.
        _resolved = (getattr(llm, "model", None) or model).rsplit("/", 1)[-1]
        if not isinstance(_resolved, str):  # defensive: mocked LLMs in tests
            _resolved = model
        served_model = (
            _resolved if _resolved == model else f"{_resolved} (for '{model}')"
        )
        if max_tokens is not None:
            # Responses-API models (the GPT-5/Codex family, whether served by
            # OpenAI or Databricks) reject max_output_tokens below 16 with
            # "invalid max_output_tokens: integer below minimum value". Tiny
            # caps only ever mean "ping / one-word reply", so floor the value
            # centrally instead of teaching every caller the provider quirk.
            llm.max_tokens = max(16, max_tokens)
        elif not getattr(llm, "max_tokens", None) and not getattr(
            llm, "max_completion_tokens", None
        ):
            llm.max_tokens = 4000
        if extra_headers:
            # Pass extra_headers to the underlying litellm call via LLM extra_headers param
            llm.extra_headers = extra_headers

        # Emit an MLflow LLM span for this call so the model + messages +
        # response show up in the active trace (generation/dispatcher root, or a
        # crew-execution trace). litellm autolog is muted in the parent process
        # (log_traces=False), so without this a raw completion leaves no span.
        # Guarded on an ALREADY-active span so standalone callers never spawn an
        # orphan root trace; fully best-effort so tracing can never break the
        # actual LLM call.
        span_cm: Any = nullcontext()
        try:
            import mlflow as _mlflow

            if (
                hasattr(_mlflow, "get_current_active_span")
                and hasattr(_mlflow, "start_span")
                and _mlflow.get_current_active_span() is not None
            ):
                span_cm = _mlflow.start_span(name="llm_completion", span_type="LLM")
        except Exception:
            span_cm = nullcontext()

        def _set_span_outputs(sp: Any, res: Optional[str]) -> None:
            if sp is not None and hasattr(sp, "set_outputs"):
                try:
                    sp.set_outputs({"response": res[:500] if res else ""})
                except Exception:
                    pass

        # Use sync call() in a thread to ensure custom wrappers
        # (e.g. DatabricksRetryLLM) are invoked correctly.
        # The async acall() bypasses those overrides.
        start_time = time.time()
        with span_cm as _span:
            if _span is not None and hasattr(_span, "set_inputs"):
                try:
                    _span.set_inputs(
                        {
                            "model": model,
                            "messages": messages,
                            "temperature": temperature,
                        }
                    )
                except Exception:
                    pass
            try:
                result = await _run_llm_blocking(llm.call, messages)
                duration = time.time() - start_time
                logger.info(
                    f"LLM completion: model={served_model}, duration={duration:.2f}s, response_length={len(result) if result else 0}"
                )
                _set_span_outputs(_span, result)
                return (result, _resolved) if with_served_model else result
            except Exception as e:
                duration = time.time() - start_time
                # On HTTP 400 with fallback enabled, retry without system messages
                if fallback_drop_system_on_400 and _is_http_400(e):
                    user_only = [m for m in messages if m.get("role") != "system"]
                    if user_only and len(user_only) < len(messages):
                        logger.warning(
                            f"LLM completion got 400, retrying without system message: model={served_model}"
                        )
                        try:
                            result = await _run_llm_blocking(llm.call, user_only)
                            fallback_duration = time.time() - start_time
                            logger.info(
                                f"LLM completion (user-only fallback): model={served_model}, "
                                f"duration={fallback_duration:.2f}s, response_length={len(result) if result else 0}"
                            )
                            _set_span_outputs(_span, result)
                            return (result, _resolved) if with_served_model else result
                        except Exception as retry_err:
                            logger.error(
                                f"LLM completion user-only fallback also failed: {retry_err}"
                            )
                            raise retry_err
                logger.error(
                    f"LLM completion failed: model={served_model}, duration={duration:.2f}s, error={e}"
                )
                raise

    @staticmethod
    async def completion_with_usage(
        messages: List[Dict[str, Any]],
        model: str,
        temperature: float = 0.1,
        max_tokens: Optional[int] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Completion that supports Anthropic prompt caching and returns usage.

        Unlike ``completion()`` (which delegates to CrewAI's ``LLM.call()`` and
        returns only ``str``, discarding ``usage``), this path calls litellm
        directly with the RESOLVED auth/base/model from ``configure_kasal_llm``,
        so it can:
          - pass STRUCTURED content blocks (a ``list`` of ``{type,text,cache_control}``
            parts) through to the serving endpoint — required to mark a stable
            skill-corpus prefix with ``cache_control: {"type": "ephemeral"}``;
          - return the ``usage`` block so callers can observe
            ``cache_read_input_tokens`` (cache hits are otherwise invisible).

        ``messages`` items may carry either a plain string ``content`` or a list
        of content-part dicts (litellm/Anthropic structured format).

        Returns ``{"content": str, "usage": dict}``. Reuses the centralized auth,
        User-Agent telemetry, and MLflow span that ``completion()`` uses.
        """
        import litellm

        group_id = LLMManager._get_group_id_from_context(required=True)
        llm = await LLMManager.configure_kasal_llm(model, group_id, temperature)

        # Read the resolved transport params off the configured LLM so we reuse
        # the exact auth/base/model resolution (OBO/PAT/SPN) without forking it.
        call_kwargs: Dict[str, Any] = {
            "model": getattr(llm, "model", None) or model,
            "messages": messages,
            "temperature": temperature,
        }
        for attr in ("api_key", "api_base"):
            val = getattr(llm, attr, None)
            if val:
                call_kwargs[attr] = val
        # Merge telemetry headers with any caller-supplied ones.
        headers = dict(getattr(llm, "extra_headers", None) or {})
        if extra_headers:
            headers.update(extra_headers)
        if headers:
            call_kwargs["extra_headers"] = headers
        if max_tokens is not None:
            call_kwargs["max_tokens"] = max_tokens
        elif getattr(llm, "max_tokens", None):
            call_kwargs["max_tokens"] = llm.max_tokens
        elif getattr(llm, "max_completion_tokens", None):
            call_kwargs["max_completion_tokens"] = llm.max_completion_tokens

        # This is the ONE remaining direct litellm call in kasal (everything else
        # goes through the engine). A litellm.completion monkey patch used to
        # sanitize messages for Databricks on its behalf; that patch is gone, so
        # apply the sanitizer explicitly — Databricks rejects assistant messages
        # whose content is empty while carrying tool_calls.
        if isinstance(messages, list):
            DatabricksRetryLLM._sanitize_messages_for_databricks(messages)

        start_time = time.time()
        resp = await asyncio.to_thread(lambda: litellm.completion(**call_kwargs))
        duration = time.time() - start_time

        content = ""
        try:
            content = resp.choices[0].message.content or ""
        except Exception:  # noqa: BLE001 — defensive extraction
            content = ""
        usage: Dict[str, Any] = {}
        try:
            u = getattr(resp, "usage", None)
            if u is not None:
                usage = u.model_dump() if hasattr(u, "model_dump") else dict(u)
        except Exception:  # noqa: BLE001
            usage = {}

        cache_read = (
            usage.get("cache_read_input_tokens") or usage.get("cache_read_tokens") or 0
        )
        logger.info(
            f"LLM completion_with_usage: model={model}, duration={duration:.2f}s, "
            f"response_length={len(content)}, cache_read_input_tokens={cache_read}"
        )
        return {"content": content, "usage": usage}

    @staticmethod
    async def configure_kasal_llm(
        model_name: str, group_id: str, temperature: Optional[float] = None
    ) -> LLM:
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
            raise ValueError(
                "group_id is REQUIRED for configure_kasal_llm (multi-tenant isolation)"
            )

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

        # The catalogue's own temperature, used when the caller states none.
        #
        # `ModelConfig.temperature` has been a seeded column on every model for
        # as long as the catalogue has existed and NOTHING read it: the two
        # blocks below both key off the `temperature` ARGUMENT, so a caller
        # passing None sent no temperature at all and the endpoint's default
        # (1.0 for an OpenAI-compatible server) silently won. `build_agent_llm`
        # passes None whenever an agent spec omits `temperature` — the common
        # case — so on crew and flow runs the column was dead data. The symptom
        # is invisible by construction: the run works, at a sampling setting
        # nobody chose, and the "Setting temperature ..." line below is simply
        # absent from the log.
        #
        # An explicit argument still wins; this only fills the gap. Both
        # `rejects_temperature` guards below still apply, so a model whose
        # endpoint 400s on the parameter is unaffected by what its row says.
        if temperature is None and model_config_dict.get("temperature") is not None:
            temperature = model_config_dict["temperature"]
            logger.info(
                f"Using catalogue temperature {temperature} for model "
                f"{model_name_value} (no override supplied)"
            )

        # Name the model that will ACTUALLY serve. get_model_config may have
        # substituted one (a Databricks model on a deployment with no workspace
        # resolves to a local/hosted stand-in), and logging the REQUESTED name
        # next to the RESOLVED provider read as "haiku is running on openai".
        if model_name_value != model_name:
            logger.info(
                "Configuring CrewAI LLM with provider: %s, model: %s "
                "(substituted for requested '%s')",
                provider,
                model_name_value,
                model_name,
            )
        else:
            logger.info(
                f"Configuring CrewAI LLM with provider: {provider}, model: {model_name_value}"
            )

        # Get API key for the provider using ApiKeysService
        api_key = None
        api_base = None

        # Set the correct provider prefix based on provider
        # Note: group_id is already passed as parameter to this function
        if provider == ModelProvider.DEEPSEEK:
            api_key = await ApiKeysService.get_provider_api_key(
                provider, group_id=group_id
            )
            api_base = os.getenv("DEEPSEEK_ENDPOINT", "https://api.deepseek.com")
            # No prefix. DeepSeek's endpoint is OpenAI-compatible and is reached
            # by api_base, so the model field must carry the bare name — this
            # branch sent "deepseek/deepseek-v4-flash" and the API rejected every
            # call with a 400: "The supported API model names are deepseek-v4-pro
            # or deepseek-v4-flash, but you passed deepseek/deepseek-v4-flash."
            #
            # The prefix was for litellm's router, which is not on this path.
            # It survived because LLM._split_provider_prefix only strips a prefix
            # it recognises AND only when that prefix is "openai" — "deepseek" is
            # in neither set, so the name travelled to the wire intact. Nothing
            # failed loudly at build time; every DeepSeek model simply looked
            # like it no longer existed.
            prefixed_model = model_name_value
        elif provider == ModelProvider.OPENAI:
            api_key = await ApiKeysService.get_provider_api_key(
                provider, group_id=group_id
            )
            # OpenAI doesn't need a prefix
            prefixed_model = model_name_value
        elif provider == ModelProvider.ANTHROPIC:
            api_key = await ApiKeysService.get_provider_api_key(
                provider, group_id=group_id
            )
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
                    api_base = DatabricksURLUtils.construct_llm_base_url(
                        auth.workspace_url
                    )
                    logger.info(
                        f"Using Databricks {auth.auth_method} authentication for CrewAI LLM"
                    )
                else:
                    # FAIL CLOSED: no usable Databricks credential resolved for the
                    # SELECTED workspace (OBO -> PAT -> SPN all unavailable for this
                    # group_id). Do NOT proceed with api_key=None — that silently
                    # falls through to litellm's OpenAI placeholder ("OPENAI_API_KEY
                    # is required"), which is a confusing error for a Databricks model.
                    # A workspace with no credentials must fail loudly and clearly.
                    raise ValueError(
                        f"No Databricks credentials available for workspace '{group_id}'. "
                        f"Add a Databricks API key (PAT) for this workspace under "
                        f"Configuration -> API Keys, or run with OBO / Service Principal auth."
                    )

            except ImportError:
                # SECURITY: databricks_auth module is required - no fallback allowed
                logger.error(
                    "Unified Databricks auth module not available for CrewAI LLM"
                )
                raise ImportError(
                    "databricks_auth module is required for Databricks authentication"
                )

            prefixed_model = f"databricks/{model_name_value}"
            is_gpt5 = (
                "gpt-5" in model_name_value.lower()
                or "gpt5" in model_name_value.lower()
            )
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

            # `additional_drop_params` used to be set here for GPT-5 (stop,
            # temperature, presence_penalty, frequency_penalty, logit_bias) and for
            # any temperature-rejecting model. It was a litellm knob and the engine
            # ignores it entirely, so it protected nothing. What actually keeps
            # those params off the wire now:
            #   - temperature: omitted below when the model rejects it;
            #   - stop: the engine only sends it when supports_stop_words(), which
            #     is False for gpt-5 and the o-series;
            #   - presence_penalty / frequency_penalty / logit_bias: never set by
            #     kasal, and the engine only forwards params that are not None.
            if is_gpt5:
                logger.info(
                    f"Databricks GPT-5 model: {model_name_value} — 300s timeout set"
                )
            elif rejects_temperature:
                logger.info(
                    f"Databricks model {model_name_value} rejects temperature — omitting it"
                )

            # Add temperature only for models that accept it.
            if temperature is not None and not rejects_temperature:
                llm_params["temperature"] = temperature
                logger.info(
                    f"Setting temperature to {temperature} for model {prefixed_model}"
                )

            # Add API key and base URL if available
            if api_key:
                llm_params["api_key"] = api_key
            if api_base:
                llm_params["api_base"] = api_base

            # Add User-Agent header for Databricks API attribution
            # Using extra_headers instead of user_agent param (which Databricks rejects in body)
            from src.utils.telemetry import KasalProduct, get_user_agent_header

            llm_params["extra_headers"] = get_user_agent_header(KasalProduct.AGENT)

            # Add max_output_tokens if defined in model config
            if (
                "max_output_tokens" in model_config_dict
                and model_config_dict["max_output_tokens"]
            ):
                if is_gpt5:
                    # GPT-5 requires max_completion_tokens (litellm Databricks transformer
                    # rewrites it to max_tokens which GPT-5 rejects — litellm#13719)
                    llm_params["max_completion_tokens"] = model_config_dict[
                        "max_output_tokens"
                    ]
                    logger.info(
                        f"Setting max_completion_tokens to {model_config_dict['max_output_tokens']} for Databricks GPT-5 model {prefixed_model}"
                    )
                else:
                    llm_params["max_tokens"] = model_config_dict["max_output_tokens"]
                    logger.info(
                        f"Setting max_tokens to {model_config_dict['max_output_tokens']} for model {prefixed_model}"
                    )

            logger.info(
                f"Creating CrewAI LLM with model: {prefixed_model}, has_api_key: {bool(api_key)}, api_base: {api_base}"
            )

            # gpt-5-3-codex ONLY supports the Responses API on Databricks.
            # DatabricksResponsesLLM extends OpenAICompletion with:
            #  - phase preservation (prevents early stopping / skipped tool calls)
            #  - stop-word suppression (GPT-5 reasoning rejects 'stop')
            #  - diagnostic logging for tool-calling debugging
            if "gpt-5-3-codex" in model_name_value.lower():
                from src.services.llm.handlers.databricks_responses_llm import (
                    DatabricksResponsesLLM,
                )

                # The Responses API is served under a DIFFERENT base path than chat:
                # /ai-gateway/openai/v1 (gateway) or /serving-endpoints (otherwise).
                # `api_base` here is the CHAT base (/ai-gateway/mlflow/v1 when the
                # gateway is on), which has no /responses route — using it yields a
                # 404 "Supervisor API is not enabled". Build the Responses base instead.
                responses_workspace = (
                    DatabricksURLUtils.extract_workspace_from_endpoint(api_base)
                )
                responses_base_url = DatabricksURLUtils.construct_responses_base_url(
                    responses_workspace
                )
                logger.info(
                    f"Using DatabricksResponsesLLM for Responses API model: {model_name_value} (base_url={responses_base_url})"
                )
                return DatabricksResponsesLLM(
                    model=model_name_value,
                    api_key=api_key,
                    base_url=responses_base_url,
                    timeout=300,
                    max_tokens=llm_params.get("max_completion_tokens")
                    or llm_params.get("max_tokens"),
                )

            # Use DatabricksRetryLLM for all other Databricks models (GPT-OSS, Llama, Claude, etc.)
            # Provides retry logic for empty responses, rate limits, and message sanitization.
            # Databricks-specific message sanitization (empty content, Llama format,
            # Gemini system-prompt merge and $ref resolution) happens inside call().
            # Declared sampling parameters, filtered by what this endpoint
            # accepts. One resolve() for every provider — see services/llm/params.
            llm_params.update(
                resolve_llm_params(
                    model_config_dict.get("params"),
                    unsupported=_refused_params(model_config_dict, model_name_value),
                )
            )
            logger.info(
                f"Using DatabricksRetryLLM wrapper for Databricks model: {model_name_value}"
            )
            return DatabricksRetryLLM(**llm_params)
        elif provider == ModelProvider.VLLM:
            # Self-hosted vLLM server — OpenAI-compatible endpoint
            api_base = os.getenv("VLLM_BASE_URL", "http://localhost:8081/v1")
            api_key = os.getenv("VLLM_API_KEY", "vllm")
            # No prefix, exactly like the OpenAI branch above. This used to build
            # "openai/<model>" so litellm would route it, and the register_model
            # call that went with it is already gone (the transport asks its own
            # BaseLLM, which reports every model tool-capable and never consults
            # litellm's registry). What the prefix still did was mislabel the
            # model: LLM._split_provider_prefix sees a known "openai/" prefix,
            # sets provider="openai", and strips it back off — so a self-hosted
            # Qwen reported itself as an OpenAI model in every log and repr, and
            # the wire value was identical either way. Dropping it leaves
            # provider unset, which is what every other OpenAI-protocol endpoint
            # here does; the one consumer (instructor._extract_provider) already
            # defaults to "openai" for the protocol.
            prefixed_model = model_name_value
        elif provider == ModelProvider.KIMI:
            # Kimi (Moonshot AI) — OpenAI-compatible endpoint. litellm 1.74.x has no
            # native "moonshot" provider, so route via the openai/ prefix with an
            # explicit api_base, exactly like the self-hosted vLLM path.
            api_key = await ApiKeysService.get_provider_api_key(
                provider, group_id=group_id
            )
            if not api_key:
                raise ValueError(
                    f"No Kimi API key found for workspace '{group_id}'. "
                    f"Add KIMI_API_KEY under Configuration -> API Keys."
                )
            api_base = os.getenv("KIMI_ENDPOINT", "https://api.moonshot.ai/v1")
            prefixed_model = f"openai/{model_name_value}"
            # (see the vLLM branch: the litellm.register_model call that used to
            # be here was inert once the engine stopped reading litellm's registry)
        elif provider == ModelProvider.GEMINI:
            # SECURITY: Use group_id parameter for multi-tenant isolation
            api_key = await ApiKeysService.get_provider_api_key(
                provider, group_id=group_id
            )
            # SECURITY: do NOT write the per-tenant key into the shared process
            # os.environ — it persists across requests and would leak to other
            # tenants and to spawned subprocesses (cross-tenant credential bleed).
            # The key is passed per-request via llm_params["api_key"] below.
            if not api_key:
                logger.warning(f"No API key found for Gemini with group_id: {group_id}")
                # Help Instructor pick the right model family when no key is set.
                os.environ["INSTRUCTOR_MODEL_NAME"] = "gemini"

            prefixed_model = f"gemini/{model_name_value}"
        else:
            # Default fallback for other providers
            logger.warning(f"Using default model name format for provider: {provider}")
            prefixed_model = (
                f"{provider.lower()}/{model_name_value}"
                if provider
                else model_name_value
            )

        # Configure LLM parameters (for all providers except Databricks which returns early)
        # 300s across the board: reasoning models can take 2-4 minutes on a complex
        # prompt, and no provider here is served by an endpoint that caps lower.
        # (This used to be a ternary whose two branches were both 300, under a
        # comment claiming 120s for non-GPT-5 models.)
        llm_params = {
            "model": prefixed_model,
            "timeout": 300,
        }

        # Temperature is OMITTED — never merely "dropped later" — for models whose
        # endpoint rejects it. There is no drop_params safety net any more:
        # `drop_params` / `additional_drop_params` were litellm knobs, and the
        # engine's LLM neither reads nor forwards them (pydantic `extra="allow"`
        # swallowed them silently), so every param set here IS sent. Three call
        # sites used to set them; all are gone.
        #
        # Two reasons a model refuses the param:
        #   - model_rejects_temperature(): the GPT-5 family and Claude Opus 4.7+ /
        #     Fable 5 return 400 for any temperature. Previously only the Databricks
        #     branch consulted this, so a DIRECT-OpenAI gpt-5 model was sent
        #     temperature=0.7 and 400ed ("Only the default (1) is supported").
        #   - Kimi K2.x: 400s on ANY temperature other than 1 ("invalid temperature:
        #     only 1 is allowed for this model"). The seeds pass 0.7 and the A2UI
        #     surface composer passes 0, both of which silently killed surface
        #     generation. Omitted = server default (1), which is what we want.
        from src.utils.model_config import model_rejects_temperature

        rejects_temperature = (
            model_rejects_temperature(model_name_value)
            or provider == ModelProvider.KIMI
        )
        if temperature is not None and not rejects_temperature:
            llm_params["temperature"] = temperature
            logger.info(
                f"Setting temperature to {temperature} for model {prefixed_model}"
            )
        elif temperature is not None:
            logger.info(
                f"Model {model_name_value} rejects `temperature` — omitting it "
                f"(requested {temperature})"
            )
        # NOTE for Kimi: `tool_choice` must likewise never be forced — K2.7 cannot
        # disable thinking, and a forced tool_choice 400s ("tool_choice 'specified'
        # is incompatible with thinking enabled"). Nothing FORCES tool_choice for
        # any model now — the two handlers that did (vLLM's opening turn, the
        # codex counter) are gone. The one handler that still names a value sends
        # "auto", it serves self-hosted vLLM only, and Kimi has its own branch
        # returning a plain LLM, so nothing reaches Kimi to strip. Any future
        # forced-tool path must still exclude it.

        # Add API key and base URL if available
        if api_key:
            llm_params["api_key"] = api_key
        if api_base:
            llm_params["api_base"] = api_base

        # Add max_output_tokens if defined in model config
        if (
            "max_output_tokens" in model_config_dict
            and model_config_dict["max_output_tokens"]
        ):
            # GPT-5 and newer OpenAI reasoning models take max_completion_tokens
            # instead of max_tokens (the engine sends whichever is set, preferring
            # max_completion_tokens).
            if provider == ModelProvider.OPENAI and "gpt-5" in model_name_value.lower():
                llm_params["max_completion_tokens"] = model_config_dict[
                    "max_output_tokens"
                ]
                logger.info(
                    f"Setting max_completion_tokens to {model_config_dict['max_output_tokens']} for GPT-5 model {prefixed_model}"
                )
            else:
                llm_params["max_tokens"] = model_config_dict["max_output_tokens"]
                logger.info(
                    f"Setting max_tokens to {model_config_dict['max_output_tokens']} for model {prefixed_model}"
                )

        # Declared sampling parameters, filtered by what this endpoint accepts.
        # Unset stays unset: a model with no `params` sends exactly what it sent
        # before this existed.
        llm_params.update(
            resolve_llm_params(
                model_config_dict.get("params"),
                unsupported=_refused_params(model_config_dict, model_name_value),
            )
        )

        # Anthropic extended thinking. `ModelConfig.extended_thinking` has existed
        # (and been editable in the Edit Model dialog) since before this line: it
        # was stored, seeded and returned by the API, but NOTHING ever read it when
        # building an LLM, so the toggle silently did nothing.
        #
        # Claude takes a token BUDGET, not an effort level — the transport turns
        # this into `extra_body: {"thinking": {"type": "enabled", "budget_tokens":
        # N}}` and raises max_tokens to satisfy the endpoint's
        # `max_tokens > budget_tokens` rule. Only the Claude 4.x line honours it
        # (see _SUPPORTS_THINKING_BUDGET_RE); for anything else the transport
        # drops it, so setting it here is safe for every provider.
        if model_config_dict.get("extended_thinking"):
            # `thinking_budget_tokens` doubles as the on-switch: the transport
            # treats any positive value as "thinking on" and then picks the shape
            # from the model. On adaptive models the number is unused (they reject
            # a budget) and `thinking_effort` carries the depth instead.
            llm_params["thinking_budget_tokens"] = int(
                model_config_dict.get("thinking_budget_tokens")
                or os.getenv("KASAL_THINKING_BUDGET_TOKENS", "10240")
            )
            effort = model_config_dict.get("reasoning_effort")
            if effort:
                llm_params["thinking_effort"] = str(effort).strip().lower()
            from src.core.llm.transport.completion import thinking_mode

            logger.info(
                f"Extended thinking enabled for {prefixed_model} "
                f"(mode={thinking_mode(model_name_value)}, "
                f"budget_tokens={llm_params['thinking_budget_tokens']}, "
                f"effort={llm_params.get('thinking_effort') or 'endpoint default'})"
            )

        logger.info(f"Creating LLM with model: {prefixed_model}")

        # Self-hosted vLLM: a subclass that states tool_choice explicitly rather
        # than inheriting the endpoint's default (see handlers/vllm.py). It used
        # to pin "required" on the opening turn; it now sends "auto", overridable
        # per deployment with VLLM_TOOL_CHOICE. Native function calling needs no
        # help from us — the transport reports every model as tool-capable — and
        # the max_tokens clamp that also lived there is now
        # OpenAICompletion._clamp_output_budget, protecting every provider.
        if (
            provider == ModelProvider.VLLM
            and os.getenv("VLLM_SUPPORTS_TOOLS", "true").lower() == "true"
        ):
            return VLLMFunctionCallingLLM(**llm_params)

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
            raise ValueError(
                "group_id is REQUIRED for get_llm (multi-tenant isolation)"
            )

        return await LLMManager.configure_kasal_llm(model_name, group_id, temperature)

    @staticmethod
    async def load_fallback_candidates(current_model_key: str, group_id: Optional[str]):
        """Enabled models usable as fallback targets for ``DatabricksRetryLLM``.

        Returns ModelCandidate(name, context_window) for every enabled model
        other than the current one, restricted to Databricks-served, non-codex
        models — those can be rebuilt and swapped through
        ``configure_kasal_llm`` with the same auth/endpoint. gpt-5-3-codex is
        excluded because it needs the Responses API (different base path).
        """
        from src.db.session import request_scoped_session
        from src.services.llm.handlers.model_fallback import (
            candidates_from_model_configs,
        )
        from src.services.settings.models import ModelConfigService

        try:
            async with request_scoped_session() as session:
                service = ModelConfigService(session, group_id=group_id)
                models = await service.find_enabled_models()
                return candidates_from_model_configs(models, current_model_key)
        except Exception as e:
            logger.warning(f"Could not load fallback model candidates: {e}")
            return []

    # ---------------------------------------------------------------- embeddings
    # The implementations live in src/core/llm/embeddings.py — a different
    # protocol (direct HTTP to an embeddings endpoint, its own auth, batching and
    # circuit breaker) that never touches the engine's LLM. These two methods stay
    # here because five knowledge/RAG services call them through LLMManager.

    @staticmethod
    async def get_embeddings(
        texts: List[str],
        model: str = "databricks-gte-large-en",
        embedder_config: Optional[Dict[str, Any]] = None,
        batch_size: Optional[int] = None,
    ) -> List[Optional[List[float]]]:
        """Embed many texts, resolving auth once and batching the requests."""
        from src.services.llm.embeddings import get_embeddings

        return await get_embeddings(
            texts, model=model, embedder_config=embedder_config, batch_size=batch_size
        )

    @staticmethod
    async def get_embedding(
        text: str,
        model: str = "databricks-gte-large-en",
        embedder_config: Optional[Dict[str, Any]] = None,
    ) -> Optional[List[float]]:
        """Embed a single text with the configured provider."""
        from src.services.llm.embeddings import get_embedding

        return await get_embedding(text, model=model, embedder_config=embedder_config)
