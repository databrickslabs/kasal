"""
LLM handlers — endpoint-specific subclasses of the engine's LLM class.

Each handler subclasses ``src.core.llm.transport.LLM`` and adds what one serving
endpoint needs, on the path the request actually takes, rather than patching a
shared HTTP layer. Every file here is named for the endpoint or protocol it
serves, never for a model: models come and go from the catalogue, and a module
named after one outlives it (``databricks_gpt_oss_handler.py`` long outlived the
models it existed for).

- ``DatabricksRetryLLM``: retry/backoff, rate-limit handling, OBO token refresh,
  cross-model fallback, and Databricks message sanitization for chat endpoints.
- ``DatabricksResponsesLLM``: the Databricks Responses API, which is served under
  a different base URL than chat completions (gpt-5-3-codex today).
- ``VLLMFunctionCallingLLM``: self-hosted vLLM, which needs a forced tool call on
  the opening turn.

``model_fallback.py`` is a helper for DatabricksRetryLLM's cross-model fallback,
not a handler, so it is not exported here.
"""

from .databricks_responses_llm import DatabricksResponsesLLM
from .databricks_retry_llm import DatabricksRetryLLM
from .vllm import VLLMFunctionCallingLLM

__all__ = [
    "DatabricksRetryLLM",
    "DatabricksResponsesLLM",
    "VLLMFunctionCallingLLM",
]
