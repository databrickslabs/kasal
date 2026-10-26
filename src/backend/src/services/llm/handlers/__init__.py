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

- ``VLLMFunctionCallingLLM``: self-hosted vLLM, which states ``tool_choice``
  explicitly instead of inheriting the endpoint's default.

A handler may DECLARE tool policy; it must not decide FOR the model
==================================================================

Two handlers used to decide *for* the model whether it had to call a tool.
``VLLMFunctionCallingLLM`` pinned ``tool_choice="required"`` until a tool result
appeared, and the Responses handler kept ``"required"`` until a tool-call counter
passed ``max(2, min(10, tool_count // 4 + 1))``. Both forcings are gone — the
Responses handler now sets nothing at all, and the vLLM one sends ``"auto"``.

Nobody else does this. CrewAI sets ``tool_choice="auto"`` and stops; LangGraph
never mentions ``tool_choice``; LangChain only passes through what the caller
asked for; LiteLLM drops even a caller's value once a tool result exists. The
forcing was also keyed on the ENDPOINT, which is the one axis none of them use:
a chat greeting and a long crew task hit the same handler and got the same
answer, so "hello how are you" opened with a web search it then invented the
query for. Measured on the live endpoint, 3 samples per cell: ``required``
called a tool 3/3 on a greeting; plain ``auto`` was 0/3 on a greeting and 3/3 on
an explicit search request — correct on every cell.

Ending a tool loop is a separate concern and already solved one layer down, the
way the frameworks solve it: ``core/llm/transport/budget.py`` spends a final call
carrying ``FORCE_FINAL_ANSWER`` **with no tools attached**, so it cannot open
another round. That is where a budget belongs — model-agnostic, not per endpoint.

An explicit ``tool_choice`` from a caller is still honoured end to end; nothing
here invents one.

Known limitation, accepted deliberately
---------------------------------------

Self-hosted Qwen3-Coder under-uses its tools. Given a large scaffolded prompt it
declines ``auto`` and writes an answer from nothing rather than calling the tool
it was handed — reproduced the day the forcing came out, on a chat turn asking
for news that answered without searching. Short prompts tool-call correctly. The
same request on a GPT model works.

This is NOT worked around per model. A ``force_tool_first_turn`` flag on the
catalogue row was written and deliberately reverted: it is a single-model hack
with no precedent in any framework surveyed, and it would have re-armed the
greeting problem for exactly the model it was meant to help — a forced opening
turn cannot tell "gather swiss news" from "hello".

The principled fix is to decide WHICH TOOLS ARE ATTACHED from the request rather
than whether a call is compelled: attach none on a turn that wants none, and no
model can call anything; attach them when the turn wants one, and a reluctant
model has a much smaller decision to get wrong. That is what LangChain's
``LLMToolSelectorMiddleware`` does, and Kasal already has the shape for it in
``services/chat/capability_router.py``. Until that exists, prefer a
tool-following model for tool-heavy work.

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
