# LLM architecture

How an agent's model call is assembled and sent: which layer owns what, why the boundaries sit where they do, and the rules that keep them from drifting apart again.

- [The four layers](#the-four-layers)
- [What each layer owns](#what-each-layer-owns)
- [The path of one call](#the-path-of-one-call)
- [Why the layering is drawn here](#why-the-layering-is-drawn-here)
- [litellm is not on the LLM path](#litellm-is-not-on-the-llm-path)
- [Rules for adding behavior](#rules-for-adding-behavior)
- [Related](#related)

## The four layers

Kasal splits LLM work into four layers. Each one knows only about the layer beneath it, and the split is by *kind of knowledge*, not by convenience.

| Layer | Location | Knows about |
|-------|----------|-------------|
| Facade | `src/backend/src/services/llm/manager.py` | The kasal API other code calls. Stable by contract — `LLMManager.completion` alone has 38+ call sites. |
| Configuration | `src/backend/src/core/llm/` | The model catalog, tenants, credentials, endpoint URLs, telemetry, embeddings. |
| Endpoint policy |  `src/backend/src/services/llm/handlers/` | How one serving endpoint misbehaves: retries, fallback, message sanitization, alternate APIs. |
| Transport | `src/backend/kasal_engine/llm/` | The OpenAI-compatible wire protocol. No database, no tenants, no catalog. |

Only two directories hold LLM code: `src/core/llm/` for everything kasal-specific (handlers included, as a subpackage), and `kasal_engine/llm/` for the transport.

### Why the engine is a separate tree

`kasal_engine` is the vendored package that replaced crewAI. It sits next to `src/` — `src/deploy.py` copies it as a sibling — because **the dependency runs one way**: the engine imports nothing from `src`. No FastAPI, no SQLAlchemy, no tenant context.

That rule is worth keeping because it is *checkable*: `grep -r "from src\." kasal_engine/` returns nothing, so a violation is visible the moment it appears. Fold the transport into `src/core/llm/` and the rule becomes a convention no one can grep for — and the first `src.services` import sliding into the transport layer goes unnoticed. Silent drift of exactly that kind is what produced the duplication described below.

Note also that `kasal_engine/llm` is not a standalone library: it is one subpackage of the engine, and `kasal_engine/memory/memory.py` refers to `BaseLLM`. Moving only `llm/` would make the engine import from `src` — the inversion.

## What each layer owns

### Transport: `kasal_engine/llm/`

The engine is where a request becomes HTTP. It is model-agnostic and tenant-agnostic by design.

| Module | Responsibility |
|--------|----------------|
| `base.py` | `BaseLLM`: the `call()` contract, LLM event emission, token-usage accumulation for `Crew.token_usage`, structured-output validation, copy/deepcopy semantics. |
| `completion.py` | `OpenAICompletion`: the OpenAI SDK client, chat-completions and Responses API loops, tool-call rounds, streaming, context-window trimming and the output clamp, per-model parameter rules. |
| `llm.py` | `LLM`: the class kasal instantiates. Normalizes provider prefixes (`databricks/…`, `openai/…`). |
| `instructor.py` | `InternalInstructor`: structured output by prompting, with per-call credentials. |
| `constants.py` | Context-window sizes and the usage ratio. |
| `exceptions.py` | `CONTEXT_LIMIT_ERRORS` and the context-length exception. |

Behavior that belongs here is anything true of *every* model on an OpenAI-compatible endpoint: the tool-call loop, budget enforcement, usage counting, event emission.

### Configuration: `src/core/llm/`

This layer answers questions the engine deliberately cannot: *which* model, on *whose* credentials, at *which* URL.

| Module | Responsibility |
|--------|----------------|
| `embeddings.py` | Embeddings — a different protocol entirely: direct HTTP to Databricks, Ollama, Google or OpenAI, with batching, auth resolution and a per-provider circuit breaker. Never touches the engine's LLM. |
| `usage_telemetry.py` | Forwards per-call token usage to Databricks logfood, by subscribing to the engine's `LLMCallCompletedEvent`. |
| `context_limits.py` | The single list of error phrasings that mean "context window overflow", extending the engine's own list. |

### Endpoint policy: `src/core/llm/handlers/`

Subclasses of the engine's `LLM` that add what one serving endpoint needs.

| Class | Responsibility |
|-------|----------------|
| `DatabricksRetryLLM` | Retry and backoff (with longer waits for rate limits), OBO token refresh, cross-model fallback, and Databricks message sanitization — empty assistant content, Llama message format, Gemini system-prompt merging and `$ref` resolution. |
| `DatabricksResponsesLLM` | The Databricks Responses API, served under a different base URL than chat completions (`gpt-5-3-codex` today). Preserves the `phase` field on assistant output items across turns, without which the model degrades into early text-only responses. |

Files here are named for the endpoint or protocol they serve, never for a model. Models leave the catalog and a module named after one outlives it — `databricks_gpt_oss_handler.py` sat in the tree long after the models it existed for were pruned.

| `VLLMFunctionCallingLLM` | Self-hosted vLLM: states `tool_choice="auto"` explicitly when tools are offered, rather than inheriting whatever the endpoint defaults to. Overridable per deployment with `VLLM_TOOL_CHOICE`. |

### A handler may declare tool policy; it must not decide for the model

`VLLMFunctionCallingLLM` used to pin `tool_choice="required"` until a tool result appeared, and `DatabricksResponsesLLM` held a second version of the same idea — `"required"` until a tool-call counter passed `max(2, min(10, tool_count // 4 + 1))`. **Both forcings are gone.** The Responses handler now sets nothing; the vLLM one sends `"auto"`, which is what a compliant server already applies when tools are present — the value of stating it is that the policy is explicit and in one place.

No mainstream framework does. CrewAI sets `"auto"` and stops; LangGraph never mentions `tool_choice`; LangChain passes through only what the caller asked for; LiteLLM drops even a caller's value once a tool result exists. Forcing was also keyed on the **endpoint**, the one axis none of them use — so a chat greeting and a long crew task hit the same handler and got the same answer, and "hello how are you" opened with a web search. Measured live, 3 samples per cell: forced called a tool 3/3 on a greeting; plain `auto` was 0/3 on a greeting and 3/3 on an explicit search request.

Ending a runaway tool loop is a separate concern, solved one layer down and model-agnostically: `transport/budget.py` spends a final call carrying `FORCE_FINAL_ANSWER` **with no tools attached**, so it cannot open another round.

A caller that genuinely needs a tool call still passes `tool_choice` explicitly, and it is honoured end to end.

**Known limitation.** Self-hosted Qwen3-Coder under-uses its tools: given a large scaffolded prompt it declines `auto` and answers from nothing rather than calling the tool it was handed, while tool-calling correctly on short prompts. A per-model `force_tool_first_turn` flag was written for it and deliberately reverted — a single-model hack with no precedent in any framework, and one that would have re-armed the greeting problem for the very model it helped, since a forced opening turn cannot tell "gather swiss news" from "hello". The principled fix is to choose **which tools are attached** from the request rather than whether a call is compelled; until that exists, prefer a tool-following model for tool-heavy work.

### Facade: `LLMManager`

Five entry points, and they are the whole public surface:

| Method | Use |
|--------|-----|
| `completion()` | A standalone call returning text — intent detection, generation services, guardrails. |
| `completion_with_usage()` | A call needing Anthropic prompt caching and the `usage` block back. The only direct litellm caller left in kasal. |
| `configure_kasal_llm()` / `get_llm()` | A configured `LLM` for crew, flow and chat execution. |
| `get_embedding()` / `get_embeddings()` | Embedding vectors; thin delegates to `src/core/llm/embeddings.py`. |

## The path of one call

Building an LLM for an agent runs down the layers in order:

1. `LLMManager.configure_kasal_llm(model_key, group_id, temperature)` looks the key up in the model catalog through `ModelConfigService`, scoped to the caller's group.
2. The provider branch resolves credentials and a base URL — Databricks resolves OBO, then PAT, then service principal, and fails closed if none work.
3. Parameters the endpoint accepts are set, and parameters it rejects are **omitted**. A GPT-5-family model is built with no `temperature`; a Kimi model likewise.
4. The right class is chosen: `DatabricksRetryLLM` for Databricks chat models, `DatabricksResponsesLLM` for the Responses API, `VLLMFunctionCallingLLM` for self-hosted vLLM, plain `LLM` otherwise.
5. The engine sends it: trim the conversation if it approaches the window, clamp the output budget so `prompt + max_tokens` fits, run tool-call rounds, emit `LLMCallStartedEvent` / `LLMCallCompletedEvent`, accumulate usage.
6. `usage_telemetry` sees the completion event and forwards token counts.

## Why the layering is drawn here

Kasal used to drive crewAI and litellm. The engine replaced both, and the migration left duplicate implementations on either side of the new boundary — each pair looking reasonable in isolation.

The failures that came out of it share a shape: **the duplicate that stopped running failed silently.**

- Token telemetry ran as a litellm callback. The engine calls the OpenAI SDK, so the callback stopped firing for every crew, flow and chat call. Usage attribution reported nothing, and nothing raised.
- Gemini message and schema fixes lived inside a `litellm.completion` monkey patch, applied "at the litellm level so every code path is covered". No path went through litellm any more, so Databricks-served Gemini models reached the endpoint unsanitized.
- `drop_params` was set on the litellm module and passed when constructing an LLM. The engine ignores it, so a parameter the code believed was being filtered was sent — a GPT-5 model received `temperature` and returned a 400.
- Retries existed in both the OpenAI SDK client and `DatabricksRetryLLM`, each unaware of the other, so a rate-limited call made up to 15 HTTP attempts instead of 5.
- Context-overflow phrases lived in three lists. A phrase learned in one did not help the others, and a missed phrase turns a run that could have compacted into a hard failure.

Each layer above therefore owns a concern *completely*. Where two layers appeared to share one, one of them was already dead.

## litellm is not on the LLM path

litellm remains a dependency, but the engine does not use it. It reaches exactly one function: `LLMManager.completion_with_usage`.

Two consequences worth internalizing:

- Anything configured on the `litellm` module — `drop_params`, callbacks, caching, `register_model`, monkey patches — affects only that one function. It will not change crew, flow or chat behavior.
- **Every parameter set when building an LLM is sent.** There is no drop-params safety net. If an endpoint rejects a parameter, do not set it.

> [!IMPORTANT]
> A fix applied to a shared library the request no longer enters is indistinguishable from no fix at all. Apply fixes on the path the request actually takes.

## Rules for adding behavior

Where new behavior belongs follows from what it needs to know:

- **True for every OpenAI-compatible model?** The engine, in `completion.py` or `base.py`. Budget enforcement, retries of protocol-level errors, response parsing.
- **Needs the catalog, a tenant, credentials or a URL?** `src/core/llm/`.
- **True of one serving endpoint?** A handler subclass in `src/core/llm/handlers/`, named for the endpoint rather than the model.
- **A new entry point for application code?** `LLMManager`, delegating downward.

Two checks before adding a workaround:

1. Does the layer below already do this? The engine reports every model as tool-capable, preserves subclasses through copies, honours `response_model` for every provider, and trims context — several past workarounds were re-asserting exactly that.
2. Does the behavior sit on the path the request takes? Prefer a subclass method over a patch on a shared module.

When a model or endpoint needs a parameter rule, add it where the other rules live: `model_rejects_temperature` and `model_supports_reasoning_effort` in `src/backend/src/utils/model_config.py`, which both the engine and the UI read, so they cannot disagree.

## Related

- [Models](./MODELS.md): the model catalog, provider setup, and model fallback.
- [Solution architecture](./ARCHITECTURE_GUIDE.md): platform layers, request lifecycle, and the security model.
- [Code structure](./CODE_STRUCTURE_GUIDE.md): where each package lives.
- [MLflow tracing setup](./mlflow-tracing-setup.md): how LLM calls become spans.

[Back to the documentation hub](./README.md)
