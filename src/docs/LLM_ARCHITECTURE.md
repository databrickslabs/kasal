# LLM architecture

How an agent's model call is assembled and sent: which layer owns what, why the boundaries sit where they do, and the rules that keep them from drifting apart again.

- [The four layers](#the-four-layers)
- [What each layer owns](#what-each-layer-owns)
- [The path of one call](#the-path-of-one-call)
- [Anthropic prompt caching](#anthropic-prompt-caching)
- [Why the layering is drawn here](#why-the-layering-is-drawn-here)
- [litellm is not on the LLM path](#litellm-is-not-on-the-llm-path)
- [Rules for adding behavior](#rules-for-adding-behavior)
- [Related](#related)

## The four layers

Kasal splits LLM work into four layers. Each one knows only about the layer beneath it, and the split is by *kind of knowledge*, not by convenience.

| Layer | Location | Knows about |
|-------|----------|-------------|
| Facade | `src/backend/src/services/llm/manager.py` | The kasal API other code calls. Stable by contract — `LLMManager.completion` alone has 38+ call sites. |
| Configuration | `src/backend/src/services/llm/` | The model catalog, tenants, credentials, endpoint URLs, per-endpoint parameter rules, embeddings. A service: it reads the database. |
| Endpoint policy | `src/backend/src/services/llm/handlers/` | How one serving endpoint misbehaves: retries, fallback, message sanitization, alternate APIs. |
| Transport | `src/backend/src/core/llm/transport/` | The OpenAI-compatible wire protocol. No database, no tenants, no catalog. |

Beside them, `src/backend/src/core/llm/` holds what hangs off an LLM call without touching the database: usage telemetry, context-limit phrasings, JSON extraction and the subprocess token.

### Why the transport sits in `core/`

The transport is first-party code in `src/backend/src/core/llm/transport/`, next to the agent runtime that calls it (`services/execution/runtime/`). It once shipped as a separate package; that package no longer exists.

What the separate tree bought is kept by the layering instead: **the dependency runs one way**. The transport imports nothing from `services`, `repositories` or `db` — no SQLAlchemy, no tenant context. That is checkable, not a convention: the import-linter contract "`core/` never imports services" (`[tool.importlinter]` in `src/backend/pyproject.toml`, run by `run_tests.py` and in CI) fails the build the moment a `src.services` import slides into the transport. Silent drift of exactly that kind is what produced the duplication described below.

## What each layer owns

### Transport: `src/core/llm/transport/`

The engine is where a request becomes HTTP. It is model-agnostic and tenant-agnostic by design.

| Module | Responsibility |
|--------|----------------|
| `base.py` | `BaseLLM`: the `call()` contract, LLM event emission, token-usage accumulation for `Crew.token_usage`, structured-output validation, copy/deepcopy semantics. |
| `completion.py` | `OpenAICompletion`: the OpenAI SDK client, chat-completions and Responses API loops, tool-call rounds, streaming, per-model parameter rules. |
| `context_window.py` | `ContextWindowBudget`, mixed into `OpenAICompletion`: the one token estimate (chars ÷ 3.4), the output clamp so `prompt + max_tokens` fits, the pre-request trim that stubs the oldest tool results, and the post-rejection recovery — when the server refuses a prompt the estimate said fit, stub by the server's own count, learn the ratio for the rest of the run, and retry the same round without re-running any tool. |
| `context_recovery.py` | The pure helpers under that recovery: the stub text, reading the server's token count out of its error message (llama.cpp, vLLM, OpenAI and Anthropic phrasings), and the oldest-first stubbing shared with the trim. |
| `llm.py` | `LLM`: the class kasal instantiates. Normalizes provider prefixes (`databricks/…`, `openai/…`). |
| `instructor.py` | `InternalInstructor`: structured output by prompting, with per-call credentials. |
| `constants.py` | Context-window sizes and the usage ratio. |
| `exceptions.py` | `CONTEXT_LIMIT_ERRORS` and the context-length exception. |
| `budget.py`, `request_deadline.py` | The execution budget for one call (tool rounds, `MAX_TOOL_ROUNDS = 15`, and wall clock), and the deadline carried through nested retries and streamed requests. |
| `tool_rounds.py` | Executing one round of tool calls with the budget check, shared by the Chat Completions and Responses API loops. |
| `response_parsing.py` | Pure helpers that pull token counts, tool calls and reasoning items out of a provider response. |
| `rpm.py` | Requests-per-minute throttling for the `max_rpm` setting on agents and crews. |
| `anthropic_client.py`, `anthropic_messages.py` | The native Claude Messages API adapter, reusing the same tool loop, budgets and events. |
| `prompt_cache.py` | Where Anthropic prompt-caching breakpoints go for each endpoint, and stripping them from endpoints that reject them. See [Anthropic prompt caching](#anthropic-prompt-caching). |

Behavior that belongs here is anything true of *every* model on an OpenAI-compatible endpoint: the tool-call loop, budget enforcement, usage counting, event emission.

### Configuration: `src/services/llm/`

This layer answers questions the engine deliberately cannot: *which* model, on *whose* credentials, at *which* URL.

| Module | Responsibility |
|--------|----------------|
| `params.py` | What gets sent with a request, decided in one place: the per-model `ModelConfig.params` declaration turned into request parameters. |
| `embeddings.py` | Embeddings — a different protocol entirely: direct HTTP to Databricks, Ollama, Google or OpenAI, with batching, auth resolution and a per-provider circuit breaker. Never touches the engine's LLM. |
| `src/core/llm/usage_telemetry.py` | Forwards per-call token usage to Databricks logfood, by subscribing to the engine's `LLMCallCompletedEvent`. |
| `src/core/llm/context_limits.py` | The single list of error phrasings that mean "context window overflow", extending the engine's own list. |

### Endpoint policy: `src/services/llm/handlers/`

Subclasses of the engine's `LLM` that add what one serving endpoint needs.

| Class | Responsibility |
|-------|----------------|
| `DatabricksRetryLLM` | Retry and backoff (with longer waits for rate limits), OBO token refresh, cross-model fallback, and Databricks message sanitization — empty assistant content, Llama message format, Gemini system-prompt merging and `$ref` resolution. |
| `DatabricksResponsesLLM` | The native OpenAI Responses API for Databricks-hosted OpenAI endpoints, served under a different base URL than chat completions. Preserves the `phase` field on assistant output items across turns, without which Codex degrades into early text-only responses. |
| `VLLMFunctionCallingLLM` | Self-hosted vLLM: states `tool_choice="auto"` explicitly when tools are offered, rather than inheriting whatever the endpoint defaults to. Overridable per deployment with `VLLM_TOOL_CHOICE`. |

Files here are named for the endpoint or protocol they serve, never for a model. Models leave the catalog and a module named after one outlives it: a handler named for the GPT-OSS models sat in the tree long after those models were pruned.

### A handler may declare tool policy; it must not decide for the model

`VLLMFunctionCallingLLM` used to pin `tool_choice="required"` until a tool result appeared, and `DatabricksResponsesLLM` held a second version of the same idea — `"required"` until a tool-call counter passed `max(2, min(10, tool_count // 4 + 1))`. **Both forcings are gone.** The Responses handler now sets nothing; the vLLM one sends `"auto"`, which is what a compliant server already applies when tools are present — the value of stating it is that the policy is explicit and in one place.

No mainstream framework does. CrewAI sets `"auto"` and stops; LangGraph never mentions `tool_choice`; LangChain passes through only what the caller asked for; LiteLLM drops even a caller's value once a tool result exists. Forcing was also keyed on the **endpoint**, the one axis none of them use — so a chat greeting and a long crew task hit the same handler and got the same answer, and "hello how are you" opened with a web search. Measured live, 3 samples per cell: forced called a tool 3/3 on a greeting; plain `auto` was 0/3 on a greeting and 3/3 on an explicit search request.

Ending a runaway tool loop is a separate concern, solved one layer down and model-agnostically: `transport/budget.py` spends a final call carrying `FORCE_FINAL_ANSWER` **with no tools attached**, so it cannot open another round.

That wrap-up makes two attempts, not one. A thinking model can spend its whole output allowance in the reasoning channel and return `finish_reason=length` with no content — observed on a 25-round research run whose compiled answer sat in 8,192 tokens of reasoning — so a blow-out gets one retry carrying `FORCE_FINAL_ANSWER_DIRECTLY` ("do not think it over again"). If neither attempt yields answer text, `ExecutionBudgetExceededError` is raised with the wrap-up's reasoning as its `partial`, labelled as a recovered draft (`budget.partial_from_reasoning`). What happens to that error is the task's `on_budget_exceeded` policy: Kasal's kernel defaults it to `degrade` (`kernel/task_builder.py`), so the run keeps the annotated partial instead of dying after every tool call succeeded; a task config may still say `raise`.

A caller that genuinely needs a tool call still passes `tool_choice` explicitly, and it is honoured end to end.

**Known limitation.** Self-hosted Qwen3-Coder under-uses its tools: given a large scaffolded prompt it declines `auto` and answers from nothing rather than calling the tool it was handed, while tool-calling correctly on short prompts. A per-model `force_tool_first_turn` flag was written for it and deliberately reverted — a single-model hack with no precedent in any framework, and one that would have re-armed the greeting problem for the very model it helped, since a forced opening turn cannot tell "gather swiss news" from "hello". The principled fix is to choose **which tools are attached** from the request rather than whether a call is compelled; until that exists, prefer a tool-following model for tool-heavy work.

### Facade: `LLMManager`

Five entry points, and they are the whole public surface:

| Method | Use |
|--------|-----|
| `completion()` | A standalone call returning text — intent detection, generation services, guardrails. |
| `completion_with_usage()` | A call needing Anthropic prompt caching and the `usage` block back. The only direct litellm caller left in kasal. |
| `configure_kasal_llm()` / `get_llm()` | A configured `LLM` for crew, flow and chat execution. |
| `get_embedding()` / `get_embeddings()` | Embedding vectors; thin delegates to `src/services/llm/embeddings.py`. |

## The path of one call

Building an LLM for an agent runs down the layers in order:

1. `LLMManager.configure_kasal_llm(model_key, group_id, temperature)` looks the key up in the model catalog through `ModelConfigService`, scoped to the caller's group.
2. The provider branch resolves credentials and a base URL — Databricks resolves OBO, then PAT, then service principal, and fails closed if none work.
3. Parameters the endpoint accepts are set, and parameters it rejects are **omitted**. A GPT-5-family model is built with no `temperature`; a Kimi model likewise.
4. The right class is chosen: `DatabricksRetryLLM` for Databricks chat models, `DatabricksResponsesLLM` for the Responses API, `VLLMFunctionCallingLLM` for self-hosted vLLM, plain `LLM` otherwise.
5. The engine sends it: trim the conversation if it approaches the window, clamp the output budget so `prompt + max_tokens` fits, run tool-call rounds, emit `LLMCallStartedEvent` / `LLMCallCompletedEvent`, accumulate usage. If the server still rejects the prompt as too long — the estimate is a chars-per-token guess, and JSON-escaped Cyrillic once measured 1.4 against the assumed 3.4 — the round is retried behind a compaction sized by the server's own count (`ContextCompactionEvent`, strategy `tool_result_stub_after_rejection`); only when nothing is left to stub does `LLMContextLengthExceededError` reach the executor.
6. `usage_telemetry` sees the completion event and forwards token counts.

## Anthropic prompt caching

Every tool round resends the system prompt, the tool schemas and the whole conversation so far. Claude caches a prompt prefix only where the request marks it with `cache_control: {"type": "ephemeral"}`, so the transport places those markers itself, in `src/backend/src/core/llm/transport/prompt_cache.py`. `cache_mode()` picks the dialect per endpoint:

| Endpoint | Mode | Where the markers go |
|---|---|---|
| Native Messages API (provider `anthropic`) | `anthropic` | On the native request built by `anthropic_messages.message_params`, including `tool_result` blocks. Thinking blocks are never marked, and replayed signed blocks are copied rather than changed |
| Databricks-hosted Claude over the chat-completions API (provider `databricks`, endpoint name containing `claude`) | `databricks` | On text and image content items; when the conversation ends in tool results, on the preceding assistant message's last `tool_calls` entry, because a `tool` message has no documented field |
| Everything else, and the Responses API | None | No markers. CrewAI's `cache_breakpoint` hints are stripped, because OpenAI-compatible servers return 400 on the unknown field |

A request carries at most 4 breakpoints, Anthropic's limit, and markers the caller already set count against it. In order, they go on:

1. The stable prefix: the last system block. Tools render before the system prompt, so one marker caches both. With no system prompt, the native path marks the last tool definition instead.
2. The rolling tail: the end of the conversation as sent. The next round's prompt starts with this one, so its marker becomes a read point.
3. CrewAI's hints: the `cache_breakpoint` key CrewAI sets on the initial task prompt becomes a marker on that user message.

An endpoint that cannot be identified as Claude gets no markers: a marker on a non-Claude endpoint is a 400, while a missing one only costs money. A prefix shorter than the model's minimum cacheable length (512 to 4,096 tokens) is not an error; the server ignores the marker.

Cache usage is counted on both paths. `response_parsing.py` reads cache reads from `prompt_tokens_details.cached_tokens` or, for Databricks-hosted Claude, Anthropic's top-level `cache_read_input_tokens`, and cache writes from `cache_creation_input_tokens`. `BaseLLM` accumulates them as `cached_prompt_tokens` and `cache_creation_tokens`, and `OTelEventBridge` records both on each `llm_response` span as `kasal.extra.cached_prompt_tokens` and `kasal.extra.cache_creation_tokens`. On the native path, `prompt_tokens` is the whole prompt, including both cache buckets.

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

litellm remains a dependency, but the engine does not use it. On the LLM request path it reaches exactly one function: `LLMManager.completion_with_usage`. Off that path, two other places touch it: the memory storage adapter (`services/memory/storage/adapter.py`) calls `litellm.embedding`, and `DatabricksResponsesLLM` reuses the configured `litellm.cache` object as a plain key/value store for Responses API calls.

Two consequences worth internalizing:

- Anything configured on the `litellm` module — `drop_params`, callbacks, caching, `register_model`, monkey patches — affects only that one function. It will not change crew, flow or chat behavior.
- **Every parameter set when building an LLM is sent.** There is no drop-params safety net. If an endpoint rejects a parameter, do not set it.

> [!IMPORTANT]
> A fix applied to a shared library the request no longer enters is indistinguishable from no fix at all. Apply fixes on the path the request actually takes.

## Rules for adding behavior

Where new behavior belongs follows from what it needs to know:

- **True for every OpenAI-compatible model?** The transport, in `src/core/llm/transport/` (`completion.py` or `base.py`). Budget enforcement, retries of protocol-level errors, response parsing.
- **Needs the catalog, a tenant, credentials or a URL?** `src/services/llm/`.
- **True of one serving endpoint?** A handler subclass in `src/services/llm/handlers/`, named for the endpoint rather than the model.
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
