# Models

Kasal runs agents and crews on large language models from several providers. This page covers the model catalog, how to choose a model, the behavior with Agent Bricks and Genie, and Kasal's automatic model fallback.

## Supported providers

Kasal ships a seeded model catalog defined in `src/backend/src/seeds/model_configs.py`. Each model is tagged with a `provider`. Only models with the `databricks` provider are enabled by default at seed time; other providers are present in the catalog but disabled until you enable them and supply credentials.

| Provider | `provider` value | Notes |
|----------|------------------|-------|
| Databricks Foundation Model APIs / Model Serving | `databricks` | Enabled by default at seed time. Served through your workspace, so they reuse Databricks auth (OBO or PAT). |
| OpenAI | `openai` | Disabled by default. Requires an OpenAI API key. |
| Google Gemini | `gemini` | Disabled by default. Requires a Gemini API key. |
| Anthropic | `anthropic` | Disabled by default. Requires an Anthropic API key. |
| DeepSeek | `deepseek` | Disabled by default. Requires a DeepSeek API key. |
| Ollama | `ollama` | Self-hosted, local models. |
| vLLM | `vllm` | Self-hosted, OpenAI-compatible serving endpoint (base URL from `VLLM_BASE_URL`). |

## The model catalog

The seeded catalog (`DEFAULT_MODELS` in `model_configs.py`) groups models by provider. Each entry defines a `name` (the id sent to the provider), a default `temperature`, the `provider`, a `context_window`, and a `max_output_tokens`. Exact model ids change over time as the seed is updated, so treat the seed file as the source of truth. The categories at the time of writing are:

- Databricks: a broad set of workspace-served endpoints, including the `databricks-claude-*` family (Opus, Sonnet, Haiku), the `databricks-gpt-5-*` family (including mini, nano, and codex variants), `databricks-gemini-*` and `databricks-gemma-*`, `databricks-llama-*`, and `databricks-qwen3-*`. Context windows range up to 1,000,000 tokens on the largest Claude and Gemini endpoints.
- OpenAI: `gpt-4`, `gpt-4o`, `gpt-4o-mini`, `gpt-4-turbo`, `gpt-3.5-turbo`, the `gpt-5` family, and the `o`-series deep-research models.
- Gemini: `gemini-2.0-flash` and the `gemini-3-*` preview models.
- Anthropic: `claude-opus-4-*` and `claude-sonnet-4-*`.
- DeepSeek: `deepseek-chat`, `deepseek-reasoner`, and `deepseek-v3` / `deepseek-coder-v2` variants.
- Ollama and vLLM: self-hosted models such as `llama3.2`, `qwen2.5`, `gemma2`, `deepseek-r1`, and `Qwen3-Coder-30B-A3B-Instruct`.

Models that have been retired or that proved incompatible are listed in `REMOVED_MODEL_KEYS` and are pruned from the database on every seed run, so they disappear from the model picker even on already-seeded installations.

## Choosing a model

General guidance:

- Start with a Databricks-served Claude Sonnet or a `databricks-gpt-5-*` model for agent and crew work. These are enabled by default and reuse your workspace authentication.
- Pick a larger `context_window` when your prompts, tool outputs, or task context are large.
- Self-hosted Ollama and vLLM models are useful for local development or air-gapped setups.

Tool-calling and structured-output caveats, all observed in the codebase and recorded in the seed file:

- Some Gemini-family endpoints fail multi-turn tool-calling crews (for example with a missing `thought_signature` error). For crews that call tools, prefer a `gpt-5*` or Claude Sonnet model.
- Reasoning models that emit a "thinking" preamble can break crew planning, which expects a JSON-only response ("Could not parse response as JSON"). Such models have been pruned from the default catalog.
- Some endpoints only support the OpenAI Responses API and cannot be used through Kasal's chat-completions path; these are also pruned.
- The internal generation services (agent, task, and crew generation) default to `databricks-gpt-5-3-codex`, overridable through environment variables such as `AGENT_MODEL`, `CREW_MODEL`, and `DEFAULT_TASK_MODEL`.

## Agent Bricks and Genie

Kasal integrates with two Databricks features through custom tools:

- Genie: the `GenieTool` answers natural-language questions over your data by calling a Genie space. It runs against your Databricks workspace using workspace authentication, independent of the agent's chat model.
- Agent Bricks: the `agentbricks_tool` calls Mosaic AI Agent Bricks endpoints, which reply in the OpenAI Responses API shape. The tool extracts the final assistant message from the Responses output.

Because both are Databricks-served, pair them with a Databricks model that handles tool calling reliably (a Claude Sonnet or `databricks-gpt-5-*` model) rather than a Gemini-family or reasoning model that struggles with multi-turn tool calls.

## Automatic fallback

Kasal wraps Databricks chat models in `DatabricksRetryLLM`, which can switch to a different model when a call fails in a way that a model swap can plausibly fix. The policy lives in `src/backend/src/core/llm_handlers/model_fallback.py`.

A failure is classified into one of three reasons, and only these trigger a fallback:

- `context_window`: the prompt exceeded the model's context window. Fallback chooses an enabled model with a larger context window.
- `fatal_4xx`: a model-incompatibility 4xx (for example a Gemini `thought_signature` error or an unsupported parameter). Fallback prefers a model from a different family, since these incompatibilities tend to be family-wide.
- `rate_limit`: a sustained 429 after same-model backoff. Fallback picks any untried enabled model, roomiest context window first.

Failures that a model swap will not fix (authentication errors, user stops, transient errors, malformed input) are not retried with a different model.

Fallback candidates are the currently enabled models, loaded from the database via `LLMManager.load_fallback_candidates`. The candidate set is restricted to Databricks-served, non-codex models, since those can be rebuilt and swapped through the same authentication and endpoint. Kasal tracks which models have already been tried so it does not retry the same model twice within a run.

## Configuration

- Per-model enablement: each model in the catalog has an `enabled` flag. Seeding enables only Databricks-provider models; you enable other providers from the model configuration UI. Model rows are seeded from `src/backend/src/seeds/model_configs.py`.
- Per-workspace overrides: model availability is resolved per group (workspace), so a workspace can enable or disable individual models for its members.
- AI Gateway toggle: the `ai_gateway_enabled` flag on `DatabricksConfig` controls how Databricks LLM and embedding traffic is routed. When it is off (the default), Kasal calls the standard `/serving-endpoints` path with the model in the URL. When it is on, traffic is routed through the OpenAI-compatible `/ai-gateway/mlflow/v1` path with the model in the request body. Routing logic lives in `src/backend/src/utils/databricks_url_utils.py`.
- Adding a model: add a new entry to `DEFAULT_MODELS` in `src/backend/src/seeds/model_configs.py` with its `name`, `temperature`, `provider`, `context_window`, and `max_output_tokens`, then re-run the seeders.
