# Models

Kasal runs agents and crews on large language models from several providers. This page covers the model catalog, how to choose a model, letting the decision model choose one (Auto), the behavior with Agent Bricks and Genie, and Kasal's automatic model fallback.

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
| vLLM | `vllm` | Self-hosted, OpenAI-compatible serving endpoint (endpoint URL set on the model in Configuration → Models). |
| OpenRouter | `openrouter` | Disabled by default. Requires `OPENROUTER_API_KEY` per workspace. Any OpenRouter model can be added in Configuration → Models: its `name` is the OpenRouter model id (for example `anthropic/claude-sonnet-4.5`), sent as is. See [OpenRouter and Jev Router](#openrouter-and-jev-router). |

## The model catalog

The seeded catalog (`DEFAULT_MODELS` in `model_configs.py`) groups models by provider. Each entry defines a `name` (the id sent to the provider), a default `temperature`, the `provider`, a `context_window`, and a `max_output_tokens`. Exact model ids change over time as the seed is updated, so treat the seed file as the source of truth. The categories at the time of writing are:

- Databricks: a broad set of workspace-served endpoints, including the `databricks-claude-*` family (Opus, Sonnet, Haiku), the `databricks-gpt-5-*` family (including mini, nano, and codex variants), `databricks-gemini-*` and `databricks-gemma-*`, `databricks-llama-*`, and `databricks-qwen3-*`. Context windows range up to 1,000,000 tokens on the largest Claude and Gemini endpoints.
- OpenAI: `gpt-4`, `gpt-4o`, `gpt-4o-mini`, `gpt-4-turbo`, `gpt-3.5-turbo`, the `gpt-5` family, and the `o`-series deep-research models.
- Gemini: `gemini-2.0-flash` and the `gemini-3-*` preview models.
- Anthropic: `claude-opus-4-*` and `claude-sonnet-4-*`.
- DeepSeek: `deepseek-chat`, `deepseek-reasoner`, and `deepseek-v3` / `deepseek-coder-v2` variants.
- Ollama and vLLM: self-hosted models such as `llama3.2`, `qwen2.5`, `gemma2`, `deepseek-r1`, and `Qwen3-Coder-30B-A3B-Instruct`.
- OpenRouter: `jev-router` (TypeSafe's Jev Router, `typesafe/jev-router`).

Models that have been retired or that proved incompatible are listed in `REMOVED_MODEL_KEYS` and are pruned from the database on every seed run, so they disappear from the model picker even on already-seeded installations.

## Choosing a model

General guidance:

- Start with a Databricks-served Claude Sonnet or a `databricks-gpt-5-*` model for agent and crew work. These are enabled by default and reuse your workspace authentication.
- Pick a larger `context_window` when your prompts, tool outputs, or task context are large.
- Self-hosted Ollama and vLLM models are useful for local development or air-gapped setups.

Tool-calling and structured-output caveats, all observed in the codebase and recorded in the seed file:

- Some Gemini-family endpoints fail multi-turn tool-calling crews (for example with a missing `thought_signature` error). For crews that call tools, prefer a `gpt-5*` or Claude Sonnet model.
- Reasoning models that emit a "thinking" preamble can break the JSON-only generation prompts ("Could not parse response as JSON"). Such models have been pruned from the default catalog.
- Reasoning is the model's own thinking budget: turning on Reasoning for a crew sets `reasoning_effort` (`low`/`medium`/`high`) on each agent's LLM. It applies only to models that accept that parameter (currently the `gpt-5*` family and the o3/o4 families); for any other model the setting is dropped silently and the run is unaffected. Support comes from Kasal's model-capabilities registry, and whether a model uses it is that model's reasoning setting in Configuration → Models; the former `KASAL_REASONING_EFFORT_*` variables are gone.
- Some endpoints only support the OpenAI Responses API and cannot be used through Kasal's chat-completions path; these are also pruned.
- The internal generation services (agent, task, and crew generation) use the model the request names (the UI sends the one you chose), else the installed default model inside Databricks Apps, else `databricks-gemini-3-8-flash` (`DEFAULT_ENGINE_MODEL` in `src/backend/src/utils/model_config.py`). The former `AGENT_MODEL`, `CREW_MODEL` and `TASK_MODEL` environment variables are no longer read.

### Changes show up without a reload

When an administrator enables, disables, adds, edits or deletes a model (Configuration → **Models**), turns the decision model on or off, or changes the Jev API URL, open chats and the Agent Builder and Flow Builder model menus update in place: there is no need to reload the page. Other open tabs of the same browser update too, and a model menu opened after a change made elsewhere (another administrator, another browser) refreshes its list, at most every 30 seconds. Switching workspace reloads the list for the new workspace.

If the model you had selected is no longer enabled, the selector falls back: in the chat to **Auto** when it is available, otherwise to the server's default model when it is enabled, otherwise to the first enabled model; in the builders to the default, otherwise the first enabled model. A model you picked yourself that is still enabled is never changed, and in the chat it comes back if it is enabled again. Agents saved with a model keep that model: the list updates, the saved agent does not.

## Auto (model selection)

In the chat, the **Model** row of the composer's **+** menu can be set to **Auto**. With Auto, the [decision model](./DECISION_MODEL.md) picks the model for each message from the models your workspace has enabled, based on what the message asks for and on each model's context window, output limit and reasoning support.

### When Auto appears

Auto appears at the top of the model list only when the decision model is available to your workspace:

1. A system administrator has chosen the connection (System administration → **Models** → **Decision model**): **Jev API** with its URL set, or **OpenRouter**.
2. Your workspace has the key that connection needs under **Configuration → API Keys**: `JEV_API_KEY` for the Jev API, `OPENROUTER_API_KEY` for OpenRouter.
3. A workspace administrator has turned on **Use a decision model** (Workspace settings → **Models** → **Decision model**).

Under either connection, Auto first asks Jev which of your **enabled** models to use, then calls that model through its own provider. Under the **OpenRouter** connection the question goes to Jev through OpenRouter; the answer still comes from the chosen enabled model (for example Anthropic, with your `ANTHROPIC_API_KEY`), not from OpenRouter. Only enabled models ever answer an Auto message: Jev can only pick from that list, Kasal checks its pick against the list again, and a router model (Jev Router, `openrouter/auto`) is never offered. See [Under the OpenRouter connection](./DECISION_MODEL.md#under-the-openrouter-connection).

When all three are true, Auto is also the default: a new chat, or a chat where you never picked a model, starts on Auto. When any of them is missing, there is no Auto option and the selector works as before.

### Choosing a model yourself

Auto is a default, not a lock. Open the **+** menu, select **Model**, and pick any model: that model is used for every message until you change it, and your choice is remembered in this browser. Pick **Auto** again to hand the choice back. A model you picked before Auto existed also stays picked, unless it was the server's default model, which the selector used to fill in for you.

### Seeing which model Auto picked

After you send a message, the run activity under it shows a step such as "Auto → databricks-claude-opus-5-5". The run's trace (Run history) starts with the decision, "Auto (Jev via OpenRouter) → claude-opus-5-5" (or "Auto (Jev) → …" under the Jev API), followed by "LLM Request — claude-opus-5-5", the tool calls and "LLM Response — claude-opus-5-5". The decision and the calls share one trace id, in the run's rows and in its OpenTelemetry/MLflow trace, and the run's details show the model it ran on.

### When the decision model cannot decide

If the decision model is unreachable, slow, unsure, or has too many models to choose from (more than 64), Auto uses your workspace's default model: the server's default model when it is enabled for your workspace, otherwise the first enabled model. The activity step then reads "Auto → `<model>` (default)". Your message still runs; Auto never blocks it.

A few chat actions that call a model directly, such as improving a prompt, drafting a skill, saving a crew from the conversation and editing one slide, do not use Auto. With Auto selected they run on the server's default model.

### Turning Auto on (administrators)

Auto needs no setting of its own: it follows the decision model. A system administrator chooses the connection (Jev API with its URL, or OpenRouter) once for the deployment. Then, per workspace, someone who manages API keys adds the connection's key (`JEV_API_KEY` or `OPENROUTER_API_KEY`), and a workspace administrator turns on **Use a decision model**. Turning the decision model off removes Auto from the selector for that workspace. For what is sent to the provider and how each decision falls back, see [Model selection (Auto)](./DECISION_MODEL.md#model-selection-auto) and [Configuration](./DECISION_MODEL.md#configuration) in the decision model guide.

## OpenRouter and Jev Router

The `openrouter` provider calls OpenRouter's OpenAI-compatible chat API:

- **Key.** `OPENROUTER_API_KEY` from the workspace's **Configuration → API Keys** (the same `<PROVIDER>_API_KEY` rule as the other hosted providers). A model build without it fails with "No OpenRouter API key found for workspace ...".
- **Endpoint.** The model's own endpoint override (Configuration → Models), else the OpenRouter URL from System administration → Models → Decision model, else `https://openrouter.ai/api/v1`.
- **Model id.** The model's `name` goes on the wire unchanged, vendor prefix included (`typesafe/jev-router`, `openai/...`). Kasal labels the LLM with provider `openrouter` so its own transport never strips an `openai/` or `anthropic/` prefix that belongs to OpenRouter.
- **Parameters.** The usual per-model rules apply (`core/llm/model_capabilities.py`). OpenRouter ignores a parameter the served model does not support.

**Jev Router** is seeded as `jev-router` (name `typesafe/jev-router`, provider `openrouter`, 1,000,000-token context, 32,768 output tokens), disabled like every non-Databricks model. It forwards each request to a model it picks from OpenRouter's whole catalogue, which can include models you never enabled (including anonymous "stealth" models that may log or train on prompts), and there is no way to restrict its choice. OpenRouter lists it with `supported_parameters: []`, so its capability entry refuses every sampling parameter (`temperature`, `top_p`, the penalties, `stop`) and offers no effort levels: Kasal sends none of them. Its output cap is conservative, since the model it picks has the real limit.

**Auto never uses Jev Router**, under either connection, even when it is enabled: router models are not offered to the decision, are never the fallback, and the LLM builder refuses Jev Router as Auto's answer. Auto asks Jev itself (`typesafe/jev-1.13` on OpenRouter) to choose among your enabled models instead; see [Under the OpenRouter connection](./DECISION_MODEL.md#under-the-openrouter-connection). Enable Jev Router in Configuration → Models only if you want to pick it by hand, knowing it answers with models outside your list.

**Privacy.** Under the OpenRouter connection, Auto's decision (the start and end of the message, at most 2,000 characters, and your enabled models' descriptions) goes through OpenRouter to TypeSafe. Turn off prompt logging and training for your OpenRouter key in the OpenRouter account's privacy settings.

Things to know:

- When you pick a router by hand, its pick is shown on each call's response row. OpenRouter names the model that served in the response's `model` field (and in each streamed chunk). The transport reads it, and the run activity's row reads "LLM Response — anthropic/claude-opus-5-5 (628 chars)" while the request row reads "LLM Request — jev-router".
- This works for any provider, not only OpenRouter. The served id is shown only when it names a different model from the one requested. Both ids are compared after lowercasing, dropping a `vendor/` prefix and a `:variant` suffix, reading `.` and `_` as `-`, and removing date stamps. If either id then contains the other, they count as the same model. So Anthropic answering `claude-sonnet-4-5-20250929` for `claude-sonnet-4-5`, or a gateway adding a prefix, shows nothing extra (`core/llm/transport/served_model.py`).
- `supported_parameters: []` also leaves open whether tool calls reach the model it picks. Kasal has not verified agent tool use through Jev Router; try a tool-using crew before relying on it.

## Agent Bricks and Genie

Kasal integrates with two Databricks features through custom tools:

- Genie: the `GenieTool` answers natural-language questions over your data by calling a Genie space. It runs against your Databricks workspace using workspace authentication, independent of the agent's chat model.
- Agent Bricks: the `agentbricks_tool` calls Mosaic AI Agent Bricks endpoints, which reply in the OpenAI Responses API shape. The tool extracts the final assistant message from the Responses output.

Because both are Databricks-served, pair them with a Databricks model that handles tool calling reliably (a Claude Sonnet or `databricks-gpt-5-*` model) rather than a Gemini-family or reasoning model that struggles with multi-turn tool calls.

## Automatic fallback

Kasal wraps Databricks chat models in `DatabricksRetryLLM`, which can switch to a different model when a call fails in a way that a model swap can plausibly fix. The policy lives in `src/backend/src/services/llm/handlers/model_fallback.py`.

A failure is classified into one of three reasons, and only these trigger a fallback:

- `context_window`: the prompt exceeded the model's context window. Fallback chooses an enabled model with a larger context window.
- `fatal_4xx`: a model-incompatibility 4xx (for example a Gemini `thought_signature` error or an unsupported parameter). Fallback prefers a model from a different family, since these incompatibilities tend to be family-wide.
- `rate_limit`: a sustained 429 after same-model backoff. Fallback picks any untried enabled model, roomiest context window first.

Failures that a model swap will not fix (authentication errors, user stops, transient errors, malformed input) are not retried with a different model.

Fallback candidates are the currently enabled models, loaded from the database via `LLMManager.load_fallback_candidates`. The candidate set is restricted to Databricks-served, non-codex models, since those can be rebuilt and swapped through the same authentication and endpoint. Kasal tracks which models have already been tried so it does not retry the same model twice within a run.

## Configuration

- Per-model enablement: each model in the catalog has an `enabled` flag. Seeding enables only Databricks-provider models; you enable other providers from the model configuration UI. Model rows are seeded from `src/backend/src/seeds/model_configs.py`.
- Per-teamspace overrides: model availability is resolved per group (teamspace), so a teamspace can enable or disable individual models for its members.
- Who can change what: the model catalog is global, shared by every teamspace, so creating, editing, deleting or globally toggling a model, and enabling or disabling all models, is limited to system admins (403 otherwise). Workspace admins toggle a model for their own teamspace only, which writes a per-teamspace override. For the routes, see the [API endpoints reference](./api_endpoints.md).
- Per-agent overrides: the Agent form can override the catalog's `temperature`, thinking settings and `max_output_tokens` ("Max Output Tokens Override") for one agent. Blank inherits the catalog value; a set value is applied to that agent's LLM by `services/execution/kernel/agent_builder.py` on whichever field the model takes (`max_tokens`, or `max_completion_tokens` for the GPT-5 family). Reasoning tokens count against it, which is the usual reason to raise it for one research agent rather than for the whole workspace.
- AI Gateway toggle: the `ai_gateway_enabled` flag on `DatabricksConfig` controls how Databricks LLM and embedding traffic is routed. When it is off (the default), Kasal calls the standard `/serving-endpoints` path with the model in the URL. When it is on, traffic is routed through the OpenAI-compatible `/ai-gateway/mlflow/v1` path with the model in the request body. Routing logic lives in `src/backend/src/utils/databricks_url_utils.py`.
- Adding a model: add a new entry to `DEFAULT_MODELS` in `src/backend/src/seeds/model_configs.py` with its `name`, `temperature`, `provider`, `context_window`, and `max_output_tokens`, then re-run the seeders.
