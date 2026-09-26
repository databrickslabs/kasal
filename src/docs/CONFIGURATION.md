# Configuration reference

The environment variables that Kasal's backend, launch scripts and frontend read, grouped by concern, with each one's default, effect and the file that reads it. For operators deploying Kasal and developers running it locally.

- [How configuration is loaded](#how-configuration-is-loaded)
- [What is not configured here](#what-is-not-configured-here)
- [Server and run.sh](#server-and-runsh)
- [Local development identity](#local-development-identity)
- [Database](#database)
- [Databricks and authentication](#databricks-and-authentication)
- [Security and API limits](#security-and-api-limits)
- [Execution and LLM tuning](#execution-and-llm-tuning)
- [Chat, A2UI and generation](#chat-a2ui-and-generation)
- [Decisions (Jev)](#decisions-jev)
- [Memory, knowledge and recipes](#memory-knowledge-and-recipes)
- [Event triggers](#event-triggers)
- [Logging](#logging)
- [Observability, MLflow and OpenTelemetry](#observability-mlflow-and-opentelemetry)
- [Frontend](#frontend)
- [Testing and CI](#testing-and-ci)
- [Internal variables](#internal-variables)
- [Recommended: a backend env example file](#recommended-a-backend-env-example-file)

Paths below are relative to the repository root. Boolean flags accept `true`/`false`; several also accept `1`/`yes`/`on`, as noted.

## How configuration is loaded

Kasal reads configuration from three places:

- **Process environment.** Most variables are read with `os.getenv` at the point of use, often at import time. Set them in your shell, in `run.sh`'s environment, or in `src/app.yaml` for a Databricks App.
- **`src/backend/src/config/settings.py`.** The pydantic `Settings` class declares `env_file=".env"`, resolved against the **current working directory**, with case-sensitive names. A `.env` file only affects the fields declared on `Settings` (`DATABASE_TYPE`, `SQLITE_DB_PATH`, `POSTGRES_*`, `LOCAL_DEV_USER_EMAIL`, `LITELLM_CACHE_*`, `LOG_LEVEL` and the other fields in that class). Nothing calls `load_dotenv`, so a `.env` file does **not** set any variable read with `os.getenv`. In particular, `LOCAL_DEV_AUTH` in `.env` has no effect: export it instead.
- **Vite `.env` files in `src/frontend/`.** Only `VITE_*` variables reach the browser bundle.

> [!IMPORTANT]
> `src/backend/src/main.py` overwrites some variables at import time: `USE_NULLPOOL=true`, `CREWAI_DISABLE_TELEMETRY=true`, `SEED_DEBUG=True`, `LOG_DIR=<backend>/logs`, and `MLFLOW_TRACKING_URI=databricks` (after saving your value as `KASAL_LAUNCH_MLFLOW_TRACKING_URI`). Setting these yourself has no effect on the server process.

## What is not configured here

Models, embedders and judges are configured in the UI, not with environment variables. Model definitions are seeded from `src/backend/src/seeds/model_configs.py` into the database, enabled and selected per workspace in the UI's model configuration, and resolved at run time through `LLMManager` (`src/backend/src/services/llm/manager.py`). Embedder and memory backends, Databricks workspace settings, MLflow and judge models, and provider API keys (stored encrypted, then loaded into the execution environment) are set the same way. For more information, see the [models reference](./MODELS.md).

A few model-name variables still exist as fallbacks when nothing is configured; they are listed under [Chat, A2UI and generation](#chat-a2ui-and-generation). Do not add new ones.

The exported Databricks App template (`src/backend/src/services/export/templates/databricks_app/`) has its own variables (`CREW_MODE`, `CHAT_MAX_ITER`, `LOCAL_LLM_*`, `PG*` and others) and its own `.env.example`. They configure the exported app, not Kasal, and are out of scope here.

## Server and run.sh

`src/backend/run.sh` is the supported way to start the backend locally. It syncs dependencies with `uv sync --frozen`, then runs `uvicorn src.main:app --reload --reload-dir src` from `src/backend/.venv`. Run `./run.sh --help` for the full list, including the per-domain logging variables.

The server variables are:

| Variable | Default | What it does | Read in |
|---|---|---|---|
| `KASAL_BIND_HOST` | `127.0.0.1` | Interface uvicorn binds to. Any value other than `127.0.0.1`, `localhost` or `::1` prints a warning, and a louder one when `LOCAL_DEV_AUTH` is on, because anyone who can reach the port then acts as the development user | `src/backend/run.sh` |
| `KASAL_PORT` | `8000` | Port uvicorn listens on. The Vite dev server reads it too, for its `/api` proxy and for the API client | `src/backend/run.sh`, `src/frontend/vite.config.ts` |
| `KASAL_KILL_PORT_OWNER` | `false` | If the port is held by something that is not a Kasal server from this checkout, `run.sh` refuses to start. Set to `true` to send it SIGTERM instead | `src/backend/run.sh` |
| `FRONTEND_STATIC_DIR` | `src/frontend_static` | Directory of built frontend assets the production entrypoint serves | `src/entrypoint.py` |

`KASAL_PORT` moves the backend and, in development, the frontend's target. `src/frontend/vite.config.ts` reads `VITE_KASAL_PORT`, else `KASAL_PORT`, else `8000`, points the `/api` proxy at that port and exposes it to the browser, where the API client builds `http://localhost:<port>/api/v1` (`src/frontend/src/shared/api/backendOrigin.ts`). Start both with the same value, for example `KASAL_PORT=8001 ./run.sh` and `KASAL_PORT=8001 npm start`.

`run.sh` changes into `src/backend` before it starts, whatever directory you call it from. It also exports these on your behalf: `DATABASE_TYPE` (see [Database](#database)), `LOCAL_DEV_AUTH=true` unless already set, `KASAL_LOG_LEVEL=INFO` and `KASAL_LOG_THIRD_PARTY=WARNING` unless already set, `USE_NULLPOOL=true` and `CREWAI_DISABLE_TELEMETRY=true`. Its `-q`, `-v`, `-d`, `--no-console` and `--no-file` flags set the logging variables described under [Logging](#logging).

The production entrypoint `src/entrypoint.py` (used by `src/app.yaml`) takes `--db-type`, `--db-url`, `--port` (default `8000`), `--reload`, `--debug` and `--environment dev|prod` flags rather than variables. `--environment dev` makes it a local run like `run.sh`: it sets `KASAL_DEPLOYMENT_MODE=local` and `LOCAL_DEV_AUTH=true` (unless you set `LOCAL_DEV_AUTH` yourself); `prod`, or no flag, changes nothing. `--reload` restarts on changes under `src/backend/src`. It binds `KASAL_BIND_HOST` if set, otherwise `0.0.0.0` inside Databricks Apps (`DATABRICKS_APP_NAME` set) and `127.0.0.1` everywhere else.

## Local development identity

Every protected API route depends on `get_group_context` (`src/backend/src/dependencies/providers.py`), which fails closed: a request with no identity gets **401**, an identity that resolves to no workspace gets 401, a workspace the user may not use gets 403, and a resolver failure gets 503. A 403 carries fixed detail text, never the resolver's reason or the requested group ID: `Access denied: no access to group` when the selected workspace is not one of the caller's (the frontend matches that phrase to drop a stale saved workspace), and `Access denied: workspace could not be resolved` for any other refusal. Outside Databricks Apps there is no proxy to supply `X-Forwarded-Email`, so a local run needs a development identity:

| Variable | Default | What it does | Read in |
|---|---|---|---|
| `LOCAL_DEV_AUTH` | Unset (`run.sh` sets `true`) | Opt-in. When `1`/`true`/`yes`/`on`, `LocalDevAuthMiddleware` adds `X-Forwarded-Email` to any request that has no identity header. Ignored, with an error log, when `DATABRICKS_APP_NAME` is set or `ENVIRONMENT` is `production`/`prod` | `src/backend/src/main.py` |
| `LOCAL_DEV_USER_EMAIL` | Empty, meaning `dev@localhost` | Email the development identity uses | `src/backend/src/config/settings.py` |
| `ENVIRONMENT` | Unset; treated as `development` where a default is needed | `production`/`prod` disables `LOCAL_DEV_AUTH` and the loopback SSE identity fallback. `development`/`dev`/`local` (the default) allows synthetic emails without a TLD and applies `ADMIN_EMAILS` | `src/backend/src/main.py`, `src/backend/src/dependencies/providers.py`, `src/backend/src/schemas/group.py`, `src/backend/src/services/groups/forwarded_identity.py` |
| `ADMIN_EMAILS` | Empty | Comma-separated emails that get the admin role when first seen, in development only. Emails matching `admin@localhost`, `admin@` or `testadmin@` also do | `src/backend/src/services/groups/forwarded_identity.py` |

If you start uvicorn yourself instead of through `run.sh`, export `LOCAL_DEV_AUTH=true` or send an `X-Forwarded-Email` header, or every API call returns 401. In development the frontend sends `X-Forwarded-Email` itself (see `VITE_DEV_USER_EMAIL`), and native `EventSource` streams carry the identity as `_sse_email` / `_sse_group_id` query parameters, which the backend honors only on `/sse/` routes from a loopback client outside production. For more information, see the [security guide](./SECURITY.md).

## Database

Kasal uses SQLite or PostgreSQL through async SQLAlchemy, or Lakebase inside Databricks Apps. The default depends on the entry point:

- **`Settings` (`src/backend/src/config/settings.py`)**, which `run.sh`, `uvicorn`, `alembic` (`src/backend/migrations/env.py`) and `python run_seeders.py` all go through: `DATABASE_TYPE` defaults to `sqlite`, and `SQLITE_DB_PATH` defaults to the absolute path `src/backend/app.db`, so the working directory does not matter and every command opens the same file.
- **`run.sh`**: defaults to SQLite (`./run.sh postgres` switches) and exports only `DATABASE_TYPE`. It does not set `SQLITE_DB_PATH`, so it uses the `Settings` default or the value you exported.
- **`src/entrypoint.py`**: `--db-type` defaults to `sqlite` with `SQLITE_DB_PATH` defaulting to `src/kasal.db`, and it exports `DATABASE_TYPE`, `DATABASE_URI`/`DATABASE_URL` and `SYNC_DATABASE_URI` directly, so the crew and flow subprocesses agree with the server. With `--db-type postgres`, a `postgresql://` (or `postgres://`) `--db-url` is rewritten to `postgresql+asyncpg://`; with no `--db-url`, it falls back to a local `postgres` database on `localhost:5432`.
- **The `kasal` command from the pip package (`packaging/kasal/cli.py`)**: defaults `DATABASE_TYPE` to `sqlite` and `SQLITE_DB_PATH` to `~/.kasal/kasal.db` (or `<--data-dir>/kasal.db`), without overriding values already in the environment. It does not set `LOCAL_DEV_AUTH`. For more information, see [installing Kasal with pip](./PIP_PACKAGE.md).
- **Databricks Apps with a Lakebase resource attached**: when `PGHOST`, `PGDATABASE` and `PGUSER` are injected by the platform, `init_db` uses the Lakebase resource and the variables above do not apply.

To use PostgreSQL or another SQLite file locally, set `DATABASE_TYPE=postgres` (with the `POSTGRES_*` variables) or `SQLITE_DB_PATH` on every command you run, not only on the server.

The database variables are:

| Variable | Default | What it does | Read in |
|---|---|---|---|
| `DATABASE_TYPE` | `sqlite` | `sqlite` or `postgres`; selects how `DATABASE_URI` is assembled | `src/backend/src/config/settings.py` |
| `SQLITE_DB_PATH` | `src/backend/app.db`, absolute (entrypoint: `src/kasal.db`; pip `kasal`: `~/.kasal/kasal.db`) | SQLite database file | `src/backend/src/config/settings.py`, `src/entrypoint.py` |
| `DATABASE_URI` | Assembled from the fields above | Full async SQLAlchemy URI; when set, it wins over `DATABASE_TYPE` | `src/backend/src/config/settings.py` |
| `POSTGRES_SERVER` | `localhost` | PostgreSQL host | `src/backend/src/config/settings.py` |
| `POSTGRES_PORT` | `5432` | PostgreSQL port | `src/backend/src/config/settings.py` |
| `POSTGRES_USER` | `postgres` | PostgreSQL user | `src/backend/src/config/settings.py` |
| `POSTGRES_PASSWORD` | `postgres` | PostgreSQL password | `src/backend/src/config/settings.py` |
| `POSTGRES_DB` | `kasal` | PostgreSQL database name | `src/backend/src/config/settings.py` |
| `KASAL_REQUIRE_LAKEBASE_RESOURCE` | Unset (`src/app.yaml`: `true`) | When `true` inside Databricks Apps, startup fails unless the `lakebase` resource is attached | `src/backend/src/db/session.py` |
| `KASAL_LAKEBASE_RESOURCE` | Empty | Lakebase endpoint name, injected from the app resource by `src/app.yaml` | `src/backend/src/core/databricks_app.py` |
| `PGHOST`, `PGDATABASE`, `PGUSER`, `PGPORT` | Unset; `PGPORT` `5432` | Injected by Databricks Apps for an attached Lakebase resource. All of host, database and user are required once any is set | `src/backend/src/core/databricks_app.py` |
| `LAKEBASE_INSTANCE_NAME` | `kasal-lakebase` | Lakebase instance used when Lakebase is configured through the UI rather than as an app resource | `src/backend/src/db/lakebase_session.py`, `src/backend/src/db/database_router.py` |
| `LAKEBASE_KNOWLEDGE_ROLE` | `databricks_superuser` | Role used for the knowledge-embedding session on Lakebase | `src/backend/src/services/knowledge/embedding_session.py` |

For more information, see the [Lakebase deployment guide](./lakebase-deployment.md).

## Databricks and authentication

Inside Databricks Apps the platform injects the app variables below. Kasal treats itself as hosted only when `DATABRICKS_APP_NAME`, `DATABRICKS_APP_PORT`, `DATABRICKS_WORKSPACE_ID` and `DATABRICKS_HOST` are all set and `KASAL_DEPLOYMENT_MODE` is not `local` (`src/backend/src/core/databricks_app.py`). Locally, configure the workspace in the UI instead.

| Variable | Default | What it does | Read in |
|---|---|---|---|
| `DATABRICKS_HOST` | Unset | Workspace URL, for example `https://<workspace>.cloud.databricks.com`. Injected by Databricks Apps; used as a fallback when no workspace is configured in the UI | `src/backend/src/core/databricks_app.py`, `src/backend/src/utils/databricks_auth.py`, many services |
| `DATABRICKS_APP_NAME`, `DATABRICKS_APP_PORT`, `DATABRICKS_WORKSPACE_ID` | Unset | Injected by Databricks Apps; together with `DATABRICKS_HOST` they mark the process as hosted. `DATABRICKS_APP_NAME` alone also marks it as production for `LOCAL_DEV_AUTH` | `src/backend/src/core/databricks_app.py`, `src/backend/src/main.py` |
| `KASAL_DEPLOYMENT_MODE` | Unset | `local` forces "not hosted" even when the app variables are present | `src/backend/src/core/databricks_app.py` |
| `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` | Unset | The app service principal's OAuth credentials, injected by Databricks Apps. Used for service-principal calls when no user token applies | `src/backend/src/utils/databricks_auth.py`, `src/backend/src/utils/databricks_app_auth.py` |
| `DATABRICKS_TOKEN`, `DATABRICKS_API_KEY` | Unset | Personal access token fallbacks for local runs and some legacy paths. The main auth path does not read them from the environment; configure a token in the UI instead | `src/backend/src/utils/databricks_auth.py` |
| `KASAL_SQL_WAREHOUSE_ID` | Empty | Default SQL warehouse, injected from the `sql-warehouse` app resource by `src/app.yaml` | `src/backend/src/core/databricks_app.py` |
| `KASAL_OUTPUT_VOLUME` | Empty | Unity Catalog volume (`/Volumes/...`) for run outputs, injected from the `volume` app resource | `src/backend/src/core/databricks_app.py` |
| `KASAL_DEFAULT_MODEL` | Empty | Default model endpoint, injected from the `serving-endpoint` app resource. Wins over `DEFAULT_LLM_MODEL` | `src/backend/src/core/databricks_app.py`, `src/backend/src/utils/model_config.py` |
| `DATABRICKS_ENABLE_AI_GATEWAY` | `false` | Routes LLM and embedding traffic through the AI Gateway instead of `/serving-endpoints`. Set by Kasal from the UI's Databricks configuration; do not set it by hand | `src/backend/src/utils/databricks_url_utils.py` |

The workspace URL you save in the UI's Databricks configuration (`POST /databricks/config`) is validated on save: it must use `https`, and its host must equal the credentialed host, which is the installation host inside Databricks Apps and the auth context's workspace elsewhere. Kasal only ever sends a Databricks credential to that host, including from a tool's `databricks_host` argument. For more information, see [Databricks credential hosts](./SECURITY.md#databricks-credential-hosts).

For more information, see the [Databricks App installation guide](./databricks-app-installation.md).

## Security and API limits

These control encryption, rate limiting and a few request-path caches:

| Variable | Default | What it does | Read in |
|---|---|---|---|
| `ENCRYPTION_KEY` | Unset | Fernet key for stored secrets. Resolution order: this variable, then the `kasal/kasal_encryption_key` secret provisioned by `src/deploy.py`, then a generated key that does not survive a restart (a warning is logged) | `src/backend/src/utils/encryption_utils.py` |
| `RATE_LIMIT_ENABLED` | `true` | Per-identity rate limit on `/api/`. `false`/`0`/`no`/`off` disables it | `src/backend/src/core/rate_limit.py` |
| `RATE_LIMIT_DEFAULT` | `600/minute` | Limit string in `limits` syntax | `src/backend/src/core/rate_limit.py` |
| `RATE_LIMIT_STORAGE_URI` | In-memory | Shared storage, for example `redis://<host>:6379`, for a multi-replica deployment | `src/backend/src/core/rate_limit.py` |
| `GROUP_MEMBERSHIP_CACHE_TTL` | `30` | Seconds a user's workspace memberships are cached per process | `src/backend/src/utils/user_context.py` |
| `SSE_HEARTBEAT_SECONDS` | `15` | Keep-alive interval for server-sent event streams, clamped to 5–120 | `src/backend/src/core/sse_manager.py` |
| `KASAL_EVENT_TRIGGERS_ALLOW_PRIVATE_WEBHOOKS` | Empty | `1`/`true`/`yes` lets trigger webhooks target private or loopback addresses. Leave unset in production | `src/backend/src/services/triggers/queue_consumer_service.py` |

## Execution and LLM tuning

These tune the agent runtime and the LLM layer. Defaults are chosen for a single app instance.

### Concurrent run limit

Crew and flow runs share one concurrent-run limit per server process. It is not an environment variable: it is the engine-config row `engine_name="kasal"`, `config_key="max_concurrent_runs"`, which a system admin creates or changes through the `/engine-config` API. With no row, or a disabled or non-integer value, the limit is 16. A value above 16 is capped at 16, the size of the run-wait and event-relay thread pools (`src/backend/src/services/execution/blocking_pools.py`), because every live run holds a thread in each; a value below 1 counts as 1. The value is re-read at most every 30 seconds; a change made through `/engine-config` applies at once in the server process that handled it (a raised limit admits queued runs immediately), and other processes pick it up within 30 seconds.

A run started over the limit is not rejected. It is queued with status `PENDING` and a `Queued: …` message naming the limit and its position, admitted first in, first out as slots free, and set to `RUNNING` when it starts. Stopping a queued run removes it from the queue. A run's timeout starts when its process starts, so time spent queued does not count against it. The gate lives in `src/backend/src/services/execution/run_admission.py`. For the API calls, see [run concurrency limit](./api_endpoints.md#run-concurrency-limit).

### Execution and LLM variables

The execution and LLM variables are:

| Variable | Default | What it does | Read in |
|---|---|---|---|
| `KASAL_LLM_MAX_CONCURRENCY` | `64` | Size of the dedicated thread pool for blocking LLM calls | `src/backend/src/services/llm/manager.py` |
| `KASAL_AGENT_MAX_EXECUTION_TIME` | `900` | Default wall-clock limit in seconds for one agent's work; `0` disables it. An explicit `max_execution_time` on the agent wins | `src/backend/src/services/execution/kernel/agent_builder.py` |
| `KASAL_FALLBACK_MODEL` | Unset | Model key to substitute for a Databricks model when no Databricks workspace is available. Ignored unless that model is enabled | `src/backend/src/services/settings/models.py` |
| `KASAL_THINKING_BUDGET_TOKENS` | `10240` | Default extended-thinking budget when the model config sets none | `src/backend/src/services/llm/manager.py` |
| `KASAL_RESPONSES_MAX_OUTPUT_TOKENS` | `16000` (falls back to `KASAL_CODEX_MAX_OUTPUT_TOKENS`) | Output-token cap for the Databricks Responses API adapter | `src/backend/src/services/llm/handlers/databricks_responses_llm.py` |
| `KASAL_REASONING_EFFORT_DISABLED` | Empty | `1`/`true`/`yes` never sends a reasoning-effort parameter | `src/backend/src/utils/model_config.py` |
| `KASAL_REASONING_EFFORT_MODELS` | Empty | Comma-separated extra model-name substrings that accept reasoning effort | `src/backend/src/utils/model_config.py` |
| `KASAL_HARNESS` | Unset (`crewai`) | Harness a spawned run uses when neither the request nor the config chooses one. Normally set by Kasal for the child process | `src/backend/src/services/execution/harnesses/selection.py` |
| `CREW_TOKEN_STREAMING` | `true` | Streams LLM tokens from crew subprocesses to the UI. `false`/`0`/`no` turns it off | `src/backend/src/services/execution/kernel/agent_builder.py` |
| `KASAL_ENGINE_STORAGE_DIR` | `~/.local/share/kasal_engine` | Engine-local storage such as flow checkpoints. `CREWAI_STORAGE_DIR` wins when set | `src/backend/src/utils/storage_paths.py` |
| `LITELLM_CACHE_ENABLED` | `true` | Response cache for the legacy LiteLLM completion path | `src/backend/src/config/settings.py` |
| `LITELLM_CACHE_TYPE` | `local` | `local` (in-memory) or `redis` | `src/backend/src/config/settings.py` |
| `LITELLM_CACHE_TTL` | `3600` | Cache TTL in seconds | `src/backend/src/config/settings.py` |
| `LITELLM_CACHE_REDIS_HOST`, `LITELLM_CACHE_REDIS_PORT`, `LITELLM_CACHE_REDIS_PASSWORD` | Unset | Redis connection when `LITELLM_CACHE_TYPE=redis` | `src/backend/src/config/settings.py` |
| `VLLM_BASE_URL` | `http://localhost:8081/v1` | Base URL for models with the `vllm` provider | `src/backend/src/services/llm/manager.py` |
| `VLLM_API_KEY` | `vllm` | API key sent to vLLM | `src/backend/src/services/llm/manager.py` |
| `VLLM_SUPPORTS_TOOLS` | `true` | Uses the function-calling adapter for vLLM | `src/backend/src/services/llm/manager.py` |
| `VLLM_TOOL_CHOICE` | `auto` | `tool_choice` sent to vLLM | `src/backend/src/services/llm/handlers/vllm.py` |
| `KAT_BASE_URL` | `http://127.0.0.1:8082/v1` | Base URL for models with the `custom` provider (a self-hosted OpenAI-compatible server) | `src/backend/src/services/llm/manager.py` |
| `KAT_API_KEY` | `local-no-auth` | API key for the `custom` provider | `src/backend/src/services/llm/manager.py` |
| `OLLAMA_API_BASE` | `http://localhost:11434` | Ollama server for the Ollama embedder | `src/backend/src/services/execution/config/embedder_config_builder.py` |
| `OLLAMA_EMBED_MODEL` | `nomic-embed-text` | Ollama embedding model | `src/backend/src/services/execution/config/embedder_config_builder.py` |
| `DEEPSEEK_ENDPOINT`, `KIMI_ENDPOINT`, `ANTHROPIC_API_BASE`, `GEMINI_API_BASE` | Provider public endpoint, or unset | Override a hosted provider's base URL | `src/backend/src/services/llm/manager.py` |

Provider API keys such as `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `SERPER_API_KEY` and `PERPLEXITY_API_KEY` are configured in the UI and stored encrypted; Kasal loads them into the execution environment itself. An exported shell value only acts as a fallback.

## Chat, A2UI and generation

These tune the chat surface, generative UI and the crew/task generators:

| Variable | Default | What it does | Read in |
|---|---|---|---|
| `A2UI_ENABLED` | `true` | Composes generative UI for chat answers | `src/backend/src/services/a2ui/runner.py` |
| `A2UI_STREAMING` | `true` | Streams A2UI surfaces as they are composed | `src/backend/src/services/a2ui/runner.py` |
| `A2UI_STREAM_INTERVAL_MS` | `120` | Minimum interval between streamed A2UI updates | `src/backend/src/services/a2ui/runner.py` |
| `A2UI_COMPOSE_RETRIES` | `2` | Retries when a composed surface fails validation | `src/backend/src/services/a2ui/runner.py` |
| `A2UI_COMPOSE_TIMEOUT` | `240` | Seconds to wait for composition | `src/backend/src/services/chat/service.py` |
| `A2UI_EARLY` | `true` | Starts composing before the answer finishes | `src/backend/src/services/a2ui/early.py` |
| `CHAT_COMPACTION` | `true` | Summarizes older chat history when it grows past the trigger | `src/backend/src/services/chat/context_compaction.py` |
| `CHAT_COMPACTION_TRIGGER_CHARS` | `8000` | History size that triggers compaction | `src/backend/src/services/chat/context_compaction.py` |
| `CHAT_COMPACTION_KEEP_ROWS` | `24` | Recent rows kept verbatim | `src/backend/src/services/chat/context_compaction.py` |
| `CHAT_COMPACTION_MODEL` | Unset (chat model) | Model used for compaction | `src/backend/src/services/chat/context_compaction.py` |
| `CHAT_SUMMARY_MAX_CHARS` | `2000` | Maximum size of the compacted summary | `src/backend/src/services/chat/context_compaction.py` |
| `CHAT_HISTORY_RECENT_LIMIT` | `120` | Rows loaded for compaction | `src/backend/src/services/chat/context_compaction.py` |
| `CHAT_HISTORY_MAX_CHARS` | `6000` | Budget for the conversation preamble | `src/backend/src/services/chat/conversation_preamble.py` |
| `CHAT_HISTORY_MAX_ASSISTANT_TURNS` | `8` | Assistant turns kept in the preamble | `src/backend/src/services/chat/conversation_preamble.py` |
| `CHAT_HISTORY_USER_CHAR_CAP`, `CHAT_HISTORY_ASSISTANT_CHAR_CAP`, `CHAT_HISTORY_LAST_ANSWER_CHAR_CAP` | `500`, `240`, `12000` | Per-message truncation in the preamble | `src/backend/src/services/chat/conversation_preamble.py` |
| `CHAT_MEMORY_SETTLE_SECONDS` | `20` | Wait for memory writes to settle before history reads | `src/backend/src/services/chat/history.py` |
| `DEFAULT_LLM_MODEL` | `databricks-gemini-3-8-flash` | Fallback model when no UI selection and no `KASAL_DEFAULT_MODEL` apply | `src/backend/src/utils/model_config.py` |
| `AGENT_MODEL`, `CREW_MODEL`, `CONNECTION_MODEL`, `DEFAULT_TASK_MODEL`, `TASK_MODEL`, `DEFAULT_IMPROVE_MODEL`, `PROMPT_IMPROVE_MODEL` | `DEFAULT_LLM_MODEL` | Fallbacks for the generators when the request names no model. Prefer choosing the model in the UI | `src/backend/src/services/generation/` |
| `DAX_LLM_BATCH_SIZE` | `12` | Measures per LLM call in the DAX fallback translator | `src/backend/src/services/tools/metric_view_utils/dax_llm_fallback.py` |

## Decisions (Jev)

The Jev decisions provider is off unless the deployment configures its endpoint:

| Variable | Default | What it does | Read in |
|---|---|---|---|
| `JEV_API_BASE` | Unset | Base URL of the Jev decisions API, for example `https://jev.example.com`. Unset means Jev is not configured: decisions stay off for every workspace, the runtime reads no credentials, and an admin cannot enable Jev (the save is refused with `400`). When set, each workspace still opts in and stores its key as `JEV_API_KEY` under **Configuration → API Keys** | `src/backend/src/config/settings.py`, `src/backend/src/services/decisions/provider.py` |

It is deployment-owned configuration and has no built-in default; a prompt or tool result never supplies it.

## Memory, knowledge and recipes

Memory maintenance runs in the background; these control it and the knowledge store:

| Variable | Default | What it does | Read in |
|---|---|---|---|
| `KASAL_MEMORY_DIR` | `~/.kasal/memory` | Root of the local memory stores, one per workspace | `src/backend/src/utils/memory_paths.py` |
| `KASAL_MEMORY_MAINTENANCE_INTERVAL` | `900` | Seconds between maintenance passes | `src/backend/src/services/memory/maintenance/passes.py` |
| `KASAL_MEMORY_LLM_CONSOLIDATION` | `true` | LLM consolidation of near-duplicate memories | `src/backend/src/services/memory/maintenance/passes.py` |
| `KASAL_MEMORY_SUPERSESSION` | `true` | Marks memories superseded by newer facts | `src/backend/src/services/memory/maintenance/supersession.py` |
| `KASAL_MEMORY_SWEEP` | `true` | Periodic sweep pass | `src/backend/src/services/memory/maintenance/sweep.py` |
| `KASAL_MEMORY_SWEEP_INTERVAL_HOURS`, `KASAL_MEMORY_SWEEP_BATCH` | `6`, `5` | Sweep cadence and batch size | `src/backend/src/services/memory/maintenance/sweep.py` |
| `KASAL_MEMORY_FORGETTING` | `false` | Deletes old low-value memories | `src/backend/src/services/memory/maintenance/forgetting.py` |
| `KASAL_MEMORY_SUPERSEDED_RETENTION_DAYS`, `KASAL_MEMORY_EPISODIC_TTL_DAYS`, `KASAL_MEMORY_IMPORTANCE_FLOOR` | `90`, `180`, `0.4` | Forgetting thresholds | `src/backend/src/services/memory/maintenance/forgetting.py` |
| `KASAL_MEMORY_WRITE_SCREENING` | `quarantine` | Prompt-injection screening of memory writes: `quarantine` drops high-severity content, `annotate` only records findings, `off` disables it | `src/backend/src/services/memory/run/write_hygiene.py` |
| `KASAL_MEMORY_RECALL_MIN_SCORE` | Per-embedder calibrated floor | Deployment-wide minimum recall score; a workspace's own tuning value wins | `src/backend/src/services/memory/engine/memory.py` |
| `KASAL_MEMORY_RECALL_MAX_DROP` | `0.12` | Drops recall candidates scoring this far below the best one | `src/backend/src/services/memory/run/recall.py` |
| `KNOWLEDGE_TTL_DAYS` | `7` | Lifetime of uploaded knowledge embeddings | `src/backend/src/services/knowledge/embedding_service.py` |
| `EMBEDDING_BATCH_SIZE` | `32` | Texts per embedding request | `src/backend/src/services/llm/embeddings.py` |
| `EMBEDDING_TIMEOUT_SECONDS`, `EMBEDDING_HTTP_TIMEOUT_SECONDS` | `60`, `30` or `60` depending on the path | Embedding timeouts | `src/backend/src/services/llm/embeddings.py` |
| `WORKFLOW_RECIPE_MIN_SIMILARITY` | `0.75` | Similarity needed to offer a past crew as a recipe | `src/backend/src/services/recipes/recipes.py` |
| `WORKFLOW_RECIPE_MINE_BATCH` | `100` | Runs mined per batch | `src/backend/src/services/recipes/recipes.py` |
| `WORKFLOW_RECIPE_HOLDOUT` | `0.0` | Fraction of generations held out from recipes, for measurement | `src/backend/src/services/recipes/recipes.py` |

For more information, see the [memory guide](./MEMORY.md).

## Event triggers

The trigger queue consumer starts with the server:

| Variable | Default | What it does | Read in |
|---|---|---|---|
| `KASAL_EVENT_TRIGGERS_INTERVAL` | `5` | Seconds between queue polls | `src/backend/src/main.py` |
| `KASAL_EVENT_TRIGGERS_BATCH` | `5` | Events handled per poll | `src/backend/src/main.py` |
| `KASAL_EVENT_TRIGGERS_MAX_HOPS` | `5` | Maximum chain depth before a triggered run stops emitting | `src/backend/src/services/triggers/emit_service.py` |

## Logging

Logs go to the console and to `src/backend/logs/`. `run.sh --help` lists every per-domain variable.

| Variable | Default | What it does | Read in |
|---|---|---|---|
| `KASAL_LOG_LEVEL` | `INFO` (falls back to `LOG_LEVEL`) | Global level: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` or `OFF` | `src/backend/src/config/logging.py`, `src/backend/src/core/logger.py` |
| `KASAL_LOG_APP` | Follows the global level | Level for Kasal's own loggers | `src/backend/src/config/logging.py` |
| `KASAL_LOG_THIRD_PARTY` | `WARNING` | Level for third-party libraries | `src/backend/src/config/logging.py` |
| `KASAL_LOG_CONSOLE`, `KASAL_LOG_FILE` | `true` | Console and file output | `src/backend/src/config/logging.py` |
| `KASAL_LOG_<DOMAIN>` | Global level | Per-domain level, for example `KASAL_LOG_CREW`, `KASAL_LOG_FLOW`, `KASAL_LOG_LLM`, `KASAL_LOG_DATABASE` | `src/backend/src/config/logging.py`, `src/backend/src/core/logger.py` |
| `KASAL_DEBUG_ALL` | `false` | Debug for every logger, including SQL | `src/backend/src/config/logging.py`, `src/backend/src/db/session.py` |
| `SQL_DEBUG` | `false` | Logs every SQL statement; slow | `src/backend/src/db/session.py` |
| `KASAL_DEBUG_TRACES` | Empty | Legacy switch for debug trace output in crew subprocesses | `src/backend/src/services/execution/subprocess_bootstrap.py` |

## Observability, MLflow and OpenTelemetry

Workspace MLflow and telemetry settings are configured in the UI. These variables cover local servers and the platform-injected OTel endpoint:

| Variable | Default | What it does | Read in |
|---|---|---|---|
| `MLFLOW_TRACKING_URI` | Forced to `databricks` | Set it at launch to a local `http(s)` MLflow server; `main.py` saves your value as `KASAL_LAUNCH_MLFLOW_TRACKING_URI` before overwriting it | `src/backend/src/main.py`, `src/backend/src/services/mlflow/local.py` |
| `MCP_SERVER_ENABLED` | Empty | With a local `MLFLOW_TRACKING_URI`, registers judges and prompts on that local server | `src/backend/src/services/prompt_optimization/gepa/mlflow_session.py` |
| `MLFLOW_CREW_TRACES_EXPERIMENT` | Derived from the workspace | Experiment for crew traces on a local server | `src/backend/src/services/mlflow/local.py` |
| `MLFLOW_TRACING_SQL_WAREHOUSE_ID` | Unset (`src/app.yaml`: the `sql-warehouse` resource) | Warehouse for trace storage in Unity Catalog | `src/backend/src/services/prompt_optimization/gepa/mlflow_session.py` |
| `MLFLOW_EVAL_MAX_ROWS` | `200` | Row cap for an evaluation run | `src/backend/src/services/mlflow/evaluation_runner.py` |
| `MLFLOW_EVAL_JUDGE_MODEL`, `GEPA_JUDGE_MODEL` | Unset | Fallback judge models when none is configured in the UI | `src/backend/src/services/mlflow/service.py`, `src/backend/src/services/prompt_optimization/gepa/judge_model.py` |
| `KASAL_OTEL_TRACING` | `true` | Kasal's OpenTelemetry tracer for crew runs | `src/backend/src/services/otel_tracing/otel_config.py` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Unset | OTLP collector; Databricks Apps injects it when app telemetry is enabled | `src/backend/src/core/logger.py` |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `grpc` | OTLP protocol | `src/backend/src/core/logger.py` |
| `OTEL_SERVICE_NAME` | `kasal` | Service name on exported logs | `src/backend/src/core/logger.py` |

For more information, see [MLflow tracing setup](./mlflow-tracing-setup.md) and [prompt optimization setup](./prompt-optimization-setup.md).

## Frontend

The frontend reads these at build or dev-server time from the environment or from `.env` files in `src/frontend/`:

| Variable | Default | What it does | Read in |
|---|---|---|---|
| `VITE_KASAL_PORT` | `KASAL_PORT`, else `8000` | Port of the local backend in development: the `/api` proxy target and the port the API client and chat streams call. Not used in a build | `src/frontend/vite.config.ts`, `src/frontend/src/shared/api/backendOrigin.ts` |
| `VITE_API_URL` | `http://localhost:<VITE_KASAL_PORT>/api/v1` in dev, `/api/v1` in a build | Overrides the base URL of the backend API. You do not need it to change the port | `src/frontend/src/shared/api/client.ts` |
| `VITE_KASAL_API_URL` | `/api/v1` | API base for the chat app store | `src/frontend/src/features/chat/store/appStore.ts` |
| `VITE_DEV_USER_EMAIL` | `dev@localhost` | Email the dev server sends as `X-Forwarded-Email`, and as `_sse_email` on loopback event streams. Development builds only | `src/frontend/src/shared/api/client.ts`, `src/frontend/src/shared/api/sseContext.ts` |
| `ANALYZE` | Unset | `true` opens a bundle-size report after `vite build` | `src/frontend/vite.config.ts` |

The Vite dev server runs on port 3000 and proxies `/api` to `http://localhost:<port>`, where the port is `VITE_KASAL_PORT`, else `KASAL_PORT`, else `8000`.

## Testing and CI

These only matter for tests and CI:

| Variable | Default | What it does | Read in |
|---|---|---|---|
| `VITEST_COVERAGE_REPORT_ONLY` | Unset | Drops the Vitest coverage thresholds so coverage is reported without failing the run; the CI coverage job sets it | `src/frontend/vitest.config.ts` |
| `PYTEST_CURRENT_TEST` | Set by pytest | Lets the database layer detect a test run | `src/backend/src/db/session.py` |
| `SEED_DEBUG` | Forced to `True` by `main.py` | Verbose seeder output when running seeders standalone | `src/backend/src/seeds/seed_runner.py` |

## Internal variables

Kasal sets these itself, mostly for spawned crew and flow subprocesses. Do not set them:

- `CREW_SUBPROCESS_MODE`, `FLOW_SUBPROCESS_MODE`, `KASAL_EXECUTION_ID`, `LAKEBASE_ACTIVE`: mark and configure a child interpreter.
- `KASAL_LAUNCH_MLFLOW_TRACKING_URI`: your launch-time `MLFLOW_TRACKING_URI`, saved by `main.py`.
- `KASAL_LOCKED_ENTRYPOINT`: guards against re-executing `src/entrypoint.py`.
- `USE_NULLPOOL`, `LOG_DIR`, `CREWAI_DISABLE_TELEMETRY`, `CREWAI_VERBOSE`, `PYTHONUNBUFFERED`: forced by `main.py`, `run.sh` or the subprocess bootstrap.
- `DATABRICKS_ENABLE_AI_GATEWAY`, and `DATABRICKS_HOST`/`DATABRICKS_TOKEN` inside a run: set from the UI configuration and the caller's credentials before a run starts.
- `DATABASE_URL`: set by `src/entrypoint.py`; the backend reads `DATABASE_URI`.

## Recommended: a backend env example file

There is no backend `.env.example`. Adding one generated from this page, covering the server, database, local identity, logging and observability groups, would give new developers a single place to start. Because `.env` only feeds `Settings` fields, it should say which variables must be exported instead. Moving the remaining `os.getenv` defaults into `Settings` would make both this page and that file easier to keep accurate.

## See also

- [Quick start](./QUICK_START.md)
- [Developer guide](./DEVELOPER_GUIDE.md)
- [Security guide](./SECURITY.md)
- [Models reference](./MODELS.md)
- [API endpoints reference](./api_endpoints.md)

---

Back to the [documentation hub](./README.md).
