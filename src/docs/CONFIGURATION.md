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
- [Configuration → Engines system settings](#configuration--engines-system-settings)
- [Settings that moved from environment variables to the UI](#settings-that-moved-from-environment-variables-to-the-ui)
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
- **`src/backend/src/config/settings.py`.** The pydantic `Settings` class declares `env_file=".env"`, resolved against the **current working directory**, with case-sensitive names. A `.env` file (like the environment) only affects the fields in `Settings.ENV_FIELDS`: `DATABASE_TYPE`, `DATABASE_URI`, `SYNC_DATABASE_URI`, `SQLITE_DB_PATH`, `POSTGRES_*`, `DEBUG_MODE` and `KASAL_EVENT_TRIGGERS_ALLOW_PRIVATE_WEBHOOKS`. Nothing calls `load_dotenv`, so a `.env` file does **not** set any variable read with `os.getenv`. In particular, `LOCAL_DEV_AUTH` in `.env` has no effect: export it instead.
- **Vite `.env` files in `src/frontend/`.** Only `VITE_*` variables reach the browser bundle.

> [!IMPORTANT]
> `src/backend/src/main.py` overwrites some variables at import time: `USE_NULLPOOL=true`, `CREWAI_DISABLE_TELEMETRY=true`, `SEED_DEBUG=True`, `LOG_DIR=<backend>/logs`, and `MLFLOW_TRACKING_URI=databricks` (a local MLflow server is set in Configuration → MLflow). Setting these yourself has no effect on the server process.

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

The production entrypoint `src/entrypoint.py` (used by `src/app.yaml`) takes `--db-type`, `--db-url`, `--port` (default `8000`), `--reload`, `--debug` and `--environment dev|prod` flags rather than variables. `--environment dev` makes it a local run like `run.sh`: it sets `KASAL_DEPLOYMENT_MODE=local` and `LOCAL_DEV_AUTH=true` (unless you set `LOCAL_DEV_AUTH` yourself); `prod`, or no flag, changes nothing. `--reload` restarts on changes under `src/backend/src`. It binds `KASAL_BIND_HOST` if set, otherwise `0.0.0.0` inside Databricks Apps (`DATABRICKS_APP_NAME` set) and `127.0.0.1` everywhere else. For how the three launchers differ, see [compare the launchers](./DEVELOPER_GUIDE.md#compare-the-launchers).

## Local development identity

Every protected API route depends on `get_group_context` (`src/backend/src/dependencies/providers.py`), which fails closed: a request with no identity gets **401**, an identity that resolves to no workspace gets 401, a workspace the user may not use gets 403, and a resolver failure gets 503. A 403 carries fixed detail text, never the resolver's reason or the requested group ID: `Access denied: no access to group` when the selected workspace is not one of the caller's (the frontend matches that phrase to drop a stale saved workspace), and `Access denied: workspace could not be resolved` for any other refusal. Outside Databricks Apps there is no proxy to supply `X-Forwarded-Email`, so a local run needs a development identity:

| Variable | Default | What it does | Read in |
|---|---|---|---|
| `LOCAL_DEV_AUTH` | Unset (`run.sh` sets `true`) | Opt-in. When `1`/`true`/`yes`/`on`, `LocalDevAuthMiddleware` adds `X-Forwarded-Email` to any request that has no identity header. Ignored, with an error log, when `DATABRICKS_APP_NAME` is set or `ENVIRONMENT` is `production`/`prod` | `src/backend/src/main.py` |
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
| `KASAL_RESPONSES_MAX_OUTPUT_TOKENS` | `16000` (falls back to `KASAL_CODEX_MAX_OUTPUT_TOKENS`) | Output-token cap for the Databricks Responses API adapter | `src/backend/src/services/llm/handlers/databricks_responses_llm.py` |
| `KASAL_REASONING_EFFORT_DISABLED` | Empty | `1`/`true`/`yes` never sends a reasoning-effort parameter | `src/backend/src/utils/model_config.py` |
| `KASAL_REASONING_EFFORT_MODELS` | Empty | Comma-separated extra model-name substrings that accept reasoning effort | `src/backend/src/utils/model_config.py` |
| `KASAL_HARNESS` | Unset (`crewai`) | Harness a spawned run uses when neither the request nor the config chooses one. Normally set by Kasal for the child process | `src/backend/src/services/execution/harnesses/selection.py` |
| `KASAL_ENGINE_STORAGE_DIR` | `~/.local/share/kasal_engine` | Engine-local storage such as flow checkpoints. `CREWAI_STORAGE_DIR` wins when set | `src/backend/src/utils/storage_paths.py` |
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

The chat, generative UI (A2UI) and generator settings are no longer environment variables; see [Settings that moved from environment variables to the UI](#settings-that-moved-from-environment-variables-to-the-ui). A run with no model named uses the model chosen in the UI, else the installed default model (Configuration → Models); `DEFAULT_LLM_MODEL` and the per-generator `AGENT_MODEL`/`CREW_MODEL`/`TASK_MODEL`/`CONNECTION_MODEL`/`PROMPT_IMPROVE_MODEL` are no longer read.

## Configuration → Engines system settings

A system administrator sets these under **Configuration → Engines → System settings**. They apply to every workspace and to runs started after saving. They replaced environment variables that a Databricks App never sets.

| Setting | Default | What it does | Replaced |
|---|---|---|---|
| Jev API URL | Unset | Base URL of the Jev decisions API; it must use `https://`. Unset means Jev is not configured: decisions stay off for every workspace, the runtime reads no credentials, and an admin cannot enable Jev (the save is refused with `400`). When set, each workspace still opts in and stores its key as `JEV_API_KEY` under **Configuration → API Keys** | `JEV_API_BASE` |
| Advanced → Agent time limit | `900` | Wall-clock seconds for one agent call when the agent sets none; `0` turns it off. An explicit `max_execution_time` on the agent wins | `KASAL_AGENT_MAX_EXECUTION_TIME` |
| Advanced → Run budget (Deep research) | Built-in profile | Tool rounds and seconds per agent call, seconds per run, and guardrail retries for deep-mode runs. Each field must be at least 1, and **Reset to default** restores the built-in value. Only modes a run applies are shown; today that is deep | `KASAL_BUDGET_<MODE>_<FIELD>` |
| Advanced → Memory maintenance | Sweep on, every `6` h, `5` workspaces per tick; `900` s between passes on one scope | The background memory sweep across all workspaces, and the throttle on the pass after a run | `KASAL_MEMORY_SWEEP`, `KASAL_MEMORY_SWEEP_INTERVAL_HOURS`, `KASAL_MEMORY_SWEEP_BATCH`, `KASAL_MEMORY_MAINTENANCE_INTERVAL` |
| Advanced → Knowledge search | Relevance `0.35`, `8` searches per agent turn (`0` = unlimited), uploads kept `7` days (`0` = forever) | Filters and limits knowledge search, and how long uploaded documents live | `KNOWLEDGE_MIN_SCORE`, `KNOWLEDGE_MAX_SEARCHES`, `KNOWLEDGE_TTL_DAYS` |

The settings are `engine_config` rows for engine `kasal`, read and written through `GET` and `PATCH /api/v1/engine-config/settings`. Synchronous readers use an in-process snapshot (`src/backend/src/services/settings/engine_settings.py`): the server loads it at startup, each crew or flow subprocess loads it when it starts, and a save updates it at once.

Two former variables are now constants in `src/backend/src/services/llm/manager.py`: the blocking-LLM thread pool (`LLM_MAX_CONCURRENCY = 64`, formerly `KASAL_LLM_MAX_CONCURRENCY`; the pool is sized at import, so a runtime setting could not apply) and the default extended-thinking budget (`10240` tokens, formerly `KASAL_THINKING_BUDGET_TOKENS`).


## Settings that moved from environment variables to the UI

A Databricks App sets only the variables its deployment injects, so every tunable below used to be fixed at its default in production. Each now lives in the Configuration section it belongs to. Server-wide ones sit in an **Advanced (all workspaces)** panel that only system administrators see; they are `engine_config` rows (engine `kasal`) read through `src/backend/src/services/settings/engine_settings.py`, loaded at startup and in each crew or flow subprocess, and applied to runs started after a save.

| Where | Settings | Replaced |
|---|---|---|
| System administration → **Output design** | Rich answers on/off (a kill switch for every workspace), early layout, streaming, stream interval, compose attempts, compose timeout | `A2UI_ENABLED`, `A2UI_EARLY`, `A2UI_STREAMING`, `A2UI_STREAM_INTERVAL_MS`, `A2UI_COMPOSE_RETRIES`, `A2UI_COMPOSE_TIMEOUT` |
| Teamspace → **Output design** → Advanced: rich answer behaviour | This teamspace's overrides of the five A2UI defaults above (not the on/off switch; the teamspace has its own). Stored in `ui_config.settings_json` | — |
| Engines → System settings → Advanced → **Chat** | Answer streaming (chat and crew), conversation summarising and its sizes, history limits and caps, memory settle time | `CHAT_TOKEN_STREAMING`, `CREW_TOKEN_STREAMING`, `CHAT_COMPACTION`, `CHAT_COMPACTION_KEEP_ROWS`, `CHAT_COMPACTION_TRIGGER_CHARS`, `CHAT_SUMMARY_MAX_CHARS`, `CHAT_HISTORY_*`, `CHAT_MEMORY_SETTLE_SECONDS` |
| **Tools** → Advanced (all workspaces) | Website text and bytes limits, DAX measures per LLM call, embedding batch size and timeouts | `SCRAPE_WEBSITE_MAX_CHARS`, `SCRAPE_WEBSITE_MAX_FETCH_BYTES`, `DAX_LLM_BATCH_SIZE`, `EMBEDDING_BATCH_SIZE`, `EMBEDDING_TIMEOUT_SECONDS`, `EMBEDDING_HTTP_TIMEOUT_SECONDS` |
| **Event triggers** → Advanced (all workspaces) | Queue check interval, events per check, chain depth limit | `KASAL_EVENT_TRIGGERS_INTERVAL`, `KASAL_EVENT_TRIGGERS_BATCH`, `KASAL_EVENT_TRIGGERS_MAX_HOPS` |
| **Prompts** → Advanced (all workspaces) | Workflow recipes: use curated past crews, minimum similarity, holdout fraction, runs mined per pass | `WORKFLOW_RECIPE_EXEMPLARS`, `WORKFLOW_RECIPE_MIN_SIMILARITY`, `WORKFLOW_RECIPE_HOLDOUT`, `WORKFLOW_RECIPE_MINE_BATCH` |
| System administration → **Models** → Advanced | Fallback model key used instead of a Databricks model when no workspace is available | `KASAL_FALLBACK_MODEL` |

Some former variables are now fixed in code, because nothing in Databricks Apps sets them and none is worth a Configuration field: the rate limit (`600/minute`, in memory), the group-membership cache (30 s), the SSE heartbeat (15 s), the LiteLLM response cache (in memory, 1 h), the local development identity (`dev@localhost`), the API docs (on locally, off in Apps), CORS (the local dev-server origins outside Apps, none inside), seeding (always) and the project name, version and API prefix. `KASAL_OTEL_TRACING`, `KASAL_DEBUG_TRACES`, `BACKEND_CORS_ORIGINS`, `DB_FILE_PATH` and `INSTRUCTOR_MODEL_NAME` were never used and are gone. `Settings` (`src/backend/src/config/settings.py`) reads the environment only for the fields in its `ENV_FIELDS` allow-list: the database connection, which the Apps launcher and local development set, and two development switches that Apps refuses.

The architecture test `src/backend/tests/unit/architecture/test_env_reads_stay_in_config.py` keeps it this way: outside `config/settings.py`, `config/logging.py` and `core/databricks_app.py`, a file may not gain an environment read, and none of the variables above may be read again.

## Memory, knowledge and recipes

Memory hygiene and retention are per-teamspace settings under **Configuration → Memory → Memory Tuning**; the memory sweep, its throttle and the knowledge limits are server-wide settings under **Configuration → Engines → System settings → Advanced**. [MEMORY.md](./MEMORY.md#settings) lists them with the variables they replaced. What remains here is host and transport configuration:

| Variable | Default | What it does | Read in |
|---|---|---|---|
| `KASAL_MEMORY_DIR` | `~/.kasal/memory` | Root of the local memory stores, one per workspace | `src/backend/src/utils/memory_paths.py` |

For more information, see the [memory guide](./MEMORY.md).

## Event triggers

The trigger queue consumer starts with the server. Its interval, batch size and chain-depth limit are under **Configuration → Event triggers → Advanced (all workspaces)**; see [Settings that moved from environment variables to the UI](#settings-that-moved-from-environment-variables-to-the-ui). `KASAL_EVENT_TRIGGERS_ALLOW_PRIVATE_WEBHOOKS` is listed under [Security and API limits](#security-and-api-limits).

## Logging

Logs go to the console and to `src/backend/logs/`. `run.sh --help` lists every per-domain variable.

| Variable | Default | What it does | Read in |
|---|---|---|---|
| `KASAL_LOG_LEVEL` | `INFO` | Global level: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` or `OFF` | `src/backend/src/config/logging.py`, `src/backend/src/core/logger.py` |
| `KASAL_LOG_APP` | Follows the global level | Level for Kasal's own loggers | `src/backend/src/config/logging.py` |
| `KASAL_LOG_THIRD_PARTY` | `WARNING` | Level for third-party libraries | `src/backend/src/config/logging.py` |
| `KASAL_LOG_CONSOLE`, `KASAL_LOG_FILE` | `true` | Console and file output | `src/backend/src/config/logging.py` |
| `KASAL_LOG_<DOMAIN>` | Global level | Per-domain level, for example `KASAL_LOG_CREW`, `KASAL_LOG_FLOW`, `KASAL_LOG_LLM`, `KASAL_LOG_DATABASE` | `src/backend/src/config/logging.py`, `src/backend/src/core/logger.py` |
| `KASAL_DEBUG_ALL` | `false` | Debug for every logger, including SQL | `src/backend/src/config/logging.py`, `src/backend/src/db/session.py` |
| `SQL_DEBUG` | `false` | Logs every SQL statement; slow | `src/backend/src/db/session.py` |

## Observability, MLflow and OpenTelemetry

Workspace MLflow and telemetry settings are configured in the UI. These variables cover local servers and the platform-injected OTel endpoint:

| Variable | Default | What it does | Read in |
|---|---|---|---|
| `MLFLOW_TRACKING_URI` | Forced to `databricks` | Not a way to configure MLflow: `main.py` overwrites it so nothing writes a local `mlruns/`. A local MLflow server is set per workspace in **Configuration → MLflow → Local MLflow server** | `src/backend/src/main.py` |
| `MLFLOW_CREW_TRACES_EXPERIMENT` | Derived from the workspace | Experiment for crew traces on a local server | `src/backend/src/services/mlflow/local.py` |
| `MLFLOW_TRACING_SQL_WAREHOUSE_ID` | Unset (`src/app.yaml`: the `sql-warehouse` resource) | Warehouse for trace storage in Unity Catalog | `src/backend/src/services/prompt_optimization/gepa/mlflow_session.py` |
| `MLFLOW_EVAL_MAX_ROWS` | `200` | Row cap for an evaluation run | `src/backend/src/services/mlflow/evaluation_runner.py` |
| `MLFLOW_EVAL_JUDGE_MODEL`, `GEPA_JUDGE_MODEL` | Unset | Fallback judge models when none is configured in the UI | `src/backend/src/services/mlflow/service.py`, `src/backend/src/services/prompt_optimization/gepa/judge_model.py` |
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
