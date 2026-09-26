# Backend CLAUDE.md

Backend-specific instructions for Claude Code when working in the backend directory.

## Commands

### Development
- **Dependencies are managed with `uv`** (not Poetry). Install/sync: `uv sync` (or `uv sync --frozen`). The venv lives at `src/backend/.venv`.
- **Start server**: `./run.sh` (defaults to SQLite) or `./run.sh postgres` for PostgreSQL. `run.sh` runs `uv sync --frozen` then `.venv/bin/uvicorn src.main:app --reload`, bound to `127.0.0.1` (set `KASAL_BIND_HOST=0.0.0.0` to expose it; see `./run.sh -h`). It also sets `LOCAL_DEV_AUTH=true`; see "Local identity" below.
- **Port**: `KASAL_PORT` (default 8000). Start the dev frontend with the same `KASAL_PORT` (it also reads `VITE_KASAL_PORT`) so its API client and proxy follow.
- **Run tests**: `python run_tests.py` (all tests + linting; **parallel by default**)
- **Lint only / tests only**: `python run_tests.py --lint-only` or `--skip-lint`
  (CI runs them as separate parallel jobs; lint no longer waits for green tests)
- **Run specific tests**: `python run_tests.py --type unit` or `python run_tests.py --type integration`
- **Run tests with coverage**: `python run_tests.py --coverage --html-coverage`
- **Run single test file**: `.venv/bin/python -m pytest tests/unit/test_file.py -v`
- **Run a slice fast**: add `-n auto --dist loadfile` to any raw pytest command.
  The unit suite is ~15s parallel vs ~130s serial; `--dist loadfile` keeps each
  file on one worker, which several suites need (they stub `sys.modules` or set
  env at module scope). Use `-n 0` to force serial when step-through debugging.
- **Watch for sleeping tests**: `--durations=12` surfaces them. A single file
  that called the real `time.sleep` in a retry path cost ~91s of the old
  runtime; patch `time.sleep` in tests that exercise retry/backoff.

### Local identity (why a direct `uvicorn` run gets 401)
A proxy in front of Kasal authenticates the user and forwards the identity as
headers; Kasal itself never logs anyone in. `utils/request_identity.py` is the
one resolver. Inside Databricks Apps (`DATABRICKS_APP_NAME` set) only
`X-Forwarded-*` is trusted and `X-Auth-Request-*` is stripped; elsewhere
`X-Auth-Request-*` (oauth2-proxy) wins over `X-Forwarded-*`.
`get_group_context` then fails closed: no identity -> 401, a workspace you may
not use -> 403, a resolver failure -> 503.

- `LOCAL_DEV_AUTH=true` (run.sh sets it) makes a request with NO identity
  header run as `dev@localhost`. It is refused
  inside Databricks Apps and with `ENVIRONMENT=production`.
- Starting `uvicorn src.main:app` yourself? Export `LOCAL_DEV_AUTH=true`, or
  every API call is a 401.
- The dev frontend always sends `X-Forwarded-Email` (`VITE_DEV_USER_EMAIL`,
  default `dev@localhost`); a header always wins over the fallback.

### Database
One local default: **SQLite at `src/backend/app.db`** (absolute, from
`core/paths.BACKEND_ROOT`, so the CWD does not matter). `run.sh`, alembic and
`run_seeders.py` all read `config/settings.py`, so they open the same file.
(`src/entrypoint.py`, the Databricks Apps entry point, sets its own path.)
Set `DATABASE_TYPE=postgres` (plus `POSTGRES_*`) for PostgreSQL, or
`SQLITE_DB_PATH` for another SQLite file, on every command you run.
- **Migrations**: `alembic upgrade head`
- **Create migration**: `alembic revision --autogenerate -m "description"`
- **Seed database**: `python run_seeders.py`

### Code Quality
- **Format code**: `python -m black src tests && python -m isort src tests`
- **Type checking**: `python -m mypy src`
- **Format check**: `python -m black --check src tests`

## Architecture

### Clean Architecture Pattern
- **Repository Pattern**: All database access through repositories, which
  RECEIVE a session and never open one (see `src/repositories/CLAUDE.md`)
- **One session owner**: the entry point picks the database, the service holds
  the transaction, the repository queries. A service that opens its own session
  is a bug and CI fails on it — see "Sessions" in `src/services/CLAUDE.md`
- **Service Layer**: Business logic orchestration
- **Dependency Injection**: Use FastAPI's built-in DI system
- **Async/Await**: All database operations are async

### Directory Structure
```
src/
├── api/             # FastAPI route handlers (see api/CLAUDE.md)
├── core/            # Dependencies, base repo/service, permissions, exceptions
├── models/          # SQLAlchemy database models
├── schemas/         # Pydantic validation schemas
├── services/        # Business logic layer (see services/CLAUDE.md)
├── repositories/    # Data access layer (see repositories/CLAUDE.md)
└── main.py          # Application entry point (uvicorn target: src.main:app)
```

### Three execution paths
Selected by `execution_type`:
- `"agent"` → **Chat** (`services/chat/`), a single agent run in-process
  (`Agent.kickoff_async`) for sub-second latency.
- `"crew"` → **Agent Builder** (`services/agent_builder/`), in a **subprocess**.
- `"flow"` → **Flow Builder** (`services/flow_builder/`), also **subprocess**.

The wire values stay `agent`/`crew`/`flow` — they are persisted in
`execution_history`. Only the code names follow the UI.

**There is no `src/engines/` package any more.** The paths are
`services/chat/`, `services/agent_builder/` and `services/flow_builder/`, over
shared machinery in `services/execution/` (hub, kernel, config, logs). See
`src/backend/src/services/execution/CLAUDE.md`.

## Database Patterns

### Migrations
- Always create Alembic migrations for model changes
- Use `alembic revision --autogenerate` to create migrations
- Test migrations on SQLite before applying to PostgreSQL
- **A migration alone does nothing at runtime.** Alembic is not run at startup and
  `create_all` never ALTERs an existing table. A column added to an existing table
  also needs a step in `src/db/self_heal/columns.py` (a new table: `tables.py`) —
  that is what heals existing SQLite, PostgreSQL and Lakebase installs.

### Connection Management
- When dealing with asyncpg and multiple event loops, asyncpg connections can conflict
- `USE_NULLPOOL=true` environment variable disables connection pooling when needed
- For testing with pytest-asyncio, use function-scoped fixtures instead of session-scoped ones

## Databricks Integration

### Model Configuration
Add new models in `src/seeds/model_configs.py`:
```python
"databricks-model-name": {
    "name": "databricks-model-name",
    "temperature": 0.7,
    "provider": "databricks",
    "context_window": 128000,
    "max_output_tokens": 25000
}
```

### User-Agent Telemetry (REQUIRED for new Databricks API callers)

Every HTTP call to a Databricks API **must** include a Kasal User-Agent header for usage tracking
(Partner Well-Architected Framework). When creating a new tool, service, or repository that calls
Databricks, add telemetry using one of these patterns:

**For direct REST calls (httpx/aiohttp):**
```python
from src.utils.telemetry import get_user_agent_header, KasalProduct
headers = {"Authorization": f"Bearer {token}", **get_user_agent_header(KasalProduct.YOUR_PRODUCT)}
```

**For headers from `auth.get_headers()`:**
```python
headers = auth.get_headers()
from src.utils.telemetry import get_user_agent_header, KasalProduct
headers.update(get_user_agent_header(KasalProduct.YOUR_PRODUCT))
```

**For WorkspaceClient SDK calls:**
```python
from databricks.sdk.useragent import with_product
from src.utils.telemetry import KASAL_BASE, VERSION, KasalProduct
with_product(f"{KASAL_BASE}_{KasalProduct.YOUR_PRODUCT}", VERSION)
w = WorkspaceClient()
```

**For LLM calls via `llm_manager.get_llm()`:** Already handled — `extra_headers` with User-Agent
is injected automatically by `llm_manager.py`.

Add new product constants to `KasalProduct` in `src/utils/telemetry.py` as needed.
Do NOT add telemetry to non-Databricks calls (e.g., PowerBI API, external APIs).

### LLM layering (do not re-implement across it)

| Layer | Owns |
|---|---|
| `src/core/llm/transport/` | Transport. OpenAI-compatible client, tool-call loop, streaming, context-window trim **and** output clamp, usage accounting, structured output, LLM events. Model- and tenant-agnostic. |
| `src/services/llm/` | Configuration. Model-catalogue lookup, per-tenant credentials, endpoint URLs, per-endpoint parameter rules, embeddings, and the endpoint handlers. A SERVICE: it reads the database. |
| `src/core/llm/` | What hangs off an LLM call without touching the DB: usage telemetry, context-limit phrases, JSON extraction, the subprocess token. |
| `src/services/llm/handlers/` | Endpoint policy. Engine-LLM subclasses: retry/backoff + fallback + message sanitization (`DatabricksRetryLLM`), Responses API (`DatabricksResponsesLLM`), self-hosted vLLM (`VLLMFunctionCallingLLM`). Named for the endpoint or protocol, never for a model. |
| `LLMManager` | The public facade (`services/llm/manager.py`). 38+ call sites use `completion`; keep it stable. |

Two rules, both learned the hard way:
- **litellm is not on the LLM path.** The transport drives endpoints with the
  OpenAI SDK. Anything configured on the `litellm` module (`drop_params`, callbacks,
  caching, `register_model`, monkey patches) affects only
  `LLMManager.completion_with_usage`. Params set when building an LLM ARE sent —
  there is no drop-params safety net.
- **Fix things on the path the request takes.** A patch on a shared library that
  the request no longer enters is indistinguishable from no fix at all, and it
  fails silently.

### Authentication Hierarchy

#### General Databricks Services
1. OBO (On-Behalf-Of) authentication using user token
2. OAuth client credentials (DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET)
3. API key from service (DATABRICKS_TOKEN or DATABRICKS_API_KEY)
4. Environment variables as last resort

#### Vector Search (Direct Access Indexes)
**Authentication Priority for Vector Search:**

1. **OBO authentication** - User token from X-Forwarded-Access-Token header (preferred)
2. **PAT from database** - Encrypted tokens stored in database
3. **PAT from environment** - DATABRICKS_TOKEN or DATABRICKS_API_KEY
4. **Default SDK authentication** - Falls back to SDK's authentication chain

**Note**: Service Principal authentication has been removed from Vector Search operations as OBO and PAT tokens provide sufficient access for Direct Access indexes.

### Vector Search Limitations
- Doesn't support bulk delete operations
- To empty an index: retrieve all vectors (up to 10,000) then delete them
- For larger indexes, consider dropping and recreating

## Memory Backend

Memory lives in `src/services/memory/`, organised by lifecycle stage — read the
package `__init__` first, then `src/docs/MEMORY.md`:

| Package | Owns |
|---|---|
| `config/` | What a teamspace configured: the `memory_backends` rows, which one is active, the Lakebase table (`LakebaseMemoryService`) |
| `storage/` | Where records live: `LocalStorageBackend` (SQLite, dev) and `LakebaseStorageBackend` (pgvector, prod) behind one `StorageBackend` protocol; `MemoryBackendFactory` builds one from a configuration; `EngineStorageAdapter` lets the engine talk to either |
| `engine/` | The `Memory` object: `remember` (label, consolidate, save) and `recall` (search, distil, explore); `MemoryRecord` |
| `run/` | A run's memory: `CrewMemoryService` builds the `Memory` for all three paths; `recall.py` before a task, `persist.py` after it |
| `maintenance/` | Between runs: dedupe, merge, supersede, forget, and the sweep that schedules them |

Rules that are easy to get wrong:

- **Every Memory Tuning knob must reach the layer that uses it.** Scoring
  weights go to the storage backend via `MemoryBackendFactory._scoring_kwargs`;
  everything else must be a declared field on `engine.Memory` — pydantic drops
  unknown kwargs silently, which is how five knobs were inert for months.
- **The crew id is deterministic and never scopes a read.** It hashes agent
  roles, task names, crew name, model and group — NOT the run name — so memory
  persists across runs of the same crew structure. Recall scopes by
  `group_id` (the tenant boundary); `crew_id` is provenance only.
- **Databricks Vector Search memory is retired.** A `databricks` backend row
  degrades to the local store; the knowledge/documentation indexes still read
  their workspace and endpoint from it, so the row must keep parsing.
- **The "Disabled Configuration"** (all memory types off) is honoured as "no
  memory" — the run proceeds without recall or persistence.

## Testing Requirements

### Coverage
- Minimum 80% coverage required
- Run with: `python run_tests.py --coverage --html-coverage`

### Test Types
- **Unit Tests**: Mock all external dependencies
- **Integration Tests**: Test full request/response cycle
- Use pytest fixtures for test setup
- Test all API endpoints with different scenarios

## Critical Rules

- **ALWAYS use async/await** - Never use sync operations that could block the event loop
- **NEVER include real URLs in code** - Use "https://example.com" or environment variables
- **DO NOT restart backend service** - It auto-reloads with `--reload` flag
- **Check service status**: `ps aux | grep uvicorn`

## AI Engine Integration

### Kasal Engine Integration Points
- **Engine Service** (`services/execution/engine_service.py`): the hub that dispatches to one of three paths.
- **Three execution paths**: `services/chat/` (in-process), `services/agent_builder/` (subprocess), `services/flow_builder/` (subprocess).
- **Kernel** (`services/execution/kernel/`): path-agnostic single-source agent/task build logic shared by all three.
- **Configuration Adapter** (`config_adapter.py`): normalizes frontend configs to engine shape.
- **Tool Factory** (`services/tools/`): extensible tool system. Tools live in
  services, not the engine — an agent calls them, but nothing about a tool
  requires a crew to be running.

See `services/execution/CLAUDE.md` for the full path model and the subprocess-boundary rules.

## Environment

- Python 3.11 required (pinned `>=3.11,<3.12` in `pyproject.toml`)
- Dependencies managed with **uv** (`pyproject.toml` + `uv.lock`, both committed). Never hand-edit `uv.lock`; run `uv lock` then `uv sync`.
- Database type controlled by environment variables / DB config (SQLite, PostgreSQL, or Databricks Lakebase)
- API keys for LLM services configured via environment or the API Keys service