## Code Structure
A fast, skimmable map of the repository to help you find the right place quickly.

### Repository layout
```

├── README.md
└── src/
    ├── backend/            # FastAPI backend
    ├── frontend/           # React + TypeScript frontend
    ├── docs/               # Markdown docs (copied to /docs in the app)
    ├── build.py            # Frontend build + docs copy
    ├── deploy.py           # Deployment utilities
    └── manifest.yaml       # App metadata
```

### Backend (src/backend/src)
- main.py: FastAPI app bootstrap, CORS, middleware, startup/shutdown, scheduler, API router registration
- api/: HTTP routers per domain (agents, crews, executions, tools, models, engine-config, etc.)
- services/: Business logic & orchestration
  - Orchestration: execution_service.py, crewai_execution_service.py, process_crew_executor.py, scheduler_service.py
  - Integrations: databricks_*_service.py, mlflow_service.py
  - Observability: execution_logs_service.py, execution_trace_service.py, documentation_embedding_service.py
- repositories/: Data access (SQL/external/vector/mlflow)
- models/: SQLAlchemy entities
- schemas/: Pydantic request/response DTOs
- db/: Sessions and Alembic integration (session.py, all_models.py)
- config/: Settings & logging (settings.py, logging.py)
- core/: Cross-cutting utilities (llm_manager.py, logger.py, permissions.py, unit_of_work.py)
- engines/: AI engine integration (CrewAI prep/runner, memory, tools, guardrails)
- utils/: Helpers (user_context.py, databricks_url_utils.py, etc.)
- seeds/, scripts/, dependencies/: Seeders, scripts, DI helpers

### Frontend (src/frontend)
- CRA + TypeScript app
- src/config/api/ApiConfig.ts: API base URL selection and Axios client
- src/api/*Service.ts: API clients per domain (Agents, Crews, Executions, Models, Tools, etc.)
- src/components/: UI components & views
- src/store/: State management
- src/hooks/: Reusable logic
- src/types/: Shared TS types
- src/utils/, src/theme/: Utilities and theme
- public/: Static assets (docs copied to /docs here)

### Key entry points
- Backend app starts in main.py (includes api_router with prefix from settings)
- Frontend docs are served from /docs (markdown files copied there at build)

### Routers (where to look)
Common examples under src/backend/src/api/:
- agents_router.py, crews_router.py, executions_router.py, execution_logs_router.py, execution_trace_router.py
- engine_config_router.py, models_router.py, tools_router.py, schemas_router.py
- databricks_*_router.py (secrets, knowledge, connection)

### Database & migrations
- DB sessions configured in src/backend/src/db/session.py
- Alembic configuration via alembic.ini (root) and migrations/ (root)
- Models aggregated in src/backend/src/db/all_models.py

### Configuration & logging
- src/backend/src/config/settings.py: env-driven settings (CORS, DB URIs, docs flags, seeding)
- src/backend/src/config/logging.py and src/backend/src/core/logger.py: centralized logging

### Core & engines
- src/backend/src/core/llm_manager.py: provider/model selection, streaming options
- src/backend/src/engines/crewai/*: crew preparation, execution runner, callbacks, memory/tool adapters

### Tips: how to trace a feature
1) Start at the router file for the endpoint.
2) Open the called service and scan business logic.
3) Inspect repository methods and related models.
4) Check Pydantic schemas for request/response contracts.
5) Search for engine usage under engines/crewai if orchestration is involved.

---

## Development conventions (back end)

### Layering philosophy
- API (FastAPI routers) → Services (business logic) → Repositories (data access) → DB
- Keep routers thin (validation, auth), services cohesive (orchestration/transactions), repositories I/O-only.

### Naming & structure
- Routers: <domain>_router.py (e.g., agents_router.py)
- Services: <domain>_service.py
- Repositories: <domain>_repository.py
- Models: singular file per entity (e.g., agent.py)
- Schemas: mirror model names (e.g., schemas/agent.py)

### Request/response contracts
- Define input/output Pydantic models in src/backend/src/schemas/*
- Routers return explicit response models (response_model=...) where practical
- Prefer DTOs over ORM entities at boundaries

### Transactions & Unit of Work
- Encapsulate write operations inside service methods
- Use the UnitOfWork pattern for multi-repository transactions (core/unit_of_work.py)
- Repositories should not commit; services decide transactional scope

### Error handling
- Raise HTTPException in routers for user input errors
- Services raise domain errors; routers translate to HTTP
- Avoid broad try/except; log and rethrow with context

---

## Back end deep‑dive (files that matter)

### App bootstrap
- main.py: lifespan init (logging, DB init, seeders), CORS, user context middleware, include api_router
- config/settings.py: environment-driven settings (DB URIs, docs toggles, seeding)

### API surface (selected)
- api/__init__.py: composes all routers
- api/executions_router.py: start/stop/get execution
- api/execution_logs_router.py, api/execution_trace_router.py: logs and trace endpoints
- api/engine_config_router.py, api/models_router.py, api/tools_router.py: engine and model config, tool registry

### Services (selected)
- services/execution_service.py: high-level execution orchestration
- services/crewai_execution_service.py, engines/crewai/execution_runner.py: CrewAI integration points
- services/scheduler_service.py: background scheduling
- services/documentation_embedding_service.py: embeddings for better generation

### Repositories (selected)
- repositories/execution_repository.py, execution_history_repository.py: persistence for runs
- repositories/databricks_*_repository.py: Databricks secrets, vector index, volumes

### Database & sessions
- db/session.py: async engine/session, SQLite lock retries, SQL_DEBUG logging
- db/all_models.py: imports all models for Alembic

### Observability
- core/logger.py: central logger manager (writes to LOG_DIR)
- services/execution_logs_service.py, execution_trace_service.py: persisted logs/trace

---

## Configuration quick reference

Defined in src/backend/src/config/settings.py:
- DATABASE_TYPE=postgres|sqlite (defaults to postgres)
- SQLITE_DB_PATH=./app.db when using SQLite
- POSTGRES_* envs for Postgres connection
- DOCS_ENABLED=true|false (exposes /api-docs, /api-redoc, /api-openapi.json)
- AUTO_SEED_DATABASE=true|false (background seeding after DB init)
- LOG_LEVEL=INFO|DEBUG
- SQL_DEBUG=true|false (emits SQL to logs for troubleshooting)

Notes:
- USE_NULLPOOL is set early in main.py to avoid asyncpg pool issues
- Logs default under src/backend/src/logs/

---

## Engines & orchestration

- Engine selection: src/backend/src/engines/engine_factory.py
- CrewAI integration lives under src/backend/src/engines/crewai/
  - crew_preparation.py: build agents, tools, memory for a run
  - execution_runner.py: run loop, callbacks/guardrails
  - trace_management.py: hook into tracing pipeline

Memory/model caveat:
- Known limitation for specific Databricks models (Claude / GPT‑OSS) on entity extraction
- Automatic fallback to databricks-llama-4-maverick for memory entity extraction only

---

## Front end deep‑dive

### Docs viewer (this page)
- Markdown fetched from /docs/<file>.md (copied from src/docs at build)
- Mermaid supported via fenced ```mermaid code blocks
- Images rendered responsively; prefer /docs/images/... or relative ./images/...
- Internal markdown links are intercepted to load other docs in‑app

### API client
- src/frontend/src/config/api/ApiConfig.ts determines API base URL
- Default dev: http://localhost:8000/api/v1; override with REACT_APP_API_URL

### UI organization
- src/components/: feature folders and shared components
- src/api/: type‑safe client wrappers by domain
- src/store/, src/hooks/, src/utils/, src/theme/

---

## End‑to‑end example (from API call to DB)

1) Router (executions_router.py) accepts POST /executions with schema
2) Service (execution_service.py) validates logic, kicks off orchestration
3) Engine (engines/crewai/...) prepares crew and runs execution
4) Logs/Traces recorded via services and repositories
5) Repositories (execution_repository.py) persist status/history
6) Client polls GET /executions/{id} and GET /execution-logs/{id}

---

## Anti‑patterns to avoid
- Business logic in routers (keep slim and delegate)
- Services directly returning ORM entities (use schemas/DTOs)
- Repositories committing transactions (services own commit/rollback)
- Ad‑hoc logging without the central logger (use core/logger.py)

---

## Quick links
- Back end entrypoint: src/backend/src/main.py
- Compose routers: src/backend/src/api/__init__.py
- Settings: src/backend/src/config/settings.py
- Sessions: src/backend/src/db/session.py
- CrewAI runner: src/backend/src/engines/crewai/execution_runner.py