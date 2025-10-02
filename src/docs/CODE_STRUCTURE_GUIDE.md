## 🟦 Code Structure
A map of folders and files to help you find what you need quickly.

### 🟩 Top-level
Core project files, app metadata, and documentation.
- `README.md`: Overview and documentation index
- `src/entrypoint.py`: App entry for Databricks Apps
- `src/build.py`: Frontend build pipeline and docs copy
- `src/deploy.py`: Deployment automation
- `src/manifest.yaml`, `src/app.yaml`: App metadata/config
- `src/docs/`: Documentation (guides, API reference, images)
- `src/frontend/`: React SPA
- `src/backend/`: FastAPI backend service

### 🟨 Backend (`src/backend/src`)
FastAPI backend: entrypoint, routers, services, repositories, and DB.
- `main.py`: FastAPI app bootstrap, CORS, middleware, startup/shutdown, scheduler, seeds, API router registration

 - `services/`: Business logic/orchestration and integrations
  - Orchestration: `execution_service.py`, `crewai_execution_service.py`, `process_crew_executor.py`, `scheduler_service.py`
  - Integrations: `databricks_*_service.py`, `mlflow_service.py`
  - Observability/aux: `execution_logs_service.py`, `execution_trace_service.py`, `documentation_embedding_service.py`
- `repositories/`: Data access (SQL/external/vector/mlflow)
  - Examples: `execution_repository.py`, `execution_history_repository.py`, `databricks_vector_index_repository.py`, `database_backup_repository.py`
- `models/`: SQLAlchemy ORM entities (tables)
- `schemas/`: Pydantic request/response models
- `db/`: DB setup and sessions
  - `session.py`: async engine/session, SQLite lock retries, optional SQL logging
  - `all_models.py`: model import aggregator
- `config/`: App configuration
  - `settings.py`: env-driven settings and flags (e.g., `DOCS_ENABLED`, `AUTO_SEED_DATABASE`)
  - `logging.py`: logging configuration
- `core/`: Cross-cutting concerns
  - `llm_manager.py`: LLM routing/provider config
  - `logger.py`: centralized logging manager
  - `permissions.py`, `dependencies.py`, `base_service.py`, `base_repository.py`, `unit_of_work.py`
  - `entity_extraction_fallback.py`: LLM memory fallback logic
- `engines/`: AI orchestration engines
  - `crewai/`: `execution_runner.py`, `crew_preparation.py`, `flow_preparation.py`, callbacks, memory, tools, guardrails
  - `engine_factory.py`: engine selection
- `utils/`: Utilities
  - `user_context.py`: Databricks header parsing, group context, middleware
  - `databricks_url_utils.py`, `databricks_auth.py`, rate limiting, crypto, prompts
- `seeds/`, `scripts/`, `dependencies/`: Seeders, scripts, DI helpers

### 🟪 Frontend (`src/frontend`)
React + TypeScript application, API client, components, and state.
- React + TypeScript (CRA + Craco)
- `src/config/api/ApiConfig.ts`: API base URL selection and Axios client


---  

# Alembic Migrations  
All scripts live in `src/backend/migrations/versions`. They evolve the database schema over time. Two core files support them:  

## src/backend/migrations/env.py  
Sets up the Alembic context, reading DB URLs from SQLAlchemy models. It:  
- Imports metadata from `all_models`.  
- Configures offline/online migration modes.  

## src/backend/migrations/script.py.mako  
A Jinja template used by Alembic to generate new revision files. It defines boilerplate for `upgrade()` and `downgrade()`.  

## src/backend/scripts/migrations/migrate_mcp_group_id.py  
A custom Python script (outside Alembic) that migrates existing MCP server records to a new `group_id` structure.  

---
# REST API Routers  
Each file under `src/backend/src/api` defines a FastAPI router. They follow a common pattern:  

```python
router = APIRouter(prefix="/v1/resource", tags=["resource"])
@router.get("/", response_model=List[ResourceSchema])
async def list_resources(...):
    return await service.list()
```

Below is an example for **Agents**. All other routers mirror this structure.

```api
{
  "title": "List Agents",
  "description": "Retrieve all agents for the current group.",
  "method": "GET",
  "baseUrl": "https://api.kasal.ai",
  "endpoint": "/v1/agents",
  "headers": [
    {"key": "Authorization", "value": "Bearer <token>", "required": true}
  ],
  "queryParams": [
    {"key": "limit", "value": "Max items to return", "required": false},
    {"key": "offset", "value": "Pagination offset", "required": false}
  ],
  "pathParams": [],
  "bodyType": "none",
  "responses": {
    "200": {
      "description": "List of agents",
      "body": "[{ \"id\": \"...\", \"name\": \"Support Bot\" }]"
    }
  }
}
```

- **agent_generation_router.py**: Endpoints to trigger agent‐based content generation.  
- **agents_router.py**: CRUD for `Agent` entities.  
- **api_keys_router.py**: Manage API keys.  
- **auth_router.py**: Login, logout, token refresh.  
- **chat_history_router.py**: Query past chat logs.  
- **connections_router.py**: Database/MCP connection configs.  
- **crew_generation_router.py**: Generate multi‐agent crews.  
- **crews_router.py**: CRUD for `Crew` entities.  
- **database_management_router.py**: DB backup/restore.  
- **databricks_knowledge_router.py**: Manage Databricks knowledge sources.  
- **databricks_router.py**: Databricks environment config.  
- **databricks_secrets_router.py**: Vault secrets for Databricks.  
- **dispatcher_router.py**: Dispatch flows to MCP servers.  
- **documentation_embeddings_router.py**: CRUD for doc embeddings.  
- **dspy_router.py**: DSPy optimization endpoints.  
- **engine_config_router.py**: Manage LLM engine settings.  
- **execution_history_router.py**: Query past execution metadata.  
- **execution_logs_router.py**: Stream logs for executions.  
- **execution_trace_router.py**: Retrieve trace timeline.  
- **executions_router.py**: Trigger and control executions.  
- **flow_execution_router.py**: Read flow‐execution mappings.  
- **flows_router.py**: CRUD for `Flow` entities.  
- **genie_router.py**: “Genie” autocomplete endpoints.  
- **group_router.py**: Manage user groups.  
- **group_tools_router.py**: Assign tools to groups.  
- **healthcheck_router.py**: Liveness and readiness probes.  
- **logs_router.py**: Aggregate application logs.  
- **mcp_router.py**: MCP server CRUD and health.  
- **memory_backend_router.py**: Manage memory backends.  
- **mlflow_router.py**: MLflow experiment tracking endpoints.  
- **models_router.py**: List and select provider models.  
- **scheduler_router.py**: Job scheduling endpoints.  
- **schemas_router.py**: Dynamically serve JSON schemas.  
- **task_generation_router.py**: Generate tasks via AI.  
- **task_tracking_router.py**: Track running tasks.  
- **tasks_router.py**: CRUD for `Task` entities.  
- **template_generation_router.py**: Generate prompt templates.  
- **templates_router.py**: CRUD for `Template` entities.  
- **tools_router.py**: CRUD for `Tool` definitions.  
- **users_router.py**: User profile and permissions management.  

---
# Configuration  
These modules centralize app settings and logging.

## src/backend/src/config/logging.py  
Sets up Python `logging` with:  
- Console and file handlers.  
- JSON-formatted logs when in structured mode.  
- Integrates with Sentry if DSN provided.  

## src/backend/src/config/settings.py  
Defines Pydantic `Settings` for:  
- Database URLs  
- Secret keys  
- Third-party API endpoints  
- Feature toggles (e.g., multitenancy)  

```python
class Settings(BaseSettings):
    database_url: PostgresDsn
    sentry_dsn: Optional[HttpUrl] = None
    multi_tenant: bool = True
```

---

# Core LLM Handlers  
These classes wrap interactions with large language models.

## src/backend/src/core/llm_handlers/databricks_gpt_oss_handler.py  
- Interfaces with Databricks’ open‐source GPT endpoint.  
- Formats requests and parses streaming tokens.  
- Applies rate limits via `asyncio_utils`.  

## src/backend/src/core/llm_handlers/gpt5_handler.py  
- Native handler for GPT-5 provider.  
- Supports function calling and chunked streaming.  

## src/backend/src/core/llm_handlers/gpt5_llm_wrapper.py  
- Adapts the `gpt5_handler` to the generic `LLM` interface.  
- Adds retry logic and exponential backoff.  

---

# Core Infrastructure  

## src/backend/src/core/base_repository.py  
Abstracts basic CRUD operations for any SQLAlchemy model:  
- `get()`, `list()`, `create()`, `update()`, `delete()`.  

## src/backend/src/core/base_service.py  
Provides transaction scoping and error handling around repositories.  

## src/backend/src/core/dependencies.py  
Defines FastAPI dependencies for injecting:  
- `UnitOfWork`  
- Service instances  
- Current user context  

## src/backend/src/core/entity_extraction_fallback.py  
Fallback logic for extracting entities when NLP fails.  

## src/backend/src/core/llm_manager.py  
Centralizes LLM calls:  
- Chooses correct handler based on config.  
- Orchestrates streaming vs. non-streaming modes.  

## src/backend/src/core/logger.py  
Wraps Python logger to include request IDs and user context.  

## src/backend/src/core/permissions.py  
Decorators and functions enforcing RBAC at service/router level.  

## src/backend/src/core/unit_of_work.py  
Coordinator for DB sessions:  
- Begins/commits/rolls back transactions.  
- Exposes repositories for each entity.  

---

# Database Layer  

## src/backend/src/db/alembic/versions/create_data_processing_table.py  
Alembic script creating the `data_processing` table and its indexes.  

## src/backend/src/db/all_models.py  
Imports and registers all SQLAlchemy `Base` models used by Alembic.  

## src/backend/src/db/base.py  
Defines `Base = declarative_base()` and common mixins (e.g., `TimestampMixin`).  

## src/backend/src/db/database_router.py  
Routes DB calls to either the primary or read-replica based on query type.  

## src/backend/src/db/lakebase_session.py  
Creates SQLAlchemy sessions for “lake” database (analytics).  

## src/backend/src/db/session.py  
Configures the main application DB session pool and event listeners.  

---

# Authentication Dependencies  

## src/backend/src/dependencies/admin_auth.py  
Ensures the current user has admin privileges before proceeding.  

## src/backend/src/dependencies/auth.py  
Validates JWT tokens, extracts user info, and enforces login.  

---