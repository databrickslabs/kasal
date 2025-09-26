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
```273:301:/Users/anshu.roy/Documents/kasal/src/backend/src/main.py
# Initialize FastAPI app
app = FastAPI(
    title=settings.PROJECT_NAME,
    description=settings.PROJECT_DESCRIPTION,
    version=settings.VERSION,
    lifespan=lifespan,
    docs_url="/api-docs" if settings.DOCS_ENABLED else None,
    redoc_url="/api-redoc" if settings.DOCS_ENABLED else None,
    openapi_url="/api-openapi.json" if settings.DOCS_ENABLED else None,
    openapi_version="3.1.0"
)

# Add user context middleware and include API
app.add_middleware(BaseHTTPMiddleware, dispatch=user_context_middleware)
app.include_router(api_router, prefix=settings.API_V1_STR)
```
- `api/`: HTTP route modules (per domain)
  - Examples: `executions_router.py`, `execution_logs_router.py`, `execution_trace_router.py`, `engine_config_router.py`, `agents_router.py`, `crews_router.py`, `databricks_*_router.py`, `memory_backend_router.py`, `schemas_router.py`, `tools_router.py`
```48:66:/Users/anshu.roy/Documents/kasal/src/backend/src/api/__init__.py
# Create the main API router
api_router = APIRouter()

# Include all the sub-routers
api_router.include_router(agents_router)
api_router.include_router(crews_router)
api_router.include_router(databricks_router)
api_router.include_router(databricks_knowledge_router)
api_router.include_router(flows_router)
api_router.include_router(healthcheck_router)
api_router.include_router(logs_router)
api_router.include_router(models_router)
api_router.include_router(databricks_secrets_router)
api_router.include_router(api_keys_router)
api_router.include_router(tasks_router)
api_router.include_router(templates_router)
api_router.include_router(group_tools_router)
api_router.include_router(mlflow_router)
```
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
```1:16:/Users/anshu.roy/Documents/kasal/src/frontend/src/config/api/ApiConfig.ts
import axios from 'axios';

export const config = {
  apiUrl:
    process.env.REACT_APP_API_URL ||
    (process.env.NODE_ENV === 'development'
      ? 'http://localhost:8000/api/v1'
      : '/api/v1'),
};

export const apiClient = axios.create({
  baseURL: config.apiUrl,
  headers: {
    'Content-Type': 'application/json',
  },
});
```
- `src/api/*Service.ts`: API clients per domain (Agents, Crews, Executions, Models, Tools, etc.)
- `src/components/`, `src/app/`, `src/store/`, `src/types/`, `src/utils/`, `src/hooks/`, `src/theme/`
- `public/` assets; `craco.config.js`, `tsconfig.json`