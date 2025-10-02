## Developer Guide

### Requirements
- Python 3.9+
- Node.js 18+
- Postgres (recommended) or SQLite for local dev
- Databricks access if exercising Databricks features

### Quick start
```bash
# Backend
cd src/backend
python -m venv .venv && source .venv/bin/activate
pip install -r ../requirements.txt
./run.sh  # http://localhost:8000 (OpenAPI at /api-docs if enabled)

# Frontend
cd ../frontend
npm install
npm start  # http://localhost:3000
```

Health check:
```bash
curl http://localhost:8000/health
# {"status":"healthy"}
```

### Configuration
Backend settings: `src/backend/src/config/settings.py`
- Core: `DEBUG_MODE`, `LOG_LEVEL`, `DOCS_ENABLED`, `AUTO_SEED_DATABASE`
- Database:
  - `DATABASE_TYPE=postgres|sqlite` (default: `postgres`)
  - Postgres: `POSTGRES_SERVER`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
  - SQLite: `SQLITE_DB_PATH=./app.db`
  - SQL logging: `SQL_DEBUG=true|false`
- Notes:
  - `USE_NULLPOOL` is set early in `main.py` to avoid asyncpg pool issues
  - Logs written under `src/backend/src/logs/`

Frontend API base URL (`REACT_APP_API_URL`) at build-time:
```bash
# Option A: dev default already points to http://localhost:8000/api/v1
# Option B: override explicitly for a build (Unix/macOS)
REACT_APP_API_URL="http://localhost:8000/api/v1" npm run build

# When using the top-level build script:
# The env var will propagate into the "npm run build" it runs
cd src
REACT_APP_API_URL="http://localhost:8000/api/v1" python build.py
```

### Conventions
- Routers (`api/*`): Validate with `schemas/*`, delegate to `services/*`
- Services: Business logic only; use repositories for I/O
- Repositories: All SQL/external I/O; don’t leak ORM to services
- Models: SQLAlchemy in `models/*`; Schemas: Pydantic in `schemas/*`

### Add a new API resource (“widgets” example)
1) Model: `models/widget.py`; import in `db/all_models.py`
2) Schemas: `schemas/widget.py` (Create/Update/Read DTOs)
3) Repository: `repositories/widget_repository.py`
4) Service: `services/widget_service.py`
5) Router: `api/widgets_router.py` (validate → call service)
6) Register router in `api/__init__.py`
7) Frontend: add `src/frontend/src/api/widgets.ts` + components/views/state
8) Tests: `src/backend/tests/`

### Add a new CrewAI tool
- Implement under `engines/crewai/tools/` (follow existing patterns)
- Expose configuration via service/router if user-configurable
- Ensure discovery/registration in the execution path (e.g., prep or service)

### Executions and tracing
- Start executions via `executions_router.py` endpoints
- Services invoke engine flow (`engines/crewai/*`)
- Logs/traces:
  - Execution logs via `execution_logs_*`
  - Traces via `execution_trace_*`
```bash
# Kick off an execution
curl -X POST http://localhost:8000/api/v1/executions -H "Content-Type: application/json" -d '{...}'

# Get execution status
curl http://localhost:8000/api/v1/executions/<id>

# Fetch logs/trace
curl http://localhost:8000/api/v1/execution-logs/<id>
curl http://localhost:8000/api/v1/execution-trace/<job_id>
```

### Background processing
- Scheduler: starts on DB-ready startup (`scheduler_service.py`)
- Embedding queue (SQLite): `embedding_queue_service.py` batches writes
- Cleanup on startup/shutdown: `execution_cleanup_service.py`

### Database & migrations
- SQLite for quick local dev (`DATABASE_TYPE=sqlite`), Postgres for multi-user
- Alembic:
```bash
# after model changes
alembic revision --autogenerate -m "add widgets"
alembic upgrade head
```

### Auth, identity, tenancy
- Databricks headers parsed by `utils/user_context.py`
- Group-aware tenants; selected group passed in `group_id` header
- JWT/basic auth in `auth_router.py`, users in `users_router.py`
- Authorization checks in `core/permissions.py`

### Logging & debugging
- App logs: `src/backend/src/logs/` (managed by `core/logger.py`)
- Verbose SQL: `export SQL_DEBUG=true`
- SQLite “database is locked”: mitigated via retry/backoff; reduce writers or use Postgres

### Frontend notes
- Axios client and base URL:
```1:13:/Users/anshu.roy/Documents/kasal/src/frontend/src/config/api/ApiConfig.ts
export const config = {
  apiUrl:
    process.env.REACT_APP_API_URL ||
    (process.env.NODE_ENV === 'development'
      ? 'http://localhost:8000/api/v1'
      : '/api/v1'),
};
export const apiClient = axios.create({ baseURL: config.apiUrl, headers: { 'Content-Type': 'application/json' } });
```

### Testing
```bash
# Backend
python run_tests.py

# Frontend
cd src/frontend
npm test
```

### Production checklist
- Use Postgres (or managed DB), not SQLite
- Harden secrets/tokens; externalize (e.g., Databricks Secrets/Vault)
- Enforce TLS and CORS
- Monitor logs/traces; set alerts
- Review `DOCS_ENABLED`, `LOG_LEVEL`, `DEBUG_MODE`

## Resources

### Quick Links
- [API Playground](/api/docs)
- [Video Tutorials](https://kasal.ai/videos)
- [Discord Community](https://discord.gg/kasal)
- [Report Issues](https://github.com/kasal/issues)

### Code Examples
- [Basic Agent Setup](https://github.com/kasal/examples/basic)
- [Multi-Agent Collaboration](https://github.com/kasal/examples/multi-agent)
- [Custom Tools](https://github.com/kasal/examples/tools)
- [Production Deployment](https://github.com/kasal/examples/deploy)

### Support
- **Chat**: Available in-app 24/7
- **Email**: dev@kasal.ai
- **Slack**: #kasal-developers

---

*Build smarter, ship faster with Kasal*