# Kasal Solution Architecture

> **Enterprise AI Orchestration Platform** - Scalable, secure, cloud-native

---

## System Overview

### Platform Vision
**Transform business workflows with autonomous AI agents** - Zero infrastructure complexity

### Architecture Principles
| Principle | Implementation |
|-----------|---------------|
| **Async-First** | Non-blocking I/O everywhere |
| **Microservices-Ready** | Clean boundaries, API contracts |
| **Zero-Trust Security** | Every request authenticated |
| **Infinite Scale** | Horizontal scaling, stateless |
| **Multi-Tenant** | Complete data isolation |

---

## High-Level Architecture

```mermaid
graph TB
    subgraph "Client Layer"
        WEB[React SPA]
        API[REST API]
        WS[WebSocket]
    end

    subgraph "Application Layer"
        GW[API Gateway]
        AUTH[Auth Service]
        ORCH[Orchestration Service]
        EXEC[Execution Service]
    end

    subgraph "AI Layer"
        CREW[CrewAI Engine]
        LLM[LLM Gateway]
        MEM[Memory Service]
        TOOLS[Tool Registry]
    end

    subgraph "Data Layer"
        PG[(PostgreSQL)]
        REDIS[(Redis)]
        VECTOR[(Vector DB)]
        S3[(Object Storage)]
    end

    WEB --> GW
    API --> GW
    WS --> GW
    GW --> AUTH
    GW --> ORCH
    ORCH --> CREW
    CREW --> LLM
    CREW --> MEM
    CREW --> TOOLS
    ORCH --> EXEC
    EXEC --> PG
    MEM --> VECTOR
    EXEC --> REDIS
    TOOLS --> S3
```

---

## Architecture Pattern

### High-level
- Layered architecture:
  - Frontend (React SPA) → API (FastAPI) → Services → Repositories → Database
- Async-first (async SQLAlchemy, background tasks, queues)
- Config via environment (`src/backend/src/config/settings.py`)
- Pluggable orchestration engine (`src/backend/src/engines/` with CrewAI)

### Request lifecycle (CRUD path)
1) Router in `api/` receives request, validates using `schemas/`
2) Router calls `services/` for business logic
3) Service uses `repositories/` for DB/external I/O
4) Data persisted via `db/session.py`
5) Response serialized with Pydantic schemas

### Orchestration lifecycle (AI execution)
- Entry via `executions_router.py` → `execution_service.py`
- Service prepares agents/tools/memory and selects engine (`engines/engine_factory.py`)
- CrewAI path:
  - Prep: `engines/crewai/crew_preparation.py` and `flow_preparation.py`
  - Run: `engines/crewai/execution_runner.py` with callbacks/guardrails
  - Observability: `execution_logs_service.py`, `execution_trace_service.py`
- Persist status/history: `execution_repository.py`, `execution_history_repository.py`

### Background processing
- Scheduler at startup: `scheduler_service.py`
- Embedding queue (SQLite): `embedding_queue_service.py` (batches writes)
- Startup/shutdown cleanup: `execution_cleanup_service.py`

### Data modeling
- ORM in `models/*` mirrors `schemas/*`
- Repositories encapsulate all SQL/external calls (Databricks APIs, Vector Search, MLflow)
- `db/session.py`:
  - Async engine and session factory
  - SQLite lock retry w/ backoff
  - Optional SQL logging via `SQL_DEBUG=true`

### Auth, identity, and tenancy
- Databricks Apps headers parsed by `utils/user_context.py`
- Group-aware multi-tenant context propagated via middleware
- JWT/basic auth routes in `auth_router.py`, users in `users_router.py`
- Authorization checks in `core/permissions.py`


### Security Controls
| Layer | Control | Implementation |
|-------|---------|----------------|
| **Network** | TLS 1.3 | End-to-end encryption |
| **API** | OAuth 2.0 | Databricks SSO |
| **Data** | AES-256 | Encryption at rest |
| **Secrets** | Vault | HashiCorp Vault |
| **Compliance** | SOC2 | Audit trails |

---

### Storage Strategy
| Data Type | Storage | Purpose |
|-----------|---------|---------|
| **Transactional** | PostgreSQL | ACID compliance |
| **Session** | Redis | Fast cache |
| **Vectors** | Databricks Vector | Semantic search |
| **Files** | S3/Azure Blob | Document storage |
| **Logs** | CloudWatch/Datadog | Observability |

---

### Observability
- Central log manager: `core/logger.py` (writes to `LOG_DIR`)
- API/SQL logging toggles (`LOG_LEVEL`, `SQL_DEBUG`)
- Execution logs/traces persisted and queryable via dedicated routes/services

### Configuration flags (selected)
- `DOCS_ENABLED`: enables `/api-docs`, `/api-redoc`, `/api-openapi.json`
- `AUTO_SEED_DATABASE`: async background seeders post DB init
- `DATABASE_TYPE`: `postgres` (default) or `sqlite` with `SQLITE_DB_PATH`



*Architected for scale, built for the future*