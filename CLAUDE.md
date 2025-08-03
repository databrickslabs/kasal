# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Important Notes

### Databricks Model Serving
- Databricks offers various LLM models through their serving endpoints, including Claude models (e.g., `databricks-claude-sonnet-4`)
- Model names with `databricks-` prefix are valid Databricks serving endpoints
- Do NOT remove or modify the `databricks-` prefix from model names in the database or configuration files
- These models are accessed through Databricks' serving infrastructure, not directly from the original providers

## Commands

### Backend Development
- **Activate virtual environment**: `source venv/bin/activate` (from project root)
- **Start backend server**: `cd src/backend && ./run.sh` (defaults to PostgreSQL) or `./run.sh sqlite` for SQLite
- **Run tests**: `cd src/backend && python run_tests.py` (runs all tests with linting)
- **Run specific tests**: `python run_tests.py --type unit` or `python run_tests.py --type integration`
- **Run tests with coverage**: `python run_tests.py --coverage --html-coverage`
- **Run single test file**: `python -m pytest tests/unit/test_file.py -v`
- **Database migrations**: `cd src/backend && alembic upgrade head`
- **Create migration**: `cd src/backend && alembic revision --autogenerate -m "description"`
- **Seed database**: `cd src/backend && python run_seeders.py`

### Frontend Development
- **Install dependencies**: `cd src/frontend && npm install`
- **Start dev server**: `cd src/frontend && npm start` (http://localhost:3000)
- **Build for production**: `cd src/frontend && npm run build`
- **Run tests**: `cd src/frontend && npm test`
- **Lint code**: `cd src/frontend && npm run lint`
- **Type check**: `cd src/frontend && npm run tsc`

### Documentation Management
- **When adding new docs**: Copy `.md` files from `src/docs/` to `src/frontend/public/docs/` 
- **Update frontend**: Add new docs to `docSections` array in `src/frontend/src/components/Documentation/Documentation.tsx`

### Linting and Code Quality
- **Backend linting**: `cd src/backend && python -m black src tests && python -m isort src tests`
- **Backend type checking**: `cd src/backend && python -m mypy src`
- **Backend code formatting check**: `cd src/backend && python -m black --check src tests`

### Build and Deploy
- **Build frontend static assets**: `python src/build.py`
- **Deploy application**: `python src/deploy.py`

## Architecture Overview

Kasal is an AI agent workflow orchestration platform with a **clean architecture pattern**:

**Frontend (React + TypeScript)** → **API (FastAPI)** → **Services** → **Repositories** → **Database**

### Backend Architecture (FastAPI + SQLAlchemy)
- **Clean Architecture**: Repository pattern, Unit of Work, Service layer, Dependency Injection
- **Database**: SQLAlchemy 2.0 with Alembic migrations (SQLite for dev, PostgreSQL for prod)
- **AI Engine**: CrewAI framework integration for agent orchestration
- **Authentication**: JWT tokens with Databricks OAuth support
- **Testing**: pytest with 80%+ coverage requirement

### Frontend Architecture (React + TypeScript)
- **State Management**: Zustand stores (migrated from Redux)
- **UI Library**: Material-UI (MUI)
- **Workflow Editor**: ReactFlow for visual workflow designer
- **API Client**: Axios for HTTP requests

### Key Directories
```
src/
├── backend/                  # FastAPI backend
│   ├── src/
│   │   ├── api/             # FastAPI route handlers
│   │   ├── core/            # Dependencies, logging, UOW
│   │   ├── models/          # SQLAlchemy database models
│   │   ├── schemas/         # Pydantic validation schemas
│   │   ├── services/        # Business logic layer
│   │   ├── repositories/    # Data access layer
│   │   ├── engines/crewai/  # CrewAI engine implementation
│   │   └── main.py          # Application entry point
│   ├── tests/               # Unit and integration tests
│   └── migrations/          # Alembic database migrations
├── frontend/                # React frontend
│   └── src/
│       ├── components/      # UI components by feature
│       ├── store/           # Zustand state management
│       ├── api/             # API service layer
│       └── types/           # TypeScript definitions
└── frontend_static/         # Built frontend assets for deployment
```

## Development Patterns

### Frontend Patterns
- **API Configuration**: All frontend services must use `apiClient` from `src/frontend/src/config/api/ApiConfig.ts` for backend communication
- **Service Layer**: Frontend services should use static methods and `apiClient` for HTTP requests (not the legacy `ApiService`)
- **TypeScript**: Strong typing for all API responses and requests using generic types like `apiClient.get<ResponseType>()`
- **Component State**: Use Zustand stores for global state management

### Backend Patterns
- **Repository Pattern**: All database access goes through repositories
- **Unit of Work**: Transaction management for complex operations
- **Service Layer**: Business logic orchestration between repositories
- **Dependency Injection**: Use FastAPI's built-in DI system
- **Async/Await**: All database operations are async

### Database Changes
- Always create Alembic migrations for model changes
- Use `alembic revision --autogenerate` to create migrations
- Test migrations on SQLite before applying to PostgreSQL

### Testing Requirements
- Backend tests require 80%+ coverage
- Write both unit tests (mocking dependencies) and integration tests
- Use pytest fixtures for test setup
- Test all API endpoints with different scenarios

### Code Quality
- Backend uses Black, isort, mypy, and flake8
- All code must pass type checking with mypy
- Follow clean architecture principles
- Never commit without running linting tools
- **CRITICAL: All operations must be async and non-blocking** - Never use sync operations that could block the event loop
- **CRITICAL: Never include real URLs, endpoints, or addresses in code** - Always use placeholder values like "https://example.com" or environment variables

## AI Engine Integration

The CrewAI engine integrates at the service layer:
- **Engine Service**: Main orchestration service
- **Configuration Adapter**: Transforms frontend configs to CrewAI format
- **Execution Runner**: Manages async execution workflows
- **Tool Factory**: Extensible tool system with custom tools

## Environment Setup

### Backend Environment
- Python 3.9+ required
- Uses Poetry for dependency management (see pyproject.toml)
- Database type controlled by environment variables
- API keys for LLM services configured via environment

### Frontend Environment
- Node.js 16+ required
- React 18 with TypeScript
- Material-UI for components
- ReactFlow for workflow visualization

## Testing Strategy

### Backend Testing
- **Unit Tests**: Mock all external dependencies
- **Integration Tests**: Test full request/response cycle
- **Coverage**: Minimum 80% with HTML reports
- **Database**: Uses separate test database

### Frontend Testing
- **Component Tests**: React Testing Library
- **Hook Tests**: Custom hooks testing
- **E2E Tests**: Cypress for user workflows

## Important Notes

### Service Management
- **DO NOT restart backend or frontend services** - They are managed externally and automatically reload when code changes
- The backend uses `--reload` flag and will automatically detect and apply code changes
- The frontend uses hot module replacement (HMR) and will automatically update in the browser
- If you need to check if services are running, use `ps aux | grep uvicorn` for backend or `ps aux | grep "npm start"` for frontend

### Event Loop and Database Connection Management
- When dealing with asyncpg and multiple event loops (e.g., in CrewAI memory backends), asyncpg connections can conflict
- The error "got result for unknown protocol state 3" or "attached to a different loop" indicates event loop conflicts
- Solutions implemented:
  - `USE_NULLPOOL=true` environment variable disables connection pooling when needed
  - CrewAIDatabricksWrapper automatically handles this for embedding generation
  - Custom embedder functions are called directly without creating new event loops
  - For testing with pytest-asyncio, use function-scoped fixtures instead of session-scoped ones
- This ensures database connections work properly across different async contexts

### Databricks Authentication
- **Enhanced Authentication Fallback**: The empty_index operation now uses a comprehensive authentication hierarchy:
  1. OBO (On-Behalf-Of) authentication using user token
  2. OAuth client credentials using DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET
  3. API key from service (DATABRICKS_TOKEN or DATABRICKS_API_KEY)
  4. Environment variables as last resort
- This ensures operations work in various deployment scenarios

### Memory Backend Configuration
- **Disabled Configuration**: A memory backend configuration with all memory types disabled (enable_short_term=false, enable_long_term=false, enable_entity=false) is treated as a "Disabled Configuration"
  - This is used to disable Databricks Vector Search memory backend
  - When a disabled configuration is found, the system ignores it and falls back to the default ChromaDB + SQLite memory
  - This allows users to disable Databricks memory without affecting the default memory functionality
  - The default memory will create storage in `/Library/Application Support/kasal_default_[crew_id]/`

### Databricks Vector Search Limitations
- **Empty Index Operation**: Databricks Vector Search doesn't support bulk delete operations
  - The `delete()` method requires specific primary keys
  - To empty an index, we first retrieve all vectors (up to 10,000) and then delete them
  - For larger indexes, consider dropping and recreating the index instead

### Crew Memory Persistence
- **Deterministic Crew ID Generation**: CrewAI crews now generate consistent IDs based on their configuration
  - The crew_id is generated using a hash of: agent roles, task names, crew name, model, run_name, and **group_id**
  - This ensures the same crew configuration gets the same ID across multiple runs
  - Memory (short-term, long-term, and entity) persists across runs for the same crew
  - Different crews or modified configurations get different IDs to avoid memory conflicts
  - **Security**: The group_id is included in the hash to ensure complete tenant isolation - different groups will have completely separate memories even with identical crew configurations
- **Memory Backend Integration**: Databricks Vector Search is fully integrated for all memory types
  - Entity memory schema correctly maps to Databricks index fields
  - All memory operations are properly logged for debugging
  - Each group's memories are isolated by the crew_id hash that includes their group_id