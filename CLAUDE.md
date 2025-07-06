# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

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