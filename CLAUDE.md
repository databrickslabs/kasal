# CLAUDE.md

Project-wide instructions for Claude Code (claude.ai/code) when working with the Kasal codebase.

## Context Layering

Claude reads context from multiple CLAUDE.md files:
- **This file**: Project-wide patterns and rules
- **src/backend/CLAUDE.md**: Backend-specific instructions
- **src/frontend/CLAUDE.md**: Frontend-specific instructions

## Important Project Rules

### Documentation Location
- **ALWAYS create documentation in `src/docs/` directory**
- Do not create docs in the root `docs/` folder
- Frontend copies from `src/docs/` to `public/docs/` for display
- Follow existing documentation patterns and naming conventions

### Test Files Location
- **ALWAYS create test scripts and temporary files in `/tmp` folder**
- Do not create test files in the project directory
- Use paths like `/tmp/test_script.py` for testing
- This keeps the project directory clean

### Service Management
- **DO NOT restart backend or frontend services** - They are managed externally
- Backend uses `--reload` flag and auto-detects code changes
- Frontend uses hot module replacement (HMR) and auto-updates in browser
- Check service status: `ps aux | grep uvicorn` (backend) or `ps aux | grep "npm start"` (frontend)

### Code Quality Standards
- **CRITICAL: All operations must be async and non-blocking**
- **CRITICAL: Never include real URLs, endpoints, or addresses in code**
- Always use placeholder values like "https://example.com" or environment variables
- Follow clean architecture principles
- Never commit without running linting tools

### Build and Deploy
- **Build frontend static assets**: `python src/build.py`
- **Deploy application**: `python src/deploy.py`

## Architecture Overview

Kasal is an AI agent workflow orchestration platform with a **clean architecture pattern**:

**Frontend (React + TypeScript)** → **API (FastAPI)** → **Services** → **Repositories** → **Database**

### Technology Stack
- **Backend**: FastAPI + SQLAlchemy 2.0 + Alembic (Python 3.9+)
- **Frontend**: React 18 + TypeScript + Material-UI + ReactFlow
- **AI Engine**: CrewAI framework for agent orchestration
- **Database**: SQLite (dev) / PostgreSQL (prod)
- **Authentication**: JWT tokens with Databricks OAuth

### Project Structure
```
src/
├── backend/                  # FastAPI backend (see backend/CLAUDE.md)
│   ├── src/                 # Core application code
│   ├── tests/               # Unit and integration tests
│   └── migrations/          # Database migrations
├── frontend/                # React frontend (see frontend/CLAUDE.md)
│   └── src/                 # React application
└── frontend_static/         # Built frontend assets
```

## Development Workflow

### Quick Start
1. **Backend**: `cd src/backend && ./run.sh` (auto-reloads on changes)
2. **Frontend**: `cd src/frontend && npm start` (hot module replacement)
3. **Tests**: See respective CLAUDE.md files for testing commands

### Key Principles
- **Clean Architecture**: Separation of concerns across layers
- **Async-First**: All I/O operations must be async
- **Type Safety**: Strong typing in both backend (mypy) and frontend (TypeScript)
- **Test Coverage**: Minimum 80% for backend, comprehensive frontend testing

## Special Considerations

### Memory and Persistence
- CrewAI crews generate deterministic IDs for memory persistence
- Group isolation ensures tenant data separation
- Databricks Vector Search integration for advanced memory backends

### Model Integration
- Support for multiple LLM providers (Databricks, OpenAI, Anthropic, etc.)
- Model configurations in `src/backend/src/seeds/model_configs.py`
- Automatic handling of provider-specific requirements

### Databricks Apps Integration
- **When searching for Databricks Apps information, always check first**: https://apps-cookbook.dev/docs/streamlit/authentication/users_obo
- This reference covers authentication patterns and user on-behalf-of (OBO) flows

For detailed backend instructions, see: **src/backend/CLAUDE.md**
For detailed frontend instructions, see: **src/frontend/CLAUDE.md**
- always make sure whenever you develop anything you need to stick to service architecture pattern, and unit of work architecture pattern and repository architecture pattern.