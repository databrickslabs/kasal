# Kasal Documentation Index

This document serves as an index to all documentation files in Kasal, providing navigation to key architectural and operational documentation.

## Core Documentation

| Document | Description |
|----------|-------------|
| [README.md](../README.md) | Project overview and deployment options |
| [GETTING_STARTED.md](GETTING_STARTED.md) | Setup instructions for development |
| [ARCHITECTURE.md](ARCHITECTURE.md) | System architecture and design patterns |
| [CREWAI_ENGINE.md](CREWAI_ENGINE.md) | AI agent orchestration engine |
| [BEST_PRACTICES.md](BEST_PRACTICES.md) | Development guidelines |
| [DATABASE_SEEDING.md](DATABASE_SEEDING.md) | Database initialization and seeding |
| [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md) | Databricks Apps deployment |
| [SHORTCUTS.md](SHORTCUTS.md) | Keyboard shortcuts reference |

## Kasal System Overview

Kasal is an AI agent workflow orchestration platform with the following key components:

- **Backend**: FastAPI service with CrewAI engine integration
- **Database**: SQLite (dev) / PostgreSQL (prod) with automated seeding
- **Deployment**: Databricks Apps with OAuth integration
- **AI Engines**: CrewAI framework with multi-LLM support

## Technical Documentation

### Backend Development
- [AUTHORIZATION.md](AUTHORIZATION.md) - Databricks OAuth and group-based access
- [SECURITY_MODEL.md](SECURITY_MODEL.md) - Security architecture
- [LOGGING.md](LOGGING.md) - Logging configuration
- [DATABASE_MIGRATIONS.md](DATABASE_MIGRATIONS.md) - Schema management
- [API.md](API.md) - REST API endpoints and usage

## Development Workflows

### Getting Started
1. [GETTING_STARTED.md](GETTING_STARTED.md) - Development environment setup
2. [DATABASE_SEEDING.md](DATABASE_SEEDING.md) - Database initialization

### AI Agent Development
1. Create agents via API endpoints
2. Configure LLM models and tools
3. Build crews and workflows
4. Execute and monitor agent performance via API

### Deployment
1. [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md) - Databricks Apps deployment
2. Configure OAuth scopes in Databricks UI
3. Set environment variables for production

## AI Engine Integration

Kasal integrates with the CrewAI framework for autonomous AI agents:

- **Engine Types**: CrewAI crews and flows (experimental)
- **LLM Providers**: OpenAI, Anthropic, DeepSeek, Ollama, Databricks
- **Tools**: Built-in CrewAI tools + custom Kasal tools (Genie, Python PPTX, etc.)
- **Monitoring**: Real-time execution traces and logging

## Quick Reference

### Key Directories
- `backend/src/engines/crewai/` - AI engine implementation
- `backend/src/api/` - FastAPI route definitions
- `backend/src/services/` - Business logic
- `backend/src/models/` - Database models
- `backend/src/repositories/` - Data access layer
- `docs/` - Documentation files

### Important Files
- `backend/src/main.py` - FastAPI application entry point
- `deploy.py` - Databricks Apps deployment script
- `entrypoint.py` - Production entry point

## Getting Started Path

For new developers:

1. Read [README.md](../README.md) for project overview
2. Follow [GETTING_STARTED.md](GETTING_STARTED.md) for setup
3. Review [ARCHITECTURE.md](ARCHITECTURE.md) for system design
4. Check [CREWAI_ENGINE.md](CREWAI_ENGINE.md) for AI engine details
5. Consult [BEST_PRACTICES.md](BEST_PRACTICES.md) for coding guidelines

## Contributing

This documentation is maintained alongside the codebase. Update documentation when making changes to reflect current implementation. 