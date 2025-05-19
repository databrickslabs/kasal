# Modern Python Backend

A modern, scalable backend architecture using FastAPI, SQLAlchemy, and best practices for 2025.

## Architecture Overview

This project follows a clean architecture pattern with clear separation of concerns:

- **API Layer**: FastAPI routes and controllers
- **Service Layer**: Business logic implementation
- **Repository Layer**: Data access abstraction
- **DB Layer**: Database models and connectivity
- **Schema Layer**: Data validation and serialization (Pydantic models)

## Features

- ✅ FastAPI with async support
- ✅ SQLAlchemy 2.0 with async support
- ✅ Clean architecture with dependency injection
- ✅ Repository pattern for data access
- ✅ Unit of Work pattern for transactions
- ✅ Environment-based configuration
- ✅ Comprehensive test setup
- ✅ Modern typing and code quality tools
- ✅ Database seeding for predefined data

## Project Structure

```
backend/
├── src/                 # Application source code
│   ├── api/             # API routes and controllers
│   │   ├── __init__.py  # Router registration
│   │   └── {domain}_router.py  # Domain-specific route handlers
│   ├── core/            # Core application components
│   ├── db/              # Database setup and session management
│   ├── models/          # SQLAlchemy models
│   ├── repositories/    # Repository pattern implementations
│   │   ├── base_repository.py  # Base repository with common operations
│   │   └── {domain}_repository.py  # Domain-specific repositories
│   ├── schemas/         # Pydantic models for validation
│   │   ├── __init__.py  # Re-exports important schemas
│   │   ├── base.py      # Common base schemas
│   │   └── {domain}/    # Domain-specific schemas
│   ├── services/        # Business logic services
│   │   └── {domain}_service.py  # Domain-specific services
│   ├── seeds/           # Database seeders for populating initial data
│   ├── config/          # Configuration management
│   ├── utils/           # Utility functions
│   ├── __init__.py
│   └── main.py          # Application entry point
├── tests/               # Test suite
│   ├── integration/     # Integration tests
│   └── unit/            # Unit tests
├── docs/                # Project documentation
│   ├── ARCHITECTURE.md  # Architectural design details
│   ├── BEST_PRACTICES.md # Coding guidelines
│   ├── MODELS.md        # Database model documentation
│   ├── SCHEMAS.md       # Schema documentation
│   ├── REPOSITORY_PATTERN.md # Repository pattern documentation
│   └── README.md        # Documentation index
├── pyproject.toml       # Project dependencies and settings
└── README.md            # Project documentation
```

### Organization Principles

Our codebase follows these organizational principles:

1. **Domain-based Structure**: Files are organized by domain (e.g., executions, agents)
2. **Consolidated Services**: Each domain has a single service file handling all related operations
3. **Dedicated Repositories**: Each domain has a repository handling data access operations
4. **Consistent Naming**: Files follow consistent naming patterns (`{domain}_service.py`, `{domain}_repository.py`)
5. **Single Responsibility**: Each component focuses on a specific domain area
6. **Layered Architecture**: Clear separation between API, service, repository, and data layers

## Getting Started

### Prerequisites

- Python 3.9 or higher
- Poetry for dependency management

### Installation

1. Clone this repository
2. Install dependencies:
   ```
   cd backend
   poetry install
   ```

### Running the Application

```bash
# Development server
poetry run uvicorn src.main:app --reload

# Production server
poetry run uvicorn src.main:app --host 0.0.0.0 --port 8000
```

### Database Migrations

Basic commands for working with database migrations:

```bash
# Initialize Alembic (first time only)
poetry run alembic init migrations

# Create a migration
poetry run alembic revision --autogenerate -m "description"

# Run migrations
poetry run alembic upgrade head
```

For a comprehensive guide on database migrations, including best practices, troubleshooting, and real-world examples, see [Database Migrations Guide](DATABASE_MIGRATIONS.md).

### Database Seeding

The application automatically seeds the database with predefined data during startup. This includes tool configurations, schemas, prompt templates, and model configurations.

By default, automatic seeding is enabled and occurs during the application's startup lifecycle in the FastAPI lifespan context manager.

For manual seeding or development needs:

```bash
# Run all seeders
poetry run python -m src.seeds.seed_runner --all

# Run specific seeders
poetry run python -m src.seeds.seed_runner --tools
poetry run python -m src.seeds.seed_runner --schemas
poetry run python -m src.seeds.seed_runner --prompt_templates
poetry run python -m src.seeds.seed_runner --model_configs

# Run with debug mode
poetry run python -m src.seeds.seed_runner --all --debug
```

You can disable automatic seeding by setting:
```
AUTO_SEED_DATABASE=false
```

For more details, see [Database Seeding](docs/DATABASE_SEEDING.md).

### Testing

```bash
# Run all tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=src
```

## Best Practices

This project implements the following best practices:

1. **Clean Architecture**: Separation of concerns with layers
2. **Repository Pattern**: Abstract database access
3. **Unit of Work Pattern**: Handle transactions
4. **Dependency Injection**: Manage dependencies cleanly
5. **Async First**: Utilize async/await for better performance
6. **Type Hints**: Comprehensive typing for better code quality
7. **Testing**: Unit and integration tests for reliable code
8. **Data Seeding**: Idempotent database population with predefined data
