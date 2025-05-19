# Modern Python Backend Documentation

This document serves as an index to all documentation files in this project, providing an overview of the modern Python backend architecture and how each component fits together.

## Documentation Overview

| Document | Description |
|----------|-------------|
| [README.md](README.md) | Project overview, features, and setup instructions |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Detailed explanation of architectural patterns and layers |
| [BEST_PRACTICES.md](BEST_PRACTICES.md) | Coding guidelines and best practices |
| [MODELS.md](MODELS.md) | Documentation for SQLAlchemy database models |
| [SCHEMAS.md](SCHEMAS.md) | Documentation for Pydantic validation schemas |
| [SCHEMAS_STRUCTURE.md](SCHEMAS_STRUCTURE.md) | Detailed guide to schemas folder structure and organization |
| [LOGGING.md](LOGGING.md) | Comprehensive guide to the logging system |
| [GETTING_STARTED.md](GETTING_STARTED.md) | Step-by-step guide for developers |
| [REPOSITORY_PATTERN.md](REPOSITORY_PATTERN.md) | Detailed explanation of the Repository Pattern implementation |

## Architecture Layers

Our backend follows a clean, layered architecture with clear separation of concerns:

```
┌─────────────────┐
│    API Layer    │ FastAPI Routes
└─────────┬───────┘
          │
          ▼
┌─────────────────┐
│  Service Layer  │ Business Logic
└─────────┬───────┘
          │
          ▼
┌─────────────────┐
│ Repository Layer│ Data Access
└─────────┬───────┘
          │
          ▼
┌─────────────────┐
│ Database Layer  │ Models & Connection
└─────────────────┘
```

Each layer has specific responsibilities and communicates only with adjacent layers.

## Component Relationships

### How Everything Fits Together

1. **API Layer** (FastAPI Routes)
   - Uses **Schemas** for request/response validation
   - Calls **Services** to execute business logic
   - Handles HTTP-specific concerns

2. **Service Layer** (Business Logic)
   - Implements business rules and workflows
   - Uses **Repositories** for data access
   - Coordinates operations across multiple repositories
   - Transforms between **Schemas** and **Models**

3. **Repository Layer** (Data Access)
   - Uses **Models** to interact with database
   - Abstracts database operations (CRUD)
   - Handles query building and execution

4. **Database Layer** (SQLAlchemy)
   - Defines **Models** that map to database tables
   - Manages database connections and sessions
   - Handles migrations and schema changes (see [Database Migrations Guide](DATABASE_MIGRATIONS.md))

### Component Details

#### Models vs. Schemas

- **Models** (SQLAlchemy)
  - Represent database tables
  - Define relationships between entities
  - Handle persistence concerns
  - Located in `src/models/`

- **Schemas** (Pydantic)
  - Validate incoming/outgoing data
  - Document API contracts
  - Transform data between layers
  - Located in `src/schemas/`
  - Organized by domain with re-exports for clean imports
  - Includes base, create, update, and response schema types

#### Design Patterns

The architecture implements several design patterns:

- **Repository Pattern**: Abstracts data access with `BaseRepository`
- **Unit of Work Pattern**: Manages transactions with `UnitOfWork`
- **Dependency Injection**: Provides dependencies to routes via FastAPI's dependency system
- **Service Layer Pattern**: Encapsulates business logic in services
- **Consolidated Services**: Groups related functionality within a domain into a cohesive service

## Service Consolidation

To improve maintainability and reduce code fragmentation, related services handling the same domain entities are consolidated. This approach follows the Single Responsibility Principle at the domain level rather than at the function level.

### Examples of Consolidated Services

- **ExecutionService**: Handles all execution-related operations including:
  - Creating and running executions
  - Tracking execution status
  - Generating descriptive names for executions
  - Managing execution metadata

This replaces separate services like `ExecutionRunnerService` and simplifies dependencies.

### Consolidated Routers

Similarly, related API endpoints are grouped into consolidated router files:

- **executions_router.py**: All execution-related endpoints including:
  - Creating executions
  - Retrieving execution status
  - Listing executions
  - Generating execution names

This approach simplifies API structure and improves discoverability.

## Directory Structure

```
backend/
├── src/                 # Application source code
│   ├── api/             # API routes and controllers
│   ├── core/            # Core components (base classes, dependency injection)
│   ├── db/              # Database setup and session management
│   ├── models/          # SQLAlchemy models
│   ├── repositories/    # Repository pattern implementations
│   ├── schemas/         # Pydantic models for validation
│   │   ├── __init__.py  # Re-exports important schemas
│   │   ├── base.py      # Common base schemas
│   │   ├── {domain}/    # Domain-specific schemas
│   │   └── common/      # Shared schemas (pagination, etc.)
│   ├── services/        # Business logic services
│   ├── config/          # Configuration management
│   │   └── logging.py   # Logging configuration
│   ├── utils/           # Utility functions
│   └── main.py          # Application entry point
├── tests/               # Test suite
│   ├── integration/     # Integration tests
│   └── unit/            # Unit tests
├── migrations/          # Alembic migration scripts
├── logs/                # Application log files (excluded from version control)
├── pyproject.toml       # Dependencies and build settings
└── docs/                # Documentation files
```

## Logging Strategy

The application uses a structured logging approach:

- **Configuration**: Defined in `src/config/logging.py` 
- **Log Files**: Stored in the top-level `logs/` directory (excluded from version control)
- **Log Levels**: DEBUG, INFO, WARNING, ERROR, CRITICAL based on environment
- **Naming Convention**: `{service_name}.{log_level}.{date}.log`
- **Handlers**:
  - File handlers for persistent logs
  - Console handlers for development
  - Optional external service integration (e.g., ELK, Datadog)

This approach keeps runtime artifacts separate from source code while maintaining configuration within the application structure.

## Data Flow

To illustrate how data flows through the system, consider a typical request lifecycle:

1. **HTTP Request** → FastAPI parses and validates using **Schemas**
2. **API Route** → Calls appropriate **Service** with validated data
3. **Service** → Applies business logic, calls **Repository** methods
4. **Repository** → Executes database operations using **Models**
5. **Database** → Returns results to Repository
6. **Repository** → Returns data to Service
7. **Service** → Processes data, returns to API Route
8. **API Route** → Converts to response **Schema**
9. **HTTP Response** → Returned to client

### Real Example: Creating an Execution

Let's walk through how creating an execution works in our system:

1. **Client** → Sends POST request to `/executions` with execution configuration
2. **FastAPI** → Validates request data using `CrewConfig` schema
3. **API Route** → Creates `ExecutionService` instance and calls `create_execution()`
4. **ExecutionService** → Generates execution ID, prepares execution data
5. **ExecutionService** → Calls `ExecutionRepository.create_execution()` with prepared data
6. **ExecutionRepository** → Creates database record, commits transaction
7. **ExecutionService** → Schedules background task for execution
8. **API Route** → Returns `ExecutionCreateResponse` to client
9. **Background** → Execution runs asynchronously, updating status via repository

## Key Design Principles

1. **Separation of Concerns**: Each layer has specific responsibilities
2. **Dependency Inversion**: Higher layers depend on abstractions, not implementations
3. **Single Responsibility**: Each component has one reason to change (at the domain level)
4. **Don't Repeat Yourself**: Common functionality is abstracted and reused
5. **Explicit is Better Than Implicit**: Clear, readable code over magic
6. **Cohesion**: Related functionality is grouped together

## Getting Started

For new developers, we recommend the following reading order:

1. [README.md](README.md) for project overview
2. [GETTING_STARTED.md](GETTING_STARTED.md) for setup and first steps
3. [ARCHITECTURE.md](ARCHITECTURE.md) for understanding the system design
4. [MODELS.md](MODELS.md) and [SCHEMAS.md](SCHEMAS.md) for data handling
5. [SCHEMAS_STRUCTURE.md](SCHEMAS_STRUCTURE.md) for schema organization
6. [DATABASE_MIGRATIONS.md](DATABASE_MIGRATIONS.md) for working with database schema changes
7. [BEST_PRACTICES.md](BEST_PRACTICES.md) for coding guidelines

## Conclusion

This documentation is designed to help developers understand and contribute to the codebase effectively. Each document focuses on a specific aspect of the architecture, but together they provide a comprehensive guide to our modern Python backend design.

If you find any documentation gaps or have suggestions for improvements, please create an issue or pull request. 