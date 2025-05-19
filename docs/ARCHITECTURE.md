# Modern Python Backend Architecture

This document provides a comprehensive guide to the architecture and design patterns used in our modern Python backend, built with FastAPI and SQLAlchemy 2.0.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Design Patterns](#design-patterns)
- [Layers and Responsibilities](#layers-and-responsibilities)
- [Dependency Injection](#dependency-injection)
- [Database Access](#database-access)
- [Database Seeding](#database-seeding)
- [API Development](#api-development)
- [Error Handling](#error-handling)
- [Testing](#testing)
- [Security Best Practices](#security-best-practices)
- [Performance Optimization](#performance-optimization)
- [Service Consolidation](#service-consolidation)

## Architecture Overview

This backend follows a layered architecture pattern with clear separation of concerns, promoting maintainability, testability, and scalability. The architecture is inspired by Domain-Driven Design (DDD) principles and Clean Architecture.

```
┌─────────────────┐
│                 │
│    API Layer    │ FastAPI Routes
│                 │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│                 │
│  Service Layer  │ Business Logic
│                 │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│                 │
│ Repository Layer│ Data Access 
│                 │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│                 │
│ Database Layer  │ SQLAlchemy Models
│                 │
└─────────────────┘
```

### Key Features

- **Fully Asynchronous**: Uses Python's async/await throughout all layers for maximum performance
- **Type Safety**: Comprehensive type hints for improved code quality and IDE support
- **Clean Separation**: Each layer has distinct responsibilities with well-defined interfaces
- **Dependency Injection**: Ensures loose coupling and easier testing
- **Repository Pattern**: Abstracts database access logic
- **Unit of Work Pattern**: Manages database transactions consistently
- **Database Seeding**: Idempotent population of predefined data
- **Consolidated Services**: Related functionality grouped in cohesive service modules

## Design Patterns

### Repository Pattern

The Repository Pattern abstracts data access logic, providing a collection-like interface for domain objects.

**Benefits:**
- Centralizes data access logic
- Decouples business logic from data access details
- Makes testing easier through mocking
- Simplifies switching data sources or ORM if needed

Example:
```python
class ExecutionRepository(BaseRepository):
    async def get_execution_by_job_id(self, job_id: str) -> Optional[Execution]:
        query = select(self.model).where(self.model.job_id == job_id)
        result = await self.session.execute(query)
        return result.scalars().first()
        
    async def create_execution(self, data: Dict[str, Any]) -> Execution:
        execution = self.model(**data)
        self.session.add(execution)
        await self.session.commit()
        await self.session.refresh(execution)
        return execution
```

### Unit of Work Pattern

The Unit of Work pattern manages database transactions and ensures consistency.

**Benefits:**
- Maintains database integrity
- Simplifies transaction management
- Groups related operations
- Ensures proper commit/rollback behavior

Example:
```python
async with UnitOfWork() as uow:
    item = await item_service.create(uow, item_data)
    # Transaction is automatically committed on exit
    # or rolled back on exception
```

### Service Layer

The Service Layer implements business logic, orchestrating operations using repositories.

**Benefits:**
- Centralizes business rules and workflows
- Coordinates across multiple repositories
- Enforces domain constraints
- Provides a clear API for the controllers/routes

Example:
```python
class ExecutionService:
    def __init__(self, db: AsyncSession):
        self.db = db
        
    async def create_execution(self, config: CrewConfig, background_tasks = None) -> Dict[str, Any]:
        # Implementation for creating a new execution
        execution_id = ExecutionService.create_execution_id()
        # ... service implementation details
        return result
```

## Layers and Responsibilities

### API Layer (FastAPI Routes)

The API layer is responsible for handling HTTP requests and responses. It's implemented using FastAPI routes.

**Responsibilities:**
- Request validation
- Route definitions
- Parameter parsing
- Response formatting
- HTTP status codes
- Authentication/Authorization checks
- Documentation

Example:
```python
@router.post("", response_model=ExecutionCreateResponse)
async def create_execution(
    config: CrewConfig,
    background_tasks: BackgroundTasks, 
    db: AsyncSession = Depends(get_db)
):
    execution_service = ExecutionService(db)
    result = await execution_service.create_execution(
        config=config,
        background_tasks=background_tasks
    )
    return ExecutionCreateResponse(**result)
```

### Service Layer

The service layer contains business logic and orchestrates operations.

**Responsibilities:**
- Implementing business rules
- Orchestrating repositories
- Transaction management
- Domain logic
- Input validation
- Business-specific validation

### Repository Layer

The repository layer abstracts data access operations.

**Responsibilities:**
- Data access operations (CRUD)
- Query building
- Custom query methods
- Database-specific implementations
- Mapping between database models and domain models

### Database Layer

The database layer defines the data models and database connection.

**Responsibilities:**
- Database connection management
- Model definitions
- Schema migrations
- Database constraints and relationships

### Seeds Layer

The seeds layer provides functionality for populating the database with predefined data.

**Responsibilities:**
- Defining default data for tables
- Idempotent insertion of records
- Supporting both development and production environments
- Ensuring data consistency across deployments

## Dependency Injection

FastAPI's dependency injection system is used throughout the application to provide:

- Database sessions
- Repositories
- Services
- Configuration
- Authentication

Benefits:
- Looser coupling between components
- Easier testing through mocking
- Cleaner code with less boilerplate
- Better separation of concerns

Example:
```python
def get_service(
    service_class: Type[BaseService],
    repository_class: Type[BaseRepository],
    model_class: Type[Base],
) -> Callable[[UOWDep], BaseService]:
    def _get_service(uow: UOWDep) -> BaseService:
        return service_class(repository_class, model_class, uow)
    return _get_service

# Usage:
get_item_service = get_service(ItemService, ItemRepository, Item)

@router.get("/{item_id}")
async def read_item(
    item_id: int,
    service: Annotated[ItemService, Depends(get_item_service)],
):
    # Use service here
```

## Database Access

Database access is built on SQLAlchemy 2.0 with asynchronous support.

**Key Components:**
- `AsyncSession`: Asynchronous database session for non-blocking database access
- `Base`: SQLAlchemy declarative base class for database models
- `Migrations`: Alembic for database schema migrations
- `UnitOfWork`: Pattern for transaction management

**Best Practices:**
- Use async/await for database operations
- Define explicit relationships between models
- Use migrations for schema changes

## Database Seeding

The application includes a database seeding system to populate tables with predefined data.

**Key Components:**
- `Seeders`: Modular components for populating specific tables
- `Seed Runner`: Utility for running seeders individually or as a group
- `Auto-Seeding`: Optional functionality to seed on application startup

**Architecture:**
```
┌─────────────────┐
│                 │
│   Seed Runner   │ Command-line interface
│                 │
└────────┬────────┘
         │
         ▼
┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │
│  Tools Seeder   │     │ Schemas Seeder  │
│                 │     │                 │
└────────┬────────┘     └────────┬────────┘
         │                       │
         ▼                       ▼
┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │
│ Templates Seeder│     │ ModelConfig Seeder
│                 │     │                 │
└────────┬────────┘     └────────┬────────┘
         │                       │
         │                       │
         ▼                       ▼
      ┌─────────────────────────────┐
      │                             │
      │         Database            │
      │                             │
      └─────────────────────────────┘
```

**Best Practices:**
- Make seeders idempotent (can be run multiple times)
- Check for existing records before inserting
- Use proper transactions for data consistency
- Split large datasets into logical modules
- Include both async and sync implementations
- Use UTC timestamps for created_at and updated_at fields

For more details, see [Database Seeding](DATABASE_SEEDING.md).

## API Development

APIs are built using FastAPI with a focus on RESTful design.

**Best Practices:**
- Use proper HTTP methods (GET, POST, PUT, PATCH, DELETE)
- Return appropriate status codes
- Validate input with Pydantic models
- Document APIs with docstrings
- Use path parameters for resource identifiers
- Use query parameters for filtering and pagination
- Implement proper error handling

## Error Handling

Errors are handled consistently across the application:

- **HTTPExceptions**: For API errors with proper status codes
- **Custom Exceptions**: For domain-specific errors
- **Validation Errors**: Handled by Pydantic and FastAPI

Error responses follow a consistent format:
```json
{
  "detail": "Error message"
}
```

## Testing

The application is designed to be testable at all layers:

- **Unit Tests**: Testing individual components in isolation
- **Integration Tests**: Testing components together
- **API Tests**: Testing the HTTP endpoints

Test tools:
- pytest for test framework
- pytest-asyncio for testing async code
- pytest-cov for coverage reports

Example unit test:
```python
@pytest.mark.asyncio
async def test_create_item(mock_uow, mock_repository):
    with patch("src.services.item_service.ItemRepository", return_value=mock_repository):
        service = ItemService(mock_uow)
        item_in = ItemCreate(name="Test Item", price=10.0)
        
        result = await service.create(item_in)
        
        assert result is not None
        assert result.name == "Test Item"
        mock_repository.create.assert_called_once_with(item_in.model_dump())
```

## Security Best Practices

The architecture supports several security best practices:

- Dependency injection for authentication
- Environment-based configuration with sensitive values
- Input validation with Pydantic
- Database connection security
- Password hashing
- JWT token-based authentication

## Performance Optimization

Several techniques are used for optimal performance:

- Asynchronous database access
- Connection pooling
- Pagination for large datasets
- Efficient query building
- Type hints for MyPy optimization
- Dependency caching

## Service Consolidation

To maintain code cleanliness and reduce redundancy, we consolidate related services that handle the same domain entities. This approach reduces code fragmentation while improving maintainability.

### Execution Service Example

The `ExecutionService` was formed by consolidating multiple execution-related services:

```python
class ExecutionService:
    """
    Service for execution-related operations.
    
    Responsible for:
    1. Running executions (crew and flow executions)
    2. Tracking execution status
    3. Generating descriptive execution names
    4. Managing execution metadata
    """
    
    # Service implementation...
```

**Benefits of Service Consolidation:**

1. **Single Responsibility per Domain**: Each service handles one domain area
2. **Reduced File Count**: Fewer files to navigate and maintain
3. **Clearer Dependencies**: Methods that rely on each other are co-located
4. **Logical Grouping**: Related operations are together
5. **Simplified Imports**: External modules need to import from fewer places

**Consolidation Strategy:**

When deciding to consolidate services, we follow these guidelines:

1. Services should operate on the same domain entities
2. The combined service should maintain a clear purpose
3. Methods should have logical cohesion
4. The combined service shouldn't become too large (>1000 lines is a warning sign)

### Router Consolidation

Similar to services, we consolidate routers that handle endpoints related to the same domain area. This approach keeps related endpoints in the same file and simplifies API discovery.

For example, the `executions_router.py` handles all execution-related endpoints:

```python
# In executions_router.py
@router.post("", response_model=ExecutionCreateResponse)
async def create_execution(...):
    # Implementation...

@router.get("/{execution_id}", response_model=ExecutionResponse)
async def get_execution_status(...):
    # Implementation...

@router.post("/generate-name", response_model=ExecutionNameGenerationResponse)
async def generate_execution_name(...):
    # Implementation...
```

This consolidation ensures that related API endpoints are logically grouped, making the API more discoverable and the codebase more maintainable.

## Conclusion

This modern Python backend architecture provides a solid foundation for building scalable, maintainable, and high-performance APIs. By following these patterns and practices, developers can create robust applications that are easy to understand, test, and extend. 