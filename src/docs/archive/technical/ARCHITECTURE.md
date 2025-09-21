# Kasal Application Architecture

This document provides a comprehensive guide to the architecture and design patterns used in Kasal, an AI agent workflow orchestration platform built with FastAPI and React.

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

Kasal is a full-stack application for building and managing AI agent workflows. The architecture follows a layered pattern with clear separation between frontend and backend concerns.

```
┌─────────────────────────────────────────────────────────┐
│                    Frontend (React)                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │
│  │ Components  │  │   Hooks     │  │ State (Zustand) │  │
│  └─────────────┘  └─────────────┘  └─────────────────┘  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │
│  │   Canvas    │  │   Dialogs   │  │   API Service   │  │
│  └─────────────┘  └─────────────┘  └─────────────────┘  │
└─────────────────────┬───────────────────────────────────┘
                      │ HTTP/WebSocket
                      ▼
┌─────────────────────────────────────────────────────────┐
│                   Backend (FastAPI)                    │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │
│  │ API Routes  │  │  Services   │  │   Repositories  │  │
│  └─────────────┘  └─────────────┘  └─────────────────┘  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │
│  │ CrewAI      │  │   LLM       │  │   Models        │  │
│  │ Engine      │  │  Manager    │  │  (SQLAlchemy)   │  │
│  └─────────────┘  └─────────────┘  └─────────────────┘  │
└─────────────────────┬───────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────┐
│              Database (SQLite/PostgreSQL)              │
└─────────────────────────────────────────────────────────┘
```

### Key Features

- **Visual Workflow Designer**: React-based drag-and-drop interface for building AI agent workflows
- **AI Agent Orchestration**: CrewAI engine integration for managing autonomous AI agents
- **Multi-LLM Support**: LLM manager supporting OpenAI, Anthropic, DeepSeek, Ollama, and Databricks
- **Real-time Execution**: Live monitoring of agent workflows with detailed logging and tracing
- **Extensible Tools**: Rich toolkit including Genie, custom APIs, MCP servers, and data connectors
- **Enterprise Security**: OAuth integration, role-based access control, and secure deployment
- **Database Flexibility**: Support for both SQLite (development) and PostgreSQL (production)
- **Databricks Integration**: Native deployment to Databricks Apps with OAuth scope management

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

The service layer contains business logic and orchestrates operations, implementing a comprehensive service pattern architecture that prioritizes modularity, testability, and maintainability.

**Responsibilities:**
- Implementing business rules
- Orchestrating repositories
- Transaction management
- Domain logic
- Input validation
- Business-specific validation

#### Service Pattern Architecture Benefits

**1. Modularity & Loose Coupling**

The service pattern creates modular, loosely coupled components that can be developed, tested, and maintained independently:

```python
# Services are injected as dependencies, not directly instantiated
@router.post("/executions")
async def create_execution(
    config: CrewConfig,
    execution_service: ExecutionService = Depends(get_execution_service),
    status_service: ExecutionStatusService = Depends(get_status_service)
):
    # Each service handles a specific domain concern
    execution = await execution_service.create_execution(config)
    await status_service.track_execution(execution.id)
    return execution
```

**Benefits:**
- **Single Responsibility**: Each service handles one domain area (ExecutionService for execution logic, StatusService for tracking)
- **Dependency Injection**: Services receive dependencies rather than creating them, enabling easy swapping of implementations
- **Interface Segregation**: Services expose only the methods needed by their consumers
- **Reduced Coupling**: Changes to one service don't cascade to others when interfaces remain stable

**2. Enhanced Testability**

The service architecture dramatically improves testability through dependency injection and clear separation of concerns:

```python
# Example: Testing ExecutionService in isolation
@pytest.mark.asyncio
async def test_create_execution_success(mock_uow, mock_execution_repo):
    # Arrange: Mock all dependencies
    mock_execution_repo.create.return_value = Mock(id="exec_123")
    service = ExecutionService(mock_uow)

    # Act: Test the service logic
    result = await service.create_execution(config_data)

    # Assert: Verify behavior without touching database
    assert result.id == "exec_123"
    mock_execution_repo.create.assert_called_once()
    mock_uow.commit.assert_called_once()

# Example: Testing API layer with mocked services
async def test_create_execution_endpoint(mock_execution_service):
    mock_execution_service.create_execution.return_value = Mock(id="exec_123")

    response = await client.post("/executions", json=config_data)

    assert response.status_code == 201
    assert response.json()["id"] == "exec_123"
    # No database interaction in this test
```

**Testing Advantages:**
- **Fast Test Execution**: Unit tests run without database connections or external dependencies
- **Isolated Testing**: Each layer can be tested independently with mocked dependencies
- **Predictable Tests**: Mocked dependencies eliminate flaky tests caused by external factors
- **Comprehensive Coverage**: Easy to test error scenarios and edge cases
- **Test Pyramid**: Clear separation enables proper unit → integration → e2e test structure

**3. Improved Maintainability**

The service pattern significantly improves code maintainability through structured organization and clear boundaries:

```python
class ExecutionService:
    """
    Centralized service for all execution-related business logic.

    Consolidates:
    - Execution creation and management
    - Status tracking coordination
    - Name generation
    - Cleanup operations

    This consolidation reduces code fragmentation while maintaining
    clear separation from data access (repositories) and presentation (API).
    """

    def __init__(self, uow: UnitOfWork):
        self.uow = uow
        # Service dependencies are injected, not hardcoded

    async def create_execution(self, config: CrewConfig) -> Execution:
        # Business logic is centralized and reusable
        execution_id = self._generate_execution_id()

        async with self.uow:
            execution = await self.uow.executions.create({
                "id": execution_id,
                "config": config,
                "status": ExecutionStatus.PENDING
            })
            await self.uow.commit()
            return execution

    async def cleanup_completed_executions(self, older_than_days: int = 30):
        # Complex business operations are encapsulated in services
        cutoff_date = datetime.now() - timedelta(days=older_than_days)

        async with self.uow:
            completed_executions = await self.uow.executions.find_completed_before(cutoff_date)
            for execution in completed_executions:
                await self._cleanup_execution_resources(execution)
            await self.uow.commit()
```

**Maintainability Benefits:**
- **Code Organization**: Related business logic is grouped in cohesive services
- **Reusability**: Service methods can be called from multiple API endpoints or background tasks
- **Change Isolation**: Business logic changes are contained within service boundaries
- **Clear Abstractions**: Services provide meaningful business operations, not just CRUD
- **Documentation**: Services serve as living documentation of business capabilities
- **Refactoring Safety**: Well-defined interfaces make refactoring safer and more predictable

**4. Service Consolidation Strategy**

We strategically consolidate related services to balance modularity with maintainability:

```python
# Before: Fragmented services
class ExecutionCreationService: ...
class ExecutionStatusService: ...
class ExecutionCleanupService: ...
class ExecutionNameService: ...

# After: Consolidated but focused service
class ExecutionService:
    """
    Consolidated service handling all execution domain operations.

    Maintains modularity through:
    - Clear method separation
    - Dependency injection
    - Single domain focus
    - Logical operation grouping
    """

    async def create_execution(self, config: CrewConfig) -> Execution: ...
    async def update_status(self, execution_id: str, status: ExecutionStatus): ...
    async def cleanup_completed(self, older_than_days: int = 30): ...
    async def generate_execution_name(self, config: CrewConfig) -> str: ...
```

**Consolidation Benefits:**
- **Reduced File Count**: Fewer service files to navigate and maintain
- **Cohesive Operations**: Related methods are co-located for better understanding
- **Simplified Dependencies**: External consumers import from fewer services
- **Logical Grouping**: Operations that work together are grouped together
- **Maintained Testability**: Individual methods remain easily testable

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

The application is designed to be testable at all layers through the service pattern architecture, which enables comprehensive testing strategies:

- **Unit Tests**: Testing individual components in isolation
- **Integration Tests**: Testing components together
- **API Tests**: Testing the HTTP endpoints

### Testing Architecture Benefits

The service pattern architecture dramatically improves testing by:

**1. Dependency Injection Enables Easy Mocking**
```python
# Service dependencies are injected, making them easy to mock
class ExecutionService:
    def __init__(self, uow: UnitOfWork, status_service: ExecutionStatusService):
        self.uow = uow
        self.status_service = status_service

# In tests, inject mocks instead of real dependencies
@pytest.mark.asyncio
async def test_create_execution_updates_status(mock_uow, mock_status_service):
    service = ExecutionService(mock_uow, mock_status_service)

    result = await service.create_execution(config)

    # Verify the service coordinated with its dependencies correctly
    mock_status_service.track_execution.assert_called_once_with(result.id)
    mock_uow.commit.assert_called_once()
```

**2. Layer Isolation for Focused Testing**
```python
# Test API layer in isolation from business logic
async def test_create_execution_endpoint(mock_execution_service):
    mock_execution_service.create_execution.return_value = Mock(id="exec_123")

    response = await client.post("/executions", json=config_data)

    assert response.status_code == 201
    # Test focuses on HTTP concerns, not business logic

# Test service layer in isolation from data access
@pytest.mark.asyncio
async def test_create_execution_business_logic(mock_uow):
    service = ExecutionService(mock_uow)

    result = await service.create_execution(config)

    # Test focuses on business logic, not database operations
    assert result.status == ExecutionStatus.PENDING
    assert result.id is not None
```

**3. Fast and Reliable Unit Tests**
```python
# No database required - tests run in milliseconds
@pytest.mark.asyncio
async def test_execution_name_generation():
    service = ExecutionService(Mock())

    name = await service.generate_execution_name(config)

    assert "Data Analysis Crew" in name
    assert len(name) <= 100
    # Test is deterministic and fast
```

Test tools:
- pytest for test framework
- pytest-asyncio for testing async code
- pytest-cov for coverage reports
- pytest-mock for advanced mocking

Example comprehensive test suite:
```python
@pytest.mark.asyncio
async def test_create_execution_success(mock_uow, mock_status_service):
    """Test successful execution creation with all dependencies mocked."""
    # Arrange
    mock_execution_repo = Mock()
    mock_execution_repo.create.return_value = Mock(
        id="exec_123",
        status=ExecutionStatus.PENDING
    )
    mock_uow.executions = mock_execution_repo

    service = ExecutionService(mock_uow, mock_status_service)
    config = CrewConfig(name="Test Crew", agents=[], tasks=[])

    # Act
    result = await service.create_execution(config)

    # Assert
    assert result.id == "exec_123"
    assert result.status == ExecutionStatus.PENDING
    mock_execution_repo.create.assert_called_once()
    mock_uow.commit.assert_called_once()
    mock_status_service.track_execution.assert_called_once_with("exec_123")

@pytest.mark.asyncio
async def test_create_execution_handles_database_error(mock_uow, mock_status_service):
    """Test error handling when database operations fail."""
    # Arrange
    mock_uow.commit.side_effect = DatabaseError("Connection failed")
    service = ExecutionService(mock_uow, mock_status_service)

    # Act & Assert
    with pytest.raises(HTTPException) as exc_info:
        await service.create_execution(config)

    assert exc_info.value.status_code == 500
    mock_uow.rollback.assert_called_once()
```

**Testing Strategy Benefits:**
- **Fast Execution**: Unit tests run without database or external services
- **Reliable**: Mocked dependencies eliminate flaky tests
- **Comprehensive**: Easy to test error scenarios and edge cases
- **Maintainable**: Tests focus on specific layer responsibilities
- **Debuggable**: Clear separation makes test failures easy to diagnose

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