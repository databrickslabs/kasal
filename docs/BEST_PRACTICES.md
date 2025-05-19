# Python Backend Best Practices

This document outlines the best practices for Python backend development using FastAPI, SQLAlchemy, and related technologies. Following these guidelines will help maintain code quality, performance, and maintainability.

## Project Structure

- Organize by feature/domain rather than by technical role
- Keep related code (routes, models, schemas, services) together
- Use consistent naming conventions
- Limit file size (max 400 lines recommended)
- Use `__init__.py` files to expose public interfaces

```
backend/
├── src/                 # Application source code
│   ├── api/             # API routes and controllers
│   ├── core/            # Core application components
│   ├── db/              # Database setup and session management
│   ├── models/          # SQLAlchemy models
│   ├── repositories/    # Repository pattern implementations
│   ├── schemas/         # Pydantic models for validation
│   ├── services/        # Business logic services
│   ├── config/          # Configuration management
│   ├── utils/           # Utility functions
│   ├── __init__.py
│   └── main.py          # Application entry point
├── tests/               # Test suite
│   ├── integration/     # Integration tests
│   └── unit/            # Unit tests
├── migrations/          # Alembic migration scripts
├── pyproject.toml       # Dependencies and build settings
├── alembic.ini          # Alembic configuration
└── README.md            # Project documentation
```

## Code Organization

### Layered Architecture

- **API Layer**: FastAPI routes and controllers
- **Service Layer**: Business logic
- **Repository Layer**: Data access and persistence
- **Database Layer**: Database models and connection

### Design Patterns

- **Repository Pattern**: For data access abstraction
- **Unit of Work Pattern**: For transaction management
- **Service Layer Pattern**: For business logic encapsulation
- **Dependency Injection**: For loose coupling and testability

## FastAPI Best Practices

### Routing

- Group related endpoints in separate router files
- Use descriptive route names
- Follow RESTful conventions for HTTP methods
- Use proper status codes
- Implement pagination for collection endpoints
- Use path parameters for resource identifiers
- Use query parameters for filtering/sorting

```python
@router.get("/{item_id}", response_model=ItemSchema)
async def read_item(
    item_id: int,
    service: Annotated[ItemService, Depends(get_item_service)],
):
    item = await service.get(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    return item
```

### Request Validation

- Use Pydantic models for request/response validation
- Create different models for different operations (Create, Update, Response)
- Use validators for complex validations
- Include detailed error messages
- Apply appropriate constraints (min/max lengths, regex patterns)

```python
class ItemCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = Field(None, max_length=1000)
    price: float = Field(..., gt=0)
```

### Response Handling

- Define response models explicitly
- Include appropriate HTTP status codes
- Use meaningful error messages
- Structure responses consistently
- Implement pagination metadata for collections
- Don't expose sensitive information

### Dependency Injection

- Use FastAPI's dependency system extensively
- Create reusable dependencies
- Chain dependencies when needed
- Cache dependencies for performance
- Use dependency overrides for testing

```python
def get_repository(repo_class, model_class):
    def _get_repo(uow: UnitOfWork = Depends(get_unit_of_work)):
        return repo_class(model_class, uow.session)
    return _get_repo
```

### Async/Await

- Use async/await for I/O bound operations
- Don't mix sync and async code
- Use proper async libraries (asyncpg, httpx)
- Be aware of the event loop
- Use background tasks for long-running operations

```python
async def get_items(skip: int = 0, limit: int = 100):
    async with async_session_factory() as session:
        query = select(Item).offset(skip).limit(limit)
        result = await session.execute(query)
        return result.scalars().all()
```

### Documentation

- Document all public APIs with docstrings
- Include parameter descriptions
- Document return values and exceptions
- Keep API documentation up-to-date
- Use FastAPI's automatic documentation (Swagger/ReDoc)

```python
@router.post("", response_model=ItemSchema, status_code=201)
async def create_item(item: ItemCreate):
    """
    Create a new item.
    
    Args:
        item: The item data to create
        
    Returns:
        The created item
        
    Raises:
        HTTPException: If an item with the same name already exists
    """
    # Implementation...
```

## Database Best Practices

### SQLAlchemy Usage

- Use SQLAlchemy 2.0 style (select instead of query)
- Define explicit relationships between models
- Use appropriate column types and constraints
- Define indexes for frequently queried columns
- Use migrations for schema changes
- Implement soft delete where appropriate

```python
class Item(Base):
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100), index=True)
    price: Mapped[float]
    is_active: Mapped[bool] = mapped_column(default=True)
```

### Database Access

- Use the repository pattern to abstract database access
- Implement transactions with Unit of Work
- Use async database access for better performance
- Apply proper pagination for large datasets
- Use database connections efficiently
- Implement database connection pooling

```python
class BaseRepository(Generic[ModelType]):
    def __init__(self, model: Type[ModelType], session: AsyncSession):
        self.model = model
        self.session = session

    async def get(self, id: int) -> Optional[ModelType]:
        query = select(self.model).where(self.model.id == id)
        result = await self.session.execute(query)
        return result.scalars().first()
```

### Database Migrations

- Use Alembic for database migrations
- Create migrations for schema changes
- Test migrations before applying to production
- Include both upgrade and downgrade paths
- Document database changes

## Testing

### Test Organization

- Separate unit tests from integration tests
- Use a consistent naming convention
- Structure tests to match application structure
- Create fixtures for common test scenarios
- Implement test helpers for repetitive tasks

### Testing Techniques

- Test all layers independently
- Mock external dependencies
- Use parametrized tests for edge cases
- Test error conditions
- Implement test database setup/teardown
- Use proper assertions

```python
@pytest.mark.asyncio
async def test_create_item(mock_uow, mock_repository):
    # Arrange
    with patch("src.services.item_service.ItemRepository", return_value=mock_repository):
        service = ItemService(mock_uow)
        item_in = ItemCreate(name="Test Item", price=10.0)
        
        # Act
        result = await service.create(item_in)
        
        # Assert
        assert result is not None
        assert result.name == "Test Item"
        mock_repository.create.assert_called_once_with(item_in.model_dump())
```

### Test Coverage

- Aim for high test coverage (>80%)
- Focus on critical paths
- Include edge cases and error handling
- Don't sacrifice test quality for coverage
- Use coverage reports to identify untested code

## Security Best Practices

- Use HTTPS in production
- Implement proper authentication/authorization
- Validate all user inputs
- Protect against common web vulnerabilities (XSS, CSRF, SQL injection)
- Store sensitive information securely
- Use environment variables for configuration
- Regularly update dependencies
- Implement rate limiting for APIs
- Use proper logging (without sensitive data)

## Performance Optimization

- Use async/await for I/O bound operations
- Implement caching where appropriate
- Optimize database queries (indexes, query optimization)
- Use connection pooling
- Paginate large data sets
- Minimize database round trips
- Profile and optimize bottlenecks
- Consider using background tasks for long-running operations

## Error Handling

- Implement global exception handlers
- Provide helpful error messages
- Log errors appropriately
- Return appropriate status codes
- Don't expose sensitive information in errors
- Handle both expected and unexpected errors
- Use custom exception classes for domain errors

```python
@app.exception_handler(CustomException)
async def custom_exception_handler(request: Request, exc: CustomException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
    )
```

## Configuration Management

- Use environment variables for configuration
- Implement different configurations for different environments
- Use Pydantic for configuration validation
- Don't hardcode sensitive information
- Provide sensible defaults
- Document configuration options

```python
class Settings(BaseSettings):
    DATABASE_URI: PostgresDsn
    API_KEY: SecretStr
    DEBUG: bool = False
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
```

## Logging

- Implement structured logging
- Use appropriate log levels
- Include contextual information
- Avoid logging sensitive data
- Configure different handlers for different environments
- Make logs searchable and filterable
- Use correlation IDs for request tracking

## Conclusion

Following these best practices will help create robust, maintainable, and efficient Python backends. These guidelines should be adapted to specific project requirements and team preferences. 