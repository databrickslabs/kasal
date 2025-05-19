# Repository Pattern Implementation

This document explains how the Repository Pattern is implemented in our project, with a focus on the `ExecutionRepository` as a practical example.

## Overview of the Repository Pattern

The Repository Pattern is a design pattern that abstracts the data access logic from the rest of the application. It provides a collection-like interface for accessing domain objects, regardless of the underlying data storage mechanism.

### Core Benefits

1. **Separation of Concerns**: Isolates data access logic from business logic
2. **Testability**: Makes unit testing easier by allowing mock repositories
3. **Flexibility**: Simplifies switching between different data sources or ORM implementations
4. **Consistency**: Provides a consistent way to interact with data across the application
5. **Code Organization**: Groups related data access operations together

## Implementation in Our Project

In our application, repositories are implemented as classes that handle database operations for specific domain entities.

### Base Repository

All repositories inherit from a base repository class that provides common CRUD operations:

```python
class BaseRepository:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.model = self._get_model()
    
    def _get_model(self):
        # Implementation to determine the model class
        
    async def get(self, id: int) -> Optional[Any]:
        # Get entity by ID
        
    async def get_all(self) -> List[Any]:
        # Get all entities
        
    async def create(self, data: Dict[str, Any]) -> Any:
        # Create new entity
        
    async def update(self, id: int, data: Dict[str, Any]) -> Optional[Any]:
        # Update existing entity
        
    async def delete(self, id: int) -> bool:
        # Delete entity by ID
```

### Example: ExecutionRepository

The `ExecutionRepository` extends the base repository and adds specific methods for execution-related operations:

```python
class ExecutionRepository(BaseRepository):
    def __init__(self, session: AsyncSession):
        super().__init__(session)
        self.model = ExecutionHistory
    
    async def get_execution_by_job_id(self, job_id: str) -> Optional[ExecutionHistory]:
        """Get execution by job ID."""
        query = select(self.model).where(self.model.job_id == job_id)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def get_execution_history(self, limit: int = 100, offset: int = 0) -> Tuple[List[ExecutionHistory], int]:
        """Get paginated execution history."""
        query = select(self.model).order_by(desc(self.model.created_at)).offset(offset).limit(limit)
        result = await self.session.execute(query)
        executions = result.scalars().all()
        
        # Get total count
        count_query = select(func.count()).select_from(self.model)
        count_result = await self.session.execute(count_query)
        total = count_result.scalar() or 0
        
        return executions, total
    
    async def create_execution(self, data: Dict[str, Any]) -> ExecutionHistory:
        """Create a new execution record."""
        execution = self.model(**data)
        self.session.add(execution)
        await self.session.commit()
        await self.session.refresh(execution)
        return execution
    
    async def update_execution(self, execution_id: int, data: Dict[str, Any]) -> Optional[ExecutionHistory]:
        """Update an execution record."""
        query = select(self.model).where(self.model.id == execution_id)
        result = await self.session.execute(query)
        execution = result.scalars().first()
        
        if not execution:
            return None
            
        for key, value in data.items():
            setattr(execution, key, value)
            
        await self.session.commit()
        await self.session.refresh(execution)
        return execution
    
    async def mark_execution_completed(self, execution_id: int, result: Dict[str, Any] = None) -> Optional[ExecutionHistory]:
        """Mark an execution as completed."""
        data = {
            "status": "completed",
            "completed_at": datetime.now(UTC)
        }
        
        if result:
            data["result"] = result
            
        return await self.update_execution(execution_id, data)
    
    async def mark_execution_failed(self, execution_id: int, error: str) -> Optional[ExecutionHistory]:
        """Mark an execution as failed."""
        data = {
            "status": "failed",
            "completed_at": datetime.now(UTC),
            "error": error
        }
        
        return await self.update_execution(execution_id, data)
```

## Use in Service Layer

Repositories are used by services to perform data operations. Here's how the `ExecutionService` uses the `ExecutionRepository`:

```python
class ExecutionService:
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def create_execution(self, config: CrewConfig, background_tasks = None) -> Dict[str, Any]:
        # Generate a new execution ID
        execution_id = ExecutionService.create_execution_id()
        
        # Generate a descriptive run name
        run_name = await generate_run_name(config.agents_yaml, config.tasks_yaml, config.model, self.db)
        
        # Create execution data
        execution_data = {
            "job_id": execution_id,
            "status": ExecutionStatus.PENDING.value,
            "inputs": {
                # Details about the execution
            },
            "run_name": run_name,
            "created_at": datetime.now(UTC)
        }
        
        # Create the execution record using the repository
        execution_repo = ExecutionRepository(self.db)
        await execution_repo.create_execution(data=execution_data)
        
        # ... additional logic
        
        return {
            "execution_id": execution_id,
            "status": ExecutionStatus.PENDING.value,
            "run_name": run_name
        }
```

## Dependency Injection

Repositories are typically instantiated with a database session passed from the API layer:

```python
@router.get("/{execution_id}", response_model=ExecutionResponse)
async def get_execution_status(execution_id: str, db: AsyncSession = Depends(get_db)):
    """Get the status of a specific execution."""
    execution_data = await ExecutionService.get_execution_status(db, execution_id)
    
    if not execution_data:
        raise HTTPException(status_code=404, detail="Execution not found")
    
    return ExecutionResponse(**execution_data)
```

In the service implementation:

```python
@staticmethod
async def get_execution_status(db: AsyncSession, execution_id: str) -> Dict[str, Any]:
    """Get the current status of an execution."""
    # Check in-memory storage first
    if execution_id in ExecutionService.executions:
        return ExecutionService.executions[execution_id]
        
    # If not in memory, check database using repository
    execution_repo = ExecutionRepository(db)
    
    try:
        run = await execution_repo.get_execution_by_job_id(job_id=execution_id)
        
        if not run:
            return None
            
        return {
            "execution_id": execution_id,
            "status": run.status,
            "created_at": run.created_at,
            "result": run.result,
            "run_name": run.run_name,
            "error": run.error
        }
    except Exception as e:
        logger.error(f"Error getting execution status: {str(e)}")
        return None
```

## Consolidation Benefits

By consolidating the `ExecutionHistoryRepository` into a properly named `ExecutionRepository`, we achieve several benefits:

1. **Semantic Clarity**: The name better represents its purpose
2. **Reduced Redundancy**: Eliminates duplicate code across multiple repositories
3. **Improved Maintainability**: All execution data access is in one place
4. **Consistent Interface**: Methods follow a consistent naming convention
5. **Focused Responsibility**: Each repository handles one domain entity

## Best Practices

When implementing repositories, follow these guidelines:

1. **Keep Repositories Focused**: Each repository should handle a single domain entity
2. **Use Descriptive Method Names**: Names should clearly communicate the operation purpose
3. **Return Domain Entities**: Not DTOs or raw data structures
4. **Handle Exceptions Appropriately**: Don't expose database errors to callers
5. **Use Typing**: Add proper type hints for better IDE support and code clarity
6. **Document Methods**: Add docstrings to explain method purpose and parameters
7. **Follow Naming Conventions**: Be consistent with method naming (e.g., `get_by_*`, `find_*`)
8. **Keep Query Logic in Repositories**: Don't leak query details to services

## Migration Path

When refactoring repositories, follow this process:

1. **Create the new repository** with appropriate naming
2. **Implement all required methods**
3. **Update service classes** to use the new repository
4. **Change dependencies** in API routes if necessary
5. **Test thoroughly** to ensure functionality is preserved
6. **Delete the old repository** once all references are updated

## Conclusion

The Repository Pattern is a powerful abstraction that improves code organization, testability, and maintainability. Our implementation with `ExecutionRepository` demonstrates how to properly structure data access in a modern Python application with clear separation of concerns. 