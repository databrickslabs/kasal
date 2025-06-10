# Kasal Development Best Practices

This document outlines the best practices for developing and maintaining the Kasal AI agent workflow orchestration platform. Following these guidelines will help maintain code quality, performance, and maintainability across the full stack application.

## Project Structure

Kasal follows a monorepo structure with clear separation between backend and frontend concerns:

- Organize by feature/domain rather than by technical role
- Keep related code (routes, models, schemas, services) together
- Use consistent naming conventions across both backend and frontend
- Limit file size (max 400 lines recommended)
- Use `__init__.py` files to expose public interfaces

```
kasal/
├── backend/                 # FastAPI backend
│   ├── src/
│   │   ├── api/             # API routes and controllers
│   │   ├── core/            # Core application components
│   │   ├── db/              # Database setup and session management
│   │   ├── models/          # SQLAlchemy models
│   │   ├── repositories/    # Repository pattern implementations
│   │   ├── schemas/         # Pydantic models for validation
│   │   ├── services/        # Business logic services
│   │   ├── engines/         # AI engine implementations (CrewAI)
│   │   ├── config/          # Configuration management
│   │   ├── utils/           # Utility functions
│   │   ├── seeds/           # Database seeding
│   │   ├── __init__.py
│   │   └── main.py          # Application entry point
│   ├── tests/               # Test suite
│   │   ├── integration/     # Integration tests
│   │   └── unit/            # Unit tests
│   ├── migrations/          # Alembic migration scripts
│   ├── pyproject.toml       # Dependencies and build settings
│   └── alembic.ini          # Alembic configuration
├── frontend/                # React frontend
│   ├── src/
│   │   ├── components/      # React components
│   │   ├── hooks/           # Custom React hooks
│   │   ├── store/           # Zustand state management
│   │   ├── api/             # API service layer
│   │   ├── types/           # TypeScript types
│   │   ├── utils/           # Utility functions
│   │   └── config/          # Frontend configuration
│   ├── public/              # Static assets
│   ├── package.json         # NPM dependencies
│   └── tsconfig.json        # TypeScript configuration
├── docs/                    # Documentation
├── deploy.py               # Deployment script
├── build.py                # Build script
└── README.md               # Project overview
```

## Code Organization

### Backend Architecture (FastAPI)

- **API Layer**: FastAPI routes and controllers
- **Service Layer**: Business logic and orchestration
- **Repository Layer**: Data access and persistence
- **Engine Layer**: AI engine implementations (CrewAI)
- **Database Layer**: SQLAlchemy models and migrations

### Frontend Architecture (React)

- **Component Layer**: React components with Material-UI styling
- **Hook Layer**: Custom React hooks for state and logic
- **Store Layer**: Zustand for global state management
- **API Layer**: Service classes for backend communication
- **Type Layer**: TypeScript definitions and interfaces

### Design Patterns

- **Repository Pattern**: For data access abstraction (backend)
- **Unit of Work Pattern**: For transaction management (backend)
- **Service Layer Pattern**: For business logic encapsulation (backend)
- **Dependency Injection**: For loose coupling and testability (backend)
- **Custom Hooks Pattern**: For reusable stateful logic (frontend)
- **State Management Pattern**: Using Zustand for predictable state updates (frontend)

## Backend Best Practices (FastAPI)

### API Design

- Group related endpoints in separate router files (e.g., `agents_router.py`, `crews_router.py`)
- Use descriptive route names that reflect Kasal domain concepts
- Follow RESTful conventions for HTTP methods
- Use proper status codes (200, 201, 404, 422, etc.)
- Implement pagination for collection endpoints
- Use path parameters for resource identifiers (agent_id, crew_id, execution_id)
- Use query parameters for filtering and sorting

```python
@router.get("/{agent_id}", response_model=AgentResponse)
async def get_agent(
    agent_id: str,
    service: Annotated[AgentService, Depends(get_agent_service)],
):
    agent = await service.get_agent(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    return agent
```

### Request Validation

- Use Pydantic models for request/response validation
- Create different schemas for different operations (AgentCreate, AgentUpdate, AgentResponse)
- Use validators for complex validations (YAML syntax, tool configurations)
- Include detailed error messages for user-friendly feedback
- Apply appropriate constraints for Kasal-specific data

```python
class AgentCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    role: str = Field(..., min_length=1, max_length=500)
    goal: str = Field(..., min_length=1, max_length=1000)
    backstory: str = Field(..., min_length=1, max_length=2000)
    tools: List[str] = Field(default_factory=list)
    model_config: Optional[Dict[str, Any]] = None
    
    @field_validator('tools')
    def validate_tools(cls, v):
        # Validate tool names against available tools
        return v
```

### Response Handling

- Define response models explicitly for all Kasal entities
- Include appropriate HTTP status codes
- Use meaningful error messages related to AI agent concepts
- Structure responses consistently across all endpoints
- Implement pagination metadata for collections (agents, crews, executions)
- Don't expose sensitive information (API keys, credentials)

### Dependency Injection

- Use FastAPI's dependency system extensively for services and repositories
- Create reusable dependencies for common Kasal services
- Chain dependencies when needed (UnitOfWork → Repository → Service)
- Cache dependencies for performance
- Use dependency overrides for testing

```python
def get_service(
    service_class: Type[BaseService],
    repository_class: Type[BaseRepository],
    model_class: Type[Base],
) -> Callable[[UOWDep], BaseService]:
    def _get_service(uow: UOWDep) -> BaseService:
        return service_class(repository_class, model_class, uow)
    return _get_service

# Usage for Kasal services
get_agent_service = get_service(AgentService, AgentRepository, Agent)
get_crew_service = get_service(CrewService, CrewRepository, Crew)
```

### Async/Await

- Use async/await for I/O bound operations (database, LLM calls, external APIs)
- Don't mix sync and async code in the same execution path
- Use proper async libraries (asyncpg for PostgreSQL, httpx for HTTP requests)
- Be aware of the event loop, especially for CrewAI engine operations
- Use background tasks for long-running operations (agent executions)

```python
async def execute_crew(execution_id: str, config: CrewConfig):
    """Execute a crew asynchronously without blocking the API"""
    async with async_session_factory() as session:
        try:
            # Start execution in background
            engine = await EngineFactory.get_engine("crewai", session)
            result = await engine.run_execution(execution_id, config)
            return result
        except Exception as e:
            logger.error(f"Execution {execution_id} failed: {e}")
            raise
```

### Documentation

- Document all public APIs with docstrings describing Kasal-specific functionality
- Include parameter descriptions for AI agent concepts
- Document return values and exceptions specific to agent execution
- Keep API documentation up-to-date with Kasal features
- Use FastAPI's automatic documentation (accessible at `/docs`)

```python
@router.post("", response_model=AgentResponse, status_code=201)
async def create_agent(agent: AgentCreate):
    """
    Create a new AI agent for workflow automation.
    
    Args:
        agent: The agent configuration including role, goal, backstory, and tools
        
    Returns:
        The created agent with assigned ID and metadata
        
    Raises:
        HTTPException: If agent name already exists or tools are invalid
    """
    # Implementation...
```

## Database Best Practices

### SQLAlchemy Usage

- Use SQLAlchemy 2.0 style (select instead of query)
- Define explicit relationships between Kasal models (Agent, Task, Crew, Execution)
- Use appropriate column types for AI-specific data (JSON for configurations, Text for long descriptions)
- Define indexes for frequently queried columns (execution status, agent names)
- Use Alembic migrations for schema changes
- Implement soft delete where appropriate for executions and logs

```python
class Agent(Base):
    __tablename__ = "agents"
    
    id: Mapped[str] = mapped_column(String, primary_key=True, default=generate_uuid)
    name: Mapped[str] = mapped_column(String(100), index=True, nullable=False)
    role: Mapped[str] = mapped_column(Text, nullable=False)
    goal: Mapped[str] = mapped_column(Text, nullable=False)
    backstory: Mapped[str] = mapped_column(Text, nullable=False)
    tools: Mapped[List[str]] = mapped_column(JSON, default=list)
    model_config: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    
    # Relationships
    tasks: Mapped[List["Task"]] = relationship("Task", back_populates="agent")
```

### Database Access

- Use the repository pattern to abstract database access for all Kasal entities
- Implement transactions with Unit of Work for complex operations
- Use async database access for better performance during agent executions
- Apply proper pagination for large datasets (execution history, logs)
- Use database connections efficiently during concurrent executions
- Implement database connection pooling for high-load scenarios

```python
class BaseRepository(Generic[ModelType]):
    def __init__(self, model: Type[ModelType], session: AsyncSession):
        self.model = model
        self.session = session

    async def get(self, id: str) -> Optional[ModelType]:
        """Get entity by UUID string ID"""
        query = select(self.model).where(self.model.id == id)
        result = await self.session.execute(query)
        return result.scalars().first()
        
    async def get_by_name(self, name: str) -> Optional[ModelType]:
        """Get entity by name (common pattern in Kasal)"""
        if hasattr(self.model, 'name'):
            query = select(self.model).where(self.model.name == name)
            result = await self.session.execute(query)
            return result.scalars().first()
        return None
```

### Database Migrations

- Use Alembic for database migrations in Kasal
- Create migrations for schema changes (new AI engines, tool configurations)
- Test migrations before applying to production Databricks environments
- Include both upgrade and downgrade paths for rollback safety
- Document database changes in migration messages
- Run `alembic revision --autogenerate -m "descriptive message"` for schema changes
- Apply migrations with `alembic upgrade head` during deployment

## Frontend Best Practices (React)

### Component Organization

- Organize components by feature (Agents, Crews, Jobs, Configuration)
- Use TypeScript for type safety across all components
- Follow Material-UI design system conventions
- Implement proper prop validation with TypeScript interfaces
- Keep components focused on single responsibilities

### State Management

- Use Zustand for global state (workflow, execution status, user preferences)
- Keep local state for component-specific data
- Implement proper state normalization for complex data
- Use custom hooks for reusable stateful logic

```typescript
// Example Zustand store for workflow management
interface WorkflowState {
  nodes: Node[];
  edges: Edge[];
  selectedNode: Node | null;
  
  addNode: (node: Node) => void;
  updateNode: (id: string, updates: Partial<Node>) => void;
  deleteNode: (id: string) => void;
}

export const useWorkflowStore = create<WorkflowState>((set) => ({
  nodes: [],
  edges: [],
  selectedNode: null,
  
  addNode: (node) => set((state) => ({ 
    nodes: [...state.nodes, node] 
  })),
  // ... other actions
}));
```

### Custom Hooks

- Create custom hooks for API interactions (`useAgents`, `useExecutions`)
- Implement hooks for complex UI logic (`useWorkflowCanvas`, `useShortcuts`)
- Use hooks for component lifecycle management
- Follow the `use` naming convention

```typescript
export const useAgents = () => {
  const [agents, setAgents] = useState<Agent[]>([]);
  const [loading, setLoading] = useState(false);
  
  const fetchAgents = useCallback(async () => {
    setLoading(true);
    try {
      const response = await AgentService.getAgents();
      setAgents(response.data);
    } catch (error) {
      console.error('Failed to fetch agents:', error);
    } finally {
      setLoading(false);
    }
  }, []);
  
  return { agents, loading, fetchAgents };
};
```

## Testing

### Backend Testing

- Separate unit tests from integration tests
- Use pytest with async support for testing Kasal services
- Mock external dependencies (LLM providers, external APIs)
- Test AI agent configurations and execution flows
- Implement test database setup/teardown

```python
@pytest.mark.asyncio
async def test_create_agent(mock_uow, mock_repository):
    # Arrange
    with patch("src.services.agent_service.AgentRepository", return_value=mock_repository):
        service = AgentService(mock_repository, Agent, mock_uow)
        agent_data = AgentCreate(
            name="Test Agent",
            role="Data Analyst",
            goal="Analyze data efficiently",
            backstory="Expert in data analysis"
        )
        
        # Act
        result = await service.create(agent_data.model_dump())
        
        # Assert
        assert result is not None
        assert result.name == "Test Agent"
        assert result.role == "Data Analyst"
        mock_repository.create.assert_called_once()
```

### Frontend Testing

- Use Jest and React Testing Library for component testing
- Test user interactions and component behavior
- Mock API calls and external dependencies
- Test keyboard shortcuts and workflow interactions
- Implement visual regression testing for complex UI components

### Test Coverage

- Aim for high test coverage (>80%) for both backend and frontend
- Focus on critical paths (agent execution, workflow management)
- Include edge cases and error handling for AI-specific scenarios
- Don't sacrifice test quality for coverage metrics
- Use coverage reports to identify untested code paths
- Test CrewAI engine integrations thoroughly
- Include tests for keyboard shortcuts and accessibility

## Security Best Practices

- Use HTTPS in production deployments (Databricks Apps automatically provides this)
- Implement proper authentication/authorization with JWT tokens
- Validate all user inputs, especially AI agent configurations and tool parameters
- Protect against common web vulnerabilities (XSS, CSRF, SQL injection)
- Store sensitive information securely (API keys, database credentials)
- Use environment variables for configuration (never hardcode secrets)
- Regularly update dependencies for both backend and frontend
- Implement rate limiting for APIs to prevent abuse
- Use proper logging without exposing sensitive data (API keys, user credentials)
- Secure LLM API keys and never expose them in frontend code
- Validate tool configurations to prevent code injection
- Implement proper CORS settings for frontend-backend communication

## Performance Optimization

### Backend Performance

- Use async/await for I/O bound operations (database, LLM calls)
- Implement caching for frequently accessed data (agent configurations, tools)
- Optimize database queries with proper indexes for execution history
- Use connection pooling for database connections
- Paginate large data sets (execution logs, agent traces)
- Minimize database round trips during agent execution
- Profile and optimize bottlenecks in AI engine operations
- Use background tasks for long-running agent executions
- Implement efficient trace processing with batching

### Frontend Performance

- Use React.memo for expensive component renders
- Implement proper list virtualization for large datasets
- Optimize Canvas rendering for complex workflows
- Use code splitting for route-based lazy loading
- Implement proper state management to avoid unnecessary re-renders
- Optimize bundle size with tree shaking
- Use proper image optimization for static assets

## Error Handling

### Backend Error Handling

- Implement global exception handlers for API errors
- Provide helpful error messages for AI-specific failures
- Log errors appropriately with execution context
- Return appropriate status codes for different error types
- Don't expose sensitive information (API keys, internal details) in errors
- Handle both expected (agent failures) and unexpected errors
- Use custom exception classes for Kasal domain errors

```python
class AgentExecutionError(Exception):
    def __init__(self, execution_id: str, message: str):
        self.execution_id = execution_id
        self.message = message
        super().__init__(f"Execution {execution_id} failed: {message}")

@app.exception_handler(AgentExecutionError)
async def agent_execution_exception_handler(request: Request, exc: AgentExecutionError):
    logger.error(f"Agent execution failed: {exc.execution_id} - {exc.message}")
    return JSONResponse(
        status_code=500,
        content={"detail": f"Agent execution failed: {exc.message}"},
    )
```

### Frontend Error Handling

- Implement error boundaries for component error isolation
- Show user-friendly error messages for API failures
- Handle network errors gracefully with retry mechanisms
- Display specific error messages for agent execution failures
- Log frontend errors for debugging purposes
- Implement proper loading states and error recovery

## Configuration Management

- Use environment variables for all configuration (database, LLM APIs, external services)
- Implement different configurations for development and production environments
- Use Pydantic for configuration validation with Kasal-specific settings
- Don't hardcode sensitive information (API keys, database URLs)
- Provide sensible defaults for optional settings
- Document all configuration options in deployment guides

```python
class KasalSettings(BaseSettings):
    # Database
    DATABASE_URL: str
    
    # LLM Providers
    OPENAI_API_KEY: Optional[SecretStr] = None
    ANTHROPIC_API_KEY: Optional[SecretStr] = None
    DATABRICKS_HOST: Optional[str] = None
    DATABRICKS_TOKEN: Optional[SecretStr] = None
    
    # Application
    DEBUG: bool = False
    LOG_LEVEL: str = "INFO"
    
    # CrewAI Engine
    DEFAULT_LLM_PROVIDER: str = "openai"
    DEFAULT_MODEL: str = "gpt-4o"
    MAX_CONCURRENT_EXECUTIONS: int = 10
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True
```

## Logging

- Implement structured logging for both backend and frontend
- Use appropriate log levels (DEBUG, INFO, WARNING, ERROR) based on importance
- Include contextual information (execution IDs, agent names, user context)
- Avoid logging sensitive data (API keys, user credentials, private data)
- Configure different handlers for different environments (development vs production)
- Make logs searchable and filterable for debugging agent executions
- Use correlation IDs for tracking requests across the entire execution flow
- Log AI engine operations with sufficient detail for troubleshooting
- Implement log rotation and retention policies for production environments

## Kasal-Specific Best Practices

### AI Engine Integration

- Always validate agent configurations before execution
- Implement proper error handling for LLM API failures
- Use async operations for all AI engine calls
- Implement timeout mechanisms for long-running agent tasks
- Cache tool configurations to avoid repeated lookups
- Handle CrewAI framework exceptions gracefully

### Workflow Management

- Validate workflow configurations before saving
- Implement proper state management for complex workflows
- Use proper node IDs and relationships in workflow data
- Handle workflow execution cancellation gracefully
- Implement proper cleanup for interrupted executions

### Tool Management

- Validate tool configurations and required parameters
- Implement proper API key management for external tools
- Handle tool failures without crashing the entire execution
- Provide clear error messages for tool configuration issues
- Implement tool discovery and registration mechanisms

### Execution Monitoring

- Provide real-time execution status updates
- Implement detailed execution traces for debugging
- Handle execution cancellation and cleanup properly
- Store execution results for future reference
- Implement proper pagination for execution history

## Conclusion

Following these best practices will help create a robust, maintainable, and efficient AI agent workflow orchestration platform. These guidelines are specifically tailored for Kasal's architecture and should be adapted as the platform evolves. Regular review and updates of these practices ensure the codebase remains clean, secure, and performant as new features are added. 