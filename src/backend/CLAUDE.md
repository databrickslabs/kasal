# Backend CLAUDE.md

Backend-specific instructions for Claude Code when working in the backend directory.

## Commands

### Development
- **Activate virtual environment**: `source ../../venv/bin/activate` (from backend dir) or `source venv/bin/activate` (from project root)
- **Start server**: `./run.sh` (defaults to PostgreSQL) or `./run.sh sqlite` for SQLite
- **Run tests**: `python run_tests.py` (runs all tests with linting)
- **Run specific tests**: `python run_tests.py --type unit` or `python run_tests.py --type integration`
- **Run tests with coverage**: `python run_tests.py --coverage --html-coverage`
- **Run single test file**: `python -m pytest tests/unit/test_file.py -v`

### Database
- **Migrations**: `alembic upgrade head`
- **Create migration**: `alembic revision --autogenerate -m "description"`
- **Seed database**: `python run_seeders.py`

### Code Quality
- **Format code**: `python -m black src tests && python -m isort src tests`
- **Type checking**: `python -m mypy src`
- **Format check**: `python -m black --check src tests`

## Architecture

### Clean Architecture Pattern
- **Repository Pattern**: All database access through repositories
- **Unit of Work**: Transaction management for complex operations
- **Service Layer**: Business logic orchestration
- **Dependency Injection**: Use FastAPI's built-in DI system
- **Async/Await**: All database operations are async

### Directory Structure
```
src/
├── api/             # FastAPI route handlers
├── core/            # Dependencies, logging, UOW
├── models/          # SQLAlchemy database models
├── schemas/         # Pydantic validation schemas
├── services/        # Business logic layer
├── repositories/    # Data access layer
├── engines/crewai/  # CrewAI engine implementation
└── main.py          # Application entry point
```

## Database Patterns

### Migrations
- Always create Alembic migrations for model changes
- Use `alembic revision --autogenerate` to create migrations
- Test migrations on SQLite before applying to PostgreSQL

### Connection Management
- When dealing with asyncpg and multiple event loops, asyncpg connections can conflict
- `USE_NULLPOOL=true` environment variable disables connection pooling when needed
- For testing with pytest-asyncio, use function-scoped fixtures instead of session-scoped ones

## Databricks Integration

### Model Configuration
Add new models in `src/seeds/model_configs.py`:
```python
"databricks-model-name": {
    "name": "databricks-model-name",
    "temperature": 0.7,
    "provider": "databricks",
    "context_window": 128000,
    "max_output_tokens": 25000
}
```

### Known Issues
- **databricks-gpt-oss Models**: Return reasoning blocks without "signature" field
- **Fix**: Automatic monkey patch in `llm_manager.py` handles missing signature fields
- **Long-term**: Upgrade to litellm 1.75.0+ when stable

### Authentication Hierarchy

#### General Databricks Services
1. OBO (On-Behalf-Of) authentication using user token
2. OAuth client credentials (DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET)
3. API key from service (DATABRICKS_TOKEN or DATABRICKS_API_KEY)
4. Environment variables as last resort

#### Vector Search (Direct Access Indexes)
**Authentication Priority for Vector Search:**

1. **OBO authentication** - User token from X-Forwarded-Access-Token header (preferred)
2. **PAT from database** - Encrypted tokens stored in database
3. **PAT from environment** - DATABRICKS_TOKEN or DATABRICKS_API_KEY
4. **Default SDK authentication** - Falls back to SDK's authentication chain

**Note**: Service Principal authentication has been removed from Vector Search operations as OBO and PAT tokens provide sufficient access for Direct Access indexes.

### Vector Search Limitations
- Doesn't support bulk delete operations
- To empty an index: retrieve all vectors (up to 10,000) then delete them
- For larger indexes, consider dropping and recreating

## Memory Backend

### Schema Layer Usage (CRITICAL)
**ALWAYS use the centralized schema layer `DatabricksIndexSchemas` for all Databricks Vector Search operations:**
- Use `DatabricksIndexSchemas.get_schema(memory_type)` to get field definitions
- Use `DatabricksIndexSchemas.get_search_columns(memory_type)` for search operations
- Use `DatabricksIndexSchemas.get_column_positions(memory_type)` for result parsing
- **NEVER hardcode column names or positions** - always reference the schema
- The schema supports memory types: "short_term", "long_term", "entity", "document"
- When saving to DatabricksVectorStorage, build records using only fields that exist in the schema

### Repository Pattern Usage (CRITICAL)
**ALWAYS use the repository pattern for database operations:**
- Use `DatabricksVectorIndexRepository` for all Vector Search index operations
- Repository methods handle async operations, authentication, and error handling
- Available repository methods:
  - `upsert()` - Insert or update records
  - `similarity_search()` - Search for similar vectors
  - `delete_records()` - Delete specific records
  - `count_documents()` - Count documents with optional filters
  - `describe_index()` - Get index metadata
- **NEVER call index client methods directly** - always go through the repository
- Handle async context properly when calling from sync code (use asyncio.run or ThreadPoolExecutor)

### Pydantic Schema Enum Handling (CRITICAL)
**When working with Pydantic schemas that have `use_enum_values = True`:**
- Enum fields like `IndexState` are automatically converted to string values
- **NEVER access `.value` attribute** - the field already contains the string value
- Example: `index_response.index.state` is already a string like "READY" or "NOT_FOUND"
- Common mistake: `index_response.index.state.value` will cause "'str' object has no attribute 'value'" error

### Disabled Configuration
- All memory types disabled = "Disabled Configuration"
- System ignores it and falls back to default ChromaDB + SQLite
- Default memory creates storage in `/Library/Application Support/kasal_default_[crew_id]/`

### Crew Memory Persistence
- Crew ID generated from hash of: agent roles, task names, crew name, model, run_name, group_id
- Same crew configuration gets same ID across runs
- Group_id ensures complete tenant isolation

## Testing Requirements

### Coverage
- Minimum 80% coverage required
- Run with: `python run_tests.py --coverage --html-coverage`

### Test Types
- **Unit Tests**: Mock all external dependencies
- **Integration Tests**: Test full request/response cycle
- Use pytest fixtures for test setup
- Test all API endpoints with different scenarios

## Critical Rules

- **ALWAYS use async/await** - Never use sync operations that could block the event loop
- **NEVER include real URLs in code** - Use "https://example.com" or environment variables
- **DO NOT restart backend service** - It auto-reloads with `--reload` flag
- **Check service status**: `ps aux | grep uvicorn`

## AI Engine Integration

### CrewAI Integration Points
- **Engine Service**: Main orchestration service
- **Configuration Adapter**: Transforms frontend configs to CrewAI format
- **Execution Runner**: Manages async execution workflows
- **Tool Factory**: Extensible tool system with custom tools

## Environment

- Python 3.9+ required
- Uses Poetry for dependency management (see pyproject.toml)
- Database type controlled by environment variables
- API keys for LLM services configured via environment