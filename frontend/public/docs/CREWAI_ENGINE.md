# CrewAI Engine Architecture

## Overview

The CrewAI Engine is a modular service that integrates the [CrewAI framework](https://docs.crewai.com) into the application. It orchestrates autonomous AI agents that collaborate to perform complex tasks through a well-defined workflow. The engine is designed with a clean separation of concerns, allowing for maintainability, extensibility, and testing.

## Core Architecture

The engine follows a layered architecture pattern:

```
┌───────────────────────────────────────┐
│            API Layer                  │
└───────────────┬───────────────────────┘
                │
┌───────────────▼───────────────────────┐
│           Service Layer               │
│    (CrewAIEngineService)              │
└───────────────┬───────────────────────┘
                │
┌───────────────▼───────────────────────┐
│         Specialized Modules           │
│  ┌─────────────┐  ┌────────────────┐  │
│  │Configuration│  │Crew Preparation│  │
│  └─────────────┘  └────────────────┘  │
│  ┌─────────────┐  ┌────────────────┐  │
│  │   Tracing   │  │   Execution    │  │
│  └─────────────┘  └────────────────┘  │
└───────────────┬───────────────────────┘
                │
┌───────────────▼───────────────────────┐
│         CrewAI Framework              │
└───────────────────────────────────────┘
```

## Key Components

### CrewAIEngineService

The main entry point and orchestrator that:
- Initializes the engine
- Manages execution lifecycles
- Coordinates between components
- Exposes public APIs for execution management

**Source**: `backend/src/engines/crewai/crewai_engine_service.py`

### Configuration Adapter

Handles transformation of configuration data between formats:
- Converts API/frontend config to engine format 
- Normalizes configs for consistent processing
- Extracts YAML configuration for agents and tasks
- Applies default values when needed

**Source**: `backend/src/engines/crewai/config_adapter.py`

### Crew Preparation

Responsible for creating all components needed for execution:
- Prepares agents with roles, goals, and tools
- Configures tasks with proper descriptions and assignments
- Creates output directories for execution artifacts
- Assembles the final Crew with proper configuration

**Source**: `backend/src/engines/crewai/crew_preparation.py`

### Execution Runner

Manages the actual execution of the Crew:
- Runs the crew asynchronously to prevent blocking
- Handles completion, cancellation, and failure cases
- Updates execution status with retry logic
- Cleans up resources after execution

**Source**: `backend/src/engines/crewai/execution_runner.py`

### Trace Management

Collects and processes trace data for monitoring and debugging:
- Maintains background tasks for processing trace data
- Batches traces for efficient database operations
- Manages start/stop of trace processing
- Coordinates with logging services

**Source**: `backend/src/engines/crewai/trace_management.py`

### Helper Modules

Specialized functions for specific aspects of agent/task management:
- `tool_helpers.py`: Tool preparation and assignment
- `agent_helpers.py`: Agent creation and configuration
- `task_helpers.py`: Task creation and conditional logic
- `conversion_helpers.py`: YAML parsing and configuration conversion

### Tool Registry

Central repository for available tools:
- Discovers and registers available CrewAI tools
- Manages custom tool implementations
- Provides tools to agents on demand
- Handles API key integration

**Source**: `backend/src/engines/crewai/tools/registry.py`

## Execution Flow

1. **Initialization**
   - Engine is initialized at application startup
   - Trace writer tasks are started
   - Tool registry loads available tools and configurations

2. **Configuration**
   - Frontend submits execution configuration
   - Configuration is normalized through the adapter
   - Planning parameters are properly configured when enabled

3. **Preparation**
   - Tools are prepared for the execution
   - Agents are created with appropriate roles and tools
   - Tasks are created and assigned to agents
   - Output directory is set up

4. **Execution**
   - Crew is created with configured parameters
   - Execution task is started asynchronously
   - Execution status is updated to "RUNNING"

5. **Monitoring**
   - Agent traces are collected during execution
   - Traces are processed and stored in the database
   - Execution logs are streamed for real-time monitoring

6. **Completion**
   - Execution completes successfully, fails, or is cancelled
   - Final status is updated with appropriate message
   - Results are stored in the database
   - Resources are cleaned up

## Special Features

### Planning Support

The engine supports CrewAI's planning feature, which enables:
- Automatic planning of task execution steps
- Addition of planning information to task descriptions
- Improved coordination between agents
- Configuration of planning-specific LLM

To enable planning, set the `planning` parameter to `true` in the execution configuration. You can also specify a different LLM for planning using the `planning_llm` parameter.

### Async Trace Processing

The engine uses an asynchronous trace processing mechanism to:
- Prevent blocking during trace collection
- Batch process traces for efficiency
- Ensure trace data is saved even if the main execution fails
- Provide real-time visibility into agent operations

### Tool Management

The tool registry provides a flexible approach to tools:
- Auto-discovery of built-in CrewAI tools
- Registration of custom tools
- API key management
- Context-sensitive tool provisioning

## Error Handling

The engine implements robust error handling:
- Exponential backoff for transient failures
- Status update retries with configurable limits
- Detailed error logging
- Exception isolation to prevent cascading failures

## Database Interaction

The engine follows the repository pattern for database access:
- No direct database access from service layer
- All database operations go through repositories
- Status updates use dedicated services
- Trace data is written asynchronously to avoid blocking

## Logging and Debugging

Comprehensive logging is implemented through:
- A centralized logging system
- Component-specific log prefixes
- Structured logs with execution context
- Debug-level detail when needed
- Execution traces for fine-grained visibility

## Integration Points

### Frontend Integration

The engine integrates with the frontend through:
- The execution API endpoints
- WebSocket connections for real-time updates
- Status queries for execution monitoring
- Execution trace retrieval for detailed inspection

### LLM Integration

The engine supports various LLM providers:
- OpenAI (default)
- Anthropic
- Local models
- Custom LLM configurations
- Model-specific parameters

## Configuration Options

Important configuration options include:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `planning` | Enable/disable planning feature | `false` |
| `planning_llm` | LLM model to use for planning | Same as main LLM |
| `process_type` | Process type (sequential/hierarchical) | `sequential` |
| `verbose` | Enable verbose logging | `true` |
| `memory` | Enable agent memory | `true` |
| `max_rpm` | Maximum requests per minute | `10` |

## Repository Structure

```
backend/src/engines/crewai/
├── __init__.py
├── crewai_engine_service.py    # Main engine service
├── config_adapter.py           # Configuration adapter
├── crew_preparation.py         # Crew preparation module
├── execution_runner.py         # Execution runner module
├── trace_management.py         # Trace management module
├── callbacks/                  # Event callbacks
├── crew_logger.py              # CrewAI logger
├── helpers/                    # Helper modules
│   ├── __init__.py
│   ├── agent_helpers.py
│   ├── task_helpers.py
│   ├── tool_helpers.py
│   └── conversion_helpers.py
└── tools/                      # Tool implementations
    ├── __init__.py
    └── registry.py
```

## Best Practices

When working with the CrewAI Engine:

1. **Configuration Handling**
   - Always use the configuration adapter for normalization
   - Provide sensible defaults for missing parameters
   - Validate configurations before execution

2. **Error Management**
   - Implement proper error handling at all levels
   - Use the retry mechanism for transient failures
   - Log errors with appropriate context

3. **Resource Cleanup**
   - Ensure all resources are properly cleaned up
   - Cancel background tasks when no longer needed
   - Close database connections and file handles

4. **Asynchronous Operations**
   - Use asynchronous code for I/O-bound operations
   - Avoid blocking the event loop
   - Implement proper cancellation handling

5. **Database Access**
   - Always use repositories for database access
   - Never access the database directly from services
   - Use transactions for related operations

## Example Usage

```python
# Initialize the engine
engine = await EngineFactory.get_engine(
    engine_type="crewai",
    db=db,
    initialize=True,
    llm_provider="openai",
    model="gpt-4o"
)

# Run an execution
execution_id = str(uuid4())
result = await engine.run_execution(
    execution_id=execution_id,
    execution_config=config
)

# Check status
status = await engine.get_execution_status(execution_id)

# Cancel execution if needed
success = await engine.cancel_execution(execution_id)
``` 