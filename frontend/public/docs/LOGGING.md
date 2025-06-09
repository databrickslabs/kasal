# Kasal Logging System

This document provides comprehensive documentation for the logging system in Kasal's AI agent workflow orchestration platform.

## Overview

Kasal uses a structured logging approach designed for AI agent workflow monitoring and debugging. The system captures execution traces, agent interactions, and system events to provide comprehensive observability for autonomous AI operations.

## Logging Architecture

The logging architecture consists of:

1. **Configuration** (`src/config/logging.py`): Contains all logging setup and configuration
2. **Log Storage** (`logs/` directory): Stores the actual log files (excluded from version control)
3. **Integration Points**: Code that uses the logging system across the application

## Directory Structure

```
backend/
├── src/
│   ├── config/
│   │   └── logging.py     # Logging configuration
│   ├── core/
│   │   └── logger.py      # Logger utilities
│   └── engines/crewai/
│       └── crew_logger.py # CrewAI-specific logging
├── logs/                  # Log files (gitignored)
│   ├── kasal.2023-06-15.log
│   ├── kasal.error.2023-06-15.log
│   ├── execution.2023-06-15.log
│   └── agent_traces.2023-06-15.log
└── ...
```

## Log File Naming Convention

Kasal log files follow domain-specific naming patterns:

- **Application logs**: `kasal.{date}.log`
- **Error logs**: `kasal.error.{date}.log`
- **Execution logs**: `execution.{date}.log`
- **Agent traces**: `agent_traces.{date}.log`
- **CrewAI engine**: `crewai_engine.{date}.log`

For example: `kasal.2023-06-15.log` or `execution.2023-06-15.log`

## Configuration Details

Kasal's logging system is configured in `backend/src/config/logging.py` and provides specialized logging for AI operations:

### Environment-Based Configuration

The logging configuration adapts based on deployment environment:

- **Development**: Verbose console output with DEBUG level for agent debugging
- **Databricks Apps**: Structured output optimized for Databricks logging infrastructure
- **Production**: Comprehensive file logging with execution trace collection

### Log Handlers

Kasal configures specialized handlers for different types of operations:

1. **Console Handler**: Real-time output for development and debugging
2. **Application File Handler**: General Kasal application events and system logs
3. **Execution File Handler**: Agent execution events, workflow progress, and completion status
4. **Trace File Handler**: Detailed agent interaction traces and CrewAI framework events
5. **Error File Handler**: Error events, failed executions, and system exceptions

### Log Formatting

Kasal uses context-aware formatting for different log types:

1. **Simple Format**: `%(asctime)s - %(levelname)s - %(message)s`
   - Used for development console output
   - Example: `2023-06-15 14:30:45 - INFO - CrewAI engine initialized`

2. **Execution Format**: `%(asctime)s - %(name)s - %(levelname)s - [EXEC:%(execution_id)s] - %(message)s`
   - Used for agent execution tracking
   - Example: `2023-06-15 14:30:45 - crewai.engine - INFO - [EXEC:abc123] - Agent started task execution`

3. **Verbose Format**: `%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s`
   - Used for file logging and system events
   - Example: `2023-06-15 14:30:45 - kasal.services.agent - INFO - [agent_service.py:45] - Agent created with ID abc123`

## Using the Logging System

### Getting a Logger

To use the logging system in Kasal code:

```python
from src.core.logger import logger

# Use the centralized logger for consistency
logger.debug("Agent configuration validated")
logger.info("Crew execution started")
logger.warning("LLM rate limit approaching")
logger.error("Agent execution failed")
logger.critical("CrewAI engine crashed")
```

### Execution Context Logging

For agent executions, include execution context:

```python
from src.core.logger import logger

# Log with execution ID for traceability
execution_id = "exec_abc123"
logger.info(f"[{execution_id}] Starting agent execution")
logger.info(f"[{execution_id}] Agent completed task: {task_name}")
logger.error(f"[{execution_id}] Agent failed: {error_message}")
```

### Kasal-Specific Logging Best Practices

1. **Use Structured Logging for AI Operations**: Include relevant context

```python
# Agent creation
logger.info("Agent created", extra={
    "agent_id": agent_id, 
    "agent_name": agent.name,
    "tools": agent.tools,
    "model": agent.model_config
})

# Execution tracking
logger.info("Execution started", extra={
    "execution_id": execution_id,
    "crew_size": len(crew.agents),
    "tasks_count": len(crew.tasks)
})
```

2. **Choose Appropriate Log Levels for AI Operations**:
   - `DEBUG`: Tool interactions, model API calls, detailed agent reasoning
   - `INFO`: Execution milestones, agent task completion, workflow progress
   - `WARNING`: LLM rate limits, tool failures, performance issues
   - `ERROR`: Agent execution failures, invalid configurations, API errors
   - `CRITICAL`: CrewAI engine crashes, system-wide failures

3. **Handle AI-Specific Exceptions**:

```python
try:
    result = await crew.kickoff()
except CrewAIException as e:
    logger.exception(f"CrewAI execution failed for {execution_id}")
except LLMRateLimitError as e:
    logger.warning(f"Rate limit hit during execution {execution_id}")
except Exception as e:
    logger.exception(f"Unexpected error in execution {execution_id}")
```

## Log Rotation

Kasal log files are automatically rotated to manage disk space:

- **Size-based rotation**: When files reach 10MB
- **Backup retention**: Maximum of 5 backup files per log type
- **Daily rotation**: Execution and trace logs rotate daily for better organization
- **Compression**: Older log files are compressed to save space

## Production Considerations

For Kasal production deployments (especially Databricks Apps):

1. **Databricks Integration**: Logs automatically integrate with Databricks logging infrastructure
2. **External Monitoring**: Configure additional handlers for Datadog, Sentry, or ELK stack
3. **Security**: Ensure logs don't contain sensitive information:
   - API keys and credentials
   - User data processed by agents
   - Proprietary business logic in agent prompts
4. **Performance Monitoring**: Set up alerts for:
   - Agent execution failures
   - LLM API errors
   - High execution times
   - Resource usage spikes
5. **Trace Sampling**: In high-volume environments, consider sampling agent traces to reduce log volume

## Initializing the Logging System

Kasal's logging system is automatically initialized during FastAPI startup:

```python
# In backend/src/main.py
from src.config.logging import setup_logging
from src.core.logger import logger

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize logging based on environment
    setup_logging()
    logger.info("Kasal logging system initialized")
    
    # Initialize AI engines with logging
    await initialize_engines()
    
    yield
    
    logger.info("Kasal shutdown complete")
```

## CrewAI Engine Logging

The CrewAI engine has specialized logging for AI operations:

```python
# In backend/src/engines/crewai/crew_logger.py
class CrewLogger:
    def log_agent_start(self, agent_name: str, task: str):
        logger.info(f"Agent {agent_name} starting task: {task}")
    
    def log_tool_use(self, agent_name: str, tool_name: str, input_data: str):
        logger.debug(f"Agent {agent_name} using tool {tool_name}: {input_data}")
    
    def log_agent_complete(self, agent_name: str, result: str):
        logger.info(f"Agent {agent_name} completed task with result: {result[:100]}...")
```

## Monitoring AI Agent Workflows

Kasal's logging system provides comprehensive monitoring for AI agent workflows:

### Execution Tracking
- Agent lifecycle events (start, task completion, finish)
- Tool usage and API calls
- LLM interactions and token usage
- Workflow progress and decision points

### Performance Monitoring
- Execution duration and resource usage
- LLM response times and rate limiting
- Tool performance and failure rates
- System resource utilization

### Debugging Support
- Detailed execution traces for failed workflows
- Agent reasoning and decision logging
- Tool input/output capture
- Error context and stack traces

## Conclusion

Kasal's logging system is designed specifically for AI agent workflow orchestration, providing the observability needed to monitor, debug, and optimize autonomous AI operations across development and production environments. 