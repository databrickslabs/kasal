# Logging Guide

This document provides comprehensive documentation for the logging system in our backend architecture.

## Overview

The application uses a structured logging approach that separates configuration from log storage. This design follows best practices for maintainability, observability, and separation of concerns.

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
│   └── ...
├── logs/                 # Log files (gitignored)
│   ├── backend.2023-06-15.log
│   ├── backend.error.2023-06-15.log
│   └── ...
└── ...
```

## Log File Naming Convention

Log files follow this naming pattern:

- Regular logs: `backend.{date}.log`
- Error logs: `backend.error.{date}.log`

For example: `backend.2023-06-15.log` or `backend.error.2023-06-15.log`

## Configuration Details

The logging system is configured in `src/config/logging.py` and provides:

### Environment-Based Configuration

The logging configuration adapts based on the application environment:

- **Development**: Verbose console output with DEBUG level, formatted for readability
- **Staging**: More structured output with INFO level
- **Production**: Minimal console output but comprehensive file logging

### Log Handlers

Three types of handlers are configured:

1. **Console Handler**: Outputs logs to the console (stdout)
2. **File Handler**: Writes all INFO and above logs to a daily log file
3. **Error File Handler**: Writes only ERROR and above logs to a separate error log file

### Log Formatting

Two formats are available:

1. **Simple Format**: `%(asctime)s - %(levelname)s - %(message)s`
   - Used for development console output
   - Example: `2023-06-15 14:30:45 - INFO - Application started`

2. **Verbose Format**: `%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s`
   - Used for file logging and production environments
   - Example: `2023-06-15 14:30:45 - myapp.services - INFO - [user_service.py:45] - User created with ID 123`

## Using the Logging System

### Getting a Logger

To use the logging system in your code:

```python
from src.config.logging import get_logger

# Use the module name for the logger
logger = get_logger(__name__)

# Now use the logger
logger.debug("Detailed debugging information")
logger.info("Normal application event")
logger.warning("Something unexpected but not critical")
logger.error("Something failed but application continues")
logger.critical("Application cannot continue")
```

### Logging Best Practices

1. **Use Structured Logging**: Include relevant context in your log messages

```python
# Instead of:
logger.info(f"User {user_id} created")

# Use:
logger.info("User created", extra={"user_id": user_id, "email": email})
```

2. **Choose Appropriate Log Levels**:
   - `DEBUG`: Detailed information for debugging
   - `INFO`: Confirmation that things are working
   - `WARNING`: Something unexpected but not an error
   - `ERROR`: An error that prevents a function from working
   - `CRITICAL`: An error that prevents the application from working

3. **Include Exception Information**:

```python
try:
    # Some code that might raise an exception
    result = complex_operation()
except Exception as e:
    logger.exception("Failed to perform complex operation")
    # The exception() method automatically includes the stack trace
```

## Log Rotation

Log files are automatically rotated when they reach 10MB, with a maximum of 5 backup files kept for each log type. This prevents logs from consuming excessive disk space.

## Production Considerations

In production environments, consider:

1. **External Log Aggregation**: Configure additional handlers for services like ELK, Datadog, or Sentry
2. **Log Security**: Ensure logs don't contain sensitive information (PII, credentials, etc.)
3. **Monitoring**: Set up alerts based on ERROR and CRITICAL log events

## Initializing the Logging System

The logging system is automatically initialized when the application starts:

```python
# In main.py or startup code
from src.config.logging import setup_logging

# Initialize logging with the current environment
setup_logging(env="development")  # or "production", "staging"
```

## Conclusion

Following these logging practices ensures that our application produces consistent, useful logs that aid in debugging, monitoring, and understanding system behavior across all environments. 