"""
Logging configuration for CrewAI executions.

This module provides utilities to configure logging for CrewAI executions,
ensuring that logs go to the appropriate files with execution context.
"""

import logging
import sys
import os
import threading
from typing import Optional, Any
from contextlib import contextmanager

# Thread-local storage for execution context
_execution_context = threading.local()


class ExecutionContextFormatter(logging.Formatter):
    """
    Custom formatter that adds execution ID to log messages.
    Preserves the prefix from the original format string.
    """

    def __init__(self, fmt=None, datefmt=None, style='%'):
        super().__init__(fmt, datefmt, style)
        # Extract prefix from original format (e.g., "[FLOW]" or "[CREW]")
        self._original_fmt = fmt or '[CREW] %(asctime)s - %(levelname)s - %(message)s'
        # Extract the prefix by finding the pattern [SOMETHING]
        import re
        match = re.match(r'(\[[\w]+\])', self._original_fmt)
        self._prefix = match.group(1) if match else '[CREW]'

    def format(self, record):
        # Get execution ID from thread-local storage
        execution_id = getattr(_execution_context, 'execution_id', None)

        if execution_id:
            # Add execution ID to the format
            record.exec_id = f"[{execution_id[:8]}]"
        else:
            record.exec_id = ""

        # Use the original format with execution ID, preserving the prefix
        if record.exec_id:
            self._style._fmt = f'{self._prefix}%(exec_id)s %(asctime)s - %(levelname)s - %(message)s'
        else:
            self._style._fmt = f'{self._prefix} %(asctime)s - %(levelname)s - %(message)s'

        return super().format(record)


def set_execution_context(execution_id: str):
    """
    Set the execution ID for the current thread.

    Args:
        execution_id: The execution ID to associate with logs
    """
    _execution_context.execution_id = execution_id


class ExecutionLogsDatabaseHandler(logging.Handler):
    """
    Custom logging handler that writes logs directly to the execution_logs database.
    Uses synchronous database operations since we're in a subprocess.

    ⚠️ IMPORTANT: This handler is for execution_logs ONLY, not execution_trace!

    execution_logs vs execution_trace:
    - execution_logs: Raw logs from subprocess (crew.log, stdout, stderr)
                     Written by this handler from within the subprocess
                     Contains unstructured log messages

    - execution_trace: Structured events from CrewAI event bus
                      Written by event listeners (AgentTraceEventListener, etc.)
                      Contains structured events like task_started, agent_execution
                      Uses the original GroupContext object

    DO NOT mix these two systems or try to unify their implementations!
    """

    def __init__(self, execution_id: str, group_context: Any = None, log_queue: Any = None):
        """
        Initialize the handler.

        Args:
            execution_id: The execution ID for this handler
            group_context: Optional group context for multi-tenant isolation
            log_queue: Optional multiprocessing Queue for sending logs to main process
        """
        super().__init__()
        self.execution_id = execution_id
        self.group_context = group_context
        self.log_queue = log_queue  # Queue for sending logs to main process
        self._db_url = None

        # Debug log the initialization
        from src.core.logger import get_logger
        logger = get_logger('crew')
        logger.info(f"[DB_HANDLER] Initialized with execution_id={execution_id}")
        logger.info(f"[DB_HANDLER] log_queue provided: {log_queue is not None}")
        if group_context:
            logger.info(f"[DB_HANDLER] Group context provided - type: {type(group_context)}")
            if hasattr(group_context, '__dict__'):
                logger.info(f"[DB_HANDLER] Group context attributes: {group_context.__dict__}")
        else:
            logger.warning(f"[DB_HANDLER] No group context provided for execution {execution_id}")

        self._init_db()

    def _init_db(self):
        """Initialize database connection settings."""
        import os
        import pathlib
        from src.core.logger import get_logger

        logger = get_logger('crew')

        # Log environment variables for debugging
        db_type_env = os.environ.get('DATABASE_TYPE', 'not set')
        logger.info(f"[DB_HANDLER] DATABASE_TYPE env var: {db_type_env}")

        # First check for DATABASE_URL environment variable
        self._db_url = os.environ.get('DATABASE_URL')

        if not self._db_url:
            # Import settings to get the proper database configuration
            try:
                from src.config.settings import settings

                # Determine database type and construct appropriate sync URL
                db_type = settings.DATABASE_TYPE.lower() if settings.DATABASE_TYPE else 'postgres'
                logger.info(f"[DB_HANDLER] Settings DATABASE_TYPE: {db_type}")

                if db_type == 'sqlite':
                    # For SQLite, use the configured path
                    sqlite_path = settings.SQLITE_DB_PATH or './app.db'
                    # Make sure we use absolute path
                    if not os.path.isabs(sqlite_path):
                        backend_root = pathlib.Path(__file__).parent.parent.parent.parent
                        sqlite_path = str((backend_root / sqlite_path).absolute())
                    self._db_url = f'sqlite:///{sqlite_path}'
                    logger.info(f"[DB_HANDLER] Using SQLite database: {sqlite_path}")
                else:
                    # For PostgreSQL, build connection string from settings
                    postgres_user = settings.POSTGRES_USER or 'postgres'
                    postgres_password = settings.POSTGRES_PASSWORD or 'postgres'
                    postgres_server = settings.POSTGRES_SERVER or 'localhost'
                    postgres_port = settings.POSTGRES_PORT or '5432'
                    postgres_db = settings.POSTGRES_DB or 'kasal'

                    # Build PostgreSQL URL (we'll convert to asyncpg in _write_to_db_async)
                    self._db_url = f'postgresql://{postgres_user}:{postgres_password}@{postgres_server}:{postgres_port}/{postgres_db}'
                    logger.info(f"[DB_HANDLER] Using PostgreSQL database: {postgres_db} on {postgres_server}:{postgres_port}")

            except ImportError as e:
                # If settings cannot be imported, use SQLite fallback
                logger.warning(f"[DB_HANDLER] Could not import settings: {e}")
                backend_root = pathlib.Path(__file__).parent.parent.parent.parent
                db_path = backend_root / 'app.db'
                self._db_url = f'sqlite:///{db_path.absolute()}'
                logger.info(f"[DB_HANDLER] Using SQLite fallback due to import error")
        else:
            # DATABASE_URL is set in environment
            logger.info(f"[DB_HANDLER] Using DATABASE_URL from environment")

    def emit(self, record):
        """
        Emit a log record by writing directly to the database.

        Args:
            record: The log record to emit
        """
        try:
            # Debug: Print that emit was called
            print(f"[DB_HANDLER EMIT] Called for {self.execution_id[:8]}, queue={self.log_queue is not None}")

            # Skip database handler's own logs to avoid recursion
            if record.name == 'crew' and '[DB_HANDLER]' in record.getMessage():
                return

            # Format the log message
            msg = self.format(record)

            # Write directly to database using synchronous operations
            self._write_to_db_sync(msg)
        except Exception as e:
            # Log errors to crew.log instead of database to avoid recursion
            import logging
            logger = logging.getLogger('crew')
            # Only log to file handler, not database handler
            for handler in logger.handlers[:]:
                if not isinstance(handler, ExecutionLogsDatabaseHandler):
                    handler.emit(logging.LogRecord(
                        name='crew',
                        level=logging.ERROR,
                        pathname='',
                        lineno=0,
                        msg=f"[DB_HANDLER] Error writing to database: {str(e)}",
                        args=(),
                        exc_info=None
                    ))

    def _write_to_db_sync(self, content: str):
        """
        Write log to database or queue.
        If log_queue is available (subprocess mode), send to queue.
        Otherwise, write directly to database (main process mode).

        Args:
            content: The log content to write
        """
        try:
            import asyncio
            from datetime import datetime

            # Prepare the data
            log_data = {
                'execution_id': self.execution_id,
                'content': content,
                'timestamp': datetime.utcnow()
            }

            # Add group context if available
            # ⚠️ WARNING: group_context handling is critical for execution_trace
            # DO NOT change how group_context is passed or accessed here
            # It MUST support both dict and object forms for compatibility
            if self.group_context:
                # Handle both dict and object forms of group_context
                if isinstance(self.group_context, dict):
                    # If it's a dict, get values directly
                    log_data['group_id'] = self.group_context.get('primary_group_id') or self.group_context.get('group_id')
                    log_data['group_email'] = self.group_context.get('group_email')
                else:
                    # If it's an object (GroupContext), use getattr
                    # This is the normal case when execution_trace is working
                    log_data['group_id'] = getattr(self.group_context, 'primary_group_id', None) or getattr(self.group_context, 'group_id', None)
                    log_data['group_email'] = getattr(self.group_context, 'group_email', None)

                # Debug logging to verify group context
                if not log_data['group_id'] and not log_data['group_email']:
                    import logging
                    debug_logger = logging.getLogger('crew')
                    debug_logger.warning(f"[DB_HANDLER] Group context exists but values are None - group_context: {self.group_context}, type: {type(self.group_context)}")
                    if hasattr(self.group_context, '__dict__'):
                        debug_logger.warning(f"[DB_HANDLER] Group context attributes: {self.group_context.__dict__}")
            else:
                log_data['group_id'] = None
                log_data['group_email'] = None

            # If we have a log_queue (subprocess mode), use it instead of direct DB write
            # This follows the same pattern as execution_trace - collect in subprocess, write in main
            if self.log_queue is not None:
                try:
                    # Put log data in queue for main process to write
                    self.log_queue.put_nowait(log_data)
                    print(f"[SUBPROCESS LOG QUEUE] Added log to queue for {self.execution_id[:8]}, content: {log_data.get('content', '')[:50]}...")

                    # Debug log to confirm queue usage
                    from src.core.logger import get_logger
                    logger = get_logger('crew')
                    for handler in logger.handlers[:]:
                        if not isinstance(handler, ExecutionLogsDatabaseHandler):
                            handler.emit(logging.LogRecord(
                                name='crew',
                                level=logging.DEBUG,
                                pathname='',
                                lineno=0,
                                msg=f"[DB_HANDLER] Queued log for {self.execution_id[:8]} (queue size approx: {self.log_queue.qsize()})",
                                args=(),
                                exc_info=None
                            ))
                            break
                    return  # Exit early - main process will handle DB write
                except Exception as queue_error:
                    # If queue fails, fall back to direct DB write
                    from src.core.logger import get_logger
                    logger = get_logger('crew')
                    logger.warning(f"[DB_HANDLER] Failed to queue log: {queue_error}, falling back to direct write")

            # Check if we're in SQLite or PostgreSQL mode
            if 'sqlite' in self._db_url:
                # Use synchronous SQLite operations
                import sqlite3
                from urllib.parse import urlparse

                # Extract the database path from the URL
                parsed = urlparse(self._db_url)
                db_path = parsed.path.lstrip('/')

                # Connect and insert
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO execution_logs (execution_id, content, timestamp, group_id, group_email)
                    VALUES (?, ?, ?, ?, ?)
                """, (log_data['execution_id'], log_data['content'], log_data['timestamp'],
                      log_data['group_id'], log_data['group_email']))
                conn.commit()

                # Debug: Log successful write (but avoid recursion)
                if '[DB_WRITE_SUCCESS]' not in log_data['content']:
                    from src.core.logger import get_logger
                    file_logger = get_logger('crew')
                    for handler in file_logger.handlers[:]:
                        if not isinstance(handler, ExecutionLogsDatabaseHandler):
                            handler.emit(logging.LogRecord(
                                name='crew',
                                level=logging.DEBUG,
                                pathname='',
                                lineno=0,
                                msg=f"[DB_WRITE_SUCCESS] Written log to execution_logs for {log_data['execution_id'][:8]}",
                                args=(),
                                exc_info=None
                            ))
                            break

                conn.close()
            else:
                # For PostgreSQL, check if we're already in an async context
                try:
                    loop = asyncio.get_running_loop()
                    # We're already in an async context, create a task
                    task = loop.create_task(self._write_to_db_async(log_data))
                    # Don't wait for it to complete (fire and forget)
                except RuntimeError:
                    # No running loop, use asyncio.run()
                    asyncio.run(self._write_to_db_async(log_data))

        except Exception as e:
            # Log error to crew.log
            from src.core.logger import get_logger
            logger = get_logger('crew')
            # Find file handler only
            for handler in logger.handlers[:]:
                if not isinstance(handler, ExecutionLogsDatabaseHandler):
                    handler.emit(logging.LogRecord(
                        name='crew',
                        level=logging.ERROR,
                        pathname='',
                        lineno=0,
                        msg=f"[DB_HANDLER] Database write error: {str(e)}",
                        args=(),
                        exc_info=None
                    ))

    async def _write_to_db_async(self, log_data: dict):
        """
        Async helper for PostgreSQL writes.

        Args:
            log_data: The log data dictionary to write
        """
        try:
            from sqlalchemy.ext.asyncio import create_async_engine
            from sqlalchemy import text

            # Convert the sync PostgreSQL URL to async
            async_url = self._db_url
            if 'postgresql+pg8000' in async_url:
                async_url = async_url.replace('postgresql+pg8000', 'postgresql+asyncpg')
            elif 'postgresql://' in async_url:
                async_url = async_url.replace('postgresql://', 'postgresql+asyncpg://')

            # Create async engine
            engine = create_async_engine(async_url)

            # Execute the insert
            async with engine.begin() as conn:
                await conn.execute(text("""
                    INSERT INTO execution_logs (execution_id, content, timestamp, group_id, group_email)
                    VALUES (:execution_id, :content, :timestamp, :group_id, :group_email)
                """), log_data)

            await engine.dispose()
        except Exception as e:
            raise Exception(f"Async database write failed: {str(e)}")


def clear_execution_context():
    """
    Clear the execution context for the current thread.
    """
    if hasattr(_execution_context, 'execution_id'):
        del _execution_context.execution_id


@contextmanager
def execution_logging_context(execution_id: str):
    """
    Context manager for execution-specific logging.

    Args:
        execution_id: The execution ID to use for logging
    """
    set_execution_context(execution_id)
    try:
        yield
    finally:
        clear_execution_context()


def configure_subprocess_logging(execution_id: str, process_type: str = "crew"):
    """
    Configure logging for a subprocess running a crew or flow execution.

    This function:
    1. Redirects stdout/stderr to prevent terminal output
    2. Configures all loggers to write to crew.log or flow.log with execution ID
    3. Suppresses verbose output from CrewAI and dependencies

    Args:
        execution_id: The execution ID to include in logs
        process_type: Type of process ("crew" or "flow") - defaults to "crew" for backward compatibility
    """
    import os  # Import at the top of the function

    # Set execution context
    set_execution_context(execution_id)

    # Suppress CrewAI verbose output
    os.environ['CREWAI_VERBOSE'] = 'false'
    os.environ['PYTHONUNBUFFERED'] = '0'

    # Configure root logger to suppress console output
    from src.core.logger import get_logger
    root_logger = logging.getLogger()
    root_logger.handlers = []
    root_logger.setLevel(logging.WARNING)

    # Suppress specific noisy loggers using centralized logger configuration
    # NOTE: 'crewai' is configured separately below to write to flow.log/crew.log
    for logger_name in [
        'openai',
        'httpx',
        'httpcore',
        'urllib3',
        'requests',
        'asyncio',
        'PIL',
        'matplotlib',
        'langchain',
        'mlflow',
        'mlflow.tracing',
        'mlflow.models',
        'mlflow.evaluate',
        'opentelemetry'
    ]:
        logger = get_logger(logger_name)
        logger.setLevel(logging.WARNING)
        logger.propagate = False

    # Import LoggerManager and configure crew logger
    from src.core.logger import LoggerManager

    # Get the log directory from environment or determine dynamically
    log_dir = os.environ.get('LOG_DIR')
    if not log_dir:
        # Determine log directory relative to backend root
        import pathlib
        backend_root = pathlib.Path(__file__).parent.parent.parent.parent
        log_dir = backend_root / 'logs'

    # Get or create logger manager with the correct log directory
    logger_manager = LoggerManager.get_instance(log_dir)
    logger_manager.initialize()

    # Get the appropriate logger based on process_type
    if process_type.lower() == "flow":
        exec_logger = logger_manager.flow
        log_prefix = "[FLOW]"
        log_filename = "flow.log"
    else:
        exec_logger = logger_manager.crew
        log_prefix = "[CREW]"
        log_filename = "crew.log"

    # IMPORTANT: Clear any existing level configuration
    # The logger might have been pre-configured with WARNING level
    exec_logger.setLevel(logging.NOTSET)  # Clear first
    exec_logger.setLevel(logging.INFO)     # Then set to INFO

    # Remove only console (non-file) StreamHandlers, keep FileHandlers
    exec_logger.handlers = [h for h in exec_logger.handlers
                            if not isinstance(h, logging.StreamHandler) or isinstance(h, logging.FileHandler)]

    # IMPORTANT: If no file handlers exist, create one
    log_path = os.path.join(log_dir, log_filename)

    # Check if we already have a file handler for the log file
    # CRITICAL: Update formatter for ALL file handlers to use correct prefix
    file_handler = None
    for handler in exec_logger.handlers:
        if isinstance(handler, logging.FileHandler):
            file_handler = handler
            # ALWAYS update formatter with correct prefix for this process type
            handler.setFormatter(ExecutionContextFormatter(
                fmt=f'{log_prefix} %(asctime)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            ))

    # If no file handler exists, create one
    if not file_handler:
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(ExecutionContextFormatter(
            fmt=f'{log_prefix} %(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))
        exec_logger.addHandler(file_handler)

    # Check if debug logging is enabled via environment variables
    import os

    # Determine which environment variable to check based on process_type
    env_var_name = f'KASAL_LOG_{process_type.upper()}'

    # Support both old KASAL_DEBUG_TRACES and new KASAL_LOG_CREW/KASAL_LOG_FLOW
    debug_enabled = (
        os.environ.get('KASAL_DEBUG_TRACES', '').lower() in ['true', '1', 'yes'] or
        os.environ.get('KASAL_DEBUG_ALL', '').lower() in ['true', '1', 'yes'] or
        os.environ.get(env_var_name, '').upper() == 'DEBUG'
    )

    # Determine log level from environment
    process_log_level = os.environ.get(env_var_name, '').upper()
    if process_log_level == 'DEBUG':
        log_level = logging.DEBUG
    elif process_log_level == 'INFO':
        log_level = logging.INFO
    elif process_log_level == 'WARNING':
        log_level = logging.WARNING
    elif process_log_level == 'ERROR':
        log_level = logging.ERROR
    elif process_log_level == 'CRITICAL':
        log_level = logging.CRITICAL
    elif process_log_level == 'OFF':
        log_level = logging.CRITICAL + 1
    elif debug_enabled:
        log_level = logging.DEBUG
    else:
        # Fall back to global KASAL_LOG_LEVEL or INFO
        # CRITICAL: Default to INFO for subprocess execution logging
        global_level = os.environ.get('KASAL_LOG_LEVEL', '').upper()
        if global_level == 'DEBUG':
            log_level = logging.DEBUG
        elif global_level == 'INFO':
            log_level = logging.INFO
        elif global_level == 'WARNING':
            log_level = logging.WARNING
        elif global_level == 'ERROR':
            log_level = logging.ERROR
        elif global_level == 'CRITICAL':
            log_level = logging.CRITICAL
        elif global_level == 'OFF':
            log_level = logging.CRITICAL + 1
        else:
            # No environment variables set - default to INFO for subprocess logging
            log_level = logging.INFO

    # Set the logger level
    exec_logger.setLevel(log_level)

    if log_level == logging.DEBUG:
        exec_logger.info(f"[TRACE_DEBUG] Debug logging enabled for {process_type} execution")

    # Apply file handler to all relevant loggers
    for logger_name in [
        'crewai',  # CrewAI framework logs (crew kickoff, task execution, etc.)
        'src.engines.crewai.callbacks.logging_callbacks',
        'src.engines.crewai.callbacks.execution_callback',
        'src.engines.crewai.trace_management',
        'src.services.trace_queue',  # Add trace queue logger
        'src.engines.crewai.execution_runner',  # Add execution runner logger
        'src.services.databricks_knowledge_service',  # Add knowledge service logger for search debugging
        'src.engines.crewai.tools.custom.powerbi_analysis_tool',  # Add PowerBI tool logger
        'src.engines.crewai.tools.custom.databricks_jobs_tool',  # Add Databricks jobs tool logger
        '__main__'  # For any direct logging in subprocess
    ]:
        module_logger = get_logger(logger_name)
        module_logger.handlers = []  # Clear existing handlers
        module_logger.addHandler(file_handler)
        module_logger.setLevel(log_level)
        module_logger.propagate = False

    # Capture MLflow errors (suppress warnings) explicitly
    mlflow_logger = get_logger('mlflow')
    mlflow_logger.handlers = []
    mlflow_logger.addHandler(file_handler)
    mlflow_logger.setLevel(logging.ERROR)
    mlflow_logger.propagate = False

    return exec_logger


def suppress_stdout_stderr():
    """
    Completely suppress stdout and stderr output.

    Returns:
        Tuple of (original_stdout, original_stderr, captured_output)
    """
    import io

    original_stdout = sys.stdout
    original_stderr = sys.stderr
    captured_output = io.StringIO()

    # Redirect both stdout and stderr to the same StringIO
    sys.stdout = captured_output
    sys.stderr = captured_output

    return original_stdout, original_stderr, captured_output


def restore_stdout_stderr(original_stdout, original_stderr):
    """
    Restore original stdout and stderr.

    Args:
        original_stdout: Original sys.stdout
        original_stderr: Original sys.stderr
    """
    sys.stdout = original_stdout
    sys.stderr = original_stderr