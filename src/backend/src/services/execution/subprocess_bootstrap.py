"""
Logging bootstrap for a spawned execution subprocess.

Not "logging config" in general — this is the one function a child interpreter
calls before it runs anything, plus the stdout/stderr muzzle that keeps engine
chatter off the terminal. Its only callers are the crew and flow runners.
"""

import logging
import os
import sys

from src.services.execution.logs.context import (
    ExecutionContextFormatter,
    set_execution_context,
)


def prepare_child_environment(execution_id: str, process_type: str = "crew") -> None:
    """The first thing a spawned crew/flow interpreter does with its environment.

    1. Applies the child allow-list (``core/databricks_app``): this interpreter
       runs ONE workspace's execution, the server that spawned it serves many,
       so nothing but platform facts (the app service principal included), Kasal
       settings, process control and DB/logging configuration survives. Never a
       workspace credential — the run reads its keys through the services.
       ``multiprocessing`` spawn cannot be handed an environment, hence here.
       Skipped when this is not a spawned child (unit tests call the child entry
       points in-process, and must not have the test runner's env scrubbed).
    2. Marks the subprocess mode and the execution id (orphan detection).
    3. Pins ``DATABASE_TYPE`` for the BASE engine to what the parent's settings
       resolved. It names the boot database, not where the run's data lives:
       with Lakebase active the child re-activates it from the same config
       (``activate_lakebase_in_subprocess``), exactly like the parent.
    """
    import multiprocessing

    if multiprocessing.parent_process() is not None:
        from src.core.databricks_app import restrict_environment_to_child_allow_list

        restrict_environment_to_child_allow_list()

    os.environ["CREW_SUBPROCESS_MODE"] = "true"
    if process_type == "flow":
        os.environ["FLOW_SUBPROCESS_MODE"] = "true"
        os.environ["CREWAI_DEBUG_TRACING"] = "true"
    os.environ["KASAL_EXECUTION_ID"] = execution_id
    if "DATABASE_TYPE" not in os.environ:
        from src.config.settings import settings

        os.environ["DATABASE_TYPE"] = settings.DATABASE_TYPE


async def open_child_database() -> bool:
    """Point this child at the server's database, then load what it configured.

    Re-activates Lakebase (it is not inherited from the parent's hot-swap) and
    THEN loads the Configuration → Engines settings snapshot, so they are read
    from the database the server uses. Returns whether Lakebase is active.
    """
    from src.db.database_router import activate_lakebase_in_subprocess
    from src.services.settings import engine_settings_loader

    lakebase_active = await activate_lakebase_in_subprocess()
    await engine_settings_loader.load()
    return lakebase_active


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

    # Set execution context
    set_execution_context(execution_id)

    # Suppress CrewAI verbose output
    os.environ["CREWAI_VERBOSE"] = "false"
    os.environ["PYTHONUNBUFFERED"] = "0"

    # Configure root logger to suppress console output
    from src.core.logger import get_logger

    root_logger = logging.getLogger()
    root_logger.handlers = []
    root_logger.setLevel(logging.WARNING)

    # Suppress specific noisy loggers using centralized logger configuration
    # NOTE: 'kasal_engine' is configured separately below to write to flow.log/crew.log
    for logger_name in [
        "openai",
        "httpx",
        "httpcore",
        "urllib3",
        "requests",
        "asyncio",
        "PIL",
        "matplotlib",
        "langchain",
        "mlflow",
        "mlflow.tracing",
        "mlflow.models",
        "mlflow.evaluate",
        "opentelemetry",
    ]:
        logger = get_logger(logger_name)
        logger.setLevel(logging.WARNING)
        logger.propagate = False

    # Import LoggerManager and configure crew logger
    from src.core.logger import LoggerManager

    # Get the log directory from environment or determine dynamically
    log_dir = os.environ.get("LOG_DIR")
    if not log_dir:
        # Determine log directory relative to backend root
        import pathlib

        backend_root = pathlib.Path(__file__).parent.parent.parent.parent
        log_dir = backend_root / "logs"

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
    exec_logger.setLevel(logging.INFO)  # Then set to INFO

    # Remove only console (non-file) StreamHandlers, keep FileHandlers
    exec_logger.handlers = [
        h
        for h in exec_logger.handlers
        if not isinstance(h, logging.StreamHandler)
        or isinstance(h, logging.FileHandler)
    ]

    # IMPORTANT: If no file handlers exist, create one
    log_path = os.path.join(log_dir, log_filename)

    # Check if we already have a file handler for the log file
    # CRITICAL: Update formatter for ALL file handlers to use correct prefix
    file_handler = None
    for handler in exec_logger.handlers:
        if isinstance(handler, logging.FileHandler):
            file_handler = handler
            # ALWAYS update formatter with correct prefix for this process type
            handler.setFormatter(
                ExecutionContextFormatter(
                    fmt=f"{log_prefix} %(asctime)s - %(levelname)s - %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
            )

    # If no file handler exists, create one
    if not file_handler:
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(
            ExecutionContextFormatter(
                fmt=f"{log_prefix} %(asctime)s - %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        exec_logger.addHandler(file_handler)

    # In Databricks Apps, also output to stderr so logs are captured by the platform
    # DATABRICKS_APP_NAME is set when running in Databricks Apps
    # Use sys.__stderr__ to bypass suppress_stdout_stderr() which redirects sys.stderr
    console_handler = None
    if os.environ.get("DATABRICKS_APP_NAME"):
        console_handler = logging.StreamHandler(sys.__stderr__)
        console_handler.setFormatter(
            ExecutionContextFormatter(
                fmt=f"{log_prefix} %(asctime)s - %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        console_handler.setLevel(logging.INFO)
        exec_logger.addHandler(console_handler)

    # Check if debug logging is enabled via environment variables

    # Determine which environment variable to check based on process_type
    env_var_name = f"KASAL_LOG_{process_type.upper()}"

    debug_enabled = (
        os.environ.get("KASAL_DEBUG_ALL", "").lower() in ["true", "1", "yes"]
        or os.environ.get(env_var_name, "").upper() == "DEBUG"
    )

    # Determine log level from environment
    process_log_level = os.environ.get(env_var_name, "").upper()
    if process_log_level == "DEBUG":
        log_level = logging.DEBUG
    elif process_log_level == "INFO":
        log_level = logging.INFO
    elif process_log_level == "WARNING":
        log_level = logging.WARNING
    elif process_log_level == "ERROR":
        log_level = logging.ERROR
    elif process_log_level == "CRITICAL":
        log_level = logging.CRITICAL
    elif process_log_level == "OFF":
        log_level = logging.CRITICAL + 1
    elif debug_enabled:
        log_level = logging.DEBUG
    else:
        # Fall back to global KASAL_LOG_LEVEL or INFO
        # CRITICAL: Default to INFO for subprocess execution logging
        global_level = os.environ.get("KASAL_LOG_LEVEL", "").upper()
        if global_level == "DEBUG":
            log_level = logging.DEBUG
        elif global_level == "INFO":
            log_level = logging.INFO
        elif global_level == "WARNING":
            log_level = logging.WARNING
        elif global_level == "ERROR":
            log_level = logging.ERROR
        elif global_level == "CRITICAL":
            log_level = logging.CRITICAL
        elif global_level == "OFF":
            log_level = logging.CRITICAL + 1
        else:
            # No environment variables set - default to INFO for subprocess logging
            log_level = logging.INFO

    # Set the logger level
    exec_logger.setLevel(log_level)

    if log_level == logging.DEBUG:
        exec_logger.info(
            f"[TRACE_DEBUG] Debug logging enabled for {process_type} execution"
        )

    # Apply file handler (and console handler in Databricks Apps) to all relevant loggers
    for logger_name in [
        "kasal_engine",  # engine logs (crew kickoff, task execution, etc.)
        "src.services.execution.kernel.execution_callback",
        "src.services.execution.logs.writer_task",
        "src.services.trace.queue",  # Add trace queue logger
        "src.services.agent_builder.execution_runner",  # Add execution runner logger
        "src.services.knowledge.databricks_service",  # Add knowledge service logger for search debugging
        "src.services.tools.tool_factory",  # Tool factory creation + config injection logs
        "src.services.tools.powerbi_analysis_tool",  # Add PowerBI tool logger
        "src.services.tools.powerbi_semantic_model_dax_tool",  # DAX Generator tool logs
        "src.services.tools.powerbi_metadata_reducer_tool",  # Metadata Reducer tool logs
        "src.services.tools.powerbi_semantic_model_fetcher_tool",  # Fetcher tool logs
        "src.services.tools.databricks_jobs_tool",  # Add Databricks jobs tool logger
        "src.services.agent_builder.task_adapter",  # Task tool resolution logs
        "src.services.agent_builder.agent_adapter",  # Agent tool resolution logs
        "src.services.security.tool_capability_manifest",  # Trifecta detection warnings
        "src.services.security.prompt_injection_detector",  # Injection detection warnings
        "src.utils.telemetry",  # Add telemetry logger for LogfoodTelemetry logging
        "__main__",  # For any direct logging in subprocess
    ]:
        module_logger = get_logger(logger_name)
        module_logger.handlers = []  # Clear existing handlers
        module_logger.addHandler(file_handler)
        if console_handler:  # Also add console handler in Databricks Apps
            module_logger.addHandler(console_handler)
        # IMPORTANT: Set to DEBUG for tool loggers to capture all DAX Generation logs
        if any(
            t in logger_name
            for t in [
                "powerbi_analysis_tool",
                "databricks_jobs_tool",
                "powerbi_semantic_model_dax_tool",
                "powerbi_metadata_reducer_tool",
                "powerbi_semantic_model_fetcher_tool",
                "tool_factory",
            ]
        ):
            module_logger.setLevel(logging.DEBUG)
        else:
            module_logger.setLevel(log_level)
        module_logger.propagate = False

    # Capture MLflow errors (suppress warnings) explicitly
    mlflow_logger = get_logger("mlflow")
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
