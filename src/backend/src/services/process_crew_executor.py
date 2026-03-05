"""Process-based crew executor for isolated AI agent execution.

This module implements process-based execution for CrewAI crews, providing
true isolation and reliable termination capabilities that thread-based
execution cannot offer.

Process isolation ensures that:
- Crew failures don't affect the main application
- Resources are properly cleaned up on termination
- Child processes are tracked and terminated on stop requests
- Memory and CPU usage are isolated per execution

Key Features:
    - True process isolation for crew execution
    - Graceful and forceful termination support
    - Child process tracking and cleanup
    - Subprocess logging configuration
    - Signal handling for clean shutdown
    - Multi-tenant support through group context

Architecture:
    The executor spawns crews in separate OS processes using multiprocessing,
    allowing complete control over the execution lifecycle including the
    ability to forcefully terminate stuck or runaway processes.

Example:
    >>> executor = ProcessCrewExecutor()
    >>> task = await executor.execute_crew_async(
    ...     execution_id="exec_123",
    ...     crew_config=config,
    ...     group_context=context
    ... )
    >>> # Later, if needed:
    >>> await executor.stop_execution("exec_123")
"""

# Global hardening: disable CrewAI cloud tracing/telemetry and suppress interactive prompts at module import time
try:
    import os as _kasal_ce_env

    _kasal_ce_env.environ["CREWAI_TRACING_ENABLED"] = "false"
    _kasal_ce_env.environ["CREWAI_TELEMETRY_OPT_OUT"] = "1"
    _kasal_ce_env.environ["CREWAI_ANALYTICS_OPT_OUT"] = "1"
    _kasal_ce_env.environ["CREWAI_CLOUD_TRACING"] = "false"
    _kasal_ce_env.environ["CREWAI_CLOUD_TRACING_ENABLED"] = "false"
    _kasal_ce_env.environ["CREWAI_VERBOSE"] = "false"
    _kasal_ce_env.environ["PYTHONUNBUFFERED"] = "0"
except Exception:
    pass

# Suppress interactive prompts globally
try:
    import builtins as _kasal_builtins_mod

    def _kasal_noinput_global(prompt=None):
        try:
            if prompt:
                print(f"[SUBPROCESS] Suppressed interactive prompt at import: {prompt}")
        except Exception:
            pass
        return "n"

    _kasal_builtins_mod.input = _kasal_noinput_global
except Exception:
    pass

# Also disable common CLI confirm/prompt mechanisms if present (click)
try:
    import click as _kasal_click

    _kasal_click.confirm = lambda *a, **k: False
    _kasal_click.prompt = lambda *a, **k: ""
except Exception:
    pass

import asyncio
import logging
import multiprocessing as mp
import os
import pickle
import signal
import traceback
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def run_crew_in_process(
    execution_id: str,
    crew_config: Dict[str, Any],
    inputs: Optional[Dict[str, Any]] = None,
    group_context: Any = None,  # Group context for tenant isolation
    log_queue: Optional[Any] = None,  # Queue for sending logs to main process
) -> Dict[str, Any]:
    """Execute a CrewAI crew in an isolated subprocess.

    This function runs in a completely separate OS process, providing
    true isolation from the main application. It rebuilds the crew from
    configuration, sets up logging, handles signals, and executes the crew.

    The function is designed to be called via multiprocessing and includes
    comprehensive error handling and cleanup logic.

    Args:
        execution_id: Unique identifier for tracking this execution.
            Used for logging and trace correlation.
        crew_config: Complete configuration dictionary to rebuild the crew,
            including agents, tasks, tools, and crew settings.
        inputs: Optional dictionary of inputs to pass to the crew.
            These become available to agents during execution.
        group_context: Optional multi-tenant context for isolation.
            Contains group_id, access_token, and other tenant info.
        log_queue: Optional queue for sending logs to the main process.
            Enables real-time log streaming to parent process.

    Returns:
        Dict[str, Any]: Execution results containing:
            - output: The crew execution output
            - status: Final execution status
            - error: Error details if execution failed
            - execution_time: Total execution duration

    Note:
        This function sets up signal handlers for SIGTERM and SIGINT
        to ensure proper cleanup of child processes on termination.

    Environment Variables Set:
        - CREW_SUBPROCESS_MODE: Marks subprocess execution mode
        - DATABASE_TYPE: Ensures correct database configuration
        - CREWAI_VERBOSE: Controls CrewAI output verbosity
    """
    # Import necessary modules at the beginning
    import logging
    import os
    import sys
    import traceback

    # Mark that we're in subprocess mode for logging purposes
    os.environ["CREW_SUBPROCESS_MODE"] = "true"
    # CRITICAL: Store execution_id in environment for orphaned process detection
    # This allows us to find and terminate this process even after server reloads
    os.environ["KASAL_EXECUTION_ID"] = execution_id

    # Ensure DATABASE_TYPE is set correctly in subprocess
    # The subprocess needs to know which database to use
    if "DATABASE_TYPE" not in os.environ:
        from src.config.settings import settings

        os.environ["DATABASE_TYPE"] = settings.DATABASE_TYPE or "postgres"
        print(f"[SUBPROCESS] Set DATABASE_TYPE to: {os.environ['DATABASE_TYPE']}")

    # Configure logging to only go to file, not stdout
    # Early validation of parameters to catch type errors
    import json

    try:
        # Handle None or empty crew_config
        if crew_config is None:
            error_msg = "crew_config is None"
            print(f"[SUBPROCESS ERROR] {error_msg}", file=sys.stderr)
            return {
                "status": "FAILED",
                "execution_id": execution_id,
                "error": error_msg,
                "process_id": os.getpid(),
            }

        # Check if crew_config is a string (JSON) and parse it BEFORE validation
        if isinstance(crew_config, str):
            try:
                crew_config = json.loads(crew_config)
                print(
                    f"[SUBPROCESS] Parsed crew_config from JSON string", file=sys.stderr
                )
            except json.JSONDecodeError as e:
                error_msg = f"Failed to parse crew_config JSON: {e}"
                print(f"[SUBPROCESS ERROR] {error_msg}", file=sys.stderr)
                return {
                    "status": "FAILED",
                    "execution_id": execution_id,
                    "error": error_msg,
                    "process_id": os.getpid(),
                }

        # Now validate crew_config type (after potential JSON parsing)
        if not isinstance(crew_config, dict):
            error_msg = f"crew_config must be a dict, got {type(crew_config)} with value: {repr(crew_config)[:100]}"
            print(f"[SUBPROCESS ERROR] {error_msg}", file=sys.stderr)
            return {
                "status": "FAILED",
                "execution_id": execution_id,
                "error": error_msg,
                "process_id": os.getpid(),
            }
    except Exception as validation_error:
        return {
            "status": "FAILED",
            "execution_id": execution_id,
            "error": f"Parameter validation error: {str(validation_error)}",
            "process_id": os.getpid() if "os" in locals() else 0,
        }

    # This must be done early before any other imports that might configure logging
    from src.engines.crewai.logging_config import (
        configure_subprocess_logging,
        restore_stdout_stderr,
        suppress_stdout_stderr,
    )

    # Suppress all stdout/stderr output
    original_stdout, original_stderr, captured_output = suppress_stdout_stderr()

    # Set up signal handlers for graceful shutdown with child process cleanup
    def signal_handler(signum, frame):
        # Kill all child processes spawned by this subprocess
        try:
            import psutil

            parent = psutil.Process(os.getpid())
            children = parent.children(recursive=True)

            for child in children:
                try:
                    child.terminate()
                except psutil.NoSuchProcess:
                    pass

            # Give them a moment to terminate gracefully
            psutil.wait_procs(children, timeout=1)

            # Force kill any remaining
            for child in children:
                try:
                    if child.is_running():
                        child.kill()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass

        except Exception as cleanup_error:
            # Ignore cleanup errors in signal handler
            pass

        # Exit the process
        import sys

        sys.exit(1)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        # Import CrewAI and dependencies here to avoid pickling issues
        import asyncio
        import os as _os_crewai_env

        # Force-disable CrewAI cloud tracing/telemetry and any interactive prompts
        _os_crewai_env.environ["CREWAI_TRACING_ENABLED"] = "false"
        _os_crewai_env.environ["CREWAI_TELEMETRY_OPT_OUT"] = "1"
        _os_crewai_env.environ["CREWAI_ANALYTICS_OPT_OUT"] = "1"
        _os_crewai_env.environ["CREWAI_CLOUD_TRACING"] = "false"
        _os_crewai_env.environ["CREWAI_CLOUD_TRACING_ENABLED"] = "false"
        _os_crewai_env.environ["CREWAI_VERBOSE"] = "false"
        _os_crewai_env.environ["PYTHONUNBUFFERED"] = "0"
        # Belt-and-suspenders: suppress any input() prompts in this subprocess
        import builtins as _kasal_builtins

        def _kasal_noinput(prompt=None):
            try:
                if prompt:
                    print(f"[SUBPROCESS] Suppressed interactive prompt: {prompt}")
            except Exception:
                pass
            return "n"

        try:
            _kasal_builtins.input = _kasal_noinput
        except Exception:
            pass

        # CRITICAL: Patch CrewAI's LLM_CONTEXT_WINDOW_SIZES BEFORE importing Crew
        # This is necessary because CrewAI has hardcoded context window sizes and
        # doesn't know about Databricks models. Without this patch, it falls back to
        # DEFAULT_CONTEXT_WINDOW_SIZE (8192 tokens), causing respect_context_window
        # to not trigger summarization when needed. This leads to empty responses from
        # models like Qwen that silently fail when context is too large.
        try:
            from crewai.llm import LLM_CONTEXT_WINDOW_SIZES

            from src.seeds.model_configs import MODEL_CONFIGS

            for model_name, config in MODEL_CONFIGS.items():
                if config.get("provider") == "databricks":
                    full_model_name = f"databricks/{model_name}"
                    context_window = config.get("context_window", 128000)
                    LLM_CONTEXT_WINDOW_SIZES[full_model_name] = context_window
            print(
                f"[SUBPROCESS] Registered Databricks models with CrewAI context window sizes",
                file=sys.stderr,
            )
        except Exception as reg_err:
            print(
                f"[SUBPROCESS] Warning: Could not register Databricks models: {reg_err}",
                file=sys.stderr,
            )

        # CRITICAL: Patch CrewAI's CONTEXT_LIMIT_ERRORS to recognize Databricks error messages
        # Databricks returns "exceeds maximum allowed content length" which is not in CrewAI's
        # default error patterns. Without this patch, CrewAI won't trigger summarization
        # when Databricks returns a context length error.
        try:
            from crewai.utilities.exceptions import context_window_exceeding_exception

            databricks_error_patterns = [
                "exceeds maximum allowed content length",  # Databricks specific
                "maximum allowed content length",  # Alternative pattern
                "requestsize",  # Part of Databricks error format
            ]
            for pattern in databricks_error_patterns:
                if (
                    pattern
                    not in context_window_exceeding_exception.CONTEXT_LIMIT_ERRORS
                ):
                    context_window_exceeding_exception.CONTEXT_LIMIT_ERRORS.append(
                        pattern
                    )
            print(
                f"[SUBPROCESS] Registered Databricks error patterns for context limit detection",
                file=sys.stderr,
            )
        except Exception as err_reg:
            print(
                f"[SUBPROCESS] Warning: Could not register Databricks error patterns: {err_reg}",
                file=sys.stderr,
            )

        from crewai import Crew

        # Configure subprocess logging with execution ID
        subprocess_logger = configure_subprocess_logging(execution_id)
        subprocess_logger.info(
            f"Process {os.getpid()} preparing crew for execution {execution_id}"
        )
        subprocess_logger.info(
            f"[EXECUTION_START] ========== Execution {execution_id} Started =========="
        )

        # Log the job configuration early to ensure it's captured
        subprocess_logger.info(
            f"[JOB_CONFIGURATION] ========== EXECUTION {execution_id} =========="
        )

        # Now safely access the config
        if isinstance(crew_config, dict):
            try:
                subprocess_logger.info(
                    f"[JOB_CONFIGURATION] Crew: {crew_config.get('run_name', 'Unnamed')} v{crew_config.get('version', '1.0')}"
                )
                subprocess_logger.info(
                    f"[JOB_CONFIGURATION] Agents: {len(crew_config.get('agents', []))}"
                )
                subprocess_logger.info(
                    f"[JOB_CONFIGURATION] Tasks: {len(crew_config.get('tasks', []))}"
                )
                # Log full config for debugging
                subprocess_logger.info(
                    f"[JOB_CONFIGURATION] Full Config: {json.dumps(crew_config, indent=2)}"
                )
            except AttributeError as e:
                subprocess_logger.error(
                    f"[JOB_CONFIGURATION] AttributeError accessing crew_config: {e}"
                )
                subprocess_logger.error(
                    f"[JOB_CONFIGURATION] crew_config type: {type(crew_config)}"
                )
                subprocess_logger.error(
                    f"[JOB_CONFIGURATION] crew_config value: {repr(crew_config)}"
                )
                # Re-raise to see the full error
                raise

            # Force flush all handlers to ensure logs are written
            for handler in subprocess_logger.handlers:
                handler.flush()
        else:
            subprocess_logger.error(
                f"[JOB_CONFIGURATION] crew_config is not a dict: {type(crew_config)}"
            )

        if inputs:
            if isinstance(inputs, str):
                subprocess_logger.info(f"[JOB_CONFIGURATION] Inputs (string): {inputs}")
            else:
                subprocess_logger.info(
                    f"[JOB_CONFIGURATION] Inputs: {json.dumps(inputs)}"
                )

        # Debug: Print to stderr to see subprocess progress
        print(
            f"[SUBPROCESS DEBUG] Starting prepare_and_run for {execution_id}",
            file=sys.stderr,
        )

        # Subprocess logger is now configured

        # SECURITY: Set up UserContext with group_id for multi-tenant isolation in subprocess
        # This is CRITICAL because the subprocess doesn't have request context
        if crew_config and isinstance(crew_config, dict) and "group_id" in crew_config:
            try:
                from src.utils.user_context import GroupContext, UserContext

                group_id = crew_config["group_id"]
                subprocess_logger.info(
                    f"[SUBPROCESS] Setting up UserContext with group_id={group_id}"
                )

                # Create a GroupContext for the subprocess
                group_context_obj = GroupContext(
                    group_ids=[group_id],
                    group_email=crew_config.get(
                        "group_email", f"{group_id}@subprocess"
                    ),
                    access_token=crew_config.get("user_token"),  # Optional OBO token
                )

                # Set it in UserContext so LLMManager can access it
                UserContext.set_group_context(group_context_obj)

                # CRITICAL: Also set user_token separately for OBO authentication
                # LLMManager.get_user_token() reads from this separate context variable
                user_token = crew_config.get("user_token")
                if user_token:
                    UserContext.set_user_token(user_token)
                    subprocess_logger.info(
                        f"[SUBPROCESS] ✓ UserContext initialized with group_id={group_id} and OBO token"
                    )
                else:
                    subprocess_logger.info(
                        f"[SUBPROCESS] ✓ UserContext initialized with group_id={group_id} (no OBO token)"
                    )

                # Verify it was set correctly
                verify_context = UserContext.get_group_context()
                if verify_context and verify_context.primary_group_id == group_id:
                    subprocess_logger.info(
                        f"[SUBPROCESS] ✓ UserContext verification successful: {verify_context.primary_group_id}"
                    )
                else:
                    subprocess_logger.error(
                        f"[SUBPROCESS] ✗ UserContext verification FAILED: got {verify_context}"
                    )
                    subprocess_logger.error(
                        f"[SUBPROCESS] This indicates a ContextVar issue in subprocess"
                    )
            except Exception as e:
                subprocess_logger.error(
                    f"[SUBPROCESS] Failed to set up UserContext: {e}"
                )
                subprocess_logger.error(
                    f"[SUBPROCESS] This will cause multi-tenant isolation failures!"
                )
        else:
            subprocess_logger.error(
                "[SUBPROCESS] SECURITY: No group_id in crew_config - multi-tenant isolation will fail"
            )

        # Rebuild the crew from config using async context
        # We need to run the async crew preparation in the subprocess
        async def prepare_and_run():
            # Import within async context
            import os  # Import os first

            # Suppress any stdout/stderr from CrewAI
            import warnings

            from src.engines.crewai.crew_preparation import CrewPreparation
            from src.engines.crewai.tools.mcp_integration import MCPIntegration
            from src.engines.crewai.tools.tool_factory import ToolFactory
            from src.services.api_keys_service import ApiKeysService
            from src.services.tool_service import ToolService

            # Reset MCP warnings at the start of each execution
            MCPIntegration.reset_warnings()

            warnings.filterwarnings("ignore")

            # Disable CrewAI's verbose output and cloud tracing
            os.environ["CREWAI_VERBOSE"] = "false"
            os.environ["CREWAI_TRACING_ENABLED"] = "false"
            os.environ["CREWAI_TELEMETRY_OPT_OUT"] = "1"
            os.environ["CREWAI_ANALYTICS_OPT_OUT"] = "1"
            os.environ["CREWAI_CLOUD_TRACING"] = "false"
            os.environ["PYTHONUNBUFFERED"] = "0"

            # Set subprocess mode flag for direct DB writes
            os.environ["CREW_SUBPROCESS_MODE"] = "true"

            # Get logger early for this function
            import logging

            async_logger = logging.getLogger("crew")

            # SECURITY: Re-initialize UserContext in async context for multi-tenant isolation
            # ContextVars can sometimes not propagate properly across sync/async boundaries
            if (
                crew_config
                and isinstance(crew_config, dict)
                and "group_id" in crew_config
            ):
                try:
                    from src.utils.user_context import GroupContext, UserContext

                    group_id = crew_config["group_id"]
                    async_logger.info(
                        f"[ASYNC] Re-initializing UserContext with group_id={group_id}"
                    )

                    # Re-create GroupContext in async context
                    group_context_obj = GroupContext(
                        group_ids=[group_id],
                        group_email=crew_config.get(
                            "group_email", f"{group_id}@subprocess"
                        ),
                        access_token=crew_config.get("user_token"),
                    )

                    # Set it in UserContext
                    UserContext.set_group_context(group_context_obj)

                    # CRITICAL: Also set user_token separately for OBO authentication
                    # LLMManager.get_user_token() reads from this separate context variable
                    user_token = crew_config.get("user_token")
                    if user_token:
                        UserContext.set_user_token(user_token)
                        async_logger.info(f"[ASYNC] ✓ UserContext set with OBO token")

                    # Verify
                    verify = UserContext.get_group_context()
                    if verify and verify.primary_group_id == group_id:
                        async_logger.info(f"[ASYNC] ✓ UserContext verified: {group_id}")
                    else:
                        async_logger.error(
                            f"[ASYNC] ✗ UserContext verification failed: {verify}"
                        )
                except Exception as e:
                    async_logger.error(f"[ASYNC] Failed to initialize UserContext: {e}")

            # Always load Databricks config for the current group to determine MLflow status
            db_config = None
            try:
                from src.db.session import async_session_factory
                from src.services.databricks_service import DatabricksService

                async with async_session_factory() as session:
                    current_group_id = (
                        getattr(group_context, "primary_group_id", None)
                        if group_context
                        else None
                    )
                    databricks_service = DatabricksService(
                        session, group_id=current_group_id
                    )
                    db_config = await databricks_service.get_databricks_config()
                    print(
                        f"[DEBUG_MLFLOW_SUBPROCESS] db_config loaded: mlflow_enabled={getattr(db_config, 'mlflow_enabled', None) if db_config else 'NO_CONFIG'}"
                    )

                    # Ensure DATABRICKS_HOST is available in subprocess via unified auth
                    try:
                        from src.utils.databricks_auth import get_auth_context

                        auth = await get_auth_context()
                        if auth and auth.workspace_url:
                            if "DATABRICKS_HOST" not in os.environ:
                                os.environ["DATABRICKS_HOST"] = auth.workspace_url
                                async_logger.info(
                                    f"[SUBPROCESS] Set DATABRICKS_HOST from unified {auth.auth_method} auth: {auth.workspace_url}"
                                )
                        else:
                            async_logger.warning(
                                "[SUBPROCESS] No workspace URL available from unified auth"
                            )
                    except Exception as e:
                        async_logger.warning(
                            f"[SUBPROCESS] Failed to get unified auth for DATABRICKS_HOST: {e}"
                        )
            except Exception as e:
                async_logger.warning(
                    f"[SUBPROCESS] Could not load Databricks config / host: {e}"
                )

            # CRITICAL: Enable OTel SDK in subprocess.
            # main.py sets OTEL_SDK_DISABLED=true to suppress CrewAI's default
            # telemetry in the parent. Subprocess needs OTel enabled for both
            # Kasal's own trace pipeline AND (optionally) MLflow tracing.
            import os as _otel_env

            _otel_env.environ["OTEL_SDK_DISABLED"] = "false"
            # Keep CrewAI telemetry disabled to prevent HTTP requests to docs.crewai.com
            _otel_env.environ["CREWAI_DISABLE_TELEMETRY"] = "true"
            async_logger.info(
                "[SUBPROCESS] Set OTEL_SDK_DISABLED=false (enabling OTel trace pipeline)"
            )

            # CRITICAL: Configure MLflow in subprocess before any LLM operations
            # This ensures MLflow autolog captures LiteLLM calls made in the subprocess
            from src.services.otel_tracing.mlflow_setup import (
                configure_mlflow_in_subprocess,
            )

            _mlflow_group_id = (
                getattr(group_context, "primary_group_id", None)
                if group_context
                else None
            )
            mlflow_result = await configure_mlflow_in_subprocess(
                db_config=db_config,
                job_id=execution_id,
                execution_id=execution_id,
                group_id=_mlflow_group_id,
                group_context=group_context,
                async_logger=async_logger,
            )
            # Create services using session factory
            from src.db.session import safe_async_session

            async with safe_async_session() as session:
                # Extract group_id first for service initialization
                group_id = None
                try:
                    if group_context and getattr(
                        group_context, "primary_group_id", None
                    ):
                        group_id = getattr(group_context, "primary_group_id")
                        async_logger.info(
                            f"[SUBPROCESS] Using group_id for services: {group_id}"
                        )
                except Exception as e:
                    async_logger.warning(
                        f"[SUBPROCESS] Could not extract group_id: {e}"
                    )

                # Create services with the session and group_id
                from src.services.api_keys_service import ApiKeysService
                from src.services.tool_service import ToolService

                tool_service = ToolService(session)
                api_keys_service = ApiKeysService(session, group_id=group_id)

                # Ensure group_id is present in config for group-aware tool loading
                try:
                    if isinstance(crew_config, dict) and group_id:
                        crew_config.setdefault("group_id", group_id)
                        async_logger.info(
                            f"[SUBPROCESS] Injected group_id into crew_config: {crew_config.get('group_id')}"
                        )
                except Exception as e:
                    async_logger.warning(
                        f"[SUBPROCESS] Could not inject group_id into crew_config: {e}"
                    )

                # CRITICAL: Inject execution inputs into crew_config BEFORE ToolFactory creation
                # This enables placeholder resolution for dynamic tool configs like {tenant_id}, {workspace_id}, etc.
                # The ToolFactory looks for inputs in config['inputs']['inputs'] for placeholder resolution
                try:
                    if isinstance(crew_config, dict) and inputs:
                        # Ensure inputs structure exists for ToolFactory placeholder resolution
                        if 'inputs' not in crew_config:
                            crew_config['inputs'] = {}
                        if isinstance(crew_config['inputs'], dict):
                            # Store inputs in nested structure that ToolFactory expects
                            crew_config['inputs']['inputs'] = inputs
                            async_logger.info(f"[SUBPROCESS] Injected execution inputs into crew_config for tool placeholder resolution: {list(inputs.keys())}")
                        else:
                            async_logger.warning(f"[SUBPROCESS] crew_config['inputs'] is not a dict, cannot inject execution inputs")
                except Exception as e:
                    async_logger.warning(f"[SUBPROCESS] Could not inject execution inputs into crew_config: {e}")

                # Extract user_token from crew_config for OBO authentication
                # The user_token is passed from the parent process via crew_config
                user_token = None
                if isinstance(crew_config, dict):
                    user_token = crew_config.get("user_token") or crew_config.get(
                        "access_token"
                    )
                    if user_token:
                        async_logger.info(
                            f"[SUBPROCESS] Found user_token in crew_config for OBO authentication (length={len(user_token)})"
                        )
                    else:
                        async_logger.warning(
                            "[SUBPROCESS] No user_token found in crew_config - tools requiring OBO will fall back to PAT/SPN auth"
                        )

                # Create a tool factory instance with user_token
                tool_factory = await ToolFactory.create(
                    crew_config, api_keys_service, user_token
                )

                # Initialize OTel tracing BEFORE crew preparation so traces
                # are captured even when MCP tool creation or other prep is slow.
                otel_provider = None
                try:
                    from src.services.otel_tracing import (
                        create_kasal_tracer_provider,
                    )
                    from opentelemetry import trace as _otel_trace
                    from opentelemetry.sdk.trace.export import (
                        SimpleSpanProcessor,
                    )

                    otel_provider = create_kasal_tracer_provider(
                        job_id=execution_id
                    )

                    # DB exporter + SSE processor
                    from src.services.otel_tracing.db_exporter import (
                        KasalDBSpanExporter,
                    )
                    from src.services.otel_tracing.sse_processor import (
                        KasalSSESpanProcessor,
                    )
                    otel_provider.add_span_processor(
                        SimpleSpanProcessor(
                            KasalDBSpanExporter(
                                execution_id, group_context
                            )
                        )
                    )
                    otel_provider.add_span_processor(
                        KasalSSESpanProcessor(execution_id)
                    )

                    # MLflow exporter (conditional on mlflow being ready)
                    if mlflow_result and mlflow_result.tracing_ready:
                        from src.services.otel_tracing.mlflow_exporter import (
                            KasalMLflowSpanExporter,
                        )
                        otel_provider.add_span_processor(
                            SimpleSpanProcessor(
                                KasalMLflowSpanExporter(
                                    execution_id, mlflow_result, group_context
                                )
                            )
                        )
                        mlflow_result.otel_exporter_active = True
                        async_logger.info(
                            f"[SUBPROCESS] MLflow span exporter added to OTel pipeline for {execution_id}"
                        )

                    _otel_trace.set_tracer_provider(otel_provider)

                    # Instrument CrewAI if instrumentor available
                    try:
                        from openinference.instrumentation.crewai import (
                            CrewAIInstrumentor,
                        )
                        CrewAIInstrumentor().instrument(
                            tracer_provider=otel_provider
                        )
                        async_logger.info(
                            f"[SUBPROCESS] OTel tracing enabled with CrewAI instrumentation for {execution_id}"
                        )
                    except ImportError:
                        async_logger.info(
                            f"[SUBPROCESS] OTel tracing enabled (no CrewAI instrumentor) for {execution_id}"
                        )

                    # Diagnostic: confirm span processors are attached
                    try:
                        proc_count = len(
                            otel_provider._active_span_processor._span_processors
                        ) if hasattr(otel_provider, '_active_span_processor') else 'unknown'
                    except Exception:
                        proc_count = 'unknown'
                    async_logger.info(
                        f"[SUBPROCESS] OTel pipeline: provider has {proc_count} span processor(s) for {execution_id}"
                    )
                except ImportError:
                    async_logger.debug(
                        "[SUBPROCESS] OTel packages not available, skipping"
                    )
                except Exception as otel_err:
                    async_logger.warning(
                        f"[SUBPROCESS] OTel initialization error (non-fatal): {otel_err}"
                    )

                # Log the JobConfiguration BEFORE crew preparation to ensure it's captured
                import json

                # async_logger already defined above - no need to redefine
                # Log configuration immediately
                async_logger.info(
                    f"[JOB_CONFIGURATION] ========== CONFIGURATION FOR {execution_id} =========="
                )

                # Use the CrewPreparation class for crew setup
                # Debug log to check knowledge_sources right before CrewPreparation
                async_logger.info(f"[DEBUG] Before CrewPreparation creation:")
                for idx, agent_cfg in enumerate(crew_config.get("agents", [])):
                    agent_id = agent_cfg.get("id", f"agent_{idx}")
                    ks = agent_cfg.get("knowledge_sources", [])
                    async_logger.info(
                        f"[DEBUG] Agent {agent_id} has {len(ks)} knowledge_sources: {ks}"
                    )

                # CRITICAL: Pass user_token from crew_config for OBO authentication
                # This is needed for Databricks memory upserts to authenticate properly
                user_token_for_crew = crew_config.get("user_token")
                async_logger.info(
                    f"[DEBUG] Creating CrewPreparation with user_token: {bool(user_token_for_crew)}"
                )

                crew_preparation = CrewPreparation(
                    crew_config, tool_service, tool_factory, user_token_for_crew
                )
                if not await crew_preparation.prepare():
                    raise RuntimeError(f"Failed to prepare crew for {execution_id}")

                # Get the prepared crew
                crew = crew_preparation.crew

                # Log the full configuration with pretty formatting (only if crew_config is valid)
                if isinstance(crew_config, dict):
                    try:
                        async_logger.info(
                            f"[JOB_CONFIGURATION] Starting execution for job {execution_id}"
                        )
                        async_logger.info(
                            f"[JOB_CONFIGURATION] Crew Name: {crew_config.get('run_name', 'Unnamed')}"
                        )
                        async_logger.info(
                            f"[JOB_CONFIGURATION] Version: {crew_config.get('version', '1.0')}"
                        )

                        # Log agents configuration
                        agents = crew_config.get("agents", [])
                        async_logger.info(
                            f"[JOB_CONFIGURATION] Number of Agents: {len(agents)}"
                        )
                        for i, agent in enumerate(agents, 1):
                            async_logger.info(
                                f"[JOB_CONFIGURATION]   Agent {i}: {agent.get('role', 'Unknown Role')}"
                            )
                            async_logger.info(
                                f"[JOB_CONFIGURATION]     Goal: {agent.get('goal', 'No goal specified')}"
                            )
                            # Log knowledge_sources if present
                            if "knowledge_sources" in agent:
                                ks = agent["knowledge_sources"]
                                async_logger.info(
                                    f"[JOB_CONFIGURATION]     Knowledge Sources: {len(ks)} sources"
                                )
                                for j, source in enumerate(ks):
                                    async_logger.info(
                                        f"[JOB_CONFIGURATION]       Source {j+1}: {source}"
                                    )
                            else:
                                async_logger.info(
                                    f"[JOB_CONFIGURATION]     Knowledge Sources: None"
                                )
                            if agent.get("llm"):
                                llm_config = agent["llm"]
                                # Handle both string and dict formats for llm
                                if isinstance(llm_config, str):
                                    async_logger.info(
                                        f"[JOB_CONFIGURATION]     LLM: {llm_config}"
                                    )
                                elif isinstance(llm_config, dict):
                                    async_logger.info(
                                        f"[JOB_CONFIGURATION]     LLM: {llm_config.get('model', 'default')} (temp: {llm_config.get('temperature', 0.7)})"
                                    )
                                else:
                                    async_logger.info(
                                        f"[JOB_CONFIGURATION]     LLM: {llm_config}"
                                    )

                        # Log tasks configuration
                        tasks = crew_config.get("tasks", [])
                        async_logger.info(
                            f"[JOB_CONFIGURATION] Number of Tasks: {len(tasks)}"
                        )
                        for i, task in enumerate(tasks, 1):
                            async_logger.info(
                                f"[JOB_CONFIGURATION]   Task {i}: {task.get('description', 'No description')[:100]}..."
                            )
                            async_logger.info(
                                f"[JOB_CONFIGURATION]     Agent: {task.get('agent', 'Unknown')}"
                            )
                            async_logger.info(
                                f"[JOB_CONFIGURATION]     Expected Output: {task.get('expected_output', 'Not specified')[:100]}..."
                            )

                        # Log inputs if provided
                        if inputs:
                            async_logger.info(
                                f"[JOB_CONFIGURATION] Inputs provided: {json.dumps(inputs, indent=2)}"
                            )
                        else:
                            async_logger.info(f"[JOB_CONFIGURATION] No inputs provided")

                        # Log complete configuration as JSON for debugging
                        async_logger.info(
                            f"[JOB_CONFIGURATION] Full Config JSON: {json.dumps(crew_config, indent=2)}"
                        )
                    except AttributeError as e:
                        async_logger.error(
                            f"[JOB_CONFIGURATION ASYNC] AttributeError accessing crew_config: {e}"
                        )
                        async_logger.error(
                            f"[JOB_CONFIGURATION ASYNC] crew_config type: {type(crew_config)}"
                        )
                        async_logger.error(
                            f"[JOB_CONFIGURATION ASYNC] crew_config value: {repr(crew_config)[:500]}"
                        )
                        raise
                else:
                    async_logger.error(
                        f"[JOB_CONFIGURATION] crew_config is not a dict in async function: {type(crew_config)}"
                    )

                async_logger.info(f"Starting execution for job {execution_id}")

                # Initialize event listeners in the subprocess BEFORE kickoff
                # These must be created and connected to the crew
                from src.engines.crewai.callbacks.execution_callback import (
                    create_execution_callbacks,
                )
                from src.engines.crewai.callbacks.logging_callbacks import (
                    AgentTraceEventListener,
                    TaskCompletionEventListener,
                )
                from src.engines.crewai.trace_management import TraceManager

                async_logger.info(
                    f"Process {os.getpid()} initializing event listeners for {execution_id}"
                )

                # Initialize event listeners

                try:
                    import sys

                    print(
                        f"[SUBPROCESS ASYNC] Starting event listener setup for {execution_id}",
                        file=sys.stderr,
                    )

                    # Start the trace writer to process queued traces
                    await TraceManager.ensure_writer_started()
                    async_logger.info(
                        f"TraceManager writer started in subprocess for {execution_id}"
                    )

                    # Create the event listeners in this subprocess
                    # Import the event bus from crewai
                    from crewai.events import crewai_event_bus

                    # Create and register the event listeners with group_context
                    agent_trace_listener = AgentTraceEventListener(
                        job_id=execution_id,
                        group_context=group_context,
                        task_event_queue=log_queue,
                    )
                    agent_trace_listener.setup_listeners(crewai_event_bus)
                    async_logger.info(
                        f"Created and registered AgentTraceEventListener for {execution_id}"
                    )

                    # Log that subprocess mode is enabled for direct DB writes
                    async_logger.info(
                        f"CREW_SUBPROCESS_MODE={os.environ.get('CREW_SUBPROCESS_MODE')} - Direct DB writes enabled"
                    )

                    # Also create and register the other event listeners
                    task_logger = TaskCompletionEventListener(job_id=execution_id)
                    task_logger.setup_listeners(crewai_event_bus)
                    async_logger.info(
                        f"Created and registered TaskCompletionEventListener for {execution_id}"
                    )

                    # DetailedOutputLogger functionality now integrated into AgentTraceEventListener
                    # No separate detailed logger needed

                    # Register OTel Event Bridge on the CrewAI event bus
                    # (OTel provider was initialized earlier, before crew preparation)
                    if otel_provider is not None:
                        try:
                            from src.services.otel_tracing.event_bridge import (
                                OTelEventBridge,
                            )

                            _bridge_tracer = otel_provider.get_tracer(
                                "kasal-event-bridge"
                            )
                            _otel_bridge = OTelEventBridge(
                                _bridge_tracer, execution_id, group_context
                            )
                            _otel_bridge.register(crewai_event_bus)
                            async_logger.info(
                                f"[SUBPROCESS] OTel Event Bridge registered on event bus for {execution_id}"
                            )
                        except Exception as bridge_err:
                            async_logger.warning(
                                f"[SUBPROCESS] OTel Event Bridge registration failed (non-fatal): {bridge_err}"
                            )

                    # Debug: Print that we're about to configure logging
                    import sys  # Import sys for stderr debugging

                    print(
                        f"[SUBPROCESS ASYNC] Event listeners created, configuring logging for {execution_id}",
                        file=sys.stderr,
                    )

                    # Configure loggers to write to crew.log file only
                    # The main process will read crew.log after execution and write to execution_logs
                    # This avoids the complexity of queue-based logging from subprocess
                    import logging

                    # os is already imported at the top of prepare_and_run()
                    # Get log directory from environment or determine dynamically
                    log_dir = os.environ.get("LOG_DIR")
                    if not log_dir:
                        # Determine log directory relative to backend root
                        import pathlib

                        backend_root = pathlib.Path(__file__).parent.parent.parent
                        log_dir = backend_root / "logs"

                    crew_log_path = os.path.join(log_dir, "crew.log")

                    # Create file handler for crew.log
                    from src.engines.crewai.logging_config import (
                        ExecutionContextFormatter,
                        set_execution_context,
                    )

                    set_execution_context(execution_id)

                    # File handler for crew.log
                    file_handler = logging.FileHandler(crew_log_path)
                    file_handler.setFormatter(
                        ExecutionContextFormatter(
                            fmt="[CREW] %(asctime)s - %(levelname)s - %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S",
                        )
                    )
                    file_handler.setLevel(logging.INFO)

                    # Apply to specific named loggers (do not touch the root logger)
                    loggers_to_configure = [
                        logging.getLogger("crew"),  # Main crew logger
                        logging.getLogger(
                            "src.engines.crewai.callbacks.logging_callbacks"
                        ),
                        logging.getLogger(
                            "src.engines.crewai.callbacks.execution_callback"
                        ),
                        logging.getLogger("src.engines.crewai"),
                        logging.getLogger("mlflow"),
                        logging.getLogger("mlflow.tracing"),
                        logging.getLogger("__main__"),
                        # OTel tracing loggers (required for visibility into trace pipeline)
                        logging.getLogger("src.services.otel_tracing"),
                        logging.getLogger("src.services.otel_tracing.db_exporter"),
                        logging.getLogger("src.services.otel_tracing.otel_config"),
                        logging.getLogger("src.services.otel_tracing.sse_processor"),
                    ]

                    for logger_obj in loggers_to_configure:
                        # Clear existing handlers to avoid duplicates
                        logger_obj.handlers = []
                        # Add file handler only
                        logger_obj.addHandler(file_handler)
                        logger_obj.setLevel(logging.INFO)
                        logger_obj.propagate = False

                    async_logger.info(
                        f"[SUBPROCESS] Configured all loggers to write to crew.log for {execution_id}"
                    )

                    # async_logger already defined above - no need to redefine

                    task_completion_logger = TaskCompletionEventListener(
                        job_id=execution_id
                    )
                    async_logger.info(
                        f"Created TaskCompletionEventListener for {execution_id}"
                    )

                    # DetailedOutputLogger functionality now integrated into AgentTraceEventListener
                    # No separate detailed logger needed
                    async_logger.info(
                        f"Detailed output logging integrated into AgentTraceEventListener for {execution_id}"
                    )

                    async_logger.info(
                        f"All event listeners initialized in subprocess for {execution_id}"
                    )

                    # Log configuration parameters at the start of execution
                    try:
                        config_summary = {
                            "execution_id": execution_id,
                            "crew_name": crew_config.get("run_name", "Unnamed"),
                            "crew_version": crew_config.get("version", "1.0"),
                            "process_type": crew_config.get("process", "sequential"),
                            "agent_count": len(crew_config.get("agents", [])),
                            "task_count": len(crew_config.get("tasks", [])),
                            "agents": [
                                agent.get("role", "Unknown")
                                for agent in crew_config.get("agents", [])
                            ],
                            "tasks": [
                                task.get("description", "Unknown")[:100]
                                for task in crew_config.get("tasks", [])
                            ],  # First 100 chars
                            "memory_config": {
                                "provider": crew_config.get("memory", {}).get(
                                    "provider", "None"
                                ),
                                "short_term": (
                                    crew_config.get("memory", {})
                                    .get("config", {})
                                    .get("embedder", {})
                                    .get("config", {})
                                    .get("model", "N/A")
                                    if crew_config.get("memory")
                                    else "Disabled"
                                ),
                                "long_term": (
                                    "Enabled"
                                    if crew_config.get("memory", {})
                                    .get("config", {})
                                    .get("long_term", {})
                                    else "Disabled"
                                ),
                            },
                            "model": crew_config.get("model_name", "default"),
                            "max_iterations": crew_config.get("max_iter", "default"),
                            "inputs_provided": list(inputs.keys()) if inputs else [],
                            # ⚠️ IMPORTANT: group_context here must use getattr() not dict access
                            # Even though we can't pickle GroupContext with access_token, it still
                            # arrives as an object (not dict) in the subprocess
                            "group_context": {
                                "group_id": (
                                    getattr(group_context, "primary_group_id", "N/A")
                                    if group_context
                                    else "N/A"
                                ),
                                "group_email": (
                                    getattr(group_context, "group_email", "N/A")
                                    if group_context
                                    else "N/A"
                                ),
                            },
                        }

                        # Format the configuration as a readable log message
                        async_logger.info("=" * 80)
                        async_logger.info("JOB CONFIGURATION PARAMETERS")
                        async_logger.info("=" * 80)
                        async_logger.info(
                            f"Execution ID: {config_summary['execution_id']}"
                        )
                        async_logger.info(
                            f"Crew: {config_summary['crew_name']} v{config_summary['crew_version']}"
                        )
                        async_logger.info(
                            f"Process Type: {config_summary['process_type']}"
                        )
                        async_logger.info(f"Model: {config_summary['model']}")
                        async_logger.info(
                            f"Max Iterations: {config_summary['max_iterations']}"
                        )
                        async_logger.info(
                            f"Agents ({config_summary['agent_count']}): {', '.join(config_summary['agents'])}"
                        )
                        async_logger.info(
                            f"Tasks ({config_summary['task_count']}): {len(config_summary['tasks'])} tasks configured"
                        )
                        async_logger.info(
                            f"Memory Provider: {config_summary['memory_config']['provider']}"
                        )
                        if config_summary["memory_config"]["provider"] != "None":
                            async_logger.info(
                                f"  - Short-term Memory: {config_summary['memory_config']['short_term']}"
                            )
                            async_logger.info(
                                f"  - Long-term Memory: {config_summary['memory_config']['long_term']}"
                            )
                        async_logger.info(
                            f"Inputs: {', '.join(config_summary['inputs_provided']) if config_summary['inputs_provided'] else 'None'}"
                        )
                        async_logger.info(
                            f"Group ID: {config_summary['group_context']['group_id']}"
                        )
                        async_logger.info(
                            f"Group Email: {config_summary['group_context']['group_email']}"
                        )
                        async_logger.info("=" * 80)

                        # Also log as JSON for structured parsing if needed
                        import json

                        async_logger.info(
                            f"Configuration JSON: {json.dumps(config_summary, indent=2)}"
                        )

                        # Add explicit marker for Job Configuration to make it easily searchable
                        async_logger.info(
                            f"[JOB_CONFIGURATION_COMPLETE] Configuration for execution {execution_id} has been logged successfully"
                        )

                    except Exception as e:
                        async_logger.error(
                            f"Failed to log configuration parameters: {e}"
                        )
                        # Continue execution even if config logging fails

                    # CRITICAL: Set callbacks on the crew so it knows about our event listeners
                    # Create execution callbacks for step and task tracking
                    step_callback, task_callback = create_execution_callbacks(
                        job_id=execution_id,
                        config=crew_config,
                        group_context=group_context,  # Pass group_context
                        crew=crew,
                    )

                    # Set the callbacks on the crew instance
                    if crew:
                        crew.step_callback = step_callback
                        crew.task_callback = task_callback
                        subprocess_logger.info(
                            f"Set execution callbacks on crew for {execution_id}"
                        )

                except Exception as e:
                    subprocess_logger.error(
                        f"Failed to initialize event listeners: {e}"
                    )
                    # Continue without listeners - don't fail the execution

                # Execute the crew synchronously (CrewAI's kickoff is sync)
                async_logger.info(f"🚀 Starting crew execution for {execution_id}")
                async_logger.info(f"Process ID: {os.getpid()}")
                async_logger.info(
                    f"Crew: {crew_config.get('run_name', 'Unnamed')} v{crew_config.get('version', '1.0')}"
                )
                async_logger.info(
                    f"Agents: {len(crew_config.get('agents', []))}, Tasks: {len(crew_config.get('tasks', []))}"
                )

                # Execute the crew (wrapped in MLflow root trace if tracing is ready)
                if inputs:
                    async_logger.info(f"Inputs provided: {list(inputs.keys())}")
                else:
                    async_logger.info("No inputs provided")
                async_logger.info(
                    f"[SUBPROCESS] ABOUT TO CALL crew.kickoff()"
                )

                from src.services.otel_tracing.mlflow_setup import (
                    execute_with_mlflow_trace,
                    post_execution_mlflow_cleanup,
                )


                kickoff_fn = (
                    (lambda: crew.kickoff(inputs=inputs))
                    if inputs
                    else crew.kickoff
                )
                result = execute_with_mlflow_trace(
                    kickoff_fn=kickoff_fn,
                    mlflow_result=mlflow_result,
                    crew_config=crew_config,
                    inputs=inputs,
                    async_logger=async_logger,
                )
                async_logger.info(f"✅ Crew execution completed successfully")

                await post_execution_mlflow_cleanup(
                    mlflow_result=mlflow_result,
                    execution_id=execution_id,
                    group_id=(
                        getattr(group_context, "primary_group_id", None)
                        if group_context
                        else None
                    ),
                    async_logger=async_logger,
                )

                # CRITICAL: Flush the CrewAI event bus to ensure all event handlers
                # (agent execution started/completed, tool usage, etc.) complete their
                # work before we return. The event bus dispatches handlers to a background
                # thread pool, so without an explicit flush, DB writes for llm_request/
                # llm_response traces may not complete before subprocess cleanup begins.
                try:
                    from crewai.events import crewai_event_bus as _event_bus

                    async_logger.info(
                        "[SUBPROCESS] Flushing CrewAI event bus to ensure all trace handlers complete..."
                    )
                    flushed = _event_bus.flush(timeout=30.0)
                    if flushed:
                        async_logger.info(
                            "[SUBPROCESS] Event bus flush completed - all handlers finished"
                        )
                    else:
                        async_logger.warning(
                            "[SUBPROCESS] Event bus flush timed out - some handlers may not have completed"
                        )
                except Exception as flush_err:
                    async_logger.warning(
                        f"[SUBPROCESS] Event bus flush error (non-fatal): {flush_err}"
                    )

                # Shutdown OTel TracerProvider to flush remaining spans
                try:
                    from src.services.otel_tracing import shutdown_provider
                    shutdown_provider()
                except Exception as otel_shutdown_err:
                    async_logger.debug(
                        f"[SUBPROCESS] OTel shutdown: {otel_shutdown_err}"
                    )

                # Stop MCP adapters to close streaming HTTP connections
                try:
                    from src.engines.crewai.tools.mcp_handler import stop_all_adapters
                    await stop_all_adapters()
                except Exception as mcp_err:
                    async_logger.debug(
                        f"[SUBPROCESS] MCP adapter cleanup: {mcp_err}"
                    )

                return result

        # Run the async preparation and execution with proper cleanup
        try:
            # Create event loop explicitly to handle cleanup properly
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(prepare_and_run())
            finally:
                # CRITICAL: Final flush of event bus to ensure all trace handlers complete
                # This is essential for llm_request/llm_response traces that are written
                # asynchronously by the event bus's thread pool
                try:
                    from crewai.events import crewai_event_bus as _cleanup_event_bus

                    _cleanup_event_bus.flush(timeout=15.0)
                except Exception:
                    pass

                # Dispose all SQLAlchemy async engines while the loop is still alive
                # to avoid MissingGreenlet/loop-mismatch errors during interpreter shutdown
                try:
                    from src.db.session import dispose_engines

                    loop.run_until_complete(dispose_engines())
                except Exception:
                    # Best-effort cleanup; continue regardless
                    pass

                # Give async tasks time to complete before closing loop
                try:
                    pending = asyncio.all_tasks(loop)
                    if pending:
                        # Give tasks a short time to complete gracefully
                        loop.run_until_complete(
                            asyncio.wait_for(
                                asyncio.gather(*pending, return_exceptions=True),
                                timeout=2.0,
                            )
                        )
                except (asyncio.TimeoutError, Exception):
                    # Ignore timeout/errors during cleanup
                    pass
                finally:
                    # Close the loop properly
                    try:
                        loop.close()
                    except Exception:
                        pass
        except Exception as async_error:
            print(f"[SUBPROCESS DEBUG] Async error: {async_error}", file=sys.stderr)
            import traceback

            traceback.print_exc(file=sys.stderr)
            raise

        # Use the regular logger after async function completes
        crew_logger = logging.getLogger("crew")
        crew_logger.info(
            f"Process {os.getpid()} completed crew execution for {execution_id}"
        )

        # Capture any output that was written to stdout/stderr
        output = captured_output.getvalue()
        if output:
            # Log the captured output to crew.log with execution ID
            # The database handler will automatically write to execution_logs
            for line in output.split("\n"):
                if line.strip():
                    crew_logger.info(f"[STDOUT] {line.strip()}")

        # Process the result safely, handling CrewAI result objects
        processed_result = None
        try:
            if hasattr(result, "raw"):
                # CrewAI result object with raw attribute
                processed_result = result.raw
            elif isinstance(result, dict):
                # Already a dictionary
                processed_result = str(result)
            else:
                # Convert any other result to string
                processed_result = str(result)
        except Exception as e:
            logger.warning(f"Error converting result to string: {e}. Using fallback.")
            processed_result = (
                "Crew execution completed but result could not be serialized"
            )

        # Collect MCP warnings to surface in the execution trace/UI
        mcp_warnings = []
        try:
            from src.engines.crewai.tools.mcp_integration import MCPIntegration
            mcp_warnings = MCPIntegration.get_warnings()
            if mcp_warnings:
                crew_logger = logging.getLogger("crew")
                crew_logger.warning(f"[MCP_WARNINGS] {'; '.join(mcp_warnings)}")
        except Exception:
            pass

        return {
            "status": "COMPLETED",
            "execution_id": execution_id,
            "result": processed_result,  # Safely processed result
            "process_id": os.getpid(),
            "warnings": mcp_warnings,
        }

    except Exception as e:
        # Log error to crew.log with execution ID
        error_logger = logging.getLogger("crew")
        error_logger.error(
            f"Process {os.getpid()} error in crew execution for {execution_id}: {e}"
        )
        error_logger.error(f"Traceback: {traceback.format_exc()}")

        # Flush event bus even on error to capture partial traces
        try:
            from crewai.events import crewai_event_bus as _event_bus

            _event_bus.flush(timeout=10.0)
        except Exception:
            pass

        # CRITICAL: Shutdown OTel on error path too, so the db_exporter's
        # thread pool drains and pending trace writes complete.
        try:
            from src.services.otel_tracing import shutdown_provider
            shutdown_provider()
        except Exception:
            pass

        # Wait for trace writer to flush remaining traces even on failure
        try:
            import time

            from src.services.trace_queue import get_trace_queue

            trace_queue = get_trace_queue()
            if trace_queue.qsize() > 0:
                logger.info(
                    f"Waiting for trace queue to flush on error (queue size: {trace_queue.qsize()})..."
                )
                max_wait = 5  # Shorter wait on error
                wait_interval = 0.1
                waited = 0

                while trace_queue.qsize() > 0 and waited < max_wait:
                    time.sleep(wait_interval)
                    waited += wait_interval

                if trace_queue.qsize() > 0:
                    logger.warning(
                        f"Trace queue still has {trace_queue.qsize()} items after error wait"
                    )

                # Give a bit more time for the writer to complete database writes
                time.sleep(0.5)

        except Exception as flush_error:
            logger.error(f"Error waiting for trace flush on error: {flush_error}")

        return {
            "status": "FAILED",
            "execution_id": execution_id,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "process_id": os.getpid(),
        }
    finally:
        # Restore stdout/stderr before process ends
        restore_stdout_stderr(original_stdout, original_stderr)

        # Clean up database connections and engines via tracing service utility
        try:
            from src.services.mlflow_tracing_service import (
                cleanup_async_db_connections as _cleanup_db,
            )

            _cleanup_db()
        except Exception as db_cleanup_error:
            logger.debug(f"Database cleanup note: {db_cleanup_error}")

        # Clean up any remaining child processes
        try:
            import psutil

            parent = psutil.Process(os.getpid())
            children = parent.children(recursive=True)

            if children:
                logger.info(f"Cleaning up {len(children)} remaining child processes")

                for child in children:
                    try:
                        child.terminate()
                    except psutil.NoSuchProcess:
                        pass

                # Give them a moment to terminate
                _, alive = psutil.wait_procs(children, timeout=2)

                # Force kill any that didn't terminate
                for p in alive:
                    try:
                        p.kill()
                    except psutil.NoSuchProcess:
                        pass
        except ImportError:
            # psutil not available, try basic cleanup
            logger.warning("psutil not available for cleanup")
        except Exception as cleanup_error:
            # Log but don't raise - we want to exit cleanly
            logger.error(f"Error during final cleanup: {cleanup_error}")


class ProcessCrewExecutor:
    """High-performance process-based executor for isolated CrewAI execution.

    This executor manages CrewAI crew executions in separate OS processes,
    providing complete isolation, resource management, and the ability to
    forcefully terminate stuck or runaway executions.

    Unlike thread-based execution, process isolation guarantees:
    - Complete memory separation between executions
    - Ability to forcefully terminate without affecting other executions
    - No resource leaks or zombie processes
    - Protection of the main application from crew failures

    Features:
        - One process per execution (no shared pool)
        - Concurrent execution management with configurable limits
        - Graceful and forceful termination support
        - Child process tracking and cleanup
        - Async/await interface for non-blocking operations
        - Comprehensive error handling and recovery

    Attributes:
        _ctx: Multiprocessing context using 'spawn' for better isolation
        _processes: Dictionary tracking active processes by execution ID
        _futures: Dictionary of asyncio futures for execution results
        _executor: Thread pool for managing process lifecycle
        _semaphore: Asyncio semaphore for concurrency control
        _lock: Asyncio lock for thread-safe operations

    Example:
        >>> executor = ProcessCrewExecutor(max_concurrent=4)
        >>> result = await executor.execute_crew_async(
        ...     execution_id="exec_123",
        ...     crew_config=config
        ... )
        >>> # To stop an execution:
        >>> stopped = await executor.stop_execution("exec_123")
    """

    def __init__(self, max_concurrent: int = 4):
        """Initialize the process executor with concurrency control.

        Sets up the multiprocessing context, tracking structures, and
        concurrency management primitives.

        Args:
            max_concurrent: Maximum number of concurrent crew executions.
                Defaults to 4 to balance resource usage and parallelism.

        Note:
            Uses 'spawn' context instead of 'fork' for better isolation
            and to avoid shared memory issues between processes.
        """
        # Use spawn method for better isolation (fork can share memory)
        self._ctx = mp.get_context("spawn")

        # Configure subprocess to suppress output
        # Set environment variable before creating the executor
        os.environ["PYTHONUNBUFFERED"] = "0"
        os.environ["CREWAI_VERBOSE"] = "false"

        # NO POOL - we create individual processes per execution
        self._max_concurrent = max_concurrent

        # Track running processes and their executors
        self._running_processes: Dict[str, mp.Process] = {}
        self._running_futures: Dict[str, Any] = {}
        self._running_executors: Dict[str, ProcessPoolExecutor] = {}

        # Metrics
        self._metrics = {
            "total_executions": 0,
            "active_executions": 0,
            "completed_executions": 0,
            "failed_executions": 0,
            "terminated_executions": 0,
        }

        logger.info(
            f"ProcessCrewExecutor initialized for per-execution processes (max concurrent: {max_concurrent})"
        )

    @staticmethod
    def _subprocess_initializer():
        """
        Initialize subprocess environment to suppress output.
        This runs once when each worker process is created.
        """
        import logging
        import os
        import sys

        # Suppress all output in subprocess
        os.environ["PYTHONUNBUFFERED"] = "0"
        os.environ["CREWAI_VERBOSE"] = "false"

        # Configure logging to suppress console output
        logging.basicConfig(level=logging.WARNING)

        # Suppress warnings
        import warnings

        warnings.filterwarnings("ignore")

    @staticmethod
    def _run_crew_wrapper(
        execution_id: str,
        crew_config: Dict[str, Any],
        inputs: Optional[Dict[str, Any]],
        group_context: Any,
        result_queue: mp.Queue,
        log_queue: mp.Queue,
    ):
        """
        Wrapper to run crew in subprocess and put result in queue.

        This method runs in the subprocess and handles the crew execution.
        """
        try:
            # Run the crew execution
            result = run_crew_in_process(
                execution_id, crew_config, inputs, group_context, log_queue
            )
            # Put result in the queue
            result_queue.put(result)
        except Exception as e:
            # Put error result in the queue
            result_queue.put(
                {"status": "FAILED", "execution_id": execution_id, "error": str(e)}
            )

    async def run_crew_isolated(
        self,
        execution_id: str,
        crew_config: Dict[str, Any],
        group_context: Any,  # MANDATORY - for tenant isolation
        inputs: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Run a crew in an isolated process using direct Process control.

        Args:
            execution_id: Unique identifier for the execution
            crew_config: Configuration to build the crew
            inputs: Optional inputs for the crew
            timeout: Optional timeout in seconds

        Returns:
            Dictionary with execution results
        """
        logger.info(
            f"[ProcessCrewExecutor] run_crew_isolated called for {execution_id}"
        )
        logger.info(
            f"[DEBUG_MLFLOW] crew_config keys: {list(crew_config.keys()) if isinstance(crew_config, dict) else 'not-dict'}"
        )

        self._metrics["total_executions"] += 1
        self._metrics["active_executions"] += 1

        start_time = datetime.now()

        # Use multiprocessing.Queue to get results from the subprocess
        result_queue = self._ctx.Queue()

        # Create a separate queue for logs from subprocess to main process
        # This follows the same pattern as execution_trace - collect in subprocess, write in main
        log_queue = self._ctx.Queue()

        # ⚠️ CRITICAL WARNING: DO NOT MODIFY GROUP_CONTEXT HANDLING ⚠️
        # The group_context MUST be passed as-is to the subprocess, even though it contains
        # an access_token that cannot be pickled. This is because:
        # 1. execution_trace relies on the original GroupContext object being available
        # 2. The subprocess execution works around the pickling issue internally
        # 3. Any attempt to convert GroupContext to a dict or minimal representation will
        #    BREAK execution_trace functionality
        #
        # Previous failed attempts that broke execution_trace:
        # - Creating a minimal dict with only essential fields
        # - Using to_dict() method on GroupContext
        # - Converting to serializable format
        #
        # execution_trace and execution_logs are DIFFERENT systems:
        # - execution_trace: Taps into CrewAI event bus for structured events
        # - execution_logs: Captures raw subprocess logs (crew.log, stdout)
        # DO NOT mix their implementations!

        # Pass group_context data to crew_config for subprocess execution
        # SECURITY: group_id is REQUIRED for multi-tenant isolation in subprocess
        # The authentication priority chain (OBO → PAT → SPN) is handled by databricks_auth.py
        if group_context:
            if isinstance(crew_config, dict):
                # Add group_id for multi-tenant isolation (REQUIRED)
                if (
                    hasattr(group_context, "primary_group_id")
                    and group_context.primary_group_id
                ):
                    crew_config["group_id"] = group_context.primary_group_id
                    logger.info(
                        f"[ProcessCrewExecutor] Added group_id to crew_config: {group_context.primary_group_id}"
                    )
                else:
                    logger.error(
                        "[ProcessCrewExecutor] SECURITY: No group_id in group_context - multi-tenant isolation may fail"
                    )

                # Add user_token for OBO authentication (optional, used by databricks_auth.py)
                if (
                    hasattr(group_context, "access_token")
                    and group_context.access_token
                ):
                    crew_config["user_token"] = group_context.access_token
                    logger.info(
                        f"[ProcessCrewExecutor] Added user_token to crew_config for OBO authentication"
                    )
                else:
                    logger.info(
                        "[ProcessCrewExecutor] No user_token - databricks_auth.py will use PAT or SPN fallback"
                    )

                # Add execution_id for memory session scoping
                # This is CRITICAL for short-term memory to only return results from the current run
                crew_config["execution_id"] = execution_id
                logger.info(
                    f"[ProcessCrewExecutor] Added execution_id to crew_config: {execution_id}"
                )
            else:
                logger.warning(
                    "[ProcessCrewExecutor] Cannot add user_token/group_id to crew_config - not a dict"
                )
        else:
            logger.error(
                "[ProcessCrewExecutor] SECURITY: No group_context provided - multi-tenant isolation will fail"
            )

        # Also add execution_id outside of group_context block to ensure it's always available
        if isinstance(crew_config, dict) and "execution_id" not in crew_config:
            crew_config["execution_id"] = execution_id
            logger.info(
                f"[ProcessCrewExecutor] Added execution_id to crew_config (fallback): {execution_id}"
            )

        # CRITICAL: Set KASAL_EXECUTION_ID in parent BEFORE spawning
        # This ensures the child process inherits it and psutil can see it
        old_kasal_exec_id = os.environ.get("KASAL_EXECUTION_ID")
        os.environ["KASAL_EXECUTION_ID"] = execution_id
        logger.info(
            f"[ProcessCrewExecutor] Set KASAL_EXECUTION_ID={execution_id} for subprocess inheritance"
        )

        # Propagate Lakebase config to subprocess so the OTel trace exporter
        # writes traces to Lakebase instead of the local DB when Lakebase is active.
        old_lakebase_active = os.environ.get("LAKEBASE_ACTIVE")
        old_lakebase_instance = os.environ.get("LAKEBASE_INSTANCE_NAME")
        try:
            from src.db.database_router import is_lakebase_enabled, get_lakebase_config_from_db
            import asyncio
            loop = asyncio.get_running_loop()
            lakebase_enabled = await is_lakebase_enabled()
            if lakebase_enabled:
                os.environ["LAKEBASE_ACTIVE"] = "true"
                lakebase_config = await get_lakebase_config_from_db()
                if lakebase_config:
                    instance_name = lakebase_config.get("instance_name") or os.environ.get("LAKEBASE_INSTANCE_NAME", "kasal-lakebase")
                    os.environ["LAKEBASE_INSTANCE_NAME"] = instance_name
                logger.info(
                    f"[ProcessCrewExecutor] Lakebase active — set LAKEBASE_ACTIVE=true, "
                    f"LAKEBASE_INSTANCE_NAME={os.environ.get('LAKEBASE_INSTANCE_NAME')}"
                )
            else:
                os.environ.pop("LAKEBASE_ACTIVE", None)
        except Exception as e:
            logger.debug(f"[ProcessCrewExecutor] Could not check Lakebase status: {e}")

        try:
            # Create a direct Process instead of using ProcessPoolExecutor
            # This gives us full control over the process lifecycle
            process = self._ctx.Process(
                target=self._run_crew_wrapper,
                args=(
                    execution_id,
                    crew_config,
                    inputs,
                    group_context,
                    result_queue,
                    log_queue,
                ),
            )

            # Store the process for tracking and termination
            self._running_processes[execution_id] = process

            # Start the process
            process.start()
            logger.info(f"Started process {process.pid} for execution {execution_id}")
        finally:
            # Restore parent's environment
            if old_kasal_exec_id is not None:
                os.environ["KASAL_EXECUTION_ID"] = old_kasal_exec_id
            else:
                os.environ.pop("KASAL_EXECUTION_ID", None)
            # Restore Lakebase env vars
            if old_lakebase_active is not None:
                os.environ["LAKEBASE_ACTIVE"] = old_lakebase_active
            else:
                os.environ.pop("LAKEBASE_ACTIVE", None)
            if old_lakebase_instance is not None:
                os.environ["LAKEBASE_INSTANCE_NAME"] = old_lakebase_instance

        try:
            # Start a background task to relay task lifecycle events from subprocess
            # to the main process for real-time SSE broadcasting
            relay_task = asyncio.create_task(
                self._relay_task_events(log_queue, execution_id)
            )

            # Wait for the process to complete with optional timeout
            if timeout:
                # Use asyncio to wait with timeout
                loop = asyncio.get_event_loop()
                future = loop.run_in_executor(None, process.join, timeout)
                await asyncio.wait_for(future, timeout=timeout)
            else:
                # Wait indefinitely
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, process.join)

            # Stop the relay task now that the subprocess has finished
            relay_task.cancel()
            try:
                await relay_task
            except asyncio.CancelledError:
                pass

            # Process logs from crew.log file and write to database
            # This reads the crew.log file after execution to capture ALL logs
            await self._process_log_queue(log_queue, execution_id, group_context)

            # Determine status based on process exit code
            # Exit codes: -15 (SIGTERM), -9 (SIGKILL) = terminated/stopped
            #            0 = normal completion
            #            >0 = error/failure
            if process.exitcode in [-15, -9]:
                # Process was terminated (stopped by user)
                logger.info(
                    f"Process {process.pid} was terminated with signal {-process.exitcode}"
                )
                result = {
                    "status": "STOPPED",
                    "execution_id": execution_id,
                    "message": f"Execution was stopped by user",
                    "exit_code": process.exitcode,
                }
            elif not result_queue.empty():
                # Process completed and returned a result
                result = result_queue.get_nowait()
                logger.info(
                    f"Process {process.pid} completed with result status: {result.get('status', 'UNKNOWN')}"
                )
            elif process.exitcode == 0:
                # Process completed normally but no result in queue
                result = {
                    "status": "COMPLETED",
                    "execution_id": execution_id,
                    "message": "Process completed successfully",
                }
            else:
                # Process failed with error
                logger.warning(
                    f"Process {process.pid} exited with error code {process.exitcode}"
                )
                result = {
                    "status": "FAILED",
                    "execution_id": execution_id,
                    "error": f"Process exited with code {process.exitcode}",
                    "exit_code": process.exitcode,
                }

            # Update metrics based on result
            if result.get("status") == "COMPLETED":
                self._metrics["completed_executions"] += 1
            elif result.get("status") == "STOPPED":
                self._metrics["terminated_executions"] += 1
            else:
                self._metrics["failed_executions"] += 1

            duration = (datetime.now() - start_time).total_seconds()
            logger.info(
                f"Process {process.pid} execution {execution_id} finished in {duration:.2f}s with status {result.get('status', 'UNKNOWN')}"
            )

            return result

        except asyncio.TimeoutError:
            logger.error(
                f"Process execution {execution_id} timed out after {timeout} seconds"
            )
            # Try to terminate the process
            await self.terminate_execution(execution_id)
            self._metrics["failed_executions"] += 1
            return {
                "status": "TIMEOUT",
                "execution_id": execution_id,
                "error": f"Execution timed out after {timeout} seconds",
            }

        except Exception as e:
            logger.error(f"Process execution {execution_id} failed: {e}")
            self._metrics["failed_executions"] += 1
            return {"status": "FAILED", "execution_id": execution_id, "error": str(e)}

        finally:
            self._metrics["active_executions"] -= 1

            # CRITICAL: Terminate the process to prevent zombie processes
            if execution_id in self._running_processes:
                process = self._running_processes[execution_id]
                if process.is_alive():
                    try:
                        # First try graceful termination
                        logger.info(
                            f"Terminating process {process.pid} for execution {execution_id}"
                        )
                        process.terminate()

                        # Wait up to 2 seconds for graceful termination
                        process.join(timeout=2)

                        if process.is_alive():
                            # Force kill if still alive
                            logger.warning(
                                f"Force killing process {process.pid} for execution {execution_id}"
                            )
                            process.kill()
                            process.join(timeout=1)  # Wait briefly for kill

                        logger.info(f"Process {process.pid} terminated successfully")
                    except Exception as e:
                        logger.error(
                            f"Error terminating process for {execution_id}: {e}"
                        )
                        # Try psutil as fallback
                        try:
                            import psutil

                            psutil_proc = psutil.Process(process.pid)
                            psutil_proc.kill()
                            logger.info(
                                f"Force killed process {process.pid} using psutil"
                            )
                        except:
                            pass

                # Remove from tracking
                del self._running_processes[execution_id]

            # Additional cleanup: Kill any lingering child processes
            # This runs after successful completion, error, or timeout
            try:
                # Try psutil first (best option)
                import psutil

                # Try to find and kill any processes still running with our execution ID
                # Look for processes that might be orphaned
                for proc in psutil.process_iter(["pid", "name", "cmdline", "ppid"]):
                    try:
                        # Check if process command line contains our execution ID
                        cmdline = proc.info.get("cmdline", [])
                        if cmdline and any(execution_id in str(arg) for arg in cmdline):
                            logger.warning(
                                f"Found orphaned process {proc.info['pid']} for execution {execution_id}, terminating..."
                            )
                            proc.terminate()
                            try:
                                proc.wait(timeout=2)  # Wait up to 2 seconds
                            except psutil.TimeoutExpired:
                                proc.kill()  # Force kill if still running
                                logger.warning(
                                    f"Force killed orphaned process {proc.info['pid']}"
                                )
                    except (
                        psutil.NoSuchProcess,
                        psutil.AccessDenied,
                        psutil.ZombieProcess,
                    ):
                        pass  # Process already gone or we can't access it

                # Also check for any multiprocessing.spawn processes without proper parents
                # These are often left over from ProcessPoolExecutor
                for proc in psutil.process_iter(["pid", "name", "ppid", "create_time"]):
                    try:
                        proc_info = proc.info
                        # Look for Python processes spawned by multiprocessing
                        if proc_info["name"] and "python" in proc_info["name"].lower():
                            # Check if parent is dead (ppid = 1 on Unix means orphaned)
                            if proc_info["ppid"] == 1:
                                # Check if it was created recently (within last 10 minutes)
                                create_time = datetime.fromtimestamp(proc.create_time())
                                age_minutes = (
                                    datetime.now() - create_time
                                ).total_seconds() / 60
                                if age_minutes < 10:
                                    logger.info(
                                        f"Found recent orphaned Python process {proc_info['pid']} (age: {age_minutes:.1f} min)"
                                    )
                                    # Don't auto-kill these - just log for now
                                    # proc.terminate()
                    except (
                        psutil.NoSuchProcess,
                        psutil.AccessDenied,
                        psutil.ZombieProcess,
                    ):
                        pass

            except ImportError:
                # Fallback: Use subprocess to find and kill processes (Unix/Linux/macOS)
                logger.info("psutil not available, using fallback process cleanup")
                try:
                    import signal
                    import subprocess

                    # Try to find processes with our execution ID using ps command
                    result = subprocess.run(
                        ["ps", "aux"], capture_output=True, text=True, timeout=5
                    )

                    if result.returncode == 0:
                        # Parse ps output to find processes with our execution ID
                        for line in result.stdout.split("\n"):
                            if (
                                execution_id[:8] in line
                                and "multiprocessing.spawn" in line
                            ):
                                # Extract PID (second column in ps aux output)
                                parts = line.split()
                                if len(parts) > 1:
                                    try:
                                        pid = int(parts[1])
                                        logger.info(
                                            f"Found orphaned process {pid} with execution ID, terminating..."
                                        )
                                        os.kill(pid, signal.SIGTERM)
                                    except (ValueError, OSError) as e:
                                        logger.debug(f"Could not kill process: {e}")

                except Exception as e:
                    logger.debug(f"Fallback process cleanup failed: {e}")

            except Exception as cleanup_error:
                logger.error(
                    f"Error during process cleanup for {execution_id}: {cleanup_error}"
                )

            # Cleanup tracking
            if execution_id in self._running_futures:
                del self._running_futures[execution_id]
            if execution_id in self._running_executors:
                # Should already be deleted above, but ensure cleanup
                del self._running_executors[execution_id]

    async def _relay_task_events(self, task_event_queue, execution_id: str):
        """
        Read task lifecycle events from the subprocess multiprocessing.Queue
        and broadcast them via SSE for real-time frontend updates.

        Runs concurrently with process.join() so events are relayed in real-time.
        """
        from queue import Empty

        from src.core.sse_manager import SSEEvent, sse_manager

        logger.info(
            f"[ProcessCrewExecutor] Starting task event relay for {execution_id}"
        )

        while True:
            try:
                # Block with short timeout to allow cancellation checks
                trace_data = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: task_event_queue.get(block=True, timeout=0.5)
                )
            except Empty:
                continue
            except asyncio.CancelledError:
                logger.info(
                    f"[ProcessCrewExecutor] Task event relay cancelled for {execution_id}"
                )
                break
            except Exception as e:
                logger.warning(
                    f"[ProcessCrewExecutor] Error reading task event queue: {e}"
                )
                continue

            if trace_data is None:
                continue

            event_type = trace_data.get("event_type", "")
            if event_type not in ("task_started", "task_completed", "task_failed"):
                continue

            try:
                extra_data = trace_data.get("extra_data", {})
                trace_metadata = trace_data.get("trace_metadata", extra_data)

                sse_trace_data = {
                    "job_id": execution_id,
                    "event_source": trace_data.get("event_source", ""),
                    "event_type": event_type,
                    "event_context": trace_data.get("event_context", ""),
                    "output": trace_data.get("output"),
                    "trace_metadata": {
                        "task_name": extra_data.get("task_name")
                        or trace_data.get("event_context"),
                        "task_id": extra_data.get("task_id"),
                        "agent_role": extra_data.get("agent_role"),
                        "crew_name": extra_data.get("crew_name"),
                        "frontend_task_id": extra_data.get("frontend_task_id"),
                        **trace_metadata,
                    },
                    "created_at": (
                        trace_data.get("created_at", datetime.now().isoformat())
                        if isinstance(trace_data.get("created_at"), str)
                        else datetime.now().isoformat()
                    ),
                }

                event = SSEEvent(
                    data=sse_trace_data,
                    event="trace",
                    id=f"{execution_id}_{event_type}_{datetime.now().timestamp()}",
                )
                sent_count = await sse_manager.broadcast_to_job(execution_id, event)
                logger.info(
                    f"[ProcessCrewExecutor] Relayed {event_type} SSE to {sent_count} clients for job {execution_id}"
                )
            except Exception as sse_err:
                logger.warning(
                    f"[ProcessCrewExecutor] Failed to broadcast relayed {event_type}: {sse_err}"
                )

    async def _process_log_queue(
        self, log_queue, execution_id: str, group_context=None
    ):
        """
        Read crew.log file and write logs for the execution to the database.
        This is a better approach than using queues since it captures ALL logs.

        Args:
            log_queue: Not used anymore, kept for compatibility
            execution_id: The execution ID for logging
            group_context: Optional group context for multi-tenant isolation
        """
        logger.info(
            f"[ProcessCrewExecutor] [DEBUG] Starting _process_log_queue for {execution_id}"
        )
        logger.info(
            f"[ProcessCrewExecutor] Processing logs from crew.log for {execution_id}"
        )

        try:
            import json
            import os
            from datetime import datetime, timezone

            from sqlalchemy import text

            # Get the crew.log path
            log_dir = os.environ.get("LOG_DIR")
            if not log_dir:
                import pathlib

                backend_root = pathlib.Path(__file__).parent.parent.parent
                log_dir = backend_root / "logs"

            crew_log_path = os.path.join(log_dir, "crew.log")

            if not os.path.exists(crew_log_path):
                logger.warning(f"crew.log file not found at {crew_log_path}")
                return

            # Extract logs for our execution ID
            logs_to_write = []
            exec_id_short = execution_id[:8]  # Use short ID for matching

            # First, add a header log entry to mark the start
            logs_to_write.append(
                {
                    "execution_id": execution_id,
                    "content": f"[EXECUTION_START] ========== Execution {execution_id} Started ==========",
                    "timestamp": datetime.utcnow(),  # Use timezone-naive UTC datetime for database consistency
                    "group_id": (
                        getattr(group_context, "primary_group_id", None)
                        if group_context
                        else None
                    ),
                    "group_email": (
                        getattr(group_context, "group_email", None)
                        if group_context
                        else None
                    ),
                }
            )

            # Read crew.log and extract relevant logs
            with open(crew_log_path, "r") as f:
                for line in f:
                    if exec_id_short in line:
                        # This log belongs to our execution
                        logs_to_write.append(
                            {
                                "execution_id": execution_id,
                                "content": line.strip(),
                                "timestamp": datetime.utcnow(),  # Use timezone-naive UTC datetime for database consistency
                                "group_id": (
                                    getattr(group_context, "primary_group_id", None)
                                    if group_context
                                    else None
                                ),
                                "group_email": (
                                    getattr(group_context, "group_email", None)
                                    if group_context
                                    else None
                                ),
                            }
                        )


            if len(logs_to_write) <= 1:  # Only has JobConfiguration
                logger.info(f"No logs found for execution {exec_id_short} in crew.log")
                # Still write the JobConfiguration log
            else:
                logger.info(
                    f"Found {len(logs_to_write)-1} logs for execution {exec_id_short} in crew.log"
                )

            # Use the application's existing engine from session.py.
            # This runs in the main process (after process.join), so the app
            # engine is available and already connected to the correct database.
            # Previously, a one-off engine was created from individual settings
            # fields (POSTGRES_SERVER, POSTGRES_PORT, etc.) which default to
            # localhost — breaking in Databricks Apps where DATABASE_URI is set
            # as a single env var pointing to the managed PostgreSQL instance.
            from src.db.session import engine as app_engine

            logger.info(
                f"[ProcessCrewExecutor] [DEBUG] Using application engine for database connection"
            )
            async with app_engine.begin() as conn:
                for log_data in logs_to_write:
                    await conn.execute(
                        text("""
                        INSERT INTO execution_logs (execution_id, content, timestamp, group_id, group_email)
                        VALUES (:execution_id, :content, :timestamp, :group_id, :group_email)
                    """),
                        log_data,
                    )

            logger.info(
                f"[ProcessCrewExecutor] Successfully wrote {len(logs_to_write)} logs to execution_logs table"
            )

        except Exception as e:
            import traceback

            logger.error(
                f"[ProcessCrewExecutor] [DEBUG] Error processing logs from crew.log: {e}"
            )
            logger.error(
                f"[ProcessCrewExecutor] [DEBUG] Error type: {type(e).__name__}"
            )
            logger.error(
                f"[ProcessCrewExecutor] [DEBUG] Full traceback:\n{traceback.format_exc()}"
            )
            logger.error(
                f"[ProcessCrewExecutor] Error processing logs from crew.log: {e}"
            )

    async def terminate_execution(self, execution_id: str) -> bool:
        """
        Forcefully terminate a running execution process.

        This directly kills the process, ensuring complete cleanup.

        Args:
            execution_id: The execution to terminate

        Returns:
            True if terminated, False if not found
        """
        terminated = False

        # Terminate the process if it exists
        if execution_id in self._running_processes:
            process = self._running_processes[execution_id]

            if process.is_alive():
                try:
                    pid = process.pid
                    logger.info(
                        f"Terminating process {pid} for execution {execution_id}"
                    )

                    # Try graceful termination first
                    process.terminate()

                    # Give it a moment to terminate (non-blocking check)
                    process.join(timeout=0.5)

                    if process.is_alive():
                        # Force kill if still alive
                        logger.warning(
                            f"Force killing process {pid} for execution {execution_id}"
                        )
                        process.kill()
                        process.join(timeout=0.5)

                    logger.info(
                        f"Successfully terminated process {pid} for execution {execution_id}"
                    )
                    terminated = True

                except Exception as e:
                    logger.error(f"Error terminating process for {execution_id}: {e}")
                    # Try psutil as fallback
                    try:
                        import psutil

                        if process.pid:
                            psutil_proc = psutil.Process(process.pid)
                            psutil_proc.kill()
                            logger.info(f"Force killed process using psutil")
                            terminated = True
                    except:
                        pass
            else:
                logger.info(f"Process for execution {execution_id} already terminated")
                terminated = True

            # Remove from tracking
            del self._running_processes[execution_id]

        # If not found in tracking (e.g., server reloaded), search ALL processes
        if not terminated:
            logger.info(
                f"[ProcessCrewExecutor] Process not in tracking, searching all processes for {execution_id}..."
            )
            terminated = self._terminate_orphaned_process(execution_id)

        if terminated:
            self._metrics["terminated_executions"] += 1

        return terminated

    def _terminate_orphaned_process(self, execution_id: str) -> bool:
        """
        Find and terminate orphaned crew processes by searching all running processes.

        This handles the case where the server was reloaded and we lost the process reference.
        It searches for processes that have the KASAL_EXECUTION_ID environment variable.

        Args:
            execution_id: The execution ID to search for

        Returns:
            True if a matching process was found and terminated
        """
        try:
            import psutil

            # Use short execution_id for matching (first 8 chars is common in logs)
            exec_id_short = execution_id[:8]
            killed_count = 0

            logger.info(
                f"[ProcessCrewExecutor] Searching for orphaned processes with execution_id {exec_id_short}..."
            )

            # Search ALL processes, not just children of current process
            for proc in psutil.process_iter(["pid", "name", "cmdline"]):
                try:
                    # Skip non-Python processes for efficiency
                    proc_name = proc.info.get("name", "").lower()
                    if "python" not in proc_name:
                        continue

                    # Check command line for execution_id
                    cmdline = proc.info.get("cmdline", [])
                    cmdline_str = " ".join(cmdline) if cmdline else ""

                    # Check for KASAL_EXECUTION_ID environment variable (most reliable)
                    kasal_exec_id = None
                    try:
                        env = proc.environ()
                        kasal_exec_id = env.get("KASAL_EXECUTION_ID", "")
                    except (psutil.AccessDenied, psutil.NoSuchProcess):
                        pass

                    # Check if this process matches our execution
                    is_match = False
                    if kasal_exec_id:
                        is_match = (
                            kasal_exec_id == execution_id
                            or kasal_exec_id.startswith(exec_id_short)
                        )
                    elif execution_id in cmdline_str or exec_id_short in cmdline_str:
                        is_match = True

                    if is_match:
                        pid = proc.info["pid"]
                        logger.info(
                            f"[ProcessCrewExecutor] Found matching process {pid}, terminating..."
                        )

                        # Kill the process and all its children
                        try:
                            parent = psutil.Process(pid)
                            children = parent.children(recursive=True)

                            # Terminate children first
                            for child in children:
                                try:
                                    child.kill()
                                    logger.info(
                                        f"[ProcessCrewExecutor] Terminated child process {child.pid}"
                                    )
                                except (psutil.NoSuchProcess, psutil.AccessDenied):
                                    pass

                            # Give children time to terminate
                            if children:
                                psutil.wait_procs(children, timeout=2)

                            # Now kill the parent
                            parent.kill()
                            logger.info(
                                f"[ProcessCrewExecutor] Successfully terminated orphaned process {pid}"
                            )
                            killed_count += 1

                        except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
                            logger.warning(
                                f"[ProcessCrewExecutor] Could not terminate process {pid}: {e}"
                            )

                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

            if killed_count > 0:
                logger.info(
                    f"[ProcessCrewExecutor] Terminated {killed_count} orphaned processes for {execution_id}"
                )
                return True
            else:
                logger.warning(
                    f"[ProcessCrewExecutor] No orphaned processes found for {execution_id}"
                )
                return False

        except ImportError:
            logger.error(
                "[ProcessCrewExecutor] psutil not available for orphaned process cleanup"
            )
            return False
        except Exception as e:
            logger.error(
                f"[ProcessCrewExecutor] Error searching for orphaned processes: {e}"
            )
            return False

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get executor metrics.

        Returns:
            Dictionary of metrics
        """
        return self._metrics.copy()

    def shutdown(self, wait: bool = True):
        """
        Shutdown the process executor and terminate all running processes.

        Args:
            wait: Whether to wait for processes to terminate
        """
        logger.info("Shutting down ProcessCrewExecutor")

        # Terminate all running processes
        for execution_id, process in list(self._running_processes.items()):
            if process.is_alive():
                try:
                    logger.info(
                        f"Terminating process {process.pid} for execution {execution_id}"
                    )
                    process.terminate()
                    if wait:
                        process.join(timeout=2)
                        if process.is_alive():
                            process.kill()
                            process.join(timeout=1)
                except Exception as e:
                    logger.error(f"Error terminating process for {execution_id}: {e}")

        # Clear all tracking
        self._running_processes.clear()
        self._running_futures.clear()
        self._running_executors.clear()

        # Extra cleanup: Kill any remaining worker processes from the pool
        try:
            import os

            import psutil

            current_pid = os.getpid()
            current_process = psutil.Process(current_pid)

            # Find all child processes (workers from ProcessPoolExecutor)
            children = current_process.children(recursive=True)
            if children:
                logger.info(f"Found {len(children)} child processes to clean up")
                for child in children:
                    try:
                        logger.info(f"Terminating child process {child.pid}")
                        child.terminate()
                    except psutil.NoSuchProcess:
                        pass

                # Give them time to terminate gracefully
                gone, alive = psutil.wait_procs(children, timeout=3)

                # Force kill any that didn't terminate
                for p in alive:
                    try:
                        logger.warning(f"Force killing stubborn child process {p.pid}")
                        p.kill()
                    except psutil.NoSuchProcess:
                        pass
        except Exception as e:
            logger.error(f"Error during executor shutdown cleanup: {e}")

        logger.info(
            f"ProcessCrewExecutor shutdown complete. Final metrics: {self.get_metrics()}"
        )

    @staticmethod
    def kill_orphan_crew_processes():
        """
        Static method to find and kill orphaned crew processes.
        Can be called independently to clean up stale processes.
        """
        try:
            import psutil

            killed_count = 0

            logger.info("Scanning for orphaned crew processes...")

            for proc in psutil.process_iter(
                ["pid", "name", "cmdline", "ppid", "create_time"]
            ):
                try:
                    proc_info = proc.info
                    cmdline = proc_info.get("cmdline", [])

                    # Look for processes that might be crew-related
                    is_crew_process = False

                    # Check if command line contains crew-related keywords
                    if cmdline:
                        cmd_str = " ".join(str(arg) for arg in cmdline)
                        crew_keywords = [
                            "run_crew_in_process",
                            "CrewAI",
                            "crew.kickoff",
                            "multiprocessing.spawn",
                            "ProcessPoolExecutor",
                        ]
                        if any(keyword in cmd_str for keyword in crew_keywords):
                            is_crew_process = True

                    # Check for orphaned Python processes (ppid = 1)
                    if proc_info["name"] and "python" in proc_info["name"].lower():
                        if proc_info["ppid"] == 1:  # Orphaned process
                            is_crew_process = True

                    if is_crew_process:
                        # Check age - only kill if older than 1 minute
                        create_time = datetime.fromtimestamp(proc.create_time())
                        age_minutes = (
                            datetime.now() - create_time
                        ).total_seconds() / 60

                        if (
                            age_minutes > 1
                        ):  # Process is old enough to be considered orphaned
                            logger.warning(
                                f"Killing orphaned crew process {proc_info['pid']} (age: {age_minutes:.1f} min)"
                            )
                            proc.terminate()
                            try:
                                proc.wait(timeout=2)
                            except psutil.TimeoutExpired:
                                proc.kill()
                            killed_count += 1

                except (
                    psutil.NoSuchProcess,
                    psutil.AccessDenied,
                    psutil.ZombieProcess,
                ):
                    pass

            if killed_count > 0:
                logger.info(f"Killed {killed_count} orphaned crew processes")
            else:
                logger.info("No orphaned crew processes found")

            return killed_count

        except ImportError:
            logger.error("psutil not available - cannot clean orphan processes")
            return 0
        except Exception as e:
            logger.error(f"Error killing orphan processes: {e}")
            return 0

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.shutdown(wait=True)
        return False


# Global instance
process_crew_executor = ProcessCrewExecutor()


# Configuration to choose execution mode
class ExecutionMode:
    """Configuration for selecting crew execution mode.

    This class defines execution modes and provides logic for determining
    whether to use thread-based or process-based execution based on the
    crew configuration and requirements.

    Attributes:
        THREAD: Thread pool execution mode - faster but less isolation
        PROCESS: Process pool execution mode - complete isolation but slower

    Note:
        Process mode is recommended for:
        - Long-running executions that need termination capability
        - Untrusted or potentially unstable code
        - Memory-intensive operations requiring isolation
        - Multi-tenant scenarios requiring strict separation
    """

    THREAD = "thread"  # Default: Use thread pool (faster, less isolation)
    PROCESS = "process"  # Use process pool (slower, complete isolation)

    @staticmethod
    def should_use_process(crew_config: Dict[str, Any]) -> bool:
        """Determine if process isolation should be used for execution.

        Analyzes the crew configuration to decide whether process-based
        execution is necessary for safety, isolation, or termination needs.

        Args:
            crew_config: The crew configuration

        Returns:
            True if process isolation should be used
        """
        # Use process isolation for:
        # 1. Untrusted or experimental crews
        # 2. Long-running crews (>10 minutes expected)
        # 3. Crews marked as requiring isolation

        if crew_config.get("require_isolation", False):
            return True

        if crew_config.get("expected_duration_minutes", 0) > 10:
            return True

        if crew_config.get("experimental", False):
            return True

        # Default to thread execution
        return False
