"""Process-based flow executor for isolated AI flow execution.

This module implements process-based execution for CrewAI flows, providing
true isolation and reliable termination capabilities similar to crew execution.

Process isolation ensures that:
- Flow failures don't affect the main application
- Resources are properly cleaned up on termination
- Child processes (crews within flows) are tracked and terminated on stop requests
- Memory and CPU usage are isolated per execution

Key Features:
    - True process isolation for flow execution
    - Graceful and forceful termination support
    - Child process tracking and cleanup (including crew processes)
    - Subprocess logging configuration
    - Signal handling for clean shutdown
    - Multi-tenant support through group context

Architecture:
    The executor spawns flows in separate OS processes using multiprocessing,
    allowing complete control over the execution lifecycle including the
    ability to forcefully terminate stuck or runaway processes.

Example:
    >>> executor = ProcessFlowExecutor()
    >>> task = await executor.execute_flow_async(
    ...     execution_id="exec_123",
    ...     flow_config=config,
    ...     group_context=context
    ... )
    >>> # Later, if needed:
    >>> await executor.stop_execution("exec_123")
"""
# Global hardening: disable CrewAI cloud tracing/telemetry and suppress interactive prompts at module import time
try:
    import os as _kasal_fe_env
    _kasal_fe_env.environ["CREWAI_TRACING_ENABLED"] = "false"
    _kasal_fe_env.environ["CREWAI_TELEMETRY_OPT_OUT"] = "1"
    _kasal_fe_env.environ["CREWAI_ANALYTICS_OPT_OUT"] = "1"
    _kasal_fe_env.environ["CREWAI_CLOUD_TRACING"] = "false"
    _kasal_fe_env.environ["CREWAI_CLOUD_TRACING_ENABLED"] = "false"
    _kasal_fe_env.environ["CREWAI_VERBOSE"] = "false"
    _kasal_fe_env.environ["PYTHONUNBUFFERED"] = "0"
except Exception:
    pass

# Suppress interactive prompts globally
try:
    import builtins as _kasal_builtins_mod
    def _kasal_noinput_global(prompt=None):
        try:
            if prompt:
                print(f"[FLOW_SUBPROCESS] Suppressed interactive prompt at import: {prompt}")
        except Exception:
            pass
        return "n"
    _kasal_builtins_mod.input = _kasal_noinput_global
except Exception:
    pass

# Also disable common CLI confirm/prompt mechanisms if present (click)
try:
    import click as _kasal_click
    _kasal_click.confirm = (lambda *a, **k: False)
    _kasal_click.prompt = (lambda *a, **k: "")
except Exception:
    pass

import asyncio
import logging
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, Any, Optional
import pickle
import traceback
import signal
import os
from datetime import datetime, timezone


logger = logging.getLogger(__name__)


def run_flow_in_process(
    execution_id: str,
    flow_config: Dict[str, Any],
    inputs: Optional[Dict[str, Any]] = None,
    group_context: Any = None,  # Group context for tenant isolation
    log_queue: Optional[Any] = None  # Queue for sending logs to main process
) -> Dict[str, Any]:
    """Execute a CrewAI flow in an isolated subprocess.

    This function runs in a completely separate OS process, providing
    true isolation from the main application. It rebuilds the flow from
    configuration, sets up logging, handles signals, and executes the flow.

    The function is designed to be called via multiprocessing and includes
    comprehensive error handling and cleanup logic.

    Args:
        execution_id: Unique identifier for tracking this execution.
            Used for logging and trace correlation.
        flow_config: Complete configuration dictionary to rebuild the flow,
            including nodes, edges, flow_config, and settings.
        inputs: Optional dictionary of inputs to pass to the flow.
            These become available during flow execution.
        group_context: Optional multi-tenant context for isolation.
            Contains group_id, access_token, and other tenant info.
        log_queue: Optional queue for sending logs to the main process.
            Enables real-time log streaming to parent process.

    Returns:
        Dict[str, Any]: Execution results containing:
            - output: The flow execution output
            - status: Final execution status
            - error: Error details if execution failed
            - execution_time: Total execution duration

    Note:
        This function sets up signal handlers for SIGTERM and SIGINT
        to ensure proper cleanup of child processes (including crew processes) on termination.

    Environment Variables Set:
        - FLOW_SUBPROCESS_MODE: Marks subprocess execution mode
        - DATABASE_TYPE: Ensures correct database configuration
        - CREWAI_VERBOSE: Controls CrewAI output verbosity
    """
    # Import necessary modules at the beginning
    import os
    import sys
    import traceback
    import logging

    # Mark that we're in subprocess mode for logging purposes
    os.environ['FLOW_SUBPROCESS_MODE'] = 'true'
    # CRITICAL: Set CREW_SUBPROCESS_MODE for AgentTraceEventListener to write directly to DB
    os.environ['CREW_SUBPROCESS_MODE'] = 'true'
    # Set debug tracing flag (default to true for comprehensive logging)
    os.environ['CREWAI_DEBUG_TRACING'] = 'true'

    # Ensure DATABASE_TYPE is set correctly in subprocess
    if 'DATABASE_TYPE' not in os.environ:
        from src.config.settings import settings
        os.environ['DATABASE_TYPE'] = settings.DATABASE_TYPE or 'postgres'
        print(f"[FLOW_SUBPROCESS] Set DATABASE_TYPE to: {os.environ['DATABASE_TYPE']}")

    # Early validation of parameters
    import json

    try:
        # Handle None or empty flow_config
        if flow_config is None:
            error_msg = "flow_config is None"
            print(f"[FLOW_SUBPROCESS ERROR] {error_msg}", file=sys.stderr)
            return {
                "status": "FAILED",
                "execution_id": execution_id,
                "error": error_msg,
                "process_id": os.getpid()
            }

        # Check if flow_config is a string (JSON) and parse it BEFORE validation
        if isinstance(flow_config, str):
            try:
                flow_config = json.loads(flow_config)
                print(f"[FLOW_SUBPROCESS] Parsed flow_config from JSON string", file=sys.stderr)
            except json.JSONDecodeError as e:
                error_msg = f"Failed to parse flow_config JSON: {e}"
                print(f"[FLOW_SUBPROCESS ERROR] {error_msg}", file=sys.stderr)
                return {
                    "status": "FAILED",
                    "execution_id": execution_id,
                    "error": error_msg,
                    "process_id": os.getpid()
                }

        # Now validate flow_config type
        if not isinstance(flow_config, dict):
            error_msg = f"flow_config must be a dict, got {type(flow_config)} with value: {repr(flow_config)[:100]}"
            print(f"[FLOW_SUBPROCESS ERROR] {error_msg}", file=sys.stderr)
            return {
                "status": "FAILED",
                "execution_id": execution_id,
                "error": error_msg,
                "process_id": os.getpid()
            }
    except Exception as validation_error:
        return {
            "status": "FAILED",
            "execution_id": execution_id,
            "error": f"Parameter validation error: {str(validation_error)}",
            "process_id": os.getpid() if 'os' in locals() else 0
        }

    # Configure logging
    from src.engines.crewai.logging_config import (
        configure_subprocess_logging,
        suppress_stdout_stderr,
        restore_stdout_stderr
    )

    # Suppress all stdout/stderr output
    original_stdout, original_stderr, captured_output = suppress_stdout_stderr()

    # Set up signal handlers for graceful shutdown with child process cleanup
    def signal_handler(signum, frame):
        # Kill all child processes spawned by this subprocess (including crew processes)
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
        # Force-disable CrewAI cloud tracing/telemetry
        _os_crewai_env.environ["CREWAI_TRACING_ENABLED"] = "false"
        _os_crewai_env.environ["CREWAI_TELEMETRY_OPT_OUT"] = "1"
        _os_crewai_env.environ["CREWAI_ANALYTICS_OPT_OUT"] = "1"
        _os_crewai_env.environ["CREWAI_CLOUD_TRACING"] = "false"
        _os_crewai_env.environ["CREWAI_CLOUD_TRACING_ENABLED"] = "false"
        _os_crewai_env.environ["CREWAI_VERBOSE"] = "false"

        # Configure subprocess logging
        async_logger = configure_subprocess_logging(execution_id, process_type="flow")

        async_logger.info(f"[FLOW_SUBPROCESS] Starting flow execution in subprocess (PID: {os.getpid()})")

        # Extract group_id from group_context for UserContext
        group_id = None
        if group_context:
            if hasattr(group_context, 'primary_group_id'):
                group_id = group_context.primary_group_id
            elif hasattr(group_context, 'group_ids') and group_context.group_ids:
                group_id = group_context.group_ids[0]

        # Initialize UserContext in subprocess
        if group_id:
            try:
                from src.utils.user_context import UserContext
                # Set group context in subprocess (contextvars don't transfer across processes)
                UserContext.set_group_context(group_context)
                async_logger.info(f"[FLOW_SUBPROCESS] ✓ Set UserContext with group_id: {group_id}")

                # Also set user token if available
                user_token = flow_config.get('user_token')
                if user_token:
                    UserContext.set_user_token(user_token)
                    async_logger.info(f"[FLOW_SUBPROCESS] ✓ UserContext set with OBO token")

                # Verify
                verify = UserContext.get_group_context()
                if verify and verify.primary_group_id == group_id:
                    async_logger.info(f"[FLOW_SUBPROCESS] ✓ UserContext verified: {group_id}")
                else:
                    async_logger.error(f"[FLOW_SUBPROCESS] ✗ UserContext verification failed: {verify}")
            except Exception as e:
                async_logger.error(f"[FLOW_SUBPROCESS] Failed to initialize UserContext: {e}")

        # Initialize TraceManager and event listeners in subprocess (CRITICAL for execution_logs and execution_trace)
        try:
            async_logger.info(f"[FLOW_SUBPROCESS] Initializing TraceManager and event listeners for {execution_id}")

            # Create new event loop for async initialization
            init_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(init_loop)

            async def initialize_tracing():
                try:
                    from src.engines.crewai.trace_management import TraceManager
                    from src.engines.crewai.callbacks.logging_callbacks import (
                        AgentTraceEventListener,
                        TaskCompletionEventListener
                    )
                    from crewai.events import crewai_event_bus

                    # Start trace and logs writers
                    await TraceManager.ensure_writer_started()
                    async_logger.info(f"[FLOW_SUBPROCESS] TraceManager writer started for {execution_id}")

                    # Create and register event listeners
                    # Read debug tracing flag from environment variable
                    debug_tracing_enabled = os.environ.get('CREWAI_DEBUG_TRACING', 'true').lower() == 'true'
                    async_logger.info(f"[FLOW_SUBPROCESS] Debug tracing flag from environment: {debug_tracing_enabled}")

                    trace_listener = AgentTraceEventListener(
                        job_id=execution_id,
                        group_context=group_context,
                        debug_tracing=debug_tracing_enabled
                    )
                    trace_listener.setup_listeners(crewai_event_bus)
                    async_logger.info(f"[FLOW_SUBPROCESS] AgentTraceEventListener registered for {execution_id} (debug_tracing={debug_tracing_enabled})")

                    # Log that subprocess mode is enabled for direct DB writes
                    async_logger.info(f"[FLOW_SUBPROCESS] CREW_SUBPROCESS_MODE={os.environ.get('CREW_SUBPROCESS_MODE')} - Direct DB writes enabled")

                    # Create task completion listener
                    task_listener = TaskCompletionEventListener(
                        job_id=execution_id,
                        group_context=group_context
                    )
                    task_listener.setup_listeners(crewai_event_bus)
                    async_logger.info(f"[FLOW_SUBPROCESS] TaskCompletionEventListener registered for {execution_id}")

                except Exception as listener_error:
                    async_logger.error(f"[FLOW_SUBPROCESS] Failed to initialize event listeners: {listener_error}", exc_info=True)
                    raise

            # Run initialization in the init loop
            init_loop.run_until_complete(initialize_tracing())
            init_loop.close()

            async_logger.info(f"[FLOW_SUBPROCESS] Successfully initialized tracing and event listeners")

        except Exception as trace_init_error:
            async_logger.error(f"[FLOW_SUBPROCESS] Failed to initialize TraceManager: {trace_init_error}", exc_info=True)
            # Continue execution even if trace initialization fails

        # Run the flow execution asynchronously
        async def run_async_flow():
            """Execute the flow in an async context"""
            try:
                from src.db.session import async_session_factory
                from src.engines.crewai.flow.flow_runner_service import FlowRunnerService

                async with async_session_factory() as session:
                    # Create flow runner service with session
                    flow_runner = FlowRunnerService(session)

                    # Extract flow parameters from config
                    flow_id = flow_config.get('flow_id')
                    run_name = flow_config.get('run_name')

                    async_logger.info(f"[FLOW_SUBPROCESS] Starting flow execution")
                    async_logger.info(f"  flow_id: {flow_id}")
                    async_logger.info(f"  execution_id: {execution_id}")
                    async_logger.info(f"  run_name: {run_name}")

                    # Call run_flow() which now uses await (not create_task) to ensure subprocess
                    # waits for completion before capturing stdout in finally block
                    result = await flow_runner.run_flow(
                        flow_id=flow_id,
                        job_id=execution_id,
                        run_name=run_name,
                        config=flow_config
                    )

                    async_logger.info(f"[FLOW_SUBPROCESS] Flow execution completed successfully")
                    return result

            except Exception as e:
                async_logger.error(f"[FLOW_SUBPROCESS] Flow execution error: {e}", exc_info=True)
                raise

        # Create new event loop for subprocess
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            # Run the flow
            result = loop.run_until_complete(run_async_flow())
            async_logger.info(f"[FLOW_SUBPROCESS] Flow completed successfully")

            # Process result
            return {
                "status": "COMPLETED",
                "execution_id": execution_id,
                "result": str(result) if result else "Flow execution completed",
                "process_id": os.getpid()
            }
        finally:
            # Cleanup async resources before closing loop
            try:
                # Cleanup litellm's async HTTP clients
                try:
                    from litellm.llms.custom_httpx.async_client_cleanup import close_litellm_async_clients
                    loop.run_until_complete(close_litellm_async_clients())
                except ImportError:
                    # Older litellm version, try alternative cleanup
                    try:
                        from litellm.llms.custom_httpx.http_handler import AsyncHTTPHandler
                        if hasattr(AsyncHTTPHandler, 'close_clients'):
                            loop.run_until_complete(AsyncHTTPHandler.close_clients())
                    except Exception:
                        pass
                except Exception:
                    # Ignore cleanup errors
                    pass

                # Cancel any pending tasks
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()

                # Wait for cancelled tasks to finish
                if pending:
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception as cleanup_err:
                # Ignore cleanup errors, just log them
                try:
                    async_logger.debug(f"Async cleanup note: {cleanup_err}")
                except:
                    pass
            finally:
                # Suppress litellm's RuntimeWarning about unawaited coroutines during loop cleanup
                # This warning is harmless - we've already cleaned up what we can above
                # Apply filters globally since warnings come from litellm's internal event loop hooks
                import warnings
                warnings.filterwarnings("ignore", category=RuntimeWarning,
                                      message=".*coroutine.*was never awaited")
                warnings.filterwarnings("ignore", category=RuntimeWarning,
                                      message=".*Enable tracemalloc.*")
                warnings.filterwarnings("ignore", category=RuntimeWarning,
                                      module=".*async_client_cleanup.*")
                loop.close()

    except Exception as e:
        # Log error to flow.log with execution ID
        error_logger = logging.getLogger('flow')
        error_logger.error(f"Process {os.getpid()} error in flow execution for {execution_id}: {e}")
        error_logger.error(f"Traceback: {traceback.format_exc()}")

        return {
            "status": "FAILED",
            "execution_id": execution_id,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "process_id": os.getpid()
        }
    finally:
        # Capture any output that was written to stdout/stderr
        try:
            output = captured_output.getvalue()
            if output:
                # Log the captured output to flow.log with execution ID
                # The database handler will automatically write to execution_logs
                for line in output.split('\n'):
                    if line.strip():
                        async_logger.info(f"[STDOUT] {line.strip()}")
        except Exception as capture_error:
            # Log any errors in stdout capture (shouldn't happen normally)
            try:
                async_logger.error(f"Error capturing stdout: {capture_error}")
            except:
                pass

        # Restore stdout/stderr before process ends
        restore_stdout_stderr(original_stdout, original_stderr)

        # Clean up database connections
        try:
            from src.services.mlflow_tracing_service import cleanup_async_db_connections as _cleanup_db
            _cleanup_db()
        except Exception as db_cleanup_error:
            logger.debug(f"Database cleanup note: {db_cleanup_error}")

        # Clean up any remaining child processes (including crew processes)
        try:
            import psutil
            parent = psutil.Process(os.getpid())
            children = parent.children(recursive=True)

            if children:
                logger.info(f"Cleaning up {len(children)} remaining child processes (including crews)")

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
            logger.warning("psutil not available for cleanup")
        except Exception as cleanup_error:
            logger.error(f"Error during final cleanup: {cleanup_error}")


class ProcessFlowExecutor:
    """High-performance process-based executor for isolated CrewAI flow execution.

    This executor manages CrewAI flow executions in separate OS processes,
    providing complete isolation, resource management, and the ability to
    forcefully terminate stuck or runaway executions.

    Unlike thread-based execution, process isolation guarantees:
    - Complete memory separation between executions
    - Ability to forcefully terminate without affecting other executions
    - No resource leaks or zombie processes
    - Protection of the main application from flow failures
    - Proper cleanup of all child processes (including crew processes)

    Features:
        - One process per execution (no shared pool)
        - Concurrent execution management with configurable limits
        - Graceful and forceful termination support
        - Child process tracking and cleanup (including crew subprocesses)
        - Async/await interface for non-blocking operations
        - Comprehensive error handling and recovery

    Attributes:
        _ctx: Multiprocessing context using 'spawn' for better isolation
        _running_processes: Dictionary tracking active processes by execution ID
        _running_futures: Dictionary of asyncio futures for execution results
        _running_executors: Dictionary of process pool executors
        _max_concurrent: Maximum number of concurrent flow executions
        _metrics: Dictionary tracking execution statistics

    Example:
        >>> executor = ProcessFlowExecutor(max_concurrent=2)
        >>> result = await executor.run_flow_isolated(
        ...     execution_id="exec_123",
        ...     flow_config=config
        ... )
        >>> # To stop an execution:
        >>> stopped = await executor.terminate_execution("exec_123")
    """

    def __init__(self, max_concurrent: int = 2):
        """Initialize the process executor with concurrency control.

        Sets up the multiprocessing context, tracking structures, and
        concurrency management primitives.

        Args:
            max_concurrent: Maximum number of concurrent flow executions.
                Defaults to 2 to balance resource usage (flows can spawn multiple crews).

        Note:
            Uses 'spawn' context instead of 'fork' for better isolation
            and to avoid shared memory issues between processes.
        """
        # Use spawn method for better isolation
        self._ctx = mp.get_context('spawn')

        # Configure subprocess to suppress output
        os.environ['PYTHONUNBUFFERED'] = '0'
        os.environ['CREWAI_VERBOSE'] = 'false'

        # NO POOL - we create individual processes per execution
        self._max_concurrent = max_concurrent

        # Track running processes and their executors
        self._running_processes: Dict[str, mp.Process] = {}
        self._running_futures: Dict[str, Any] = {}
        self._running_executors: Dict[str, ProcessPoolExecutor] = {}

        # Metrics
        self._metrics = {
            'total_executions': 0,
            'active_executions': 0,
            'completed_executions': 0,
            'failed_executions': 0,
            'terminated_executions': 0
        }

        logger.info(f"ProcessFlowExecutor initialized for per-execution processes (max concurrent: {max_concurrent})")

    @staticmethod
    def _run_flow_wrapper(execution_id: str, flow_config: Dict[str, Any],
                         inputs: Optional[Dict[str, Any]], group_context: Any,
                         result_queue: mp.Queue, log_queue: mp.Queue):
        """
        Wrapper to run flow in subprocess and put result in queue.

        This method runs in the subprocess and handles the flow execution.
        """
        try:
            # Run the flow execution
            result = run_flow_in_process(execution_id, flow_config, inputs, group_context, log_queue)
            # Put result in the queue
            result_queue.put(result)
        except Exception as e:
            # Put error result in the queue
            result_queue.put({
                "status": "FAILED",
                "execution_id": execution_id,
                "error": str(e)
            })

    async def run_flow_isolated(
        self,
        execution_id: str,
        flow_config: Dict[str, Any],
        group_context: Any,  # MANDATORY - for tenant isolation
        inputs: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Run a flow in an isolated process using direct Process control.

        Args:
            execution_id: Unique identifier for the execution
            flow_config: Configuration to build the flow
            group_context: Group context for tenant isolation (MANDATORY)
            inputs: Optional inputs for the flow
            timeout: Optional timeout in seconds

        Returns:
            Dictionary with execution results
        """
        logger.info(f"[ProcessFlowExecutor] run_flow_isolated called for {execution_id}")

        self._metrics['total_executions'] += 1
        self._metrics['active_executions'] += 1

        start_time = datetime.now()

        # Use multiprocessing.Queue to get results from the subprocess
        result_queue = self._ctx.Queue()
        log_queue = self._ctx.Queue()

        # Pass group_context data to flow_config for subprocess execution
        if group_context:
            if isinstance(flow_config, dict):
                if hasattr(group_context, 'primary_group_id') and group_context.primary_group_id:
                    flow_config['group_id'] = group_context.primary_group_id
                    logger.info(f"[ProcessFlowExecutor] Added group_id to flow_config: {group_context.primary_group_id}")
                else:
                    logger.error("[ProcessFlowExecutor] SECURITY: No group_id in group_context - multi-tenant isolation may fail")

                # Add user token if available
                if hasattr(group_context, 'access_token') and group_context.access_token:
                    flow_config['user_token'] = group_context.access_token
                    logger.info("[ProcessFlowExecutor] Added user_token to flow_config for OBO authentication")

        # Create and start the subprocess
        process = self._ctx.Process(
            target=self._run_flow_wrapper,
            args=(execution_id, flow_config, inputs, group_context, result_queue, log_queue),
            daemon=False  # Don't make daemon so it can spawn its own child processes (crews)
        )

        # Store the process
        self._running_processes[execution_id] = process

        # Start the process
        process.start()
        logger.info(f"[ProcessFlowExecutor] Started flow process {process.pid} for execution {execution_id}")

        # Wait for result in background
        loop = asyncio.get_event_loop()
        future = loop.run_in_executor(None, self._wait_for_result, execution_id, process, result_queue, timeout)
        self._running_futures[execution_id] = future

        try:
            result = await future

            # Process logs from flow.log file and write to database
            # This reads the flow.log file after execution to capture ALL logs
            await self._process_log_queue(log_queue, execution_id, group_context)

            # Update metrics
            if result.get('status') == 'COMPLETED':
                self._metrics['completed_executions'] += 1
            else:
                self._metrics['failed_executions'] += 1

            return result

        except asyncio.TimeoutError:
            logger.error(f"Flow execution {execution_id} timed out after {timeout}s")
            await self.terminate_execution(execution_id)
            self._metrics['failed_executions'] += 1
            return {
                "status": "FAILED",
                "execution_id": execution_id,
                "error": f"Execution timed out after {timeout} seconds"
            }
        except Exception as e:
            logger.error(f"Error waiting for flow execution {execution_id}: {e}")
            self._metrics['failed_executions'] += 1
            return {
                "status": "FAILED",
                "execution_id": execution_id,
                "error": str(e)
            }
        finally:
            self._metrics['active_executions'] -= 1
            # Clean up tracking
            self._running_processes.pop(execution_id, None)
            self._running_futures.pop(execution_id, None)

    def _wait_for_result(self, execution_id: str, process: mp.Process,
                         result_queue: mp.Queue, timeout: Optional[float]) -> Dict[str, Any]:
        """
        Wait for process to complete and return result.

        This runs in a thread pool executor.
        """
        try:
            # Wait for process to finish
            process.join(timeout=timeout)

            if process.is_alive():
                # Timeout occurred
                logger.warning(f"Flow process {process.pid} for {execution_id} still running after timeout")
                process.terminate()
                process.join(timeout=5)
                if process.is_alive():
                    process.kill()
                    process.join()
                raise TimeoutError(f"Flow execution timed out after {timeout} seconds")

            # Get result from queue
            if not result_queue.empty():
                result = result_queue.get(timeout=1)
                return result
            else:
                # No result in queue - process ended without producing result
                return {
                    "status": "FAILED",
                    "execution_id": execution_id,
                    "error": "Process ended without producing result",
                    "exit_code": process.exitcode
                }

        except Exception as e:
            logger.error(f"Error waiting for flow result: {e}")
            return {
                "status": "FAILED",
                "execution_id": execution_id,
                "error": str(e)
            }

    async def terminate_execution(self, execution_id: str, graceful: bool = False) -> bool:
        """
        Terminate a running flow execution.

        Args:
            execution_id: ID of the execution to terminate
            graceful: If True, send SIGTERM; if False, send SIGKILL

        Returns:
            True if termination successful, False otherwise
        """
        logger.info(f"[ProcessFlowExecutor] Terminating execution {execution_id} (graceful={graceful})")

        process = self._running_processes.get(execution_id)
        if not process:
            logger.warning(f"No process found for execution {execution_id}")
            return False

        try:
            if process.is_alive():
                if graceful:
                    # Send SIGTERM for graceful shutdown
                    process.terminate()
                    logger.info(f"Sent SIGTERM to flow process {process.pid}")
                    # Wait a bit for graceful shutdown
                    process.join(timeout=5)

                # Force kill if still alive
                if process.is_alive():
                    process.kill()
                    logger.info(f"Sent SIGKILL to flow process {process.pid}")
                    process.join(timeout=2)

                self._metrics['terminated_executions'] += 1
                return True
            else:
                logger.info(f"Process {process.pid} already terminated")
                return True

        except Exception as e:
            logger.error(f"Error terminating flow process: {e}")
            return False
        finally:
            # Clean up tracking
            self._running_processes.pop(execution_id, None)
            self._running_futures.pop(execution_id, None)

    async def _process_log_queue(self, log_queue, execution_id: str, group_context=None):
        """
        Process logs from flow.log file and write them to the execution_logs table.

        This method reads the flow.log file, extracts logs for the specific execution,
        and writes them to the database for persistent storage and later retrieval.

        Args:
            log_queue: Not used anymore, kept for compatibility
            execution_id: The execution ID for logging
            group_context: Optional group context for multi-tenant isolation
        """
        logger.info(f"[ProcessFlowExecutor] Processing logs from flow.log for {execution_id}")

        try:
            import os
            import json
            from datetime import datetime, timezone
            from sqlalchemy.ext.asyncio import create_async_engine
            from sqlalchemy import text
            from src.config.settings import settings

            # Get the flow.log path
            log_dir = os.environ.get('LOG_DIR')
            if not log_dir:
                import pathlib
                backend_root = pathlib.Path(__file__).parent.parent.parent
                log_dir = backend_root / 'logs'

            flow_log_path = os.path.join(log_dir, 'flow.log')

            if not os.path.exists(flow_log_path):
                logger.warning(f"flow.log file not found at {flow_log_path}")
                return

            # Extract logs for our execution ID
            logs_to_write = []
            exec_id_short = execution_id[:8]  # Use short ID for matching

            # First, add a header log entry to mark the start
            logs_to_write.append({
                'execution_id': execution_id,
                'content': f'[EXECUTION_START] ========== Execution {execution_id} Started ==========',
                'timestamp': datetime.utcnow(),  # Use timezone-naive UTC datetime for database consistency
                'group_id': getattr(group_context, 'primary_group_id', None) if group_context else None,
                'group_email': getattr(group_context, 'group_email', None) if group_context else None
            })

            # Read flow.log and extract relevant logs
            with open(flow_log_path, 'r') as f:
                for line in f:
                    if exec_id_short in line:
                        # This log belongs to our execution
                        logs_to_write.append({
                            'execution_id': execution_id,
                            'content': line.strip(),
                            'timestamp': datetime.utcnow(),  # Use timezone-naive UTC datetime for database consistency
                            'group_id': getattr(group_context, 'primary_group_id', None) if group_context else None,
                            'group_email': getattr(group_context, 'group_email', None) if group_context else None
                        })

            if len(logs_to_write) <= 1:  # Only has header
                logger.info(f"No logs found for execution {exec_id_short} in flow.log")
                # Still write the header log
            else:
                logger.info(f"Found {len(logs_to_write)-1} logs for execution {exec_id_short} in flow.log")

            # Build database URL
            if settings.DATABASE_TYPE == 'sqlite':
                db_url = f'sqlite+aiosqlite:///{settings.SQLITE_DB_PATH}'
                logger.info(f"[ProcessFlowExecutor] Using SQLite database: {settings.SQLITE_DB_PATH}")
            else:
                postgres_user = settings.POSTGRES_USER or 'postgres'
                postgres_password = settings.POSTGRES_PASSWORD or 'postgres'
                postgres_server = settings.POSTGRES_SERVER or 'localhost'
                postgres_port = settings.POSTGRES_PORT or '5432'
                postgres_db = settings.POSTGRES_DB or 'kasal'
                db_url = f'postgresql+asyncpg://{postgres_user}:{postgres_password}@{postgres_server}:{postgres_port}/{postgres_db}'
                logger.info(f"[ProcessFlowExecutor] Using PostgreSQL database: {postgres_server}:{postgres_port}/{postgres_db}")

            # Write logs to database
            engine = create_async_engine(db_url)
            async with engine.begin() as conn:
                for log_data in logs_to_write:
                    await conn.execute(text("""
                        INSERT INTO execution_logs (execution_id, content, timestamp, group_id, group_email)
                        VALUES (:execution_id, :content, :timestamp, :group_id, :group_email)
                    """), log_data)

            await engine.dispose()
            logger.info(f"[ProcessFlowExecutor] Successfully wrote {len(logs_to_write)} logs to execution_logs table")

        except Exception as e:
            import traceback
            logger.error(f"[ProcessFlowExecutor] Error processing logs from flow.log: {e}")
            logger.error(f"[ProcessFlowExecutor] Full traceback:\n{traceback.format_exc()}")

    def get_execution_info(self, execution_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a running execution."""
        process = self._running_processes.get(execution_id)
        if not process:
            return None

        return {
            "execution_id": execution_id,
            "pid": process.pid,
            "is_alive": process.is_alive(),
            "exitcode": process.exitcode
        }

    def get_metrics(self) -> Dict[str, int]:
        """Get executor metrics."""
        return self._metrics.copy()


# Global executor instance
process_flow_executor = ProcessFlowExecutor(max_concurrent=2)
