"""CrewAI Engine Service for AI agent orchestration.

This module provides the core engine service for CrewAI-based agent execution,
handling both individual crew executions and complex flow orchestrations.

The service integrates with the CrewAI framework to manage multi-agent systems,
coordinate task execution, and provide comprehensive tracing and monitoring
capabilities for AI workflows.

Key Features:
    - Crew preparation and configuration management
    - Flow orchestration for complex multi-crew workflows
    - Process-based execution isolation for reliability
    - Real-time trace capture and event monitoring
    - Tool factory integration for dynamic tool loading
    - Multi-tenant support with group context isolation

Architecture:
    The service extends BaseEngineService and acts as the primary interface
    between the application layer and the CrewAI framework. It manages the
    lifecycle of crew executions, from configuration through completion.

Example:
    >>> service = CrewAIEngineService()
    >>> await service.initialize(llm_provider="openai", model="gpt-4")
    >>> result = await service.run_execution(
    ...     execution_id="exec_123",
    ...     execution_config=crew_config,
    ...     group_context=group_ctx
    ... )
"""

import logging
import asyncio
import os
from datetime import datetime, UTC
from typing import Dict, List, Any, Optional

from src.engines.base.base_engine_service import BaseEngineService
from src.models.execution_status import ExecutionStatus

# Import helper modules
from src.engines.crewai.trace_management import TraceManager
from src.engines.crewai.execution_runner import run_crew, run_crew_in_process, update_execution_status_with_retry
from src.engines.crewai.flow.flow_execution_runner import run_flow_in_process
from src.engines.crewai.config_adapter import normalize_config, normalize_flow_config
from src.engines.crewai.crew_preparation import CrewPreparation
from src.engines.crewai.flow_preparation import FlowPreparation
from src.services.tool_service import ToolService
from src.engines.crewai.tools.tool_factory import ToolFactory

# Import the logging callbacks
from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener, TaskCompletionEventListener

# Import CrewAI components
from crewai import Crew
from crewai.flow import Flow

# Import logger manager
from src.core.logger import LoggerManager

from src.schemas.execution import CrewConfig, FlowConfig
from src.utils.user_context import GroupContext

logger = LoggerManager.get_instance().crew

class CrewAIEngineService(BaseEngineService):
    """Core engine service for CrewAI agent orchestration and execution.
    
    This service provides comprehensive management of CrewAI-based agent systems,
    handling everything from crew configuration to execution monitoring. It supports
    both simple crew executions and complex flow orchestrations with multiple crews.
    
    The service integrates with various subsystems including:
    - Trace management for execution monitoring
    - Tool factory for dynamic tool provisioning
    - Process isolation for reliable execution
    - Event callbacks for real-time updates
    
    Attributes:
        _running_jobs: Dictionary mapping execution IDs to job information
        _get_execution_repository: Factory function for execution repository access
        _status_service: Reference to ExecutionStatusService for status updates
    
    Inheritance:
        Extends BaseEngineService to provide CrewAI-specific implementation
    
    Note:
        The service uses process-based execution for isolation and reliability,
        ensuring that crew failures don't affect the main application process.
    """
    
    def __init__(self, db=None):
        """Initialize the CrewAI engine service with database connection.
        
        Sets up the service with repository access patterns and initializes
        tracking structures for running jobs.
        
        Args:
            db: Optional database connection for repository access.
                If not provided, repositories will use their default connections.
        
        Note:
            The service doesn't store the db directly but uses repository
            factory functions to maintain proper separation of concerns.
        """
        # Don't store db directly - repositories should handle db access
        self._running_jobs = {}  # Map of execution_id -> job info
        
        # Import repository factory functions
        from src.repositories.execution_repository import get_execution_repository
        from src.services.execution_status_service import ExecutionStatusService
        
        self._get_execution_repository = lambda session: get_execution_repository(session)
        self._status_service = ExecutionStatusService  # Store reference to service
        

    async def initialize(self, **kwargs) -> bool:
        """Initialize the CrewAI engine service and its dependencies.
        
        Performs startup initialization including trace writer setup,
        logger configuration, and LLM provider initialization.
        
        Args:
            **kwargs: Initialization parameters including:
                - llm_provider: LLM provider name (default: "openai")
                - model: Model identifier (default: "gpt-4o")
                - Additional provider-specific configuration
            
        Returns:
            bool: True if initialization successful, False otherwise
        
        Note:
            This method ensures the trace writer is started for execution
            monitoring and configures the CrewAI library logging.
        
        Example:
            >>> success = await service.initialize(
            ...     llm_provider="anthropic",
            ...     model="claude-3-opus"
            ... )
        """
        # Ensure trace writer is started when engine initializes
        await TraceManager.ensure_writer_started()
        try:
            # Set up CrewAI library logging via our centralized logger
            from src.engines.crewai.crew_logger import crew_logger

            # Choose logger based on execution type if provided
            execution_type = kwargs.get("execution_type", "crew")
            if execution_type and execution_type.lower() == "flow":
                init_logger = LoggerManager.get_instance().flow
            else:
                init_logger = logger

            # Additional initialization if needed
            llm_provider = kwargs.get("llm_provider", "openai")
            model = kwargs.get("model", "gpt-4o")
            init_logger.info(f"Initializing CrewAI engine with {llm_provider} using model {model}")

            return True

        except Exception as e:
            # Use appropriate logger for error too
            execution_type = kwargs.get("execution_type", "crew")
            if execution_type and execution_type.lower() == "flow":
                error_logger = LoggerManager.get_instance().flow
            else:
                error_logger = logger
            error_logger.error(f"Failed to initialize CrewAI engine: {str(e)}")
            return False
    
    async def run_execution(self, execution_id: str, execution_config: Dict[str, Any], group_context: GroupContext = None, session = None) -> str:
        """Execute a CrewAI crew with process isolation and comprehensive monitoring.
        
        Orchestrates the complete execution lifecycle including crew preparation,
        process-based execution, trace capture, and status updates. Supports
        multi-tenant isolation through group context.
        
        Args:
            execution_id: Unique identifier for tracking this execution.
                Used for trace correlation and status updates.
            execution_config: Complete crew configuration including:
                - crew: Crew-level settings (name, process, memory, etc.)
                - agents: List of agent configurations
                - tasks: List of task configurations
                - tools: Tool configurations and API keys
                - output_settings: Output format and destination
            group_context: Optional multi-tenant context containing:
                - primary_group_id: Group identifier for isolation
                - access_token: User token for authenticated operations
                - group_email: Group email for notifications
            
        Returns:
            str: The execution ID for tracking the execution
        
        Raises:
            Exception: Propagates exceptions from crew preparation or execution
        
        Note:
            The method assumes the execution record is already created with
            RUNNING status by the caller. It focuses on actual execution
            and status updates.
        
        Example:
            >>> exec_id = await service.run_execution(
            ...     execution_id="exec_123",
            ...     execution_config={
            ...         "crew": {"name": "Research Crew"},
            ...         "agents": [...],
            ...         "tasks": [...]
            ...     },
            ...     group_context=user_group_context
            ... )
        """
        try:
            # Normalize config to ensure consistent format
            execution_config = normalize_config(execution_config)
            
            # Add group_id to config if we have group_context
            if group_context and group_context.primary_group_id:
                execution_config["group_id"] = group_context.primary_group_id
                logger.info(f"[CrewAIEngineService] Added group_id to config: {group_context.primary_group_id}")
            
            # Extract crew definition sections from config
            crew_config = execution_config.get("crew", {})
            agent_configs = execution_config.get("agents", [])
            task_configs = execution_config.get("tasks", [])
            
            # Log agent configurations to debug knowledge_sources
            logger.info(f"[CrewAIEngineService] Processing {len(agent_configs)} agents for execution {execution_id}")
            for idx, agent_config in enumerate(agent_configs):
                agent_id = agent_config.get('id', f'agent_{idx}')
                logger.info(f"[CrewAIEngineService] Agent {agent_id} config keys: {list(agent_config.keys())}")
                if 'knowledge_sources' in agent_config:
                    ks = agent_config['knowledge_sources']
                    logger.info(f"[CrewAIEngineService] Agent {agent_id} has {len(ks)} knowledge_sources: {ks}")
                else:
                    logger.info(f"[CrewAIEngineService] Agent {agent_id} has NO knowledge_sources")
            
            # Setup output directory
            output_dir = self._setup_output_directory(execution_id)
            execution_config['output_dir'] = output_dir
            
            # We assume the execution record is already created by the caller
            # We will only update the status
            
            # Ensure writer is started before running execution
            await TraceManager.ensure_writer_started()
            
            logger.info(f"[CrewAIEngineService] Starting run_execution for ID: {execution_id} (already has RUNNING status)")
            
            try:
                # Create services using the passed session
                from src.services.tool_service import ToolService
                from src.services.api_keys_service import ApiKeysService

                # Extract group_id for API keys service
                group_id = group_context.primary_group_id if group_context else None

                # Use the passed session for services
                if session:
                    # Create services directly with session
                    tool_service = ToolService(session)
                    api_keys_service = ApiKeysService(session, group_id=group_id)
                else:
                    # Fallback: create a new session if none provided
                    from src.db.session import async_session_factory
                    async with async_session_factory() as db_session:
                        tool_service = ToolService(db_session)
                        api_keys_service = ApiKeysService(db_session, group_id=group_id)

                # Extract user token from group context for tool factory
                user_token = group_context.access_token if group_context else None

                # Create a tool factory instance with API keys service and user token
                tool_factory = await ToolFactory.create(execution_config, api_keys_service, user_token)
                logger.info(f"[CrewAIEngineService] Created ToolFactory for {execution_id} with user token: {bool(user_token)}")

                # IMPORTANT: Do NOT prepare crew in main process when using subprocess execution
                # The subprocess will prepare its own crew with the full config including knowledge_sources
                # Preparing here would modify the config and remove knowledge_sources before subprocess gets them

                # Debug log to check if knowledge_sources are still present
                logger.info(f"[CrewAIEngineService] DEBUG: Config before subprocess for {execution_id}:")
                for idx, agent_config in enumerate(execution_config.get("agents", [])):
                    agent_id = agent_config.get('id', f'agent_{idx}')
                    ks = agent_config.get('knowledge_sources', [])
                    logger.info(f"[CrewAIEngineService] Agent {agent_id} has {len(ks)} knowledge_sources: {ks}")

                # Skip crew preparation in main process - let subprocess handle it
                # This preserves the original config with knowledge_sources intact
                crew = None  # No crew object needed in main process for subprocess execution
            
            except Exception as e:
                logger.error(f"[CrewAIEngineService] Error running CrewAI execution {execution_id}: {str(e)}", exc_info=True)
                try:
                    await self._update_execution_status(
                        execution_id, 
                        ExecutionStatus.FAILED.value,
                        f"Failed during crew preparation/launch: {str(e)}"
                    )
                except Exception as update_err:
                    logger.critical(f"[CrewAIEngineService] CRITICAL: Failed to update status to FAILED for {execution_id} after run_execution error: {update_err}", exc_info=True)
                raise
            
            # Event listeners are now initialized in the subprocess
            # This ensures they're in the same process as the crew execution
            logger.debug(f"[CrewAIEngineService] Event listeners will be initialized in subprocess for {execution_id}")
            
            # Status is already RUNNING from creation, no need to update
            logger.info(f"[CrewAIEngineService] Execution {execution_id} ready to start (status already RUNNING)")
            
            # User token was already extracted and passed to tool factory above
            user_token = group_context.access_token if group_context else None
            
            # Use process-based execution for true termination capability
            logger.info(f"[CrewAIEngineService] Starting process-based execution for {execution_id}")
            
            # Create a task for process-based crew execution with exception handler
            async def run_with_exception_handler():
                try:
                    logger.info(f"[CrewAIEngineService] About to call run_crew_in_process for {execution_id}")
                    await run_crew_in_process(
                        execution_id=execution_id,
                        config=execution_config,
                        running_jobs=self._running_jobs,
                        group_context=group_context,
                        user_token=user_token
                    )
                    logger.info(f"[CrewAIEngineService] run_crew_in_process completed for {execution_id}")
                except Exception as e:
                    logger.error(f"[CrewAIEngineService] CRITICAL: Exception in run_crew_in_process for {execution_id}: {e}", exc_info=True)
                    # Write to file as backup
                    import traceback
                    with open(f'/tmp/task_error_{execution_id[:8]}.log', 'w') as f:
                        f.write(f"Exception in background task: {e}\n")
                        f.write(traceback.format_exc())
            
            execution_task = asyncio.create_task(run_with_exception_handler())
            
            logger.info(f"[CrewAIEngineService] Created execution task for {execution_id}")
            
            # Store job info (no crew object since it runs in a separate process)
            self._running_jobs[execution_id] = {
                "task": execution_task,
                "crew": None,  # Crew runs in separate process
                "start_time": datetime.now(),
                "config": execution_config,
                "execution_mode": "process"  # Mark this as process-based
            }
            
            return execution_id
            
        except Exception as e:
            logger.error(f"Error running execution {execution_id}: {str(e)}", exc_info=True)
            raise
    
    def _setup_output_directory(self, execution_id: Optional[str] = None, execution_logger=None) -> str:
        """
        Set up output directory for workflow execution

        Args:
            execution_id: Optional execution ID for the workflow
            execution_logger: Optional logger to use (defaults to module logger)

        Returns:
            str: Path to output directory
        """
        # Use provided logger or fall back to module-level logger
        log = execution_logger or logger

        try:
            # Create base output directory
            from pathlib import Path
            base_dir = Path(os.getcwd()) / "tmp" / "crew_outputs"
            base_dir.mkdir(parents=True, exist_ok=True)

            # Create execution-specific directory if ID provided
            if execution_id:
                output_dir = base_dir / execution_id
                output_dir.mkdir(exist_ok=True)
                log.info(f"Created output directory: {output_dir}")
                return str(output_dir)

            return str(base_dir)

        except Exception as e:
            log.error(f"Error setting up output directory: {str(e)}")
            return os.path.join(os.getcwd(), "tmp", "crew_outputs")
    
    async def _update_execution_status(self, 
                                 execution_id: str, 
                                 status: str, 
                                 message: str,
                                 result: Any = None) -> None:
        """
        Update execution status via service layer.
        
        Args:
            execution_id: Execution ID
            status: New status
            message: Status message
            result: Optional execution result
        """
        # Delegate to the update_execution_status_with_retry function
        await update_execution_status_with_retry(
            execution_id=execution_id,
            status=status,
            message=message,
            result=result
        )

    async def get_execution_status(self, execution_id: str) -> Dict[str, Any]:
        """
        Get the status of an execution
        
        Args:
            execution_id: Execution ID
            
        Returns:
            Dict with execution status information
        """
        # Check in-memory jobs first
        if execution_id in self._running_jobs:
            job_info = self._running_jobs[execution_id]
            return {
                "status": ExecutionStatus.RUNNING.value,
                "start_time": job_info["start_time"].isoformat(),
                "message": "Execution is currently running"
            }
        
        # Get status from database via service
        try:
            # Use execution status service - service should handle DB access through repositories
            from src.services.execution_status_service import ExecutionStatusService
            
            # Service should handle DB sessions internally
            status = await ExecutionStatusService.get_status(execution_id)
            
            if status:
                return {
                    "status": status.status,
                    "message": status.message,
                    "result": status.result,
                    "updated_at": status.updated_at.isoformat() if status.updated_at else None,
                    "created_at": status.created_at.isoformat() if status.created_at else None,
                }
            else:
                return {
                    "status": "UNKNOWN",
                    "message": "Execution status not found"
                }
        except Exception as e:
            logger.error(f"Error getting execution status: {str(e)}")
            return {
                "status": "ERROR",
                "message": f"Error retrieving execution status: {str(e)}"
            }
    
    async def cancel_execution(self, execution_id: str) -> bool:
        """
        Cancel a running execution
        
        Args:
            execution_id: Execution ID
            
        Returns:
            bool: True if cancelled successfully
        """
        if execution_id not in self._running_jobs:
            logger.warning(f"Cannot cancel execution {execution_id}: not found in running jobs")
            return False
            
        try:
            # Get the job info
            job_info = self._running_jobs[execution_id]
            execution_mode = job_info.get("execution_mode", "thread")
            
            # If process-based execution, terminate the process
            if execution_mode == "process":
                from src.services.process_crew_executor import process_crew_executor
                terminated = await process_crew_executor.terminate_execution(execution_id)
                if terminated:
                    logger.info(f"Successfully terminated process for execution {execution_id}")
                    
                    # Cancel the asyncio task as well
                    task = job_info["task"]
                    if task and not task.done():
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
                    
                    # Update status in database
                    await self._update_execution_status(
                        execution_id,
                        ExecutionStatus.STOPPED.value,
                        "Execution stopped by user (process terminated)"
                    )
                    
                    # Clean up
                    del self._running_jobs[execution_id]
                    return True
                else:
                    logger.warning(f"Could not terminate process for execution {execution_id}")
            
            # For thread-based execution (fallback or if process termination fails)
            task = job_info["task"]
            
            # Cancel the task
            task.cancel()
            
            # Wait for task to be cancelled
            try:
                await task
            except asyncio.CancelledError:
                pass
            
            # Update status in database - use STOPPED instead of CANCELLED for user-initiated stops
            await self._update_execution_status(
                execution_id, 
                ExecutionStatus.STOPPED.value,
                "Execution stopped by user"
            )
            
            # Clean up
            del self._running_jobs[execution_id]
            
            return True
        except Exception as e:
            logger.error(f"Error cancelling execution {execution_id}: {str(e)}")
            return False 

    async def run_flow(self, execution_id: str, flow_config: Dict[str, Any], group_context: GroupContext = None, user_token: str = None) -> str:
        """
        Run a CrewAI flow with the given configuration using process isolation.

        Args:
            execution_id: Unique ID for this flow execution
            flow_config: Configuration for the flow
            group_context: Group context for multi-tenant isolation
            user_token: User access token for OAuth authentication

        Returns:
            Execution ID
        """
        # Use flow-specific logger for flow execution
        from src.core.logger import LoggerManager
        flow_logger = LoggerManager.get_instance().flow

        try:
            # Normalize flow config
            flow_config = normalize_flow_config(flow_config)

            # Add group_id to config if we have group_context
            if group_context and group_context.primary_group_id:
                flow_config["group_id"] = group_context.primary_group_id
                flow_logger.info(f"[CrewAIEngineService] Added group_id to flow config: {group_context.primary_group_id}")

            # Setup output directory
            output_dir = self._setup_output_directory(execution_id, execution_logger=flow_logger)
            flow_config['output_dir'] = output_dir

            # Ensure writer is started before running execution
            await TraceManager.ensure_writer_started()

            flow_logger.info(f"[CrewAIEngineService] Starting run_flow for ID: {execution_id} (process-based)")
            flow_logger.info(f"[CrewAIEngineService] Flow config has {len(flow_config.get('nodes', []))} nodes and {len(flow_config.get('edges', []))} edges")

            # Status is already RUNNING from creation, no need to update
            flow_logger.info(f"[CrewAIEngineService] Execution {execution_id} ready to start flow (status already RUNNING)")

            # Use process-based execution for true termination capability
            flow_logger.info(f"[CrewAIEngineService] Starting process-based flow execution for {execution_id}")

            # Create a task for process-based flow execution with exception handler
            async def run_with_exception_handler():
                try:
                    flow_logger.info(f"[CrewAIEngineService] About to call run_flow_in_process for {execution_id}")
                    await run_flow_in_process(
                        execution_id=execution_id,
                        config=flow_config,
                        running_jobs=self._running_jobs,
                        group_context=group_context,
                        user_token=user_token
                    )
                    flow_logger.info(f"[CrewAIEngineService] run_flow_in_process completed for {execution_id}")
                except Exception as e:
                    flow_logger.error(f"[CrewAIEngineService] CRITICAL: Exception in run_flow_in_process for {execution_id}: {e}", exc_info=True)
                    # Write to file as backup
                    import traceback
                    with open(f'/tmp/flow_task_error_{execution_id[:8]}.log', 'w') as f:
                        f.write(f"Exception in flow background task: {e}\n")
                        f.write(traceback.format_exc())

            execution_task = asyncio.create_task(run_with_exception_handler())

            flow_logger.info(f"[CrewAIEngineService] Created flow execution task for {execution_id}")

            # Store job info (no flow object since it runs in a separate process)
            self._running_jobs[execution_id] = {
                "task": execution_task,
                "flow": None,  # Flow runs in separate process
                "start_time": datetime.now(),
                "config": flow_config,
                "execution_mode": "process"  # Mark this as process-based
            }

            flow_logger.info(f"[CrewAIEngineService] Stored job info for {execution_id} in running_jobs")

            return execution_id

        except Exception as e:
            flow_logger.error(f"[CrewAIEngineService] Error in run_flow for {execution_id}: {str(e)}", exc_info=True)
            await self._update_execution_status(
                execution_id,
                ExecutionStatus.FAILED.value,
                f"Flow execution failed: {str(e)}"
            )
            raise

    async def _execute_flow(self, execution_id: str, flow: Flow) -> None:
        """
        Execute a flow and handle its completion.
        
        Args:
            execution_id: Execution ID
            flow: The flow to execute
        """
        try:
            # Execute the flow
            result = await flow.kickoff()
            
            # Update status to COMPLETED
            await self._update_execution_status(
                execution_id,
                ExecutionStatus.COMPLETED.value,
                "Flow execution completed successfully"
            )
            
            # Store the result
            if execution_id in self._running_jobs:
                self._running_jobs[execution_id]["result"] = result
                
        except Exception as e:
            logger.error(f"[CrewAIEngineService] Error executing flow {execution_id}: {str(e)}", exc_info=True)
            await self._update_execution_status(
                execution_id,
                ExecutionStatus.FAILED.value,
                f"Flow execution failed: {str(e)}"
            )
            
        finally:
            # Clean up the running job entry
            if execution_id in self._running_jobs:
                self._running_jobs[execution_id]["end_time"] = datetime.now(UTC) 