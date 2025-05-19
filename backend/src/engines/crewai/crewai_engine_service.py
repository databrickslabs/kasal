"""
CrewAI Engine Service module.

This module provides the CrewAI engine service implementation.
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
from src.engines.crewai.execution_runner import run_crew, update_execution_status_with_retry
from src.engines.crewai.config_adapter import normalize_config, normalize_flow_config
from src.engines.crewai.crew_preparation import CrewPreparation
from src.engines.crewai.flow_preparation import FlowPreparation
from src.services.tool_service import ToolService
from src.engines.crewai.tools.tool_factory import ToolFactory

# Import the logging callbacks
from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener, TaskCompletionLogger, DetailedOutputLogger

# Import CrewAI components
from crewai import Crew
from crewai.flow import Flow

# Import logger manager
from src.core.logger import LoggerManager

from src.schemas.execution import CrewConfig, FlowConfig

logger = LoggerManager.get_instance().crew

class CrewAIEngineService(BaseEngineService):
    """
    CrewAI Engine Service implementation
    
    This service handles CrewAI agent/crew configuration, preparation,
    and execution management, as well as flow execution.
    """
    
    def __init__(self, db=None):
        """
        Initialize the CrewAI engine service
        
        Args:
            db: The database connection to pass to repositories
        """
        # Don't store db directly - repositories should handle db access
        self._running_jobs = {}  # Map of execution_id -> job info
        
        # Import repository factory functions
        from src.repositories.execution_repository import get_execution_repository
        from src.services.execution_status_service import ExecutionStatusService
        
        self._get_execution_repository = lambda session: get_execution_repository(session)
        self._status_service = ExecutionStatusService  # Store reference to service
        

    async def initialize(self, **kwargs) -> bool:
        """
        Initialize the engine service
        
        Args:
            **kwargs: Additional parameters
            
        Returns:
            bool: True if initialization successful
        """
        # Ensure trace writer is started when engine initializes
        await TraceManager.ensure_writer_started()
        try:
            # Set up CrewAI library logging via our centralized logger
            from src.engines.crewai.crew_logger import crew_logger
            
            # Additional initialization if needed
            llm_provider = kwargs.get("llm_provider", "openai")
            model = kwargs.get("model", "gpt-4o")
            logger.info(f"Initializing CrewAI engine with {llm_provider} using model {model}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize CrewAI engine: {str(e)}")
            return False
    
    async def run_execution(self, execution_id: str, execution_config: Dict[str, Any]) -> str:
        """
        Run a CrewAI execution with the given configuration.
        
        Args:
            execution_id: Unique ID for this execution
            execution_config: Configuration for the execution
            
        Returns:
            Execution ID
        """
        try:
            # Normalize config to ensure consistent format
            execution_config = normalize_config(execution_config)
            
            # Extract crew definition sections from config
            crew_config = execution_config.get("crew", {})
            agent_configs = execution_config.get("agents", [])
            task_configs = execution_config.get("tasks", [])
            
            # Setup output directory
            output_dir = self._setup_output_directory(execution_id)
            execution_config['output_dir'] = output_dir
            
            # We assume the execution record is already created by the caller
            # We will only update the status
            
            # Ensure writer is started before running execution
            await TraceManager.ensure_writer_started()
            
            logger.info(f"[CrewAIEngineService] Starting run_execution for ID: {execution_id}")
            await self._update_execution_status(
                execution_id, 
                ExecutionStatus.PREPARING.value,
                "Preparing CrewAI execution"
            )
            
            try:
                # Create services using the Unit of Work pattern
                from src.core.unit_of_work import UnitOfWork
                from src.services.tool_service import ToolService
                from src.services.api_keys_service import ApiKeysService
                
                # Use a single UnitOfWork to manage all repositories
                async with UnitOfWork() as uow:
                    # Create services from the UnitOfWork
                    tool_service = await ToolService.from_unit_of_work(uow)
                    api_keys_service = await ApiKeysService.from_unit_of_work(uow)
                    
                    # Create a tool factory instance with API keys service
                    tool_factory = await ToolFactory.create(execution_config, api_keys_service)
                    logger.info(f"[CrewAIEngineService] Created ToolFactory for {execution_id}")
                    
                    # Use the CrewPreparation class for crew setup with tool_service and tool_factory
                    crew_preparation = CrewPreparation(execution_config, tool_service, tool_factory)
                    if not await crew_preparation.prepare():
                        logger.error(f"[CrewAIEngineService] Failed to prepare crew for {execution_id}")
                        await self._update_execution_status(
                            execution_id, 
                            ExecutionStatus.FAILED.value,
                            "Failed to prepare crew"
                        )
                        return execution_id
                    
                    # Get the prepared crew for use after UnitOfWork context
                    crew = crew_preparation.crew
            
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
            
            # --- Instantiate Event Listeners --- 
            logger.debug(f"[CrewAIEngineService] Instantiating event listeners for {execution_id}")
            try:
                # Create the event listeners with proper error handling
                # Just creating the instances is enough - they'll register with the event bus during init
                agent_trace_callback = AgentTraceEventListener(job_id=execution_id)
                logger.info(f"[CrewAIEngineService] Successfully created AgentTraceEventListener for {execution_id}")
                
                # Add task completion logger
                task_completion_logger = TaskCompletionLogger(job_id=execution_id)
                logger.info(f"[CrewAIEngineService] Successfully created TaskCompletionLogger for {execution_id}")
                
                # Add detailed output logger
                detailed_output_logger = DetailedOutputLogger(job_id=execution_id)
                logger.info(f"[CrewAIEngineService] Successfully created DetailedOutputLogger for {execution_id}")
                
                # No need to manually register with the crew - the listeners register with the global event bus
                logger.info(f"[CrewAIEngineService] All event listeners initialized for {execution_id}")
            except Exception as callback_error:
                logger.error(f"[CrewAIEngineService] Error creating event listeners: {callback_error}", exc_info=True)
                # Continue execution without the callbacks - we don't want to fail the entire execution
                # if trace logging doesn't work
            
            # Update status to RUNNING
            await self._update_execution_status(
                execution_id, 
                ExecutionStatus.RUNNING.value,
                "CrewAI execution started"
            )
            
            # Create a task for crew execution
            execution_task = asyncio.create_task(run_crew(
                execution_id=execution_id, 
                crew=crew,
                running_jobs=self._running_jobs
            ))
            
            # Store job info
            self._running_jobs[execution_id] = {
                "task": execution_task,
                "crew": crew,
                "start_time": datetime.now(),
                "config": execution_config
            }
            
            return execution_id
            
        except Exception as e:
            logger.error(f"Error running execution {execution_id}: {str(e)}", exc_info=True)
            raise
    
    def _setup_output_directory(self, execution_id: Optional[str] = None) -> str:
        """
        Set up output directory for workflow execution
        
        Args:
            execution_id: Optional execution ID for the workflow
            
        Returns:
            str: Path to output directory
        """
        try:
            # Create base output directory
            from pathlib import Path
            base_dir = Path(os.getcwd()) / "tmp" / "crew_outputs"
            base_dir.mkdir(parents=True, exist_ok=True)
            
            # Create execution-specific directory if ID provided
            if execution_id:
                output_dir = base_dir / execution_id
                output_dir.mkdir(exist_ok=True)
                logger.info(f"Created output directory: {output_dir}")
                return str(output_dir)
            
            return str(base_dir)
            
        except Exception as e:
            logger.error(f"Error setting up output directory: {str(e)}")
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
            task = job_info["task"]
            
            # Cancel the task
            task.cancel()
            
            # Wait for task to be cancelled
            try:
                await task
            except asyncio.CancelledError:
                pass
            
            # Update status in database
            await self._update_execution_status(
                execution_id, 
                ExecutionStatus.CANCELLED.value,
                "Execution cancelled by user"
            )
            
            # Clean up
            del self._running_jobs[execution_id]
            
            return True
        except Exception as e:
            logger.error(f"Error cancelling execution {execution_id}: {str(e)}")
            return False 

    async def run_flow(self, execution_id: str, flow_config: Dict[str, Any]) -> str:
        """
        Run a CrewAI flow with the given configuration.
        
        Args:
            execution_id: Unique ID for this flow execution
            flow_config: Configuration for the flow
            
        Returns:
            Execution ID
        """
        try:
            # Normalize flow config
            flow_config = normalize_flow_config(flow_config)
            
            # Setup output directory
            output_dir = self._setup_output_directory(execution_id)
            flow_config['output_dir'] = output_dir
            
            # Ensure writer is started before running execution
            await TraceManager.ensure_writer_started()
            
            logger.info(f"[CrewAIEngineService] Starting run_flow for ID: {execution_id}")
            await self._update_execution_status(
                execution_id, 
                ExecutionStatus.PREPARING.value,
                "Preparing CrewAI flow"
            )
            
            try:
                # Create services using the Unit of Work pattern
                from src.core.unit_of_work import UnitOfWork
                from src.services.tool_service import ToolService
                from src.services.api_keys_service import ApiKeysService
                
                # Use a single UnitOfWork to manage all repositories
                async with UnitOfWork() as uow:
                    # Create services from the UnitOfWork
                    tool_service = await ToolService.from_unit_of_work(uow)
                    api_keys_service = await ApiKeysService.from_unit_of_work(uow)
                    
                    # Create a tool factory instance with API keys service
                    tool_factory = await ToolFactory.create(flow_config, api_keys_service)
                    logger.info(f"[CrewAIEngineService] Created ToolFactory for flow {execution_id}")
                    
                    # Use the FlowPreparation class for flow setup
                    flow_preparation = FlowPreparation(flow_config, tool_service, tool_factory)
                    if not await flow_preparation.prepare():
                        logger.error(f"[CrewAIEngineService] Failed to prepare flow for {execution_id}")
                        await self._update_execution_status(
                            execution_id, 
                            ExecutionStatus.FAILED.value,
                            "Failed to prepare flow"
                        )
                        return execution_id
                    
                    # Get the prepared flow
                    flow = flow_preparation.flow
                    
                    # Store flow configuration and instance in running jobs
                    self._running_jobs[execution_id] = {
                        "type": "flow",
                        "config": flow_config,
                        "flow": flow,
                        "start_time": datetime.now(UTC)
                    }
                    
                    # Update status to RUNNING
                    await self._update_execution_status(
                        execution_id,
                        ExecutionStatus.RUNNING.value,
                        "Flow execution started"
                    )
                    
                    # Execute the flow asynchronously
                    asyncio.create_task(self._execute_flow(execution_id, flow))
                    
                    return execution_id
                    
            except Exception as e:
                logger.error(f"[CrewAIEngineService] Error running flow execution {execution_id}: {str(e)}", exc_info=True)
                await self._update_execution_status(
                    execution_id,
                    ExecutionStatus.FAILED.value,
                    f"Failed during flow preparation/launch: {str(e)}"
                )
                raise
                
        except Exception as e:
            logger.error(f"[CrewAIEngineService] Error in run_flow for {execution_id}: {str(e)}", exc_info=True)
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