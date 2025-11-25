"""Execution service for managing AI agent workflow executions.

This module provides the core service layer for managing execution operations
in the AI agent system. It handles flow execution, status tracking, and 
coordination between different execution engines.

Key Features:
    - Asynchronous flow execution with job tracking
    - Thread pool management for concurrent operations
    - Integration with CrewAI execution engine
    - Automatic execution name generation
    - Status monitoring and error handling

The service acts as the main orchestrator for all execution-related operations,
delegating specific tasks to specialized services while maintaining a unified
interface for the API layer.

Example:
    >>> service = ExecutionService()
    >>> result = await service.execute_flow(
    ...     flow_id=flow_uuid,
    ...     job_id="job_123",
    ...     config={"timeout": 300}
    ... )
"""

import logging
import sys
import traceback
import json
import os
import uuid
import concurrent.futures
import asyncio
from typing import Dict, Any, Optional, List, Union
from datetime import datetime, UTC
import litellm
from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from src.core.logger import LoggerManager
from src.schemas.execution import ExecutionStatus, CrewConfig, ExecutionNameGenerationRequest, ExecutionCreateResponse
from src.utils.asyncio_utils import run_in_thread_with_loop, create_and_run_loop
from src.services.crewai_execution_service import CrewAIExecutionService
from src.services.execution_status_service import ExecutionStatusService
from src.services.execution_name_service import ExecutionNameService
from src.utils.user_context import GroupContext


# Configure logging
logger = logging.getLogger(__name__)
crew_logger = LoggerManager.get_instance().crew
exec_logger = LoggerManager.get_instance().crew

class ExecutionService:
    """High-level service for orchestrating AI agent workflow executions.
    
    This service provides the main interface for executing flows, managing
    execution lifecycles, and coordinating between different execution engines.
    It maintains a thread pool for concurrent operations and tracks active
    executions across the system.
    
    Attributes:
        executions: Class-level dictionary tracking all active executions
        _thread_pool: Thread pool executor for concurrent operations (10 workers)
        execution_name_service: Service for generating descriptive execution names
        crewai_execution_service: Service for CrewAI-specific execution logic
    
    Note:
        The service uses class-level attributes for shared state across instances,
        enabling centralized execution tracking in a multi-threaded environment.
    """
    
    # Initialize the executions dictionary as a class attribute
    executions = {}
    
    # Initialize the thread pool executor
    _thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=10)
    
    def __init__(self, session=None):
        """Initialize the ExecutionService with required dependencies.

        Args:
            session: Optional database session for repository operations.
                     If provided, repositories will use this session instead
                     of creating their own.

        Sets up the execution name service for generating descriptive names
        and the CrewAI execution service for handling CrewAI-specific operations.

        Note:
            Uses factory methods to ensure proper configuration of dependent services.
        """
        # Store the session for repository operations
        self.session = session

        # Use factory method to create properly configured ExecutionNameService
        self.execution_name_service = ExecutionNameService.create(session)
        # Create a CrewAIExecutionService instance for all execution operations
        self.crewai_execution_service = CrewAIExecutionService()
    
    async def execute_flow(self, flow_id: Optional[uuid.UUID] = None, 
                           nodes: Optional[List[Dict[str, Any]]] = None, 
                           edges: Optional[List[Dict[str, Any]]] = None,
                           job_id: Optional[str] = None,
                           config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute a flow asynchronously with job tracking.
        
        Orchestrates the execution of either a saved flow (by ID) or a dynamic
        flow (by nodes/edges). Generates a job ID if not provided and delegates
        to the CrewAI execution service for actual processing.
        
        Args:
            flow_id: Optional UUID of a saved flow to execute. If provided,
                the flow definition will be loaded from storage.
            nodes: Optional list of node definitions for dynamic flow execution.
                Each node represents an agent or task in the workflow.
            edges: Optional list of edge definitions connecting nodes.
                Defines the execution order and dependencies.
            job_id: Optional unique identifier for tracking this execution.
                Auto-generated if not provided.
            config: Optional configuration dictionary with execution parameters
                such as timeout, retry settings, or environment variables.
            
        Returns:
            Dictionary containing execution result with keys:
                - job_id: The execution job identifier
                - status: Current execution status
                - result: Execution output (when completed)
                - error: Error details (if failed)
        
        Raises:
            HTTPException: Re-raised from underlying services for HTTP errors
            HTTPException(500): For unexpected errors during execution
        
        Example:
            >>> result = await service.execute_flow(
            ...     flow_id=uuid.UUID("123e4567-e89b-12d3-a456-426614174000"),
            ...     config={"timeout": 300, "max_retries": 3}
            ... )
        """
        logger.info(f"Executing flow with ID: {flow_id}, job_id: {job_id}")
        
        try:
            # If no job_id is provided, generate a random UUID
            if not job_id:
                job_id = str(uuid.uuid4())
                logger.info(f"Generated random job_id: {job_id}")
            
            # Prepare the execution config
            execution_config = config or {}
            
            # Delegate to CrewAIExecutionService for flow execution
            logger.info(f"Delegating flow execution to CrewAIExecutionService")
            result = await self.crewai_execution_service.run_flow_execution(
                flow_id=str(flow_id) if flow_id else None,
                nodes=nodes,
                edges=edges,
                job_id=job_id,
                config=execution_config
            )
            logger.info(f"Flow execution started successfully: {result}")
            return result
        except HTTPException:
            # Re-raise HTTP exceptions
            raise
        except Exception as e:
            error_msg = f"Unexpected error in execute_flow: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=error_msg
            )
    
    async def get_execution(self, execution_id: int) -> Dict[str, Any]:
        """
        Get details of a specific execution
        
        Args:
            execution_id: ID of the execution to retrieve
            
        Returns:
            Dictionary with execution details
        """
        try:
            return await self.crewai_execution_service.get_flow_execution(execution_id)
        except Exception as e:
            logger.error(f"Error getting execution: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error getting execution: {str(e)}"
            )
    
    async def get_executions_by_flow(self, flow_id: uuid.UUID) -> Dict[str, Any]:
        """
        Get all executions for a specific flow
        
        Args:
            flow_id: ID of the flow to get executions for
            
        Returns:
            Dictionary with execution details
        """
        try:
            return await self.crewai_execution_service.get_flow_executions_by_flow(str(flow_id))
        except Exception as e:
            logger.error(f"Error getting executions: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error getting executions: {str(e)}"
            )

    # Methods from ExecutionRunnerService
    @staticmethod
    def create_execution_id() -> str:
        """
        Generate a unique execution ID.
        
        Returns:
            A unique execution ID
        """
        return str(uuid.uuid4())
        
    @staticmethod
    def get_execution(execution_id: str) -> Optional[Dict[str, Any]]:
        """
        Get execution data from in-memory storage.
        
        Args:
            execution_id: ID of the execution to retrieve
            
        Returns:
            Execution data dictionary or None if not found
        """
        return ExecutionService.executions.get(execution_id)
        
    @staticmethod
    def add_execution_to_memory(
        execution_id: str, 
        status: str, 
        run_name: str,
        created_at: datetime = None,
        group_id: Optional[int] = None,
        group_email: Optional[str] = None
    ) -> None:
        """
        Add an execution to in-memory storage.
        
        Args:
            execution_id: ID of the execution
            status: Status of the execution
            run_name: Name of the execution run
            created_at: Creation timestamp (defaults to now)
            group_id: ID of the group that owns this execution
            group_email: Email of the group that owns this execution
        """
        ExecutionService.executions[execution_id] = {
            "execution_id": execution_id,
            "status": status,
            "created_at": created_at or datetime.now(),  # Use timezone-naive datetime
            "run_name": run_name,
            "output": "",
            "group_id": group_id,
            "group_email": group_email
        }
    
    @staticmethod
    def sanitize_for_database(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ensure all data is properly serializable for database storage.
        
        Args:
            data: Dictionary containing execution data
            
        Returns:
            Sanitized data safe for database storage
        """
        # Create a deep copy to avoid modifying the original
        result = {}
        
        for key, value in data.items():
            if isinstance(value, dict):
                result[key] = ExecutionService.sanitize_for_database(value)
            elif isinstance(value, list):
                result[key] = [
                    ExecutionService.sanitize_for_database(item) if isinstance(item, dict) else item
                    for item in value
                ]
            elif isinstance(value, uuid.UUID):
                # Convert UUID to string
                result[key] = str(value)
            else:
                # Ensure value is JSON serializable
                try:
                    json.dumps(value)
                    result[key] = value
                except (TypeError, OverflowError):
                    # Convert to string if not serializable
                    result[key] = str(value)
                    
        return result
    
    @staticmethod
    async def run_crew_execution(
        execution_id: str,
        config: CrewConfig,
        execution_type: str = "crew",
        group_context: GroupContext = None,
        session = None
    ) -> Dict[str, Any]:
        """
        Run a crew execution with the provided configuration.
        
        Args:
            execution_id: Unique identifier for the execution
            config: Configuration for the execution
            execution_type: Type of execution (crew, flow)
            
        Returns:
            Dictionary with execution result
        """
        # Create a dedicated logger for execution-specific logging
        # Use flow logger for flow executions, crew logger for crew executions
        if execution_type and execution_type.lower() == "flow":
            exec_logger = LoggerManager.get_instance().flow
        else:
            exec_logger = LoggerManager.get_instance().crew

        exec_logger.info(f"[run_crew_execution] Starting {execution_type} execution for execution_id: {execution_id}")
        exec_logger.info(f"[run_crew_execution] Execution type: {execution_type}")
        exec_logger.info(f"[run_crew_execution] Config attributes: {dir(config)}")
        
        try:
            # Execution is already created with RUNNING status, no need to update to PREPARING
            exec_logger.info(f"[run_crew_execution] Execution {execution_id} already has RUNNING status from creation")
            
            # Create an instance of CrewAIExecutionService
            crew_execution_service = CrewAIExecutionService()
            
            # Process different execution types
            if execution_type.lower() == "flow":
                exec_logger.info(f"[run_crew_execution] This is a FLOW execution - delegating to CrewAIExecutionService")
                
                # Convert config to dictionary
                execution_config = {}
                if hasattr(config, 'model_dump'):
                    try:
                        execution_config = config.model_dump()
                    except Exception as dump_error:
                        exec_logger.warning(f"[run_crew_execution] Error calling model_dump() on config: {dump_error}")
                        # Create minimal config manually if model_dump() fails
                        for attr in ['nodes', 'edges', 'flow_config', 'model', 'planning', 'inputs']:
                            if hasattr(config, attr):
                                execution_config[attr] = getattr(config, attr)
                else:
                    # Create config dictionary manually
                    for attr in ['nodes', 'edges', 'flow_config', 'model', 'planning', 'inputs']:
                        if hasattr(config, attr):
                            execution_config[attr] = getattr(config, attr)
                
                # Extract flow_id from config
                flow_id = None
                if hasattr(config, 'flow_id') and config.flow_id:
                    flow_id = config.flow_id
                    exec_logger.info(f"[run_crew_execution] Found flow_id in direct attribute: {flow_id}")
                elif hasattr(config, 'inputs') and config.inputs and isinstance(config.inputs, dict) and 'flow_id' in config.inputs:
                    flow_id = config.inputs['flow_id']
                    exec_logger.info(f"[run_crew_execution] Found flow_id in inputs dict: {flow_id}")
                
                # Sanitize the config for database
                sanitized_config = ExecutionService.sanitize_for_database(execution_config)
                
                # Delegate flow execution to CrewAIExecutionService
                result = await crew_execution_service.run_flow_execution(
                    flow_id=str(flow_id) if flow_id else None,
                    nodes=sanitized_config.get('nodes'),
                    edges=sanitized_config.get('edges'),
                    job_id=execution_id,
                    config=sanitized_config,
                    group_context=group_context
                )
                exec_logger.info(f"[run_crew_execution] Flow execution initiated: {result}")
                return result
                
            # For crew executions, use the proper method from CrewAIExecutionService
            elif execution_type.lower() == "crew":
                exec_logger.debug(f"[run_crew_execution] This is a CREW execution - delegating to CrewAIExecutionService")
                
                # NOTE: Databricks authentication is now handled via get_auth_context() in databricks_auth.py
                # No need to set up environment variables here - each component uses unified auth

                exec_logger.debug(f"[run_crew_execution] Calling crew_execution_service.run_crew_execution for job_id: {execution_id}")
                # This call should handle PREPARING/RUNNING updates internally
                result = await crew_execution_service.run_crew_execution(
                    execution_id=execution_id,
                    config=config,
                    group_context=group_context,
                    session=session
                )
                exec_logger.info(f"[run_crew_execution] Successfully initiated crew execution via CrewAIExecutionService for job_id: {execution_id}. Result: {result}")
                return result # Return result from run_crew_execution
            else:
                # For other execution types, use the standard thread pool approach
                exec_logger.debug(f"[run_crew_execution] Using thread pool execution for {execution_type} job_id {execution_id}")
                future = ExecutionService._thread_pool.submit(
                    run_in_thread_with_loop,
                    ExecutionService._execute_crew,
                    execution_id, config, execution_type
                )
                
                # Return immediate response with execution details
                return {
                    "execution_id": execution_id,
                    "status": ExecutionStatus.RUNNING.value,
                    "message": f"{execution_type.capitalize()} execution started (logging may be incomplete)"
                }
            
        except Exception as e:
            exec_logger.error(f"[run_crew_execution] Error during initiation of {execution_type} execution {execution_id}: {str(e)}", exc_info=True)
            # Attempt to update status to FAILED using ExecutionStatusService
            try:
                exec_logger.error(f"[run_crew_execution] Attempting to update status to FAILED for execution_id: {execution_id} due to error.")
                await ExecutionStatusService.update_status(
                    job_id=execution_id,
                    status="failed",
                    message=f"Failed during initiation: {str(e)}"
                )
                exec_logger.info(f"[run_crew_execution] Successfully updated status to FAILED for execution_id: {execution_id}.")
            except Exception as update_err:
                exec_logger.critical(f"[run_crew_execution] CRITICAL: Failed to update status to FAILED for execution_id: {execution_id} after initiation error: {update_err}", exc_info=True)
            
            raise # Re-raise the original exception
    
    async def list_executions(self, group_ids: List[str] = None, user_email: str = None, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """
        List executions from both database and in-memory storage with group and user filtering.

        Args:
            group_ids: List of group IDs for filtering
            user_email: User email for user-level filtering
            limit: Maximum number of executions to return
            offset: Number of executions to skip

        Returns:
            List of execution data dictionaries
        """
        try:
            # Get executions from database using ExecutionRepository
            from src.repositories.execution_repository import ExecutionRepository

            logger.debug(f"[list_executions] Starting database query - group_ids: {group_ids}, user_email: {user_email}")

            if self.session:
                logger.debug(f"[list_executions] Using injected database session: {self.session}")
                repo = ExecutionRepository(self.session)
                logger.debug(f"[list_executions] Created repository: {repo}")

                # Get executions with group and user filtering using the correct repository method
                logger.debug(f"[list_executions] Calling repo.get_execution_history with group_ids={group_ids}")
                db_executions_list, total_count = await repo.get_execution_history(
                    limit=limit,
                    offset=offset,
                    group_ids=group_ids,
                    user_email=user_email
                )
                logger.debug(f"[list_executions] Repository returned {len(db_executions_list)} items, total_count={total_count}")
                
                logger.debug(f"[list_executions] Database returned {len(db_executions_list)} executions for group_ids: {group_ids}")

                # Debug what we got
                if db_executions_list:
                    logger.debug(f"[list_executions] First execution: job_id={db_executions_list[0].job_id}, group_id={db_executions_list[0].group_id}, run_name={db_executions_list[0].run_name}")
                else:
                    logger.warning(f"[list_executions] No executions found for group_ids: {group_ids}")
                
                # Convert to list of dicts, including inputs with agents_yaml and tasks_yaml
                import json
                db_executions = []
                for e in db_executions_list:
                    exec_dict = {
                        "execution_id": e.job_id,
                        "status": e.status,
                        "created_at": e.created_at,
                        "run_name": e.run_name,
                        "result": e.result,
                        "error": e.error,
                        "group_email": e.group_email,
                        "group_id": e.group_id,  # CRITICAL: Include group_id for frontend security filtering
                        "inputs": e.inputs  # Include the inputs field
                    }
                    
                    # Also extract agents_yaml and tasks_yaml from inputs for direct access
                    if e.inputs and isinstance(e.inputs, dict):
                        if 'agents_yaml' in e.inputs:
                            exec_dict['agents_yaml'] = json.dumps(e.inputs['agents_yaml']) if isinstance(e.inputs['agents_yaml'], dict) else e.inputs.get('agents_yaml', '')
                        if 'tasks_yaml' in e.inputs:
                            exec_dict['tasks_yaml'] = json.dumps(e.inputs['tasks_yaml']) if isinstance(e.inputs['tasks_yaml'], dict) else e.inputs.get('tasks_yaml', '')
                    
                    db_executions.append(exec_dict)
            else:
                logger.error(f"[list_executions] No database session available")
                db_executions = []

            # Get in-memory executions that might not be in the database yet
            memory_executions = {}
            for execution_id, execution_data in ExecutionService.executions.items():
                # Check if this execution is already in the list from the database
                if not any(e.get("execution_id") == execution_id for e in db_executions):
                    memory_executions[execution_id] = execution_data
            
            # Combine results
            results = db_executions.copy()
            for execution_id, data in memory_executions.items():
                execution_data = data.copy()
                if "execution_id" not in execution_data:
                    execution_data["execution_id"] = execution_id
                results.append(execution_data)
            
            logger.debug(f"Returning {len(results)} total executions ({len(db_executions)} from DB, {len(memory_executions)} from memory)")
            return results
                
        except Exception as e:
            logger.error(f"Database connection failed while listing executions: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")

            # CRITICAL: Re-raise the exception so we can see what's happening
            raise
            
            # Check database configuration
            from src.config.settings import settings
            logger.error(f"Database URI: {settings.DATABASE_URI}")
            logger.error(f"Database type: {settings.DATABASE_TYPE}")
            
            # If database access fails, just return in-memory executions
            memory_only_results = [
                {**data, "execution_id": execution_id} 
                for execution_id, data in ExecutionService.executions.items()
            ]
            logger.info(f"Falling back to {len(memory_only_results)} in-memory executions")
            return memory_only_results
    
    @staticmethod
    def _execute_crew(
        execution_id: str,
        config: CrewConfig,
        execution_type: str
    ) -> None:
        """
        Execute a crew or flow with proper database updates.
        
        Args:
            execution_id: String ID for the execution
            config: Configuration for the execution
            execution_type: Type of execution (crew or flow)
        """
        exec_logger.info(f"Executing {execution_type} with ID {execution_id}")
        
        result = None
        success = False
        
        try:
            # NOTE: Databricks authentication is now handled via get_auth_context() in databricks_auth.py
            # No need to set up environment variables here - each component uses unified auth

            # Main execution logic would go here
            # For non-crew executions, such as flows
            if execution_type == "flow":
                # Run flow execution
                result = {"status": "completed", "message": "Flow execution completed"}
            else:
                # Generic execution handling
                result = {"status": "completed", "message": f"{execution_type} execution completed"}
                
            # Mark as successful
            success = True
            exec_logger.info(f"{execution_type.capitalize()} execution {execution_id} completed successfully")
            
        except Exception as e:
            exec_logger.error(f"Error during {execution_type} execution {execution_id}: {str(e)}")
            result = {"status": "failed", "error": str(e)}
            
        finally:
            # Update execution status in database using a new session
            # We need a new session since this runs in a different thread
            try:
                # Use create_and_run_loop to properly manage the event loop
                create_and_run_loop(
                    ExecutionService._update_execution_status(
                        execution_id, 
                        ExecutionStatus.COMPLETED.value if success else ExecutionStatus.FAILED.value,
                        result
                    )
                )
            except Exception as update_error:
                exec_logger.error(f"Error updating execution status: {str(update_error)}")
    
    @staticmethod
    async def _update_execution_status(
        execution_id: str,
        status: str,
        result: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Update execution status in the database.
        
        Args:
            execution_id: String ID of the execution
            status: New status for the execution
            result: Optional result data
        """
        try:
            # Use ExecutionStatusService to update status
            from src.services.execution_status_service import ExecutionStatusService
            
            # Sanitize result for database storage if needed
            update_data = {"status": status}
            if result:
                update_data["result"] = ExecutionService.sanitize_for_database(result)
            
            # Update execution status using the service
            # No need to use create_and_run_loop here since execute_db_operation_with_fresh_engine 
            # already handles event loop isolation
            success = await ExecutionStatusService.update_status(
                job_id=execution_id,
                status=status,
                message=f"Status updated to {status}",
                result=result
            )
            
            if not success:
                exec_logger.error(f"Failed to update execution {execution_id} status to {status}")
            else:
                exec_logger.info(f"Updated execution {execution_id} status to {status}")
                
        except Exception as e:
            exec_logger.error(f"Error updating execution status: {str(e)}")
    
    async def get_execution_status(self, execution_id: str, group_ids: List[str] = None) -> Dict[str, Any]:
        """
        Get the current status of an execution from the database with group filtering.
        
        Args:
            execution_id: String ID of the execution
            group_ids: List of group IDs for filtering
            
        Returns:
            Dictionary with execution status information or None if not found
        """
        try:
            # Use ExecutionHistoryRepository to get execution with group filtering
            from src.repositories.execution_history_repository import ExecutionHistoryRepository

            # Create repository with session if available
            if self.session:
                repository = ExecutionHistoryRepository(self.session)
                execution = await repository.get_execution_by_job_id(execution_id, group_ids=group_ids)
            else:
                # Log error if no session available
                exec_logger.error(f"No database session available for getting execution status")
                return None
            
            if not execution:
                # Check in-memory for very early states if needed
                exec_logger.warning(f"Execution {execution_id} not found in database.")
                return None
            
            return {
                "execution_id": execution_id,
                "status": execution.status,
                "created_at": execution.created_at,
                "result": execution.result,
                "run_name": execution.run_name,
                "error": execution.error,
                # MLflow integration fields
                "mlflow_trace_id": execution.mlflow_trace_id,
                "mlflow_experiment_name": execution.mlflow_experiment_name,
                "mlflow_evaluation_run_id": execution.mlflow_evaluation_run_id
            }
        except Exception as e:
            exec_logger.error(f"Error getting execution status for {execution_id}: {str(e)}")
            return None
    
    async def create_execution(
        self,
        config: CrewConfig, 
        background_tasks = None,
        group_context: GroupContext = None
    ) -> Dict[str, Any]:
        """
        Create a new execution and start it in the background.
        
        Args:
            config: Configuration for the execution
            background_tasks: Optional FastAPI background tasks object
            group_context: Group context for multi-tenant execution
            
        Returns:
            Dictionary with execution details
        """
        # Use consistent logger instance defined at the module level
        # Choose logger based on execution type
        execution_type = config.execution_type if hasattr(config, 'execution_type') and config.execution_type else "crew"
        if execution_type.lower() == "flow":
            logger = LoggerManager.get_instance().flow
            # Also update exec_logger for backward compatibility with existing code
            exec_logger = LoggerManager.get_instance().flow
        else:
            logger = crew_logger
            exec_logger = crew_logger

        logger.debug("[ExecutionService.create_execution] Received request to create execution.")

        try:
            # Check for running jobs to enforce single job execution constraint
            # await self._check_for_running_jobs(group_context)  # COMMENTED OUT FOR TESTING
            pass

        except ValueError as e:
            # Re-raise validation errors (like active job constraint) as HTTPException
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=str(e)
            )

        try:
            # Generate a new execution ID
            execution_id = ExecutionService.create_execution_id()
            logger.debug(f"[ExecutionService.create_execution] Generated execution_id: {execution_id}")

            # Generate a descriptive run name
            # Determine model safely
            model = config.model if config.model else "default-model" # Provide a default if model can be None
            # Ensure agents_yaml and tasks_yaml are dictionaries
            agents_yaml = config.agents_yaml if isinstance(config.agents_yaml, dict) else {}
            tasks_yaml = config.tasks_yaml if isinstance(config.tasks_yaml, dict) else {}
            
            # Log the agents_yaml to see if knowledge_sources are present
            logger.info(f"[ExecutionService.create_execution] Received agents_yaml with {len(agents_yaml)} agents")
            for agent_id, agent_config in agents_yaml.items():
                logger.info(f"[ExecutionService.create_execution] Agent {agent_id} keys: {list(agent_config.keys())}")
                if "knowledge_sources" in agent_config:
                    knowledge_sources = agent_config.get("knowledge_sources", [])
                    logger.info(f"[ExecutionService.create_execution] Agent {agent_id} has {len(knowledge_sources)} knowledge_sources")
                    for idx, source in enumerate(knowledge_sources):
                        logger.info(f"[ExecutionService.create_execution] Agent {agent_id} knowledge_source[{idx}]: {source}")
                else:
                    logger.warning(f"[ExecutionService.create_execution] Agent {agent_id} has NO knowledge_sources field")

            # Ensure GroupContext is available in UserContext for authentication
            # This is critical for both OBO (user_token) and PAT (group_id) authentication
            if group_context:
                from src.utils.user_context import UserContext
                UserContext.set_group_context(group_context)
                logger.info(f"[ExecutionService.create_execution] Set GroupContext for execution name generation: primary_group_id={group_context.primary_group_id}, has_access_token={bool(group_context.access_token)}")

                # Also set user_token if available for OBO authentication
                if hasattr(group_context, 'access_token') and group_context.access_token:
                    UserContext.set_user_token(group_context.access_token)
                    logger.info("[ExecutionService.create_execution] Set user_token for OBO authentication")

            request = ExecutionNameGenerationRequest(
                agents_yaml=agents_yaml,
                tasks_yaml=tasks_yaml,
                model=model
            )
            response = await self.execution_name_service.generate_execution_name(request)
            run_name = response.name
            logger.debug(f"[ExecutionService.create_execution] Generated run_name: {run_name} for execution_id: {execution_id}")

            # Add run_name to config inputs for crew consistency
            if not config.inputs:
                config.inputs = {}
            config.inputs["run_name"] = run_name
            logger.info(f"[ExecutionService.create_execution] Added run_name to config.inputs for consistent crew_id generation")

            # Extract execution type and flow_id
            # Note: execution_type is already captured above for logger selection
            flow_id = None

            if execution_type == "flow":
                logger.info(f"[ExecutionService.create_execution] Creating flow execution for execution_id: {execution_id}")
                
                # Check if flow_id is directly available in config
                if hasattr(config, 'flow_id') and config.flow_id:
                    flow_id = config.flow_id
                    logger.info(f"[ExecutionService.create_execution] Using flow_id from config: {flow_id}")
                # Also try to get flow_id from inputs
                elif config.inputs and "flow_id" in config.inputs:
                    flow_id = config.inputs.get("flow_id")
                    logger.info(f"[ExecutionService.create_execution] Using flow_id from inputs: {flow_id}")
                
                # If no flow_id is provided, check if nodes/edges are provided for ad-hoc execution
                # This allows "test before save" workflow from the canvas
                if not flow_id:
                    # Check if nodes and edges are provided in config for ad-hoc execution
                    has_nodes = hasattr(config, 'nodes') and config.nodes
                    has_edges = hasattr(config, 'edges') and config.edges

                    if has_nodes and has_edges:
                        # Ad-hoc flow execution with nodes/edges from canvas (no database save required)
                        exec_logger.info(f"[ExecutionService.create_execution] No flow_id provided, but nodes ({len(config.nodes)}) and edges ({len(config.edges)}) present - allowing ad-hoc flow execution")
                    else:
                        # No flow_id and no nodes/edges, try to find the most recent flow from database
                        exec_logger.info(f"[ExecutionService.create_execution] No flow_id or nodes/edges provided for execution_id: {execution_id}, trying to find most recent flow from database")
                        try:
                            # Use async query for the most recent flow from the database
                            from src.db.session import async_session_factory
                            from src.models.flow import Flow
                            from sqlalchemy import select, desc

                            async with async_session_factory() as db:
                                # Get the most recent flow using async query
                                stmt = select(Flow).order_by(desc(Flow.created_at)).limit(1)
                                result = await db.execute(stmt)
                                most_recent_flow = result.scalars().first()

                                if most_recent_flow:
                                    flow_id = most_recent_flow.id
                                    exec_logger.info(f"[ExecutionService.create_execution] Found most recent flow with ID {flow_id} for execution_id: {execution_id}")
                                else:
                                    exec_logger.error(f"[ExecutionService.create_execution] No flows found in database for execution_id: {execution_id}")
                                    raise ValueError("No flow found in the database. Please create a flow first, or provide nodes and edges for ad-hoc execution.")
                        except Exception as e:
                            exec_logger.error(f"[ExecutionService.create_execution] Error finding most recent flow: {str(e)}")
                            raise ValueError(f"Error finding most recent flow: {str(e)}")

            # Create database entry
            inputs = {
                "agents_yaml": config.agents_yaml,
                "tasks_yaml": config.tasks_yaml,
                "inputs": config.inputs,
                "planning": config.planning,
                "model": config.model,
                "execution_type": execution_type,
                "schema_detection_enabled": config.schema_detection_enabled
            }

            # For flow executions, make sure to include nodes and edges in the inputs
            if execution_type == "flow":
                # Make sure we have nodes and edges for flow execution
                if hasattr(config, 'nodes') and config.nodes:
                    inputs["nodes"] = config.nodes
                    logger.info(f"[ExecutionService.create_execution] Added {len(config.nodes)} nodes to flow execution")
                elif not flow_id:
                    # Only warn about missing nodes if we don't have a flow_id
                    logger.warning(f"[ExecutionService.create_execution] No nodes provided for flow execution {execution_id} and no flow_id present, this will cause an error")
                else:
                    logger.info(f"[ExecutionService.create_execution] No nodes provided for flow execution {execution_id}, but flow_id {flow_id} is present. Nodes will be loaded from the database.")

                if hasattr(config, 'edges') and config.edges:
                    inputs["edges"] = config.edges
                    logger.info(f"[ExecutionService.create_execution] Added {len(config.edges)} edges to flow execution")

                # Add flow configuration if available
                if hasattr(config, 'flow_config') and config.flow_config:
                    inputs["flow_config"] = config.flow_config
                    logger.info(f"[ExecutionService.create_execution] Added flow_config to flow execution")

            # Add flow_id to inputs if it exists
            if flow_id:
                inputs["flow_id"] = flow_id
                # Also set it directly on the config's inputs dictionary
                if not config.inputs:
                    config.inputs = {}
                config.inputs["flow_id"] = str(flow_id)
                logger.info(f"[ExecutionService.create_execution] Added flow_id {flow_id} to config.inputs")

            # Sanitize inputs to ensure all values are JSON serializable
            sanitized_inputs = ExecutionService.sanitize_for_database(inputs)

            # Create execution data with RUNNING status for immediate visibility
            execution_data = {
                "job_id": execution_id,
                "status": ExecutionStatus.RUNNING.value, # Start with RUNNING status for immediate visibility
                "inputs": sanitized_inputs,
                "planning": bool(config.planning),  # Ensure boolean type
                "run_name": run_name,
                "created_at": datetime.now()  # Remove timezone to match database column type
            }

            logger.debug(f"[ExecutionService.create_execution] Attempting to create DB record for execution_id: {execution_id} with status RUNNING")

            # Use ExecutionStatusService to create the execution
            from src.services.execution_status_service import ExecutionStatusService
            success = await ExecutionStatusService.create_execution(execution_data, group_context=group_context)

            if not success:
                raise ValueError(f"Failed to create execution record for {execution_id}")

            logger.info(f"[ExecutionService.create_execution] Successfully created DB record for execution_id: {execution_id} with status RUNNING")

            # Add to in-memory storage with RUNNING status
            ExecutionService.add_execution_to_memory(
                execution_id=execution_id,
                status=ExecutionStatus.RUNNING.value,
                run_name=run_name,
                created_at=datetime.now()  # Remove timezone to match database column type
            )
            logger.debug(f"[ExecutionService.create_execution] Added execution_id: {execution_id} to in-memory store with status RUNNING")

            # Start execution in background
            logger.info(f"[ExecutionService.create_execution] Preparing to launch background task for execution_id: {execution_id}...")

            if background_tasks:
                async def run_execution_task():
                    # Use context-aware logger based on execution type
                    task_logger = LoggerManager.get_instance().flow if execution_type.lower() == "flow" else LoggerManager.get_instance().crew
                    task_logger.info(f"[run_execution_task] Background task started for execution_id: {execution_id}")
                    try:
                        task_logger.debug(f"[run_execution_task] Calling ExecutionService.run_crew_execution for execution_id: {execution_id}")
                        await ExecutionService.run_crew_execution(
                            execution_id=execution_id,
                            config=config,
                            execution_type=execution_type,
                            group_context=group_context,
                            session=self.session
                        )
                        task_logger.info(f"[run_execution_task] ExecutionService.run_crew_execution completed for execution_id: {execution_id}")
                    except Exception as task_error:
                        # This catches errors that escape run_crew_execution (e.g., if it re-raises)
                        task_logger.error(f"[run_execution_task] Error escaped from ExecutionService.run_crew_execution for execution_id: {execution_id}: {str(task_error)}", exc_info=True)
                        # Fallback: Attempt to update status if the status update in run_crew_execution failed
                        task_logger.error(f"[run_execution_task] Fallback: Attempting to update status to FAILED for execution_id: {execution_id} due to escaped task error.")
                        try:
                            await ExecutionStatusService.update_status(
                                job_id=execution_id,
                                status="failed",
                                message=f"Execution failed due to error: {str(task_error)}"
                            )
                            task_logger.info(f"[run_execution_task] Fallback: Successfully committed FAILED status for {execution_id} due to escaped task error.")
                        except Exception as status_ex:
                            task_logger.error(f"[run_execution_task] Fallback: Failed to update status for {execution_id}: {status_ex}")
                    task_logger.info(f"[run_execution_task] Background task finished for execution_id: {execution_id}")

                background_tasks.add_task(run_execution_task)
                logger.info(f"[ExecutionService.create_execution] Added run_execution_task to FastAPI BackgroundTasks for execution_id: {execution_id}")
            else:
                # Fallback using asyncio.create_task
                logger.warning(f"[ExecutionService.create_execution] FastAPI BackgroundTasks not available for {execution_id}, using asyncio.create_task.")
                task = asyncio.create_task(ExecutionService._run_in_background(
                    execution_id=execution_id,
                    config=config,
                    execution_type=execution_type,
                    group_context=group_context,
                    session=self.session
                ))
                # Store the task reference so we can cancel it later
                if execution_id in ExecutionService.executions:
                    ExecutionService.executions[execution_id]["task"] = task
                    logger.debug(f"[ExecutionService.create_execution] Stored asyncio task reference for {execution_id}")
                logger.info(f"[ExecutionService.create_execution] Launched _run_in_background task via asyncio for execution_id: {execution_id}")

            logger.info(f"[ExecutionService.create_execution] Execution {execution_id} launch initiated. Returning initial response.")

            # Return execution details immediately after DB creation and task launch
            from src.schemas.execution import ExecutionCreateResponse
            return ExecutionCreateResponse( # Use Pydantic model for response
                execution_id=execution_id,
                status=ExecutionStatus.RUNNING.value, # Return RUNNING status for immediate visibility
                run_name=run_name
            ).model_dump() # Return as dict

        except Exception as e:
            logger.error(f"[ExecutionService.create_execution] Error during initial creation for execution: {str(e)}", exc_info=True)
            # Re-raise as HTTPException for the API boundary
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create execution: {str(e)}"
            )
    
    @staticmethod
    async def _run_in_background(execution_id: str, config: CrewConfig, execution_type: str = "crew", group_context: GroupContext = None, session = None):
        """
        Run an execution in the background using a new database session.
        This is used when FastAPI's background_tasks is not available.
        
        Args:
            execution_id: ID of the execution
            config: Configuration for the execution
            execution_type: Type of execution (crew or flow)
        """
        # Use a separate logger instance potentially if needed, or reuse exec_logger
        task_logger = LoggerManager.get_instance().crew
        task_logger.info(f"[_run_in_background] Asyncio background task started for execution_id: {execution_id}")
        try:
            task_logger.debug(f"[_run_in_background] Calling ExecutionService.run_crew_execution for execution_id: {execution_id}")
            await ExecutionService.run_crew_execution(
                execution_id=execution_id,
                config=config,
                execution_type=execution_type,
                group_context=group_context,
                session=session
            )
            task_logger.info(f"[_run_in_background] ExecutionService.run_crew_execution completed for execution_id: {execution_id}")
        except Exception as e:
            task_logger.error(f"[_run_in_background] Error during ExecutionService.run_crew_execution for execution_id: {execution_id}: {str(e)}", exc_info=True)
            # Note: No explicit FAILED status update here, assuming run_crew_execution handles its internal errors
            # and updates status before raising, or the session rollback handles cleanup.
        task_logger.info(f"[_run_in_background] Asyncio background task finished for execution_id: {execution_id}")
    
    async def _check_for_running_jobs(self, group_context: GroupContext = None) -> None:
        """
        Check for running jobs to enforce single job execution constraint.
        
        Args:
            group_context: Group context for filtering (ensures users can only see their own group's jobs)
            
        Raises:
            ValueError: If there are any running jobs
        """
        try:
            # Get active statuses that should block new executions
            active_statuses = [
                ExecutionStatus.PENDING.value,
                ExecutionStatus.PREPARING.value, 
                ExecutionStatus.RUNNING.value
            ]
            
            # Use ExecutionRepository to check for active executions
            from src.db.session import async_session_factory
            from src.repositories.execution_repository import ExecutionRepository
            
            async with async_session_factory() as db:
                repo = ExecutionRepository(db)
                
                # Get executions with group filtering
                group_ids = group_context.group_ids if group_context else None
                active_executions, _ = await repo.get_execution_history(
                    limit=1,  # We only need to know if any exist
                    offset=0,
                    group_ids=group_ids,
                    status_filter=active_statuses  # Filter for active statuses
                )
                
                if active_executions:
                    active_execution = active_executions[0]
                    error_msg = (
                        f"Cannot start new job. Another job is currently running: "
                        f"'{active_execution.run_name}' (Status: {active_execution.status}). "
                        f"Please wait for it to complete. "
                        f"Note: In future releases, we plan to add support for concurrent job execution."
                    )
                    crew_logger.warning(f"[ExecutionService._check_for_running_jobs] {error_msg}")
                    raise ValueError(error_msg)
                    
        except ValueError:
            # Re-raise ValueError (our constraint violation)
            raise
        except Exception as e:
            # Log other errors but don't block execution creation
            crew_logger.error(f"[ExecutionService._check_for_running_jobs] Error checking for running jobs: {str(e)}")
            # We don't raise here to avoid blocking execution if the check fails for technical reasons
    
    async def generate_execution_name(self, request: ExecutionNameGenerationRequest) -> Dict[str, str]:
        """
        Generate a descriptive name for an execution based on agents and tasks configuration.
        
        Args:
            request: The execution name generation request
            
        Returns:
            Dict containing the generated name
        """
        response = await self.execution_name_service.generate_execution_name(request)
        return {"name": response.name}
    
    async def stop_execution(
        self,
        execution_id: str,
        stop_type: str,
        reason: Optional[str] = None,
        requested_by: Optional[str] = None,
        preserve_partial_results: bool = True,
        db: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        Stop a running execution.
        
        Args:
            execution_id: ID of the execution to stop
            stop_type: Type of stop (graceful or force)
            reason: Optional reason for stopping
            requested_by: User who requested the stop
            preserve_partial_results: Whether to save partial results
            db: Database session
            
        Returns:
            Dict with stop status and partial results if available
        """
        from src.models.execution_status import ExecutionStatus
        from src.schemas.execution import StopExecutionResponse
        from src.models.execution_history import ExecutionHistory
        from sqlalchemy import update
        from datetime import datetime
        
        try:
            # Update execution status to STOPPING
            if db:
                # First set the is_stopping flag and status to STOPPING
                update_stmt = (
                    update(ExecutionHistory)
                    .where(ExecutionHistory.job_id == execution_id)
                    .values(
                        status=ExecutionStatus.STOPPING.value,
                        is_stopping=True,
                        stop_reason=reason,
                        stop_requested_by=requested_by
                    )
                )
                await db.execute(update_stmt)
                await db.commit()
                
                # Get current execution state for partial results
                from sqlalchemy import select
                stmt = select(ExecutionHistory).where(ExecutionHistory.job_id == execution_id)
                result = await db.execute(stmt)
                execution = result.scalar_one_or_none()
                
                partial_results = None
                if preserve_partial_results and execution:
                    partial_results = execution.result
            
            # Check if execution is in our active executions dictionary
            if execution_id in self.executions:
                execution_info = self.executions[execution_id]
                
                # For force stop, cancel the asyncio task if it exists
                if stop_type == "force" and "task" in execution_info:
                    task = execution_info["task"]
                    if not task.done():
                        task.cancel()
                        crew_logger.info(f"Force cancelled task for execution {execution_id}")
                
                # For graceful stop, set a flag that the execution should check
                if stop_type == "graceful":
                    execution_info["stop_requested"] = True
                    crew_logger.info(f"Graceful stop requested for execution {execution_id}")
            
            # Try to stop using ProcessCrewExecutor first (for process-based executions)
            process_terminated = False
            try:
                from src.services.process_crew_executor import process_crew_executor
                
                # Try to terminate the process
                process_terminated = await process_crew_executor.terminate_execution(execution_id)
                if process_terminated:
                    crew_logger.info(f"Successfully terminated process for execution {execution_id}")
                else:
                    crew_logger.info(f"Execution {execution_id} not found in ProcessCrewExecutor (may be thread-based)")
                    
            except Exception as process_error:
                crew_logger.debug(f"Could not stop via ProcessCrewExecutor: {process_error}")
            
            # If not process-based, try the thread-based crew_executor  
            if not process_terminated:
                try:
                    from src.services.crew_executor import crew_executor
                    
                    # Request cooperative stop through the executor
                    stop_requested = crew_executor.request_stop(execution_id)
                    if stop_requested:
                        crew_logger.info(f"Stop requested for execution {execution_id} via CrewExecutor")
                        
                        # For force stop, also cancel the asyncio task if it exists
                        if stop_type == "force" and execution_id in self.executions and "task" in self.executions[execution_id]:
                            task = self.executions[execution_id]["task"]
                            if not task.done():
                                task.cancel()
                                crew_logger.info(f"Force cancelled asyncio task for execution {execution_id}")
                                
                                # Wait briefly for cancellation
                                try:
                                    await asyncio.wait_for(asyncio.shield(task), timeout=2.0)
                                except (asyncio.CancelledError, asyncio.TimeoutError):
                                    crew_logger.info(f"Task cancellation completed for {execution_id}")
                    else:
                        crew_logger.warning(f"Execution {execution_id} not found in CrewExecutor")
                        
                except Exception as executor_error:
                    crew_logger.warning(f"Could not stop via CrewExecutor: {executor_error}")
            
            # Also try to cancel via CrewAIEngineService
            try:
                from src.engines.crewai.crewai_engine_service import CrewAIEngineService
                crew_service = CrewAIEngineService()
                cancelled = await crew_service.cancel_execution(execution_id)
                if cancelled:
                    crew_logger.info(f"Successfully cancelled execution {execution_id} via CrewAIEngineService")
            except Exception as cancel_error:
                crew_logger.warning(f"Could not cancel via CrewAIEngineService: {cancel_error}")
            
            # Remove from active executions to stop tracking it
            if execution_id in self.executions:
                del self.executions[execution_id]
                crew_logger.info(f"Removed {execution_id} from active executions tracking")
            
            # Log that we've attempted to stop the execution threads
            crew_logger.info(
                f"Execution {execution_id} stop initiated. ThreadManager attempted to stop related threads. "
                "Note: CrewAI does not natively support cancellation, but threads have been targeted for termination."
            )
            
            # Final update to mark as STOPPED
            if db:
                final_update_stmt = (
                    update(ExecutionHistory)
                    .where(ExecutionHistory.job_id == execution_id)
                    .values(
                        status=ExecutionStatus.STOPPED.value,
                        is_stopping=False,
                        stopped_at=datetime.utcnow(),
                        partial_results=partial_results if preserve_partial_results else None
                    )
                )
                await db.execute(final_update_stmt)
                await db.commit()
            
            return {
                "execution_id": execution_id,
                "status": ExecutionStatus.STOPPED.value,
                "message": f"Execution {stop_type} stopped successfully",
                "partial_results": partial_results
            }
            
        except Exception as e:
            crew_logger.error(f"Error stopping execution {execution_id}: {str(e)}")
            
            # Try to update status to indicate stop failed
            if db:
                try:
                    error_update_stmt = (
                        update(ExecutionHistory)
                        .where(ExecutionHistory.job_id == execution_id)
                        .values(
                            is_stopping=False,
                            error=f"Failed to stop: {str(e)}"
                        )
                    )
                    await db.execute(error_update_stmt)
                    await db.commit()
                except:
                    pass
            
            raise Exception(f"Failed to stop execution: {str(e)}")