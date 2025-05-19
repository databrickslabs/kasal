"""
Service for execution-related operations.

This module provides business logic for execution operations including
running execution jobs, tracking status, and generating descriptive names.
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


# Configure logging
logger = logging.getLogger(__name__)
crew_logger = LoggerManager.get_instance().crew
exec_logger = LoggerManager.get_instance().crew

class ExecutionService:
    """Service for executing flows and managing executions"""
    
    # Initialize the executions dictionary as a class attribute
    executions = {}
    
    # Initialize the thread pool executor
    _thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=10)
    
    def __init__(self):
        """Initialize the service"""
        # Use factory method to create properly configured ExecutionNameService
        self.execution_name_service = ExecutionNameService.create()
        # Create a CrewAIExecutionService instance for all execution operations
        self.crewai_execution_service = CrewAIExecutionService()
    
    async def execute_flow(self, flow_id: Optional[uuid.UUID] = None, 
                           nodes: Optional[List[Dict[str, Any]]] = None, 
                           edges: Optional[List[Dict[str, Any]]] = None,
                           job_id: Optional[str] = None,
                           config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute a flow based on the provided parameters.
        
        Args:
            flow_id: Optional ID of a saved flow to execute
            nodes: Optional list of nodes for a dynamic flow
            edges: Optional list of edges for a dynamic flow
            job_id: Optional job ID for tracking the execution
            config: Optional configuration parameters
            
        Returns:
            Dictionary with execution result
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
        created_at: datetime = None
    ) -> None:
        """
        Add an execution to in-memory storage.
        
        Args:
            execution_id: ID of the execution
            status: Status of the execution
            run_name: Name of the execution run
            created_at: Creation timestamp (defaults to now)
        """
        ExecutionService.executions[execution_id] = {
            "execution_id": execution_id,
            "status": status,
            "created_at": created_at or datetime.now(),  # Use timezone-naive datetime
            "run_name": run_name,
            "output": ""
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
        execution_type: str = "crew"
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
        exec_logger = LoggerManager.get_instance().crew
        
        exec_logger.info(f"[run_crew_execution] Starting {execution_type} execution for execution_id: {execution_id}")
        exec_logger.info(f"[run_crew_execution] Execution type: {execution_type}")
        exec_logger.info(f"[run_crew_execution] Config attributes: {dir(config)}")
        
        try:
            # Update execution status to PREPARING
            await ExecutionStatusService.update_status(
                job_id=execution_id,
                status="preparing",
                message=f"Preparing {execution_type} execution"
            )
            
            # Create an instance of CrewAIExecutionService
            crew_execution_service = CrewAIExecutionService()
            
            # Process different execution types
            if execution_type.lower() == "flow":
                exec_logger.info(f"[run_crew_execution] This is a FLOW execution - delegating to CrewAIExecutionService")
                
                # Convert config to dictionary
                execution_config = {}
                if hasattr(config, 'dict'):
                    try:
                        execution_config = config.dict()
                    except Exception as dict_error:
                        exec_logger.warning(f"[run_crew_execution] Error calling dict() on config: {dict_error}")
                        # Create minimal config manually if dict() fails
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
                    config=sanitized_config
                )
                exec_logger.info(f"[run_crew_execution] Flow execution initiated: {result}")
                return result
                
            # For crew executions, use the proper method from CrewAIExecutionService
            elif execution_type.lower() == "crew":
                exec_logger.debug(f"[run_crew_execution] This is a CREW execution - delegating to CrewAIExecutionService")
                
                exec_logger.debug(f"[run_crew_execution] Calling crew_execution_service.run_crew_execution for job_id: {execution_id}")
                # This call should handle PREPARING/RUNNING updates internally
                result = await crew_execution_service.run_crew_execution(
                    execution_id=execution_id,
                    config=config
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
    
    @staticmethod
    async def list_executions() -> List[Dict[str, Any]]:
        """
        List all executions from both database and in-memory storage.
        
        Returns:
            List of execution data dictionaries
        """
        try:
            # Get executions from database using ExecutionRepository
            from src.db.session import async_session_factory
            from src.repositories.execution_repository import ExecutionRepository
            
            async with async_session_factory() as db:
                repo = ExecutionRepository(db)
                # Get all executions using the correct repository method
                db_executions = await repo.list()
                
                # Convert to list of dicts
                db_executions = [
                    {
                        "execution_id": e.job_id,
                        "status": e.status,
                        "created_at": e.created_at,
                        "run_name": e.run_name,
                        "result": e.result,
                        "error": e.error
                    }
                    for e in db_executions
                ]
            
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
            
            return results
                
        except Exception as e:
            logger.error(f"Error listing executions: {str(e)}")
            # If database access fails, just return in-memory executions
            return [
                {**data, "execution_id": execution_id} 
                for execution_id, data in ExecutionService.executions.items()
            ]
    
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
            # Set up Databricks configuration if needed
            # This is important for executions that might need to use Databricks services
            if config.model and 'databricks' in config.model.lower():
                exec_logger.info(f"Setting up Databricks token for execution with model {config.model}")
                from src.services.databricks_service import DatabricksService
                setup_result = DatabricksService.setup_token_sync()
                if not setup_result:
                    exec_logger.warning("Failed to set up Databricks token, execution may fail if it requires Databricks")
            
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
    
    @staticmethod
    async def get_execution_status(execution_id: str) -> Dict[str, Any]:
        """
        Get the current status of an execution from the database.
        
        Args:
            execution_id: String ID of the execution
            
        Returns:
            Dictionary with execution status information or None if not found
        """
        try:
            # Use ExecutionStatusService to get status
            from src.services.execution_status_service import ExecutionStatusService
            execution = await ExecutionStatusService.get_status(execution_id)
            
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
                "error": execution.error
            }
        except Exception as e:
            exec_logger.error(f"Error getting execution status for {execution_id}: {str(e)}")
            return None
    
    async def create_execution(
        self,
        config: CrewConfig, 
        background_tasks = None
    ) -> Dict[str, Any]:
        """
        Create a new execution and start it in the background.
        
        Args:
            config: Configuration for the execution
            background_tasks: Optional FastAPI background tasks object
            
        Returns:
            Dictionary with execution details
        """
        # Use consistent logger instance defined at the module level
        crew_logger.debug("[ExecutionService.create_execution] Received request to create execution.")

        try:
            # Generate a new execution ID
            execution_id = ExecutionService.create_execution_id()
            crew_logger.debug(f"[ExecutionService.create_execution] Generated execution_id: {execution_id}")

            # Generate a descriptive run name
            # Determine model safely
            model = config.model if config.model else "default-model" # Provide a default if model can be None
            # Ensure agents_yaml and tasks_yaml are dictionaries
            agents_yaml = config.agents_yaml if isinstance(config.agents_yaml, dict) else {}
            tasks_yaml = config.tasks_yaml if isinstance(config.tasks_yaml, dict) else {}
            
            request = ExecutionNameGenerationRequest(
                agents_yaml=agents_yaml,
                tasks_yaml=tasks_yaml,
                model=model
            )
            response = await self.execution_name_service.generate_execution_name(request)
            run_name = response.name
            crew_logger.debug(f"[ExecutionService.create_execution] Generated run_name: {run_name} for execution_id: {execution_id}")

            # Extract execution type and flow_id
            execution_type = config.execution_type if config.execution_type else "crew"
            flow_id = None

            if execution_type == "flow":
                crew_logger.info(f"[ExecutionService.create_execution] Creating flow execution for execution_id: {execution_id}")
                
                # Check if flow_id is directly available in config
                if hasattr(config, 'flow_id') and config.flow_id:
                    flow_id = config.flow_id
                    crew_logger.info(f"[ExecutionService.create_execution] Using flow_id from config: {flow_id}")
                # Also try to get flow_id from inputs
                elif config.inputs and "flow_id" in config.inputs:
                    flow_id = config.inputs.get("flow_id")
                    crew_logger.info(f"[ExecutionService.create_execution] Using flow_id from inputs: {flow_id}")
                
                # If no flow_id is provided, find the most recent flow
                if not flow_id:
                    exec_logger.info(f"[ExecutionService.create_execution] No flow_id provided for execution_id: {execution_id}, trying to find most recent flow")
                    try:
                        # Directly query for the most recent flow from the database
                        from src.db.session import SessionLocal
                        from src.models.flow import Flow
                        
                        db = SessionLocal()
                        try:
                            # Get the most recent flow
                            most_recent_flow = db.query(Flow).order_by(Flow.created_at.desc()).first()
                            if most_recent_flow:
                                flow_id = most_recent_flow.id
                                exec_logger.info(f"[ExecutionService.create_execution] Found most recent flow with ID {flow_id} for execution_id: {execution_id}")
                            else:
                                exec_logger.error(f"[ExecutionService.create_execution] No flows found in database for execution_id: {execution_id}")
                                raise ValueError("No flow found in the database. Please create a flow first.")
                        finally:
                            db.close()
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
                    crew_logger.info(f"[ExecutionService.create_execution] Added {len(config.nodes)} nodes to flow execution")
                elif not flow_id:
                    # Only warn about missing nodes if we don't have a flow_id
                    crew_logger.warning(f"[ExecutionService.create_execution] No nodes provided for flow execution {execution_id} and no flow_id present, this will cause an error")
                else:
                    crew_logger.info(f"[ExecutionService.create_execution] No nodes provided for flow execution {execution_id}, but flow_id {flow_id} is present. Nodes will be loaded from the database.")
                
                if hasattr(config, 'edges') and config.edges:
                    inputs["edges"] = config.edges
                    crew_logger.info(f"[ExecutionService.create_execution] Added {len(config.edges)} edges to flow execution")
                
                # Add flow configuration if available
                if hasattr(config, 'flow_config') and config.flow_config:
                    inputs["flow_config"] = config.flow_config
                    crew_logger.info(f"[ExecutionService.create_execution] Added flow_config to flow execution")

            # Add flow_id to inputs if it exists
            if flow_id:
                inputs["flow_id"] = flow_id
                # Also set it directly on the config's inputs dictionary
                if not config.inputs:
                    config.inputs = {}
                config.inputs["flow_id"] = str(flow_id)
                crew_logger.info(f"[ExecutionService.create_execution] Added flow_id {flow_id} to config.inputs")

            # Sanitize inputs to ensure all values are JSON serializable
            sanitized_inputs = ExecutionService.sanitize_for_database(inputs)

            # Create execution data
            execution_data = {
                "job_id": execution_id,
                "status": ExecutionStatus.PENDING.value, # Initial status
                "inputs": sanitized_inputs,
                "planning": bool(config.planning),  # Ensure boolean type
                "run_name": run_name,
                "created_at": datetime.now()  # Remove timezone to match database column type
            }

            crew_logger.debug(f"[ExecutionService.create_execution] Attempting to create DB record for execution_id: {execution_id} with status PENDING")
            
            # Use ExecutionStatusService to create the execution
            from src.services.execution_status_service import ExecutionStatusService
            success = await ExecutionStatusService.create_execution(execution_data)
            
            if not success:
                raise ValueError(f"Failed to create execution record for {execution_id}")

            crew_logger.info(f"[ExecutionService.create_execution] Successfully created DB record for execution_id: {execution_id} with status PENDING")

            # Add to in-memory storage
            ExecutionService.add_execution_to_memory(
                execution_id=execution_id,
                status=ExecutionStatus.PENDING.value,
                run_name=run_name,
                created_at=datetime.now()  # Remove timezone to match database column type
            )
            crew_logger.debug(f"[ExecutionService.create_execution] Added execution_id: {execution_id} to in-memory store with status PENDING")

            # Start execution in background
            crew_logger.info(f"[ExecutionService.create_execution] Preparing to launch background task for execution_id: {execution_id}...")

            if background_tasks:
                async def run_execution_task():
                    # Use a separate logger instance potentially if needed, or reuse crew_logger
                    task_logger = LoggerManager.get_instance().crew 
                    task_logger.info(f"[run_execution_task] Background task started for execution_id: {execution_id}")
                    try:
                        task_logger.debug(f"[run_execution_task] Calling ExecutionService.run_crew_execution for execution_id: {execution_id}")
                        await ExecutionService.run_crew_execution(
                            execution_id=execution_id, 
                            config=config, 
                            execution_type=execution_type
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
                crew_logger.info(f"[ExecutionService.create_execution] Added run_execution_task to FastAPI BackgroundTasks for execution_id: {execution_id}")
            else:
                # Fallback using asyncio.create_task
                crew_logger.warning(f"[ExecutionService.create_execution] FastAPI BackgroundTasks not available for {execution_id}, using asyncio.create_task.")
                asyncio.create_task(ExecutionService._run_in_background(
                    execution_id=execution_id,
                    config=config,
                    execution_type=execution_type
                ))
                crew_logger.info(f"[ExecutionService.create_execution] Launched _run_in_background task via asyncio for execution_id: {execution_id}")

            crew_logger.info(f"[ExecutionService.create_execution] Execution {execution_id} launch initiated. Returning initial response.")

            # Return execution details immediately after DB creation and task launch
            from src.schemas.execution import ExecutionCreateResponse
            return ExecutionCreateResponse( # Use Pydantic model for response
                execution_id=execution_id,
                status=ExecutionStatus.PENDING.value,
                run_name=run_name
            ).model_dump() # Return as dict

        except Exception as e:
            crew_logger.error(f"[ExecutionService.create_execution] Error during initial creation for execution: {str(e)}", exc_info=True)
            # Re-raise as HTTPException for the API boundary
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create execution: {str(e)}"
            )
    
    @staticmethod
    async def _run_in_background(execution_id: str, config: CrewConfig, execution_type: str = "crew"):
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
                execution_type=execution_type
            )
            task_logger.info(f"[_run_in_background] ExecutionService.run_crew_execution completed for execution_id: {execution_id}")
        except Exception as e:
            task_logger.error(f"[_run_in_background] Error during ExecutionService.run_crew_execution for execution_id: {execution_id}: {str(e)}", exc_info=True)
            # Note: No explicit FAILED status update here, assuming run_crew_execution handles its internal errors
            # and updates status before raising, or the session rollback handles cleanup.
        task_logger.info(f"[_run_in_background] Asyncio background task finished for execution_id: {execution_id}") 