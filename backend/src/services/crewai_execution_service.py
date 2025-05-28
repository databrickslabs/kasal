"""
Service for crew execution operations.

This module provides business logic for executing CrewAI operations including
running execution jobs, managing execution lifecycle, and handling results.
"""
import asyncio
import traceback
from datetime import datetime, UTC
from typing import Dict, Any, Optional, List
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy.ext.asyncio import AsyncSession
import logging
from enum import Enum
import uuid

from src.core.logger import LoggerManager
from src.models.execution_status import ExecutionStatus
from src.schemas.execution import CrewConfig
from src.repositories.execution_repository import ExecutionRepository
from src.repositories.flow_repository import SyncFlowRepository, get_sync_flow_repository
from src.engines.factory import EngineFactory
from src.engines.crewai.crewai_engine_service import CrewAIEngineService
from src.services.execution_status_service import ExecutionStatusService
from src.engines.crewai.crewai_flow_service import CrewAIFlowService


# Initialize logger
logger = LoggerManager.get_instance().crew

# Configure logging
crew_logger = logging.getLogger("crewai.service")

# Set to store active tasks to prevent garbage collection
_active_tasks = set()

# Global in-memory storage of executions
executions = {}


class JobStatus(Enum):
    """Status of a job."""
    PENDING = "PENDING"
    PREPARING = "PREPARING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class CrewAIExecutionService:
    """Service for managing CrewAI executions."""
    
    def __init__(self):
        """
        Initialize the service.
        """
        pass
    
    async def prepare_and_run_crew(
        self,
        execution_id: str,
        config: CrewConfig
    ) -> Dict[str, Any]:
        """
        Prepare and run a crew execution.
        
        Args:
            execution_id: ID of the execution
            config: Configuration for the crew
            
        Returns:
            Dictionary with execution results
        """
        try:
            # Update status to PREPARING
            await ExecutionStatusService.update_status(
                job_id=execution_id,
                status=ExecutionStatus.PREPARING.value,
                message="Preparing crew execution"
            )
            
            # Prepare the engine
            engine = await self._prepare_engine(config)
            
            # Wait for engine initialization to complete if it's still running
            if hasattr(engine, '_init_task') and not engine._init_task.done():
                await engine._init_task
            
            # Update status to RUNNING
            await ExecutionStatusService.update_status(
                job_id=execution_id,
                status=ExecutionStatus.RUNNING.value,
                message="Running crew execution"
            )
            
            # Run the crew via the engine - this starts the execution but doesn't wait for it to complete
            # The engine will update the status to COMPLETED or FAILED when done
            result = await engine.run_execution(execution_id, config)
            
            # Return the execution ID - do NOT update status to COMPLETED here
            # as the execution is running asynchronously and will be updated by the engine
            return {"execution_id": execution_id, "status": ExecutionStatus.RUNNING.value}
            
        except Exception as e:
            # Update status to FAILED
            await ExecutionStatusService.update_status(
                job_id=execution_id,
                status=ExecutionStatus.FAILED.value,
                message=f"Crew execution failed: {str(e)}"
            )
            raise
    
    async def _prepare_engine(self, config: CrewConfig) -> Any:
        """
        Prepare the engine for execution.
        
        Args:
            config: Configuration for the engine
            
        Returns:
            Initialized engine
        """
        # Get engine from factory
        engine = await EngineFactory.get_engine(
            engine_type="crewai",
            initialize=True,
            model=config.model
        )
        
        if not engine:
            raise ValueError("Failed to initialize CrewAI engine")
            
        return engine
    
    async def run_crew_execution(self, execution_id: str, config: CrewConfig) -> Dict[str, Any]:
        """
        Run a crew execution with the provided configuration.
        
        Args:
            execution_id: Unique ID for the execution
            config: Configuration for the execution
            
        Returns:
            Dictionary with execution results
        """
        crew_logger.info(f"Running crew execution {execution_id}")
        
        # Create an asyncio task for executing the crew
        task = asyncio.create_task(self.prepare_and_run_crew(
            execution_id=execution_id,
            config=config
        ))
        
        # Store task reference to prevent garbage collection
        _active_tasks.add(task)
        # Remove from active tasks when done
        task.add_done_callback(lambda t: _active_tasks.remove(t))
        
        # Store the task in memory for potential cancellation
        executions[execution_id] = {
            "task": task,
            "status": ExecutionStatus.PENDING.value,
            "created_at": datetime.now()
        }
        
        # Return immediate response
        return {
            "execution_id": execution_id,
            "status": ExecutionStatus.RUNNING.value,
            "message": "CrewAI execution started successfully"
        }
    
    @staticmethod
    def get_execution(execution_id: str) -> Optional[Dict[str, Any]]:
        """
        Get execution from in-memory storage.
        
        Args:
            execution_id: ID of the execution
            
        Returns:
            Execution dictionary or None if not found
        """
        return executions.get(execution_id)
    
    @staticmethod
    def add_execution_to_memory(
        execution_id: str, 
        status: str, 
        run_name: str,
        created_at: datetime = None
    ) -> None:
        """
        Add execution to in-memory storage.
        
        Args:
            execution_id: ID of the execution
            status: Status of the execution
            run_name: Name of the execution
            created_at: Timestamp when execution was created
        """
        executions[execution_id] = {
            "execution_id": execution_id,
            "status": status,
            "run_name": run_name,
            "created_at": created_at or datetime.now()
        }
    
    async def update_execution_status(
        self,
        execution_id: str,
        status: ExecutionStatus,
        message: str,
        result: Any = None
    ) -> None:
        """
        Update execution status in memory and database.
        
        Args:
            execution_id: ID of the execution
            status: New execution status
            message: Status message
            result: Execution result
        """
        # Update in-memory execution status
        if execution_id in executions:
            executions[execution_id]["status"] = status.value
            executions[execution_id]["message"] = message
            if result:
                executions[execution_id]["result"] = result
        
        # Update database through ExecutionStatusService
        await ExecutionStatusService.update_status(
            job_id=execution_id,
            status=status.value,
            message=message,
            result=result
        )
    
    async def cancel_execution(self, execution_id: str) -> bool:
        """
        Cancel a running execution.
        
        Args:
            execution_id: ID of the execution to cancel
            
        Returns:
            Boolean indicating success
        """
        crew_logger.info(f"Cancelling execution {execution_id}")
        
        # Check if execution exists in memory
        if execution_id not in executions:
            crew_logger.warning(f"Execution {execution_id} not found in memory")
            return False
            
        # Get engine from factory
        engine = await EngineFactory.get_engine(
            engine_type="crewai",
            initialize=False
        )
        
        if not engine:
            crew_logger.error(f"Failed to get engine for cancelling execution {execution_id}")
            return False
            
        # Cancel execution through engine
        return await engine.cancel_execution(execution_id)
    
    async def get_execution_status(self, execution_id: str) -> Dict[str, Any]:
        """
        Get the status of an execution.
        
        Args:
            execution_id: ID of the execution
            
        Returns:
            Dictionary with execution status details
        """
        crew_logger.info(f"Getting status for execution {execution_id}")
        
        # Check memory first
        if execution_id in executions:
            memory_status = executions[execution_id]["status"]
            
            # If terminal status, just return from memory
            if memory_status in [
                ExecutionStatus.COMPLETED.value,
                ExecutionStatus.FAILED.value,
                ExecutionStatus.CANCELLED.value
            ]:
                return executions[execution_id]
                
        # Get engine from factory
        engine = await EngineFactory.get_engine(
            engine_type="crewai",
            initialize=False
        )
        
        if not engine:
            crew_logger.error(f"Failed to get engine for execution status {execution_id}")
            return None
            
        # Get status from engine
        return await engine.get_execution_status(execution_id)
    
    async def run_flow_execution(
        self,
        flow_id: Optional[str] = None,
        nodes: Optional[List[Dict[str, Any]]] = None,
        edges: Optional[List[Dict[str, Any]]] = None,
        job_id: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Run a flow execution with the provided configuration.
        This method handles all flow data loading and execution preparation.
        
        Args:
            flow_id: Optional ID of flow to execute
            nodes: Optional list of nodes for a dynamic flow
            edges: Optional list of edges for a dynamic flow
            job_id: Optional job ID for tracking the execution
            config: Optional configuration parameters
            
        Returns:
            Dictionary with execution result
        """
        crew_logger.info(f"Running flow execution with flow_id={flow_id}, job_id={job_id}")
        
        # If no job_id is provided, generate a random UUID
        if not job_id:
            job_id = str(uuid.uuid4())
            crew_logger.info(f"Generated random job_id: {job_id}")
        
        try:
            # Initialize configuration
            execution_config = config or {}
            
            # If flow_id is provided but no nodes/edges, load flow data from repository
            if flow_id and (not nodes or not isinstance(nodes, list)):
                crew_logger.info(f"No nodes provided but flow_id exists: {flow_id}. Loading flow data from repository")
                try:
                    # Get repository instance through factory function - no need to manage session directly
                    flow_repository = get_sync_flow_repository()
                    
                    # Find flow by ID
                    flow = flow_repository.find_by_id(flow_id)
                    if not flow:
                        crew_logger.error(f"Flow with ID {flow_id} not found in repository")
                        return {
                            "success": False,
                            "error": f"Flow with ID {flow_id} not found",
                            "job_id": job_id
                        }
                    
                    # Add flow data to execution config
                    if flow.nodes:
                        execution_config['nodes'] = flow.nodes
                        crew_logger.info(f"Loaded {len(flow.nodes)} nodes from repository for flow {flow_id}")
                    if flow.edges:
                        execution_config['edges'] = flow.edges
                        crew_logger.info(f"Loaded {len(flow.edges)} edges from repository for flow {flow_id}")
                    if flow.flow_config:
                        execution_config['flow_config'] = flow.flow_config
                        crew_logger.info(f"Loaded flow_config from repository for flow {flow_id}")
                except Exception as e:
                    crew_logger.error(f"Error loading flow data from repository: {str(e)}", exc_info=True)
                    return {
                        "success": False,
                        "error": f"Error loading flow data: {str(e)}",
                        "job_id": job_id
                    }
            
            # If nodes are provided directly, add them to the config
            if nodes:
                crew_logger.info(f"Adding {len(nodes)} nodes to execution config")
                execution_config['nodes'] = nodes
                execution_config['edges'] = edges or []
            elif not flow_id:
                # Neither flow_id nor nodes provided - can't proceed
                crew_logger.error("No flow_id or nodes provided, cannot execute flow")
                return {
                    "success": False,
                    "error": "Either flow_id or nodes must be provided for flow execution",
                    "job_id": job_id
                }
            
            # Make sure flow_id is in the config
            if flow_id:
                execution_config['flow_id'] = flow_id
            
            # Create a flow service instance
            flow_service = CrewAIFlowService()
            
            # Run the flow
            try:
                # Call the flow service to run the flow
                result = await flow_service.run_flow(
                    flow_id=flow_id,
                    job_id=job_id,
                    config=execution_config
                )
                
                crew_logger.info(f"Flow execution started successfully: {result}")
                return result
            except Exception as e:
                crew_logger.error(f"Error running flow execution: {e}", exc_info=True)
                # Update status to FAILED
                await ExecutionStatusService.update_status(
                    job_id=job_id,
                    status=ExecutionStatus.FAILED.value,
                    message=f"Flow execution failed: {str(e)}"
                )
                return {
                    "success": False,
                    "error": str(e),
                    "job_id": job_id
                }
        except Exception as e:
            crew_logger.error(f"Unexpected error in run_flow_execution: {e}", exc_info=True)
            await ExecutionStatusService.update_status(
                job_id=job_id,
                status=ExecutionStatus.FAILED.value,
                message=f"Unexpected error in flow execution: {str(e)}"
            )
            return {
                "success": False,
                "error": str(e),
                "job_id": job_id
            }
    
    async def get_flow_execution(self, execution_id: int) -> Dict[str, Any]:
        """
        Get details of a specific flow execution.
        
        Args:
            execution_id: ID of the flow execution to retrieve
            
        Returns:
            Dictionary with execution details
        """
        crew_logger.info(f"Getting flow execution {execution_id}")
        
        # Create a flow service instance
        flow_service = CrewAIFlowService()
        
        # Get the execution details
        try:
            return await flow_service.get_flow_execution(execution_id)
        except Exception as e:
            crew_logger.error(f"Error getting flow execution: {e}", exc_info=True)
            raise
    
    async def get_flow_executions_by_flow(self, flow_id: str) -> Dict[str, Any]:
        """
        Get all executions for a specific flow.
        
        Args:
            flow_id: ID of the flow to get executions for
            
        Returns:
            Dictionary with list of executions
        """
        crew_logger.info(f"Getting executions for flow {flow_id}")
        
        # Create a flow service instance
        flow_service = CrewAIFlowService()
        
        # Get the executions
        try:
            return await flow_service.get_flow_executions_by_flow(flow_id)
        except Exception as e:
            crew_logger.error(f"Error getting flow executions: {e}", exc_info=True)
            raise
    