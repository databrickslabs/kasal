"""
CrewAI Flow Service - Interface for flow execution operations.

This is an adapter service that interfaces between the execution_service 
and the flow_runner_service now located in the crewai engine folder.
"""

import logging
import uuid
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from sqlalchemy.orm import Session
from fastapi import HTTPException, status

from src.core.logger import LoggerManager
from src.db.session import SessionLocal
from src.engines.crewai.flow.flow_runner_service import FlowRunnerService, BackendFlow

# Initialize logger
logger = LoggerManager.get_instance().crew

class CrewAIFlowService:
    """Service for interfacing with the CrewAI Flow Runner"""
    
    def __init__(self, session: Optional[Session] = None):
        """
        Initialize the service with an optional database session.
        
        Args:
            session: Optional database session
        """
        self.session = session
        
    def _get_flow_runner(self) -> FlowRunnerService:
        """
        Get a FlowRunnerService instance with appropriate session handling.
        
        Returns:
            FlowRunnerService instance
        """
        # If a session was provided to this service, use it
        if self.session:
            return FlowRunnerService(self.session)
        
        # Otherwise, create a new session
        new_session = SessionLocal()
        # This session will be closed by the FlowRunnerService
        return FlowRunnerService(new_session)
    
    async def run_flow(self, 
                      flow_id: Optional[Union[uuid.UUID, str]] = None, 
                      job_id: str = None,
                      config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute a flow based on the provided parameters.
        
        Args:
            flow_id: Optional ID of flow to execute
            job_id: Job ID for tracking execution
            config: Configuration parameters
            
        Returns:
            Dictionary with execution result
        """
        logger.info(f"CrewAIFlowService.run_flow called with flow_id={flow_id}, job_id={job_id}")
        
        try:
            # Create a UUID for job_id if not provided
            if not job_id:
                job_id = str(uuid.uuid4())
                logger.info(f"Generated job_id: {job_id}")
            
            # Get the FlowRunnerService
            flow_runner = self._get_flow_runner()
            
            # Call the run_flow method on the runner service
            result = await flow_runner.run_flow(
                flow_id=flow_id,
                job_id=job_id,
                config=config or {}
            )
            
            logger.info(f"Flow execution started successfully: {result}")
            return result
            
        except Exception as e:
            error_msg = f"Error executing flow: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=error_msg
            )
    
    async def get_flow_execution(self, execution_id: int) -> Dict[str, Any]:
        """
        Get details for a specific flow execution.
        
        Args:
            execution_id: ID of the flow execution
            
        Returns:
            Dictionary with execution details
        """
        try:
            flow_runner = self._get_flow_runner()
            return flow_runner.get_flow_execution(execution_id)
        except Exception as e:
            error_msg = f"Error getting flow execution: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=error_msg
            )
    
    async def get_flow_executions_by_flow(self, flow_id: Union[uuid.UUID, str]) -> Dict[str, Any]:
        """
        Get all executions for a specific flow.
        
        Args:
            flow_id: ID of the flow
            
        Returns:
            Dictionary with list of executions
        """
        try:
            # Convert string to UUID if needed
            if isinstance(flow_id, str):
                try:
                    flow_id = uuid.UUID(flow_id)
                except ValueError:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Invalid flow_id format: {flow_id}"
                    )
            
            flow_runner = self._get_flow_runner()
            return flow_runner.get_flow_executions_by_flow(flow_id)
        except HTTPException:
            # Re-raise HTTP exceptions
            raise
        except Exception as e:
            error_msg = f"Error getting flow executions: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=error_msg
            ) 