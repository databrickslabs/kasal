"""
CrewAI Flow Service - Interface for flow execution operations.

This is an adapter service that interfaces between the execution_service 
and the flow_runner_service now located in the crewai engine folder.
"""

import logging
import uuid
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException, status

from src.core.logger import LoggerManager
# SessionLocal removed - use async_session_factory instead
from src.engines.crewai.flow.flow_runner_service import FlowRunnerService, BackendFlow

# Initialize flow-specific logger
logger = LoggerManager.get_instance().flow

class CrewAIFlowService:
    """Service for interfacing with the CrewAI Flow Runner"""
    
    def __init__(self, session: Optional[AsyncSession] = None):
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
        
        # Cannot create sync session - need async refactoring
        # For now, return service without session
        logger.warning("FlowRunnerService created without session - needs async refactoring")
        return FlowRunnerService(None)
    
    async def run_flow(self,
                      flow_id: Optional[Union[uuid.UUID, str]] = None,
                      job_id: str = None,
                      run_name: Optional[str] = None,
                      config: Optional[Dict[str, Any]] = None,
                      group_context = None,
                      user_token: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute a flow based on the provided parameters using process isolation.

        Args:
            flow_id: Optional ID of flow to execute
            job_id: Job ID for tracking execution
            run_name: Optional descriptive name for the execution
            config: Configuration parameters
            group_context: Group context for multi-tenant isolation
            user_token: User access token for OAuth authentication

        Returns:
            Dictionary with execution result
        """
        logger.info("="*100)
        logger.info("CREWAI FLOW SERVICE - RECEIVED REQUEST (PROCESS ISOLATION)")
        logger.info(f"  flow_id: {flow_id}")
        logger.info(f"  job_id: {job_id}")
        logger.info(f"  run_name: {run_name}")
        logger.info(f"  group_context: {group_context}")
        logger.info(f"  user_token: {'<present>' if user_token else '<not provided>'}")
        if config:
            logger.info(f"  config keys: {list(config.keys())}")
            logger.info(f"  config.nodes: {len(config.get('nodes', []))} nodes")
            logger.info(f"  config.edges: {len(config.get('edges', []))} edges")
            if 'flow_config' in config:
                flow_config = config.get('flow_config', {})
                logger.info(f"  flow_config.startingPoints: {len(flow_config.get('startingPoints', []))}")
                logger.info(f"  flow_config.listeners: {len(flow_config.get('listeners', []))}")
        logger.info("="*100)

        try:
            # Create a UUID for job_id if not provided
            if not job_id:
                job_id = str(uuid.uuid4())
                logger.info(f"Generated job_id: {job_id}")

            # Get the CrewAI engine
            from src.engines.engine_factory import EngineFactory
            engine = await EngineFactory.get_engine(
                engine_type="crewai",
                db=None,  # No session needed for cached instance
                init_params={}  # Will initialize if not cached
            )

            if not engine:
                raise ValueError("Failed to get CrewAI engine")

            # Prepare flow config for engine
            flow_config = {
                'flow_id': str(flow_id) if flow_id else None,
                'run_name': run_name,
                'nodes': config.get('nodes', []) if config else [],
                'edges': config.get('edges', []) if config else [],
                'flow_config': config.get('flow_config', {}) if config else {},
                'inputs': config.get('inputs', {}) if config else {},
                'model': config.get('model') if config else None,
                'planning': config.get('planning') if config else None
            }

            # Add group_context to config if provided
            if group_context:
                flow_config['group_context'] = group_context
                if hasattr(group_context, 'primary_group_id'):
                    flow_config['group_id'] = group_context.primary_group_id

            logger.info(f"[CrewAIFlowService] Calling engine.run_flow() for {job_id}")

            # Call the engine's run_flow method with process isolation
            execution_id = await engine.run_flow(
                execution_id=job_id,
                flow_config=flow_config,
                group_context=group_context,
                user_token=user_token
            )

            logger.info(f"[CrewAIFlowService] Engine.run_flow() returned execution_id: {execution_id}")

            return {
                "success": True,
                "execution_id": execution_id,
                "job_id": job_id,
                "message": "Flow execution started in isolated process"
            }

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