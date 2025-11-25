"""
Flow Execution Service.

This service handles all business logic for flow execution state management,
following the service architecture pattern: API -> Service -> Repository -> Model.
"""

import uuid
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.logger import LoggerManager
from src.repositories.flow_execution_repository import (
    FlowExecutionRepository,
    FlowNodeExecutionRepository
)
from src.schemas.flow_execution import (
    FlowExecutionCreate,
    FlowExecutionUpdate,
    FlowNodeExecutionCreate,
    FlowNodeExecutionUpdate,
    FlowExecutionStatus
)
from src.models.flow_execution import FlowExecution, FlowNodeExecution

logger = LoggerManager.get_instance().flow


class FlowExecutionService:
    """
    Service for managing flow execution state.

    This service provides business logic for:
    - Creating and managing flow executions
    - Tracking flow execution state
    - Managing node executions within flows
    - Persisting and retrieving flow state

    All database operations go through the repository layer.
    """

    def __init__(self, session: AsyncSession):
        """
        Initialize the flow execution service.

        Args:
            session: Database session for repository operations
        """
        self.session = session
        self.flow_execution_repo = FlowExecutionRepository(session)
        self.node_execution_repo = FlowNodeExecutionRepository(session)

    async def create_execution(
        self,
        flow_id: Union[uuid.UUID, str],
        job_id: str,
        run_name: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        group_id: Optional[str] = None
    ) -> FlowExecution:
        """
        Create a new flow execution with multi-tenant isolation.

        Args:
            flow_id: ID of the flow to execute
            job_id: Job ID for tracking
            run_name: Optional descriptive name for the execution
            config: Optional configuration for the execution
            group_id: Optional group ID for multi-tenant isolation.
                     If not provided, will be inherited from the parent flow.

        Returns:
            Created FlowExecution instance

        Raises:
            ValueError: If flow_id is invalid
        """
        logger.info(f"Creating flow execution for flow {flow_id}, job {job_id}, run_name={run_name}, group {group_id}")

        # Convert string to UUID if needed
        if isinstance(flow_id, str):
            try:
                flow_id = uuid.UUID(flow_id)
            except ValueError as e:
                logger.error(f"Invalid UUID format for flow_id: {flow_id}")
                raise ValueError(f"Invalid UUID format: {str(e)}")

        # If group_id not provided, inherit from the parent flow
        if group_id is None:
            from src.repositories.flow_repository import FlowRepository
            flow_repo = FlowRepository(self.session)
            flow = await flow_repo.get(flow_id)
            if flow and flow.group_id:
                group_id = flow.group_id
                logger.info(f"Inherited group_id {group_id} from parent flow {flow_id}")

        # Generate run_name if not provided
        if not run_name:
            # Use a default format with flow_id and timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            flow_id_str = str(flow_id)[:8]  # Use first 8 chars of flow_id
            run_name = f"Flow Execution {flow_id_str} - {timestamp}"
            logger.info(f"Generated default run_name: {run_name}")

        # Create execution via repository
        execution_data = FlowExecutionCreate(
            flow_id=flow_id,
            job_id=job_id,
            status=FlowExecutionStatus.PENDING,
            config=config or {},
            run_name=run_name,
            group_id=group_id
        )

        execution = await self.flow_execution_repo.create(execution_data)
        logger.info(f"Created flow execution {execution.id} for group {group_id}")

        return execution

    async def get_execution(self, execution_id: int) -> Optional[FlowExecution]:
        """
        Get a flow execution by ID.

        Args:
            execution_id: ID of the execution

        Returns:
            FlowExecution instance or None if not found
        """
        return await self.flow_execution_repo.get_by_id(execution_id)

    async def get_executions_by_flow(
        self,
        flow_id: Union[uuid.UUID, str]
    ) -> List[FlowExecution]:
        """
        Get all executions for a specific flow.

        Args:
            flow_id: ID of the flow

        Returns:
            List of FlowExecution instances
        """
        if isinstance(flow_id, str):
            flow_id = uuid.UUID(flow_id)

        return await self.flow_execution_repo.get_by_flow_id(flow_id)

    async def update_execution_status(
        self,
        execution_id: int,
        status: FlowExecutionStatus,
        error: Optional[str] = None,
        result: Optional[Dict[str, Any]] = None
    ) -> FlowExecution:
        """
        Update the status of a flow execution.

        Args:
            execution_id: ID of the execution
            status: New status
            error: Optional error message
            result: Optional result data

        Returns:
            Updated FlowExecution instance
        """
        logger.info(f"Updating execution {execution_id} status to {status}")

        update_data = FlowExecutionUpdate(
            status=status,
            error=error,
            result=result
        )

        # The repository will automatically set completed_at for terminal statuses
        execution = await self.flow_execution_repo.update(execution_id, update_data)
        logger.info(f"Updated execution {execution_id} to status {status}")

        return execution

    async def update_execution_config(
        self,
        execution_id: int,
        config: Dict[str, Any]
    ) -> FlowExecution:
        """
        Update the configuration/state of a flow execution.

        This method is used to persist flow state during execution.

        Args:
            execution_id: ID of the execution
            config: Updated configuration/state data

        Returns:
            Updated FlowExecution instance
        """
        logger.debug(f"Updating execution {execution_id} config")

        update_data = FlowExecutionUpdate(config=config)
        execution = await self.flow_execution_repo.update(execution_id, update_data)

        return execution

    async def create_node_execution(
        self,
        flow_execution_id: int,
        node_id: str,
        agent_id: Optional[Union[uuid.UUID, str]] = None,
        task_id: Optional[Union[uuid.UUID, str]] = None,
        group_id: Optional[str] = None
    ) -> FlowNodeExecution:
        """
        Create a new node execution within a flow with multi-tenant isolation.

        Args:
            flow_execution_id: ID of the parent flow execution
            node_id: ID of the node being executed
            agent_id: Optional ID of the agent executing the node
            task_id: Optional ID of the task being executed
            group_id: Optional group ID for multi-tenant isolation.
                     If not provided, will be inherited from the parent flow execution.

        Returns:
            Created FlowNodeExecution instance
        """
        logger.info(f"Creating node execution for node {node_id} in flow execution {flow_execution_id}")

        # If group_id not provided, inherit from parent flow execution
        if group_id is None:
            flow_execution = await self.get_execution(flow_execution_id)
            if flow_execution and flow_execution.group_id:
                group_id = flow_execution.group_id
                logger.info(f"Inherited group_id {group_id} from parent flow execution {flow_execution_id}")

        node_data = FlowNodeExecutionCreate(
            flow_execution_id=flow_execution_id,
            node_id=node_id,
            agent_id=agent_id,
            task_id=task_id,
            status=FlowExecutionStatus.RUNNING,
            group_id=group_id
        )

        node_execution = await self.node_execution_repo.create(node_data)
        logger.info(f"Created node execution {node_execution.id} for group {group_id}")

        return node_execution

    async def update_node_execution(
        self,
        node_execution_id: int,
        status: FlowExecutionStatus,
        result: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None
    ) -> FlowNodeExecution:
        """
        Update a node execution.

        Args:
            node_execution_id: ID of the node execution
            status: New status
            result: Optional result data
            error: Optional error message

        Returns:
            Updated FlowNodeExecution instance
        """
        logger.info(f"Updating node execution {node_execution_id} to status {status}")

        update_data = FlowNodeExecutionUpdate(
            status=status,
            result=result,
            error=error
        )

        # Note: completed_at is set automatically by the repository for terminal statuses
        node_execution = await self.node_execution_repo.update(node_execution_id, update_data)
        logger.info(f"Updated node execution {node_execution_id} to status {status}")

        return node_execution

    async def get_node_executions(
        self,
        flow_execution_id: int
    ) -> List[FlowNodeExecution]:
        """
        Get all node executions for a flow execution.

        Args:
            flow_execution_id: ID of the flow execution

        Returns:
            List of FlowNodeExecution instances
        """
        return await self.node_execution_repo.get_by_flow_execution_id(flow_execution_id)

    async def delete_execution(self, execution_id: int) -> bool:
        """
        Delete a flow execution and its node executions.

        Args:
            execution_id: ID of the execution to delete

        Returns:
            True if deleted successfully
        """
        logger.info(f"Deleting flow execution {execution_id}")

        # Delete node executions first
        node_executions = await self.get_node_executions(execution_id)
        for node_execution in node_executions:
            await self.node_execution_repo.delete(node_execution.id)

        # Delete the flow execution
        await self.flow_execution_repo.delete(execution_id)
        logger.info(f"Deleted flow execution {execution_id}")

        return True
