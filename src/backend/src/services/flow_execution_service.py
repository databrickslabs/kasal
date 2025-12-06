"""
Flow Execution Service.

This service handles all business logic for flow execution state management,
following the service architecture pattern: API -> Service -> Repository -> Model.

NOTE: This service now uses the consolidated ExecutionHistory model instead of
the deprecated FlowExecution model. All flow executions are tracked in the
executionhistory table with execution_type='flow'.
"""

import uuid
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.core.logger import LoggerManager
from src.models.execution_history import ExecutionHistory
from src.repositories.execution_history_repository import ExecutionHistoryRepository

logger = LoggerManager.get_instance().flow


class FlowExecutionService:
    """
    Service for managing flow execution state.

    This service provides business logic for:
    - Creating and managing flow executions (stored in executionhistory with execution_type='flow')
    - Tracking flow execution state
    - Persisting and retrieving flow state

    All database operations go through the repository layer or direct session access.
    """

    def __init__(self, session: AsyncSession):
        """
        Initialize the flow execution service.

        Args:
            session: Database session for repository operations
        """
        self.session = session
        self.execution_repo = ExecutionHistoryRepository(session)

    async def create_execution(
        self,
        flow_id: Union[uuid.UUID, str],
        job_id: str,
        run_name: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        group_id: Optional[str] = None
    ) -> ExecutionHistory:
        """
        Create a new flow execution with multi-tenant isolation.

        Args:
            flow_id: ID of the flow to execute (can be None for ad-hoc flows)
            job_id: Job ID for tracking
            run_name: Optional descriptive name for the execution
            config: Optional configuration for the execution
            group_id: Optional group ID for multi-tenant isolation.
                     If not provided, will be inherited from the parent flow.

        Returns:
            Created ExecutionHistory instance

        Raises:
            ValueError: If flow_id is invalid
        """
        logger.info(f"Creating flow execution for flow {flow_id}, job {job_id}, run_name={run_name}, group {group_id}")

        # Convert string to UUID if needed
        if flow_id is not None and isinstance(flow_id, str):
            try:
                flow_id = uuid.UUID(flow_id)
            except ValueError as e:
                logger.error(f"Invalid UUID format for flow_id: {flow_id}")
                raise ValueError(f"Invalid UUID format: {str(e)}")

        # If group_id not provided, inherit from the parent flow
        if group_id is None and flow_id is not None:
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
            flow_id_str = str(flow_id)[:8] if flow_id else "adhoc"
            run_name = f"Flow Execution {flow_id_str} - {timestamp}"
            logger.info(f"Generated default run_name: {run_name}")

        # Check if an execution record already exists (created by execution_service.py)
        existing_execution = await self.session.execute(
            select(ExecutionHistory).where(ExecutionHistory.job_id == job_id)
        )
        execution = existing_execution.scalar_one_or_none()

        if execution:
            # Update existing record with flow-specific fields
            logger.info(f"Found existing execution record for job_id={job_id}, updating with flow fields")
            execution.execution_type = "flow"
            execution.flow_id = flow_id
            if run_name:
                execution.run_name = run_name
            if config:
                execution.inputs = config
            if group_id:
                execution.group_id = group_id
        else:
            # Create new execution record in executionhistory table
            execution = ExecutionHistory(
                job_id=job_id,
                status="pending",
                inputs=config or {},
                run_name=run_name,
                execution_type="flow",
                flow_id=flow_id,
                group_id=group_id,
                created_at=datetime.utcnow()
            )
            self.session.add(execution)

        await self.session.commit()
        await self.session.refresh(execution)

        logger.info(f"Flow execution {execution.id} (job_id={job_id}) ready for group {group_id}")

        return execution

    async def get_execution(self, execution_id: int) -> Optional[ExecutionHistory]:
        """
        Get a flow execution by ID.

        Args:
            execution_id: ID of the execution

        Returns:
            ExecutionHistory instance or None if not found
        """
        result = await self.session.execute(
            select(ExecutionHistory).where(
                ExecutionHistory.id == execution_id,
                ExecutionHistory.execution_type == "flow"
            )
        )
        return result.scalar_one_or_none()

    async def get_execution_by_job_id(self, job_id: str) -> Optional[ExecutionHistory]:
        """
        Get a flow execution by job_id.

        Args:
            job_id: Job ID of the execution

        Returns:
            ExecutionHistory instance or None if not found
        """
        result = await self.session.execute(
            select(ExecutionHistory).where(
                ExecutionHistory.job_id == job_id,
                ExecutionHistory.execution_type == "flow"
            )
        )
        return result.scalar_one_or_none()

    async def get_executions_by_flow(
        self,
        flow_id: Union[uuid.UUID, str]
    ) -> List[ExecutionHistory]:
        """
        Get all executions for a specific flow.

        Args:
            flow_id: ID of the flow

        Returns:
            List of ExecutionHistory instances
        """
        if isinstance(flow_id, str):
            flow_id = uuid.UUID(flow_id)

        result = await self.session.execute(
            select(ExecutionHistory).where(
                ExecutionHistory.flow_id == flow_id,
                ExecutionHistory.execution_type == "flow"
            ).order_by(ExecutionHistory.created_at.desc())
        )
        return list(result.scalars().all())

    async def update_execution_status(
        self,
        execution_id: int,
        status: str,
        error: Optional[str] = None,
        result: Optional[Dict[str, Any]] = None
    ) -> ExecutionHistory:
        """
        Update the status of a flow execution.

        Args:
            execution_id: ID of the execution
            status: New status (pending, running, completed, failed)
            error: Optional error message
            result: Optional result data

        Returns:
            Updated ExecutionHistory instance
        """
        logger.info(f"Updating execution {execution_id} status to {status}")

        execution = await self.get_execution(execution_id)
        if not execution:
            raise ValueError(f"Execution {execution_id} not found")

        execution.status = status
        if error:
            execution.error = error
        if result:
            execution.result = result

        # Set completed_at for terminal statuses
        if status in ["completed", "failed"]:
            execution.completed_at = datetime.utcnow()

        await self.session.commit()
        await self.session.refresh(execution)

        logger.info(f"Updated execution {execution_id} to status {status}")

        return execution

    async def update_execution_status_by_job_id(
        self,
        job_id: str,
        status: str,
        error: Optional[str] = None,
        result: Optional[Dict[str, Any]] = None
    ) -> Optional[ExecutionHistory]:
        """
        Update the status of a flow execution by job_id.

        Args:
            job_id: Job ID of the execution
            status: New status (pending, running, completed, failed)
            error: Optional error message
            result: Optional result data

        Returns:
            Updated ExecutionHistory instance or None if not found
        """
        logger.info(f"Updating execution with job_id {job_id} status to {status}")

        execution = await self.get_execution_by_job_id(job_id)
        if not execution:
            logger.warning(f"Execution with job_id {job_id} not found")
            return None

        execution.status = status
        if error:
            execution.error = error
        if result:
            execution.result = result

        # Set completed_at for terminal statuses
        if status in ["completed", "failed"]:
            execution.completed_at = datetime.utcnow()

        await self.session.commit()
        await self.session.refresh(execution)

        logger.info(f"Updated execution {execution.id} (job_id={job_id}) to status {status}")

        return execution

    async def update_execution_config(
        self,
        execution_id: int,
        config: Dict[str, Any]
    ) -> ExecutionHistory:
        """
        Update the configuration/state of a flow execution.

        This method is used to persist flow state during execution.

        Args:
            execution_id: ID of the execution
            config: Updated configuration/state data

        Returns:
            Updated ExecutionHistory instance
        """
        logger.debug(f"Updating execution {execution_id} config")

        execution = await self.get_execution(execution_id)
        if not execution:
            raise ValueError(f"Execution {execution_id} not found")

        execution.inputs = config
        await self.session.commit()
        await self.session.refresh(execution)

        return execution

    async def delete_execution(self, execution_id: int) -> bool:
        """
        Delete a flow execution.

        Args:
            execution_id: ID of the execution to delete

        Returns:
            True if deleted successfully
        """
        logger.info(f"Deleting flow execution {execution_id}")

        execution = await self.get_execution(execution_id)
        if not execution:
            logger.warning(f"Execution {execution_id} not found")
            return False

        await self.session.delete(execution)
        await self.session.commit()

        logger.info(f"Deleted flow execution {execution_id}")

        return True

    async def delete_executions_by_flow(self, flow_id: Union[uuid.UUID, str]) -> int:
        """
        Delete all executions for a specific flow.

        Args:
            flow_id: ID of the flow

        Returns:
            Number of executions deleted
        """
        if isinstance(flow_id, str):
            flow_id = uuid.UUID(flow_id)

        logger.info(f"Deleting all executions for flow {flow_id}")

        executions = await self.get_executions_by_flow(flow_id)
        deleted_count = 0

        for execution in executions:
            if await self.delete_execution(execution.id):
                deleted_count += 1

        logger.info(f"Deleted {deleted_count} executions for flow {flow_id}")

        return deleted_count

    # Backward compatibility methods for FlowRunnerService
    async def get_node_executions(self, execution_id: int) -> List:
        """
        Get node executions for a flow execution.

        NOTE: Node-level tracking was never implemented. This returns an empty list
        for backward compatibility with FlowRunnerService.get_flow_execution().

        Individual task statuses are tracked in the 'taskstatus' table instead.
        """
        return []
