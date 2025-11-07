"""
Repository for flow execution and flow node execution operations.
"""
import logging
import uuid
from typing import List, Optional, Dict, Any, Union
from datetime import datetime, UTC
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from sqlalchemy import select, update

from src.models.flow_execution import FlowExecution, FlowNodeExecution
from src.schemas.flow_execution import (
    FlowExecutionCreate,
    FlowExecutionUpdate,
    FlowNodeExecutionCreate,
    FlowNodeExecutionUpdate,
    FlowExecutionStatus
)

logger = logging.getLogger(__name__)

class FlowExecutionRepository:
    """Repository for Flow execution data operations"""
    
    def __init__(self, session: AsyncSession):
        """Initialize with async session"""
        self.session = session
    
    async def create(self, flow_execution_data: FlowExecutionCreate) -> FlowExecution:
        """
        Create a new flow execution record.
        
        Args:
            flow_execution_data: Data for the new flow execution
            
        Returns:
            The created flow execution record
        """
        now = datetime.now(UTC)
        execution = FlowExecution(
            flow_id=flow_execution_data.flow_id,
            job_id=flow_execution_data.job_id,
            status=flow_execution_data.status,
            config=flow_execution_data.config or {},
            created_at=now,
            updated_at=now
        )
        
        self.session.add(execution)
        await self.session.commit()
        await self.session.refresh(execution)
        
        logger.info(f"Created flow execution with ID {execution.id} for flow {execution.flow_id}, job {execution.job_id}")
        return execution
    
    async def get(self, execution_id: int) -> Optional[FlowExecution]:
        """
        Get a flow execution by ID.
        
        Args:
            execution_id: ID of the flow execution to retrieve
            
        Returns:
            The flow execution record or None if not found
        """
        query = select(FlowExecution).where(FlowExecution.id == execution_id)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def get_by_job_id(self, job_id: str) -> Optional[FlowExecution]:
        """
        Get a flow execution by job ID.
        
        Args:
            job_id: Job ID of the flow execution to retrieve
            
        Returns:
            The flow execution record or None if not found
        """
        query = select(FlowExecution).where(FlowExecution.job_id == job_id)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def get_by_flow_id(self, flow_id: Union[uuid.UUID, str]) -> List[FlowExecution]:
        """
        Get all flow executions for a specific flow.
        
        Args:
            flow_id: ID of the flow
            
        Returns:
            List of flow execution records
        """
        # Convert string to UUID if needed
        if isinstance(flow_id, str):
            try:
                flow_id = uuid.UUID(flow_id)
            except ValueError:
                logger.error(f"Invalid UUID format for flow_id: {flow_id}")
                return []
                
        query = select(FlowExecution).where(FlowExecution.flow_id == flow_id)
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def update(self, execution_id: int, execution_data: FlowExecutionUpdate) -> Optional[FlowExecution]:
        """
        Update a flow execution.
        
        Args:
            execution_id: ID of the flow execution to update
            execution_data: Update data
            
        Returns:
            The updated flow execution record or None if not found
        """
        now = datetime.now(UTC)
        update_data = {"updated_at": now}
        
        if execution_data.status is not None:
            update_data["status"] = execution_data.status
            
            # If the status is terminal (COMPLETED or FAILED), set completed_at
            if execution_data.status in [FlowExecutionStatus.COMPLETED, FlowExecutionStatus.FAILED]:
                update_data["completed_at"] = now
        
        if execution_data.result is not None:
            update_data["result"] = execution_data.result
            
        if execution_data.error is not None:
            update_data["error"] = execution_data.error
        
        stmt = (
            update(FlowExecution)
            .where(FlowExecution.id == execution_id)
            .values(**update_data)
            .returning(FlowExecution)
        )
        
        result = await self.session.execute(stmt)
        await self.session.commit()
        
        updated = result.scalars().first()
        if updated:
            logger.info(f"Updated flow execution {execution_id} with status {execution_data.status}")
        
        return updated
        
    async def get_all(self) -> List[FlowExecution]:
        """
        Get all flow executions.
        
        Returns:
            List of all flow execution records
        """
        query = select(FlowExecution)
        result = await self.session.execute(query)
        return list(result.scalars().all())


class FlowNodeExecutionRepository:
    """Repository for Flow node execution data operations"""
    
    def __init__(self, session: AsyncSession):
        """Initialize with async session"""
        self.session = session
    
    async def create(self, node_execution_data: FlowNodeExecutionCreate) -> FlowNodeExecution:
        """
        Create a new flow node execution record.
        
        Args:
            node_execution_data: Data for the new flow node execution
            
        Returns:
            The created flow node execution record
        """
        now = datetime.now(UTC)
        node_execution = FlowNodeExecution(
            flow_execution_id=node_execution_data.flow_execution_id,
            node_id=node_execution_data.node_id,
            status=node_execution_data.status,
            agent_id=node_execution_data.agent_id,
            task_id=node_execution_data.task_id,
            created_at=now,
            updated_at=now
        )
        
        self.session.add(node_execution)
        await self.session.commit()
        await self.session.refresh(node_execution)
        
        logger.info(f"Created node execution with ID {node_execution.id} for node {node_execution.node_id}")
        return node_execution
    
    async def get(self, node_execution_id: int) -> Optional[FlowNodeExecution]:
        """
        Get a flow node execution by ID.
        
        Args:
            node_execution_id: ID of the node execution to retrieve
            
        Returns:
            The flow node execution record or None if not found
        """
        query = select(FlowNodeExecution).where(FlowNodeExecution.id == node_execution_id)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def get_by_flow_execution(self, flow_execution_id: int) -> List[FlowNodeExecution]:
        """
        Get all node executions for a specific flow execution.
        
        Args:
            flow_execution_id: ID of the flow execution
            
        Returns:
            List of flow node execution records
        """
        query = select(FlowNodeExecution).where(FlowNodeExecution.flow_execution_id == flow_execution_id)
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def update(self, node_execution_id: int, node_execution_data: FlowNodeExecutionUpdate) -> Optional[FlowNodeExecution]:
        """
        Update a flow node execution.
        
        Args:
            node_execution_id: ID of the node execution to update
            node_execution_data: Update data
            
        Returns:
            The updated flow node execution record or None if not found
        """
        now = datetime.now(UTC)
        update_data = {"updated_at": now}
        
        if node_execution_data.status is not None:
            update_data["status"] = node_execution_data.status
            
            # If the status is terminal (COMPLETED or FAILED), set completed_at
            if node_execution_data.status in [FlowExecutionStatus.COMPLETED, FlowExecutionStatus.FAILED]:
                update_data["completed_at"] = now
        
        if node_execution_data.result is not None:
            update_data["result"] = node_execution_data.result
            
        if node_execution_data.error is not None:
            update_data["error"] = node_execution_data.error
        
        stmt = (
            update(FlowNodeExecution)
            .where(FlowNodeExecution.id == node_execution_id)
            .values(**update_data)
            .returning(FlowNodeExecution)
        )
        
        result = await self.session.execute(stmt)
        await self.session.commit()
        
        updated = result.scalars().first()
        if updated:
            logger.info(f"Updated node execution {node_execution_id} with status {node_execution_data.status}")
        
        return updated
        
# Synchronous versions for use with the crew execution code

class SyncFlowExecutionRepository:
    """Repository for synchronous Flow execution data operations"""
    
    def __init__(self, db: Session):
        """Initialize with synchronous session"""
        self.db = db
    
    def create(self, flow_execution_data: FlowExecutionCreate) -> FlowExecution:
        """
        Create a new flow execution record.
        
        Args:
            flow_execution_data: Data for the new flow execution
            
        Returns:
            The created flow execution record
        """
        now = datetime.now(UTC)
        execution = FlowExecution(
            flow_id=flow_execution_data.flow_id,
            job_id=flow_execution_data.job_id,
            status=flow_execution_data.status,
            config=flow_execution_data.config or {},
            created_at=now,
            updated_at=now
        )
        
        self.db.add(execution)
        self.db.commit()
        self.db.refresh(execution)
        
        logger.info(f"Created flow execution with ID {execution.id} for flow {execution.flow_id}, job {execution.job_id}")
        return execution
    
    def get(self, execution_id: int) -> Optional[FlowExecution]:
        """
        Get a flow execution by ID.
        
        Args:
            execution_id: ID of the flow execution to retrieve
            
        Returns:
            The flow execution record or None if not found
        """
        return self.db.query(FlowExecution).filter(FlowExecution.id == execution_id).first()
    
    def get_by_job_id(self, job_id: str) -> Optional[FlowExecution]:
        """
        Get a flow execution by job ID.
        
        Args:
            job_id: Job ID of the flow execution to retrieve
            
        Returns:
            The flow execution record or None if not found
        """
        return self.db.query(FlowExecution).filter(FlowExecution.job_id == job_id).first()
    
    def get_by_flow_id(self, flow_id: Union[uuid.UUID, str]) -> List[FlowExecution]:
        """
        Get all flow executions for a specific flow.
        
        Args:
            flow_id: ID of the flow
            
        Returns:
            List of flow execution records
        """
        # Convert string to UUID if needed
        if isinstance(flow_id, str):
            try:
                flow_id = uuid.UUID(flow_id)
            except ValueError:
                logger.error(f"Invalid UUID format for flow_id: {flow_id}")
                return []
                
        return self.db.query(FlowExecution).filter(FlowExecution.flow_id == flow_id).all()
    
    def update(self, execution_id: int, execution_data: FlowExecutionUpdate) -> Optional[FlowExecution]:
        """
        Update a flow execution.
        
        Args:
            execution_id: ID of the flow execution to update
            execution_data: Update data
            
        Returns:
            The updated flow execution record or None if not found
        """
        execution = self.db.query(FlowExecution).filter(FlowExecution.id == execution_id).first()
        if not execution:
            return None
            
        now = datetime.now(UTC)
        execution.updated_at = now
        
        if execution_data.status is not None:
            execution.status = execution_data.status
            
            # If the status is terminal (COMPLETED or FAILED), set completed_at
            if execution_data.status in [FlowExecutionStatus.COMPLETED, FlowExecutionStatus.FAILED]:
                execution.completed_at = now
        
        if execution_data.result is not None:
            execution.result = execution_data.result
            
        if execution_data.error is not None:
            execution.error = execution_data.error
        
        self.db.commit()
        self.db.refresh(execution)
        
        logger.info(f"Updated flow execution {execution_id} with status {execution_data.status}")
        return execution


class SyncFlowNodeExecutionRepository:
    """Repository for synchronous Flow node execution data operations"""
    
    def __init__(self, db: Session):
        """Initialize with synchronous session"""
        self.db = db
    
    def create(self, node_execution_data: FlowNodeExecutionCreate) -> FlowNodeExecution:
        """
        Create a new flow node execution record.
        
        Args:
            node_execution_data: Data for the new flow node execution
            
        Returns:
            The created flow node execution record
        """
        now = datetime.now(UTC)
        node_execution = FlowNodeExecution(
            flow_execution_id=node_execution_data.flow_execution_id,
            node_id=node_execution_data.node_id,
            status=node_execution_data.status,
            agent_id=node_execution_data.agent_id,
            task_id=node_execution_data.task_id,
            created_at=now,
            updated_at=now
        )
        
        self.db.add(node_execution)
        self.db.commit()
        self.db.refresh(node_execution)
        
        logger.info(f"Created node execution with ID {node_execution.id} for node {node_execution.node_id}")
        return node_execution
    
    def get(self, node_execution_id: int) -> Optional[FlowNodeExecution]:
        """
        Get a flow node execution by ID.
        
        Args:
            node_execution_id: ID of the node execution to retrieve
            
        Returns:
            The flow node execution record or None if not found
        """
        return self.db.query(FlowNodeExecution).filter(FlowNodeExecution.id == node_execution_id).first()
    
    def get_by_flow_execution(self, flow_execution_id: int) -> List[FlowNodeExecution]:
        """
        Get all node executions for a specific flow execution.
        
        Args:
            flow_execution_id: ID of the flow execution
            
        Returns:
            List of flow node execution records
        """
        return self.db.query(FlowNodeExecution).filter(FlowNodeExecution.flow_execution_id == flow_execution_id).all()
    
    def update(self, node_execution_id: int, node_execution_data: FlowNodeExecutionUpdate) -> Optional[FlowNodeExecution]:
        """
        Update a flow node execution.
        
        Args:
            node_execution_id: ID of the node execution to update
            node_execution_data: Update data
            
        Returns:
            The updated flow node execution record or None if not found
        """
        node_execution = self.db.query(FlowNodeExecution).filter(FlowNodeExecution.id == node_execution_id).first()
        if not node_execution:
            return None
            
        now = datetime.now(UTC)
        node_execution.updated_at = now
        
        if node_execution_data.status is not None:
            node_execution.status = node_execution_data.status
            
            # If the status is terminal (COMPLETED or FAILED), set completed_at
            if node_execution_data.status in [FlowExecutionStatus.COMPLETED, FlowExecutionStatus.FAILED]:
                node_execution.completed_at = now
        
        if node_execution_data.result is not None:
            node_execution.result = node_execution_data.result
            
        if node_execution_data.error is not None:
            node_execution.error = node_execution_data.error
        
        self.db.commit()
        self.db.refresh(node_execution)
        
        logger.info(f"Updated node execution {node_execution_id} with status {node_execution_data.status}")
        return node_execution 