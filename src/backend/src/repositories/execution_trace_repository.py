"""
Repository for execution trace operations.

This module provides functions for CRUD operations on execution traces.
"""

import logging
from typing import List, Optional, Dict, Any, Tuple
from sqlalchemy import select, delete, update, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.exc import SQLAlchemyError

from src.models.execution_trace import ExecutionTrace
from src.models.execution_history import ExecutionHistory
from src.core.logger import LoggerManager
from src.core.base_repository import BaseRepository

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().system

class ExecutionTraceRepository(BaseRepository[ExecutionTrace]):
    """Repository class for handling ExecutionTrace database operations."""

    def __init__(self, session: AsyncSession):
        """
        Initialize repository with session.

        Args:
            session: Database session from FastAPI DI
        """
        super().__init__(ExecutionTrace, session)
        self.session = session

    # Methods that require an existing session (primarily for internal use)
    
    async def _create(self, trace_data: Dict[str, Any]) -> ExecutionTrace:
        """
        Create a new execution trace record.

        Args:
            trace_data: Dictionary with trace data
            
        Returns:
            Created ExecutionTrace record
        """
        try:
            trace = ExecutionTrace(**trace_data)
            self.session.add(trace)
            # Flush to assign primary key before commit (important for some backends)
            await self.session.flush()
            # Capture id early in case refresh fails post-commit
            _trace_id = getattr(trace, 'id', None)
            await self.session.commit()
            # Best-effort refresh; not strictly needed with expire_on_commit=False
            try:
                if getattr(trace, 'id', None) is None and _trace_id is not None:
                    # If PK wasn’t populated, set it from pre-commit value
                    trace.id = _trace_id
                else:
                    await self.session.refresh(trace)
            except Exception as refresh_err:
                logger.debug(f"Refresh after trace insert failed (non-fatal): {refresh_err}")
            return trace
        except SQLAlchemyError as e:
            await self.session.rollback()
            logger.error(f"Database error creating execution trace: {str(e)}")
            raise
    
    async def _get_by_id(self, trace_id: int) -> Optional[ExecutionTrace]:
        """
        Get an execution trace by ID.

        Args:
            trace_id: ID of the trace to retrieve
            
        Returns:
            ExecutionTrace if found, None otherwise
        """
        try:
            stmt = select(ExecutionTrace).where(ExecutionTrace.id == trace_id)
            result = await self.session.execute(stmt)
            return result.scalars().first()
        except SQLAlchemyError as e:
            logger.error(f"Database error retrieving execution trace {trace_id}: {str(e)}")
            raise
    
    async def _get_by_run_id(
        self,
        run_id: int,
        limit: Optional[int] = None,
        offset: Optional[int] = 0
    ) -> List[ExecutionTrace]:
        """
        Get execution traces by run_id.

        Args:
            run_id: Run ID to filter by
            limit: Maximum number of traces to return
            offset: Number of traces to skip
            
        Returns:
            List of ExecutionTrace records
        """
        try:
            stmt = select(ExecutionTrace).where(ExecutionTrace.run_id == run_id)
            
            if offset is not None:
                stmt = stmt.offset(offset)
            if limit is not None:
                stmt = stmt.limit(limit)
                
            result = await self.session.execute(stmt)
            return result.scalars().all()
        except SQLAlchemyError as e:
            logger.error(f"Database error retrieving traces for run_id {run_id}: {str(e)}")
            raise
    
    async def _get_by_job_id(
        self,
        job_id: str,
        limit: Optional[int] = None,
        offset: Optional[int] = 0
    ) -> List[ExecutionTrace]:
        """
        Get execution traces by job_id.

        Args:
            job_id: Job ID to filter by
            limit: Maximum number of traces to return
            offset: Number of traces to skip
            
        Returns:
            List of ExecutionTrace records
        """
        try:
            stmt = select(ExecutionTrace).where(ExecutionTrace.job_id == job_id)
            
            if offset is not None:
                stmt = stmt.offset(offset)
            if limit is not None:
                stmt = stmt.limit(limit)
                
            result = await self.session.execute(stmt)
            return result.scalars().all()
        except SQLAlchemyError as e:
            logger.error(f"Database error retrieving traces for job_id {job_id}: {str(e)}")
            raise
    
    async def _get_all_traces(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = 0
    ) -> Tuple[List[ExecutionTrace], int]:
        """
        Get all execution traces with pagination.

        Args:
            limit: Maximum number of traces to return
            offset: Number of traces to skip
            
        Returns:
            Tuple of (list of ExecutionTrace records, total count)
        """
        try:
            # Get all traces
            stmt = select(ExecutionTrace).order_by(ExecutionTrace.created_at.desc())
            
            if offset is not None:
                stmt = stmt.offset(offset)
            if limit is not None:
                stmt = stmt.limit(limit)
                
            result = await self.session.execute(stmt)
            traces = result.scalars().all()
            
            # Get total count
            count_stmt = select(func.count()).select_from(ExecutionTrace)
            total_count_result = await self.session.execute(count_stmt)
            total_count = total_count_result.scalar() or 0
            
            return traces, total_count
        except SQLAlchemyError as e:
            logger.error(f"Database error retrieving all traces: {str(e)}")
            raise
    
    async def _get_execution_job_id_by_run_id(
        self,
        run_id: int
    ) -> Optional[str]:
        """
        Get job_id for an execution by run_id.

        Args:
            run_id: Run ID to look up
            
        Returns:
            job_id if found, None otherwise
        """
        try:
            stmt = select(ExecutionHistory.job_id).where(ExecutionHistory.id == run_id)
            result = await self.session.execute(stmt)
            return result.scalar()
        except SQLAlchemyError as e:
            logger.error(f"Database error retrieving job_id for run_id {run_id}: {str(e)}")
            raise
    
    async def _get_execution_run_id_by_job_id(
        self,
        job_id: str
    ) -> Optional[int]:
        """
        Get run_id for an execution by job_id.

        Args:
            job_id: Job ID to look up
            
        Returns:
            run_id if found, None otherwise
        """
        try:
            stmt = select(ExecutionHistory.id).where(ExecutionHistory.job_id == job_id)
            result = await self.session.execute(stmt)
            return result.scalar()
        except SQLAlchemyError as e:
            logger.error(f"Database error retrieving run_id for job_id {job_id}: {str(e)}")
            raise
    
    async def _delete_by_id(self, trace_id: int) -> int:
        """
        Delete an execution trace by ID.

        Args:
            trace_id: ID of the trace to delete
            
        Returns:
            Number of deleted records (0 or 1)
        """
        try:
            stmt = delete(ExecutionTrace).where(ExecutionTrace.id == trace_id)
            result = await self.session.execute(stmt)
            await self.session.commit()
            return result.rowcount
        except SQLAlchemyError as e:
            await self.session.rollback()
            logger.error(f"Database error deleting trace {trace_id}: {str(e)}")
            raise
    
    async def _delete_by_run_id(self, run_id: int) -> int:
        """
        Delete all execution traces by run_id.

        Args:
            run_id: Run ID to filter by
            
        Returns:
            Number of deleted records
        """
        try:
            stmt = delete(ExecutionTrace).where(ExecutionTrace.run_id == run_id)
            result = await self.session.execute(stmt)
            await self.session.commit()
            return result.rowcount
        except SQLAlchemyError as e:
            await self.session.rollback()
            logger.error(f"Database error deleting traces for run_id {run_id}: {str(e)}")
            raise
    
    async def _delete_by_job_id(self, job_id: str) -> int:
        """
        Delete all execution traces by job_id.

        Args:
            job_id: Job ID to filter by
            
        Returns:
            Number of deleted records
        """
        try:
            stmt = delete(ExecutionTrace).where(ExecutionTrace.job_id == job_id)
            result = await self.session.execute(stmt)
            await self.session.commit()
            return result.rowcount
        except SQLAlchemyError as e:
            await self.session.rollback()
            logger.error(f"Database error deleting traces for job_id {job_id}: {str(e)}")
            raise
    
    async def _delete_all(self) -> int:
        """
        Delete all execution traces.

        Returns:
            Number of deleted records
        """
        try:
            stmt = delete(ExecutionTrace)
            result = await self.session.execute(stmt)
            await self.session.commit()
            return result.rowcount
        except SQLAlchemyError as e:
            await self.session.rollback()
            logger.error(f"Database error deleting all traces: {str(e)}")
            raise
    
    # Public methods that manage their own session lifecycle
    
    async def create(self, trace_data: Dict[str, Any]) -> ExecutionTrace:
        """
        Create a new execution trace record.

        Args:
            trace_data: Dictionary with trace data

        Returns:
            Created ExecutionTrace record

        Raises:
            ValueError: If job_id is provided but doesn't exist in ExecutionHistory
        """
        # Check if the job exists first
        job_id = trace_data.get("job_id")
        if job_id:
            # Check if job exists in executionhistory
            stmt = select(ExecutionHistory).where(ExecutionHistory.job_id == job_id)
            result = await self.session.execute(stmt)
            job_exists = result.scalars().first()

            # If job doesn't exist, raise an error instead of creating orphan records
            if not job_exists:
                logger.warning(f"Attempt to create trace for non-existent job {job_id}")
                raise ValueError(f"Job {job_id} does not exist in ExecutionHistory. Trace creation aborted.")
            else:
                # Job exists, ensure run_id is set in trace_data
                if "run_id" not in trace_data and job_exists:
                    trace_data["run_id"] = job_exists.id
                    logger.info(f"Setting run_id={job_exists.id} for existing job {job_id}")

        # Create the trace with the existing job
        return await self._create(trace_data)
    
    async def get_by_id(self, trace_id: int) -> Optional[ExecutionTrace]:
        """
        Get an execution trace by ID.
        
        Args:
            trace_id: ID of the trace to retrieve
            
        Returns:
            ExecutionTrace if found, None otherwise
        """
        return await self._get_by_id(trace_id)
    
    async def get_by_run_id(
        self, 
        run_id: int,
        limit: Optional[int] = None,
        offset: Optional[int] = 0
    ) -> List[ExecutionTrace]:
        """
        Get execution traces by run_id.
        
        Args:
            run_id: Run ID to filter by
            limit: Maximum number of traces to return
            offset: Number of traces to skip
            
        Returns:
            List of ExecutionTrace records
        """
        return await self._get_by_run_id(run_id, limit, offset)
    
    async def get_by_job_id(
        self, 
        job_id: str,
        limit: Optional[int] = None,
        offset: Optional[int] = 0
    ) -> List[ExecutionTrace]:
        """
        Get execution traces by job_id.
        
        Args:
            job_id: Job ID to filter by
            limit: Maximum number of traces to return
            offset: Number of traces to skip
            
        Returns:
            List of ExecutionTrace records
        """
        return await self._get_by_job_id(job_id, limit, offset)
    
    async def get_all_traces(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = 0
    ) -> Tuple[List[ExecutionTrace], int]:
        """
        Get all execution traces with pagination.
        
        Args:
            limit: Maximum number of traces to return
            offset: Number of traces to skip
            
        Returns:
            Tuple of (list of ExecutionTrace records, total count)
        """
        return await self._get_all_traces(limit, offset)
    
    async def get_execution_job_id_by_run_id(self, run_id: int) -> Optional[str]:
        """
        Get job_id for an execution by run_id.
        
        Args:
            run_id: Run ID to look up
            
        Returns:
            job_id if found, None otherwise
        """
        return await self._get_execution_job_id_by_run_id(run_id)
    
    async def get_execution_run_id_by_job_id(self, job_id: str) -> Optional[int]:
        """
        Get run_id for an execution by job_id.
        
        Args:
            job_id: Job ID to look up
            
        Returns:
            run_id if found, None otherwise
        """
        return await self._get_execution_run_id_by_job_id(job_id)
    
    async def delete_by_id(self, trace_id: int) -> int:
        """
        Delete an execution trace by ID.
        
        Args:
            trace_id: ID of the trace to delete
            
        Returns:
            Number of deleted records (0 or 1)
        """
        return await self._delete_by_id(trace_id)
    
    async def delete_by_run_id(self, run_id: int) -> int:
        """
        Delete all execution traces by run_id.
        
        Args:
            run_id: Run ID to filter by
            
        Returns:
            Number of deleted records
        """
        return await self._delete_by_run_id(run_id)
    
    async def delete_by_job_id(self, job_id: str) -> int:
        """
        Delete all execution traces by job_id.
        
        Args:
            job_id: Job ID to filter by
            
        Returns:
            Number of deleted records
        """
        return await self._delete_by_job_id(job_id)
    
    async def delete_all(self) -> int:
        """
        Delete all execution traces.
        
        Returns:
            Number of deleted records
        """
        return await self._delete_all() 