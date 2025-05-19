"""
Repository for execution history data access.

This module provides database operations for execution history models.
"""

from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import desc, func, delete
from sqlalchemy.exc import SQLAlchemyError

from src.models.execution_history import ExecutionHistory, TaskStatus, ErrorTrace
from src.db.session import async_session_factory


class ExecutionHistoryRepository:
    """Repository for execution history data access operations."""
    
    async def get_execution_history(
        self, 
        limit: int = 50, 
        offset: int = 0
    ) -> tuple[List[ExecutionHistory], int]:
        """
        Get paginated execution history.
        
        Args:
            limit: Maximum number of items to return
            offset: Number of items to skip
            
        Returns:
            Tuple of (list of Run objects, total count)
        """
        async with async_session_factory() as session:
            # Get total count
            count_stmt = select(func.count()).select_from(ExecutionHistory)
            total_count_result = await session.execute(count_stmt)
            total_count = total_count_result.scalar() or 0
            
            # Get paginated runs
            stmt = select(ExecutionHistory).order_by(ExecutionHistory.created_at.desc()).offset(offset).limit(limit)
            result = await session.execute(stmt)
            runs = result.scalars().all()
            
            return runs, total_count
    
    async def get_execution_by_id(self, execution_id: int) -> Optional[ExecutionHistory]:
        """
        Get a specific execution by ID.
        
        Args:
            execution_id: ID of the execution
            
        Returns:
            Run object if found, None otherwise
        """
        async with async_session_factory() as session:
            stmt = select(ExecutionHistory).where(ExecutionHistory.id == execution_id)
            result = await session.execute(stmt)
            return result.scalars().first()
    
    async def get_execution_by_job_id(self, job_id: str) -> Optional[ExecutionHistory]:
        """
        Get a specific execution by job_id.
        
        Args:
            job_id: Job ID of the execution
            
        Returns:
            Run object if found, None otherwise
        """
        async with async_session_factory() as session:
            stmt = select(ExecutionHistory).where(ExecutionHistory.job_id == job_id)
            result = await session.execute(stmt)
            return result.scalars().first()
    
    async def check_execution_exists(self, execution_id: int) -> bool:
        """
        Check if an execution exists.
        
        Args:
            execution_id: ID of the execution
            
        Returns:
            True if exists, False otherwise
        """
        async with async_session_factory() as session:
            stmt = select(func.count()).select_from(ExecutionHistory).where(ExecutionHistory.id == execution_id)
            result = await session.execute(stmt)
            count = result.scalar() or 0
            return count > 0
    
    async def delete_execution(self, execution_id: int) -> Optional[Dict[str, Any]]:
        """
        Delete a specific execution and its associated data.
        
        Args:
            execution_id: ID of the execution
            
        Returns:
            Dictionary with deletion counts or None if execution not found
        """
        async with async_session_factory() as session:
            try:
                # Get the run first to check existence and get job_id
                run = await self._get_execution_by_id_internal(session, execution_id)
                if not run:
                    return None
                
                job_id = run.job_id
                result = {}
                
                # Delete associated task statuses
                task_status_stmt = delete(TaskStatus).where(TaskStatus.job_id == job_id)
                task_status_result = await session.execute(task_status_stmt)
                result['task_status_count'] = task_status_result.rowcount
                
                # Delete associated error traces
                error_trace_stmt = delete(ErrorTrace).where(ErrorTrace.run_id == execution_id)
                error_trace_result = await session.execute(error_trace_stmt)
                result['error_trace_count'] = error_trace_result.rowcount
                
                # Delete the run
                run_stmt = delete(ExecutionHistory).where(ExecutionHistory.id == execution_id)
                await session.execute(run_stmt)
                
                await session.commit()
                
                return {
                    'execution_id': execution_id,
                    'job_id': job_id,
                    'task_status_count': result['task_status_count'],
                    'error_trace_count': result['error_trace_count']
                }
            except Exception as e:
                await session.rollback()
                raise e
    
    async def _get_execution_by_id_internal(self, session: AsyncSession, execution_id: int) -> Optional[ExecutionHistory]:
        """Internal method to get execution by ID using provided session."""
        stmt = select(ExecutionHistory).where(ExecutionHistory.id == execution_id)
        result = await session.execute(stmt)
        return result.scalars().first()
    
    async def delete_execution_by_job_id(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Delete a specific execution and its associated data by job_id.
        
        Args:
            job_id: Job ID of the execution
            
        Returns:
            Dictionary with deletion counts or None if execution not found
        """
        async with async_session_factory() as session:
            try:
                # Get the run first to check existence
                run = await self._get_execution_by_job_id_internal(session, job_id)
                if not run:
                    return None
                
                execution_id = run.id
                result = {}
                
                # Delete associated task statuses
                task_status_stmt = delete(TaskStatus).where(TaskStatus.job_id == job_id)
                task_status_result = await session.execute(task_status_stmt)
                result['task_status_count'] = task_status_result.rowcount
                
                # Delete associated error traces
                error_trace_stmt = delete(ErrorTrace).where(ErrorTrace.run_id == execution_id)
                error_trace_result = await session.execute(error_trace_stmt)
                result['error_trace_count'] = error_trace_result.rowcount
                
                # Delete the run
                run_stmt = delete(ExecutionHistory).where(ExecutionHistory.job_id == job_id)
                await session.execute(run_stmt)
                
                await session.commit()
                
                return {
                    'execution_id': execution_id,
                    'job_id': job_id,
                    'task_status_count': result['task_status_count'],
                    'error_trace_count': result['error_trace_count']
                }
            except Exception as e:
                await session.rollback()
                raise e
    
    async def _get_execution_by_job_id_internal(self, session: AsyncSession, job_id: str) -> Optional[ExecutionHistory]:
        """Internal method to get execution by job ID using provided session."""
        stmt = select(ExecutionHistory).where(ExecutionHistory.job_id == job_id)
        result = await session.execute(stmt)
        return result.scalars().first()
    
    async def delete_all_executions(self) -> Dict[str, int]:
        """
        Delete all executions and associated data.
        
        Returns:
            Dictionary with deletion counts
        """
        async with async_session_factory() as session:
            try:
                result = {}
                
                # Delete all task statuses
                task_status_stmt = delete(TaskStatus)
                task_status_result = await session.execute(task_status_stmt)
                result['task_status_count'] = task_status_result.rowcount
                
                # Delete all error traces
                error_trace_stmt = delete(ErrorTrace)
                error_trace_result = await session.execute(error_trace_stmt)
                result['error_trace_count'] = error_trace_result.rowcount
                
                # Delete all runs and count them
                count_stmt = select(func.count()).select_from(ExecutionHistory)
                count_result = await session.execute(count_stmt)
                run_count = count_result.scalar() or 0
                
                run_stmt = delete(ExecutionHistory)
                await session.execute(run_stmt)
                
                await session.commit()
                
                return {
                    'run_count': run_count,
                    'task_status_count': result['task_status_count'],
                    'error_trace_count': result['error_trace_count']
                }
            except Exception as e:
                await session.rollback()
                raise e


# Create a singleton instance
execution_history_repository = ExecutionHistoryRepository() 