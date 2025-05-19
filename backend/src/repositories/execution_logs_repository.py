"""
Repository for execution logs data access.

This module provides database operations for execution logs.
"""

from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import desc, func, delete, text
import logging
from datetime import datetime, timezone

from src.models.execution_logs import ExecutionLog
from src.db.session import async_session_factory
from src.core.logger import LoggerManager

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().system


class ExecutionLogsRepository:
    """Repository for execution logs data access operations."""
    
    def _normalize_timestamp(self, timestamp):
        """
        Convert timestamp to timezone-naive UTC datetime.
        
        Args:
            timestamp: The timestamp to normalize
            
        Returns:
            Timezone-naive UTC datetime
        """
        if timestamp is None:
            return None
            
        # If timestamp has timezone information
        if hasattr(timestamp, 'tzinfo') and timestamp.tzinfo is not None:
            # Convert to UTC and make it timezone-naive
            return timestamp.astimezone(timezone.utc).replace(tzinfo=None)
        
        # Already timezone-naive
        return timestamp
    
    async def create(self, session: AsyncSession, execution_id: str, content: str, timestamp=None) -> ExecutionLog:
        """
        Create a new execution log entry.
        
        Args:
            session: Database session
            execution_id: ID of the execution
            content: Log content text
            timestamp: Optional timestamp, will use current time if not provided
            
        Returns:
            Created ExecutionLog object
        """
        try:
            # Normalize the timestamp to timezone-naive UTC
            normalized_timestamp = self._normalize_timestamp(timestamp)
            
            # Create the log object
            log = ExecutionLog(
                execution_id=execution_id,
                content=content,
                timestamp=normalized_timestamp  # If None, the model default will be used
            )
            
            # Add it to the session
            session.add(log)
            
            # Commit the transaction
            await session.commit()
            
            # Refresh to get the ID
            await session.refresh(log)
            
            return log
        except Exception as e:
            logger.error(f"[ExecutionLogsRepository.create] Error creating log: {e}", exc_info=True)
            
            # Try to rollback if possible
            try:
                await session.rollback()
            except Exception as rollback_error:
                logger.error(f"[ExecutionLogsRepository.create] Rollback failed: {rollback_error}")
            
            # Re-raise to caller
            raise
    
    async def create_with_managed_session(self, execution_id: str, content: str, timestamp=None) -> ExecutionLog:
        """
        Create a new execution log entry with internal session management.
        
        Args:
            execution_id: ID of the execution
            content: Log content text
            timestamp: Optional timestamp, will use current time if not provided
            
        Returns:
            Created ExecutionLog object
        """
        # Create a new session
        try:
            async with async_session_factory() as session:
                # Use the session to create the log
                try:
                    # Normalize the timestamp to timezone-naive UTC
                    normalized_timestamp = self._normalize_timestamp(timestamp)
                    
                    result = await self.create(
                        session=session,
                        execution_id=execution_id,
                        content=content,
                        timestamp=normalized_timestamp
                    )
                    return result
                except Exception as create_error:
                    logger.error(f"[ExecutionLogsRepository.create_with_managed_session] Create method failed: {create_error}")
                    # Use direct SQL as fallback
                    try:
                        # Normalize timestamp before formatting for SQL
                        normalized_timestamp = self._normalize_timestamp(timestamp)
                        
                        # Format timestamp for SQL or use CURRENT_TIMESTAMP
                        timestamp_str = f"'{normalized_timestamp}'" if normalized_timestamp else "CURRENT_TIMESTAMP"
                        
                        # Escape single quotes in content
                        safe_content = content.replace("'", "''")
                        
                        # Execute direct SQL
                        query = text(f"INSERT INTO execution_logs (execution_id, content, timestamp) VALUES ('{execution_id}', '{safe_content}', {timestamp_str}) RETURNING id")
                        result = await session.execute(query)
                        log_id = result.scalar_one()
                        await session.commit()
                        
                        # Get the created log
                        query = select(ExecutionLog).where(ExecutionLog.id == log_id)
                        result = await session.execute(query)
                        log = result.scalars().first()
                        return log
                    except Exception as sql_error:
                        logger.error(f"[ExecutionLogsRepository.create_with_managed_session] Direct SQL insert failed: {sql_error}", exc_info=True)
                        raise sql_error
        except Exception as session_error:
            logger.error(f"[ExecutionLogsRepository.create_with_managed_session] Session error: {session_error}", exc_info=True)
            raise
    
    async def get_by_execution_id(
        self, 
        session: AsyncSession, 
        execution_id: str, 
        limit: int = 1000, 
        offset: int = 0,
        newest_first: bool = False
    ) -> List[ExecutionLog]:
        """
        Retrieve logs for a specific execution.
        
        Args:
            session: Database session
            execution_id: ID of the execution to fetch logs for
            limit: Maximum number of logs to return
            offset: Number of logs to skip
            newest_first: If True, return newest logs first
            
        Returns:
            List of ExecutionLog objects
        """
        query = select(ExecutionLog).where(
            ExecutionLog.execution_id == execution_id
        )
        
        if newest_first:
            query = query.order_by(desc(ExecutionLog.timestamp))
        else:
            query = query.order_by(ExecutionLog.timestamp)
            
        query = query.offset(offset).limit(limit)
        
        result = await session.execute(query)
        return result.scalars().all()
    
    async def get_by_id(self, session: AsyncSession, log_id: int) -> Optional[ExecutionLog]:
        """
        Retrieve a specific log by ID.
        
        Args:
            session: Database session
            log_id: ID of the log to retrieve
            
        Returns:
            ExecutionLog object if found, None otherwise
        """
        query = select(ExecutionLog).where(ExecutionLog.id == log_id)
        result = await session.execute(query)
        return result.scalars().first()
    
    async def delete_by_execution_id(self, session: AsyncSession, execution_id: str) -> int:
        """
        Delete all logs for a specific execution.
        
        Args:
            session: Database session
            execution_id: ID of the execution to delete logs for
            
        Returns:
            Number of deleted records
        """
        result = await session.execute(
            text(f"DELETE FROM execution_logs WHERE execution_id = '{execution_id}'")
        )
        await session.commit()
        return result.rowcount
    
    async def delete_all(self, session: AsyncSession) -> int:
        """
        Delete all execution logs.
        
        Args:
            session: Database session
            
        Returns:
            Number of deleted records
        """
        stmt = delete(ExecutionLog)
        result = await session.execute(stmt)
        await session.commit()
        return result.rowcount
    
    async def count_by_execution_id(self, session: AsyncSession, execution_id: str) -> int:
        """
        Count logs for a specific execution.
        
        Args:
            session: Database session
            execution_id: ID of the execution to count logs for
            
        Returns:
            Number of logs
        """
        result = await session.execute(
            text(f"SELECT COUNT(*) FROM execution_logs WHERE execution_id = '{execution_id}'")
        )
        return result.scalar_one()
    
    async def get_by_execution_id_with_managed_session(
        self, 
        execution_id: str, 
        limit: int = 1000, 
        offset: int = 0,
        newest_first: bool = False
    ) -> List[ExecutionLog]:
        """
        Retrieve logs for a specific execution with internal session management.
        
        Args:
            execution_id: ID of the execution to fetch logs for
            limit: Maximum number of logs to return
            offset: Number of logs to skip
            newest_first: If True, return newest logs first
            
        Returns:
            List of ExecutionLog objects
        """
        async with async_session_factory() as session:
            return await self.get_by_execution_id(
                session=session,
                execution_id=execution_id,
                limit=limit,
                offset=offset,
                newest_first=newest_first
            )
    
    async def count_by_execution_id_with_managed_session(self, execution_id: str) -> int:
        """
        Count logs for a specific execution with internal session management.
        
        Args:
            execution_id: ID of the execution to count logs for
            
        Returns:
            Number of logs
        """
        async with async_session_factory() as session:
            return await self.count_by_execution_id(session, execution_id)
    
    async def delete_by_execution_id_with_managed_session(self, execution_id: str) -> int:
        """
        Delete all logs for a specific execution with internal session management.
        
        Args:
            execution_id: ID of the execution to delete logs for
            
        Returns:
            Number of deleted records
        """
        async with async_session_factory() as session:
            return await self.delete_by_execution_id(session, execution_id)
            
    async def delete_all_with_managed_session(self) -> int:
        """
        Delete all execution logs with internal session management.
        
        Returns:
            Number of deleted records
        """
        async with async_session_factory() as session:
            return await self.delete_all(session)


# Create a singleton instance
execution_logs_repository = ExecutionLogsRepository() 