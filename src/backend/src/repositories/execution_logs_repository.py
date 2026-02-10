"""
Repository for execution logs data access.

This module provides database operations for execution logs.
"""

from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import desc, func, delete
import logging
from datetime import datetime, timezone

from src.models.execution_logs import ExecutionLog
from src.core.logger import LoggerManager

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().system


class ExecutionLogsRepository:
    """Repository for execution logs data access operations."""

    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session injection.

        Args:
            session: Database session for dependency injection (required)
        """
        self.session = session

    async def create_log(self, execution_id: str, content: str, timestamp=None, group_id: str = None, group_email: str = None) -> ExecutionLog:
        """
        Create a new execution log entry using injected session.

        Args:
            execution_id: ID of the execution
            content: Log content text
            timestamp: Optional timestamp, will use current time if not provided
            group_id: Optional group ID for multi-tenant isolation
            group_email: Optional user email for audit trail

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
                timestamp=normalized_timestamp,  # If None, the model default will be used
                group_id=group_id,
                group_email=group_email,
            )

            # Add it to the session
            self.session.add(log)

            # Just flush, don't commit - let the session manager handle commits
            await self.session.flush()

            return log
        except Exception as e:
            logger.error(f"[ExecutionLogsRepository.create_log] Error creating log: {e}", exc_info=True)

            # Try to rollback if possible
            try:
                await self.session.rollback()
            except Exception as rollback_error:
                logger.error(f"[ExecutionLogsRepository.create_log] Rollback failed: {rollback_error}")

            # Re-raise to caller
            raise

    async def get_logs_by_execution_id(
        self,
        execution_id: str,
        limit: int = 1000,
        offset: int = 0,
        newest_first: bool = False,
        group_ids: list = None
    ) -> List[ExecutionLog]:
        """
        Retrieve logs for a specific execution using injected session.

        Args:
            execution_id: ID of the execution to fetch logs for
            limit: Maximum number of logs to return
            offset: Number of logs to skip
            newest_first: If True, return newest logs first
            group_ids: Optional list of group IDs for tenant isolation

        Returns:
            List of ExecutionLog objects
        """
        query = select(ExecutionLog).where(
            ExecutionLog.execution_id == execution_id
        )
        if group_ids:
            query = query.where(ExecutionLog.group_id.in_(group_ids))

        if newest_first:
            query = query.order_by(desc(ExecutionLog.timestamp))
        else:
            query = query.order_by(ExecutionLog.timestamp)

        query = query.offset(offset).limit(limit)

        result = await self.session.execute(query)
        return result.scalars().all()

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

        try:
            # If it's already a datetime object (aware or naive)
            if isinstance(timestamp, datetime):
                dt = timestamp
            # If it's a string, try to parse ISO 8601; if it fails, ignore
            elif isinstance(timestamp, str):
                try:
                    dt = datetime.fromisoformat(timestamp)
                except Exception:
                    return None
            else:
                return None

            # Normalize to timezone-naive UTC
            if getattr(dt, 'tzinfo', None) is not None:
                return dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt
        except Exception:
            # On any unexpected input, fall back to None so model default applies
            return None
    
    
    
    
    async def get_by_id(self, log_id: int) -> Optional[ExecutionLog]:
        """
        Retrieve a specific log by ID using injected session.

        Args:
            log_id: ID of the log to retrieve

        Returns:
            ExecutionLog object if found, None otherwise
        """
        query = select(ExecutionLog).where(ExecutionLog.id == log_id)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def delete_by_execution_id(self, execution_id: str, group_ids: list = None) -> int:
        """
        Delete all logs for a specific execution using injected session.

        Args:
            execution_id: ID of the execution to delete logs for
            group_ids: Optional list of group IDs for tenant isolation

        Returns:
            Number of deleted records
        """
        stmt = delete(ExecutionLog).where(ExecutionLog.execution_id == execution_id)
        if group_ids:
            stmt = stmt.where(ExecutionLog.group_id.in_(group_ids))
        result = await self.session.execute(stmt)
        # Don't commit here - let the service/router manage transactions
        await self.session.flush()
        return result.rowcount
    
    async def delete_all(self) -> int:
        """
        Delete all execution logs using injected session.

        Returns:
            Number of deleted records
        """
        stmt = delete(ExecutionLog)
        result = await self.session.execute(stmt)
        # Don't commit here - let the service/router manage transactions
        await self.session.flush()
        return result.rowcount
    
    async def count_by_execution_id(self, execution_id: str, group_ids: list = None) -> int:
        """
        Count logs for a specific execution using injected session.

        Args:
            execution_id: ID of the execution to count logs for
            group_ids: Optional list of group IDs for tenant isolation

        Returns:
            Number of logs
        """
        stmt = select(func.count()).select_from(ExecutionLog).where(
            ExecutionLog.execution_id == execution_id
        )
        if group_ids:
            stmt = stmt.where(ExecutionLog.group_id.in_(group_ids))
        result = await self.session.execute(stmt)
        return result.scalar_one()
    
    
    
    
            
    
 