"""
Repository for agent trace operations.

This module provides functions for recording and retrieving agent execution traces.
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime, UTC
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from src.models.execution_history import ExecutionHistory
from src.models.execution_trace import ExecutionTrace
from src.core.logger import LoggerManager
from src.db.session import SessionLocal

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().system

class AgentTraceRepository:
    """Repository class for handling agent trace operations."""
    
    def __init__(self, db: Optional[Session] = None):
        """
        Initialize the repository with database session.
        
        Args:
            db: SQLAlchemy session
        """
        self.db = db
    
    async def create_trace(self, 
                        job_id: str, 
                        agent_name: str, 
                        task_name: str, 
                        event_type: str,
                        content: str,
                        timestamp: Optional[str] = None,
                        trace_metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Asynchronously create an execution trace record in the database.
        
        Args:
            job_id: Execution job ID
            agent_name: Name of the agent
            task_name: Name of the task being executed
            event_type: Type of trace event (e.g., agent_step, tool_start)
            content: Content of the trace (output, input, etc.)
            timestamp: ISO formatted timestamp or None to use current time
            trace_metadata: Additional metadata to store with the trace
            
        Returns:
            ID of the created trace record
        """
        logger.info(f"Creating trace for job_id: {job_id}, event: {event_type}, agent: {agent_name}")
        
        try:
            if not isinstance(self.db, AsyncSession):
                raise ValueError(f"Database session must be AsyncSession, got {type(self.db)}")
                
            # Find the run record for this job_id (async version)
            from sqlalchemy import select
            query = select(ExecutionHistory).where(ExecutionHistory.job_id == job_id)
            result = await self.db.execute(query)
            db_run = result.scalars().first()
                
            if not db_run:
                logger.error(f"No run record found for job_id: {job_id}")
                raise ValueError(f"No execution record found for job_id: {job_id}")
                
            # Parse timestamp or use current time
            if timestamp:
                try:
                    created_at = datetime.fromisoformat(timestamp)
                except ValueError:
                    logger.warning(f"Invalid timestamp format: {timestamp}, using current time")
                    created_at = datetime.now(UTC)
            else:
                created_at = datetime.now(UTC)
                
            # Create trace record
            trace = ExecutionTrace(
                run_id=db_run.id,
                job_id=job_id,
                agent_name=agent_name,
                task_name=task_name,
                event_type=event_type,
                output=content,
                trace_metadata=trace_metadata or {},
                created_at=created_at
            )
                
            self.db.add(trace)
            # We don't commit here - caller should commit the session
            
            logger.info(f"Successfully created trace record for job_id: {job_id}, event: {event_type}")
            return trace.id
            
        except Exception as e:
            logger.error(f"Error creating trace record: {str(e)}", exc_info=True)
            raise
    
    def record_trace(self, job_id: str, agent_name: str, task_name: str, output_content: Any) -> Optional[ExecutionTrace]:
        """
        Record an agent trace to the database.
        
        Args:
            job_id: The job identifier
            agent_name: Name of the agent
            task_name: Name of the task
            output_content: Output content from the agent
            
        Returns:
            Created ExecutionTrace record if successful, None otherwise
        """
        try:
            # If no db is provided, create a new session
            if self.db is None:
                logger.info(f"No db session provided, creating a new one for job_id: {job_id}")
                db = SessionLocal()
                close_session = True
            else:
                db = self.db
                close_session = False
                
            try:
                # Find the run record for this job_id
                db_run = db.query(ExecutionHistory).filter(ExecutionHistory.job_id == job_id).first()
                
                if not db_run:
                    logger.error(f"No run record found for job_id: {job_id}")
                    return None
                    
                # Create trace record
                trace = ExecutionTrace(
                    run_id=db_run.id,
                    job_id=job_id,
                    agent_name=agent_name,
                    task_name=task_name,
                    output=output_content,
                    created_at=datetime.now(UTC)
                )
                
                db.add(trace)
                db.commit()
                db.refresh(trace)
                
                logger.info(f"Successfully created trace for job_id: {job_id}, agent: {agent_name}, task: {task_name}")
                return trace
            except SQLAlchemyError as e:
                db.rollback()
                logger.error(f"Database error creating agent trace: {str(e)}")
                return None
            finally:
                # Close the session if we created it
                if close_session:
                    db.close()
                    
        except Exception as e:
            logger.error(f"Error recording agent trace: {str(e)}")
            return None

# Global instance for convenience 
agent_trace_repository = AgentTraceRepository(None)

def get_agent_trace_repository(db: Optional[Session] = None) -> AgentTraceRepository:
    """
    Get a configured agent trace repository.
    
    Args:
        db: SQLAlchemy session
        
    Returns:
        Configured AgentTraceRepository instance
    """
    if db is not None:
        agent_trace_repository.db = db
    return agent_trace_repository 