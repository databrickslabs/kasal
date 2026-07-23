"""
Repository for handling job and task output combination.

This module provides functions for retrieving run information needed by the OutputCombinerCallback.
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from src.models.execution_history import ExecutionHistory
from src.core.logger import LoggerManager

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().system

class OutputCombinerRepository:
    """Repository for handling job and task output combination data access."""
    
    def __init__(self, db: Session):
        """
        Initialize the repository with database session.
        
        Args:
            db: SQLAlchemy session
        """
        self.db = db
    
    def get_run_by_job_id(self, job_id: str) -> Optional[ExecutionHistory]:
        """
        Get a run record by its job ID.
        
        Args:
            job_id: The job identifier
            
        Returns:
            Run record if found, None otherwise
        """
        try:
            return self.db.query(ExecutionHistory).filter(ExecutionHistory.job_id == job_id).first()
        except SQLAlchemyError as e:
            logger.error(f"Database error retrieving run with job_id {job_id}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Error retrieving run with job_id {job_id}: {str(e)}")
            return None

# Global instance for convenience
output_combiner_repository = OutputCombinerRepository(None)

def get_output_combiner_repository(db: Session) -> OutputCombinerRepository:
    """
    Get a configured output combiner repository.
    
    Args:
        db: SQLAlchemy session
        
    Returns:
        Configured OutputCombinerRepository instance
    """
    output_combiner_repository.db = db
    return output_combiner_repository 