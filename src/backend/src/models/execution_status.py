"""
Execution Status Models.

This module defines the execution status enum used by the execution engine.
This is the single source of truth for all execution status values across the application.
"""

from enum import Enum

class ExecutionStatus(str, Enum):
    """
    Execution status enum.
    
    This enum defines the possible states of an execution.
    Use this as the single source of truth for all status values.
    """
    PENDING = "PENDING"
    PREPARING = "PREPARING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED" 