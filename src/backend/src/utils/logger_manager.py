"""
Re-export LoggerManager from src.core.logger for backwards compatibility.

This module provides a convenience import path for LoggerManager.
The actual implementation is in src.core.logger.
"""

from src.core.logger import LoggerManager

__all__ = ["LoggerManager"]
