"""
Base engine package for Kasal.

This package provides base interfaces for engine implementations.
"""

from src.engines.base.base_engine_service import BaseEngineService
from src.engines.base.base_tool_registry import BaseToolRegistry

__all__ = ['BaseEngineService', 'BaseToolRegistry']
