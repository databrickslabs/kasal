"""
Base engine service abstract class.

This module defines the base interface for all AI execution engines.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List

class BaseEngineService(ABC):
    """Abstract base class for all AI execution engines"""
    
    @abstractmethod
    async def initialize(self, **kwargs) -> None:
        """
        Initialize the engine with configuration
        
        Args:
            **kwargs: Engine-specific configuration parameters
        """
        pass
    
    @abstractmethod
    async def run_execution(self, execution_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run an execution with the engine
        
        Args:
            execution_id: ID of the execution
            config: Configuration for the execution
            
        Returns:
            Dictionary with execution results
        """
        pass
    
    
    @abstractmethod
    async def get_execution_status(self, execution_id: str) -> Dict[str, Any]:
        """
        Get status of an execution
        
        Args:
            execution_id: ID of the execution
            
        Returns:
            Dictionary with execution status
        """
        pass
    
    @abstractmethod
    async def cancel_execution(self, execution_id: str) -> bool:
        """
        Cancel an execution
        
        Args:
            execution_id: ID of the execution
            
        Returns:
            True if cancellation successful, False otherwise
        """
        pass 