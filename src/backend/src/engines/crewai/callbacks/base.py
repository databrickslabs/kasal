"""
Base callbacks for CrewAI engine.

This module provides base callback classes for CrewAI.
"""
import logging
from typing import Any, Optional, Dict
from datetime import datetime
from abc import ABC, abstractmethod
import asyncio

logger = logging.getLogger(__name__)

class CrewAICallback(ABC):
    """Base class for all callbacks with common functionality."""
    
    def __init__(self, max_retries: int = 3, task_key: Optional[str] = None):
        self.max_retries = max_retries
        self.task_key = task_key
        self.retry_count = 0
        self.metadata: Dict[str, Any] = {}
    
    async def __call__(self, output: Any) -> Any:
        """Main entry point for callback execution with retry logic."""
        try:
            logger.info(f"=== Starting {self.__class__.__name__} ===")
            logger.info(f"Task: {self.task_key or 'Unknown'}")
            logger.info(f"Attempt: {self.retry_count + 1}/{self.max_retries}")
            
            result = await self.execute(output)
            
            logger.info(f"=== Successfully completed {self.__class__.__name__} ===")
            return result
            
        except Exception as e:
            logger.error(f"Error in {self.__class__.__name__}: {e}")
            logger.error("Stack trace:", exc_info=True)
            
            if self.retry_count < self.max_retries:
                self.retry_count += 1
                logger.info(f"Retrying {self.__class__.__name__} (Attempt {self.retry_count + 1}/{self.max_retries})")
                # Add delay between retries
                await asyncio.sleep(1)  # 1 second delay
                return await self.__call__(output)
            else:
                logger.error(f"{self.__class__.__name__} failed after {self.max_retries} attempts")
                raise CallbackFailedError(
                    callback_name=self.__class__.__name__,
                    task_key=self.task_key,
                    error=str(e),
                    metadata=self.metadata
                )
    
    @abstractmethod
    async def execute(self, output: Any) -> Any:
        """Execute the actual callback logic. Must be implemented by subclasses."""
        pass
    
    def _log_output_info(self, output: Any) -> None:
        """Helper method to log output information."""
        logger.info(f"Output Type: {type(output)}")
        if hasattr(output, 'raw'):
            logger.info(f"Output Content: {output.raw[:500]}...")
        elif isinstance(output, (str, int, float, bool)):
            logger.info(f"Output Content: {str(output)[:500]}...")
        elif isinstance(output, dict):
            logger.info(f"Output Content: {str(output)[:500]}...")
        else:
            logger.info(f"Output Content: {str(output)[:500]}...")

class CallbackFailedError(Exception):
    """Exception raised when a callback fails after all retries."""
    
    def __init__(self, callback_name: str, task_key: Optional[str], error: str, metadata: Dict[str, Any]):
        self.callback_name = callback_name
        self.task_key = task_key
        self.error = error
        self.metadata = metadata
        self.timestamp = datetime.now()
        
        message = (
            f"Callback '{callback_name}' failed for task '{task_key or 'Unknown'}'\n"
            f"Error: {error}\n"
            f"Timestamp: {self.timestamp}\n"
            f"Metadata: {metadata}"
        )
        super().__init__(message) 