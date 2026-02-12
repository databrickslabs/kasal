"""
Storage callbacks for CrewAI engine.

This module provides callbacks for storing outputs in the database.
"""
from typing import Any, Optional, Dict
import logging
from datetime import datetime

from src.engines.crewai.callbacks.base import CrewAICallback

logger = logging.getLogger(__name__)


class DatabaseStorage(CrewAICallback):
    """Stores output in database."""
    
    def __init__(self, repository, **kwargs):
        super().__init__(**kwargs)
        self.repository = repository
    
    async def execute(self, output: Any) -> int:
        # Convert output to dict if needed
        if hasattr(output, 'model_dump'):
            data = output.model_dump()
        elif hasattr(output, 'dict'):
            data = output.dict()
        elif hasattr(output, '__dict__'):
            data = output.__dict__
        else:
            data = {'output': str(output)}
        
        # Create database record using repository pattern
        record_data = {
            'task_key': self.task_key,
            'data': data,
            'metadata': self.metadata,
            'created_at': datetime.now()
        }
        
        # Use repository to create record (no manual session management)
        record = await self.repository.create(**record_data)
        
        logger.info(f"Stored output in database with id {record.id}")
        return record.id