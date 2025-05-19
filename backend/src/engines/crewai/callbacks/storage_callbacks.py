"""
Storage callbacks for CrewAI engine.

This module provides callbacks for storing outputs in various locations (files, database).
"""
from typing import Any, Optional, Dict
import logging
import json
import os
from datetime import datetime
from pathlib import Path

from src.engines.crewai.callbacks.base import CrewAICallback

logger = logging.getLogger(__name__)

class JsonFileStorage(CrewAICallback):
    """Stores output as JSON file."""
    
    def __init__(self, output_dir: str, filename_prefix: str = "", **kwargs):
        super().__init__(**kwargs)
        self.output_dir = Path(output_dir)
        self.filename_prefix = filename_prefix
    
    def execute(self, output: Any) -> str:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.filename_prefix}_{self.task_key}_{timestamp}.json"
        filepath = self.output_dir / filename
        
        # Convert output to JSON-serializable format
        if hasattr(output, 'dict'):
            data = output.dict()
        elif hasattr(output, '__dict__'):
            data = output.__dict__
        else:
            data = output
        
        # Add metadata
        full_data = {
            'task_key': self.task_key,
            'timestamp': timestamp,
            'data': data,
            'metadata': self.metadata
        }
        
        # Write to file
        with open(filepath, 'w') as f:
            json.dump(full_data, f, indent=2)
        
        logger.info(f"Stored output in {filepath}")
        return str(filepath)

class DatabaseStorage(CrewAICallback):
    """Stores output in database."""
    
    def __init__(self, db_session, model_class, **kwargs):
        super().__init__(**kwargs)
        self.db_session = db_session
        self.model_class = model_class
    
    def execute(self, output: Any) -> int:
        # Convert output to dict if needed
        if hasattr(output, 'dict'):
            data = output.dict()
        elif hasattr(output, '__dict__'):
            data = output.__dict__
        else:
            data = {'output': str(output)}
        
        # Create database record
        record = self.model_class(
            task_key=self.task_key,
            data=data,
            metadata=self.metadata,
            created_at=datetime.now()
        )
        
        self.db_session.add(record)
        self.db_session.commit()
        
        logger.info(f"Stored output in database with id {record.id}")
        return record.id

class FileSystemStorage(CrewAICallback):
    """Stores output in filesystem with organization."""
    
    def __init__(self, 
                 base_dir: str,
                 create_date_dirs: bool = True,
                 max_file_size_mb: float = 10.0,
                 **kwargs):
        super().__init__(**kwargs)
        self.base_dir = Path(base_dir)
        self.create_date_dirs = create_date_dirs
        self.max_file_size_mb = max_file_size_mb
    
    def execute(self, output: Any) -> str:
        # Create directory structure
        current_date = datetime.now()
        if self.create_date_dirs:
            output_dir = self.base_dir / str(current_date.year) / f"{current_date.month:02d}"
        else:
            output_dir = self.base_dir
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename
        timestamp = current_date.strftime("%Y%m%d_%H%M%S")
        filename = f"{self.task_key}_{timestamp}.txt"
        filepath = output_dir / filename
        
        # Convert output to string
        content = str(output)
        
        # Check file size
        size_mb = len(content.encode('utf-8')) / (1024 * 1024)
        if size_mb > self.max_file_size_mb:
            raise ValueError(
                f"Output size ({size_mb:.2f}MB) exceeds maximum allowed size "
                f"({self.max_file_size_mb}MB)"
            )
        
        # Write to file
        with open(filepath, 'w') as f:
            f.write(content)
        
        logger.info(f"Stored output in {filepath}")
        return str(filepath) 