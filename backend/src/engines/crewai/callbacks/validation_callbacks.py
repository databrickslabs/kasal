"""
Validation callbacks for CrewAI engine.

This module provides callbacks for validating outputs against schemas and rules.
"""
from typing import Any, Optional, List, Dict, Callable
import logging
import json
import re

from src.engines.crewai.callbacks.base import CrewAICallback

logger = logging.getLogger(__name__)

class SchemaValidator(CrewAICallback):
    """Validates output against a JSON schema."""
    
    def __init__(self, schema: Dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.schema = schema
    
    def execute(self, output: Any) -> bool:
        try:
            from jsonschema import validate
            
            # Convert output to dict if needed
            if hasattr(output, 'dict'):
                data = output.dict()
            elif hasattr(output, '__dict__'):
                data = output.__dict__
            else:
                data = output
                
            validate(instance=data, schema=self.schema)
            return True
        except Exception as e:
            self.metadata['validation_error'] = str(e)
            raise

class ContentValidator(CrewAICallback):
    """Validates output content against specified rules."""
    
    def __init__(self, 
                 required_fields: Optional[List[str]] = None,
                 min_length: Optional[int] = None,
                 max_length: Optional[int] = None,
                 pattern: Optional[str] = None,
                 custom_validator: Optional[Callable[[Any], bool]] = None,
                 **kwargs):
        super().__init__(**kwargs)
        self.required_fields = required_fields or []
        self.min_length = min_length
        self.max_length = max_length
        self.pattern = pattern
        self.custom_validator = custom_validator
    
    def execute(self, output: Any) -> bool:
        content = str(output)
        
        # Check required fields
        if self.required_fields:
            if hasattr(output, '__dict__'):
                missing = [f for f in self.required_fields if f not in output.__dict__]
                if missing:
                    self.metadata['missing_fields'] = missing
                    raise ValueError(f"Missing required fields: {missing}")
        
        # Check length constraints
        if self.min_length and len(content) < self.min_length:
            raise ValueError(f"Content length {len(content)} is less than minimum {self.min_length}")
        
        if self.max_length and len(content) > self.max_length:
            raise ValueError(f"Content length {len(content)} exceeds maximum {self.max_length}")
        
        # Check pattern
        if self.pattern and not re.search(self.pattern, content):
            raise ValueError(f"Content does not match pattern: {self.pattern}")
        
        # Run custom validator
        if self.custom_validator and not self.custom_validator(output):
            raise ValueError("Custom validation failed")
        
        return True

class TypeValidator(CrewAICallback):
    """Validates output type and structure."""
    
    def __init__(self, expected_type: type, allow_none: bool = False, **kwargs):
        super().__init__(**kwargs)
        self.expected_type = expected_type
        self.allow_none = allow_none
    
    def execute(self, output: Any) -> bool:
        if output is None:
            if not self.allow_none:
                raise ValueError("Output is None but allow_none is False")
            return True
        
        if not isinstance(output, self.expected_type):
            raise TypeError(f"Expected type {self.expected_type.__name__}, got {type(output).__name__}")
        
        return True 