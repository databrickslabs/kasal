"""
Transformation callbacks for CrewAI engine.

This module provides callbacks for transforming outputs into different formats.
"""
from typing import Any, Optional, Dict, List
import logging
import json
import re
from datetime import datetime

from src.engines.crewai.callbacks.base import CrewAICallback

logger = logging.getLogger(__name__)

class OutputFormatter(CrewAICallback):
    """Formats output according to specified rules."""
    
    def __init__(self, 
                 format_type: str = "json",
                 indent: int = 2,
                 max_length: Optional[int] = None,
                 **kwargs):
        super().__init__(**kwargs)
        self.format_type = format_type
        self.indent = indent
        self.max_length = max_length
    
    def execute(self, output: Any) -> str:
        if self.format_type == "json":
            if hasattr(output, 'dict'):
                formatted = json.dumps(output.dict(), indent=self.indent)
            elif hasattr(output, '__dict__'):
                formatted = json.dumps(output.__dict__, indent=self.indent)
            else:
                formatted = json.dumps(output, indent=self.indent)
        else:
            formatted = str(output)
        
        if self.max_length and len(formatted) > self.max_length:
            formatted = formatted[:self.max_length] + "..."
        
        return formatted

class DataExtractor(CrewAICallback):
    """Extracts specific fields or patterns from output."""
    
    def __init__(self, 
                 fields: Optional[List[str]] = None,
                 patterns: Optional[Dict[str, str]] = None,
                 **kwargs):
        super().__init__(**kwargs)
        self.fields = fields or []
        self.patterns = patterns or {}
    
    def execute(self, output: Any) -> Dict[str, Any]:
        result = {}
        
        # Extract specified fields
        if self.fields:
            if hasattr(output, 'dict'):
                data = output.dict()
            elif hasattr(output, '__dict__'):
                data = output.__dict__
            elif isinstance(output, dict):
                data = output
            else:
                data = {'content': str(output)}
            
            for field in self.fields:
                if field in data:
                    result[field] = data[field]
        
        # Extract patterns
        content = str(output)
        for key, pattern in self.patterns.items():
            matches = re.findall(pattern, content)
            if matches:
                result[key] = matches[0] if len(matches) == 1 else matches
        
        return result

class OutputEnricher(CrewAICallback):
    """Enriches output with additional information."""
    
    def __init__(self, 
                 add_timestamp: bool = True,
                 add_metadata: bool = True,
                 custom_enrichments: Optional[Dict[str, Any]] = None,
                 **kwargs):
        super().__init__(**kwargs)
        self.add_timestamp = add_timestamp
        self.add_metadata = add_metadata
        self.custom_enrichments = custom_enrichments or {}
    
    def execute(self, output: Any) -> Dict[str, Any]:
        # Convert output to dict if needed
        if hasattr(output, 'dict'):
            result = output.dict()
        elif hasattr(output, '__dict__'):
            result = output.__dict__
        elif isinstance(output, dict):
            result = output.copy()
        else:
            result = {'content': str(output)}
        
        # Add timestamp
        if self.add_timestamp:
            result['timestamp'] = datetime.now().isoformat()
        
        # Add metadata
        if self.add_metadata:
            result['metadata'] = self.metadata
        
        # Add custom enrichments
        result.update(self.custom_enrichments)
        
        return result

class OutputSummarizer(CrewAICallback):
    """Summarizes output content."""
    
    def __init__(self, 
                 max_length: int = 200,
                 include_stats: bool = True,
                 **kwargs):
        super().__init__(**kwargs)
        self.max_length = max_length
        self.include_stats = include_stats
    
    def execute(self, output: Any) -> Dict[str, Any]:
        content = str(output)
        words = content.split()
        
        # Create summary
        if len(content) > self.max_length:
            summary = content[:self.max_length] + "..."
        else:
            summary = content
        
        result = {'summary': summary}
        
        # Add statistics if requested
        if self.include_stats:
            result.update({
                'total_length': len(content),
                'word_count': len(words),
                'has_numbers': any(c.isdigit() for c in content),
                'sentence_count': len(re.split(r'[.!?]+', content))
            })
        
        return result 