from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, JSON, DateTime
import json

from src.db.base import Base


class Schema(Base):
    """
    Schema model for storing data schemas and their definitions.
    """
    
    __tablename__ = "schema"
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    description = Column(String, nullable=False)
    schema_type = Column(String, nullable=False)  # e.g., 'data_model', 'tool_config'
    schema_definition = Column(JSON, nullable=False)  # Schema definition in JSON format
    field_descriptions = Column(JSON, default=dict)  # Descriptions for each field
    keywords = Column(JSON, default=list)  # List of related keywords
    tools = Column(JSON, default=list)  # Tools that can use this schema
    example_data = Column(JSON)  # Example data conforming to this schema
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __init__(self, **kwargs):
        """
        Initialize a Schema object with enhanced JSON handling.
        
        Args:
            **kwargs: Keyword arguments for schema attributes
        """
        # Handle schema_json to schema_definition conversion for backward compatibility
        if 'schema_json' in kwargs:
            kwargs['schema_definition'] = kwargs.pop('schema_json')
        
        # Handle JSON strings for fields that should be JSON objects
        for json_field in ['schema_definition', 'field_descriptions', 'example_data', 'keywords', 'tools']:
            if json_field in kwargs and isinstance(kwargs[json_field], str):
                try:
                    kwargs[json_field] = json.loads(kwargs[json_field])
                except (json.JSONDecodeError, TypeError):
                    # If string is invalid JSON and the field is required, set to default
                    if json_field == 'schema_definition':
                        kwargs[json_field] = {}
                    elif json_field in ['keywords', 'tools']:
                        kwargs[json_field] = []
                    elif json_field == 'field_descriptions':
                        kwargs[json_field] = {}
                    # For example_data, we leave it as is since it's nullable
        
        super(Schema, self).__init__(**kwargs)
        
        # Set defaults for JSON fields if None
        if self.field_descriptions is None:
            self.field_descriptions = {}
        if self.keywords is None:
            self.keywords = []
        if self.tools is None:
            self.tools = []
        # example_data can be None, so we don't set a default
    
    def as_dict(self):
        """
        Convert the schema object to a dictionary with proper JSON handling.
        
        Returns:
            dict: Schema data as a dictionary
        """
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'schema_type': self.schema_type,
            'schema_definition': self.schema_definition,
            'field_descriptions': self.field_descriptions or {},
            'keywords': self.keywords or [],
            'tools': self.tools or [],
            'example_data': self.example_data,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        } 