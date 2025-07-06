from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, JSON, Boolean, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import ARRAY
from uuid import uuid4

from src.db.base import Base


def generate_uuid():
    return str(uuid4())


class Task(Base):
    """
    Task model representing a task in the system.
    Enhanced with group isolation for multi-group deployments.
    """
    __tablename__ = "tasks"
    
    id = Column(String, primary_key=True, default=generate_uuid)
    name = Column(String, nullable=False)
    description = Column(String, nullable=False)
    agent_id = Column(String, ForeignKey("agents.id"), nullable=True)
    expected_output = Column(String, nullable=False)
    tools = Column(JSON, default=list, nullable=False)
    async_execution = Column(Boolean, default=False)
    context = Column(JSON, default=list)
    config = Column(JSON, default=dict)
    
    # Multi-group fields
    group_id = Column(String(100), index=True, nullable=True)  # Group isolation
    created_by_email = Column(String(255), nullable=True)  # Creator email for audit
    
    # Output configuration
    output_json = Column(String)
    output_pydantic = Column(String)
    output_file = Column(String)
    output = Column(JSON)
    markdown = Column(Boolean, default=False)
    
    # Advanced configuration
    callback = Column(String)
    human_input = Column(Boolean, default=False)
    converter_cls = Column(String)
    guardrail = Column(String, nullable=True)  # Store guardrail configuration as JSON string
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __init__(self, **kwargs):
        # Store the explicitly provided kwargs before calling super
        explicit_kwargs = set(kwargs.keys())
        
        # Extract condition if present (it's not a column, but should be in config)
        condition = kwargs.pop('condition', None)
        
        super(Task, self).__init__(**kwargs)
        if self.id is None:
            self.id = generate_uuid()
        if self.tools is None:
            self.tools = []
        if self.context is None:
            self.context = []
        if self.config is None:
            self.config = {}
        if self.async_execution is None:
            self.async_execution = False
        if self.markdown is None:
            self.markdown = False
        if self.human_input is None:
            self.human_input = False
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if self.updated_at is None:
            self.updated_at = datetime.utcnow()
            
        # Ensure synchronization between config and dedicated fields
        # If output_pydantic is in config, update the dedicated field
        if self.config and 'output_pydantic' in self.config and self.config['output_pydantic']:
            self.output_pydantic = self.config['output_pydantic']
        # If output_pydantic is set as a field but not in config, add it to config
        elif self.output_pydantic and (not self.config.get('output_pydantic')):
            self.config['output_pydantic'] = self.output_pydantic
            
        # Same for other config values that have dedicated fields
        if self.config and 'output_json' in self.config and self.config['output_json']:
            self.output_json = self.config['output_json']
        elif self.output_json and (not self.config.get('output_json')):
            self.config['output_json'] = self.output_json
            
        if self.config and 'output_file' in self.config and self.config['output_file']:
            self.output_file = self.config['output_file']
        elif self.output_file and (not self.config.get('output_file')):
            self.config['output_file'] = self.output_file
            
        if self.config and 'callback' in self.config and self.config['callback']:
            self.callback = self.config['callback']
        elif self.callback and (not self.config.get('callback')):
            self.config['callback'] = self.callback
            
        # Synchronize markdown field
        if self.config and 'markdown' in self.config and self.config['markdown'] is not None:
            self.markdown = self.config['markdown']
        elif 'markdown' in explicit_kwargs and (self.config.get('markdown') is None):
            # Only add to config if markdown was explicitly provided
            self.config['markdown'] = self.markdown
            
        # Ensure condition is properly structured in config if present
        if condition is not None:
            # Note: self.config is guaranteed to be a dict at this point due to line 66
            self.config['condition'] = {
                'type': condition.get('type'),
                'parameters': condition.get('parameters', {}),
                'dependent_task': condition.get('dependent_task')
            } 