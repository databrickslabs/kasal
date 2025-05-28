from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, JSON, Boolean, DateTime
from uuid import uuid4

from src.db.base import Base


def generate_uuid():
    return str(uuid4())


class Agent(Base):
    """
    Agent model representing an AI agent in the system.
    """
    __tablename__ = "agents"
    
    id = Column(String, primary_key=True, default=generate_uuid)
    name = Column(String, nullable=False)
    role = Column(String, nullable=False)
    goal = Column(String, nullable=False)
    backstory = Column(String)
    
    # Core configuration
    llm = Column(String, default="databricks-llama-4-maverick")
    tools = Column(JSON, default=list, nullable=False)
    function_calling_llm = Column(String)
    
    # Execution settings
    max_iter = Column(Integer, default=25)
    max_rpm = Column(Integer)
    max_execution_time = Column(Integer)
    verbose = Column(Boolean, default=False)
    allow_delegation = Column(Boolean, default=False)
    cache = Column(Boolean, default=True)
    
    # Memory settings
    memory = Column(Boolean, default=True)
    embedder_config = Column(JSON)
    
    # Templates
    system_template = Column(String)
    prompt_template = Column(String)
    response_template = Column(String)
    
    # Code execution settings
    allow_code_execution = Column(Boolean, default=False)
    code_execution_mode = Column(String, default='safe')
    
    # Additional settings
    max_retry_limit = Column(Integer, default=2)
    use_system_prompt = Column(Boolean, default=True)
    respect_context_window = Column(Boolean, default=True)
    
    # Knowledge sources
    knowledge_sources = Column(JSON, default=list)
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __init__(self, **kwargs):
        super(Agent, self).__init__(**kwargs)
        if self.tools is None:
            self.tools = []
        if self.knowledge_sources is None:
            self.knowledge_sources = [] 