from datetime import datetime, timezone
from sqlalchemy import Column, String, DateTime, JSON, Boolean
import uuid
from sqlalchemy.dialects.postgresql import UUID

from src.db.base import Base


class Crew(Base):
    """
    SQLAlchemy model for crews.
    Enhanced with group isolation for multi-group deployments.
    """
    __tablename__ = "crews"

    id = Column(UUID(as_uuid=True), primary_key=True, index=True, default=uuid.uuid4)
    name = Column(String, index=True)
    agent_ids = Column(JSON, default=lambda: [])
    task_ids = Column(JSON, default=lambda: [])
    nodes = Column(JSON, nullable=True)
    edges = Column(JSON, nullable=True)

    # Crew execution configuration
    process = Column(String(50), default='sequential')  # sequential or hierarchical
    planning = Column(Boolean, default=False)  # Enable planning mode
    planning_llm = Column(String(255), nullable=True)  # LLM for planning
    reasoning = Column(Boolean, default=False)  # Enable reasoning mode
    reasoning_llm = Column(String(255), nullable=True)  # LLM for reasoning
    manager_llm = Column(String(255), nullable=True)  # LLM for hierarchical manager
    tool_configs = Column(JSON, nullable=True)  # Crew-level tool configurations (MCP servers, etc.)
    memory = Column(Boolean, default=True)  # Enable memory
    verbose = Column(Boolean, default=True)  # Verbose output
    max_rpm = Column(JSON, nullable=True)  # Max requests per minute (can be int or None)

    # Multi-group fields
    group_id = Column(String(100), index=True, nullable=True)  # Group isolation
    created_by_email = Column(String(255), nullable=True)  # Creator email for audit

    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __init__(self, **kwargs):
        super(Crew, self).__init__(**kwargs)
        if self.agent_ids is None:
            self.agent_ids = []
        if self.task_ids is None:
            self.task_ids = []
        if self.nodes is None:
            self.nodes = []
        if self.edges is None:
            self.edges = []


# Keeping Plan class for backward compatibility, but making it use the same table
class Plan(Crew):
    """
    Plan class (alias for Crew) for backward compatibility.
    """
    pass 