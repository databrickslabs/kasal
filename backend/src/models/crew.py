from datetime import datetime, timezone
from sqlalchemy import Column, String, DateTime, JSON
import uuid
from sqlalchemy.dialects.postgresql import UUID

from src.db.base import Base


class Crew(Base):
    """
    SQLAlchemy model for crews.
    """
    __tablename__ = "crews"
    
    id = Column(UUID(as_uuid=True), primary_key=True, index=True, default=uuid.uuid4)
    name = Column(String, index=True)
    agent_ids = Column(JSON, default=lambda: [])
    task_ids = Column(JSON, default=lambda: [])
    nodes = Column(JSON, nullable=True)
    edges = Column(JSON, nullable=True)
    
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