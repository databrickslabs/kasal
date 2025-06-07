from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, JSON, ForeignKey, DateTime
from sqlalchemy.dialects.postgresql import UUID
import uuid

from src.db.base import Base


class Flow(Base):
    """
    Flow model representing a workflow definition with nodes and edges.
    Enhanced with tenant isolation for multi-tenant deployments.
    """
    __tablename__ = "flows"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)
    crew_id = Column(UUID(as_uuid=True), ForeignKey("crews.id"), nullable=True)
    nodes = Column(JSON, default=list)
    edges = Column(JSON, default=list)
    flow_config = Column(JSON, default=dict)
    
    # Multi-tenant fields
    tenant_id = Column(String(100), index=True, nullable=True)  # Tenant isolation
    created_by_email = Column(String(255), nullable=True)  # Creator email for audit
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __init__(self, **kwargs):
        super(Flow, self).__init__(**kwargs)
        if self.nodes is None:
            self.nodes = []
        if self.edges is None:
            self.edges = []
        if self.flow_config is None:
            self.flow_config = {"actions": []}
        elif isinstance(self.flow_config, dict) and "actions" not in self.flow_config:
            self.flow_config["actions"] = [] 