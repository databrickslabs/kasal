"""
Memory backend configuration database model.

This module defines the SQLAlchemy model for storing memory backend configurations.
"""
from sqlalchemy import Column, String, JSON, Boolean, ForeignKey, DateTime, Enum
from sqlalchemy.orm import relationship
from datetime import datetime
import enum
from uuid import uuid4

from src.db.base import Base


def generate_uuid():
    return str(uuid4())


class MemoryBackendTypeEnum(str, enum.Enum):
    """Memory backend type enumeration."""
    DEFAULT = "default"
    DATABRICKS = "databricks"
    # Future backends
    # PINECONE = "pinecone"
    # QDRANT = "qdrant"
    # WEAVIATE = "weaviate"


class MemoryBackend(Base):
    """
    Memory backend configuration model.
    
    Stores configuration for different vector database backends
    used for agent memory storage.
    """
    __tablename__ = "memory_backends"
    
    id = Column(String, primary_key=True, default=generate_uuid)
    
    # Group isolation (consistent with other models)
    group_id = Column(String(100), index=True, nullable=False)
    
    # Basic configuration
    name = Column(String(255), nullable=False)
    description = Column(String(1000), nullable=True)
    backend_type = Column(
        Enum(MemoryBackendTypeEnum), 
        nullable=False, 
        default=MemoryBackendTypeEnum.DEFAULT
    )
    
    # Backend-specific configuration (stored as JSON)
    databricks_config = Column(JSON, nullable=True)
    
    # Common settings
    enable_short_term = Column(Boolean, default=True)
    enable_long_term = Column(Boolean, default=True)
    enable_entity = Column(Boolean, default=True)
    
    # Advanced settings
    enable_relationship_retrieval = Column(Boolean, default=False)
    
    # Additional configuration
    custom_config = Column(JSON, nullable=True)
    
    # Metadata
    is_active = Column(Boolean, default=True)
    is_default = Column(Boolean, default=False)  # User's default config
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def to_dict(self):
        """Convert model to dictionary."""
        return {
            "id": self.id,
            "group_id": self.group_id,
            "name": self.name,
            "description": self.description,
            "backend_type": self.backend_type.value if self.backend_type else None,
            "databricks_config": self.databricks_config,
            "enable_short_term": self.enable_short_term,
            "enable_long_term": self.enable_long_term,
            "enable_entity": self.enable_entity,
            "enable_relationship_retrieval": self.enable_relationship_retrieval,
            "custom_config": self.custom_config,
            "is_active": self.is_active,
            "is_default": self.is_default,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
    
    def to_config_dict(self):
        """Convert to configuration format expected by CrewAI."""
        config = {
            "backend_type": self.backend_type.value if self.backend_type else "default",
            "enable_short_term": self.enable_short_term,
            "enable_long_term": self.enable_long_term,
            "enable_entity": self.enable_entity,
            "enable_relationship_retrieval": self.enable_relationship_retrieval,
        }
        
        if self.backend_type == MemoryBackendTypeEnum.DATABRICKS and self.databricks_config:
            config["databricks_config"] = self.databricks_config
            
        if self.custom_config:
            config["custom_config"] = self.custom_config
            
        return config