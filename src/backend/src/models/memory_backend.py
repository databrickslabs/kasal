"""
Memory backend configuration database model.

This module defines the SQLAlchemy model for storing memory backend configurations.
CrewAI 1.10+ uses a single unified ``Memory`` class, so this model no longer
carries per-type enable flags — memory is either on (``is_active=True``) or off.
"""

import enum
from datetime import datetime
from uuid import uuid4

from sqlalchemy import JSON, Boolean, Column, DateTime, Enum, String

from src.db.base import Base


def generate_uuid():
    return str(uuid4())


class MemoryBackendTypeEnum(str, enum.Enum):
    """Memory backend type enumeration."""

    DEFAULT = "default"
    DATABRICKS = "databricks"
    LAKEBASE = "lakebase"


class MemoryBackend(Base):
    """Memory backend configuration model.

    Stores one configuration per named memory backend. A single row defines
    the unified cognitive memory for a given tenant: backend type, connection
    details, cognitive tuning parameters, and activation flags.
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
        default=MemoryBackendTypeEnum.DEFAULT,
    )

    # Backend-specific configuration (stored as JSON).
    databricks_config = Column(JSON, nullable=True)
    lakebase_config = Column(JSON, nullable=True)

    # CrewAI 1.10+ unified cognitive memory tuning (weights, consolidation,
    # recall depth). Stored as JSON so the shape can evolve without requiring
    # a migration per field.
    cognitive_config = Column(JSON, nullable=True)

    # Escape hatch for backend-specific options that haven't graduated to a
    # first-class schema field yet.
    custom_config = Column(JSON, nullable=True)

    # Metadata
    is_active = Column(Boolean, default=True)
    is_default = Column(Boolean, default=False)
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
            "lakebase_config": self.lakebase_config,
            "cognitive_config": self.cognitive_config,
            "custom_config": self.custom_config,
            "is_active": self.is_active,
            "is_default": self.is_default,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    def to_config_dict(self):
        """Convert to runtime configuration for the memory factory."""
        config = {
            "backend_type": self.backend_type.value if self.backend_type else "default",
        }

        if (
            self.backend_type == MemoryBackendTypeEnum.DATABRICKS
            and self.databricks_config
        ):
            config["databricks_config"] = self.databricks_config

        if self.backend_type == MemoryBackendTypeEnum.LAKEBASE and self.lakebase_config:
            config["lakebase_config"] = self.lakebase_config

        if self.cognitive_config:
            config["cognitive_config"] = self.cognitive_config

        if self.custom_config:
            config["custom_config"] = self.custom_config

        return config
