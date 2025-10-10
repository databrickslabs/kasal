"""
SQLAlchemy models for DSPy optimization configuration and tracking.

These models store DSPy optimization results, training examples, and configuration.
"""

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Column, String, Float, Integer, Boolean,
    DateTime, ForeignKey, JSON, Text, Index
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from src.db.base import Base


class DSPyConfig(Base):
    """Stores DSPy optimized module configurations and metadata."""

    __tablename__ = "dspy_configs"

    id = Column(String(100), primary_key=True, default=lambda: str(uuid.uuid4()))
    optimization_type = Column(String(50), nullable=False)  # intent_detection, agent_generation, etc.
    version = Column(Integer, nullable=False, default=1)

    # MLflow tracking
    mlflow_run_id = Column(Text)
    mlflow_model_uri = Column(Text)
    mlflow_experiment_id = Column(Text)

    # Optimized prompts and configuration
    prompts_json = Column(JSON)  # Stores the optimized prompts/few-shot examples
    module_config = Column(JSON)  # DSPy module configuration
    optimizer_config = Column(JSON)  # Optimizer settings used

    # Performance metrics
    performance_metrics = Column(JSON)  # Accuracy, latency, token usage, etc.
    test_score = Column(Float)
    num_training_examples = Column(Integer)

    # Status and lifecycle
    is_active = Column(Boolean, default=True)
    deployment_stage = Column(String(20), default="staging")  # staging, production, archived

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    deployed_at = Column(DateTime)

    # Multi-tenant support
    group_id = Column(String(100), ForeignKey("groups.id"))
    group = relationship("Group")

    # Relationships
    training_examples = relationship("DSPyTrainingExample", back_populates="config")
    optimization_runs = relationship("DSPyOptimizationRun", back_populates="config")

    # Indexes for efficient querying
    __table_args__ = (
        Index("idx_dspy_config_type_active", "optimization_type", "is_active"),
        Index("idx_dspy_config_group_type", "group_id", "optimization_type"),
        Index("idx_dspy_config_stage", "deployment_stage"),
    )


class DSPyTrainingExample(Base):
    """Stores training examples collected from MLflow traces for DSPy optimization."""

    __tablename__ = "dspy_training_examples"

    id = Column(String(100), primary_key=True, default=lambda: str(uuid.uuid4()))
    optimization_type = Column(String(50), nullable=False)

    # Example data
    input_data = Column(JSON, nullable=False)  # Input to the module
    output_data = Column(JSON, nullable=False)  # Expected output
    metadata_json = Column(JSON)  # Additional context

    # Quality and source tracking
    quality_score = Column(Float, default=0.0)  # 0.0 to 1.0
    trace_id = Column(Text)  # MLflow trace ID
    execution_id = Column(String(100))  # Link to crew execution
    source_type = Column(String(20), default="trace")  # trace, manual, synthetic

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    collected_at = Column(DateTime)  # When collected from trace

    # Usage tracking
    used_in_optimization = Column(Boolean, default=False)
    optimization_run_ids = Column(JSON)  # List of optimization runs that used this example

    # Multi-tenant support
    group_id = Column(String(100), ForeignKey("groups.id"))
    group = relationship("Group")

    # Relationships
    config_id = Column(String(100), ForeignKey("dspy_configs.id"))
    config = relationship("DSPyConfig", back_populates="training_examples")

    # Indexes
    __table_args__ = (
        Index("idx_dspy_example_type_score", "optimization_type", "quality_score"),
        Index("idx_dspy_example_trace", "trace_id"),
        Index("idx_dspy_example_created", "created_at"),
    )


class DSPyOptimizationRun(Base):
    """Tracks DSPy optimization runs and their results."""

    __tablename__ = "dspy_optimization_runs"

    id = Column(String(100), primary_key=True, default=lambda: str(uuid.uuid4()))
    optimization_type = Column(String(50), nullable=False)

    # Run configuration
    optimizer_type = Column(String(50))  # bootstrap_fewshot, bootstrap_random_search, mipro
    optimizer_params = Column(JSON)  # Optimizer parameters

    # Dataset information
    num_training_examples = Column(Integer)
    num_validation_examples = Column(Integer)
    min_quality_threshold = Column(Float)

    # Results
    status = Column(String(20), default="pending")  # pending, running, completed, failed
    metrics = Column(JSON)  # Training metrics, validation scores
    best_score = Column(Float)

    # Timing
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    duration_seconds = Column(Integer)

    # Error tracking
    error_message = Column(Text)
    error_traceback = Column(Text)

    # MLflow integration
    mlflow_run_id = Column(Text)

    # Trigger information
    triggered_by = Column(String(20))  # scheduled, manual, api
    triggered_by_user = Column(String(100))

    # Multi-tenant support
    group_id = Column(String(100), ForeignKey("groups.id"))

    # Relationships
    config_id = Column(String(100), ForeignKey("dspy_configs.id"))
    config = relationship("DSPyConfig", back_populates="optimization_runs")

    # Indexes
    __table_args__ = (
        Index("idx_dspy_run_type_status", "optimization_type", "status"),
        Index("idx_dspy_run_started", "started_at"),
    )


class DSPyModuleCache(Base):
    """Caches loaded DSPy modules for performance optimization."""

    __tablename__ = "dspy_module_cache"

    id = Column(String(100), primary_key=True, default=lambda: str(uuid.uuid4()))
    optimization_type = Column(String(50), nullable=False)
    config_version = Column(Integer, nullable=False)

    # Serialized module
    module_pickle = Column(Text)  # Base64 encoded pickle of the module
    module_hash = Column(String(64))  # SHA256 hash for verification

    # Cache metadata
    cache_key = Column(String(255), unique=True)
    loaded_at = Column(DateTime, default=datetime.utcnow)
    last_accessed = Column(DateTime, default=datetime.utcnow)
    access_count = Column(Integer, default=0)
    ttl_hours = Column(Integer, default=24)

    # Multi-tenant support
    group_id = Column(String(100), ForeignKey("groups.id"))

    # Indexes
    __table_args__ = (
        Index("idx_dspy_cache_key", "cache_key"),
        Index("idx_dspy_cache_type", "optimization_type", "group_id"),
    )