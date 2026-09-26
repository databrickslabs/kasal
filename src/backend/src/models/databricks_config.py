from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import Boolean, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class DatabricksConfig(Base):
    """
    DatabricksConfig model for Databricks integration settings with multi-tenant support.
    """

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    workspace_url: Mapped[str] = mapped_column(
        String, nullable=True, default=""
    )  # Make nullable with empty string default
    warehouse_id: Mapped[str] = mapped_column(String, nullable=False)
    catalog: Mapped[str] = mapped_column(String, nullable=False)
    schema: Mapped[str] = mapped_column(String, nullable=False)
    is_active: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=True
    )  # To track the currently active configuration
    is_enabled: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=True
    )  # To enable/disable Databricks integration
    encrypted_personal_access_token: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )  # Encrypted personal access token

    # AI Gateway: route LLM/embedding traffic through /ai-gateway/mlflow/v1
    # (OpenAI-compatible, model in body) instead of /serving-endpoints invocations.
    ai_gateway_enabled: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=True
    )

    # MLflow configuration
    mlflow_enabled: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=True
    )  # Enable/disable MLflow tracking for this workspace
    mlflow_experiment_name: Mapped[str] = mapped_column(
        String, nullable=True, default="kasal-crew-execution-traces"
    )  # MLflow experiment name
    evaluation_enabled: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=True
    )  # Enable/disable MLflow evaluation for this workspace
    evaluation_judge_model: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )  # Databricks judge endpoint route, e.g., "databricks:/<endpoint>"

    # Multi-tenant fields
    group_id: Mapped[Optional[str]] = mapped_column(
        String(100), index=True, nullable=True
    )  # Group isolation
    created_by_email: Mapped[Optional[str]] = mapped_column(
        String(255), index=True, nullable=True
    )  # Creator email for audit

    # Volume configuration fields
    volume_enabled: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=True
    )  # Enable/disable volume uploads for all tasks
    volume_path: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )  # Default volume path (e.g., catalog.schema.volume)
    volume_file_format: Mapped[str] = mapped_column(
        String, nullable=True, default="json"
    )  # Default file format
    volume_create_date_dirs: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=True
    )  # Create date-based directories

    # Knowledge source volume configuration fields
    knowledge_volume_enabled: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=True
    )  # Enable/disable knowledge volume
    knowledge_volume_path: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )  # Knowledge volume path (e.g., catalog.schema.knowledge)
    knowledge_chunk_size: Mapped[int] = mapped_column(
        Integer, default=1000, nullable=True
    )  # Chunk size for knowledge processing
    knowledge_chunk_overlap: Mapped[int] = mapped_column(
        Integer, default=200, nullable=True
    )  # Chunk overlap for context preservation

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.now(timezone.utc), nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.now(timezone.utc),
        onupdate=datetime.now(timezone.utc),
        nullable=True,
    )
