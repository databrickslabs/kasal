from datetime import datetime
from typing import Any, Optional

from sqlalchemy import JSON, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class LLMLog(Base):
    """
    LLMLog model for tracking LLM interactions and usage.
    Enhanced with group isolation for multi-group deployments.
    """

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    endpoint: Mapped[str] = mapped_column(
        String, nullable=False
    )  # e.g., 'generate-crew', 'generate-agent'
    prompt: Mapped[str] = mapped_column(String, nullable=False)  # The input prompt
    response: Mapped[str] = mapped_column(String, nullable=False)  # The LLM response
    model: Mapped[str] = mapped_column(String, nullable=False)  # e.g., 'gpt-4'
    tokens_used: Mapped[Optional[int]] = mapped_column(Integer)  # Total tokens used
    duration_ms: Mapped[Optional[int]] = mapped_column(
        Integer
    )  # Time taken in milliseconds
    status: Mapped[str] = mapped_column(String, nullable=False)  # 'success' or 'error'
    error_message: Mapped[Optional[str]] = mapped_column(String)  # Error message if any
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=True
    )  # Use timezone-naive UTC time
    extra_data: Mapped[Any] = mapped_column(
        JSON, nullable=True
    )  # Any additional metadata

    # Multi-group fields
    group_id: Mapped[Optional[str]] = mapped_column(
        String(100), index=True, nullable=True
    )  # Group isolation
    group_email: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True
    )  # Creator email for audit

    def __init__(self, **kwargs: Any) -> None:
        super(LLMLog, self).__init__(**kwargs)
        if self.created_at is None:
            self.created_at = datetime.utcnow()
