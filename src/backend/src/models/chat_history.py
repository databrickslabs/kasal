from datetime import datetime
from typing import Any, Optional
from uuid import uuid4

from sqlalchemy import JSON, DateTime, Index, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


def generate_uuid():
    return str(uuid4())


class ChatHistory(Base):
    """
    ChatHistory model for tracking chat conversations in the workflow designer.
    Enhanced with group isolation for multi-group deployments.
    """

    __tablename__ = "chat_history"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=generate_uuid)
    session_id: Mapped[str] = mapped_column(
        String, nullable=False, index=True
    )  # Group related messages
    user_id: Mapped[str] = mapped_column(
        String, nullable=False, index=True
    )  # User identifier
    message_type: Mapped[str] = mapped_column(
        String, nullable=False
    )  # 'user' or 'assistant'
    content: Mapped[str] = mapped_column(Text, nullable=False)  # Message content
    intent: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )  # Detected intent (generate_agent, etc.)
    confidence: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )  # Confidence score as string
    generation_result: Mapped[Any] = mapped_column(
        JSON, nullable=True
    )  # Generated agent/task/crew data
    timestamp: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )  # Timezone-naive UTC

    # Multi-group fields (REQUIRED for all models)
    group_id: Mapped[Optional[str]] = mapped_column(
        String(100), index=True, nullable=True
    )  # Group isolation
    group_email: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True
    )  # Creator email for audit

    # Database indexes for performance
    __table_args__ = (
        Index("idx_chat_history_session_timestamp", "session_id", "timestamp"),
        Index("idx_chat_history_user_timestamp", "user_id", "timestamp"),
        Index("idx_chat_history_group_timestamp", "group_id", "timestamp"),
    )

    def __init__(self, **kwargs):
        """Initialize ChatHistory with default values."""
        super(ChatHistory, self).__init__(**kwargs)
        if self.id is None:
            self.id = generate_uuid()
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()
