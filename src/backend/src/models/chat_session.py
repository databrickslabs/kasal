from datetime import datetime
from uuid import uuid4

from sqlalchemy import Column, DateTime, Index, String

from src.db.base import Base


def generate_uuid():
    return str(uuid4())


class ChatSession(Base):
    """
    Named chat session for the chat-mode workspace.

    chat_history rows carry the messages; this table carries the session's
    identity (title, owner, workspace) so sessions are renamable and listable
    server-side instead of living in browser IndexedDB. Stored through the
    smart-routed session, so it lands in SQLite locally and Lakebase when a
    Lakebase backend is active.
    """

    __tablename__ = "chat_sessions"

    id = Column(String, primary_key=True, default=generate_uuid)
    title = Column(String(255), nullable=False, default="New Chat")
    user_id = Column(String, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    # Multi-group fields (REQUIRED for all models)
    group_id = Column(String(100), index=True, nullable=True)
    group_email = Column(String(255), nullable=True)

    __table_args__ = (
        Index("idx_chat_sessions_group_updated", "group_id", "updated_at"),
        Index("idx_chat_sessions_user_updated", "user_id", "updated_at"),
    )
