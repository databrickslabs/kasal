from datetime import datetime
from typing import Optional
from uuid import uuid4

from sqlalchemy import DateTime, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

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

    id: Mapped[str] = mapped_column(String, primary_key=True, default=generate_uuid)
    title: Mapped[str] = mapped_column(String(255), nullable=False, default="New Chat")
    mode: Mapped[str] = mapped_column(
        String(16), nullable=False, default="chat", server_default="chat"
    )
    canvas_state: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True
    )  # Encrypted builder state, loaded on demand
    canvas_revision: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    user_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    # In-flight crew job for refresh reconnect (replaces the browser IndexedDB
    # marker). NULL when no run is active for this session.
    running_job_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # The session's rendered preview (A2UI deliverable), so it survives reload
    # and follows the user across browsers/devices (replaces the IndexedDB
    # 'previews' store). All NULL when the session has no preview yet.
    preview_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    preview_data: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    preview_title: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)

    # Context compaction: running summary of turns older than the verbatim
    # window injected into each chat run. context_summary_upto marks the
    # timestamp of the newest chat_history row folded into the summary —
    # rows after it are injected verbatim. NULL until the first compaction.
    context_summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    context_summary_upto: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True
    )

    # Multi-group fields (REQUIRED for all models)
    group_id: Mapped[Optional[str]] = mapped_column(
        String(100), index=True, nullable=True
    )
    group_email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    __table_args__ = (
        Index("idx_chat_sessions_group_updated", "group_id", "updated_at"),
        Index("idx_chat_sessions_user_updated", "user_id", "updated_at"),
    )
