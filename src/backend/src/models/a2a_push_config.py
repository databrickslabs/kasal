from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class A2APushConfig(Base):
    """Where to POST when a task changes state.

    A2A lets a client register a webhook per task instead of holding a stream
    open. That matters here more than for most agents: crew runs take minutes
    and the budget work contemplates an hour, so "keep a connection open" is the
    weakest of the options and push is what makes a long run practical.

    One row per (task, url). Registering the same URL twice for a task is an
    update, not a second delivery — a caller retrying a registration must not
    silently double every future notification.

    The secret is used to HMAC-sign the body so the receiver can verify the call
    came from Kasal. Stored as given: it is the caller's own secret for their own
    endpoint, and re-encrypting it here would only move the problem.
    """

    __tablename__ = "a2a_push_configs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    #: The run this watches. Not a foreign key: executions outlive and predate
    #: this table, and a config for a finished run is harmless.
    task_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)

    url: Mapped[str] = mapped_column(Text, nullable=False)
    #: Optional bearer token, sent as Authorization on delivery.
    token: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    #: Optional HMAC secret. When set, deliveries carry X-Kasal-Signature.
    secret: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)

    #: Tenant isolation. A push config addresses a run, and runs are
    #: group-scoped, so this is what stops one workspace registering a webhook
    #: on another's task.
    group_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    created_by_email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    #: Delivery bookkeeping. Kept on the row rather than in a separate log
    #: because the question people actually ask is "is this webhook working",
    #: and that is answered by the last outcome, not by a history nobody reads.
    last_status: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    last_attempt_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    consecutive_failures: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )

    def __repr__(self) -> str:
        return f"<A2APushConfig task={self.task_id} url={self.url!r}>"
