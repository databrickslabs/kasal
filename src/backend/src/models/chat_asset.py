"""An image (or other binary) attached in the chat — stored whole, per group.

Documents attached in the chat go to the knowledge index and are never kept
as files. An image is different: it is not searched, it is SHOWN — placed in
a slide, an HTML page, a diagram — so the bytes themselves must survive, and
be servable to the renderer later. Stored in the database (Lakebase in
production) so the store is as durable and as tenant-scoped as everything
else, with no dependency on app-local disk.
"""

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Integer, LargeBinary, String
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


def generate_uuid() -> str:
    return str(uuid.uuid4())


class ChatAsset(Base):
    __tablename__ = "chat_assets"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=generate_uuid)
    group_id: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True, index=True
    )
    created_by_email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    #: The chat session it was attached in — for listing and cleanup.
    session_id: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, index=True
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    mime: Mapped[str] = mapped_column(String(100), nullable=False)
    size: Mapped[int] = mapped_column(Integer, nullable=False)
    #: Pixel dimensions, reported by the uploader (the browser measures them);
    #: what the prompt tells the model so it can size the image in a layout.
    width: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    height: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    data: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )
