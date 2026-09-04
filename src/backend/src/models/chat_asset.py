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

from sqlalchemy import Column, DateTime, Integer, LargeBinary, String

from src.db.base import Base


def generate_uuid():
    return str(uuid.uuid4())


class ChatAsset(Base):
    __tablename__ = "chat_assets"

    id = Column(String(36), primary_key=True, default=generate_uuid)
    group_id = Column(String(100), nullable=True, index=True)
    created_by_email = Column(String(255), nullable=True)
    #: The chat session it was attached in — for listing and cleanup.
    session_id = Column(String(255), nullable=True, index=True)
    name = Column(String(255), nullable=False)
    mime = Column(String(100), nullable=False)
    size = Column(Integer, nullable=False)
    #: Pixel dimensions, reported by the uploader (the browser measures them);
    #: what the prompt tells the model so it can size the image in a layout.
    width = Column(Integer, nullable=True)
    height = Column(Integer, nullable=True)
    data = Column(LargeBinary, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
