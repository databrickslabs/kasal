from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class InitializationStatus(Base):
    """
    InitializationStatus model to track database initialization state.
    """

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    is_initialized: Mapped[bool] = mapped_column(Boolean, default=False, nullable=True)
    initialized_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=True
    )
    last_updated: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )
