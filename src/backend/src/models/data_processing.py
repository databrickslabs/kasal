"""
Data Processing model.

This module defines the SQLAlchemy model for the data_processing table.
"""

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import Boolean, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class DataProcessing(Base):
    """
    SQLAlchemy model for data processing records.
    """

    __tablename__ = "data_processing"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    che_number: Mapped[str] = mapped_column(
        String, unique=True, index=True, nullable=False
    )
    processed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    company_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # Metadata
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )

    def __init__(self, **kwargs: Any) -> None:
        """
        Initialize a data processing record.

        Args:
            **kwargs: Keyword arguments for model fields
        """
        super(DataProcessing, self).__init__(**kwargs)
        if self.processed is None:
            self.processed = False

    def __repr__(self) -> str:
        """String representation of the model."""
        return f"<DataProcessing(id={self.id}, che_number={self.che_number}, processed={self.processed}, company_name={self.company_name})>"
