"""Teamspace rates used to estimate the cost of recorded model calls."""

from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import DateTime, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class ModelBillingRate(Base):
    __tablename__ = "model_billing_rates"

    group_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    model: Mapped[str] = mapped_column(String(255), primary_key=True)
    input_per_million: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    output_per_million: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    cached_input_per_million: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 8), nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )
