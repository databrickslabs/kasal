"""Teamspace rates used to estimate the cost of recorded model calls."""

from datetime import datetime

from sqlalchemy import Column, DateTime, Numeric, String

from src.db.base import Base


class ModelBillingRate(Base):
    __tablename__ = "model_billing_rates"

    group_id = Column(String(100), primary_key=True)
    model = Column(String(255), primary_key=True)
    input_per_million = Column(Numeric(18, 8), nullable=False)
    output_per_million = Column(Numeric(18, 8), nullable=False)
    cached_input_per_million = Column(Numeric(18, 8), nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
