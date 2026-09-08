"""Usage estimates, not provider invoices. All rates and costs are in USD."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Rate = Annotated[Decimal, Field(ge=0, le=1000000, max_digits=18, decimal_places=8)]


class ModelRate(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    model: str = Field(min_length=1, max_length=255)
    input_per_million: Rate
    output_per_million: Rate
    cached_input_per_million: Rate | None = None

    @field_validator("model")
    @classmethod
    def clean_model(cls, value):
        value = value.strip()
        if not value:
            raise ValueError("A model name is required")
        return value


class BillingQuery(BaseModel):
    start: datetime
    end: datetime
    execution_ids: list[str] | None = Field(default=None, max_length=1000)

    @field_validator("start", "end")
    @classmethod
    def utc(cls, value):
        return (
            value.replace(tzinfo=timezone.utc)
            if value.tzinfo is None
            else value.astimezone(timezone.utc)
        )

    @model_validator(mode="after")
    def valid_period(self):
        if not timedelta(0) < self.end - self.start <= timedelta(days=366):
            raise ValueError("Choose a period of up to one year")
        return self


class UsageTotals(BaseModel):
    calls: int = 0
    measured_calls: int = 0
    priced_calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cached_input_tokens: int = 0
    total_tokens: int = 0
    estimated_cost_usd: float | None = None


class UsageBreakdown(UsageTotals):
    id: str
    label: str
    execution_type: str | None = None


class BillingSummary(BaseModel):
    start: datetime
    end: datetime
    totals: UsageTotals
    models: list[UsageBreakdown]
    runs: list[UsageBreakdown]
    days: list[UsageBreakdown]
    rates: list[ModelRate]
    can_manage_rates: bool
    currency: str = "USD"
    basis: str = (
        "Recorded model calls at the current teamspace rates. Estimates exclude calls without usage or pricing, deleted traces, and non-model charges."
    )
