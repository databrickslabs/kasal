"""
PowerBI Semantic Model Metadata Cache Model

Stores cached semantic model metadata to avoid re-fetching when running
multiple analyses on the same dataset within the same day.

Cached data includes:
- Measures
- Table relationships
- Schema (column descriptions, table information)
- Sample data values
- Default filters (report-level filters)
"""

from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import JSON, Date, DateTime, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.schema import UniqueConstraint

from src.db.base import Base


class PowerBISemanticModelCache(Base):
    """
    Cache for Power BI semantic model metadata.

    Stores expensive-to-fetch metadata (measures, relationships, schema, sample data)
    that doesn't change frequently. Cache is day-scoped: same dataset_id on same day
    reuses cached metadata.

    User-provided inputs (business_mappings, field_synonyms, active_filters) are NOT
    cached - they come fresh from user input each time.
    """

    __tablename__ = "powerbi_semantic_model_cache"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # Multi-tenant isolation
    group_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)

    # Power BI resource identification
    dataset_id: Mapped[str] = mapped_column(String(255), nullable=False)
    workspace_id: Mapped[str] = mapped_column(String(255), nullable=False)

    # Optional report identification (if default filters are report-specific)
    report_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Cache validity
    cached_date: Mapped[date] = mapped_column(
        Date, nullable=False
    )  # Date the cache was written

    # Cached metadata (stored as JSON)
    # Note: Named 'cache_data' instead of 'metadata' to avoid SQLAlchemy reserved name
    cache_data: Mapped[Any] = mapped_column(JSON, nullable=False)
    """
    Cache data structure:
    {
        "measures": [...],           # List of measure definitions from TMDL
        "relationships": [...],      # Table relationships from TMDL
        "schema": {                  # Column descriptions and table info
            "tables": [...],
            "columns": [...]
        },
        "sample_data": {...},        # Sample values for columns
        "default_filters": {...}     # Report-level default filters (if report_id provided)
    }
    """

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.now(timezone.utc), nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.now(timezone.utc),
        onupdate=datetime.now(timezone.utc),
        nullable=True,
    )

    # Unique constraint: one cache per dataset per day per group (optional report_id)
    __table_args__ = (
        UniqueConstraint(
            "group_id",
            "dataset_id",
            "cached_date",
            "report_id",
            name="uq_semantic_model_cache_daily",
        ),
        Index("idx_semantic_cache_group_dataset", "group_id", "dataset_id"),
        Index("idx_semantic_cache_date", "cached_date"),
    )

    def is_valid_for_today(self, cache_ttl_days: int = 1) -> bool:
        """Check if cache is still valid within the given TTL window.

        Args:
            cache_ttl_days: How many days back to accept a cached entry (default 1 = today only)
        """
        cutoff = date.today() - timedelta(days=cache_ttl_days - 1)
        # A future-dated cache is never valid for today (clock skew / bad data).
        return cutoff <= self.cached_date <= date.today()
