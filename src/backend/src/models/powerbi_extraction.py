"""
PowerBI Extraction model.

Persists the RAW artifacts the Pipeline Config Generator extracts from a Power BI
/ Fabric dataset on every run — relationships, measures (+ DAX), admin/TMDL table
metadata, the report definition, and the derived config — so they are queryable
after the fact (BI/analytics review, debugging "did we actually get the DAX?",
lineage of the model's table graph).

One row per config-gen run, scoped by group_id for tenant isolation. The heavy
artifacts are stored as JSON columns; scalar counts are promoted to their own
columns so the common "how much did we extract for workspace X?" queries need no
JSON traversal.
"""

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import JSON, DateTime, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class PowerBIExtraction(Base):
    """Raw Power BI extraction artifacts from one Pipeline Config Generator run."""

    __tablename__ = "powerbi_extraction"

    # Primary key
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Provenance / linkage
    execution_id: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True, index=True
    )  # crew/flow job id
    workspace_id: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True, index=True
    )  # PBI workspace
    dataset_id: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True, index=True
    )  # PBI dataset
    report_id: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True
    )  # PBI report (if used)

    # Raw extracted artifacts (JSON — the full rows, not summaries)
    relationships: Mapped[Any] = mapped_column(
        JSON, nullable=True
    )  # [{from_table, from_column, from_cardinality, to_*, is_active, id}]
    measures: Mapped[Any] = mapped_column(
        JSON, nullable=True
    )  # [{measure_name, table_name, expression (DAX), description}]
    admin_tables: Mapped[Any] = mapped_column(
        JSON, nullable=True
    )  # {table_name: {columns, mquery_expression, measures}}
    # {name: raw_M} model-level named/shared expressions (staging queries +
    # parameters) — parsed alongside admin_tables (parse_admin_expressions /
    # parse_tmdl_expressions), needed by resolve_mquery_with_context to follow
    # a table's reference to a disabled staging query or substitute a
    # parameter-driven Value.NativeQuery. Without this, the UCMV Generator
    # fallback can only redo the DIRECT resolution tier, not the fuller
    # reference-following / parameter-substitution one.
    expressions: Mapped[Any] = mapped_column(JSON, nullable=True)
    report_definition: Mapped[Any] = mapped_column(
        JSON, nullable=True
    )  # report visual bindings (measure expressions)
    proposed_config: Mapped[Any] = mapped_column(
        JSON, nullable=True
    )  # the derived pipeline_config
    warnings: Mapped[Any] = mapped_column(
        JSON, nullable=True
    )  # list of extraction warnings

    # Promoted scalar counts (queryable without JSON traversal)
    relationships_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    measures_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    measures_with_dax_count: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True
    )
    admin_tables_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Human-readable one-liner (mirrors conversion_history.input_summary)
    summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Multi-tenant isolation
    group_id: Mapped[Optional[str]] = mapped_column(
        String(100), index=True, nullable=True
    )
    created_by_email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Timestamp
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )

    # Indexes for common queries
    __table_args__ = (
        Index("ix_powerbi_extraction_group_created", "group_id", "created_at"),
        Index("ix_powerbi_extraction_workspace_dataset", "workspace_id", "dataset_id"),
    )

    def __repr__(self):
        return (
            f"<PowerBIExtraction(id={self.id}, "
            f"workspace={self.workspace_id}, dataset={self.dataset_id}, "
            f"relationships={self.relationships_count}, measures={self.measures_count})>"
        )
