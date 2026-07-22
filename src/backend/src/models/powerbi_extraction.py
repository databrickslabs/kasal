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

from sqlalchemy import Column, Integer, String, JSON, DateTime, Text, Index

from src.db.base import Base


class PowerBIExtraction(Base):
    """Raw Power BI extraction artifacts from one Pipeline Config Generator run."""

    __tablename__ = "powerbi_extraction"

    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Provenance / linkage
    execution_id = Column(String(100), nullable=True, index=True)  # crew/flow job id
    workspace_id = Column(String(100), nullable=True, index=True)  # PBI workspace
    dataset_id = Column(String(100), nullable=True, index=True)    # PBI dataset
    report_id = Column(String(100), nullable=True)                 # PBI report (if used)

    # Raw extracted artifacts (JSON — the full rows, not summaries)
    relationships = Column(JSON, nullable=True)   # [{from_table, from_column, from_cardinality, to_*, is_active, id}]
    measures = Column(JSON, nullable=True)        # [{measure_name, table_name, expression (DAX), description}]
    admin_tables = Column(JSON, nullable=True)    # {table_name: {columns, mquery_expression, measures}}
    report_definition = Column(JSON, nullable=True)  # report visual bindings (measure expressions)
    proposed_config = Column(JSON, nullable=True)    # the derived pipeline_config
    warnings = Column(JSON, nullable=True)           # list of extraction warnings

    # Promoted scalar counts (queryable without JSON traversal)
    relationships_count = Column(Integer, nullable=True)
    measures_count = Column(Integer, nullable=True)
    measures_with_dax_count = Column(Integer, nullable=True)
    admin_tables_count = Column(Integer, nullable=True)

    # Human-readable one-liner (mirrors conversion_history.input_summary)
    summary = Column(Text, nullable=True)

    # Multi-tenant isolation
    group_id = Column(String(100), index=True, nullable=True)
    created_by_email = Column(String(255), nullable=True)

    # Timestamp
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

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
