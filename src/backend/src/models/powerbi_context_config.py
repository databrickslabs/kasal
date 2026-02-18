from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Text, DateTime, JSON
from sqlalchemy.schema import UniqueConstraint, Index

from src.db.base import Base


class PowerBIBusinessMapping(Base):
    """
    Business terminology mappings for Power BI semantic models.
    Maps natural language terms to DAX expressions for context-aware query generation.
    Example: "Complete CGR" -> "[tbl_initial_sizing_tracking][description] = 'Complete CGR'"
    """
    __tablename__ = 'powerbi_business_mappings'

    id = Column(Integer, primary_key=True)

    # Multi-tenant isolation
    group_id = Column(String(255), nullable=False, index=True)

    # Power BI resource identification
    semantic_model_id = Column(String(255), nullable=False)

    # Business mapping
    natural_term = Column(String(500), nullable=False)
    dax_expression = Column(Text, nullable=False)
    description = Column(Text, nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))

    # Unique constraint: one mapping per term per model per group
    __table_args__ = (
        UniqueConstraint('group_id', 'semantic_model_id', 'natural_term', name='uq_business_mapping_term'),
        Index('idx_business_mappings_group_model', 'group_id', 'semantic_model_id'),
    )


class PowerBIFieldSynonym(Base):
    """
    Field synonyms for Power BI semantic models.
    Maps alternative field names to canonical field names for flexible querying.
    Example: field_name="num_customers", synonyms=["number of customers", "customer count"]
    """
    __tablename__ = 'powerbi_field_synonyms'

    id = Column(Integer, primary_key=True)

    # Multi-tenant isolation
    group_id = Column(String(255), nullable=False, index=True)

    # Power BI resource identification
    semantic_model_id = Column(String(255), nullable=False)

    # Field synonym mapping
    field_name = Column(String(255), nullable=False)
    synonyms = Column(JSON, nullable=False)  # List of alternative names

    # Timestamps
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))

    # Unique constraint: one synonym set per field per model per group
    __table_args__ = (
        UniqueConstraint('group_id', 'semantic_model_id', 'field_name', name='uq_field_synonym'),
        Index('idx_field_synonyms_group_model', 'group_id', 'semantic_model_id'),
    )
