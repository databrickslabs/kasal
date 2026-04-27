"""Data classes used across the metric-view generation pipeline."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class TranslationResult:
    """Result of translating a single DAX measure to SQL."""
    measure_name: str       # snake_case
    original_name: str      # PBI name
    sql_expr: str | None    # SQL or None if untranslatable
    is_translatable: bool
    skip_reason: str
    dax_expression: str
    confidence: str
    category: str           # "single_table" | "cross_table" | "unassigned" | "base"
    window_spec: dict | None = None  # UC Metric View window: {order, range, semiadditive}


@dataclass
class TableInfo:
    """Structure extracted from a single MQuery transpiled SQL."""
    table_name: str                          # PBI table name (e.g. "fact_pe002")
    source_table: str                        # Databricks 3-level name from FROM clause
    aggregate_columns: list[dict]            # [{name, source_col}]
    group_by_columns: list[str]             # column names from GROUP BY
    calculated_columns: list[dict]           # [{name, expr}]
    is_fact: bool                            # True if has aggregate columns
    full_sql: str                            # original transpiled SQL
    raw_transpiled_sql: str = ''             # full transpiled SQL for scan enrichment
    dim_source_tables: dict[str, str] = field(default_factory=dict)
    static_filters: list[str] = field(default_factory=list)


@dataclass
class MetricViewSpec:
    """Complete UC Metric View specification for one fact table."""
    fact_table_key: str
    source_table: str
    view_name: str
    comment: str
    joins: list[dict]
    dimensions: list[dict]
    measures: list[TranslationResult]
    untranslatable: list[TranslationResult]
    base_measure_count: int = 0
    dax_measure_count: int = 0
    switch_measure_count: int = 0
    source_filter: str = ''   # MQuery WHERE → UC MV filter: key
    source_sql: str = ''      # Inline SQL for source: |-


@dataclass
class MStep:
    """A single M transform step from PBI scan data."""
    step_type: str        # "SelectRows" | "ReplaceValue" | "AddColumn" | etc.
    raw_expression: str   # original M code for this step


@dataclass
class ScanTableInfo:
    """Extracted info from a PBI scan table with NativeQuery."""
    pbi_table_name: str
    raw_m_expression: str
    native_sql: str          # SQL extracted from Value.NativeQuery(...)
    m_steps: list            # list[MStep]
    has_union: bool
    pbi_columns: list        # columns from scan (name, dataType, columnType, expression)
