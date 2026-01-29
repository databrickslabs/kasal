"""
M-Query Converter Data Models

This module defines the data models for Power BI M-Query expressions,
tables, and conversion results.

Author: Kasal Team
Date: 2025
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from enum import Enum


class ExpressionType(str, Enum):
    """Types of Power BI M-Query expressions"""
    NATIVE_QUERY = "native_query"           # Value.NativeQuery - SQL passthrough
    DATABRICKS_CATALOG = "databricks_catalog"  # DatabricksMultiCloud.Catalogs
    SQL_DATABASE = "sql_database"           # Sql.Database
    TABLE_FROM_ROWS = "table_from_rows"     # Table.FromRows (static data)
    ODBC = "odbc"                           # Odbc.Query
    ORACLE = "oracle"                       # Oracle.Database
    SNOWFLAKE = "snowflake"                 # Snowflake.Databases
    OTHER = "other"                         # Unknown/complex expressions


class ColumnDataType(str, Enum):
    """Power BI column data types"""
    STRING = "String"
    INT64 = "Int64"
    DOUBLE = "Double"
    BOOLEAN = "Boolean"
    DATETIME = "DateTime"
    DATE = "Date"
    TIME = "Time"
    DECIMAL = "Decimal"
    BINARY = "Binary"
    UNKNOWN = "Unknown"


class StorageMode(str, Enum):
    """Power BI storage modes"""
    IMPORT = "Import"
    DIRECT_QUERY = "DirectQuery"
    DUAL = "Dual"
    PUSH = "Push"


@dataclass
class TableColumn:
    """Represents a column in a Power BI table"""
    name: str
    data_type: ColumnDataType
    is_hidden: bool = False
    column_type: str = "Data"  # Data, Calculated, etc.
    description: Optional[str] = None
    format_string: Optional[str] = None
    expression: Optional[str] = None  # DAX expression for calculated columns

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TableColumn":
        """Create TableColumn from API response dict"""
        return cls(
            name=data.get("name", ""),
            data_type=ColumnDataType(data.get("dataType", "Unknown")),
            is_hidden=data.get("isHidden", False),
            column_type=data.get("columnType", "Data"),
            description=data.get("description"),
            format_string=data.get("formatString"),
            expression=data.get("expression")  # Capture DAX for calculated columns
        )

    @property
    def is_calculated(self) -> bool:
        """Check if this is a calculated column"""
        return self.column_type == "Calculated" and self.expression is not None


@dataclass
class TableMeasure:
    """Represents a DAX measure in a Power BI table"""
    name: str
    expression: str
    description: Optional[str] = None
    display_folder: Optional[str] = None
    format_string: Optional[str] = None
    is_hidden: bool = False

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TableMeasure":
        """Create TableMeasure from API response dict"""
        return cls(
            name=data.get("name", ""),
            expression=data.get("expression", ""),
            description=data.get("description"),
            display_folder=data.get("displayFolder"),
            format_string=data.get("formatString"),
            is_hidden=data.get("isHidden", False)
        )


@dataclass
class MQueryExpression:
    """Represents a parsed M-Query expression"""
    raw_expression: str
    expression_type: ExpressionType

    # Extracted SQL (for native queries)
    embedded_sql: Optional[str] = None

    # Connection info
    server: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None
    catalog: Optional[str] = None
    warehouse_path: Optional[str] = None

    # Parameters detected in the expression
    parameters: List[Dict[str, str]] = field(default_factory=list)

    # Power Query transformations after the source
    transformations: List[Dict[str, Any]] = field(default_factory=list)

    # Additional metadata
    enable_folding: bool = False
    additional_options: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PowerBITable:
    """Represents a Power BI table with its M-Query source"""
    name: str
    is_hidden: bool
    storage_mode: StorageMode
    columns: List[TableColumn]
    measures: List[TableMeasure]
    source_expressions: List[MQueryExpression]

    # Metadata
    description: Optional[str] = None
    row_count: Optional[int] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PowerBITable":
        """Create PowerBITable from API response dict"""
        # Parse columns
        columns = [
            TableColumn.from_dict(col)
            for col in data.get("columns", [])
        ]

        # Parse measures
        measures = [
            TableMeasure.from_dict(m)
            for m in data.get("measures", [])
        ]

        # Parse source expressions (raw, will be parsed later)
        source_expressions = []
        for source in data.get("source", []):
            expr = source.get("expression", "")
            source_expressions.append(MQueryExpression(
                raw_expression=expr,
                expression_type=ExpressionType.OTHER  # Will be detected later
            ))

        return cls(
            name=data.get("name", ""),
            is_hidden=data.get("isHidden", False),
            storage_mode=StorageMode(data.get("storageMode", "Import")),
            columns=columns,
            measures=measures,
            source_expressions=source_expressions,
            description=data.get("description")
        )


@dataclass
class TableRelationship:
    """Represents a relationship between Power BI tables"""
    name: str
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    cross_filtering_behavior: str = "OneDirection"
    is_active: bool = True
    cardinality: str = "ManyToOne"  # ManyToOne, OneToOne, ManyToMany

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TableRelationship":
        """Create TableRelationship from API response dict"""
        return cls(
            name=data.get("name", ""),
            from_table=data.get("fromTable", ""),
            from_column=data.get("fromColumn", ""),
            to_table=data.get("toTable", ""),
            to_column=data.get("toColumn", ""),
            cross_filtering_behavior=data.get("crossFilteringBehavior", "OneDirection"),
            is_active=data.get("isActive", True),
            cardinality=data.get("cardinality", "ManyToOne")
        )


@dataclass
class HierarchyLevel:
    """Represents a single level in a Power BI hierarchy"""
    name: str
    ordinal: int  # Position in the hierarchy (0 = top level)
    column_name: str  # The column this level is based on
    description: Optional[str] = None

    @classmethod
    def from_info_levels_row(cls, row: Dict[str, Any]) -> "HierarchyLevel":
        """Create HierarchyLevel from INFO.LEVELS() row"""
        return cls(
            name=row.get("[Name]", ""),
            ordinal=row.get("[Ordinal]", 0),
            column_name=row.get("[Column]", "") or row.get("[ColumnName]", ""),
            description=row.get("[Description]")
        )


@dataclass
class Hierarchy:
    """Represents a Power BI hierarchy with its levels"""
    name: str
    table_name: str
    levels: List[HierarchyLevel]
    description: Optional[str] = None
    is_hidden: bool = False

    @classmethod
    def from_info_hierarchies_row(
        cls,
        row: Dict[str, Any],
        levels: Optional[List[HierarchyLevel]] = None
    ) -> "Hierarchy":
        """Create Hierarchy from INFO.HIERARCHIES() row"""
        return cls(
            name=row.get("[Name]", ""),
            table_name=row.get("[TableName]", "") or row.get("[Table]", ""),
            levels=levels or [],
            description=row.get("[Description]"),
            is_hidden=row.get("[IsHidden]", False)
        )

    def get_columns_ordered(self) -> List[str]:
        """Get column names ordered by hierarchy level (top to bottom)"""
        sorted_levels = sorted(self.levels, key=lambda x: x.ordinal)
        return [level.column_name for level in sorted_levels]

    def to_sql_comment(self) -> str:
        """Generate SQL comment documenting the hierarchy"""
        level_info = " → ".join([
            f"{level.name} ({level.column_name})"
            for level in sorted(self.levels, key=lambda x: x.ordinal)
        ])
        return f"-- Hierarchy: {self.name} on table {self.table_name}\n-- Levels: {level_info}"


@dataclass
class SemanticModel:
    """Represents a complete Power BI semantic model"""
    id: str
    name: str
    tables: List[PowerBITable]
    relationships: List[TableRelationship]

    # Metadata
    workspace_id: Optional[str] = None
    workspace_name: Optional[str] = None
    description: Optional[str] = None
    configured_by: Optional[str] = None

    # Expressions/Parameters
    expressions: List[Dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_scan_result(cls, dataset_data: Dict[str, Any], workspace_id: Optional[str] = None, workspace_name: Optional[str] = None) -> "SemanticModel":
        """Create SemanticModel from Admin API scan result"""
        tables = [
            PowerBITable.from_dict(t)
            for t in dataset_data.get("tables", [])
        ]

        relationships = [
            TableRelationship.from_dict(r)
            for r in dataset_data.get("relationships", [])
        ]

        return cls(
            id=dataset_data.get("id", ""),
            name=dataset_data.get("name", ""),
            tables=tables,
            relationships=relationships,
            workspace_id=workspace_id,
            workspace_name=workspace_name,
            description=dataset_data.get("description"),
            configured_by=dataset_data.get("configuredBy"),
            expressions=dataset_data.get("expressions", [])
        )


@dataclass
class CalculatedColumnResult:
    """Result from calculated column DAX to SQL conversion"""
    column_name: str
    original_dax: str
    sql_expression: Optional[str] = None
    data_type: Optional[str] = None
    success: bool = False
    error_message: Optional[str] = None
    notes: Optional[str] = None


@dataclass
class ConversionResult:
    """Result from M-Query to SQL conversion"""
    table_name: str
    expression_type: ExpressionType
    success: bool

    # Original source expression (for display/debugging)
    original_expression: Optional[str] = None

    # Generated SQL
    databricks_sql: Optional[str] = None
    create_view_sql: Optional[str] = None

    # Generated Python/PySpark (optional)
    databricks_python: Optional[str] = None

    # Extracted metadata
    parameters: List[Dict[str, str]] = field(default_factory=list)
    transformations: List[Dict[str, Any]] = field(default_factory=list)

    # Calculated columns (DAX expressions converted to SQL)
    calculated_columns: List[CalculatedColumnResult] = field(default_factory=list)

    # LLM metadata
    llm_explanation: Optional[str] = None
    llm_model: Optional[str] = None
    tokens_used: Optional[int] = None

    # Error info
    error_message: Optional[str] = None
    notes: Optional[str] = None

    # Source info
    source_connection: Optional[Dict[str, Any]] = None


@dataclass
class ScanStatus:
    """Status of a Power BI Admin API scan"""
    scan_id: str
    status: str  # Running, Succeeded, Failed
    created_at: Optional[str] = None
    error: Optional[str] = None


@dataclass
class MQueryConversionConfig:
    """Configuration for M-Query conversion"""
    # Power BI Admin API connection
    # Option 1: Service Principal (requires Admin-level permissions)
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    # Option 2: User OAuth token (alternative to Service Principal)
    access_token: Optional[str] = None

    # Required
    workspace_id: str = ""
    dataset_id: Optional[str] = None  # If None, scan all datasets

    # LLM configuration
    llm_model: str = "databricks-claude-sonnet-4"
    llm_workspace_url: Optional[str] = None
    llm_token: Optional[str] = None
    max_tokens: int = 4000

    # Output configuration
    target_catalog: Optional[str] = None
    target_schema: Optional[str] = None
    generate_views: bool = True
    generate_python: bool = False

    # Scan options
    include_lineage: bool = True
    include_datasource_details: bool = True
    include_dataset_schema: bool = True
    include_dataset_expressions: bool = True
    include_artifact_users: bool = False

    # Processing options
    include_hidden_tables: bool = False
    skip_static_tables: bool = True  # Skip Table.FromRows
