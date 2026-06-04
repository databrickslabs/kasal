"""
Unit tests for the M-Query converter data models.

Tests ExpressionType, ColumnDataType, StorageMode enums, and the
dataclass models: TableColumn, TableMeasure, MQueryExpression,
PowerBITable, TableRelationship, SemanticModel, ConversionResult,
CalculatedColumnResult, and MQueryConversionConfig.
"""

import pytest
from src.converters.services.mquery.models import (
    ExpressionType,
    ColumnDataType,
    StorageMode,
    TableColumn,
    TableMeasure,
    MQueryExpression,
    PowerBITable,
    TableRelationship,
    SemanticModel,
    ConversionResult,
    CalculatedColumnResult,
    MQueryConversionConfig,
    HierarchyLevel,
    Hierarchy,
)


# ---------------------------------------------------------------------------
# Enum tests
# ---------------------------------------------------------------------------

def test_expression_type_values():
    """ExpressionType enum contains expected values."""
    assert ExpressionType.NATIVE_QUERY == "native_query"
    assert ExpressionType.DATABRICKS_CATALOG == "databricks_catalog"
    assert ExpressionType.TABLE_FROM_ROWS == "table_from_rows"
    assert ExpressionType.OTHER == "other"


def test_column_data_type_values():
    """ColumnDataType enum covers expected SQL-level types."""
    assert ColumnDataType.STRING == "String"
    assert ColumnDataType.INT64 == "Int64"
    assert ColumnDataType.DOUBLE == "Double"
    assert ColumnDataType.BOOLEAN == "Boolean"
    assert ColumnDataType.DATETIME == "DateTime"


def test_storage_mode_values():
    """StorageMode enum has Import, DirectQuery, Dual, Push."""
    assert StorageMode.IMPORT == "Import"
    assert StorageMode.DIRECT_QUERY == "DirectQuery"
    assert StorageMode.DUAL == "Dual"
    assert StorageMode.PUSH == "Push"


# ---------------------------------------------------------------------------
# TableColumn tests
# ---------------------------------------------------------------------------

def test_table_column_from_dict_minimal():
    """TableColumn.from_dict works with minimal required keys."""
    col = TableColumn.from_dict({"name": "SalesAmount", "dataType": "Double"})
    assert col.name == "SalesAmount"
    assert col.data_type == ColumnDataType.DOUBLE
    assert col.is_hidden is False
    assert col.is_calculated is False


def test_table_column_from_dict_calculated():
    """TableColumn with columnType='Calculated' and expression is_calculated=True."""
    col = TableColumn.from_dict({
        "name": "Tax",
        "dataType": "Decimal",
        "columnType": "Calculated",
        "expression": "[SalesAmount] * 0.2",
    })
    assert col.is_calculated is True
    assert col.expression == "[SalesAmount] * 0.2"


def test_table_column_from_dict_data_column_not_calculated():
    """Regular data column with expression should NOT be is_calculated."""
    col = TableColumn.from_dict({
        "name": "Price",
        "dataType": "Double",
        "columnType": "Data",
        "expression": "something",
    })
    assert col.is_calculated is False


def test_table_column_from_dict_defaults():
    """from_dict handles missing optional keys gracefully."""
    col = TableColumn.from_dict({})
    assert col.name == ""
    assert col.data_type == ColumnDataType.UNKNOWN
    assert col.description is None


# ---------------------------------------------------------------------------
# TableMeasure tests
# ---------------------------------------------------------------------------

def test_table_measure_from_dict():
    """TableMeasure.from_dict parses API response dict."""
    m = TableMeasure.from_dict({
        "name": "Total Sales",
        "expression": "SUM(Sales[Amount])",
        "description": "Total revenue",
        "isHidden": False,
    })
    assert m.name == "Total Sales"
    assert m.expression == "SUM(Sales[Amount])"
    assert m.description == "Total revenue"
    assert m.is_hidden is False


def test_table_measure_from_dict_defaults():
    """TableMeasure.from_dict uses sensible defaults for missing keys."""
    m = TableMeasure.from_dict({})
    assert m.name == ""
    assert m.expression == ""
    assert m.is_hidden is False


# ---------------------------------------------------------------------------
# MQueryExpression tests
# ---------------------------------------------------------------------------

def test_mquery_expression_defaults():
    """MQueryExpression default fields are correctly initialised."""
    expr = MQueryExpression(raw_expression="let Source = 1 in Source", expression_type=ExpressionType.OTHER)
    assert expr.embedded_sql is None
    assert expr.server is None
    assert expr.enable_folding is False
    assert expr.parameters == []
    assert expr.transformations == []


# ---------------------------------------------------------------------------
# PowerBITable tests
# ---------------------------------------------------------------------------

def test_powerbi_table_from_dict_full():
    """PowerBITable.from_dict correctly parses a complete dict."""
    data = {
        "name": "Orders",
        "isHidden": False,
        "storageMode": "Import",
        "columns": [{"name": "OrderID", "dataType": "Int64"}],
        "measures": [{"name": "Count", "expression": "COUNTROWS(Orders)"}],
        "source": [{"expression": "let Source = Sql.Database(\"server\", \"db\") in Source"}],
    }
    table = PowerBITable.from_dict(data)
    assert table.name == "Orders"
    assert len(table.columns) == 1
    assert len(table.measures) == 1
    assert len(table.source_expressions) == 1
    assert table.source_expressions[0].expression_type == ExpressionType.OTHER  # not yet parsed


def test_powerbi_table_from_dict_empty():
    """PowerBITable.from_dict handles empty dict without error."""
    table = PowerBITable.from_dict({})
    assert table.name == ""
    assert table.columns == []
    assert table.measures == []
    assert table.source_expressions == []


# ---------------------------------------------------------------------------
# TableRelationship tests
# ---------------------------------------------------------------------------

def test_table_relationship_from_dict():
    """TableRelationship.from_dict populates all fields."""
    rel = TableRelationship.from_dict({
        "name": "rel_orders_customers",
        "fromTable": "Orders",
        "fromColumn": "CustomerID",
        "toTable": "Customers",
        "toColumn": "ID",
        "isActive": True,
        "cardinality": "ManyToOne",
    })
    assert rel.from_table == "Orders"
    assert rel.to_table == "Customers"
    assert rel.is_active is True
    assert rel.cardinality == "ManyToOne"


# ---------------------------------------------------------------------------
# SemanticModel tests
# ---------------------------------------------------------------------------

def test_semantic_model_from_scan_result():
    """SemanticModel.from_scan_result parses tables and relationships."""
    dataset_data = {
        "id": "ds-1",
        "name": "Sales Model",
        "tables": [
            {"name": "Sales", "isHidden": False, "storageMode": "Import", "columns": [], "measures": [], "source": []}
        ],
        "relationships": [],
        "configuredBy": "admin@example.com",
    }
    model = SemanticModel.from_scan_result(dataset_data, workspace_id="ws-1", workspace_name="MyWorkspace")
    assert model.id == "ds-1"
    assert model.name == "Sales Model"
    assert len(model.tables) == 1
    assert model.workspace_id == "ws-1"
    assert model.workspace_name == "MyWorkspace"
    assert model.configured_by == "admin@example.com"


# ---------------------------------------------------------------------------
# ConversionResult tests
# ---------------------------------------------------------------------------

def test_conversion_result_defaults():
    """ConversionResult has correct default values."""
    result = ConversionResult(
        table_name="Orders",
        expression_type=ExpressionType.NATIVE_QUERY,
        success=True,
    )
    assert result.databricks_sql is None
    assert result.parameters == []
    assert result.transformations == []
    assert result.calculated_columns == []
    assert result.error_message is None


def test_conversion_result_failed():
    """ConversionResult captures error message on failure."""
    result = ConversionResult(
        table_name="BadTable",
        expression_type=ExpressionType.OTHER,
        success=False,
        error_message="LLM conversion required",
    )
    assert result.success is False
    assert result.error_message == "LLM conversion required"


# ---------------------------------------------------------------------------
# CalculatedColumnResult tests
# ---------------------------------------------------------------------------

def test_calculated_column_result():
    """CalculatedColumnResult stores conversion output."""
    r = CalculatedColumnResult(
        column_name="TaxAmount",
        original_dax="[SalesAmount] * 0.2",
        sql_expression="`SalesAmount` * 0.2",
        data_type="Double",
        success=True,
    )
    assert r.success is True
    assert r.sql_expression == "`SalesAmount` * 0.2"


# ---------------------------------------------------------------------------
# MQueryConversionConfig tests
# ---------------------------------------------------------------------------

def test_mquery_conversion_config_defaults():
    """MQueryConversionConfig has sensible defaults."""
    cfg = MQueryConversionConfig()
    assert cfg.workspace_id == ""
    assert cfg.llm_model == "databricks-claude-sonnet-4"
    assert cfg.max_tokens == 4000
    assert cfg.include_hidden_tables is False
    assert cfg.generate_views is True


def test_mquery_conversion_config_custom():
    """MQueryConversionConfig allows custom values."""
    cfg = MQueryConversionConfig(
        workspace_id="ws-abc",
        target_catalog="my_catalog",
        target_schema="my_schema",
        llm_workspace_url="https://example.databricks.com",
        llm_token="dapi123",
    )
    assert cfg.workspace_id == "ws-abc"
    assert cfg.target_catalog == "my_catalog"
    assert cfg.llm_workspace_url == "https://example.databricks.com"


# ---------------------------------------------------------------------------
# Hierarchy tests
# ---------------------------------------------------------------------------

def test_hierarchy_get_columns_ordered():
    """Hierarchy.get_columns_ordered returns columns sorted by ordinal."""
    h = Hierarchy(
        name="CalendarHierarchy",
        table_name="Date",
        levels=[
            HierarchyLevel(name="Year", ordinal=0, column_name="Year"),
            HierarchyLevel(name="Quarter", ordinal=1, column_name="Quarter"),
            HierarchyLevel(name="Month", ordinal=2, column_name="Month"),
        ],
    )
    cols = h.get_columns_ordered()
    assert cols == ["Year", "Quarter", "Month"]


def test_hierarchy_to_sql_comment():
    """Hierarchy.to_sql_comment generates readable SQL comment."""
    h = Hierarchy(
        name="ProductHierarchy",
        table_name="Products",
        levels=[
            HierarchyLevel(name="Category", ordinal=0, column_name="Category"),
            HierarchyLevel(name="Subcategory", ordinal=1, column_name="Subcategory"),
        ],
    )
    comment = h.to_sql_comment()
    assert "ProductHierarchy" in comment
    assert "Category" in comment
    assert "Subcategory" in comment
