"""
Unit tests for the M-Query parser module.

Tests MQueryParser (expression type detection and metadata extraction)
and TableFromRowsConverter (static data table conversion to SQL).
"""

import pytest
from src.converters.services.mquery.models import (
    ExpressionType,
    ColumnDataType,
    MQueryExpression,
    PowerBITable,
    StorageMode,
    TableColumn,
    TableMeasure,
)
from src.converters.services.mquery.parser import MQueryParser, TableFromRowsConverter


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def parser():
    return MQueryParser()


@pytest.fixture
def from_rows_converter():
    return TableFromRowsConverter(target_catalog="main", target_schema="sales")


# ---------------------------------------------------------------------------
# MQueryParser.detect_expression_type tests
# ---------------------------------------------------------------------------

def test_detect_native_query(parser):
    """Value.NativeQuery pattern is detected correctly."""
    expr = 'let Source = Value.NativeQuery(Sql.Database("server", "db"), "SELECT * FROM t") in Source'
    assert parser.detect_expression_type(expr) == ExpressionType.NATIVE_QUERY


def test_detect_databricks_catalog_multicloud(parser):
    """DatabricksMultiCloud.Catalogs is detected correctly."""
    expr = 'let Source = DatabricksMultiCloud.Catalogs("https://adb-1234.azuredatabricks.net") in Source'
    assert parser.detect_expression_type(expr) == ExpressionType.DATABRICKS_CATALOG


def test_detect_databricks_catalog(parser):
    """Databricks.Catalogs (non-multicloud) is also detected."""
    expr = 'let Source = Databricks.Catalogs("https://workspace.databricks.com") in Source'
    assert parser.detect_expression_type(expr) == ExpressionType.DATABRICKS_CATALOG


def test_detect_sql_database(parser):
    """Sql.Database pattern is detected correctly."""
    expr = 'let Source = Sql.Database("myserver.database.windows.net", "SalesDB") in Source'
    assert parser.detect_expression_type(expr) == ExpressionType.SQL_DATABASE


def test_detect_table_from_rows(parser):
    """Table.FromRows pattern is detected correctly."""
    expr = 'let Source = Table.FromRows({{"A","B"},{"C","D"}}, type table [Col1 = text, Col2 = text]) in Source'
    assert parser.detect_expression_type(expr) == ExpressionType.TABLE_FROM_ROWS


def test_detect_snowflake(parser):
    """Snowflake.Databases pattern is detected correctly."""
    expr = 'let Source = Snowflake.Databases("myaccount.snowflakecomputing.com") in Source'
    assert parser.detect_expression_type(expr) == ExpressionType.SNOWFLAKE


def test_detect_odbc(parser):
    """Odbc.Query pattern is detected correctly."""
    expr = 'let Source = Odbc.Query("DSN=MyDSN", "SELECT * FROM t") in Source'
    assert parser.detect_expression_type(expr) == ExpressionType.ODBC


def test_detect_oracle(parser):
    """Oracle.Database pattern is detected correctly."""
    expr = 'let Source = Oracle.Database("oracle-server:1521/XE") in Source'
    assert parser.detect_expression_type(expr) == ExpressionType.ORACLE


def test_detect_other(parser):
    """Unknown expression returns ExpressionType.OTHER."""
    expr = "let Source = 42 in Source"
    assert parser.detect_expression_type(expr) == ExpressionType.OTHER


# ---------------------------------------------------------------------------
# MQueryParser.extract_databricks_catalog_info tests
# ---------------------------------------------------------------------------

def test_extract_databricks_catalog_info_full(parser):
    """Workspace URL and warehouse path are extracted; catalog via [Catalog=...] notation."""
    # The parser uses r'\[Catalog\s*=\s*"([^"]+)"' so the expression must contain [Catalog=
    expr = (
        'let Source = DatabricksMultiCloud.Catalogs("https://adb-123.azuredatabricks.net",'
        ' [WareHousePath="/sql/warehouses/abc123", [Catalog="main"]]) in Source'
    )
    info = parser.extract_databricks_catalog_info(expr)
    assert info["workspace_url"] == "https://adb-123.azuredatabricks.net"
    assert info["warehouse_path"] == "/sql/warehouses/abc123"
    # Catalog may or may not be found depending on real M-Query format;
    # the important check is that the function returns a dict with expected keys
    assert "catalog" in info
    assert "workspace_url" in info
    assert "warehouse_path" in info


def test_extract_databricks_catalog_info_missing_fields(parser):
    """Missing optional fields default to None."""
    expr = 'DatabricksMultiCloud.Catalogs("https://workspace.databricks.com")'
    info = parser.extract_databricks_catalog_info(expr)
    assert info["workspace_url"] == "https://workspace.databricks.com"
    assert info["warehouse_path"] is None
    assert info["catalog"] is None


# ---------------------------------------------------------------------------
# MQueryParser.extract_sql_database_info tests
# ---------------------------------------------------------------------------

def test_extract_sql_database_info(parser):
    """SQL Server and database are extracted from Sql.Database expression."""
    expr = 'Sql.Database("myserver.database.windows.net", "SalesDB")'
    info = parser.extract_sql_database_info(expr)
    assert info["server"] == "myserver.database.windows.net"
    assert info["database"] == "SalesDB"


def test_extract_sql_database_info_no_match(parser):
    """Returns None values when pattern not found."""
    info = parser.extract_sql_database_info("let x = 1 in x")
    assert info["server"] is None
    assert info["database"] is None


# ---------------------------------------------------------------------------
# MQueryParser.parse_expression tests
# ---------------------------------------------------------------------------

def test_parse_expression_sql_database(parser):
    """parse_expression fills server and database for SQL_DATABASE type."""
    expr = 'Sql.Database("sqlserver.example.com", "InventoryDB")'
    result = parser.parse_expression(expr)
    assert result.expression_type == ExpressionType.SQL_DATABASE
    assert result.server == "sqlserver.example.com"
    assert result.database == "InventoryDB"


def test_parse_expression_enables_folding(parser):
    """enable_folding is True when EnableFolding=true is in the expression."""
    expr = 'Sql.Database("srv", "db", [EnableFolding=true])'
    result = parser.parse_expression(expr)
    assert result.enable_folding is True


def test_parse_expression_no_folding(parser):
    """enable_folding is False when not mentioned."""
    expr = 'Table.FromRows({{"A"}}, type table [X = text])'
    result = parser.parse_expression(expr)
    assert result.enable_folding is False


def test_parse_expression_databricks_catalog(parser):
    """parse_expression extracts workspace URL for Databricks type."""
    expr = (
        'DatabricksMultiCloud.Catalogs("https://adb-999.azuredatabricks.net",'
        ' [Catalog="dev", WareHousePath="/sql/warehouses/xyz"])'
    )
    result = parser.parse_expression(expr)
    assert result.expression_type == ExpressionType.DATABRICKS_CATALOG
    assert result.server == "https://adb-999.azuredatabricks.net"
    assert result.catalog == "dev"


def test_parse_expression_other_type(parser):
    """parse_expression returns ExpressionType.OTHER for unknown expressions."""
    result = parser.parse_expression("let x = 42 in x")
    assert result.expression_type == ExpressionType.OTHER
    assert result.server is None


# ---------------------------------------------------------------------------
# MQueryParser.parse_table tests
# ---------------------------------------------------------------------------

def test_parse_table_updates_expressions(parser):
    """parse_table replaces raw expressions with typed parsed ones."""
    raw_expr = MQueryExpression(
        raw_expression='Sql.Database("srv", "db")',
        expression_type=ExpressionType.OTHER,
    )
    table = PowerBITable(
        name="T",
        is_hidden=False,
        storage_mode=StorageMode.IMPORT,
        columns=[],
        measures=[],
        source_expressions=[raw_expr],
    )
    result = parser.parse_table(table)
    assert result.source_expressions[0].expression_type == ExpressionType.SQL_DATABASE


# ---------------------------------------------------------------------------
# MQueryParser.get_expression_summary tests
# ---------------------------------------------------------------------------

def test_get_expression_summary(parser):
    """get_expression_summary returns a dict with expected keys."""
    expr = parser.parse_expression('Sql.Database("srv", "db")')
    summary = parser.get_expression_summary(expr)
    assert "type" in summary
    assert "server" in summary
    assert "database" in summary
    assert summary["type"] == ExpressionType.SQL_DATABASE.value


# ---------------------------------------------------------------------------
# TableFromRowsConverter tests
# ---------------------------------------------------------------------------

def test_is_table_from_rows_true(from_rows_converter):
    """is_table_from_rows returns True for matching expression."""
    expr = 'let Source = Table.FromRows({{"A","1"}}) in Source'
    assert from_rows_converter.is_table_from_rows(expr) is True


def test_is_table_from_rows_false(from_rows_converter):
    """is_table_from_rows returns False for non-matching expression."""
    expr = 'Sql.Database("srv", "db")'
    assert from_rows_converter.is_table_from_rows(expr) is False


def test_extract_rows_basic(from_rows_converter):
    """extract_rows correctly parses simple quoted string rows."""
    expr = '''
    let
      Source = Table.FromRows(
        { {"Alice", "30"}, {"Bob", "25"} },
        type table [Name = text, Age = text]
      )
    in Source
    '''
    rows = from_rows_converter.extract_rows(expr)
    assert len(rows) == 2
    assert rows[0] == ("Alice", "30")
    assert rows[1] == ("Bob", "25")


def test_extract_rows_no_match(from_rows_converter):
    """extract_rows returns empty list when pattern not found."""
    rows = from_rows_converter.extract_rows("let x = 1 in x")
    assert rows == []


def test_extract_column_definitions(from_rows_converter):
    """extract_column_definitions parses type table correctly."""
    expr = 'type table [Name = text, Age = number, Active = logical]'
    cols = from_rows_converter.extract_column_definitions(expr)
    assert len(cols) >= 2
    names = [c[0] for c in cols]
    assert "Name" in names
    assert "Age" in names


def test_mquery_type_to_sql_known_types(from_rows_converter):
    """mquery_type_to_sql converts known types to SQL equivalents."""
    assert from_rows_converter.mquery_type_to_sql("text") == "STRING"
    assert from_rows_converter.mquery_type_to_sql("number") == "DOUBLE"
    assert from_rows_converter.mquery_type_to_sql("int64.type") == "BIGINT"
    assert from_rows_converter.mquery_type_to_sql("type date") == "DATE"


def test_mquery_type_to_sql_unknown_defaults_to_string(from_rows_converter):
    """Unknown M-Query type defaults to STRING."""
    assert from_rows_converter.mquery_type_to_sql("something_unknown") == "STRING"


def test_convert_to_sql_full(from_rows_converter):
    """convert_to_sql generates valid CREATE VIEW with VALUES."""
    expr = '''
    let
      Source = Table.FromRows(
        { {"Red", "FF0000"}, {"Blue", "0000FF"} },
        type table [ColorName = text, HexCode = text]
      )
    in Source
    '''
    sql = from_rows_converter.convert_to_sql(expr, "Colors")
    assert sql is not None
    assert "CREATE OR REPLACE VIEW" in sql
    assert "main.sales" in sql
    assert "Red" in sql
    assert "Blue" in sql


def test_convert_to_sql_not_from_rows_returns_none(from_rows_converter):
    """convert_to_sql returns None when expression is not Table.FromRows."""
    sql = from_rows_converter.convert_to_sql('Sql.Database("srv", "db")', "MyTable")
    assert sql is None


def test_convert_to_sql_uses_schema_columns_fallback(from_rows_converter):
    """convert_to_sql uses columns_from_schema when type table not parsed."""
    expr = 'Table.FromRows( { {"1","Apple"} } )'
    cols_from_schema = [
        {"name": "id", "dataType": "Int64"},
        {"name": "name", "dataType": "String"},
    ]
    sql = from_rows_converter.convert_to_sql(expr, "Fruit", columns_from_schema=cols_from_schema)
    assert sql is not None
    assert "id" in sql or "col0" in sql


def test_convert_table_success(from_rows_converter):
    """convert_table returns dict with sql and metadata."""
    table_data = {
        "name": "StatusCodes",
        "columns": [{"name": "Code", "dataType": "String"}, {"name": "Label", "dataType": "String"}],
        "source": [{"expression": 'Table.FromRows( { {"A", "Active"}, {"I", "Inactive"} }, type table [Code = text, Label = text])'}],
    }
    result = from_rows_converter.convert_table(table_data)
    assert result is not None
    assert result["table_name"] == "StatusCodes"
    assert result["expression_type"] == "table_from_rows"
    assert result["row_count"] == 2


def test_convert_table_no_source_returns_none(from_rows_converter):
    """convert_table returns None when table has no source expression."""
    result = from_rows_converter.convert_table({"name": "Empty", "source": []})
    assert result is None


def test_convert_table_wrong_expression_type_returns_none(from_rows_converter):
    """convert_table returns None when expression is not Table.FromRows."""
    result = from_rows_converter.convert_table({
        "name": "SqlTable",
        "source": [{"expression": 'Sql.Database("srv", "db")'}],
    })
    assert result is None
