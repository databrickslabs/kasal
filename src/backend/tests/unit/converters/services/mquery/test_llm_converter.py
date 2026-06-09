"""
Unit tests for the M-Query LLM Converter module.

Tests MQueryLLMConverter._call_llm, _parse_llm_response,
_rule_based_conversion, convert_expression, convert_calculated_columns,
_dax_to_sql_basic, _convert_switch_to_case, and _enhance_sql_with_calculated_columns
with mocked HTTP calls to avoid real network dependencies.
"""

import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.converters.services.mquery.models import (
    ExpressionType,
    ColumnDataType,
    MQueryExpression,
    TableColumn,
    PowerBITable,
    StorageMode,
    CalculatedColumnResult,
)
from src.converters.services.mquery.llm_converter import MQueryLLMConverter


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def converter_no_llm():
    """Converter without LLM credentials – uses rule-based fallback."""
    return MQueryLLMConverter()


@pytest.fixture
def converter_with_llm():
    """Converter with LLM credentials configured."""
    return MQueryLLMConverter(
        workspace_url="https://example.databricks.com",
        token="dapi-test-token",
        model="databricks-claude-sonnet-4",
    )


@pytest.fixture
def simple_expression():
    return MQueryExpression(
        raw_expression='Sql.Database("server", "db")',
        expression_type=ExpressionType.SQL_DATABASE,
        server="server",
        database="db",
    )


@pytest.fixture
def calc_column():
    return TableColumn(
        name="TaxAmount",
        data_type=ColumnDataType.DOUBLE,
        column_type="Calculated",
        expression="[SalesAmount] * 0.2",
    )


# ---------------------------------------------------------------------------
# _call_llm tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_call_llm_no_credentials_returns_error(converter_no_llm):
    """_call_llm returns error dict when credentials are not configured."""
    result = await converter_no_llm._call_llm("prompt", "system")
    assert result["content"] is None
    assert "error" in result


@pytest.mark.asyncio
async def test_call_llm_success(converter_with_llm):
    """_call_llm returns content when LLMManager.completion() succeeds."""
    with patch("src.core.llm_manager.LLMManager.completion", new_callable=AsyncMock) as mock_completion:
        mock_completion.return_value = '{"success": true, "databricks_sql": "SELECT 1"}'
        result = await converter_with_llm._call_llm("user prompt", "system prompt")

    assert result["content"] is not None
    assert result["content"] == '{"success": true, "databricks_sql": "SELECT 1"}'


@pytest.mark.asyncio
async def test_call_llm_http_error_returns_error_dict(converter_with_llm):
    """_call_llm returns error dict when LLMManager.completion() raises."""
    with patch("src.core.llm_manager.LLMManager.completion", new_callable=AsyncMock) as mock_completion:
        mock_completion.side_effect = Exception("connection refused")
        result = await converter_with_llm._call_llm("prompt", "sys")

    assert result["content"] is None
    assert "connection refused" in result.get("error", "")


# ---------------------------------------------------------------------------
# _parse_llm_response tests
# ---------------------------------------------------------------------------

def test_parse_llm_response_valid_json(converter_no_llm):
    """_parse_llm_response correctly parses valid JSON."""
    payload = json.dumps({"success": True, "databricks_sql": "SELECT 1", "create_view_sql": "CREATE VIEW v AS SELECT 1"})
    result = converter_no_llm._parse_llm_response(payload)
    assert result["success"] is True
    assert result["databricks_sql"] == "SELECT 1"


def test_parse_llm_response_strips_markdown_code_block(converter_no_llm):
    """_parse_llm_response strips ```json ... ``` wrapper."""
    payload = '```json\n{"success": true, "create_view_sql": "CREATE VIEW v AS SELECT 1"}\n```'
    result = converter_no_llm._parse_llm_response(payload)
    assert result["success"] is True


def test_parse_llm_response_strips_plain_code_block(converter_no_llm):
    """_parse_llm_response strips ``` ... ``` wrapper (no language tag)."""
    payload = '```\n{"success": false, "error": "LLM failed"}\n```'
    result = converter_no_llm._parse_llm_response(payload)
    assert result["success"] is False


def test_parse_llm_response_invalid_json_returns_error(converter_no_llm):
    """_parse_llm_response returns error dict for invalid JSON."""
    result = converter_no_llm._parse_llm_response("this is not json at all!!!")
    assert result.get("success") is False
    assert "error" in result


# ---------------------------------------------------------------------------
# _rule_based_conversion tests
# ---------------------------------------------------------------------------

def test_rule_based_conversion_returns_failure(converter_no_llm, simple_expression):
    """_rule_based_conversion always returns success=False with guidance message."""
    result = converter_no_llm._rule_based_conversion(
        table_name="Orders",
        expression=simple_expression,
        columns=[],
    )
    assert result.success is False
    assert "LLM conversion required" in result.error_message
    assert result.table_name == "Orders"
    assert result.expression_type == ExpressionType.SQL_DATABASE


# ---------------------------------------------------------------------------
# convert_expression tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_convert_expression_no_llm_falls_back_to_rule_based(converter_no_llm, simple_expression):
    """convert_expression uses rule-based when LLM is not configured."""
    result = await converter_no_llm.convert_expression(
        table_name="Orders",
        expression=simple_expression,
        columns=[{"name": "id", "data_type": "Int64"}],
    )
    assert result.success is False
    assert "LLM" in result.error_message


@pytest.mark.asyncio
async def test_convert_expression_llm_success(converter_with_llm, simple_expression):
    """convert_expression returns success result from LLM."""
    llm_content = json.dumps({
        "success": True,
        "databricks_sql": "SELECT * FROM orders",
        "create_view_sql": "CREATE OR REPLACE VIEW main.default.orders AS SELECT * FROM orders",
        "parameters": [],
        "transformations": [],
        "explanation": "Direct SQL passthrough",
        "notes": None,
    })

    with patch.object(converter_with_llm, "_call_llm", new=AsyncMock(return_value={"content": llm_content, "usage": {"total_tokens": 50}})):
        result = await converter_with_llm.convert_expression(
            table_name="Orders",
            expression=simple_expression,
            columns=[],
            target_catalog="main",
            target_schema="default",
        )

    assert result.success is True
    assert result.databricks_sql == "SELECT * FROM orders"
    assert converter_with_llm.total_tokens == 50


@pytest.mark.asyncio
async def test_convert_expression_llm_returns_error_in_json(converter_with_llm, simple_expression):
    """convert_expression handles LLM returning success=false in JSON."""
    llm_content = json.dumps({"success": False, "error": "Cannot parse expression"})

    with patch.object(converter_with_llm, "_call_llm", new=AsyncMock(return_value={"content": llm_content, "usage": {}})):
        result = await converter_with_llm.convert_expression(
            table_name="T",
            expression=simple_expression,
            columns=[],
        )

    assert result.success is False
    assert result.error_message == "Cannot parse expression"


@pytest.mark.asyncio
async def test_convert_expression_llm_call_fails_falls_back(converter_with_llm, simple_expression):
    """When LLM call returns no content, falls back to rule-based conversion."""
    with patch.object(converter_with_llm, "_call_llm", new=AsyncMock(return_value={"content": None, "error": "timeout"})):
        result = await converter_with_llm.convert_expression(
            table_name="T",
            expression=simple_expression,
            columns=[],
        )

    assert result.success is False


# ---------------------------------------------------------------------------
# _dax_to_sql_basic tests
# ---------------------------------------------------------------------------

def test_dax_to_sql_basic_removes_table_reference(converter_no_llm):
    """Table name prefix is stripped from column references."""
    sql = converter_no_llm._dax_to_sql_basic("Orders[Amount] * 1.1", "Orders")
    assert "Orders[" not in sql
    assert "`Amount`" in sql


def test_dax_to_sql_basic_if_to_case(converter_no_llm):
    """IF(cond, true, false) is converted to CASE WHEN."""
    sql = converter_no_llm._dax_to_sql_basic("IF([Amount] > 100, 'High', 'Low')", "T")
    assert "CASE WHEN" in sql
    assert "THEN" in sql
    assert "ELSE" in sql
    assert "END" in sql


# ---------------------------------------------------------------------------
# _convert_switch_to_case tests
# ---------------------------------------------------------------------------

def test_convert_switch_to_case_basic(converter_no_llm):
    """SWITCH(TRUE(), ...) args are correctly mapped to CASE WHEN."""
    args_str = "`status` = 1, 'Active', `status` = 2, 'Inactive', 'Unknown'"
    result = converter_no_llm._convert_switch_to_case(args_str)
    assert "CASE" in result
    assert "WHEN `status` = 1 THEN 'Active'" in result
    assert "WHEN `status` = 2 THEN 'Inactive'" in result
    assert "ELSE 'Unknown'" in result
    assert "END" in result


# ---------------------------------------------------------------------------
# _enhance_sql_with_calculated_columns tests
# ---------------------------------------------------------------------------

def test_enhance_sql_with_calculated_columns_inserts_before_from(converter_no_llm):
    """Calculated columns are inserted before the FROM clause."""
    base_sql = "SELECT id, name FROM orders"
    calc_cols = [
        CalculatedColumnResult(column_name="TaxAmt", original_dax="[Amt]*0.1", sql_expression="`Amt` * 0.1", data_type="Double", success=True)
    ]
    result = converter_no_llm._enhance_sql_with_calculated_columns(base_sql, calc_cols)
    from_pos = result.find("FROM")
    tax_pos = result.find("TaxAmt")
    assert tax_pos < from_pos


def test_enhance_sql_with_no_successful_columns_returns_unchanged(converter_no_llm):
    """SQL is unchanged when no calculated columns were successfully converted."""
    base_sql = "SELECT id FROM t"
    calc_cols = [
        CalculatedColumnResult(column_name="BadCol", original_dax="", data_type="String", success=False)
    ]
    result = converter_no_llm._enhance_sql_with_calculated_columns(base_sql, calc_cols)
    assert result == base_sql


def test_enhance_sql_empty_sql_returns_unchanged(converter_no_llm):
    """Empty SQL string is returned as-is."""
    result = converter_no_llm._enhance_sql_with_calculated_columns("", [])
    assert result == ""


# ---------------------------------------------------------------------------
# convert_calculated_columns tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_convert_calculated_columns_no_llm_uses_rules(converter_no_llm, calc_column):
    """Calculated column conversion falls back to rules when LLM unavailable."""
    results = await converter_no_llm.convert_calculated_columns(
        table_name="Sales",
        calculated_columns=[calc_column],
        use_llm=True,  # Will fall back because no credentials
    )
    assert len(results) == 1
    # Rule-based conversion on simple arithmetic should succeed
    assert results[0].column_name == "TaxAmount"


@pytest.mark.asyncio
async def test_convert_calculated_columns_empty_expression_skipped(converter_no_llm):
    """Columns without an expression are skipped."""
    col = TableColumn(name="NoExpr", data_type=ColumnDataType.STRING, column_type="Calculated", expression=None)
    results = await converter_no_llm.convert_calculated_columns(
        table_name="T",
        calculated_columns=[col],
    )
    assert len(results) == 0


# ---------------------------------------------------------------------------
# convert_table tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_convert_table_processes_all_expressions(converter_no_llm):
    """convert_table processes all source expressions in a table."""
    expr1 = MQueryExpression(raw_expression='Sql.Database("srv", "db")', expression_type=ExpressionType.SQL_DATABASE)
    expr2 = MQueryExpression(raw_expression='Sql.Database("srv2", "db2")', expression_type=ExpressionType.SQL_DATABASE)
    table = PowerBITable(
        name="Sales",
        is_hidden=False,
        storage_mode=StorageMode.IMPORT,
        columns=[TableColumn(name="id", data_type=ColumnDataType.INT64)],
        measures=[],
        source_expressions=[expr1, expr2],
    )
    results = await converter_no_llm.convert_table(table, use_llm=False)
    assert len(results) == 2


@pytest.mark.asyncio
async def test_convert_table_no_expressions_returns_empty(converter_no_llm):
    """convert_table returns empty list for table with no source expressions."""
    table = PowerBITable(
        name="Empty",
        is_hidden=False,
        storage_mode=StorageMode.IMPORT,
        columns=[],
        measures=[],
        source_expressions=[],
    )
    results = await converter_no_llm.convert_table(table)
    assert results == []
