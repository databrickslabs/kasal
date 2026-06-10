"""
Unit tests for the MQueryConnector class.

Tests the connector lifecycle (connect/disconnect), metadata extraction,
get_tables_with_mquery, get_relationships, get_calculated_columns,
generate_summary_report, and the async context manager protocol.
All external dependencies (AadService, PowerBIAdminScanner, MQueryLLMConverter)
are mocked.
"""

import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

from src.converters.services.mquery.models import (
    ExpressionType,
    ColumnDataType,
    StorageMode,
    MQueryConversionConfig,
    SemanticModel,
    PowerBITable,
    TableColumn,
    TableMeasure,
    TableRelationship,
    MQueryExpression,
    ConversionResult,
)
from src.converters.services.mquery.connector import MQueryConnector


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config(**kwargs):
    defaults = dict(
        tenant_id="ten-1",
        client_id="cli-1",
        client_secret="sec-1",
        workspace_id="ws-1",
        target_catalog="main",
        target_schema="default",
    )
    defaults.update(kwargs)
    return MQueryConversionConfig(**defaults)


def _make_table(name="SalesTable", with_calculated_col=False, with_measure=False, expr_type=ExpressionType.SQL_DATABASE):
    cols = [TableColumn(name="id", data_type=ColumnDataType.INT64)]
    if with_calculated_col:
        cols.append(TableColumn(
            name="Tax",
            data_type=ColumnDataType.DOUBLE,
            column_type="Calculated",
            expression="[Amount] * 0.2",
        ))
    measures = []
    if with_measure:
        measures.append(TableMeasure(name="TotalSales", expression="SUM([Amount])"))

    source_exprs = [MQueryExpression(raw_expression="Sql.Database(\"srv\",\"db\")", expression_type=expr_type)]

    return PowerBITable(
        name=name,
        is_hidden=False,
        storage_mode=StorageMode.IMPORT,
        columns=cols,
        measures=measures,
        source_expressions=source_exprs,
    )


def _make_semantic_model(tables=None, relationships=None):
    return SemanticModel(
        id="ds-1",
        name="SalesModel",
        tables=tables or [_make_table()],
        relationships=relationships or [],
        workspace_id="ws-1",
        workspace_name="MyWorkspace",
    )


# ---------------------------------------------------------------------------
# Connector construction
# ---------------------------------------------------------------------------

def test_connector_initial_state():
    """Newly created connector is not connected."""
    cfg = _make_config()
    conn = MQueryConnector(cfg)
    assert conn.is_connected is False
    assert conn._access_token is None
    assert conn._connected is False


def test_connector_uses_access_token_from_config():
    """access_token provided in config is stored on the connector."""
    cfg = _make_config(access_token="pre-obtained-token")
    conn = MQueryConnector(cfg)
    assert conn._access_token == "pre-obtained-token"


# ---------------------------------------------------------------------------
# connect / disconnect
# ---------------------------------------------------------------------------

def test_connect_sets_connected_state():
    """connect() marks the connector as connected."""
    cfg = _make_config(access_token="tok")  # skip real auth

    with patch("src.converters.services.mquery.connector.AadService") as MockAuth, \
         patch("src.converters.services.mquery.connector.PowerBIAdminScanner"):
        conn = MQueryConnector(cfg)
        conn.connect()

    assert conn._connected is True
    assert conn.is_connected is True


def test_connect_idempotent():
    """Calling connect() twice is a no-op on the second call."""
    cfg = _make_config(access_token="tok")

    with patch("src.converters.services.mquery.connector.AadService"), \
         patch("src.converters.services.mquery.connector.PowerBIAdminScanner"):
        conn = MQueryConnector(cfg)
        conn.connect()
        # Second call should not re-initialize
        conn.connect()

    assert conn._connected is True


def test_disconnect_resets_state():
    """disconnect() resets all state to initial."""
    cfg = _make_config(access_token="tok")

    with patch("src.converters.services.mquery.connector.AadService"), \
         patch("src.converters.services.mquery.connector.PowerBIAdminScanner"):
        conn = MQueryConnector(cfg)
        conn.connect()

    conn.disconnect()
    assert conn._connected is False
    assert conn._access_token is None
    assert conn._scanner is None


def test_connect_without_llm_creds_creates_llm_converter():
    """LLM converter is always created — LLMManager authenticates internally,
    so no per-connector credentials are needed."""
    cfg = _make_config(access_token="tok")  # No llm_workspace_url / llm_token

    with patch("src.converters.services.mquery.connector.AadService"), \
         patch("src.converters.services.mquery.connector.PowerBIAdminScanner"), \
         patch("src.converters.services.mquery.connector.MQueryLLMConverter") as MockLLM:
        conn = MQueryConnector(cfg)
        conn.connect()

    MockLLM.assert_called_once_with(model=cfg.llm_model)


def test_connect_with_llm_creds_creates_llm_converter():
    """Legacy llm_workspace_url/llm_token config values are accepted but the
    converter is built with the model only (auth via LLMManager)."""
    cfg = _make_config(
        access_token="tok",
        llm_workspace_url="https://example.databricks.com",
        llm_token="dapi-abc",
        llm_model="databricks-claude-sonnet-4",
    )

    with patch("src.converters.services.mquery.connector.AadService"), \
         patch("src.converters.services.mquery.connector.PowerBIAdminScanner"), \
         patch("src.converters.services.mquery.connector.MQueryLLMConverter") as MockLLM:
        conn = MQueryConnector(cfg)
        conn.connect()

    MockLLM.assert_called_once_with(model="databricks-claude-sonnet-4")


# ---------------------------------------------------------------------------
# async context manager
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_async_context_manager_connects_and_disconnects():
    """Connector async context manager calls connect/disconnect."""
    cfg = _make_config(access_token="tok")

    with patch("src.converters.services.mquery.connector.AadService"), \
         patch("src.converters.services.mquery.connector.PowerBIAdminScanner"), \
         patch("src.converters.services.mquery.connector.MQueryLLMConverter"):
        async with MQueryConnector(cfg) as conn:
            assert conn.is_connected is True
        assert conn.is_connected is False


# ---------------------------------------------------------------------------
# extract_measures
# ---------------------------------------------------------------------------

def test_extract_measures_empty_when_no_models():
    """extract_measures returns empty list when no models are loaded."""
    cfg = _make_config()
    conn = MQueryConnector(cfg)
    assert conn.extract_measures() == []


def test_extract_measures_returns_kpis():
    """extract_measures returns KPI for each measure in all tables."""
    cfg = _make_config()
    conn = MQueryConnector(cfg)
    conn._semantic_models = [_make_semantic_model(tables=[_make_table(with_measure=True)])]

    kpis = conn.extract_measures()
    assert len(kpis) == 1
    assert kpis[0].technical_name == "SalesTable.TotalSales"


# ---------------------------------------------------------------------------
# get_metadata
# ---------------------------------------------------------------------------

def test_get_metadata_no_models():
    """get_metadata works when no models have been scanned."""
    cfg = _make_config()
    conn = MQueryConnector(cfg)
    meta = conn.get_metadata()
    assert meta.connected is False
    assert meta.measure_count == 0


def test_get_metadata_with_models():
    """get_metadata reports correct table and measure counts."""
    cfg = _make_config()
    conn = MQueryConnector(cfg)
    conn._connected = True
    conn._semantic_models = [_make_semantic_model(tables=[
        _make_table("T1", with_measure=True),
        _make_table("T2"),
    ])]

    meta = conn.get_metadata()
    assert meta.measure_count == 1


# ---------------------------------------------------------------------------
# get_relationships
# ---------------------------------------------------------------------------

def test_get_relationships_empty_when_no_models():
    """get_relationships returns empty list when no models loaded."""
    conn = MQueryConnector(_make_config())
    assert conn.get_relationships() == []


def test_get_relationships_generates_fk_sql():
    """get_relationships returns FK SQL for each relationship."""
    rel = TableRelationship(
        name="fk_orders_customers",
        from_table="Orders",
        from_column="CustomerID",
        to_table="Customers",
        to_column="ID",
    )
    conn = MQueryConnector(_make_config())
    conn._semantic_models = [_make_semantic_model(relationships=[rel])]

    rels = conn.get_relationships()
    assert len(rels) == 1
    assert "ALTER TABLE" in rels[0]["uc_fk_sql"]
    assert "FOREIGN KEY" in rels[0]["uc_fk_sql"]


def test_get_relationships_truncates_long_constraint_name():
    """Constraint names longer than 128 chars are truncated."""
    long_name = "A" * 200
    rel = TableRelationship(
        name=long_name,
        from_table=long_name,
        from_column=long_name,
        to_table="Customers",
        to_column="ID",
    )
    conn = MQueryConnector(_make_config())
    conn._semantic_models = [_make_semantic_model(relationships=[rel])]

    rels = conn.get_relationships()
    # constraint_name is embedded in fk sql; check the part before FOREIGN KEY
    fk_sql = rels[0]["uc_fk_sql"]
    # Grab constraint name line
    constraint_line = [l for l in fk_sql.splitlines() if "ADD CONSTRAINT" in l][0]
    parts = constraint_line.split()
    constraint_name_in_sql = parts[-1]
    assert len(constraint_name_in_sql) <= 128


# ---------------------------------------------------------------------------
# get_calculated_columns
# ---------------------------------------------------------------------------

def test_get_calculated_columns_empty_when_no_models():
    """get_calculated_columns returns empty dict when no models loaded."""
    conn = MQueryConnector(_make_config())
    assert conn.get_calculated_columns() == {}


def test_get_calculated_columns_returns_columns():
    """get_calculated_columns returns dict with table name mapping."""
    conn = MQueryConnector(_make_config())
    conn._semantic_models = [_make_semantic_model(tables=[_make_table("Sales", with_calculated_col=True)])]

    result = conn.get_calculated_columns()
    assert "Sales" in result
    assert len(result["Sales"]) == 1
    assert result["Sales"][0]["name"] == "Tax"


# ---------------------------------------------------------------------------
# generate_summary_report
# ---------------------------------------------------------------------------

def test_generate_summary_report_no_models():
    """generate_summary_report returns error when no models scanned."""
    conn = MQueryConnector(_make_config())
    report = conn.generate_summary_report()
    assert "error" in report


def test_generate_summary_report_with_models():
    """generate_summary_report returns correct statistics."""
    conn = MQueryConnector(_make_config())
    conn._semantic_models = [_make_semantic_model(tables=[
        _make_table("T1", with_measure=True, with_calculated_col=True),
        _make_table("T2"),
    ])]

    report = conn.generate_summary_report()
    assert report["total_tables"] == 2
    assert report["total_measures"] == 1
    assert report["total_calculated_columns"] == 1
    assert "model_count" in report
    assert "expression_types" in report
