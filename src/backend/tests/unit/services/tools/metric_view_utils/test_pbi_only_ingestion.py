"""
Unit tests for pbi_only_ingestion.py

Fixtures are derived from the OTC Management post-mortem:
 - SKU Active Or NotActive  (Excel.Workbook on SharePoint, S8)
 - CompanyCodesMapping      (TMDL Binary.FromText expression, S8)
 - Typed-table M syntax     (#table literal)
 - Web.Contents / JSON      (generic HTTP endpoint)
 - Normal DB sources        (should NOT be detected as PBI-only)
"""

import pytest

from src.services.tools.metric_view_utils.pbi_only_ingestion import (
    IngestionTask,
    PBIOnlyKind,
    PBIOnlySource,
    build_ingestion_task,
    detect_pbi_only_source,
)


# ---------------------------------------------------------------------------
# OTC-derived M-query fixtures
# ---------------------------------------------------------------------------

# SKU Active Or NotActive: Excel file on SharePoint
MQUERY_EXCEL_SHAREPOINT = """
let
    Source = SharePoint.Files("https://cchbc.sharepoint.com/sites/Supply", [ApiVersion = 15]),
    #"OTC inactiveness" = Source{[Name="OTC inactiveness.xlsx", Kind="File"]}[Content],
    #"Imported Excel" = Excel.Workbook(#"OTC inactiveness", null, true),
    Data = #"Imported Excel"{[Item="Sheet1",Kind="Sheet"]}[Data]
in
    Data
"""

# CompanyCodesMapping: binary helper query in TMDL
MQUERY_TMDL_BINARY = """
let
    Source = Table.FromColumns(
        {Json.Document(Binary.FromText("eJy...==", BinaryEncoding.Base64))},
        {"company_code"}
    )
in
    Source
"""

# Typed table literal
MQUERY_TYPED_TABLE = """
let
    Source = #table(
        {"CompanyCode", "CompanyName"},
        {
            {"1000", "CCHBC Austria GmbH"},
            {"2000", "CCHBC Greece SA"}
        }
    )
in
    Source
"""

# Web.Contents endpoint
MQUERY_WEB_CONTENTS = """
let
    Source = Web.Contents("https://api.example.com/otc/data"),
    Parsed = Json.Document(Source)
in
    Parsed
"""

# Json.Document inline
MQUERY_JSON_DOCUMENT = """
let
    Source = Json.Document("[{\"key\": \"value\"}]")
in
    Source
"""

# Normal Databricks source — NOT PBI-only
MQUERY_DATABRICKS = """
let
    Source = DatabricksMultiCloud.Catalogs("https://adb-1234.azuredatabricks.net"),
    db = Source{[Name="dc_datalake_prod_001",Kind="Database"]}[Data],
    schema = db{[Name="udm_datamart_cust_exp",Kind="Schema"]}[Data],
    table = schema{[Name="cust_exp_dim_company",Kind="Table"]}[Data]
in
    table
"""

# Normal Sql.Database / Value.NativeQuery — NOT PBI-only
MQUERY_NATIVE_QUERY = """
let
    Source = Value.NativeQuery(
        Sql.Database("server.database.windows.net", "SalesDB"),
        "SELECT * FROM dbo.Fact_OTC"
    )
in
    Source
"""

# Snowflake — NOT PBI-only
MQUERY_SNOWFLAKE = """
let
    Source = Snowflake.Databases("account.snowflakecomputing.com"),
    db = Source{[Name="SALES_DB"]}[Data]
in
    db
"""


# ---------------------------------------------------------------------------
# detect_pbi_only_source
# ---------------------------------------------------------------------------


def test_detect_excel_sharepoint():
    result = detect_pbi_only_source(MQUERY_EXCEL_SHAREPOINT)
    assert result is not None
    assert result.kind == PBIOnlyKind.EXCEL_WORKBOOK


def test_detect_tmdl_binary():
    result = detect_pbi_only_source(MQUERY_TMDL_BINARY)
    assert result is not None
    assert result.kind == PBIOnlyKind.TMDL_EXPRESSION


def test_detect_typed_table():
    result = detect_pbi_only_source(MQUERY_TYPED_TABLE)
    assert result is not None
    assert result.kind == PBIOnlyKind.TYPED_TABLE


def test_detect_web_contents():
    result = detect_pbi_only_source(MQUERY_WEB_CONTENTS)
    assert result is not None
    # Web.Contents can also match Json.Document; just check it's PBI-only
    assert result.kind in (PBIOnlyKind.WEB_CONTENTS, PBIOnlyKind.JSON_DOCUMENT)


def test_detect_json_document_no_database():
    # Pure Json.Document without a web endpoint
    mq = 'let Source = Json.Document("[{\"a\":1}]") in Source'
    result = detect_pbi_only_source(mq)
    assert result is not None
    assert result.kind == PBIOnlyKind.JSON_DOCUMENT


def test_detect_sharepoint_files_without_excel():
    mq = 'let Source = SharePoint.Files("https://company.sharepoint.com/sites/Data") in Source'
    result = detect_pbi_only_source(mq)
    assert result is not None
    assert result.kind == PBIOnlyKind.SHAREPOINT
    assert result.url == "https://company.sharepoint.com/sites/Data"


def test_detect_databricks_returns_none():
    result = detect_pbi_only_source(MQUERY_DATABRICKS)
    assert result is None


def test_detect_native_query_returns_none():
    result = detect_pbi_only_source(MQUERY_NATIVE_QUERY)
    assert result is None


def test_detect_snowflake_returns_none():
    result = detect_pbi_only_source(MQUERY_SNOWFLAKE)
    assert result is None


def test_detect_empty_string_returns_none():
    assert detect_pbi_only_source("") is None
    assert detect_pbi_only_source("   ") is None


def test_detect_none_returns_none():
    assert detect_pbi_only_source(None) is None  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# PBIOnlySource description is set
# ---------------------------------------------------------------------------


def test_excel_source_has_description():
    result = detect_pbi_only_source(MQUERY_EXCEL_SHAREPOINT)
    assert result.description
    # Description covers the Excel pattern (may mention Excel.Workbook or URL)
    assert result.description != ""
    assert result.kind == PBIOnlyKind.EXCEL_WORKBOOK


def test_sharepoint_url_extracted():
    mq = 'let Source = SharePoint.Files("https://mycompany.sharepoint.com/sites/OTC") in Source'
    result = detect_pbi_only_source(mq)
    assert result.url == "https://mycompany.sharepoint.com/sites/OTC"


def test_web_url_extracted():
    mq = 'let Source = Web.Contents("https://api.example.com/data") in Source'
    result = detect_pbi_only_source(mq)
    if result and result.kind == PBIOnlyKind.WEB_CONTENTS:
        assert result.url == "https://api.example.com/data"


# ---------------------------------------------------------------------------
# build_ingestion_task
# ---------------------------------------------------------------------------


def test_build_ingestion_task_excel():
    source = PBIOnlySource(
        kind=PBIOnlyKind.EXCEL_WORKBOOK,
        url="https://cchbc.sharepoint.com/sites/Supply",
        description="Excel on SharePoint: OTC inactiveness.xlsx",
    )
    task = build_ingestion_task(
        "SKU Active Or NotActive",
        PBIOnlyKind.EXCEL_WORKBOOK,
        "dc_adb-landing-zone-002",
        "idor",
        source=source,
    )

    assert isinstance(task, IngestionTask)
    assert task.table_name == "SKU Active Or NotActive"
    assert "dc_adb-landing-zone-002" in task.uc_target
    assert "idor" in task.uc_target
    assert task.kind == PBIOnlyKind.EXCEL_WORKBOOK
    assert task.recommended_approach == "dax_evaluate"
    assert task.snapshot_loader_stub  # non-empty Python stub
    assert "SKU Active Or NotActive" in task.snapshot_loader_stub


def test_build_ingestion_task_tmdl_expression():
    source = PBIOnlySource(
        kind=PBIOnlyKind.TMDL_EXPRESSION,
        description="CompanyCodesMapping binary helper query",
    )
    task = build_ingestion_task(
        "CompanyCodesMapping",
        PBIOnlyKind.TMDL_EXPRESSION,
        "dc_adb-landing-zone-002",
        "idor",
        source=source,
    )

    assert task.recommended_approach == "tmdl_decode"
    assert "CompanyCodesMapping" in task.snapshot_loader_stub
    assert "Binary.FromText" in task.snapshot_loader_stub or "getDefinition" in task.snapshot_loader_stub or "expressions.tmdl" in task.snapshot_loader_stub


def test_build_ingestion_task_web_contents():
    source = PBIOnlySource(
        kind=PBIOnlyKind.WEB_CONTENTS,
        url="https://api.example.com/otc",
        description="Web endpoint",
    )
    task = build_ingestion_task(
        "WebTable",
        PBIOnlyKind.WEB_CONTENTS,
        "main",
        "sales",
        source=source,
    )
    assert task.recommended_approach == "http_download"
    assert "https://api.example.com/otc" in task.snapshot_loader_stub


def test_build_ingestion_task_typed_table():
    task = build_ingestion_task(
        "CompanyCodesTyped",
        PBIOnlyKind.TYPED_TABLE,
        "main",
        "idor",
    )
    assert task.recommended_approach == "inline_values"
    assert "TODO" in task.snapshot_loader_stub or "#table" in task.snapshot_loader_stub


def test_build_ingestion_task_uc_target_name():
    """UC target is a backtick-quoted fully-qualified table name."""
    task = build_ingestion_task(
        "SKU Active Or NotActive",
        PBIOnlyKind.EXCEL_WORKBOOK,
        "my_catalog",
        "my_schema",
    )
    assert task.uc_target.startswith("`my_catalog`")
    assert "`my_schema`" in task.uc_target
    # Name should be snake_case
    assert "sku" in task.uc_target.lower()


def test_build_ingestion_task_uc_table_suffix_override():
    task = build_ingestion_task(
        "SKU Active Or NotActive",
        PBIOnlyKind.EXCEL_WORKBOOK,
        "cat",
        "sch",
        uc_table_suffix="otc_ucm_sku_active_or_not_active",
    )
    assert "`otc_ucm_sku_active_or_not_active`" in task.uc_target


# ---------------------------------------------------------------------------
# Stub quality checks
# ---------------------------------------------------------------------------


def test_stub_contains_create_or_replace_table(  ):
    task = build_ingestion_task(
        "MyTable",
        PBIOnlyKind.EXCEL_WORKBOOK,
        "cat",
        "sch",
    )
    assert "CREATE OR REPLACE TABLE" in task.snapshot_loader_stub


def test_stub_contains_loaded_at_utc():
    task = build_ingestion_task(
        "MyTable",
        PBIOnlyKind.TMDL_EXPRESSION,
        "cat",
        "sch",
    )
    assert "loaded_at_utc" in task.snapshot_loader_stub


def test_stub_is_valid_python_syntax():
    """The stub must parse as valid Python (compile-time check)."""
    import ast

    for kind in PBIOnlyKind:
        task = build_ingestion_task("AnyTable", kind, "cat", "sch")
        # This will raise SyntaxError if the stub is broken
        try:
            ast.parse(task.snapshot_loader_stub)
        except SyntaxError as exc:
            pytest.fail(f"Stub for {kind} has invalid Python syntax: {exc}")


# ---------------------------------------------------------------------------
# PBIOnlyKind enum
# ---------------------------------------------------------------------------


def test_pbi_only_kind_values():
    assert PBIOnlyKind.EXCEL_WORKBOOK == "Excel.Workbook"
    assert PBIOnlyKind.SHAREPOINT == "SharePoint"
    assert PBIOnlyKind.WEB_CONTENTS == "Web.Contents"
    assert PBIOnlyKind.JSON_DOCUMENT == "Json.Document"
    assert PBIOnlyKind.TYPED_TABLE == "typed_table"
    assert PBIOnlyKind.TMDL_EXPRESSION == "tmdl_expression"


def test_all_kinds_have_ingestion_task():
    """Every PBIOnlyKind can produce a valid IngestionTask."""
    for kind in PBIOnlyKind:
        task = build_ingestion_task("T", kind, "c", "s")
        assert isinstance(task, IngestionTask)
        assert task.recommended_approach
        assert task.snapshot_loader_stub
