"""
PBI-only data source detection and ingestion task generation.

Item S8 from the IDOR/DQA post-mortem: some PBI tables have *no* Unity
Catalog source — Power BI builds them itself from Excel files on SharePoint,
web endpoints, inline JSON, or typed-in literal rows in the model definition.
These tables need an ingestion / snapshot step, not a SQL translation.

Two entry points
----------------
``detect_pbi_only_source(mquery)``
    Inspect an M-query expression and return a :class:`PBIOnlySource` if the
    table's data comes from a PBI-internal source, or ``None`` if it reads from
    a normal database (Databricks / SQL Server / …).

``build_ingestion_task(table_name, kind, uc_catalog, uc_schema, ...)``
    Given a table name and its detected kind, return an :class:`IngestionTask`
    describing *how* to snapshot the data into Unity Catalog, plus a Python
    code stub (modelled on ``load_pbi_only_tables.py``) that the integrator can
    adapt to a Databricks job.

Neither function makes network calls or imports pipeline state.

Detection patterns (with OTC examples)
---------------------------------------
* ``Excel.Workbook`` — ``"OTC inactiveness.xlsx"`` on SharePoint →
  ``SKU Active Or NotActive`` table; real data, needs SharePoint download or
  ``EVALUATE '<table>'`` via the PBI executeQueries API.
* ``SharePoint.Files`` / ``SharePoint.Tables`` — general SharePoint connector.
* ``Web.Contents`` — HTTP endpoint; snapshot via Python ``requests`` call.
* ``Json.Document`` — inline JSON literal embedded in the M expression.
* **Typed table** — ``#table({"col1","col2"}, {{row1},{row2}})`` syntax; data is
  literally in the model definition and can be decoded from TMDL.
* **TMDL expression** — a binary-compressed helper query (like
  ``CompanyCodesMapping``) whose data is stored in ``expressions.tmdl``; decoded
  via ``getDefinition`` + TMDL parse (as in ``load_pbi_only_tables.py``).
"""

from __future__ import annotations

import re
import textwrap
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


# ---------------------------------------------------------------------------
# PBIOnlyKind enum
# ---------------------------------------------------------------------------


class PBIOnlyKind(str, Enum):
    """Classification of a PBI-only data source."""

    EXCEL_WORKBOOK = "Excel.Workbook"
    """Excel file (.xlsx / .xls), typically on SharePoint."""

    SHAREPOINT = "SharePoint"
    """SharePoint.Files or SharePoint.Tables connector (non-Excel)."""

    WEB_CONTENTS = "Web.Contents"
    """HTTP/HTTPS endpoint read with ``Web.Contents``."""

    JSON_DOCUMENT = "Json.Document"
    """Inline JSON literal embedded in the M expression."""

    TYPED_TABLE = "typed_table"
    """M ``#table(…)`` literal — data is inside the model definition."""

    TMDL_EXPRESSION = "tmdl_expression"
    """Binary-compressed helper query stored in ``expressions.tmdl``
    (e.g. ``CompanyCodesMapping``).  Decoded via ``getDefinition``."""


# ---------------------------------------------------------------------------
# Output dataclasses
# ---------------------------------------------------------------------------


@dataclass
class PBIOnlySource:
    """Detection result for one M-query expression."""

    kind: PBIOnlyKind
    url: Optional[str] = None
    """URL / SharePoint site extracted from the M expression, when present."""
    description: str = ""
    """Human-readable summary of the detected pattern."""


@dataclass
class IngestionTask:
    """Instructions for snapshotting one PBI-only table into UC.

    ``snapshot_loader_stub`` is a self-contained Python module string that the
    integrator can save to a ``.py`` file and adapt to a Databricks job or
    notebook.  It follows the pattern of ``load_pbi_only_tables.py`` from the
    OTC post-mortem.
    """

    table_name: str
    """PBI table name (as it appears in the model)."""

    uc_target: str
    """Fully-qualified UC target table, e.g. ``"`cat`.`sch`.`snap_table`"``."""

    kind: PBIOnlyKind

    source_description: str
    """Human-readable origin, e.g. ``"Excel on SharePoint: inactiveness.xlsx"``."""

    recommended_approach: str
    """One of: ``"dax_evaluate"``, ``"tmdl_decode"``, ``"http_download"``,
    ``"inline_values"``."""

    snapshot_loader_stub: str
    """Python stub for the snapshot loader (ready to adapt)."""

    columns_hint: str = ""
    """Optional schema hint extracted from the M expression (may be empty)."""


# ---------------------------------------------------------------------------
# Detection regex patterns
# ---------------------------------------------------------------------------

_RE_EXCEL = re.compile(r"\bExcel\.Workbook\s*\(", re.IGNORECASE)
_RE_SHAREPOINT = re.compile(r"\bSharePoint\.(Files|Tables)\s*\(", re.IGNORECASE)
_RE_WEB = re.compile(r"\bWeb\.Contents\s*\(", re.IGNORECASE)
_RE_JSON = re.compile(r"\bJson\.Document\s*\(", re.IGNORECASE)
_RE_TYPED_TABLE = re.compile(r"#table\s*\(", re.IGNORECASE)
_RE_TMDL_BINARY = re.compile(r"Binary\.FromText\s*\(", re.IGNORECASE)

# URL extraction patterns
_RE_SHAREPOINT_URL = re.compile(
    r"(?:SharePoint\.Files|SharePoint\.Tables)\s*\(\s*\"([^\"]+)\"", re.IGNORECASE
)
_RE_WEB_URL = re.compile(r"Web\.Contents\s*\(\s*\"([^\"]+)\"", re.IGNORECASE)
_RE_EXCEL_URL = re.compile(r"File\.Contents\s*\(\s*\"([^\"]+)\"", re.IGNORECASE)

# Patterns that indicate a normal database source (not PBI-only)
_RE_DATABRICKS = re.compile(
    r"\b(?:DatabricksMultiCloud|Databricks)\.Catalogs?\s*\(", re.IGNORECASE
)
_RE_SQL_DB = re.compile(r"\b(?:Sql|Value\.NativeQuery)\b", re.IGNORECASE)
_RE_ODBC = re.compile(r"\bOdbc\.Query\s*\(", re.IGNORECASE)
_RE_ORACLE = re.compile(r"\bOracle\.Database\s*\(", re.IGNORECASE)
_RE_SNOWFLAKE = re.compile(r"\bSnowflake\.Databases\s*\(", re.IGNORECASE)


# ---------------------------------------------------------------------------
# detect_pbi_only_source
# ---------------------------------------------------------------------------


def detect_pbi_only_source(mquery: str) -> Optional[PBIOnlySource]:
    """Detect whether an M-query expression reads from a PBI-internal source.

    Returns ``None`` when the table reads from a standard database connector
    (Databricks, SQL Server, ODBC, Snowflake, Oracle).

    Returns a :class:`PBIOnlySource` for Excel, SharePoint, Web, JSON, typed
    tables, or TMDL binary expressions.

    Parameters
    ----------
    mquery:
        The raw M-query expression string for one table partition.
    """
    if not mquery or not mquery.strip():
        return None

    # Fast-exit for known database patterns
    if (
        _RE_DATABRICKS.search(mquery)
        or _RE_SQL_DB.search(mquery)
        or _RE_ODBC.search(mquery)
        or _RE_ORACLE.search(mquery)
        or _RE_SNOWFLAKE.search(mquery)
    ):
        return None

    # TMDL binary expression (highest specificity)
    if _RE_TMDL_BINARY.search(mquery):
        return PBIOnlySource(
            kind=PBIOnlyKind.TMDL_EXPRESSION,
            description=(
                "Helper query encoded as a Binary.FromText expression in "
                "expressions.tmdl; decode via getDefinition."
            ),
        )

    # Typed table literal
    if _RE_TYPED_TABLE.search(mquery):
        return PBIOnlySource(
            kind=PBIOnlyKind.TYPED_TABLE,
            description="Table literal defined with #table() inside the M expression.",
        )

    # JSON document
    if _RE_JSON.search(mquery):
        return PBIOnlySource(
            kind=PBIOnlyKind.JSON_DOCUMENT,
            description="Inline JSON embedded in the M expression via Json.Document.",
        )

    # Web contents
    if _RE_WEB.search(mquery):
        url_m = _RE_WEB_URL.search(mquery)
        url = url_m.group(1) if url_m else None
        return PBIOnlySource(
            kind=PBIOnlyKind.WEB_CONTENTS,
            url=url,
            description=f"HTTP endpoint via Web.Contents{': ' + url if url else ''}.",
        )

    # Excel workbook — check BEFORE SharePoint because the M pattern for Excel files
    # on SharePoint uses *both* SharePoint.Files (to navigate to the file) AND
    # Excel.Workbook (to parse it).  The underlying data source is Excel; SharePoint
    # is merely the transport.
    if _RE_EXCEL.search(mquery):
        # URL is typically the SharePoint site in this pattern
        url_m = _RE_SHAREPOINT_URL.search(mquery) or _RE_EXCEL_URL.search(mquery)
        url = url_m.group(1) if url_m else None
        return PBIOnlySource(
            kind=PBIOnlyKind.EXCEL_WORKBOOK,
            url=url,
            description=f"Excel workbook via Excel.Workbook{': ' + url if url else ''}.",
        )

    # SharePoint (non-Excel files: CSV, JSON, etc.)
    if _RE_SHAREPOINT.search(mquery):
        url_m = _RE_SHAREPOINT_URL.search(mquery)
        url = url_m.group(1) if url_m else None
        return PBIOnlySource(
            kind=PBIOnlyKind.SHAREPOINT,
            url=url,
            description=f"SharePoint connector{': ' + url if url else ''}.",
        )

    return None


# ---------------------------------------------------------------------------
# Stub template helpers
# ---------------------------------------------------------------------------


def _dax_evaluate_stub(table_name: str, uc_target: str) -> str:
    return textwrap.dedent(f'''\
        """
        Snapshot '{table_name}' from the PBI semantic model into Unity Catalog.

        Re-run after each PBI refresh to keep the snapshot current.
        Credentials: TENANT_ID, CLIENT_ID, USER_NAME, USER_PASSWORD (service account),
                     DATABRICKS_HOST, DATABRICKS_TOKEN.
        """
        from datetime import datetime, timezone
        from typing import Callable, Any
        from dataclasses import dataclass

        @dataclass
        class Config:
            evaluate_fn: Callable[[str], list[dict]]   # runs DAX against PBI model
            execute_fn: Callable[[str], Any]           # runs SQL against UC (spark.sql)

        TABLE_NAME = "{table_name}"
        UC_TARGET  = "{uc_target}"


        def _sql_lit(v) -> str:
            if v is None:
                return "NULL"
            if isinstance(v, (int, float)):
                return repr(v)
            return "'" + str(v).replace("\\\\", "\\\\\\\\").replace("'", "\\\\'") + "'"


        def run(cfg: Config) -> None:
            rows = cfg.evaluate_fn(f"EVALUATE '{{TABLE_NAME}}'")
            if not rows:
                print(f"WARNING: {{TABLE_NAME}} returned 0 rows — skipping snapshot.")
                return

            columns = list(rows[0].keys())
            col_defs = ", ".join(f"{{c}} STRING" for c in columns)
            loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            vals = ",\\n  ".join(
                "(" + ", ".join(_sql_lit(row.get(c)) for c in columns) + ")"
                for row in rows
            )
            cfg.execute_fn(f"""
        CREATE OR REPLACE TABLE {{UC_TARGET}}
          COMMENT 'Snapshot of PBI table {{TABLE_NAME}}.  Loaded {{loaded_at}} UTC.'
        AS
        SELECT *, TIMESTAMP'{{loaded_at}}' AS loaded_at_utc
        FROM VALUES
          {{vals}}
          AS t({{', '.join(columns)}})
        """)
            print(f"OK  {{UC_TARGET}}: {{len(rows)}} rows")
    ''')


def _tmdl_decode_stub(table_name: str, uc_target: str) -> str:
    return textwrap.dedent(f'''\
        """
        Snapshot '{table_name}' (TMDL binary expression) into Unity Catalog.

        The data is stored as a Binary.FromText(...) blob in expressions.tmdl.
        Use the Fabric getDefinition API (service-principal token) to decode it.
        See load_pbi_only_tables.py :: fetch_company_codes() for the pattern.
        """
        import base64, json, zlib, re
        from typing import Callable, Any
        from dataclasses import dataclass
        from datetime import datetime, timezone

        @dataclass
        class Config:
            get_tmdl_fn: Callable[[], str]    # returns the expressions.tmdl text
            execute_fn: Callable[[str], Any]  # runs SQL against UC

        TABLE_NAME  = "{table_name}"
        UC_TARGET   = "{uc_target}"


        def _sql_lit(v) -> str:
            if v is None:
                return "NULL"
            if isinstance(v, (int, float)):
                return repr(v)
            return "'" + str(v).replace("\\\\", "\\\\\\\\").replace("'", "\\\\'") + "'"


        def run(cfg: Config) -> None:
            tmdl = cfg.get_tmdl_fn()
            m = re.search(
                rf'expression {{TABLE_NAME}} =.*?Binary\\.FromText\\("([^"]+)"',
                tmdl, re.S
            )
            if not m:
                raise ValueError(f"{{TABLE_NAME}} not found in expressions.tmdl")
            data = json.loads(zlib.decompress(base64.b64decode(m.group(1)), -15))
            if not data:
                print(f"WARNING: {{TABLE_NAME}} decoded to 0 rows — skipping.")
                return
            columns = list(data[0].keys())
            loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            vals = ",\\n  ".join(
                "(" + ", ".join(_sql_lit(row.get(c)) for c in columns) + ")"
                for row in data
            )
            cfg.execute_fn(f"""
        CREATE OR REPLACE TABLE {{UC_TARGET}}
          COMMENT 'Snapshot of PBI helper query {{TABLE_NAME}}.  Loaded {{loaded_at}} UTC.'
        AS
        SELECT *, TIMESTAMP'{{loaded_at}}' AS loaded_at_utc
        FROM VALUES
          {{vals}}
          AS t({{', '.join(columns)}})
        """)
            print(f"OK  {{UC_TARGET}}: {{len(data)}} rows")
    ''')


def _http_download_stub(table_name: str, uc_target: str, url: Optional[str]) -> str:
    url_str = url or "https://example.com/data"
    return textwrap.dedent(f'''\
        """
        Snapshot '{table_name}' (Web.Contents / JSON endpoint) into Unity Catalog.

        Adapt URL, headers, and parsing to the real endpoint.
        """
        import requests
        from typing import Callable, Any
        from dataclasses import dataclass
        from datetime import datetime, timezone

        @dataclass
        class Config:
            execute_fn: Callable[[str], Any]  # runs SQL against UC
            url: str = "{url_str}"
            headers: dict = None

        TABLE_NAME = "{table_name}"
        UC_TARGET  = "{uc_target}"


        def _sql_lit(v) -> str:
            if v is None:
                return "NULL"
            if isinstance(v, (int, float)):
                return repr(v)
            return "'" + str(v).replace("\\\\", "\\\\\\\\").replace("'", "\\\\'") + "'"


        def run(cfg: Config) -> None:
            resp = requests.get(cfg.url, headers=cfg.headers or {{}}, timeout=60)
            resp.raise_for_status()
            data = resp.json()  # adapt for non-JSON or nested structures
            if not data:
                print(f"WARNING: {{TABLE_NAME}} returned 0 rows — skipping.")
                return
            columns = list(data[0].keys())
            loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            vals = ",\\n  ".join(
                "(" + ", ".join(_sql_lit(row.get(c)) for c in columns) + ")"
                for row in data
            )
            cfg.execute_fn(f"""
        CREATE OR REPLACE TABLE {{UC_TARGET}}
          COMMENT 'Snapshot of PBI table {{TABLE_NAME}} from web endpoint.  Loaded {{loaded_at}} UTC.'
        AS
        SELECT *, TIMESTAMP'{{loaded_at}}' AS loaded_at_utc
        FROM VALUES
          {{vals}}
          AS t({{', '.join(columns)}})
        """)
            print(f"OK  {{UC_TARGET}}: {{len(data)}} rows")
    ''')


def _inline_values_stub(table_name: str, uc_target: str) -> str:
    return textwrap.dedent(f'''\
        """
        Snapshot '{table_name}' (typed-table / #table literal) into Unity Catalog.

        The data is embedded in the M expression.  Parse it from the TMDL
        partition definition and materialise it with the SQL below.
        """
        # TODO: extract the column names and typed values from the #table(...) literal
        # in the partition M expression.  Each row is a list inside the outer list.
        # Then build a CREATE OR REPLACE TABLE ... AS SELECT ... FROM VALUES ... statement.

        TABLE_NAME = "{table_name}"
        UC_TARGET  = "{uc_target}"

        # Example manual approach when the data is small and stable:
        #
        # ROWS = [
        #     ("col1_value", "col2_value"),
        #     ...
        # ]
        # spark.createDataFrame(ROWS, schema="col1 STRING, col2 STRING") \\
        #      .write.format("delta").mode("overwrite").saveAsTable(UC_TARGET)
    ''')


# ---------------------------------------------------------------------------
# Approach mapping
# ---------------------------------------------------------------------------

_APPROACH: dict = {
    PBIOnlyKind.EXCEL_WORKBOOK:   "dax_evaluate",
    PBIOnlyKind.SHAREPOINT:       "dax_evaluate",
    PBIOnlyKind.WEB_CONTENTS:     "http_download",
    PBIOnlyKind.JSON_DOCUMENT:    "http_download",
    PBIOnlyKind.TYPED_TABLE:      "inline_values",
    PBIOnlyKind.TMDL_EXPRESSION:  "tmdl_decode",
}

_STUB_BUILDERS = {
    "dax_evaluate":  lambda t, u, src: _dax_evaluate_stub(t, u),
    "tmdl_decode":   lambda t, u, src: _tmdl_decode_stub(t, u),
    "http_download": lambda t, u, src: _http_download_stub(t, u, src.url if src else None),
    "inline_values": lambda t, u, src: _inline_values_stub(t, u),
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_ingestion_task(
    table_name: str,
    kind: PBIOnlyKind,
    uc_catalog: str,
    uc_schema: str,
    *,
    source: Optional[PBIOnlySource] = None,
    uc_table_suffix: Optional[str] = None,
) -> IngestionTask:
    """Build an :class:`IngestionTask` for a PBI-only table.

    Parameters
    ----------
    table_name:
        PBI table name (as it appears in the model).
    kind:
        Detected source kind from :func:`detect_pbi_only_source`.
    uc_catalog:
        UC catalog for the snapshot table.
    uc_schema:
        UC schema for the snapshot table.
    source:
        The :class:`PBIOnlySource` returned by :func:`detect_pbi_only_source`,
        used to extract URLs for stubs and descriptions.
    uc_table_suffix:
        Override for the UC table name.  Defaults to a snake_case version of
        ``table_name`` with a ``snap_`` prefix.
    """
    # Build UC target name
    safe_name = re.sub(r"[^a-z0-9]+", "_", table_name.lower()).strip("_")
    table_suffix = uc_table_suffix or f"snap_{safe_name}"
    uc_target = f"`{uc_catalog}`.`{uc_schema}`.`{table_suffix}`"

    approach = _APPROACH.get(kind, "dax_evaluate")
    stub_builder = _STUB_BUILDERS.get(approach, _STUB_BUILDERS["dax_evaluate"])
    stub = stub_builder(table_name, uc_target, source)

    source_desc = (
        source.description
        if source
        else f"PBI-only source of kind {kind.value}"
    )

    return IngestionTask(
        table_name=table_name,
        uc_target=uc_target,
        kind=kind,
        source_description=source_desc,
        recommended_approach=approach,
        snapshot_loader_stub=stub,
    )
