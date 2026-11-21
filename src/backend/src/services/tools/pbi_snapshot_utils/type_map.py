"""Power BI column types to Databricks types, and typed SQL literals for snapshot rows.

The Power BI type names are the Tabular ``dataType`` values (as in TMDL and the
model metadata), compared case-insensitively. A type not listed here refuses
the table: nothing is silently cast to STRING.

``executeQueries`` returns values as JSON, so a DECIMAL column arrives as a JSON
number. It is stored as DOUBLE and the precision is that of a JSON double.
Binary and variant columns have no faithful JSON form and are refused.
"""

from __future__ import annotations

import math
import re
from datetime import datetime

from src.services.tools.metric_view_utils.dax_condition_sql import _sql_str_literal

#: Power BI dataType (lower case) -> (Databricks type, python kind for values).
_TYPE_MAP: dict[str, tuple[str, str]] = {
    "string": ("STRING", "string"),
    "int64": ("BIGINT", "int64"),
    "double": ("DOUBLE", "double"),
    "decimal": ("DOUBLE", "double"),
    "boolean": ("BOOLEAN", "boolean"),
    "datetime": ("TIMESTAMP_NTZ", "datetime"),
}

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
_IDENT_CONTROL = re.compile(r"[\x00-\x1f\x7f]")


class SnapshotRefused(Exception):
    """A table cannot be snapshotted; the message is the reason shown to the user."""


def databricks_type(pbi_type: str) -> str:
    """The Databricks type for a Power BI dataType, or ``SnapshotRefused``."""
    entry = _TYPE_MAP.get((pbi_type or "").strip().lower())
    if entry is None:
        raise SnapshotRefused(
            f"column type '{pbi_type}' has no Databricks mapping in the snapshot"
        )
    return entry[0]


def python_kind(pbi_type: str) -> str:
    entry = _TYPE_MAP.get((pbi_type or "").strip().lower())
    if entry is None:
        raise SnapshotRefused(
            f"column type '{pbi_type}' has no Databricks mapping in the snapshot"
        )
    return entry[1]


def quote_identifier(name: str) -> str:
    """Back-quote one identifier part; a back-quote inside is doubled.

    Hyphens and spaces are allowed inside the quotes. Control characters are
    refused: they have no business in a model name and could break the statement.
    """
    text = str(name)
    if not text or _IDENT_CONTROL.search(text):
        raise SnapshotRefused(f"identifier {text!r} is empty or has control characters")
    return "`" + text.replace("`", "``") + "`"


def qualified_name(catalog: str, schema: str, table: str) -> str:
    """``catalog.schema.table`` with every part quoted."""
    return ".".join(quote_identifier(part) for part in (catalog, schema, table))


def sql_literal(value: object, pbi_type: str) -> str:
    """One row value as a Databricks SQL literal of the mapped type.

    Strings go through the shared escaper (``_sql_str_literal``). Numbers are
    written as quoted text and cast, so no value is ever spliced in as raw
    syntax. A value that does not match its declared type refuses the table.
    """
    kind = python_kind(pbi_type)
    if value is None:
        return "NULL"
    if kind == "string":
        if not isinstance(value, str):
            raise SnapshotRefused(f"a {pbi_type} column holds a non-text value")
        return _sql_str_literal(value)
    if kind == "boolean":
        if not isinstance(value, bool):
            raise SnapshotRefused("a boolean column holds a non-boolean value")
        return "TRUE" if value else "FALSE"
    if kind == "int64":
        if isinstance(value, bool) or not isinstance(value, int):
            raise SnapshotRefused("an int64 column holds a non-integer value")
        if not _INT64_MIN <= value <= _INT64_MAX:
            raise SnapshotRefused("an int64 value is outside the 64-bit range")
        return f"CAST('{value}' AS BIGINT)"
    if kind == "double":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise SnapshotRefused("a numeric column holds a non-numeric value")
        number = float(value)
        if not math.isfinite(number):
            raise SnapshotRefused("a numeric column holds NaN or infinity")
        return f"CAST('{number!r}' AS DOUBLE)"
    # datetime
    if not isinstance(value, str):
        raise SnapshotRefused("a datetime column holds a non-text value")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise SnapshotRefused("a datetime value is not ISO-8601") from exc
    if parsed.tzinfo is not None:
        raise SnapshotRefused("a datetime value carries a time zone; refused")
    return f"CAST('{parsed.isoformat(sep=' ')}' AS TIMESTAMP_NTZ)"
