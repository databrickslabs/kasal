"""Tables whose rows are embedded in the Power Query itself, as a deployable SQL base.

"Enter data" tables (mapping and selector tables) carry their rows in the M expression::

    Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("<base64>",
        BinaryEncoding.Base64), Compression.Deflate)),
        let _t = ((type nullable text) meta [Serialized.Text = true])
        in type table [Tab = _t, Type = _t, #"AC_UC_%age" = _t])

The payload is base64 of raw-deflate JSON: a list of rows, each a list of values in column
order. Decoding it needs no access to Power BI, so the table becomes a source-layer view over
``VALUES``, which the metric views can join like any other. The steps that follow in the M
(types, replaced values) are folded on top by the caller as usual.

Pure functions: no I/O.
"""

from __future__ import annotations

import base64
import binascii
import json
import re
import zlib

from .dax_condition_sql import _sql_str_literal
from .m_column_expressions import sql_ident

_PAYLOAD = re.compile(
    r"Table\.FromRows\(\s*Json\.Document\(\s*Binary\.Decompress\(\s*Binary\.FromText\(\s*\"([A-Za-z0-9+/=]+)\""
)
_TYPE_TABLE = re.compile(r"type\s+table\s*\[(.*?)\]\s*\)", re.DOTALL)
_COLUMN = re.compile(r'(?:#"((?:[^"]|"")+)"|([A-Za-z_][\w ]*?))\s*=\s*_t')
MAX_ROWS = 5000
# Deflate expands about 1000:1: bound what a model's payload may inflate to.
MAX_DECODED_BYTES = 8 * 1024 * 1024
MAX_DECLARATION_CHARS = 50_000


def _inflate(payload: str) -> bytes | None:
    """The raw-deflate payload, None when it is not one or inflates past the bound."""
    try:
        inflater = zlib.decompressobj(-15)
        data = inflater.decompress(base64.b64decode(payload), MAX_DECODED_BYTES + 1)
    except (ValueError, zlib.error, binascii.Error):
        return None
    if len(data) > MAX_DECODED_BYTES or inflater.unconsumed_tail:
        return None
    return data


def decode_embedded_table(
    m_expression: str,
) -> tuple[list[str], list[list[object]]] | None:
    """``(columns, rows)`` of a ``Table.FromRows(Json.Document(Binary.Decompress(...)))`` table."""
    payload = _PAYLOAD.search(m_expression or "")
    declared = (
        _TYPE_TABLE.search(m_expression or "", payload.end()) if payload else None
    )
    if not payload or not declared:
        return None
    if len(declared.group(1)) > MAX_DECLARATION_CHARS:
        return None
    columns = [
        (q or b).replace('""', '"').strip()
        for q, b in _COLUMN.findall(declared.group(1))
    ]
    data = _inflate(payload.group(1))
    if data is None:
        return None
    try:
        rows = json.loads(data)
    except ValueError:
        return None
    if not columns or not isinstance(rows, list) or len(rows) > MAX_ROWS:
        return None
    if not all(isinstance(r, list) and len(r) == len(columns) for r in rows):
        return None
    return columns, rows


def _literal(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    return _sql_str_literal(str(value))


def embedded_table_sql(m_expression: str) -> str:
    """``SELECT columns FROM VALUES ... AS t(columns)`` for an embedded table, "" if it is not one.
    Values stay text, as Power Query serialises them; the M's type steps cast them."""
    decoded = decode_embedded_table(m_expression)
    if decoded is None:
        return ""
    columns, rows = decoded
    if not rows:
        return ""
    values = ",\n  ".join(
        "(" + ", ".join(_literal(v) for v in row) + ")" for row in rows
    )
    names = ", ".join(sql_ident(c) for c in columns)
    return f"SELECT {names} FROM VALUES\n  {values}\nAS _embedded({names})"  # literals escaped — nosemgrep: tmp.databricks.sql-injection.fstring-select
