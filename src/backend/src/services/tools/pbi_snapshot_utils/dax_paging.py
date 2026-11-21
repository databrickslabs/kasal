"""DAX for reading one Power BI table completely, in a checked number of queries.

Two strategies, both checked against a ``COUNTROWS`` probe:

* **single query** (``EVALUATE 'T'``): used when the table fits one query
  (at most ``min(100,000, 1,000,000 / columns)`` rows). There is no order to
  depend on because the snapshot is a set of rows.
* **keyset paging** (``TOPN(n, FILTER('T', 'T'[k] > last), 'T'[k], ASC)``): used
  for larger tables when the table has an int64 column ``k`` that is unique and
  never blank. The key is checked by the probe in the same request, so no
  duplicate row can be lost or read twice. The next page starts after the
  largest key read so far, so a page cut short by the 15 MB cap loses nothing.

Keys of type double or decimal are not paged on. DAX compares them as numbers,
but the literal used to continue a page would have to round-trip a float, and
that is not worth the risk. A table that needs such a key is refused.
"""

from __future__ import annotations

from dataclasses import dataclass

from src.services.tools.pbi_snapshot_utils.limits import (
    PAGE_ROWS,
    PBI_MAX_ROWS_PER_QUERY,
    PBI_MAX_VALUES_PER_QUERY,
)
from src.services.tools.pbi_snapshot_utils.type_map import SnapshotRefused
from src.services.tools.ucmv_source_layer_utils.pbi_client import dax_column, dax_table


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    pbi_type: str


def single_query_limit(column_count: int) -> int:
    """The most rows one query can return for a table with this many columns."""
    if column_count < 1:
        raise SnapshotRefused("the table has no columns")
    return min(PBI_MAX_ROWS_PER_QUERY, PBI_MAX_VALUES_PER_QUERY // column_count)


def page_size(column_count: int) -> int:
    """Rows per keyset page: the page constant, bounded by the values limit."""
    if column_count < 1:
        raise SnapshotRefused("the table has no columns")
    return min(PAGE_ROWS, PBI_MAX_VALUES_PER_QUERY // column_count)


def probe_query(table: str, columns: list[ColumnSpec]) -> str:
    """One DAX query: the row count, and for each int64 column its distinct and
    blank counts. The result is a single row with keys ``rows``, ``u<i>``, ``b<i>``
    (``i`` is the column's index in ``columns``).
    """
    parts = ['"rows", COUNTROWS(' + dax_table(table) + ")"]
    for index, column in enumerate(columns):
        if column.pbi_type.lower() != "int64":
            continue
        ref = dax_column(table, column.name)
        parts.append(f'"u{index}", DISTINCTCOUNT({ref})')
        parts.append(f'"b{index}", COUNTBLANK({ref})')
    return "EVALUATE ROW(" + ", ".join(parts) + ")"


def full_table_query(table: str) -> str:
    return f"EVALUATE {dax_table(table)}"


def keyset_first_page_query(table: str, key: str, rows: int) -> str:
    ref = dax_column(table, key)
    return f"EVALUATE TOPN({rows}, {dax_table(table)}, {ref}, ASC)"


def keyset_next_page_query(table: str, key: str, after: int, rows: int) -> str:
    """The page of rows whose key is greater than ``after``. ``after`` is an int,
    so it is written as a plain integer literal."""
    if isinstance(after, bool) or not isinstance(after, int):
        raise SnapshotRefused("a keyset position must be an integer")
    ref = dax_column(table, key)
    filtered = f"FILTER({dax_table(table)}, {ref} > {after})"
    return f"EVALUATE TOPN({rows}, {filtered}, {ref}, ASC)"


def unique_int_key(
    columns: list[ColumnSpec], probe_row: dict[str, object]
) -> str | None:
    """The first int64 column that is unique and never blank, or None.

    ``probe_row`` is the probe's result with its keys cleaned. Unique means the
    distinct count equals the row count. Blank means ``COUNTBLANK`` is zero.
    """
    rows = _as_int(probe_row.get("rows"))
    for index, column in enumerate(columns):
        if column.pbi_type.lower() != "int64":
            continue
        distinct = _as_int(probe_row.get(f"u{index}"))
        blanks = _as_int(probe_row.get(f"b{index}"))
        if rows is not None and distinct == rows and blanks == 0:
            return column.name
    return None


def _as_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return int(value)
