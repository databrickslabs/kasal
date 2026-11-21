"""Snapshot the PBI-only tables of one run into Unity Catalog, table by table.

Order for each table (any step can refuse, and a refusal writes nothing):

1. the table is not one whose rows are already embedded in the model (those are
   emitted as SQL by the source layer and need no copy);
2. the table is not under row-level security;
3. every column has a Databricks mapping;
4. the target name is absent or ours (checked BEFORE any data is read);
5. a COUNTROWS probe gives the row count; it must not exceed ``max_rows``;
6. the rows are read in one query or by keyset paging;
7. the rows read must equal the probe count, with unique keys and the declared
   columns, or the table is refused;
8. the rows are written with the marker, comment and timestamp (sql_writer).

A refused table is returned with its reason and stays an ingestion task. Nothing
is partially loaded: a table is written only after every check has passed.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Collection, Mapping, Sequence
from datetime import datetime, timezone

from src.services.tools.metric_view_utils.embedded_tables import decode_embedded_table
from src.services.tools.metric_view_utils.utils import to_snake_case
from src.services.tools.pbi_snapshot_utils import dax_paging
from src.services.tools.pbi_snapshot_utils.dax_paging import ColumnSpec
from src.services.tools.pbi_snapshot_utils.limits import PAGE_ROWS, SnapshotSettings
from src.services.tools.pbi_snapshot_utils.sql_writer import (
    SnapshotColumn,
    SqlRunFn,
    target_state,
    write_snapshot,
)
from src.services.tools.pbi_snapshot_utils.type_map import (
    SnapshotRefused,
    databricks_type,
    sql_literal,
)

logger = logging.getLogger(__name__)

#: ``pbi_query(dax) -> rows`` with keys cleaned (``Col`` not ``Table[Col]``).
PbiQueryFn = Callable[[str], Awaitable[list[dict[str, object]]]]

SNAPSHOT_NOTE = "snapshot, not refreshed"
_LEAF_PREFIX = "snap_"


def snapshot_leaf_name(table: str) -> str:
    """The Unity Catalog table name for a PBI table's snapshot: ``snap_<snake>``.

    It differs from the source-layer view name (``src_<snake>`` by default), so
    the two never collide.
    """
    return f"{_LEAF_PREFIX}{to_snake_case(table)}"


def _skipped(table: str, reason: str) -> dict[str, object]:
    return {"table": table, "status": "skipped", "reason": reason}


def _int_or_none(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return int(value)


def _key_value(row: Mapping[str, object], key: str) -> int:
    value = row.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise SnapshotRefused("the paging key holds a value that is not a whole number")
    return value


async def _read_keyset(
    pbi_query: PbiQueryFn,
    table: str,
    key: str,
    total: int,
    page_size: int,
) -> tuple[list[dict[str, object]], int]:
    """Read ``total`` rows by keyset pages on ``key``. Returns ``(rows, pages)``.

    Each page starts after the largest key read so far, and keys must strictly
    increase. The loop stops when the probe count is reached or a page is empty;
    the caller then checks the count.
    """
    collected: list[dict[str, object]] = []
    after: int | None = None
    pages = 0
    while len(collected) < total:
        if after is None:
            dax = dax_paging.keyset_first_page_query(table, key, page_size)
        else:
            dax = dax_paging.keyset_next_page_query(table, key, after, page_size)
        page = await pbi_query(dax)
        pages += 1
        if not page:
            break
        keys = [_key_value(row, key) for row in page]
        if after is not None and keys[0] <= after:
            raise SnapshotRefused("paging did not advance; the table is not read")
        if any(later <= earlier for earlier, later in zip(keys, keys[1:])):
            raise SnapshotRefused("a page is not in key order; the table is not read")
        collected.extend(page)
        after = keys[-1]
    return collected, pages


def _check_read(
    rows: Sequence[Mapping[str, object]],
    total: int,
    names: set[str],
    key: str | None,
) -> None:
    if len(rows) != total:
        raise SnapshotRefused(
            f"read {len(rows)} rows but the model reports {total}; not loaded"
        )
    for row in rows:
        if set(row.keys()) != names:
            raise SnapshotRefused("the columns read differ from the model's columns")
    if key is not None and len({row[key] for row in rows}) != total:
        raise SnapshotRefused("the paging key is not unique in the rows read")


def _literal_rows(
    rows: Sequence[Mapping[str, object]], specs: Sequence[ColumnSpec]
) -> list[str]:
    return [
        "(" + ", ".join(sql_literal(row[s.name], s.pbi_type) for s in specs) + ")"
        for row in rows
    ]


async def _snapshot_table(
    table: str,
    *,
    settings: SnapshotSettings,
    catalog: str,
    schema: str,
    mquery: str,
    rls_tables: Collection[str],
    columns: Sequence[tuple[str, str]] | None,
    pbi_query: PbiQueryFn,
    run_sql: SqlRunFn,
    taken_at: str,
) -> dict[str, object]:
    if mquery and decode_embedded_table(mquery) is not None:
        raise SnapshotRefused(
            "the rows are embedded in the model and the source layer already emits "
            "them as SQL; no copy is needed"
        )
    if table in rls_tables:
        raise SnapshotRefused(
            "row-level security applies to this table; a snapshot taken with one "
            "identity would show that identity's rows to everyone"
        )
    if not columns:
        raise SnapshotRefused("no column types for the table in the model metadata")
    specs = [ColumnSpec(name=name, pbi_type=pbi_type) for name, pbi_type in columns]
    snapshot_columns = [
        SnapshotColumn(name=s.name, databricks_type=databricks_type(s.pbi_type))
        for s in specs
    ]
    leaf = snapshot_leaf_name(table)
    # Raises SnapshotRefused for a table this tool did not create.
    await target_state(run_sql, catalog, schema, leaf)

    probe_rows = await pbi_query(dax_paging.probe_query(table, specs))
    probe = probe_rows[0] if probe_rows else {}
    total = _int_or_none(probe.get("rows"))
    if total is None:
        raise SnapshotRefused("the row count could not be read from Power BI")
    if total > settings.max_rows:
        raise SnapshotRefused(
            f"{total} rows is over pbi_snapshot_max_rows ({settings.max_rows}); "
            "not loaded"
        )
    names = {s.name for s in specs}
    ncols = len(specs)
    single_limit = dax_paging.single_query_limit(ncols)
    key = dax_paging.unique_int_key(specs, probe)
    pages = 1
    if total <= PAGE_ROWS and total <= single_limit:
        strategy = "single_query"
        rows = await pbi_query(dax_paging.full_table_query(table))
    elif key is not None:
        strategy = "keyset_pages"
        rows, pages = await _read_keyset(
            pbi_query, table, key, total, dax_paging.page_size(ncols)
        )
    elif total <= single_limit:
        strategy = "single_query"
        rows = await pbi_query(dax_paging.full_table_query(table))
    else:
        raise SnapshotRefused(
            f"{total} rows exceed one query and the table has no unique whole-number "
            "column to page on; not loaded"
        )
    _check_read(rows, total, names, key)
    literal_rows = _literal_rows(rows, specs)
    await write_snapshot(
        run_sql,
        catalog=catalog,
        schema=schema,
        table=leaf,
        columns=snapshot_columns,
        literal_rows=literal_rows,
        expected_rows=total,
        source_table=table,
        taken_at=taken_at,
    )
    return {
        "table": table,
        "status": "written",
        "target": f"{catalog}.{schema}.{leaf}",
        "rows": total,
        "strategy": strategy,
        "pages": pages,
        "taken_at": taken_at,
        "note": SNAPSHOT_NOTE,
    }


async def snapshot_pbi_only_tables(
    tables: Sequence[str],
    *,
    settings: SnapshotSettings,
    catalog: str,
    schema: str,
    mquery_expressions: Mapping[str, str],
    rls_tables: Collection[str],
    columns_by_table: Mapping[str, Sequence[tuple[str, str]]],
    pbi_query: PbiQueryFn,
    run_sql: SqlRunFn,
    now: datetime | None = None,
) -> list[dict[str, object]]:
    """Snapshot each listed PBI-only table. Returns one result per table.

    Off by default: with ``settings.enabled`` false, nothing is read or written
    and the result is empty. Each table is independent: a refusal or an error
    for one table does not stop the others.
    """
    if not settings.enabled:
        return []
    stamp = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    taken_at = stamp.strftime("%Y-%m-%d %H:%M:%S")
    results: list[dict[str, object]] = []
    for table in tables:
        try:
            result = await _snapshot_table(
                table,
                settings=settings,
                catalog=catalog,
                schema=schema,
                mquery=mquery_expressions.get(table, ""),
                rls_tables=rls_tables,
                columns=columns_by_table.get(table),
                pbi_query=pbi_query,
                run_sql=run_sql,
                taken_at=taken_at,
            )
        except SnapshotRefused as exc:
            result = _skipped(table, str(exc))
        except Exception as exc:  # noqa: BLE001 — one table must not stop the run
            logger.warning(
                "[pbi_snapshot] %s failed: %s", table, type(exc).__name__, exc_info=True
            )
            result = _skipped(table, f"snapshot failed ({type(exc).__name__})")
        results.append(result)
    return results
