"""Writing a snapshot table in Unity Catalog, safely.

Every snapshot table carries the table property ``kasal.pbi_snapshot = 'true'``.
A target that exists WITHOUT that property is never replaced. A view is never
replaced either.

Write order:

1. the target must be absent or carry the marker;
2. a staging table is created with the declared column types and marked;
3. the rows are inserted in batches of literal values;
4. the staging table's row count must equal the number of rows read;
5. ``CREATE OR REPLACE TABLE target ... AS SELECT * FROM staging`` (with the
   comment and the marker), then the staging table is dropped.

Only the schema the run writes its source layer to is used. The schema is not
created here: if it is missing, the first statement fails and nothing is written.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass

from src.services.tools.metric_view_utils.dax_condition_sql import _sql_str_literal
from src.services.tools.pbi_snapshot_utils.type_map import (
    SnapshotRefused,
    qualified_name,
    quote_identifier,
)

logger = logging.getLogger(__name__)

#: ``run_sql(sql) -> data_array rows (lists of text/None)``; raises on failure.
SqlRunFn = Callable[[str], Awaitable[Sequence[Sequence[object]]]]

MARKER_KEY = "kasal.pbi_snapshot"
TAKEN_AT_KEY = "kasal.pbi_snapshot.taken_at"
INSERT_BATCH_ROWS = 500

ABSENT = "absent"
OURS = "ours"
_TABLE_TYPES = frozenset({"MANAGED", "EXTERNAL"})


@dataclass(frozen=True)
class SnapshotColumn:
    name: str
    databricks_type: str


def _text(value: object) -> str:
    return "" if value is None else str(value)


async def target_state(run_sql: SqlRunFn, catalog: str, schema: str, table: str) -> str:
    """``absent``, ``ours`` (carries the marker), or raises ``SnapshotRefused``."""
    info = (
        f"SELECT table_type FROM {quote_identifier(catalog)}.information_schema.tables "
        f"WHERE table_schema = {_sql_str_literal(schema)} "
        f"AND table_name = {_sql_str_literal(table)}"
    )
    found = await run_sql(info)
    if not found:
        return ABSENT
    if _text(found[0][0]).upper() not in _TABLE_TYPES:
        raise SnapshotRefused("the target name is taken by a view or other object")
    props = await run_sql(
        f"SHOW TBLPROPERTIES {qualified_name(catalog, schema, table)}"
    )
    for row in props:
        if len(row) >= 2 and _text(row[0]) == MARKER_KEY and _text(row[1]) == "true":
            return OURS
    raise SnapshotRefused(
        "a table with this name already exists and was not created by a snapshot; "
        "it is not replaced"
    )


def _create_table_sql(name: str, columns: Sequence[SnapshotColumn]) -> str:
    cols = ", ".join(f"{quote_identifier(c.name)} {c.databricks_type}" for c in columns)
    return f"CREATE TABLE {name} ({cols}) " f"TBLPROPERTIES ('{MARKER_KEY}' = 'true')"


def _insert_statements(
    name: str, columns: Sequence[SnapshotColumn], literal_rows: Sequence[str]
) -> list[str]:
    """``INSERT`` statements of at most ``INSERT_BATCH_ROWS`` value tuples each.
    Each tuple is already a sequence of safe literals in column order."""
    statements: list[str] = []
    for start in range(0, len(literal_rows), INSERT_BATCH_ROWS):
        batch = literal_rows[start : start + INSERT_BATCH_ROWS]
        values = ",\n".join(batch)
        statements.append(f"INSERT INTO {name} VALUES\n{values}")
    return statements


async def write_snapshot(
    run_sql: SqlRunFn,
    *,
    catalog: str,
    schema: str,
    table: str,
    columns: Sequence[SnapshotColumn],
    literal_rows: Sequence[str],
    expected_rows: int,
    source_table: str,
    taken_at: str,
) -> None:
    """Write the rows to ``catalog.schema.table``. Raises on any failure.

    ``literal_rows`` holds one ``(v1, v2, ...)`` text per row, built with
    ``type_map.sql_literal``. ``expected_rows`` is the count read from Power BI.
    """
    if len(literal_rows) != expected_rows:
        raise SnapshotRefused("the rows to write do not match the rows read")
    target = qualified_name(catalog, schema, table)
    stage = qualified_name(catalog, schema, f"{table}__stage_{uuid.uuid4().hex[:8]}")
    comment = (
        f"Snapshot of Power BI table {source_table}, taken {taken_at} UTC. "
        "Copied from Power BI, not refreshed; re-run the snapshot to update it."
    )
    await run_sql(_create_table_sql(stage, columns))
    try:
        for statement in _insert_statements(stage, columns, literal_rows):
            await run_sql(statement)
        counted = await run_sql(f"SELECT COUNT(*) FROM {stage}")
        written = int(_text(counted[0][0])) if counted else -1
        if written != expected_rows:
            raise SnapshotRefused(
                f"the staged copy has {written} rows, {expected_rows} were read"
            )
        await run_sql(
            f"CREATE OR REPLACE TABLE {target} "
            f"COMMENT {_sql_str_literal(comment)} "
            f"TBLPROPERTIES ('{MARKER_KEY}' = 'true', "
            f"'{TAKEN_AT_KEY}' = {_sql_str_literal(taken_at)}) "
            f"AS SELECT * FROM {stage}"
        )
    finally:
        # A failed drop must not hide the error that made the write fail.
        try:
            await run_sql(f"DROP TABLE IF EXISTS {stage}")
        except Exception:  # noqa: BLE001 — cleanup only; the write's own error wins
            logger.warning("[pbi_snapshot] could not drop staging table %s", stage)
