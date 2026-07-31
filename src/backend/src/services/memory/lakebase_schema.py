"""Self-healing the Lakebase memory table.

The memory table's DDL lives in ``LakebaseService.initialize_tables``, which is
reachable from exactly ONE place: an admin-only HTTP endpoint. Nothing calls it
at startup and nothing calls it when memory is used. So a column added to that
DDL reaches a FRESH workspace and no other — every workspace whose table already
existed keeps the old shape indefinitely, because ``CREATE TABLE IF NOT EXISTS``
is a no-op on an existing table.

That failure is invisible, which is what makes it dangerous. Memory is
best-effort by design — every save is wrapped in ``except Exception: log`` — so a
missing column does not crash a run. It makes every INSERT and every SELECT fail
silently, forever, and the symptom reads as "memory isn't very good" rather than
"memory is broken". Recall returns nothing and no writes land.

Alembic does not manage Lakebase, so this is the equivalent of the ``_ensure_*``
helpers in ``db/session.py``: idempotent, additive, and run automatically. The
local SQLite backend already self-heals in ``LocalMemoryStorage._migrate_columns``
— this is the same guarantee for the backend that actually matters in production.

Runs at most once per process per table: one cheap ``information_schema`` lookup
on the first memory operation, and nothing at all after that.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text

logger = logging.getLogger(__name__)

# Columns added after the table's first release, in the shape
# ``LakebaseService.initialize_tables`` creates them. Keep the two in step: this
# is what an EXISTING table gets, that is what a NEW table gets.
_ADDED_COLUMNS: tuple[tuple[str, str], ...] = (
    ("kind", "TEXT NOT NULL DEFAULT 'episodic'"),
    ("valid_from", "TIMESTAMPTZ"),
    ("valid_to", "TIMESTAMPTZ"),
    ("superseded_by", "TEXT"),
)

# Tables checked in this process. The check is skipped entirely once a table is
# known good, so the steady-state cost of this module is one set lookup.
#
# No lock: ``ADD COLUMN IF NOT EXISTS`` is idempotent, so the worst case for two
# concurrent first-operations is that both run the same harmless statements. A
# lock here would have to be held across an await and bound to an event loop,
# and this backend deliberately bridges between loops.
_healed: set[str] = set()


def reset_schema_cache() -> None:
    """Forget which tables have been checked. For tests and process reuse."""
    _healed.clear()


def needs_check(table_name: str) -> bool:
    """Whether ``table_name`` still has to be checked in this process.

    Lets a caller skip even OPENING a connection once a table is known good —
    the check runs in its own session (see :func:`ensure_memory_columns`), and
    paying for one on every memory operation would be a poor trade for a set
    lookup.
    """
    return table_name not in _healed


async def ensure_memory_columns(session: Any, table_name: str) -> None:
    """Add any missing memory column to ``table_name``. Never raises.

    **Give this its own session.** ``get_lakebase_session`` commits on clean exit
    and ROLLS BACK on exception, and Postgres DDL is transactional — so running
    these statements inside the caller's transaction means a failure in the
    caller's own SQL silently undoes the repair. The table would stay broken
    while the cache below recorded it as fixed, which is worse than never having
    tried. :func:`needs_check` exists so the extra session is opened once rather
    than per operation.

    A failure here must not break the caller's operation: the operation is about
    to run its own SQL and will report the real error itself, which is more
    useful than one raised from a migration helper. The table is still marked as
    checked so a workspace whose credentials cannot ALTER does not pay for five
    failing statements on every single memory read and write.
    """
    if table_name in _healed:
        return
    try:
        result = await session.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = :table_name"
            ),
            {"table_name": table_name},
        )
        existing = {row[0] for row in result.fetchall()}
        # An empty result means the table is not visible to this connection at
        # all (wrong schema, or not created yet). ALTERing it would fail; leave
        # it to initialize_tables and let the caller's own SQL report the truth.
        if not existing:
            _healed.add(table_name)
            return

        missing = [(c, ddl) for c, ddl in _ADDED_COLUMNS if c not in existing]
        for column, ddl in missing:
            await session.execute(
                text(
                    f"ALTER TABLE {table_name} "
                    f"ADD COLUMN IF NOT EXISTS {column} {ddl}"
                )
            )
        if missing:
            # Recall filters on valid_to and scores on kind, so the index that
            # supports that pattern belongs with the columns.
            await session.execute(
                text(
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_current "
                    f"ON {table_name} (group_id, kind) WHERE valid_to IS NULL"
                )
            )
            logger.info(
                "Added %d missing column(s) to memory table %s: %s",
                len(missing),
                table_name,
                [column for column, _ in missing],
            )
    except Exception as exc:  # noqa: BLE001 — see the docstring
        logger.warning(
            "Could not ensure memory columns on %s (%s). If memory reads and "
            "writes fail, re-run Lakebase table initialization.",
            table_name,
            exc,
        )
    _healed.add(table_name)
