"""Elevate a Lakebase connection to the shared owner role, and enable pgvector.

Two problems on a deployed Lakebase share ONE root cause and ONE fix:

1. **``must be owner of table``** — the app connects as its own service
   principal, but a table may have been created by a DIFFERENT principal (an
   earlier deploy, another of the apps sharing this instance). ``ALTER TABLE``
   is owner-only in PostgreSQL, and ``GRANT ALL`` does NOT include it — so a
   column migration (e.g. ``executionhistory.crew_id``) fails, and because it
   fails first it aborts the whole self-heal transaction.

2. **``CREATE EXTENSION vector`` denied** — ``vector`` is not a trusted
   extension, so installing pgvector needs a superuser too.

On Lakebase, every identity provisioned on the instance — each app's service
principal AND the human admins — is a MEMBER of the ``databricks_superuser``
role. A member can ``SET ROLE`` to it, and while acting as it can ALTER any
table and create extensions, regardless of who owns what. Nothing is reassigned
away from anyone: ownership stays shared, every member keeps full control, and
all the apps on the instance can heal the schema.

So: before running DDL, ``SET ROLE databricks_superuser`` (best-effort), enable
pgvector, and create/alter as the shared role. On a plain BYO-Postgres with no
such role this is a no-op that logs and continues — behaviour is exactly what it
was before, so nothing regresses.
"""

import logging

from sqlalchemy import text

logger = logging.getLogger(__name__)

#: The role every Lakebase identity (app SPNs + admins) is a member of. Acting
#: as it bypasses table-ownership checks and permits CREATE EXTENSION.
SUPERUSER_ROLE = "databricks_superuser"


# Why every statement here runs in its OWN SAVEPOINT: the callers pass a
# connection that is ALREADY inside a transaction (the self-heal session, or an
# ``engine.begin()`` block). On PostgreSQL a failed statement aborts the whole
# transaction, and catching the Python exception does NOT un-abort it — every
# later statement then dies with "current transaction is aborted". SET ROLE (bad
# role name) and CREATE EXTENSION (not superuser) are both allowed to fail, so
# each must roll back to a savepoint to leave the transaction usable. This is the
# exact mechanism that turned one "must be owner" into a total no-op before.


async def _run_isolated_async(conn, sql: str, ok_log: str, fail_log: str) -> bool:
    try:
        nested = conn.begin_nested()
    except Exception:  # noqa: BLE001 — no savepoint support (test mock); run bare
        try:
            await conn.execute(text(sql))
            logger.info(ok_log)
            return True
        except Exception as exc:  # noqa: BLE001
            logger.info(f"{fail_log}: {exc}")
            return False
    try:
        async with nested:
            await conn.execute(text(sql))
        logger.info(ok_log)
        return True
    except Exception as exc:  # noqa: BLE001 — rolled back to the savepoint
        logger.info(f"{fail_log}: {exc}")
        return False


def _run_isolated_sync(conn, sql: str, ok_log: str, fail_log: str) -> bool:
    try:
        nested = conn.begin_nested()
    except Exception:  # noqa: BLE001
        try:
            conn.execute(text(sql))
            logger.info(ok_log)
            return True
        except Exception as exc:  # noqa: BLE001
            logger.info(f"{fail_log}: {exc}")
            return False
    try:
        with nested:
            conn.execute(text(sql))
        logger.info(ok_log)
        return True
    except Exception as exc:  # noqa: BLE001
        logger.info(f"{fail_log}: {exc}")
        return False


async def enter_superuser_async(conn) -> bool:
    """``SET ROLE databricks_superuser`` on an async connection. Best-effort.

    Returns True when the session is now acting as the shared owner role, False
    when the role does not exist / is not grantable (non-Lakebase Postgres),
    in which case the caller proceeds exactly as before.
    """
    return await _run_isolated_async(
        conn,
        f'SET ROLE "{SUPERUSER_ROLE}"',
        f"Elevated Lakebase DDL session to role {SUPERUSER_ROLE}",
        f"Could not SET ROLE {SUPERUSER_ROLE} (continuing as connecting role)",
    )


def enter_superuser_sync(conn) -> bool:
    """``SET ROLE databricks_superuser`` on a sync connection. Best-effort."""
    return _run_isolated_sync(
        conn,
        f'SET ROLE "{SUPERUSER_ROLE}"',
        f"Elevated Lakebase DDL session to role {SUPERUSER_ROLE}",
        f"Could not SET ROLE {SUPERUSER_ROLE} (continuing as connecting role)",
    )


async def enable_pgvector_async(conn) -> bool:
    """``CREATE EXTENSION IF NOT EXISTS vector``. Best-effort; requires superuser.

    Call AFTER :func:`enter_superuser_async`. Returns True when pgvector is
    available afterwards (created now or already present), False when it could
    not be installed — the caller then falls back to the vector-free schema path.
    """
    return await _run_isolated_async(
        conn,
        "CREATE EXTENSION IF NOT EXISTS vector",
        "pgvector extension ensured (CREATE EXTENSION IF NOT EXISTS vector)",
        "Could not enable pgvector (embedding columns will be skipped)",
    )


def enable_pgvector_sync(conn) -> bool:
    """``CREATE EXTENSION IF NOT EXISTS vector`` on a sync connection. Best-effort."""
    return _run_isolated_sync(
        conn,
        "CREATE EXTENSION IF NOT EXISTS vector",
        "pgvector extension ensured (CREATE EXTENSION IF NOT EXISTS vector)",
        "Could not enable pgvector (embedding columns will be skipped)",
    )
