"""Lakebase preflight diagnostics.

Runs when the user connects to / enables Lakebase. It verifies that the app's
service principal can actually OPERATE the schema Kasal needs — and when it
cannot, returns precise, actionable remediation instead of letting the app fail
later with a cryptic ``column ... does not exist`` on every request.

Why this is necessary (all confirmed against Lakebase behaviour):

* PostgreSQL ``ALTER TABLE ... ADD COLUMN`` is **owner-only** — privileges,
  even "ALL PRIVILEGES", do not grant it.
* On Lakebase, ``SET ROLE`` is **disabled** (role flag ``set=F``) and
  ``databricks_superuser`` is ``NOLOGIN`` and **not a real superuser**, so it
  cannot ``ALTER OWNER`` / ``REASSIGN`` another role's objects.
* A Databricks App's Postgres role name is its service-principal client id. If
  the app is **deleted and recreated**, a NEW service principal is minted, and
  the tables the OLD service principal created become **orphaned** — the new SP
  can read/write the data (``pg_read_all_data`` / ``pg_write_all_data``) but can
  never ``ALTER`` those tables, and there is no in-code workaround.

So Kasal's schema self-heal can only add columns to tables the current SP
**owns**. This preflight tells the operator, up front, whether that holds — and
if not, exactly what to do.

The expected schema (tables + columns) is read from the SQLAlchemy models, so
this check never drifts from what the app actually queries.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set

from sqlalchemy import text

logger = logging.getLogger(__name__)

STATUS_HEALTHY = "healthy"
STATUS_ACTION_REQUIRED = "action_required"
STATUS_ERROR = "error"


def _expected_schema() -> Dict[str, Set[str]]:
    """{table_name: {column_names}} from the SQLAlchemy models.

    Importing ``src.models`` ensures every mapped table is registered on the
    shared ``Base.metadata`` before we read it.
    """
    import src.models  # noqa: F401 — registers all models on Base.metadata
    from src.db.base import Base

    return {
        table.name: {c.name for c in table.columns}
        for table in Base.metadata.tables.values()
    }


async def run_lakebase_preflight(conn) -> Dict[str, Any]:
    """Run the preflight against an ACTIVE Lakebase connection.

    ``conn`` is an async SQLAlchemy connection already pointed at the Lakebase
    database (the caller owns its lifecycle). Never raises: any failure is
    reported as ``status=error`` with the reason, because this sits on the
    connect path and its whole job is to explain problems, not create them.
    """
    report: Dict[str, Any] = {
        "status": STATUS_HEALTHY,
        "current_user": None,
        "checks": [],
        "tables": [],
        "remediation": None,
    }
    checks: List[Dict[str, Any]] = report["checks"]

    try:
        current_user = (await conn.execute(text("SELECT current_user"))).scalar()
        report["current_user"] = current_user
        checks.append(
            {
                "name": "connectivity",
                "ok": True,
                "detail": f"connected as {current_user}",
            }
        )
    except Exception as e:  # noqa: BLE001
        report["status"] = STATUS_ERROR
        checks.append({"name": "connectivity", "ok": False, "detail": str(e)[:300]})
        return report

    # pgvector — informational; embedding features need it but its absence is not
    # what this preflight gates on.
    try:
        has_vec = (
            await conn.execute(
                text("SELECT 1 FROM pg_extension WHERE extname = 'vector'")
            )
        ).scalar()
        checks.append(
            {
                "name": "pgvector",
                "ok": bool(has_vec),
                "detail": (
                    "installed"
                    if has_vec
                    else "not installed (embedding features degrade)"
                ),
            }
        )
    except Exception as e:  # noqa: BLE001
        checks.append({"name": "pgvector", "ok": False, "detail": str(e)[:200]})

    expected = _expected_schema()
    names = list(expected.keys())

    # Owners of the tables that exist, and their actual columns — two batched reads.
    try:
        owner_rows = (
            await conn.execute(
                text(
                    "SELECT tablename, tableowner FROM pg_tables "
                    "WHERE schemaname NOT IN ('pg_catalog','information_schema') "
                    "AND tablename = ANY(:names)"
                ),
                {"names": names},
            )
        ).fetchall()
        owners: Dict[str, str] = {r[0]: r[1] for r in owner_rows}

        col_rows = (
            await conn.execute(
                text(
                    "SELECT table_name, column_name FROM information_schema.columns "
                    "WHERE table_name = ANY(:names)"
                ),
                {"names": names},
            )
        ).fetchall()
        actual_cols: Dict[str, Set[str]] = {}
        for tname, cname in col_rows:
            actual_cols.setdefault(tname, set()).add(cname)
    except Exception as e:  # noqa: BLE001
        report["status"] = STATUS_ERROR
        checks.append(
            {"name": "schema_introspection", "ok": False, "detail": str(e)[:300]}
        )
        return report

    orphaned: List[Dict[str, Any]] = []
    self_fixable: List[Dict[str, Any]] = []

    for tname in names:
        if tname not in owners:
            # Table not present yet. On first use the app CREATEs it and thus
            # OWNS it, so this is not a problem to flag here.
            report["tables"].append(
                {
                    "name": tname,
                    "exists": False,
                    "owner": None,
                    "owned_by_app": None,
                    "missing_columns": [],
                    "status": "absent",
                }
            )
            continue
        owner = owners[tname]
        owned_by_app = owner == current_user
        missing = sorted(expected[tname] - actual_cols.get(tname, set()))
        if not missing:
            status = "ok"
        elif owned_by_app:
            # The app owns it, so the self-heal CAN add the column. Not a blocker.
            status = "auto_fixable"
            self_fixable.append({"table": tname, "missing_columns": missing})
        else:
            # The app does NOT own it and columns are missing → cannot be fixed
            # in code on Lakebase. This is the blocker.
            status = "action_required"
            orphaned.append(
                {"table": tname, "owner": owner, "missing_columns": missing}
            )
        report["tables"].append(
            {
                "name": tname,
                "exists": True,
                "owner": owner,
                "owned_by_app": owned_by_app,
                "missing_columns": missing,
                "status": status,
            }
        )

    checks.append(
        {
            "name": "schema_ownership",
            "ok": not orphaned,
            "detail": (
                "all required tables are owned by this app's service principal"
                if not orphaned
                else f"{len(orphaned)} table(s) owned by another principal are missing columns"
            ),
        }
    )
    if self_fixable:
        checks.append(
            {
                "name": "self_heal_pending",
                "ok": True,
                "detail": f"{len(self_fixable)} owned table(s) will be auto-migrated on startup",
            }
        )

    if orphaned:
        report["status"] = STATUS_ACTION_REQUIRED
        report["remediation"] = _build_remediation(current_user, orphaned)

    return report


async def preflight_via_service(
    service, instance_name: Optional[str] = None
) -> Dict[str, Any]:
    """Build a Lakebase connection the way ``check_lakebase_tables`` does, then
    run the preflight against it. Never raises — returns ``status=error`` on any
    setup failure (instance not found, no endpoint, auth, …).

    ``service`` is a ``LakebaseService`` (passed untyped to avoid an import
    cycle); it supplies the config, instance lookup and connection helpers.
    ``instance_name`` overrides the configured instance (used by test-connection,
    where the operator is trying an instance before saving it).
    """
    try:
        if not instance_name:
            config = await service.get_config()
            instance_name = config.get("instance_name", "kasal-lakebase")
        instance = await service.get_instance(instance_name)
        if not instance or instance.get("state") == "NOT_FOUND":
            return {
                "status": STATUS_ERROR,
                "current_user": None,
                "checks": [
                    {
                        "name": "instance",
                        "ok": False,
                        "detail": f"instance '{instance_name}' not found",
                    }
                ],
                "tables": [],
                "remediation": None,
            }
        endpoint = instance.get("read_write_dns")
        if not endpoint:
            return {
                "status": STATUS_ERROR,
                "current_user": None,
                "checks": [
                    {
                        "name": "instance",
                        "ok": False,
                        "detail": "instance has no read_write endpoint",
                    }
                ],
                "tables": [],
                "remediation": None,
            }
        username = await service.connection_service.get_username()
        cred = await service.connection_service.generate_credentials(instance_name)
        engine = await service.connection_service.create_lakebase_engine_async(
            endpoint, username, cred.token
        )
        try:
            async with engine.begin() as conn:
                return await run_lakebase_preflight(conn)
        finally:
            try:
                await engine.dispose()
            except Exception:  # noqa: BLE001
                pass
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Lakebase preflight setup failed: {e}")
        return {
            "status": STATUS_ERROR,
            "current_user": None,
            "checks": [{"name": "setup", "ok": False, "detail": str(e)[:300]}],
            "tables": [],
            "remediation": None,
        }


def _build_remediation(
    app_sp: Optional[str], orphaned: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Actionable fix for the orphaned-ownership case, tailored to the findings."""
    owners = sorted({o["owner"] for o in orphaned})
    tables = ", ".join(o["table"] for o in orphaned)
    old_owner = owners[0] if len(owners) == 1 else "the previous service principal"

    summary = (
        f"Kasal cannot update the Lakebase schema: table(s) [{tables}] are owned "
        f"by a different Postgres role ({', '.join(owners)}), not this app's "
        f"service principal ({app_sp}). On PostgreSQL, adding a column is "
        f"owner-only; on Lakebase SET ROLE is disabled and databricks_superuser "
        f"cannot reassign another role's objects, so the app cannot fix this "
        f"itself. This typically happens when a Databricks App is deleted and "
        f"recreated (a new service principal is minted, orphaning the old one's "
        f"tables)."
    )
    steps = [
        "Recommended — point Kasal at a fresh schema this app owns: set the "
        "Lakebase schema to a new name (e.g. 'kasal2') the current service "
        "principal will create and own; Kasal builds the full up-to-date schema "
        "there. (Existing data stays in the old schema; migrate it if needed.)",
        "Or reassign ownership: in Compute → Apps → this app → Edit, REMOVE the "
        "Lakebase (Database) resource with the 'Can manage' permission — the "
        "platform reassigns the old service principal's objects — then re-add the "
        "resource and redeploy.",
        "Prevention: redeploy the app in place; never delete + recreate it (that "
        "rotates the service principal). If you must recreate, remove the Lakebase "
        "resource first so ownership is reassigned cleanly.",
    ]
    # Reference SQL for an operator who has a privileged Postgres identity
    # (the object owner, or Databricks support / cloud_admin). A plain app SP
    # cannot run these on Lakebase (SET ROLE is disabled).
    commands = [
        f"-- Run as a role that can act as {old_owner} (owner / cloud_admin):",
        f'REASSIGN OWNED BY "{old_owner}" TO "{app_sp}";',
    ] + [f'ALTER TABLE "{o["table"]}" OWNER TO "{app_sp}";' for o in orphaned]
    return {"summary": summary, "steps": steps, "commands": commands}
