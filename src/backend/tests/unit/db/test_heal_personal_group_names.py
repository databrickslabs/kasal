"""Unit test for the workspace→teamspace group-name data heal
(_heal_personal_group_names).

Auto-created personal groups persisted "Personal Workspace - <email>" in
groups.name before the tenant concept was renamed to "teamspace" (the personal
tenant is now the "Personal Space"). The heal rewrites those rows in place on
startup. Verifies the rename, idempotency,
that non-personal / Databricks-unrelated names are untouched, and that a
missing table doesn't raise.
"""

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from src.db import session as sess


@pytest.mark.asyncio
async def test_heal_renames_personal_workspace_rows_and_is_idempotent():
    engine = create_async_engine("sqlite+aiosqlite://")  # in-memory, single shared conn
    try:
        async with engine.begin() as conn:
            await conn.exec_driver_sql('CREATE TABLE "groups" (id TEXT, name TEXT)')
            await conn.exec_driver_sql(
                'INSERT INTO "groups" VALUES '
                "('user_u_x_com', 'Personal Workspace - u@x.com'), "
                "('user_no_mail', 'Personal Workspace'), "
                "('acme_corp',   'Acme Corp'), "
                "('ws_admins',   'Workspace Admins')"  # user-authored name: untouched
            )

            await sess._heal_personal_group_names(conn)
            rows = {
                r[0]: r[1]
                for r in (
                    await conn.exec_driver_sql('SELECT id, name FROM "groups"')
                ).fetchall()
            }
            assert rows["user_u_x_com"] == "Personal Space - u@x.com"
            assert rows["user_no_mail"] == "Personal Space"
            assert rows["acme_corp"] == "Acme Corp"
            assert rows["ws_admins"] == "Workspace Admins"

            # idempotent: second run changes nothing
            await sess._heal_personal_group_names(conn)
            rows2 = {
                r[0]: r[1]
                for r in (
                    await conn.exec_driver_sql('SELECT id, name FROM "groups"')
                ).fetchall()
            }
            assert rows2 == rows
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_heal_noop_when_table_missing():
    """Fresh DB (create_all not yet run): the heal must swallow the error."""
    engine = create_async_engine("sqlite+aiosqlite://")
    try:
        async with engine.begin() as conn:
            await sess._heal_personal_group_names(conn)  # must not raise
    finally:
        await engine.dispose()
