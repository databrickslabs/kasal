"""The startup data heal that moves Genie One registrations to the GA endpoint.

Databricks retires the Beta workspace-wide Genie MCP (``{host}/api/2.0/mcp/genie``)
on 2026-10-31; the GA service is ``system.ai.genie_one_mcp`` behind Unity
Gateway. ``_migrate_genie_one_mcp_urls`` rewrites stored rows on every startup:
same host, new path, and a Databricks ``auth_type`` (the runtime only attaches
Databricks credentials to a non-``/api/2.0/mcp/`` URL when ``auth_type`` says
so). Per-space Genie servers and every other URL stay as they are.
"""

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from src.db.self_heal import data as heal
from src.models.mcp_server import MCPServer

HOST = "https://workspace.example.com"
GA = f"{HOST}/ai-gateway/mcp-services/system.ai.genie_one_mcp"

# id -> (server_url, auth_type)
ROWS = {
    1: (f"{HOST}/api/2.0/mcp/genie", "databricks_obo"),
    2: (f"{HOST}/api/2.0/mcp/genie/", "databricks_spn"),
    3: ("https://other.example.com/api/2.0/mcp/genie", "api_key"),
    4: (f"{HOST}/api/2.0/mcp/genie", None),
    # Never touched:
    10: (f"{HOST}/api/2.0/mcp/genie/01f0space", "databricks_obo"),
    11: (f"{HOST}/api/2.0/mcp/sql", "databricks_obo"),
    12: (GA, "databricks_obo"),
    13: ("https://proxy.example.com/prefix/api/2.0/mcp/genie", "api_key"),
    14: ("https://proxy.example.com/f?u=https://w.example.com/api/2.0/mcp/genie", None),
    15: (f"{HOST}/api/2.0/mcp/genie?x=1", "api_key"),
    16: (f"{HOST}/api/2.0/mcp/genies", "api_key"),
}

EXPECTED = {
    1: (GA, "databricks_obo"),
    2: (GA, "databricks_spn"),
    3: (
        "https://other.example.com/ai-gateway/mcp-services/system.ai.genie_one_mcp",
        "databricks_obo",
    ),
    4: (GA, "databricks_obo"),
    **{row_id: ROWS[row_id] for row_id in (10, 11, 12, 13, 14, 15, 16)},
}


async def _snapshot(conn):
    result = await conn.execute(
        text("SELECT id, name, server_url, auth_type, group_id FROM mcp_servers")
    )
    return {row[0]: tuple(row[1:]) for row in result.fetchall()}


@pytest.mark.asyncio
async def test_migrates_beta_genie_one_rows_and_leaves_the_rest():
    engine = create_async_engine("sqlite+aiosqlite://")
    try:
        async with engine.begin() as conn:
            await conn.run_sync(MCPServer.__table__.create)
            for row_id, (url, auth) in ROWS.items():
                await conn.execute(
                    text(
                        "INSERT INTO mcp_servers (id, name, server_url, auth_type, "
                        "group_id) VALUES (:id, :name, :url, :auth, :gid)"
                    ),
                    {
                        "id": row_id,
                        "name": f"server {row_id}",
                        "url": url,
                        "auth": auth,
                        "gid": None if row_id % 2 else f"team_{row_id}",
                    },
                )
            before = await _snapshot(conn)

            await heal._migrate_genie_one_mcp_urls(conn)
            after = await _snapshot(conn)

            got = {row_id: (row[1], row[2]) for row_id, row in after.items()}
            assert got == EXPECTED
            # Names and tenant scope never change: crews resolve servers by name.
            for row_id in ROWS:
                assert after[row_id][0] == before[row_id][0]
                assert after[row_id][3] == before[row_id][3]

            # Idempotent: a second startup changes nothing.
            await heal._migrate_genie_one_mcp_urls(conn)
            assert await _snapshot(conn) == after
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_a_missing_table_is_isolated_by_the_pass_not_swallowed():
    """The step raises so its SAVEPOINT rolls back; the pass records it and
    carries on. Swallowing it here aborted PostgreSQL's transaction."""
    from src.db.self_heal.runner import _run_self_heal_step

    engine = create_async_engine("sqlite+aiosqlite://")
    try:
        async with engine.begin() as conn:
            with pytest.raises(Exception, match="mcp_servers"):
                await heal._migrate_genie_one_mcp_urls(conn)
            assert (
                await _run_self_heal_step(conn, heal._migrate_genie_one_mcp_urls)
                is False
            )
    finally:
        await engine.dispose()


def test_heal_targets_the_same_paths_as_the_presets():
    """src.db may not import services, so the heal keeps literal copies."""
    from src.services.mcp.mcp_client.databricks_presets import (
        GENIE_ONE_BETA_PATH,
        GENIE_ONE_PATH,
    )

    assert heal._GENIE_ONE_GA_PATH == GENIE_ONE_PATH
    assert heal._GENIE_ONE_BETA_PATH == GENIE_ONE_BETA_PATH


@pytest.mark.asyncio
async def test_the_startup_self_heal_pass_runs_it():
    """Wired into run_schema_self_heal, the pass init_db and the Lakebase swap run."""
    from src.db.self_heal.runner import run_schema_self_heal

    engine = create_async_engine("sqlite+aiosqlite://")
    try:
        async with engine.begin() as conn:
            await conn.run_sync(MCPServer.__table__.create)
            await conn.execute(
                text(
                    "INSERT INTO mcp_servers (id, name, server_url, auth_type) "
                    "VALUES (1, 'databricks genie: genie one', :url, 'databricks_obo')"
                ),
                {"url": f"{HOST}/api/2.0/mcp/genie"},
            )
            await run_schema_self_heal(conn)
            url = (
                await conn.execute(text("SELECT server_url FROM mcp_servers"))
            ).scalar_one()
            assert url == GA
    finally:
        await engine.dispose()
