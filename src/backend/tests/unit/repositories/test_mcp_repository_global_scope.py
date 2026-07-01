"""
Repository tests for the GLOBAL + per-workspace override resolution model.

A base server (group_id IS NULL) that is globally available (enabled=True) is
visible to every workspace; a group-specific row of the same name shadows the
base for THAT group only. Disabled base servers are hidden from workspaces.
"""

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from src.models.mcp_server import MCPServer
from src.repositories.mcp_repository import MCPServerRepository


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MCPServer.__table__.create)
    maker = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with maker() as s:
        yield s
    await engine.dispose()


async def _add(session, name, group_id=None, enabled=True):
    srv = MCPServer(
        name=name,
        server_url=f"https://ws/api/2.0/mcp/external/{name}",
        server_type="streamable",
        auth_type="databricks_spn",
        enabled=enabled,
        group_id=group_id,
    )
    session.add(srv)
    await session.flush()
    return srv


def _names(servers):
    return sorted(s.name for s in servers)


@pytest.mark.asyncio
async def test_globally_available_base_is_visible_to_all_workspaces(session):
    repo = MCPServerRepository(session)
    await _add(session, "global_a", group_id=None, enabled=True)

    assert _names(await repo.list_for_group_scope("ws1")) == ["global_a"]
    assert _names(await repo.list_for_group_scope("ws2")) == ["global_a"]


@pytest.mark.asyncio
async def test_globally_unavailable_base_is_hidden_from_workspaces(session):
    repo = MCPServerRepository(session)
    await _add(session, "unpublished", group_id=None, enabled=False)

    assert await repo.list_for_group_scope("ws1") == []
    # …but the system-admin catalog still lists it.
    assert _names(await repo.find_all_base()) == ["unpublished"]


@pytest.mark.asyncio
async def test_workspace_override_shadows_base_only_for_that_workspace(session):
    repo = MCPServerRepository(session)
    await _add(session, "shared", group_id=None, enabled=True)
    # ws1 disabled it for itself (override row, enabled=False).
    await _add(session, "shared", group_id="ws1", enabled=False)

    # ws1 sees only its (disabled) override, not the base.
    ws1 = await repo.list_for_group_scope("ws1")
    assert len(ws1) == 1 and ws1[0].group_id == "ws1" and ws1[0].enabled is False
    # ws2 is unaffected — still sees the globally-available base.
    ws2 = await repo.list_for_group_scope("ws2")
    assert len(ws2) == 1 and ws2[0].group_id is None and ws2[0].enabled is True


@pytest.mark.asyncio
async def test_workspace_own_server_is_listed(session):
    repo = MCPServerRepository(session)
    await _add(session, "global_a", group_id=None, enabled=True)
    await _add(session, "ws1_only", group_id="ws1", enabled=True)

    assert _names(await repo.list_for_group_scope("ws1")) == ["global_a", "ws1_only"]
    assert _names(await repo.list_for_group_scope("ws2")) == ["global_a"]


@pytest.mark.asyncio
async def test_find_all_base_returns_only_base_rows(session):
    repo = MCPServerRepository(session)
    await _add(session, "base_on", group_id=None, enabled=True)
    await _add(session, "base_off", group_id=None, enabled=False)
    await _add(session, "ws_row", group_id="ws1", enabled=True)

    assert _names(await repo.find_all_base()) == ["base_off", "base_on"]


@pytest.mark.asyncio
async def test_find_by_names_group_scope_is_opt_in_per_workspace(session):
    """Opt-in model: a workspace only resolves servers it has explicitly enabled
    (its own override rows). An enabled base server does NOT auto-resolve."""
    repo = MCPServerRepository(session)
    await _add(session, "shared", group_id=None, enabled=True)  # globally available
    await _add(session, "shared", group_id="ws1", enabled=True)  # ws1 opted in
    await _add(session, "shared", group_id="ws2", enabled=False)  # ws2 disabled it

    # ws1: enabled override → resolved.
    ws1 = await repo.find_by_names_group_scope(["shared"], "ws1")
    assert len(ws1) == 1 and ws1[0].group_id == "ws1" and ws1[0].enabled is True
    # ws2: disabled override → not resolved.
    assert await repo.find_by_names_group_scope(["shared"], "ws2") == []
    # ws3: never opted in (only the enabled base exists) → not resolved.
    assert await repo.find_by_names_group_scope(["shared"], "ws3") == []


@pytest.mark.asyncio
async def test_find_by_names_group_scope_base_resolves_without_group(session):
    """With no workspace context (seeding / global path) enabled base rows still
    resolve."""
    repo = MCPServerRepository(session)
    await _add(session, "global_a", group_id=None, enabled=True)
    await _add(session, "global_off", group_id=None, enabled=False)

    assert _names(await repo.find_by_names_group_scope(
        ["global_a", "global_off"], None
    )) == ["global_a"]


# ── Global disable cascades to workspaces ─────────────────────────────────────


@pytest.mark.asyncio
async def test_disabled_base_hides_enabled_workspace_override_in_list(session):
    """Global disable cascades: when a system admin disables the base, a workspace
    that opted in (enabled override) no longer sees the server."""
    repo = MCPServerRepository(session)
    await _add(session, "shared", group_id=None, enabled=False)   # global disabled
    await _add(session, "shared", group_id="ws1", enabled=True)   # ws1 opted in

    assert await repo.list_for_group_scope("ws1") == []


@pytest.mark.asyncio
async def test_disabled_base_blocks_resolution_despite_enabled_override(session):
    """Execution-time: a disabled global base blocks resolution even where the
    workspace override is enabled. A workspace-only server (no base) is unaffected."""
    repo = MCPServerRepository(session)
    await _add(session, "shared", group_id=None, enabled=False)   # global disabled
    await _add(session, "shared", group_id="ws1", enabled=True)   # ws1 opted in
    await _add(session, "wsonly", group_id="ws1", enabled=True)   # no base row

    resolved = await repo.find_by_names_group_scope(["shared", "wsonly"], "ws1")
    assert _names(resolved) == ["wsonly"]


@pytest.mark.asyncio
async def test_enabled_base_still_allows_enabled_override_to_resolve(session):
    """Sanity: with the base enabled, an enabled workspace override still resolves."""
    repo = MCPServerRepository(session)
    await _add(session, "shared", group_id=None, enabled=True)
    await _add(session, "shared", group_id="ws1", enabled=True)

    resolved = await repo.find_by_names_group_scope(["shared"], "ws1")
    assert len(resolved) == 1 and resolved[0].group_id == "ws1"


# ── Global delete cascades to workspaces (hard delete) ────────────────────────


@pytest.mark.asyncio
async def test_delete_overrides_by_name_removes_only_group_rows(session):
    """delete_overrides_by_name hard-deletes every per-workspace override for a
    name and leaves the base row (and other servers) untouched."""
    repo = MCPServerRepository(session)
    base = await _add(session, "shared", group_id=None, enabled=True)
    await _add(session, "shared", group_id="ws1", enabled=True)
    await _add(session, "shared", group_id="ws2", enabled=False)
    await _add(session, "other", group_id="ws1", enabled=True)

    removed = await repo.delete_overrides_by_name("shared")
    assert removed == 2

    # Base survives; overrides for 'shared' are gone; unrelated server untouched.
    all_rows = list((await session.execute(select(MCPServer))).scalars().all())
    assert base.group_id is None
    # Only the surviving base 'shared' (group_id None) + the unrelated 'other' remain.
    assert sorted((r.name, r.group_id) for r in all_rows) == [
        ("other", "ws1"),
        ("shared", None),
    ]


# ── Picker source: globally-disabled servers never surface (even to admins) ───


@pytest.mark.asyncio
async def test_picker_source_excludes_globally_disabled_even_for_admins(session):
    """get_all_servers_effective backs GET /mcp/servers (the chat/builder MCP
    picker). A globally-disabled server must not appear even on the admin path
    (enabled_only=False), where workspace-disabled servers are still shown."""
    from src.services.mcp_service import MCPService

    await _add(session, "shared", group_id=None, enabled=False)   # globally disabled
    await _add(session, "shared", group_id="ws1", enabled=True)   # ws1 opted in
    await _add(session, "live", group_id=None, enabled=True)      # globally available
    await _add(session, "live", group_id="ws1", enabled=True)     # ws1 opted in

    svc = MCPService(session)
    resp = await svc.get_all_servers_effective("ws1", enabled_only=False)
    assert _names(resp.servers) == ["live"]
