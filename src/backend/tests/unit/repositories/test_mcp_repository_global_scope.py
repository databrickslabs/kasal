"""
Repository tests for the GLOBAL + per-workspace override resolution model.

A base server (group_id IS NULL) that is globally available (enabled=True) is
visible to every workspace; a group-specific row of the same name shadows the
base for THAT group only. Disabled base servers are hidden from workspaces.
"""

import pytest
import pytest_asyncio
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
async def test_find_by_names_group_scope_honors_per_group_override(session):
    repo = MCPServerRepository(session)
    await _add(session, "shared", group_id=None, enabled=True)
    await _add(session, "shared", group_id="ws1", enabled=False)  # ws1 disabled it

    # ws1: override is disabled → not resolved at execution time.
    assert await repo.find_by_names_group_scope(["shared"], "ws1") == []
    # ws2: inherits the enabled base.
    ws2 = await repo.find_by_names_group_scope(["shared"], "ws2")
    assert len(ws2) == 1 and ws2[0].group_id is None
