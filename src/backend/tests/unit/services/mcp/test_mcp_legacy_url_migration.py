"""Legacy external-MCP URL migration: read-only GET, explicit tenant-scoped POST.

The catalog GET used to rewrite registrations as a side effect after loading
every tenant's MCP rows. These tests pin that the GET only counts, the POST
writes, and both only ever see the caller's workspace (plus base rows for a
system admin).
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.api.mcp_router import get_databricks_mcp_options, migrate_external_mcp_urls
from src.core.exceptions import ForbiddenError
from src.db.base import Base
from src.models.mcp_server import MCPServer
from src.services.mcp.mcp_client.legacy_urls import (
    MCPLegacyUrlService,
    plan_legacy_url_migration,
)

LEGACY = "https://ws.example.com/api/2.0/mcp/external/websearch_connection"
GATEWAY = "https://ws.example.com/ai-gateway/mcp-services/kasal.agents.websearch"
OPTIONS = [
    {"name": "kasal.agents.websearch", "server_url": GATEWAY},
    {
        "name": "kasal.agents.custom",
        "server_url": "https://ws.example.com/ai-gateway/mcp-services/kasal.agents.custom",
    },
]


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda c: Base.metadata.create_all(c, tables=[MCPServer.__table__])
        )
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as s:
        s.add_all(
            [
                MCPServer(
                    id=1,
                    name="kasal.agents.websearch",
                    server_url=LEGACY,
                    group_id=None,
                ),
                MCPServer(
                    id=2,
                    name="kasal.agents.websearch",
                    server_url=LEGACY,
                    group_id="t1",
                ),
                MCPServer(
                    id=3,
                    name="kasal.agents.websearch",
                    server_url=LEGACY,
                    group_id="t2",
                ),
                MCPServer(
                    id=4,
                    name="kasal.agents.custom",
                    server_url="https://custom.example.com/mcp",
                    group_id="t1",
                ),
            ]
        )
        await s.flush()
        yield s
    await engine.dispose()


async def _urls(session):
    rows = await session.run_sync(
        lambda s: {r.id: r.server_url for r in s.query(MCPServer).all()}
    )
    return rows


def test_plan_only_rewrites_legacy_proxy_rows_with_a_confirmed_service():
    rows = [
        SimpleNamespace(id=1, name="KASAL.agents.websearch", server_url=LEGACY),
        SimpleNamespace(id=2, name="other", server_url=LEGACY),
        SimpleNamespace(id=3, name="kasal.agents.websearch", server_url=GATEWAY),
        SimpleNamespace(
            id=4, name="kasal.agents.websearch", server_url="https://x.example.com"
        ),
    ]
    plan = plan_legacy_url_migration(rows, OPTIONS)
    assert [(r.id, url) for r, url in plan] == [(1, GATEWAY)]
    # An option not on the AI Gateway is never a replacement.
    assert (
        plan_legacy_url_migration(rows, [{"name": "other", "server_url": LEGACY}]) == []
    )


@pytest.mark.asyncio
async def test_count_is_read_only_and_scoped(session):
    svc = MCPLegacyUrlService(session)
    assert await svc.count_pending(OPTIONS, "t1", include_base=False) == 1
    assert await svc.count_pending(OPTIONS, "t1", include_base=True) == 2
    assert await svc.count_pending(OPTIONS, None, include_base=False) == 0
    assert (await _urls(session))[2] == LEGACY


@pytest.mark.asyncio
async def test_migrate_changes_only_the_callers_workspace(session):
    changed = await MCPLegacyUrlService(session).migrate(
        OPTIONS, "t1", include_base=False
    )
    assert changed == 1
    urls = await _urls(session)
    assert urls[2] == GATEWAY  # t1's legacy row
    assert urls[1] == LEGACY  # base row: not a system admin
    assert urls[3] == LEGACY  # another tenant's row
    assert urls[4] == "https://custom.example.com/mcp"  # custom endpoint


@pytest.mark.asyncio
async def test_system_admin_migration_includes_base_rows(session):
    changed = await MCPLegacyUrlService(session).migrate(
        OPTIONS, "t1", include_base=True
    )
    assert changed == 2
    urls = await _urls(session)
    assert urls[1] == GATEWAY and urls[2] == GATEWAY and urls[3] == LEGACY


@pytest.mark.asyncio
async def test_repository_never_loads_other_tenants(session):
    from src.repositories.mcp_repository import MCPServerRepository

    rows = await MCPServerRepository(session).find_legacy_external_in_scope(
        "t1", include_base=False
    )
    assert {r.id for r in rows} == {2}


# ------------------------------------------------------------------- router


def _ctx(role="admin", group="t1"):
    return SimpleNamespace(user_role=role, current_user=None, primary_group_id=group)


def _router_patches(external):
    auth = SimpleNamespace(workspace_url="https://ws.example.com", get_headers=dict)
    return (
        patch(
            "src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=auth)
        ),
        patch(
            "src.utils.databricks_auth.extract_user_token_from_request",
            return_value="tok",
        ),
        patch(
            "src.api.mcp_router._list_external_mcp_options",
            AsyncMock(return_value=external),
        ),
    )


@pytest.mark.asyncio
async def test_catalog_get_reports_but_never_writes(session):
    p1, p2, p3 = _router_patches(OPTIONS)
    with p1, p2, p3:
        result = await get_databricks_mcp_options(
            MagicMock(headers={}), session=session, group_context=_ctx()
        )
    assert result["legacy_external_count"] == 1
    assert (await _urls(session))[2] == LEGACY


@pytest.mark.asyncio
async def test_migrate_post_writes_for_the_caller_only(session):
    p1, p2, p3 = _router_patches(OPTIONS)
    with p1, p2, p3:
        result = await migrate_external_mcp_urls(
            MagicMock(headers={}), session=session, group_context=_ctx()
        )
    assert result == {"migrated": 1}
    urls = await _urls(session)
    assert urls[2] == GATEWAY and urls[3] == LEGACY and urls[1] == LEGACY


@pytest.mark.asyncio
async def test_migrate_post_is_admin_only(session):
    with pytest.raises(ForbiddenError):
        await migrate_external_mcp_urls(
            MagicMock(headers={}), session=session, group_context=_ctx("operator")
        )
    assert (await _urls(session))[2] == LEGACY
