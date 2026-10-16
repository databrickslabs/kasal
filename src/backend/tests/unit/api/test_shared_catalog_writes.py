"""Shared catalog rows (``group_id`` NULL) change for system admins only.

A row with no group is every workspace's row: a global tool, a globally
registered remote agent, a base prompt template. These tests mount the real
routers over a real in-memory SQLite database and call them as different
users. Every role short of system admin gets 403 on a shared-row write,
including a user in their own personal (``user_``) workspace, where the
effective role is at least editor. Workspace rows keep their existing rules.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.api.a2a_agents_router import router as a2a_router
from src.api.mcp_router import router as mcp_router
from src.api.templates_router import router as templates_router
from src.api.tools_router import router as tools_router
from src.db.database_router import get_smart_db_session
from src.dependencies.providers import get_group_context
from src.models.a2a_agent import A2AAgent
from src.models.mcp_server import MCPServer
from src.models.template import PromptTemplate
from src.models.tool import Tool
from src.utils.user_context import GroupContext
from tests.unit.api.conftest import register_exception_handlers

_TABLES = (Tool, A2AAgent, PromptTemplate, MCPServer)


def _caller(
    group_id: str,
    role=None,
    system_admin: bool = False,
    workspace_manager: bool = False,
) -> GroupContext:
    return GroupContext(
        group_ids=[group_id],
        group_email="user@example.com",
        email_domain="example.com",
        user_role=role,
        current_user=SimpleNamespace(
            is_system_admin=system_admin,
            is_personal_workspace_manager=workspace_manager,
        ),
    )


#: Everyone who is NOT a system admin but can write something somewhere.
NON_SYSTEM_ADMINS = {
    "personal-workspace editor": _caller("user_alice"),
    "personal-workspace manager": _caller("user_alice", workspace_manager=True),
    "team admin": _caller("team_a", role="admin"),
    "team editor": _caller("team_a", role="editor"),
}
SYSTEM_ADMIN = _caller("team_a", role="admin", system_admin=True)


@pytest_asyncio.fixture
async def db():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        for model in _TABLES:
            await conn.run_sync(model.__table__.create)
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with maker() as session:
        yield session
    await engine.dispose()


@pytest_asyncio.fixture
async def rows(db):
    """A global and a team row of each kind, plus another team's rows."""
    made = {
        "global_tool": Tool(title="Shared", description="d", icon="i", group_id=None),
        "team_tool": Tool(title="Mine", description="d", icon="i", group_id="team_a"),
        "other_tool": Tool(
            title="Theirs", description="d", icon="i", group_id="team_b"
        ),
        "global_agent": A2AAgent(
            name="Remote", card_url="https://remote.example.com", group_id=None
        ),
        "global_template": PromptTemplate(
            name="base_prompt", template="t", group_id=None
        ),
        "team_template": PromptTemplate(
            name="base_prompt", template="t", group_id="team_a"
        ),
    }
    db.add_all(made.values())
    await db.flush()
    return {key: row.id for key, row in made.items()}


def _client(db, caller) -> AsyncClient:
    app = FastAPI()
    for router in (tools_router, a2a_router, templates_router, mcp_router):
        app.include_router(router)
    register_exception_handlers(app)

    async def _session():
        yield db

    app.dependency_overrides[get_smart_db_session] = _session
    app.dependency_overrides[get_group_context] = lambda: caller
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


def _no_card_fetch():
    return patch(
        "src.services.a2a.a2a_client.client.fetch_card",
        new=AsyncMock(return_value={"name": "Remote"}),
    )


#: Every write that targets a shared row: label -> (method, path, json body).
#: Paths are formatted with the ids from the ``rows`` fixture.
GLOBAL_WRITES = {
    "tool update": (
        "PUT",
        "/tools/{global_tool}",
        {"config": {"base_url": "https://example.com"}},
    ),
    "tool global availability": (
        "PATCH",
        "/tools/{global_tool}/global-availability",
        {"enabled": False},
    ),
    "tool delete": ("DELETE", "/tools/{global_tool}", None),
    "agent create": (
        "POST",
        "/a2a-agents",
        {"name": "New", "card_url": "https://new.example.com"},
    ),
    "agent update": ("PUT", "/a2a-agents/{global_agent}", {"description": "x"}),
    "agent global availability": (
        "PATCH",
        "/a2a-agents/{global_agent}/global-availability",
        {"enabled": False},
    ),
    "agent delete": ("DELETE", "/a2a-agents/{global_agent}", None),
    "template delete": ("DELETE", "/templates/{global_template}", None),
}


async def _send(client, rows, write):
    method, path, body = GLOBAL_WRITES[write]
    with _no_card_fetch():
        return await client.request(method, path.format(**rows), json=body)


async def _shared_rows_untouched(db, rows) -> bool:
    tool = await db.get(Tool, rows["global_tool"])
    agent = await db.get(A2AAgent, rows["global_agent"])
    template = await db.get(PromptTemplate, rows["global_template"])
    if tool is None or agent is None or template is None:
        return False
    await db.refresh(tool)
    await db.refresh(agent)
    created = (
        await db.execute(select(A2AAgent).where(A2AAgent.name == "New"))
    ).scalar_one_or_none()
    return (
        tool.enabled is True
        and tool.config == {}
        and agent.enabled is True
        and agent.description is None
        and created is None
    )


class TestSharedRowsRefuseEveryoneButASystemAdmin:
    @pytest.mark.parametrize("write", sorted(GLOBAL_WRITES))
    @pytest.mark.parametrize("who", sorted(NON_SYSTEM_ADMINS))
    @pytest.mark.asyncio
    async def test_a_shared_write_is_403(self, db, rows, who, write):
        async with _client(db, NON_SYSTEM_ADMINS[who]) as client:
            resp = await _send(client, rows, write)
        assert resp.status_code == 403, resp.text
        assert await _shared_rows_untouched(db, rows)

    @pytest.mark.parametrize("write", sorted(GLOBAL_WRITES))
    @pytest.mark.asyncio
    async def test_a_system_admin_can_make_it(self, db, rows, write):
        async with _client(db, SYSTEM_ADMIN) as client:
            resp = await _send(client, rows, write)
        assert resp.status_code < 300, resp.text
        assert not await _shared_rows_untouched(db, rows)


class TestWorkspaceCopiesLeaveTheSharedRowAlone:
    """Toggling or configuring a global tool writes the caller's own copy."""

    @pytest.mark.asyncio
    async def test_toggle_and_configure_are_copy_on_write(self, db, rows):
        async with _client(db, NON_SYSTEM_ADMINS["team admin"]) as client:
            toggled = await client.patch(f"/tools/{rows['global_tool']}/toggle-enabled")
            configured = await client.put(
                "/tools/configurations/Shared", json={"k": "v"}
            )
        assert toggled.status_code == 200 and configured.status_code == 200

        shared = await db.get(Tool, rows["global_tool"])
        await db.refresh(shared)
        assert shared.enabled is True and shared.config == {}


class TestWorkspaceRowsKeepTheirRules:
    @pytest.mark.asyncio
    async def test_a_team_editor_still_edits_and_deletes_its_own_tool(self, db, rows):
        async with _client(db, NON_SYSTEM_ADMINS["team editor"]) as client:
            put = await client.put(
                f"/tools/{rows['team_tool']}", json={"description": "new"}
            )
            delete = await client.delete(f"/tools/{rows['team_tool']}")
        assert put.status_code == 200 and put.json()["description"] == "new"
        assert delete.status_code == 204

    @pytest.mark.asyncio
    async def test_another_teams_tool_is_still_not_found(self, db, rows):
        async with _client(db, NON_SYSTEM_ADMINS["team admin"]) as client:
            put = await client.put(
                f"/tools/{rows['other_tool']}", json={"description": "x"}
            )
            delete = await client.delete(f"/tools/{rows['other_tool']}")
        assert put.status_code == 404 and delete.status_code == 404

    @pytest.mark.asyncio
    async def test_an_operator_is_still_refused_on_its_own_tool(self, db, rows):
        async with _client(db, _caller("team_a", role="operator")) as client:
            put = await client.put(
                f"/tools/{rows['team_tool']}", json={"description": "x"}
            )
        assert put.status_code == 403

    @pytest.mark.asyncio
    async def test_a_team_template_is_still_deletable_by_its_team(self, db, rows):
        async with _client(db, NON_SYSTEM_ADMINS["team editor"]) as client:
            resp = await client.delete(f"/templates/{rows['team_template']}")
        assert resp.status_code == 200
        assert await db.get(PromptTemplate, rows["team_template"]) is None


class TestMcpListingsStayInTheCallersScope:
    @pytest_asyncio.fixture
    async def servers(self, db):
        def server(name, group_id, global_enabled=False):
            return MCPServer(
                name=name,
                server_url=f"https://example.com/{name}",
                enabled=True,
                global_enabled=global_enabled,
                group_id=group_id,
            )

        db.add_all(
            [
                server("shared", None, global_enabled=True),
                server("ours", "team_a", global_enabled=True),
                server("theirs", "team_b", global_enabled=True),
            ]
        )
        await db.flush()

    @pytest.mark.parametrize("path", ["/mcp/servers/enabled", "/mcp/servers/global"])
    @pytest.mark.asyncio
    async def test_another_groups_servers_are_not_listed(self, db, servers, path):
        async with _client(db, NON_SYSTEM_ADMINS["team admin"]) as client:
            resp = await client.get(path)
        assert resp.status_code == 200
        names = sorted(s["name"] for s in resp.json()["servers"])
        assert names == ["ours", "shared"]
        assert resp.json()["count"] == 2
