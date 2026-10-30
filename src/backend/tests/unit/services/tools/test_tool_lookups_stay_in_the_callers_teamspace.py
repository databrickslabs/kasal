"""Tool lookups by title read the caller's own row or the global row, nothing else.

``get_tool_config_by_name`` runs at crew build time (its ``result_as_answer`` is
what the build reads); without a group it reads the global row only. The
listing overlays the current workspace's rows only. Real ``ToolService`` and
repository on a real SQLite file.
"""

from contextlib import asynccontextmanager

import pytest
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

from src.db.base import Base
from src.models.tool import Tool
from src.services.tools.tool_service import ToolService
from src.utils.user_context import GroupContext

A, B = "ws_a", "ws_b"


@pytest.fixture
def db_url(tmp_path):
    path = tmp_path / "tools.db"
    sync = create_engine(f"sqlite:///{path}")
    Base.metadata.create_all(sync, tables=[Tool.__table__])
    with Session(sync) as s:
        s.add_all(
            [
                Tool(id=1, title="Search", description="d", icon="i", config={"g": 1}),
                Tool(
                    title="Search",
                    description="d",
                    icon="i",
                    config={"b": 1},
                    enabled=False,
                    group_id=B,
                ),
                Tool(
                    title="A only",
                    description="d",
                    icon="i",
                    config={"secret": "a"},
                    group_id=A,
                ),
            ]
        )
        s.commit()
    sync.dispose()
    return f"sqlite+aiosqlite:///{path}"


@asynccontextmanager
async def _service(db_url):
    engine = create_async_engine(db_url, poolclass=NullPool)
    try:
        async with async_sessionmaker(engine, expire_on_commit=False)() as s:
            yield ToolService(s)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_tool_config_by_name_never_reads_another_teamspace(db_url):
    async with _service(db_url) as svc:
        assert await svc.get_tool_config_by_name("A only") is None
        assert await svc.get_tool_config_by_name("A only", B) is None
        assert await svc.get_tool_config_by_name("A only", A) == {"secret": "a"}
        assert await svc.get_tool_config_by_name("Search") == {"g": 1}
        assert await svc.get_tool_config_by_name("Search", B) == {"b": 1}


@pytest.mark.asyncio
async def test_tool_listing_overlays_the_current_workspace_only(db_url):
    # A member of both teamspaces, working in A: B's copy is not A's.
    in_a = GroupContext(group_ids=[A, B], group_email="u@example.com")
    async with _service(db_url) as svc:
        listed = {t.title: t for t in (await svc.get_all_tools_for_group(in_a)).tools}
    assert listed["Search"].id == 1
    assert listed["Search"].enabled is True
    assert listed["A only"].config == {"secret": "a"}
