"""Tool rows created at run time get ids at or above ``TOOL_ID_FLOOR``.

Ids below the floor belong to the tools ``seeds/tools.py`` ships. A workspace
row that took the next id after the seeded maximum would sit on the id a later
release gives its next tool. Real ``ToolRepository`` on a real SQLite file.
"""

from contextlib import asynccontextmanager

import pytest
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

from src.db.base import Base
from src.models.tool import Tool
from src.repositories.tool_repository import TOOL_ID_FLOOR, ToolRepository


@pytest.fixture
def db_url(tmp_path):
    path = tmp_path / "tools.db"
    sync = create_engine(f"sqlite:///{path}")
    Base.metadata.create_all(sync, tables=[Tool.__table__])
    with Session(sync) as s:
        s.add_all(
            [
                Tool(id=i, title=f"seeded {i}", description="d", icon="i")
                for i in (1, 97)
            ]
        )
        s.commit()
    sync.dispose()
    return f"sqlite+aiosqlite:///{path}"


@asynccontextmanager
async def _repo(db_url):
    engine = create_async_engine(db_url, poolclass=NullPool)
    try:
        async with async_sessionmaker(engine, expire_on_commit=False)() as s:
            yield ToolRepository(s)
            await s.commit()
    finally:
        await engine.dispose()


def _row(title, group_id="ws_a"):
    return {"title": title, "description": "d", "icon": "i", "group_id": group_id}


@pytest.mark.asyncio
async def test_new_workspace_tools_start_at_the_floor(db_url):
    async with _repo(db_url) as repo:
        first = await repo.create(_row("copy"))
        second = await repo.create(_row("custom", "ws_b"))
    assert first.id == TOOL_ID_FLOOR
    assert second.id == TOOL_ID_FLOOR + 1
