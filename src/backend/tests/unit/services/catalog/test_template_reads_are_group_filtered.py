"""Reading a prompt template by name: the caller's own row, else the global row.

``get_template_content`` serves intent detection, run naming and generation.
The read is scoped to the caller's group: its own row wins, and without a group
only the global row is read. Real ``TemplateService`` and repository on a real
SQLite file.
"""

from contextlib import asynccontextmanager

import pytest
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

from src.db.base import Base
from src.models.template import PromptTemplate
from src.services.catalog.templates import TemplateService
from src.utils.user_context import GroupContext

A, B = "ws_a", "ws_b"


@pytest.fixture
def db_url(tmp_path):
    path = tmp_path / "templates.db"
    sync = create_engine(f"sqlite:///{path}")
    Base.metadata.create_all(sync, tables=[PromptTemplate.__table__])
    with Session(sync) as s:
        s.add_all(
            [
                PromptTemplate(name="only_a", template="A's prompt", group_id=A),
                PromptTemplate(name="detect_intent", template="A's", group_id=A),
                PromptTemplate(name="detect_intent", template="global"),
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
            yield TemplateService(s)
    finally:
        await engine.dispose()


def _ctx(group_id: str) -> GroupContext:
    return GroupContext(group_ids=[group_id], group_email="u@example.com")


@pytest.mark.asyncio
async def test_template_content_without_a_group_is_the_global_row_only(db_url):
    async with _service(db_url) as svc:
        assert await svc.get_template_content("only_a") == ""
        assert await svc.get_template_content("only_a", "fallback") == "fallback"
        assert await svc.get_template_content("detect_intent") == "global"


@pytest.mark.asyncio
async def test_template_content_prefers_the_callers_own_row(db_url):
    async with _service(db_url) as svc:
        get = svc.get_template_content
        assert await get("detect_intent", group_context=_ctx(A)) == "A's"
        assert await get("detect_intent", group_context=_ctx(B)) == "global"
        assert await get("only_a", group_context=_ctx(A)) == "A's prompt"
        assert await get("only_a", group_context=_ctx(B)) == ""
