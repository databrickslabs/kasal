"""Catalog lookups resolve the caller's own row, else the global row.

Models, prompt templates and tools keep one global row plus per-teamspace rows
under the same natural key, and each lookup is filtered by group and ordered.
Here the two teamspace rows are written BEFORE the global row (an order
PostgreSQL can return rows in), so the answer must not depend on row order.
"""

from contextlib import asynccontextmanager

import pytest
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

from src.db.base import Base
from src.models.model_config import ModelConfig
from src.models.template import PromptTemplate
from src.models.tool import Tool
from src.repositories.model_config_repository import ModelConfigRepository
from src.repositories.template_repository import TemplateRepository
from src.repositories.tool_repository import ToolRepository

A, B, C = "ws_a", "ws_b", "ws_c"


@pytest.fixture
def db_url(tmp_path):
    path = tmp_path / "catalog.db"
    sync = create_engine(f"sqlite:///{path}")
    tables = [ModelConfig.__table__, PromptTemplate.__table__, Tool.__table__]
    Base.metadata.create_all(sync, tables=tables)
    with Session(sync) as s:
        for owner in (A, B, None):  # teamspace rows first, the global row last
            label = owner or "global"
            s.add(ModelConfig(key="m", name=label, provider="openai", group_id=owner))
            s.add(PromptTemplate(name="p", template=label, group_id=owner))
            s.add(Tool(title="t", description=label, icon="i", group_id=owner))
        # Only teamspace A has these: nobody else may read them by name.
        s.add(ModelConfig(key="m-a", name=A, provider="openai", group_id=A))
        s.add(PromptTemplate(name="p-a", template=A, group_id=A))
        s.add(Tool(title="t-a", description=A, icon="i", group_id=A))
        s.commit()
    sync.dispose()
    return f"sqlite+aiosqlite:///{path}"


@asynccontextmanager
async def _session(db_url):
    engine = create_async_engine(db_url, poolclass=NullPool)
    try:
        async with async_sessionmaker(engine, expire_on_commit=False)() as s:
            yield s
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_model_key_resolves_own_override_else_global(db_url):
    async with _session(db_url) as s:
        repo = ModelConfigRepository(s)
        for _ in range(2):  # the same answer every time
            # Without a teamspace: the global row, never a teamspace's.
            assert (await repo.find_by_key("m")).name == "global"
            assert await repo.find_by_key("m-a") is None
            assert (await repo.find_global_by_key("m")).name == "global"
            assert (await repo.find_by_key("m", A)).name == A
            assert (await repo.find_by_key("m", B)).name == B
            assert (await repo.find_by_key("m", C)).name == "global"
            assert (await repo.find_by_key("m-a", A)).name == A
            assert await repo.find_by_key("m-a", B) is None


@pytest.mark.asyncio
async def test_template_name_resolves_own_row_else_global(db_url):
    async with _session(db_url) as s:
        repo = TemplateRepository(s)
        for _ in range(2):
            assert await repo.find_by_name("p-a") is None
            assert (await repo.find_by_name("p")).template == "global"
            assert (await repo.find_by_name("p", A)).template == A
            assert (await repo.find_by_name("p", B)).template == B
            assert (await repo.find_by_name("p", C)).template == "global"
            assert (await repo.find_by_name("p-a", A)).template == A
            assert await repo.find_by_name("p-a", B) is None


@pytest.mark.asyncio
async def test_tool_title_resolves_own_row_else_global(db_url):
    async with _session(db_url) as s:
        repo = ToolRepository(s)
        for _ in range(2):
            assert (await repo.find_by_title("t")).description == "global"
            assert await repo.find_by_title("t-a") is None
            assert (await repo.find_base_by_title("t")).description == "global"
            assert (await repo.find_by_title("t", A)).description == A
            assert (await repo.find_by_title("t", B)).description == B
            assert (await repo.find_by_title("t", C)).description == "global"
            assert (await repo.find_by_title("t-a", A)).description == A
            assert await repo.find_by_title("t-a", B) is None
