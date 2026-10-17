"""Intent detection uses the workspace's own ``detect_intent`` template.

A workspace that customised the template (by hand or by applying an
optimisation run) gets it; one that did not gets the global template. Real
``TemplateService`` and repository on a real SQLite file; only the LLM call is
replaced, to capture the system prompt it is sent.
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

from src.core.cache import intent_cache
from src.db.base import Base
from src.models.template import PromptTemplate
from src.services.catalog.templates import TemplateService
from src.services.chat.dispatcher import DispatcherService
from src.utils.user_context import GroupContext

A, B = "ws_a", "ws_b"
REPLY = '{"intent": "generate_crew", "confidence": 0.9}'


@pytest.fixture
def db_url(tmp_path):
    path = tmp_path / "templates.db"
    sync = create_engine(f"sqlite:///{path}")
    Base.metadata.create_all(sync, tables=[PromptTemplate.__table__])
    with Session(sync) as s:
        s.add_all(
            [
                PromptTemplate(name="detect_intent", template="GLOBAL PROMPT"),
                PromptTemplate(
                    name="detect_intent", template="WS A PROMPT", group_id=A
                ),
            ]
        )
        s.commit()
    sync.dispose()
    return f"sqlite+aiosqlite:///{path}"


@asynccontextmanager
async def _dispatcher(db_url):
    engine = create_async_engine(db_url, poolclass=NullPool)
    try:
        async with async_sessionmaker(engine, expire_on_commit=False)() as s:
            svc = DispatcherService.__new__(DispatcherService)
            svc.session = s
            svc.log_service = AsyncMock()
            svc.template_service = TemplateService(s)
            yield svc
    finally:
        await engine.dispose()


async def _system_prompt(svc, group_id):
    await intent_cache.clear()
    completion = AsyncMock(return_value=REPLY)
    with patch("src.services.chat.dispatcher.LLMManager.completion", completion):
        await svc.detect_intent(
            "build me a research crew",
            "test-model",
            group_context=GroupContext(group_ids=[group_id], group_email="u@x.com"),
        )
    messages = completion.await_args.kwargs.get("messages") or (
        completion.await_args.args[0]
    )
    return messages[0]["content"]


@pytest.mark.asyncio
async def test_detect_intent_uses_the_workspace_override(db_url):
    async with _dispatcher(db_url) as svc:
        assert await _system_prompt(svc, A) == "WS A PROMPT"
        assert await _system_prompt(svc, B) == "GLOBAL PROMPT"
    await intent_cache.clear()
