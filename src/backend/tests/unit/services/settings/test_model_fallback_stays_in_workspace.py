"""The run-time model fallbacks resolve models for the run's own workspace.

The local fallback (a Databricks model on a deployment with no workspace) and
the retry handler's fallback candidates pick from "the enabled models". For a
workspace that is its own row for a key when it has one, else the global row,
and never another workspace's row.

Real ``ModelConfigService`` and repository on a real SQLite file.
"""

from contextlib import asynccontextmanager

import pytest
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

from src.core.cache import model_config_cache
from src.db.base import Base
from src.models.model_config import ModelConfig
from src.services.settings.models import ModelConfigService

A, B = "ws_a", "ws_b"


def _row(key, provider, enabled, group_id=None):
    return ModelConfig(
        key=key,
        name=key,
        provider=provider,
        temperature=0.2,
        context_window=32000,
        max_output_tokens=4096,
        enabled=enabled,
        group_id=group_id,
    )


@pytest.fixture
def db_url(tmp_path):
    """B turned on two models that are off globally; A turned one off."""
    path = tmp_path / "models.db"
    sync = create_engine(f"sqlite:///{path}")
    Base.metadata.create_all(sync, tables=[ModelConfig.__table__])
    with Session(sync) as s:
        s.add_all(
            [
                # Overrides first: the answer must not depend on row order.
                _row("b-local", "vllm", True, B),
                _row("b-served", "databricks", True, B),
                _row("a-off", "openai", False, A),
                _row("b-local", "vllm", False),
                _row("b-served", "databricks", False),
                _row("a-off", "openai", True),
                _row("shared", "databricks", True),
            ]
        )
        s.commit()
    sync.dispose()
    return f"sqlite+aiosqlite:///{path}"


@asynccontextmanager
async def _session(url):
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        async with async_sessionmaker(engine, expire_on_commit=False)() as s:
            yield s
    finally:
        await engine.dispose()


@pytest.fixture(autouse=True)
async def _empty_model_cache():
    await model_config_cache.clear()
    yield
    await model_config_cache.clear()


@pytest.mark.asyncio
async def test_enabled_models_are_the_workspaces_own_view(db_url):
    async with _session(db_url) as s:
        in_a = {m.key for m in await ModelConfigService(s, A).find_enabled_models()}
        in_b = {m.key for m in await ModelConfigService(s, B).find_enabled_models()}
        no_group = {m.key for m in await ModelConfigService(s).find_enabled_models()}

    # B's toggles are B's alone; A's own "off" wins over the global "on".
    assert in_a == {"shared"}
    assert in_b == {"b-local", "b-served", "shared", "a-off"}
    assert no_group == {"shared", "a-off"}


@pytest.mark.asyncio
async def test_local_fallback_never_picks_another_workspaces_model(db_url, monkeypatch):
    from src.services.settings import engine_settings

    monkeypatch.setattr(engine_settings, "setting", lambda key: None)

    async def _usable(self, provider, params=None):
        return True

    monkeypatch.setattr(ModelConfigService, "_fallback_candidate_usable", _usable)

    async with _session(db_url) as s:
        for_a = await ModelConfigService(s, A)._local_fallback_config()
        for_b = await ModelConfigService(s, B)._local_fallback_config()

    # A has no enabled non-Databricks model of its own: nothing to substitute.
    assert for_a is None
    assert for_b["key"] == "b-local"


@pytest.mark.asyncio
async def test_fallback_candidates_come_from_the_runs_workspace(db_url, monkeypatch):
    import src.db.session as db_session
    from src.services.llm.manager import LLMManager

    monkeypatch.setattr(db_session, "routed_scoped_session", lambda: _session(db_url))

    in_a = await LLMManager.load_fallback_candidates("current", A)
    in_b = await LLMManager.load_fallback_candidates("current", B)

    assert [c.name for c in in_a] == ["shared"]
    assert sorted(c.name for c in in_b) == ["b-served", "shared"]
