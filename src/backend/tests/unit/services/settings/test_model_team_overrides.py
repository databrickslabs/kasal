"""A teamspace's model toggle is an override only that teamspace resolves.

Real ``ModelConfigService`` and repository on a real SQLite file. Where row
order matters, the override rows are written BEFORE the global row (an order
PostgreSQL can return rows in), so the answer must not depend on row order.
"""

from contextlib import asynccontextmanager

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

from src.core.cache import model_config_cache
from src.db.base import Base
from src.models.model_config import ModelConfig
from src.services.settings.models import ModelConfigService
from src.utils.user_context import GroupContext

A, B, C = "ws_a", "ws_b", "ws_c"
KEY = "self-hosted-model"
ENDPOINT = {"api_base": "https://models.example.com/v1"}


def _global() -> ModelConfig:
    return ModelConfig(
        key=KEY,
        name=KEY,
        provider="vllm",
        temperature=0.3,
        context_window=32000,
        max_output_tokens=4096,
        params=ENDPOINT,
        unsupported_params=["stop"],
        reasoning_effort="high",
        enabled=True,
    )


def _partial_override(group_id: str, enabled: bool) -> ModelConfig:
    """What the toggle used to write: no params, refusals or thinking."""
    return ModelConfig(
        key=KEY, name=KEY, provider="vllm", enabled=enabled, group_id=group_id
    )


def _db(tmp_path, *rows) -> str:
    path = tmp_path / "models.db"
    sync = create_engine(f"sqlite:///{path}")
    Base.metadata.create_all(sync, tables=[ModelConfig.__table__])
    with Session(sync) as s:
        s.add_all(list(rows))
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


async def _resolved(url, group_id):
    async with _session(url) as s:
        return await ModelConfigService(s, group_id=group_id).get_model_config(KEY)


@pytest.mark.asyncio
async def test_each_team_resolves_its_own_override_or_the_global_row(tmp_path):
    url = _db(
        tmp_path, _partial_override(A, False), _partial_override(B, True), _global()
    )
    for _ in range(2):  # the second round is served from the per-team cache
        a, b, c = [await _resolved(url, g) for g in (A, B, C)]
        assert (a["enabled"], b["enabled"], c["enabled"]) == (False, True, True)
        # How to call the model always comes from the global row.
        for config in (a, b, c):
            assert config["params"] == ENDPOINT
            assert config["unsupported_params"] == ["stop"]
            assert config["reasoning_effort"] == "high"


@pytest.mark.asyncio
async def test_team_toggle_does_not_change_what_another_team_resolves(tmp_path):
    url = _db(tmp_path, _global(), _partial_override(B, True))
    # A member of both teamspaces, working in A, turns the model off.
    in_a = GroupContext(group_ids=[A, B], group_email="u@example.com")
    async with _session(url) as s:
        await ModelConfigService(s, group_id=A).toggle_model_enabled_with_group(
            KEY, False, in_a
        )
        await s.commit()

    assert (await _resolved(url, A))["enabled"] is False
    assert (await _resolved(url, B))["enabled"] is True
    assert (await _resolved(url, C))["enabled"] is True

    async with _session(url) as s:
        rows = (await s.execute(select(ModelConfig))).scalars().all()
    by_group = {r.group_id: r for r in rows}
    assert by_group[B].enabled is True
    # A's override is a complete copy, not a partial one.
    override = by_group[A]
    assert override.params == ENDPOINT
    assert override.unsupported_params == ["stop"]
    assert override.reasoning_effort == "high"
    assert override.enabled is False


@pytest.mark.asyncio
async def test_admin_edit_lands_on_the_global_row(tmp_path):
    url = _db(tmp_path, _partial_override(A, False), _global())
    async with _session(url) as s:
        updated = await ModelConfigService(s).update_model_config(
            KEY, {"max_output_tokens": 999}
        )
        await s.commit()
    assert updated.group_id is None

    async with _session(url) as s:
        rows = (await s.execute(select(ModelConfig))).scalars().all()
    tokens = {r.group_id: r.max_output_tokens for r in rows}
    assert tokens == {None: 999, A: None}


@pytest.mark.asyncio
async def test_list_and_get_show_the_global_definition_with_the_team_toggle(tmp_path):
    url = _db(tmp_path, _partial_override(A, False), _global())
    in_a = GroupContext(group_ids=[A], group_email="u@example.com")
    async with _session(url) as s:
        svc = ModelConfigService(s, group_id=A)
        listed = {m.key: m for m in await svc.find_all_for_group(in_a)}[KEY]
        got = await svc.find_by_key(KEY)
        enabled_keys = [m.key for m in await svc.find_enabled_models_for_group(in_a)]
    for view in (listed, got):
        assert (view.enabled, view.group_id) == (False, A)
        assert (view.max_output_tokens, view.params) == (4096, ENDPOINT)
    assert KEY not in enabled_keys
