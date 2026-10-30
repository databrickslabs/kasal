"""The tool, model and prompt-template seeders write global rows only.

Seeding runs on every start. Each restart here is a real ``seed_async`` against a
real SQLite file, with the workspace rows and admin edits a running install
accumulates in between, so the WHERE clauses are exercised rather than mocked.
"""

from contextlib import asynccontextmanager

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from src.db.base import Base
from src.models.group_tool import GroupTool
from src.models.model_config import ModelConfig
from src.models.template import PromptTemplate
from src.models.tool import Tool
from src.seeds import model_configs as model_seed
from src.seeds import prompt_templates as template_seed
from src.seeds import tools as tool_seed

WS_A, WS_B = "ws_a", "ws_b"


@pytest.fixture
def factory(tmp_path, monkeypatch):
    """A session factory on a fresh file database, wired into all three seeders."""
    path = tmp_path / "catalog.db"
    sync = create_engine(f"sqlite:///{path}")
    Base.metadata.create_all(
        sync,
        tables=[
            Tool.__table__,
            GroupTool.__table__,
            ModelConfig.__table__,
            PromptTemplate.__table__,
        ],
    )
    sync.dispose()
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}", poolclass=NullPool)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    @asynccontextmanager
    async def open_session():
        async with sessions() as session:
            yield session

    for module in (tool_seed, model_seed, template_seed):
        monkeypatch.setattr(module, "async_session_factory", open_session)
    return open_session


async def _all(factory, model, *where):
    async with factory() as s:
        return list((await s.execute(select(model).where(*where))).scalars().all())


# ── Tools ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_tool_restarts_keep_unpublished_tools_and_workspace_rows(factory):
    await tool_seed.seed_async()  # first boot
    tool_id, title, description, icon = tool_seed.tools_data[0]

    async with factory() as s:
        base = await s.get(Tool, tool_id)
        base.enabled = False  # a system admin unpublishes it
        base.config = {**(base.config or {}), "admin_setting": "kept"}
        s.add_all(
            [
                # A workspace's toggle copy, its saved credentials, a custom tool.
                Tool(
                    title=title,
                    description=description,
                    icon=icon,
                    enabled=False,
                    group_id=WS_A,
                ),
                Tool(
                    title="Power BI Tool",
                    description="d",
                    icon="i",
                    config={"client_secret": "ws-a-secret"},
                    group_id=WS_A,
                ),
                Tool(title="My custom tool", description="d", icon="i", group_id=WS_B),
                GroupTool(
                    tool_id=tool_id, group_id=WS_B, enabled=True, config={"k": "v"}
                ),
                # A global tool this release no longer ships: still retired.
                Tool(id=9999, title="Retired", description="d", icon="i"),
            ]
        )
        await s.commit()

    await tool_seed.seed_async()  # restart
    await tool_seed.seed_async()  # and again

    base = (await _all(factory, Tool, Tool.id == tool_id))[0]
    assert base.enabled is False
    assert base.config["admin_setting"] == "kept"

    workspace_rows = await _all(factory, Tool, Tool.group_id.isnot(None))
    assert sorted((t.group_id, t.title) for t in workspace_rows) == sorted(
        [(WS_A, "Power BI Tool"), (WS_A, title), (WS_B, "My custom tool")]
    )
    by_title = {t.title: t for t in workspace_rows}
    assert by_title[title].enabled is False
    assert by_title["Power BI Tool"].config == {"client_secret": "ws-a-secret"}

    mappings = await _all(factory, GroupTool)
    assert [(m.tool_id, m.group_id, m.config) for m in mappings] == [
        (tool_id, WS_B, {"k": "v"})
    ]
    assert await _all(factory, Tool, Tool.id == 9999) == []


@pytest.mark.asyncio
async def test_tool_seed_id_held_by_a_workspace_row_does_not_fail(factory):
    tool_id = tool_seed.tools_data[1][0]
    async with factory() as s:
        # An install from before workspace rows had their own id range.
        s.add(Tool(id=tool_id, title="Ours", description="d", icon="i", group_id=WS_A))
        await s.commit()

    await tool_seed.seed_async()  # must not raise
    await tool_seed.seed_async()

    row = (await _all(factory, Tool, Tool.id == tool_id))[0]
    assert (row.title, row.group_id) == ("Ours", WS_A)


@pytest.mark.asyncio
async def test_tool_cleanup_keeps_global_rows_created_at_runtime(factory):
    async with factory() as s:
        # Created by the app, so its id is at the floor, not from a past seed.
        s.add(Tool(id=10_000, title="Runtime", description="d", icon="i"))
        await s.commit()

    await tool_seed.seed_async()

    assert [t.title for t in await _all(factory, Tool, Tool.id == 10_000)] == [
        "Runtime"
    ]


# ── Models ───────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_model_reseed_ships_catalog_fixes_and_keeps_admin_choices(
    factory, monkeypatch
):
    key, seed = next(
        (k, v)
        for k, v in model_seed.DEFAULT_MODELS.items()
        if v["provider"] == "databricks"
    )
    async with factory() as s:
        # A workspace override that outlived its global row (a key that was
        # retired and later shipped again): it sits FIRST in the table.
        s.add(
            ModelConfig(
                key=key,
                name="ws-a-name",
                provider="databricks",
                temperature=0.11,
                enabled=True,
                group_id=WS_A,
            )
        )
        await s.commit()

    await model_seed.seed_async()  # first boot
    async with factory() as s:
        row = (
            await s.execute(
                select(ModelConfig).where(
                    ModelConfig.key == key, ModelConfig.group_id.is_(None)
                )
            )
        ).scalar_one()
        row.enabled = False  # a system admin disables the catalog model
        await s.commit()

    # The next release corrects the model's output limit.
    fixed = seed["max_output_tokens"] - 1
    monkeypatch.setitem(
        model_seed.DEFAULT_MODELS, key, {**seed, "max_output_tokens": fixed}
    )
    await model_seed.seed_async()  # restart
    await model_seed.seed_async()  # and again

    rows = await _all(factory, ModelConfig, ModelConfig.key == key)
    by_group = {r.group_id: r for r in rows}
    assert set(by_group) == {None, WS_A}
    glob = by_group[None]
    assert glob.max_output_tokens == fixed  # the release fix reached the row
    assert glob.enabled is False  # the admin's choice was not reset
    override = by_group[WS_A]
    assert (override.name, override.temperature, override.max_output_tokens) == (
        "ws-a-name",
        0.11,
        None,
    )


@pytest.mark.asyncio
async def test_retiring_a_model_removes_its_workspace_overrides(factory):
    retired = model_seed.REMOVED_MODEL_KEYS[0]
    async with factory() as s:
        s.add_all(
            [
                ModelConfig(key=retired, name=retired, provider="openai"),
                ModelConfig(
                    key=retired, name=retired, provider="openai", group_id=WS_A
                ),
            ]
        )
        await s.commit()

    await model_seed.seed_async()

    assert await _all(factory, ModelConfig, ModelConfig.key == retired) == []


# ── Prompt templates ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_template_reseed_keeps_a_workspace_customisation(factory):
    first, second = template_seed.DEFAULT_TEMPLATES[:2]
    async with factory() as s:
        # Workspace A customised (or GEPA-applied) both; the global row of the
        # first is missing (a system admin deleted it).
        s.add_all(
            [
                PromptTemplate(
                    name=first["name"], template="ws-a custom 1", group_id=WS_A
                ),
                PromptTemplate(
                    name=second["name"], template="ws-a custom 2", group_id=WS_A
                ),
                PromptTemplate(name=second["name"], template="stale", is_active=False),
            ]
        )
        await s.commit()

    await template_seed.seed_async()
    await template_seed.seed_async()

    ws_rows = await _all(factory, PromptTemplate, PromptTemplate.group_id == WS_A)
    assert sorted(t.template for t in ws_rows) == ["ws-a custom 1", "ws-a custom 2"]

    globals_ = {
        t.name: t
        for t in await _all(factory, PromptTemplate, PromptTemplate.group_id.is_(None))
    }
    assert globals_[first["name"]].template == first["template"]  # re-created
    assert globals_[second["name"]].template == second["template"]  # refreshed
    assert globals_[second["name"]].is_active is False  # left as it was set
