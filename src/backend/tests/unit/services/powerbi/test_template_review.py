"""Reviewing Power BI templates created before the N2 fix (read-only).

A template is visible to every tenant. Rows a workspace user made a template
before creation needed a system admin must be found and reviewed; nothing is
migrated automatically.
"""

from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.core.exceptions import ForbiddenError
from src.models.conversion import SavedConverterConfiguration
from src.models.user import User
from src.services.powerbi.conversions import ConverterService
from src.utils.user_context import GroupContext


def _config(name, email, *, template=True, group="team_a"):
    return SavedConverterConfiguration(
        name=name,
        source_format="powerbi",
        target_format="dax",
        configuration={},
        is_template=template,
        group_id=group,
        created_by_email=email,
    )


@pytest_asyncio.fixture
async def maker():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        for table in (User.__table__, SavedConverterConfiguration.__table__):
            await conn.run_sync(lambda sync, t=table: t.create(sync, checkfirst=True))
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as s:
        s.add_all(
            [
                User(username="root", email="Root@Example.com", is_system_admin=True),
                User(username="alice", email="alice@example.com"),
                _config("admin template", "root@example.com"),
                _config("pre-fix template", "alice@example.com"),
                _config("orphan template", "gone@example.com", group="team_b"),
                _config("private config", "alice@example.com", template=False),
            ]
        )
        await s.commit()
    yield factory
    await engine.dispose()


@pytest.mark.asyncio
async def test_lists_only_templates_by_non_admins(maker):
    async with maker() as s:
        rows = await ConverterService(s).list_templates_created_by_non_admins()
        # Admin email matched case-insensitively; non-templates never listed.
        assert sorted(r.name for r in rows) == ["orphan template", "pre-fix template"]
        # Read-only: nothing pending.
        assert not s.new and not s.dirty and not s.deleted


@pytest.mark.asyncio
async def test_system_admin_context_may_review(maker):
    admin = GroupContext(
        group_ids=["team_a"], current_user=SimpleNamespace(is_system_admin=True)
    )
    async with maker() as s:
        rows = await ConverterService(s, admin).list_templates_created_by_non_admins()
        assert len(rows) == 2


@pytest.mark.asyncio
async def test_workspace_user_may_not_review(maker):
    user = GroupContext(
        group_ids=["team_a"], current_user=SimpleNamespace(is_system_admin=False)
    )
    async with maker() as s:
        with pytest.raises(ForbiddenError):
            await ConverterService(s, user).list_templates_created_by_non_admins()


def test_script_prints_the_rows(maker, monkeypatch, capsys):
    import json

    from scripts.maintenance import list_unreviewed_powerbi_templates as script

    import src.db.session as db_session

    @asynccontextmanager
    async def routed():
        async with maker() as s:
            yield s

    monkeypatch.setattr(db_session, "routed_scoped_session", routed)
    assert script.main(["--json"]) == 0
    rows = json.loads(capsys.readouterr().out)
    assert sorted(r["created_by_email"] for r in rows) == [
        "alice@example.com",
        "gone@example.com",
    ]
