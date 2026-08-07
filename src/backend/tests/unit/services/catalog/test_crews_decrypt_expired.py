"""Decrypting a crew's tool_configs must not trigger a lazy load.

``_decrypt_crew_tool_configs`` is SYNC and is called from 13 places, several
immediately after a write. That makes plain attribute access unsafe: when the ORM
considers the instance EXPIRED, ``crew.tool_configs`` is not a dict read but a
lazy refresh — database IO from a sync frame. On the deployed app that raised, and
every crew save returned 500::

    POST /api/v1/crews  500 Internal Server Error
    greenlet_spawn has not been called; can't call await_only() here
      crews_router.py:202   create_crew
      crews.py:347          self._decrypt_crew_tool_configs(crew)
      crews.py:50           if crew and crew.tool_configs:
      sqlalchemy/orm/attributes.py  state._load_expired(...)

The fix reads the attribute defensively rather than chasing whichever operation
expired the instance — deliberately, because every sessionmaker in this codebase
sets ``expire_on_commit=False``, so the expiry source was NOT reproducible locally
and the guard must hold without knowing it.

``inspect(crew).dict`` is the loaded-attribute dict: a key is present only when the
value is in memory. Absent means touching the attribute would emit a SELECT.
``state.expired`` is not the test — an expired instance can still hold some
attributes, and only the genuinely missing ones would do IO.
"""

import tempfile
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import src.db.all_models  # noqa: F401  (register every model)
from src.db.base import Base
from src.models.crew import Crew
from src.services.catalog.crews import CrewService

SECRET = {"some_tool": {"api_key": "s3cret"}}


async def _engine():
    path = Path(tempfile.mkdtemp()) / "crews.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    return engine


def _crew(**overrides) -> Crew:
    fields = {
        "name": "c",
        "agent_ids": [],
        "task_ids": [],
        "tool_configs": SECRET,
        "group_id": "g1",
    }
    fields.update(overrides)
    return Crew(**fields)


@pytest.mark.asyncio
class TestAnExpiredInstance:
    async def test_decrypt_does_not_raise(self):
        """THE 500. ``expire_on_commit=True`` reproduces the deployed expiry."""
        engine = await _engine()
        try:
            sessions = async_sessionmaker(engine, expire_on_commit=True)
            async with sessions() as session:
                crew = _crew()
                session.add(crew)
                await session.flush()
                await session.commit()  # instance is now EXPIRED

                # Must not raise MissingGreenlet.
                result = CrewService(session)._decrypt_crew_tool_configs(crew)
                assert result is crew
        finally:
            await engine.dispose()

    async def test_the_whole_create_path_works(self):
        """create_with_group is what the endpoint calls; test it, not just the helper."""
        from src.schemas.crew import CrewCreate
        from src.utils.user_context import GroupContext

        engine = await _engine()
        try:
            sessions = async_sessionmaker(engine, expire_on_commit=True)
            async with sessions() as session:
                await session.commit()  # provoke the expiring condition
                crew = await CrewService(session).create_with_group(
                    CrewCreate(
                        name="My Crew",
                        agent_ids=[],
                        task_ids=[],
                        nodes=[],
                        edges=[],
                        tool_configs=SECRET,
                    ),
                    GroupContext(
                        group_ids=["g1"],
                        group_email="me@example.com",
                        email_domain="example.com",
                    ),
                )
            assert crew.name == "My Crew"
        finally:
            await engine.dispose()


@pytest.mark.asyncio
class TestALoadedInstance:
    async def test_decryption_still_happens(self):
        """The guard must not turn decryption off for the normal case."""
        engine = await _engine()
        try:
            sessions = async_sessionmaker(engine, expire_on_commit=False)
            async with sessions() as session:
                crew = _crew()
                session.add(crew)
                await session.flush()

                CrewService(session)._decrypt_crew_tool_configs(crew)
                # Nothing was dropped; the value is still there and usable.
                assert crew.tool_configs is not None
                assert "some_tool" in crew.tool_configs
        finally:
            await engine.dispose()

    async def test_the_loaded_check_reports_correctly(self):
        engine = await _engine()
        try:
            sessions = async_sessionmaker(engine, expire_on_commit=True)
            async with sessions() as session:
                crew = _crew()
                session.add(crew)
                await session.flush()
                assert CrewService._loaded_tool_configs(crew) is not None

                await session.commit()  # expire
                assert CrewService._loaded_tool_configs(crew) is None
        finally:
            await engine.dispose()


class TestDegenerateInputs:
    def test_none_is_returned_unchanged(self):
        assert (
            CrewService.__dict__["_decrypt_crew_tool_configs"](
                CrewService.__new__(CrewService), None
            )
            is None
        )

    def test_a_non_orm_object_does_not_break_the_check(self):
        """Test doubles and plain objects reach this helper too."""

        class NotAModel:
            tool_configs = None
            id = 1

        assert CrewService._loaded_tool_configs(NotAModel()) is None

    def test_an_empty_tool_configs_is_left_alone(self):
        engine_free_crew = _crew(tool_configs={})
        assert CrewService._loaded_tool_configs(engine_free_crew) == {}
