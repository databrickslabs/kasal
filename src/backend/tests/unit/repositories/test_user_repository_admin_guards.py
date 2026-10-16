"""The system-admin guards are single conditional UPDATEs; check them on SQLite.

``revoke_system_admin_unless_last`` must never leave zero admins, even when two
admins remove each other at once; ``grant_first_system_admin`` must promote the
earliest user only, and only while no admin exists.
"""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.models.user import User
from src.repositories.user_repository import UserRepository

_T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _user(name, admin=False, minutes=0):
    return User(
        id=name,
        username=name,
        email=f"{name}@example.com",
        is_system_admin=admin,
        is_personal_workspace_manager=admin,
        created_at=_T0 + timedelta(minutes=minutes),
    )


@pytest_asyncio.fixture
async def maker(tmp_path):
    # A file database, so two sessions really are two connections.
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'users.db'}",
        connect_args={"timeout": 2},
    )
    async with engine.begin() as conn:
        await conn.run_sync(User.__table__.create)
    yield async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    await engine.dispose()


async def _seed(maker, *users):
    async with maker() as session:
        session.add_all(users)
        await session.commit()


async def _admins(maker):
    async with maker() as session:
        return await UserRepository(User, session).count_system_admins()


@pytest.mark.asyncio
async def test_the_last_admin_is_never_revoked(maker):
    await _seed(maker, _user("a", admin=True), _user("b", admin=True))
    async with maker() as session:
        repo = UserRepository(User, session)
        assert await repo.revoke_system_admin_unless_last("a") is True
        assert await repo.revoke_system_admin_unless_last("b") is False
        await session.commit()
    assert await _admins(maker) == 1


@pytest.mark.asyncio
async def test_two_admins_removing_each_other_leave_one(maker):
    """Each transaction would see the other admin if it only counted first."""
    await _seed(maker, _user("a", admin=True), _user("b", admin=True))
    first, second = maker(), maker()
    try:
        assert await UserRepository(User, first).revoke_system_admin_unless_last("a")
        # The second demotion starts while the first is still uncommitted.
        pending = asyncio.ensure_future(
            UserRepository(User, second).revoke_system_admin_unless_last("b")
        )
        await asyncio.sleep(0.2)
        await first.commit()
        try:
            await pending
            await second.commit()
        except OperationalError:
            await second.rollback()  # SQLite refused the second writer
    finally:
        await first.close()
        await second.close()
    assert await _admins(maker) == 1


@pytest.mark.asyncio
async def test_only_the_earliest_user_becomes_the_first_admin(maker):
    await _seed(maker, _user("later", minutes=5), _user("first", minutes=0))
    async with maker() as session:
        repo = UserRepository(User, session)
        assert await repo.grant_first_system_admin("later") is False
        assert await repo.grant_first_system_admin("first") is True
        assert await repo.grant_first_system_admin("first") is False  # already one
        await session.commit()
    assert await _admins(maker) == 1


@pytest.mark.asyncio
async def test_no_first_admin_once_an_admin_exists(maker):
    await _seed(maker, _user("first", minutes=0), _user("admin", True, minutes=5))
    async with maker() as session:
        assert (
            await UserRepository(User, session).grant_first_system_admin("first")
            is False
        )
