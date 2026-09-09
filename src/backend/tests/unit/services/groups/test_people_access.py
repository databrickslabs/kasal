"""Persistence regressions for pre-provisioned identities and login reporting."""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.models.user import User
from src.schemas.user import UserPermissionUpdate
from src.services.groups.users import UserService


@pytest_asyncio.fixture
async def database():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(User.__table__.create)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


@pytest.mark.asyncio
async def test_permissions_survive_first_login_and_timestamp_is_durable(database):
    async with database() as session:
        session.add(
            User(username="admin", email="admin@example.com", is_system_admin=True)
        )
        await session.commit()
        service = UserService(session)
        person = await service.provision_user("new@example.com")
        assert person.last_login is None
        assert not person.is_system_admin
        assert not person.is_personal_workspace_manager
        assert person.personal_group_id
        identity = person.id
        await service.update_user_permissions(
            identity, UserPermissionUpdate(is_personal_workspace_manager=True)
        )
        await session.commit()

    async with database() as session:
        service = UserService(session)
        duplicate = await service.provision_user("new@example.com")
        assert duplicate.id == identity
        assert duplicate.last_login is None
        signed_in = await service.get_or_create_user_by_email("new@example.com")
        assert signed_in.id == identity
        assert signed_in.is_personal_workspace_manager
        await service.record_login(identity)

    async with database() as session:
        service = UserService(session)
        signed_in = await service.get_user(identity)
        assert signed_in.last_login is not None
        with patch.object(
            service.user_repo, "update_last_login", new_callable=AsyncMock
        ) as write:
            await service.record_login(identity)
            write.assert_not_awaited()
        signed_in.last_login = datetime.now(timezone.utc) - timedelta(minutes=6)
        await session.commit()
        await service.record_login(identity)
        await session.refresh(signed_in)
        assert signed_in.last_login.replace(tzinfo=timezone.utc) > datetime.now(
            timezone.utc
        ) - timedelta(seconds=10)


@pytest.mark.asyncio
async def test_search_is_case_insensitive_literal_and_paginated(database):
    async with database() as session:
        service = UserService(session)
        await service.provision_user("ada@example.com")
        await service.provision_user("adam@example.com")
        await service.provision_user("someone@example.com")
        first = await service.get_users(search="ADA", limit=1)
        second = await service.get_users(search="ADA", skip=1, limit=1)
        assert [p.email for p in first] == ["ada@example.com"]
        assert [p.email for p in second] == ["adam@example.com"]
        assert await service.get_users(search="%") == []
        assert await service.get_users(search="_") == [
            first[0],
            second[0],
            await service.user_repo.get_by_email("someone@example.com"),
        ]
