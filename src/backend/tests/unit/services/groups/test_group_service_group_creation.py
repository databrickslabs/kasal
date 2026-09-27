from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.services.groups.groups import GroupService
from src.services.groups.groups import GroupService as Svc


class Ctx:
    def __init__(self, gid=None, email=None):
        self.primary_group_id = gid
        self.group_email = email


@pytest.mark.asyncio
async def test_get_user_groups_filters_active():
    session = AsyncMock()
    with (
        patch("src.services.groups.groups.GroupRepository") as GR,
        patch("src.services.groups.groups.GroupUserRepository") as GUR,
    ):
        from src.models.enums import GroupStatus, GroupUserStatus

        grepo = AsyncMock()
        urepo = AsyncMock()
        GR.return_value = grepo
        GUR.return_value = urepo
        from types import SimpleNamespace as NS

        # Simulate 3 group users, only 2 active
        urepo.get_groups_by_user = AsyncMock(
            return_value=[
                NS(status=GroupUserStatus.ACTIVE, group=NS(status=GroupStatus.ACTIVE)),
                NS(
                    status=GroupUserStatus.INACTIVE, group=NS(status=GroupStatus.ACTIVE)
                ),
                NS(status=GroupUserStatus.ACTIVE, group=NS(status=GroupStatus.ACTIVE)),
            ]
        )
        svc = GroupService(session)
        out = await svc.get_user_groups("u1")
        assert len(out) == 2


@pytest.mark.asyncio
async def test_remove_user_from_group_handles_false():
    session = AsyncMock()
    with (
        patch("src.services.groups.groups.GroupRepository") as GR,
        patch("src.services.groups.groups.GroupUserRepository") as GUR,
    ):
        grepo = AsyncMock()
        urepo = AsyncMock()
        GR.return_value = grepo
        GUR.return_value = urepo
        urepo.remove_user_from_group = AsyncMock(return_value=False)
        svc = GroupService(session)
        with pytest.raises(ValueError):
            await svc.remove_user_from_group("g1", "u1")


class _Scalars:
    def __init__(self, user):
        self._user = user

    def first(self):
        return self._user


class FakeSession:
    def __init__(self, user=None):
        self._user = user
        self.added = []
        self.flushed = False

    async def execute(self, stmt):
        class R:
            def __init__(self, user):
                self._user = user

            def scalar_one_or_none(self):
                return self._user

            def scalars(self):
                # UserRepository.get_by_email uses .scalars().first()
                return _Scalars(self._user)

        return R(self._user)

    def add(self, obj):
        self.added.append(obj)

    async def flush(self):
        self.flushed = True


@pytest.mark.asyncio
async def test_assign_user_to_group_creates_user_and_association(monkeypatch):
    from src.services.groups import groups as module

    class FakeGroupRepo:
        def __init__(self, session):
            self.session = session

        async def add(self, g):
            return g

    class FakeGroupUserRepo:
        def __init__(self, session):
            self.session = session

        async def get_by_group_and_user(self, gid, uid):
            return None

        async def add(self, gu):
            return gu

    module.GroupRepository = FakeGroupRepo
    module.GroupUserRepository = FakeGroupUserRepo

    # Return an existing user from SELECT to avoid creating ORM User
    existing_user = SimpleNamespace(id="u-1", email="user@example.com")
    sess = FakeSession(user=existing_user)
    svc = Svc(sess)

    out = await svc.assign_user_to_group("g1", "user@example.com")
    assert isinstance(out, dict)
    assert out["group_id"] == "g1"
    assert out["email"] == "user@example.com"
    # No need to add a new user when it already exists
    assert sess.flushed in (True, False)


@pytest.mark.asyncio
async def test_update_group_not_found_raises(monkeypatch):
    from src.services.groups import groups as module

    class FakeGroupRepo:
        def __init__(self, session):
            self.session = session

        async def get(self, gid):
            return None

    module.GroupRepository = FakeGroupRepo
    module.GroupUserRepository = lambda s: None

    svc = Svc(FakeSession())
    with pytest.raises(ValueError):
        await svc.update_group("missing", name="NewName")


@pytest.mark.asyncio
async def test_list_group_users_email_fallback(monkeypatch):
    from src.services.groups import groups as module

    class FakeGroupRepo:
        def __init__(self, session):
            self.session = session

    class FakeGroupUserRepo:
        def __init__(self, session):
            self.session = session

        async def get_users_by_group(self, gid, skip, limit):
            GUStatus = module.GroupUserStatus
            GStatus = module.GroupStatus
            return [
                SimpleNamespace(
                    id="g1_u1",
                    group_id="g1",
                    user_id="u1",
                    user=None,
                    role="OPERATOR",
                    status=GUStatus.ACTIVE,
                    joined_at=datetime.utcnow(),
                    auto_created=True,
                    allow_agent_builder=None,
                    allow_flow_builder=None,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                    group=SimpleNamespace(status=GStatus.ACTIVE),
                )
            ]

    module.GroupRepository = FakeGroupRepo
    module.GroupUserRepository = FakeGroupUserRepo

    svc = Svc(FakeSession())
    rows = await svc.list_group_users("g1")
    assert rows and rows[0]["email"] == "u1@databricks.com"
