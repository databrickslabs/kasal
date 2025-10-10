import pytest
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace

from src.services.group_service import GroupService


class Ctx:
    def __init__(self, gid=None, email=None):
        self.primary_group_id = gid
        self.group_email = email


@pytest.mark.asyncio
async def test_ensure_group_exists_creates_personal_workspace():
    session = AsyncMock()
    with patch('src.services.group_service.GroupRepository') as GR, \
         patch('src.services.group_service.GroupUserRepository') as GUR:
        grepo = AsyncMock(); urepo = AsyncMock()
        GR.return_value = grepo; GUR.return_value = urepo
        grepo.get = AsyncMock(return_value=None)
        grepo.add = AsyncMock(side_effect=lambda g: g)
        svc = GroupService(session)
        ctx = Ctx(gid='user_abc', email='u@x.com')
        out = await svc.ensure_group_exists(ctx)
        assert out.name.startswith('Personal Workspace')
        grepo.add.assert_awaited()


@pytest.mark.asyncio
async def test_get_user_groups_filters_active():
    session = AsyncMock()
    with patch('src.services.group_service.GroupRepository') as GR, \
         patch('src.services.group_service.GroupUserRepository') as GUR:
        from src.models.enums import GroupUserStatus, GroupStatus
        grepo = AsyncMock(); urepo = AsyncMock()
        GR.return_value = grepo; GUR.return_value = urepo
        from types import SimpleNamespace as NS
        # Simulate 3 group users, only 2 active
        urepo.get_groups_by_user = AsyncMock(return_value=[
            NS(status=GroupUserStatus.ACTIVE, group=NS(status=GroupStatus.ACTIVE)),
            NS(status=GroupUserStatus.INACTIVE, group=NS(status=GroupStatus.ACTIVE)),
            NS(status=GroupUserStatus.ACTIVE, group=NS(status=GroupStatus.ACTIVE)),
        ])
        svc = GroupService(session)
        out = await svc.get_user_groups('u1')
        assert len(out) == 2


@pytest.mark.asyncio
async def test_remove_user_from_group_handles_false():
    session = AsyncMock()
    with patch('src.services.group_service.GroupRepository') as GR, \
         patch('src.services.group_service.GroupUserRepository') as GUR:
        grepo = AsyncMock(); urepo = AsyncMock()
        GR.return_value = grepo; GUR.return_value = urepo
        urepo.remove_user_from_group = AsyncMock(return_value=False)
        svc = GroupService(session)
        with pytest.raises(ValueError):
            await svc.remove_user_from_group('g1', 'u1')

