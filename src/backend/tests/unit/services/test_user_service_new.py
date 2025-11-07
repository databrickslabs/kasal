import pytest
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace

from src.services.user_service import UserService


@pytest.mark.asyncio
async def test_get_users_search_merges_and_limits():
    session = AsyncMock()
    with patch('src.services.user_service.UserRepository') as Repo:
        repo = AsyncMock()
        user1 = SimpleNamespace(id='1')
        user2 = SimpleNamespace(id='2')
        # Overlap (user2 present in both lists)
        repo.list = AsyncMock(side_effect=[[user1, user2], [user2]])
        Repo.return_value = repo

        svc = UserService(session)
        out = await svc.get_users(skip=0, limit=10, search='abc')
        assert [u.id for u in out] == ['1', '2']
        assert repo.list.call_count == 2


@pytest.mark.asyncio
async def test_update_user_uniqueness_and_success():
    session = AsyncMock()
    with patch('src.services.user_service.UserRepository') as Repo:
        repo = AsyncMock()
        # Existing user fetched initially
        repo.get = AsyncMock(return_value=SimpleNamespace(id='u1'))
        # Username conflict
        repo.get_by_username = AsyncMock(return_value=SimpleNamespace(id='other'))
        Repo.return_value = repo

        svc = UserService(session)
        from src.schemas.user import UserUpdate
        with pytest.raises(ValueError):
            await svc.update_user('u1', UserUpdate(username='dup'))

        # Now no conflicts, email unique
        repo.get_by_username = AsyncMock(return_value=None)
        repo.get_by_email = AsyncMock(return_value=None)
        repo.update = AsyncMock()
        # get() called twice: existence check and return updated
        repo.get = AsyncMock(side_effect=[SimpleNamespace(id='u1'), SimpleNamespace(id='u1')])
        await svc.update_user('u1', UserUpdate(username='okay', email='a@b.com'))
        assert repo.update.called


@pytest.mark.asyncio
async def test_update_user_not_found_returns_none():
    session = AsyncMock()
    with patch('src.services.user_service.UserRepository') as Repo:
        repo = AsyncMock()
        repo.get = AsyncMock(return_value=None)
        Repo.return_value = repo
        from src.schemas.user import UserUpdate
        svc = UserService(session)
        out = await svc.update_user('nope', UserUpdate())
        assert out is None


@pytest.mark.asyncio
async def test_assign_role_calls_update():
    session = AsyncMock()
    with patch('src.services.user_service.UserRepository') as Repo:
        repo = AsyncMock()
        repo.get = AsyncMock(return_value=SimpleNamespace(id='u1'))
        repo.update = AsyncMock()
        Repo.return_value = repo
        svc = UserService(session)
        await svc.assign_role('u1', 'ADMIN')
        repo.update.assert_called_once()


@pytest.mark.asyncio
async def test_get_or_create_user_by_email_existing_user_no_update_login():
    session = AsyncMock()
    with patch('src.services.user_service.UserRepository') as Repo:
        repo = AsyncMock()
        existing = SimpleNamespace(id='u1', email='e@x.com')
        repo.get_by_email = AsyncMock(return_value=existing)
        Repo.return_value = repo

        svc = UserService(session)
        # Patch admin setup to avoid DB count path
        with patch.object(svc, '_handle_first_user_admin_setup', new=AsyncMock()) as setup:
            out = await svc.get_or_create_user_by_email('e@x.com')
            assert out is existing
            setup.assert_awaited()


@pytest.mark.asyncio
async def test_get_or_create_user_by_email_create_new_and_first_user_admin():
    session = AsyncMock()
    with patch('src.services.user_service.UserRepository') as Repo:
        repo = AsyncMock()
        repo.get_by_email = AsyncMock(return_value=None)
        repo.get_by_username = AsyncMock(side_effect=[None, None])
        created = SimpleNamespace(id='u2', email='new@x.com')
        repo.create = AsyncMock(return_value=created)
        repo.count = AsyncMock(return_value=1)  # first user
        repo.update = AsyncMock()
        Repo.return_value = repo

        svc = UserService(session)
        out = await svc.get_or_create_user_by_email('new@x.com')
        assert out is created
        # Should grant admin to first user
        repo.update.assert_called()

