"""
Coverage tests for services/user_service.py
Covers missing lines: get_user (25), get_user_complete (68), update_user_permissions (101-112),
assign_role (122), get_or_create_user_by_email branches, _handle_first_user_admin_setup, delete_user
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from src.services.groups.users import UserService
from src.schemas.user import UserUpdate, UserPermissionUpdate, UserRole


def make_service():
    session = AsyncMock()
    with patch('src.services.groups.users.UserRepository') as MockRepo:
        mock_repo = AsyncMock()
        MockRepo.return_value = mock_repo
        svc = UserService(session)
        svc.user_repo = mock_repo
    return svc


def make_user(id="u1", email="user@example.com", is_system_admin=False):
    user = MagicMock()
    user.id = id
    user.email = email
    user.username = "user"
    user.display_name = "User"
    user.is_system_admin = is_system_admin
    user.is_personal_workspace_manager = False
    return user


# ---- get_user ----

@pytest.mark.asyncio
async def test_get_user():
    svc = make_service()
    user = make_user()
    svc.user_repo.get = AsyncMock(return_value=user)
    result = await svc.get_user("u1")
    assert result is user


# ---- get_user_complete ----

@pytest.mark.asyncio
async def test_get_user_complete():
    svc = make_service()
    user = make_user()
    svc.user_repo.get = AsyncMock(return_value=user)
    result = await svc.get_user_complete("u1")
    assert result is user


# ---- update_user_permissions ----

@pytest.mark.asyncio
async def test_update_user_permissions_not_found():
    svc = make_service()
    svc.user_repo.get = AsyncMock(return_value=None)
    perm = MagicMock(spec=UserPermissionUpdate)
    perm.model_dump.return_value = {"is_system_admin": True}
    result = await svc.update_user_permissions("u1", perm)
    assert result is None


@pytest.mark.asyncio
async def test_update_user_permissions_success():
    svc = make_service()
    user = make_user()
    svc.user_repo.get = AsyncMock(return_value=user)
    svc.user_repo.update = AsyncMock()
    perm = MagicMock(spec=UserPermissionUpdate)
    perm.model_dump.return_value = {"is_system_admin": True}
    result = await svc.update_user_permissions("u1", perm)
    assert result is user


# ---- assign_role ----

@pytest.mark.asyncio
async def test_assign_role_not_found():
    svc = make_service()
    svc.user_repo.get = AsyncMock(return_value=None)
    result = await svc.assign_role("u1", "admin")
    assert result is None


@pytest.mark.asyncio
async def test_assign_role_success():
    svc = make_service()
    user = make_user()
    svc.user_repo.get = AsyncMock(return_value=user)
    svc.user_repo.update = AsyncMock()
    result = await svc.assign_role("u1", "admin")
    assert result is user


# ---- get_or_create_user_by_email ----

@pytest.mark.asyncio
async def test_get_or_create_existing_user_no_update_login():
    svc = make_service()
    user = make_user(is_system_admin=True)
    svc.user_repo.get_by_email = AsyncMock(return_value=user)
    with patch.object(svc, '_handle_first_user_admin_setup', new_callable=AsyncMock):
        result = await svc.get_or_create_user_by_email("user@example.com", update_login=False)
    assert result is user


@pytest.mark.asyncio
async def test_get_or_create_existing_user_with_update_login():
    svc = make_service()
    user = make_user()
    svc.user_repo.get_by_email = AsyncMock(return_value=user)
    svc.user_repo.update_last_login = AsyncMock()
    with patch.object(svc, '_handle_first_user_admin_setup', new_callable=AsyncMock):
        result = await svc.get_or_create_user_by_email("user@example.com", update_login=True)
    assert result is user
    svc.user_repo.update_last_login.assert_called_once_with("u1")


@pytest.mark.asyncio
async def test_get_or_create_update_login_exception():
    """Test that last_login update exception is swallowed."""
    svc = make_service()
    user = make_user()
    svc.user_repo.get_by_email = AsyncMock(return_value=user)
    svc.user_repo.update_last_login = AsyncMock(side_effect=Exception("lock error"))
    with patch.object(svc, '_handle_first_user_admin_setup', new_callable=AsyncMock):
        result = await svc.get_or_create_user_by_email("user@example.com", update_login=True)
    assert result is user  # Should still return user despite login update error


@pytest.mark.asyncio
async def test_get_or_create_new_user_unique_username():
    svc = make_service()
    new_user = make_user(email="newuser@example.com")
    svc.user_repo.get_by_email = AsyncMock(return_value=None)
    svc.user_repo.get_by_username = AsyncMock(return_value=None)  # username is available
    svc.user_repo.create = AsyncMock(return_value=new_user)
    with patch.object(svc, '_handle_first_user_admin_setup', new_callable=AsyncMock):
        result = await svc.get_or_create_user_by_email("newuser@example.com")
    assert result is new_user


@pytest.mark.asyncio
async def test_get_or_create_new_user_duplicate_username():
    svc = make_service()
    new_user = make_user(email="newuser@example.com")
    existing_user_same_name = make_user(id="other", email="other@example.com")
    svc.user_repo.get_by_email = AsyncMock(return_value=None)
    # First call: username taken; second call: username with counter available
    svc.user_repo.get_by_username = AsyncMock(side_effect=[existing_user_same_name, None])
    svc.user_repo.create = AsyncMock(return_value=new_user)
    with patch.object(svc, '_handle_first_user_admin_setup', new_callable=AsyncMock):
        result = await svc.get_or_create_user_by_email("newuser@example.com")
    assert result is new_user


@pytest.mark.asyncio
async def test_get_or_create_race_condition():
    """Test race condition handling when UNIQUE constraint fails."""
    svc = make_service()
    recovered_user = make_user(email="race@example.com")
    svc.user_repo.get_by_email = AsyncMock(side_effect=[
        None,  # First check: not found
        recovered_user,  # After rollback: found
    ])
    svc.user_repo.get_by_username = AsyncMock(return_value=None)
    svc.user_repo.create = AsyncMock(side_effect=Exception("UNIQUE constraint failed: users.email"))
    svc.session = AsyncMock()
    svc.session.rollback = AsyncMock()
    svc.session.expunge_all = MagicMock()
    with patch.object(svc, '_handle_first_user_admin_setup', new_callable=AsyncMock):
        result = await svc.get_or_create_user_by_email("race@example.com")
    assert result is recovered_user


@pytest.mark.asyncio
async def test_get_or_create_race_condition_user_still_not_found():
    """Test race condition where user still not found after rollback."""
    svc = make_service()
    svc.user_repo.get_by_email = AsyncMock(side_effect=[None, None])
    svc.user_repo.get_by_username = AsyncMock(return_value=None)
    svc.user_repo.create = AsyncMock(side_effect=Exception("UNIQUE constraint failed"))
    svc.session = AsyncMock()
    svc.session.rollback = AsyncMock()
    svc.session.expunge_all = MagicMock()
    with pytest.raises(Exception):
        await svc.get_or_create_user_by_email("race@example.com")


@pytest.mark.asyncio
async def test_get_or_create_invalid_email():
    """Test that invalid email returns None."""
    svc = make_service()
    result = await svc.get_or_create_user_by_email("invalid-not-email")
    assert result is None


@pytest.mark.asyncio
async def test_get_or_create_at_localhost_allowed():
    """Test that @localhost email is allowed."""
    svc = make_service()
    user = make_user(email="admin@localhost")
    svc.user_repo.get_by_email = AsyncMock(return_value=user)
    with patch.object(svc, '_handle_first_user_admin_setup', new_callable=AsyncMock):
        result = await svc.get_or_create_user_by_email("admin@localhost")
    assert result is user


# ---- _handle_first_user_admin_setup ----

@pytest.mark.asyncio
async def test_handle_first_user_admin_already_admin():
    svc = make_service()
    user = make_user(is_system_admin=True)
    # Already admin - should return early
    await svc._handle_first_user_admin_setup(user, is_new_user=False)
    svc.user_repo.update.assert_not_called()


@pytest.mark.asyncio
async def test_handle_first_user_admin_no_admins_existing():
    svc = make_service()
    user = make_user(is_system_admin=False)
    
    # No admins exist, should grant admin
    svc.user_repo.count_system_admins = AsyncMock(return_value=0)
    svc.user_repo.update = AsyncMock()
    
    await svc._handle_first_user_admin_setup(user, is_new_user=False)
    svc.user_repo.update.assert_called_once()


@pytest.mark.asyncio
async def test_handle_first_user_admin_existing_admins():
    svc = make_service()
    user = make_user(is_system_admin=False)
    
    # Admins exist - don't grant
    svc.user_repo.count_system_admins = AsyncMock(return_value=2)
    svc.user_repo.update = AsyncMock()
    
    await svc._handle_first_user_admin_setup(user, is_new_user=False)
    svc.user_repo.update.assert_not_called()


@pytest.mark.asyncio
async def test_handle_first_user_new_user_first():
    svc = make_service()
    user = make_user(is_system_admin=False)
    svc.user_repo.count = AsyncMock(return_value=1)  # Only this user
    svc.user_repo.update = AsyncMock()
    
    await svc._handle_first_user_admin_setup(user, is_new_user=True)
    svc.user_repo.update.assert_called_once()


@pytest.mark.asyncio
async def test_handle_first_user_new_user_not_first():
    svc = make_service()
    user = make_user(is_system_admin=False)
    svc.user_repo.count = AsyncMock(return_value=5)  # Multiple users
    svc.user_repo.update = AsyncMock()
    
    await svc._handle_first_user_admin_setup(user, is_new_user=True)
    svc.user_repo.update.assert_not_called()


@pytest.mark.asyncio
async def test_handle_first_user_exception_swallowed():
    svc = make_service()
    user = make_user()
    svc.user_repo.count = AsyncMock(side_effect=Exception("count error"))
    # Should not raise
    await svc._handle_first_user_admin_setup(user, is_new_user=True)


# ---- delete_user ----

@pytest.mark.asyncio
async def test_delete_user_not_found():
    svc = make_service()
    svc.user_repo.get = AsyncMock(return_value=None)
    result = await svc.delete_user("u1")
    assert result is False


@pytest.mark.asyncio
async def test_delete_user_success():
    svc = make_service()
    user = make_user()
    svc.user_repo.get = AsyncMock(return_value=user)
    svc.user_repo.delete = AsyncMock()

    mock_group_svc = AsyncMock()
    mock_group_svc.get_user_groups = AsyncMock(return_value=[])

    # GroupService is imported locally inside delete_user
    with patch.dict('sys.modules', {
        'src.services.groups.groups': MagicMock(GroupService=MagicMock(return_value=mock_group_svc))
    }):
        result = await svc.delete_user("u1")

    assert result is True
    svc.user_repo.delete.assert_called_once_with("u1")


@pytest.mark.asyncio
async def test_delete_user_with_groups():
    svc = make_service()
    user = make_user()
    svc.user_repo.get = AsyncMock(return_value=user)
    svc.user_repo.delete = AsyncMock()

    group = MagicMock()
    group.id = "g1"

    mock_group_svc = AsyncMock()
    mock_group_svc.get_user_groups = AsyncMock(return_value=[group])
    mock_group_svc.remove_user_from_group = AsyncMock()

    with patch.dict('sys.modules', {
        'src.services.groups.groups': MagicMock(GroupService=MagicMock(return_value=mock_group_svc))
    }):
        result = await svc.delete_user("u1")

    assert result is True
    mock_group_svc.remove_user_from_group.assert_called_once_with("g1", "u1")

# ==========================================================================
# Additional coverage: get_users search merging, update_user uniqueness
# checks, and more get_or_create_user_by_email / assign_role branches
# ==========================================================================
from types import SimpleNamespace


@pytest.mark.asyncio
async def test_get_users_search_merges_and_limits():
    session = AsyncMock()
    with patch('src.services.groups.users.UserRepository') as Repo:
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
    with patch('src.services.groups.users.UserRepository') as Repo:
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
    with patch('src.services.groups.users.UserRepository') as Repo:
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
    with patch('src.services.groups.users.UserRepository') as Repo:
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
    with patch('src.services.groups.users.UserRepository') as Repo:
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
    with patch('src.services.groups.users.UserRepository') as Repo:
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

