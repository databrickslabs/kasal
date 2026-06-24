"""Regression tests for the group-membership resolution cache.

The run-history UI polls execution status/traces while a job is RUNNING, and
every poll resolves the caller's group memberships. Resolving from the DB on
each poll (users + group_users + groups + commit) was the dominant query load
on the hot path. These tests pin the short-TTL cache that makes repeat polls
free, and the invalidation that keeps it correct when membership changes.
"""

import pytest
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace

import src.utils.user_context as uc
from src.utils.user_context import GroupContext, clear_membership_cache


@pytest.fixture(autouse=True)
def _clear_cache():
    """Each test starts and ends with an empty cache (module-level state)."""
    clear_membership_cache()
    yield
    clear_membership_cache()


def _user(email="alice@company.com"):
    return SimpleNamespace(
        id="u1",
        email=email,
        is_system_admin=False,
        is_personal_workspace_manager=False,
    )


@pytest.mark.asyncio
async def test_membership_resolved_once_within_ttl():
    """Two lookups for the same email within the TTL hit the DB only once."""
    user = _user()
    mock_exec = AsyncMock(return_value=(user, []))
    with patch("src.utils.asyncio_utils.execute_db_operation_smart", mock_exec):
        r1 = await GroupContext._get_user_group_memberships_with_roles(
            "alice@company.com"
        )
        r2 = await GroupContext._get_user_group_memberships_with_roles(
            "alice@company.com"
        )

    assert r1 == (user, [])
    assert r2 == (user, [])
    # Second call was served from cache.
    assert mock_exec.await_count == 1


@pytest.mark.asyncio
async def test_distinct_emails_are_cached_separately():
    """Different users do not share a cache entry."""
    mock_exec = AsyncMock(side_effect=[(_user("a@x.com"), []), (_user("b@x.com"), [])])
    with patch("src.utils.asyncio_utils.execute_db_operation_smart", mock_exec):
        await GroupContext._get_user_group_memberships_with_roles("a@x.com")
        await GroupContext._get_user_group_memberships_with_roles("b@x.com")

    assert mock_exec.await_count == 2


@pytest.mark.asyncio
async def test_clear_membership_cache_for_email_forces_relookup():
    """Invalidating a specific user re-reads on the next request."""
    user = _user()
    mock_exec = AsyncMock(return_value=(user, []))
    with patch("src.utils.asyncio_utils.execute_db_operation_smart", mock_exec):
        await GroupContext._get_user_group_memberships_with_roles("alice@company.com")
        clear_membership_cache("alice@company.com")
        await GroupContext._get_user_group_memberships_with_roles("alice@company.com")

    assert mock_exec.await_count == 2


@pytest.mark.asyncio
async def test_clear_membership_cache_all_forces_relookup():
    """A full clear (used by user_id-only mutations) drops every entry."""
    mock_exec = AsyncMock(return_value=(_user(), []))
    with patch("src.utils.asyncio_utils.execute_db_operation_smart", mock_exec):
        await GroupContext._get_user_group_memberships_with_roles("alice@company.com")
        clear_membership_cache()  # whole-cache invalidation
        await GroupContext._get_user_group_memberships_with_roles("alice@company.com")

    assert mock_exec.await_count == 2


@pytest.mark.asyncio
async def test_expired_entry_triggers_relookup(monkeypatch):
    """An entry past its TTL is not served from cache."""
    mock_exec = AsyncMock(return_value=(_user(), []))
    # TTL of 0 => the entry is already stale when the next call checks it.
    monkeypatch.setattr(uc, "_MEMBERSHIP_CACHE_TTL", 0)
    with patch("src.utils.asyncio_utils.execute_db_operation_smart", mock_exec):
        await GroupContext._get_user_group_memberships_with_roles("alice@company.com")
        await GroupContext._get_user_group_memberships_with_roles("alice@company.com")

    assert mock_exec.await_count == 2


@pytest.mark.asyncio
async def test_failed_lookup_is_not_cached():
    """A failed resolution (user is None) is not pinned for the whole TTL."""
    mock_exec = AsyncMock(return_value=(None, []))
    with patch("src.utils.asyncio_utils.execute_db_operation_smart", mock_exec):
        await GroupContext._get_user_group_memberships_with_roles("ghost@company.com")
        await GroupContext._get_user_group_memberships_with_roles("ghost@company.com")

    assert mock_exec.await_count == 2
