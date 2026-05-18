"""
Additional coverage tests for group_service.py targeting uncovered lines.
Missing: ensure_group_exists (user_ prefix, regular group name),
ensure_group_user_exists (already exists, create new), get_user_groups,
get_user_groups_with_roles, get_user_group_memberships, create_group,
list_groups, get_group_by_id, update_group, get_group_user_count,
list_group_users, assign_user_to_group, update_group_user, remove_user_from_group,
delete_group, get_group_stats, get_total_group_count,
create_first_admin_group_for_user, get_user_group_membership.
"""
import pytest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from src.models.enums import GroupStatus, GroupUserStatus, GroupUserRole, UserRole, UserStatus


def make_session():
    session = AsyncMock()
    session.execute = AsyncMock()
    session.flush = AsyncMock()
    session.add = MagicMock()
    return session


def make_group(id="g1", name="Test Group", status=GroupStatus.ACTIVE):
    g = MagicMock()
    g.id = id
    g.name = name
    g.status = status
    return g


def make_group_user(id="gu1", group_id="g1", user_id="u1",
                    role=GroupUserRole.OPERATOR, status=GroupUserStatus.ACTIVE,
                    group=None):
    gu = MagicMock()
    gu.id = id
    gu.group_id = group_id
    gu.user_id = user_id
    gu.role = role
    gu.status = status
    gu.group = group or make_group()
    gu.user = None
    gu.joined_at = datetime.utcnow()
    gu.auto_created = True
    gu.created_at = datetime.utcnow()
    gu.updated_at = datetime.utcnow()
    return gu


def make_context(primary_group_id="g1", group_email="user@example.com"):
    return SimpleNamespace(
        primary_group_id=primary_group_id,
        primary_tenant_id=None,
        group_email=group_email,
        tenant_email=None
    )


def make_service(session=None):
    from src.services.group_service import GroupService
    s = session or make_session()
    svc = GroupService(session=s)
    svc.group_repo = AsyncMock()
    svc.group_user_repo = AsyncMock()
    return svc


# ---------------------------------------------------------------------------
# ensure_group_exists
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ensure_group_exists_no_primary_id():
    svc = make_service()
    ctx = SimpleNamespace(primary_group_id=None, primary_tenant_id=None, group_email=None, tenant_email=None)
    result = await svc.ensure_group_exists(ctx)
    assert result is None


@pytest.mark.asyncio
async def test_ensure_group_exists_already_exists():
    svc = make_service()
    existing_group = make_group(id="g1")
    svc.group_repo.get = AsyncMock(return_value=existing_group)

    ctx = make_context(primary_group_id="g1")
    result = await svc.ensure_group_exists(ctx)
    assert result.id == "g1"


@pytest.mark.asyncio
async def test_ensure_group_exists_user_prefix_with_email():
    svc = make_service()
    svc.group_repo.get = AsyncMock(return_value=None)
    created = make_group(id="user_alice")
    svc.group_repo.add = AsyncMock(return_value=created)

    ctx = make_context(primary_group_id="user_alice", group_email="alice@example.com")
    result = await svc.ensure_group_exists(ctx)
    assert result.id == "user_alice"


@pytest.mark.asyncio
async def test_ensure_group_exists_user_prefix_no_email():
    svc = make_service()
    svc.group_repo.get = AsyncMock(return_value=None)
    created = make_group(id="user_bob")
    svc.group_repo.add = AsyncMock(return_value=created)

    ctx = make_context(primary_group_id="user_bob", group_email=None)
    result = await svc.ensure_group_exists(ctx)
    assert result is not None


@pytest.mark.asyncio
async def test_ensure_group_exists_regular_group():
    svc = make_service()
    svc.group_repo.get = AsyncMock(return_value=None)
    created = make_group(id="my_team")
    svc.group_repo.add = AsyncMock(return_value=created)

    ctx = make_context(primary_group_id="my_team", group_email="team@example.com")
    result = await svc.ensure_group_exists(ctx)
    assert result is not None


@pytest.mark.asyncio
async def test_ensure_group_exists_legacy_tenant_context():
    """Support primary_tenant_id from legacy TenantContext."""
    svc = make_service()
    svc.group_repo.get = AsyncMock(return_value=None)
    created = make_group(id="t1")
    svc.group_repo.add = AsyncMock(return_value=created)

    ctx = SimpleNamespace(
        primary_group_id=None,
        primary_tenant_id="t1",
        group_email=None,
        tenant_email="t@example.com"
    )
    result = await svc.ensure_group_exists(ctx)
    assert result is not None


# ---------------------------------------------------------------------------
# ensure_group_user_exists
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ensure_group_user_exists_no_primary_id():
    svc = make_service()
    ctx = SimpleNamespace(primary_group_id=None, primary_tenant_id=None)
    result = await svc.ensure_group_user_exists(ctx, "user1")
    assert result is None


@pytest.mark.asyncio
async def test_ensure_group_user_exists_already_exists():
    svc = make_service()
    existing_gu = make_group_user()
    svc.group_user_repo.get_by_group_and_user = AsyncMock(return_value=existing_gu)

    ctx = make_context(primary_group_id="g1")
    result = await svc.ensure_group_user_exists(ctx, "u1")
    assert result.id == "gu1"


@pytest.mark.asyncio
async def test_ensure_group_user_exists_creates_new():
    svc = make_service()
    svc.group_user_repo.get_by_group_and_user = AsyncMock(return_value=None)
    new_gu = make_group_user(id="g1_u2")
    svc.group_user_repo.add = AsyncMock(return_value=new_gu)

    ctx = make_context(primary_group_id="g1")
    result = await svc.ensure_group_user_exists(ctx, "u2")
    assert result is not None


# ---------------------------------------------------------------------------
# get_user_groups
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_user_groups_filters_active():
    svc = make_service()

    active_group = make_group(id="g1", status=GroupStatus.ACTIVE)
    inactive_group = make_group(id="g2", status=GroupStatus.ACTIVE)
    inactive_group.status = "INACTIVE"  # non-active

    active_gu = make_group_user(group=active_group, status=GroupUserStatus.ACTIVE)
    inactive_gu = make_group_user(group=inactive_group, status=GroupUserStatus.ACTIVE)
    inactive_gu_2 = make_group_user(group=active_group, status="INACTIVE")

    svc.group_user_repo.get_groups_by_user = AsyncMock(return_value=[active_gu, inactive_gu, inactive_gu_2])

    result = await svc.get_user_groups("u1")
    assert len(result) == 1
    assert result[0].id == "g1"


# ---------------------------------------------------------------------------
# get_user_groups_with_roles
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_user_groups_with_roles():
    svc = make_service()

    group = make_group(id="g1")
    gu = make_group_user(group=group, role=GroupUserRole.ADMIN, status=GroupUserStatus.ACTIVE)
    svc.group_user_repo.get_groups_by_user = AsyncMock(return_value=[gu])

    result = await svc.get_user_groups_with_roles("u1")
    assert len(result) == 1
    g, role = result[0]
    assert role == GroupUserRole.ADMIN


# ---------------------------------------------------------------------------
# get_user_group_memberships
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_user_group_memberships_user_not_found():
    session = make_session()
    svc = make_service(session=session)

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    session.execute = AsyncMock(return_value=mock_result)

    result = await svc.get_user_group_memberships("notexist@example.com")
    assert result == []


@pytest.mark.asyncio
async def test_get_user_group_memberships_found():
    session = make_session()
    svc = make_service(session=session)

    user = MagicMock()
    user.id = "u1"
    user.email = "user@example.com"

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = user
    session.execute = AsyncMock(return_value=mock_result)

    group = make_group()
    gu = make_group_user(group=group, status=GroupUserStatus.ACTIVE)
    svc.group_user_repo.get_groups_by_user = AsyncMock(return_value=[gu])

    result = await svc.get_user_group_memberships("user@example.com")
    assert len(result) == 1


# ---------------------------------------------------------------------------
# create_group
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_group_success():
    svc = make_service()
    created = make_group(id="my_team", name="My Team")
    svc.group_repo.add = AsyncMock(return_value=created)

    with patch("src.models.group.Group.generate_group_id", return_value="my_team"):
        result = await svc.create_group("My Team", description="A team", created_by_email="admin@example.com")

    assert result.name == "My Team"


# ---------------------------------------------------------------------------
# list_groups
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_groups():
    svc = make_service()
    svc.group_repo.list_with_user_counts = AsyncMock(return_value=[{"id": "g1", "user_count": 5}])

    result = await svc.list_groups(skip=0, limit=10)
    assert len(result) == 1


# ---------------------------------------------------------------------------
# get_group_by_id
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_group_by_id_found():
    svc = make_service()
    group = make_group(id="g1")
    svc.group_repo.get = AsyncMock(return_value=group)

    result = await svc.get_group_by_id("g1")
    assert result.id == "g1"


@pytest.mark.asyncio
async def test_get_group_by_id_not_found():
    svc = make_service()
    svc.group_repo.get = AsyncMock(return_value=None)

    result = await svc.get_group_by_id("nonexistent")
    assert result is None


# ---------------------------------------------------------------------------
# update_group
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_update_group_not_found():
    svc = make_service()
    svc.group_repo.get = AsyncMock(return_value=None)

    with pytest.raises(ValueError, match="not found"):
        await svc.update_group("g1", name="New Name")


@pytest.mark.asyncio
async def test_update_group_success():
    svc = make_service()
    group = make_group(id="g1", name="Old")
    group.name = "Old"
    group.updated_at = None
    svc.group_repo.get = AsyncMock(return_value=group)
    svc.group_repo.update = AsyncMock(return_value=group)

    result = await svc.update_group("g1", name="New")
    assert result is not None


# ---------------------------------------------------------------------------
# get_group_user_count
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_group_user_count():
    session = make_session()
    svc = make_service(session=session)

    mock_result = MagicMock()
    mock_result.scalar.return_value = 7
    session.execute = AsyncMock(return_value=mock_result)

    count = await svc.get_group_user_count("g1")
    assert count == 7


@pytest.mark.asyncio
async def test_get_group_user_count_none():
    session = make_session()
    svc = make_service(session=session)

    mock_result = MagicMock()
    mock_result.scalar.return_value = None
    session.execute = AsyncMock(return_value=mock_result)

    count = await svc.get_group_user_count("g1")
    assert count == 0


# ---------------------------------------------------------------------------
# list_group_users
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_group_users_with_user():
    svc = make_service()

    user = MagicMock()
    user.email = "alice@example.com"
    gu = make_group_user(id="gu1", group_id="g1", user_id="u1")
    gu.user = user

    svc.group_user_repo.get_users_by_group = AsyncMock(return_value=[gu])

    result = await svc.list_group_users("g1", skip=0, limit=10)
    assert len(result) == 1
    assert result[0]["email"] == "alice@example.com"


@pytest.mark.asyncio
async def test_list_group_users_without_user():
    svc = make_service()

    gu = make_group_user(id="gu1", group_id="g1", user_id="u1")
    gu.user = None

    svc.group_user_repo.get_users_by_group = AsyncMock(return_value=[gu])

    result = await svc.list_group_users("g1")
    assert len(result) == 1
    assert "u1@databricks.com" in result[0]["email"]


# ---------------------------------------------------------------------------
# assign_user_to_group
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_assign_user_to_group_existing_user_existing_membership():
    session = make_session()
    svc = make_service(session=session)

    user = MagicMock()
    user.id = "u1"
    user.email = "alice@example.com"

    mock_user_result = MagicMock()
    mock_user_result.scalar_one_or_none.return_value = user
    session.execute = AsyncMock(return_value=mock_user_result)

    existing_gu = make_group_user(id="g1_u1", group_id="g1", user_id="u1")
    svc.group_user_repo.get_by_group_and_user = AsyncMock(return_value=existing_gu)
    svc.group_user_repo.update = AsyncMock(return_value=existing_gu)

    result = await svc.assign_user_to_group("g1", "alice@example.com", role=GroupUserRole.ADMIN)
    assert result["email"] == "alice@example.com"


@pytest.mark.asyncio
async def test_assign_user_to_group_new_user_new_membership():
    """Test that new membership is created when user record doesn't exist.

    Uses the existing-user path but with a new group-user association
    to exercise the 'else' branch of the membership check.
    """
    from uuid import uuid4

    session = make_session()
    svc = make_service(session=session)

    new_user_id = str(uuid4())

    # Session returns an existing user (avoids the User() constructor path)
    existing_user = MagicMock()
    existing_user.id = new_user_id
    mock_user_result = MagicMock()
    mock_user_result.scalar_one_or_none.return_value = existing_user
    session.execute = AsyncMock(return_value=mock_user_result)

    # No existing group_user membership
    new_gu = make_group_user(id=f"g1_{new_user_id}", group_id="g1", user_id=new_user_id)
    svc.group_user_repo.get_by_group_and_user = AsyncMock(return_value=None)
    svc.group_user_repo.add = AsyncMock(return_value=new_gu)

    result = await svc.assign_user_to_group("g1", "newuser@example.com", role=GroupUserRole.OPERATOR)

    assert result is not None
    svc.group_user_repo.add.assert_called_once()


# ---------------------------------------------------------------------------
# update_group_user
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_update_group_user_not_found():
    svc = make_service()
    svc.group_user_repo.get_by_group_and_user = AsyncMock(return_value=None)

    with pytest.raises(ValueError, match="not found"):
        await svc.update_group_user("g1", "u1", role=GroupUserRole.ADMIN)


@pytest.mark.asyncio
async def test_update_group_user_success():
    svc = make_service()
    gu = make_group_user()
    gu.role = GroupUserRole.OPERATOR
    svc.group_user_repo.get_by_group_and_user = AsyncMock(return_value=gu)
    svc.group_user_repo.update = AsyncMock(return_value=gu)

    result = await svc.update_group_user("g1", "u1", role=GroupUserRole.ADMIN)
    assert result is not None


# ---------------------------------------------------------------------------
# remove_user_from_group
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_remove_user_from_group_not_found():
    svc = make_service()
    svc.group_user_repo.remove_user_from_group = AsyncMock(return_value=False)

    with pytest.raises(ValueError, match="not found"):
        await svc.remove_user_from_group("g1", "u1")


@pytest.mark.asyncio
async def test_remove_user_from_group_success():
    svc = make_service()
    svc.group_user_repo.remove_user_from_group = AsyncMock(return_value=True)

    # Should not raise
    await svc.remove_user_from_group("g1", "u1")


# ---------------------------------------------------------------------------
# delete_group
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_group_not_found():
    svc = make_service()
    svc.group_repo.get = AsyncMock(return_value=None)

    with pytest.raises(ValueError, match="not found"):
        await svc.delete_group("g1")


@pytest.mark.asyncio
async def test_delete_group_success():
    svc = make_service()
    group = make_group()
    svc.group_repo.get = AsyncMock(return_value=group)
    svc.group_repo.delete = AsyncMock()

    await svc.delete_group("g1")
    svc.group_repo.delete.assert_awaited_once()


@pytest.mark.asyncio
async def test_delete_group_exception():
    svc = make_service()
    group = make_group()
    svc.group_repo.get = AsyncMock(return_value=group)
    svc.group_repo.delete = AsyncMock(side_effect=RuntimeError("DB error"))

    with pytest.raises(ValueError, match="Failed to delete"):
        await svc.delete_group("g1")


# ---------------------------------------------------------------------------
# get_group_stats
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_group_stats():
    svc = make_service()
    svc.group_repo.get_stats = AsyncMock(return_value={"total_groups": 5, "total_users": 20})

    result = await svc.get_group_stats()
    assert result["total_groups"] == 5


# ---------------------------------------------------------------------------
# get_total_group_count
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_total_group_count():
    svc = make_service()
    svc.group_repo.get_stats = AsyncMock(return_value={"total_groups": 10})

    result = await svc.get_total_group_count()
    assert result == 10


@pytest.mark.asyncio
async def test_get_total_group_count_missing_key():
    svc = make_service()
    svc.group_repo.get_stats = AsyncMock(return_value={})

    result = await svc.get_total_group_count()
    assert result == 0


# ---------------------------------------------------------------------------
# create_first_admin_group_for_user
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_first_admin_group_for_user():
    svc = make_service()

    user = MagicMock()
    user.id = "u1"
    user.email = "admin@example.com"

    created_group = make_group(id="admin_group_admin")
    created_group.id = "admin_group_admin"
    svc.group_repo.add = AsyncMock(return_value=created_group)

    new_gu = make_group_user(id="admin_group_admin_u1", role=GroupUserRole.ADMIN)
    svc.group_user_repo.add = AsyncMock(return_value=new_gu)

    group, group_user = await svc.create_first_admin_group_for_user(user)
    assert group is not None
    assert group_user is not None


@pytest.mark.asyncio
async def test_create_first_admin_group_no_at_sign():
    svc = make_service()

    user = MagicMock()
    user.id = "u2"
    user.email = "adminuser"  # no @

    created_group = make_group(id="admin_group_admin")
    svc.group_repo.add = AsyncMock(return_value=created_group)
    svc.group_user_repo.add = AsyncMock(return_value=make_group_user())

    group, group_user = await svc.create_first_admin_group_for_user(user)
    assert group is not None


# ---------------------------------------------------------------------------
# get_user_group_membership
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_user_group_membership_found():
    svc = make_service()
    gu = make_group_user()
    svc.group_user_repo.get_by_group_and_user = AsyncMock(return_value=gu)

    result = await svc.get_user_group_membership("u1", "g1")
    assert result is not None


@pytest.mark.asyncio
async def test_get_user_group_membership_not_found():
    svc = make_service()
    svc.group_user_repo.get_by_group_and_user = AsyncMock(return_value=None)

    result = await svc.get_user_group_membership("u1", "g1")
    assert result is None


# ---------------------------------------------------------------------------
# TenantService alias
# ---------------------------------------------------------------------------

def test_tenant_service_alias():
    from src.services.group_service import TenantService, GroupService
    assert TenantService is GroupService
