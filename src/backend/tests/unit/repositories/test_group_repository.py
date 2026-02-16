"""Unit tests for GroupRepository and GroupUserRepository."""

import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime

from src.repositories.group_repository import (
    GroupRepository,
    GroupUserRepository,
    TenantRepository,
    TenantUserRepository,
)
from src.models.group import Group, GroupUser


@pytest.fixture
def mock_session():
    session = AsyncMock()
    session.execute = AsyncMock()
    session.flush = AsyncMock()
    session.rollback = AsyncMock()
    session.add = MagicMock()
    return session


class TestGroupRepository:

    @pytest.fixture
    def repo(self, mock_session):
        return GroupRepository(mock_session)

    @pytest.mark.asyncio
    async def test_get_with_users(self, repo, mock_session):
        group = MagicMock(spec=Group, id="g-1")
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = group
        mock_session.execute.return_value = mock_result

        result = await repo.get_with_users("g-1")

        assert result == group

    @pytest.mark.asyncio
    async def test_get_with_users_not_found(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        mock_session.execute.return_value = mock_result

        result = await repo.get_with_users("missing")

        assert result is None

    @pytest.mark.asyncio
    async def test_list_with_user_counts(self, repo, mock_session):
        group = MagicMock(spec=Group)
        group.id = "g-1"
        group.name = "Group 1"
        group.status = "ACTIVE"
        group.description = "desc"
        group.auto_created = False
        group.created_by_email = "a@b.com"
        group.created_at = datetime(2024, 1, 1)
        group.updated_at = datetime(2024, 1, 1)

        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([(group, 5)]))
        mock_session.execute.return_value = mock_result

        result = await repo.list_with_user_counts()

        assert len(result) == 1
        assert result[0]["user_count"] == 5
        assert result[0]["name"] == "Group 1"

    @pytest.mark.asyncio
    async def test_list_with_user_counts_empty(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([]))
        mock_session.execute.return_value = mock_result

        result = await repo.list_with_user_counts()

        assert result == []

    @pytest.mark.asyncio
    async def test_get_stats(self, repo, mock_session):
        total_result = MagicMock()
        total_result.scalar.return_value = 10
        active_result = MagicMock()
        active_result.scalar.return_value = 7
        users_result = MagicMock()
        users_result.scalar.return_value = 25
        status_result = MagicMock()
        status_result.__iter__ = MagicMock(return_value=iter([("ACTIVE", 7), ("INACTIVE", 3)]))

        mock_session.execute.side_effect = [total_result, active_result, users_result, status_result]

        result = await repo.get_stats()

        assert result["total_groups"] == 10
        assert result["active_groups"] == 7
        assert result["total_users"] == 25
        assert result["groups_by_status"]["ACTIVE"] == 7


class TestGroupUserRepository:

    @pytest.fixture
    def repo(self, mock_session):
        return GroupUserRepository(mock_session)

    @pytest.mark.asyncio
    async def test_get_by_group_and_user(self, repo, mock_session):
        membership = MagicMock(spec=GroupUser)
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = membership
        mock_session.execute.return_value = mock_result

        result = await repo.get_by_group_and_user("g-1", "u-1")

        assert result == membership

    @pytest.mark.asyncio
    async def test_get_by_group_and_user_not_found(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        mock_session.execute.return_value = mock_result

        result = await repo.get_by_group_and_user("g-1", "u-missing")

        assert result is None

    @pytest.mark.asyncio
    async def test_get_users_by_group(self, repo, mock_session):
        users = [MagicMock(spec=GroupUser), MagicMock(spec=GroupUser)]
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = users
        mock_session.execute.return_value = mock_result

        result = await repo.get_users_by_group("g-1")

        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_get_groups_by_user(self, repo, mock_session):
        groups = [MagicMock(spec=GroupUser)]
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = groups
        mock_session.execute.return_value = mock_result

        result = await repo.get_groups_by_user("u-1")

        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_user_emails_by_group(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.scalars.return_value = iter(["a@b.com", "c@d.com"])
        mock_session.execute.return_value = mock_result

        result = await repo.get_user_emails_by_group("g-1")

        assert result == ["a@b.com", "c@d.com"]

    @pytest.mark.asyncio
    async def test_remove_user_from_group(self, repo, mock_session):
        mock_result = MagicMock(rowcount=1)
        mock_session.execute.return_value = mock_result

        result = await repo.remove_user_from_group("g-1", "u-1")

        assert result is True
        mock_session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_remove_user_not_found(self, repo, mock_session):
        mock_result = MagicMock(rowcount=0)
        mock_session.execute.return_value = mock_result

        result = await repo.remove_user_from_group("g-1", "u-missing")

        assert result is False

    @pytest.mark.asyncio
    async def test_update_user_role(self, repo, mock_session):
        updated = MagicMock(spec=GroupUser, role="admin")
        # First call is the update, second is the get_by_group_and_user
        update_result = MagicMock()
        get_result = MagicMock()
        get_result.scalars.return_value.first.return_value = updated
        mock_session.execute.side_effect = [update_result, get_result]

        result = await repo.update_user_role("g-1", "u-1", "admin")

        assert result == updated
        mock_session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_user_groups_with_roles(self, repo, mock_session):
        group_user = MagicMock(role="member", status="active", joined_at=datetime(2024, 1, 1), auto_created=False)
        group = MagicMock(spec=Group)
        group.id = "g-1"
        group.name = "Group 1"

        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([(group_user, group)]))
        mock_session.execute.return_value = mock_result

        result = await repo.get_user_groups_with_roles("u-1")

        assert len(result) == 1
        assert result[0]["group_name"] == "Group 1"
        assert result[0]["role"] == "member"

    @pytest.mark.asyncio
    async def test_get_user_groups_with_roles_empty(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([]))
        mock_session.execute.return_value = mock_result

        result = await repo.get_user_groups_with_roles("u-1")

        assert result == []


class TestLegacyAliases:

    def test_tenant_repository_is_group_repository(self):
        assert TenantRepository is GroupRepository

    def test_tenant_user_repository_is_group_user_repository(self):
        assert TenantUserRepository is GroupUserRepository
