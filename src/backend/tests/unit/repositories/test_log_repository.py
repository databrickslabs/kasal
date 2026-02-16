"""Unit tests for LLMLogRepository."""

import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timezone

from src.repositories.log_repository import LLMLogRepository
from src.models.log import LLMLog


@pytest.fixture
def mock_session():
    session = AsyncMock()
    session.execute = AsyncMock()
    session.flush = AsyncMock()
    session.refresh = AsyncMock()
    session.rollback = AsyncMock()
    session.add = MagicMock()
    return session


@pytest.fixture
def repo(mock_session):
    return LLMLogRepository(mock_session)


class TestGetLogsPaginated:

    @pytest.mark.asyncio
    async def test_returns_paginated_logs(self, repo, mock_session):
        logs = [MagicMock(spec=LLMLog), MagicMock(spec=LLMLog)]
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = logs
        mock_session.execute.return_value = mock_result

        result = await repo.get_logs_paginated(page=0, per_page=10)

        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_filters_by_endpoint(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result

        result = await repo.get_logs_paginated(endpoint="chat/completions")

        assert result == []

    @pytest.mark.asyncio
    async def test_all_endpoint_does_not_filter(self, repo, mock_session):
        logs = [MagicMock(spec=LLMLog)]
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = logs
        mock_session.execute.return_value = mock_result

        result = await repo.get_logs_paginated(endpoint="all")

        assert len(result) == 1


class TestCountLogs:

    @pytest.mark.asyncio
    async def test_returns_count(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [MagicMock()] * 5
        mock_session.execute.return_value = mock_result

        result = await repo.count_logs()

        assert result == 5

    @pytest.mark.asyncio
    async def test_returns_zero_for_empty(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result

        result = await repo.count_logs()

        assert result == 0

    @pytest.mark.asyncio
    async def test_filters_by_endpoint(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [MagicMock()] * 3
        mock_session.execute.return_value = mock_result

        result = await repo.count_logs(endpoint="chat/completions")

        assert result == 3


class TestGetUniqueEndpoints:

    @pytest.mark.asyncio
    async def test_returns_unique_endpoints(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.all.return_value = [("chat/completions",), ("embeddings",)]
        mock_session.execute.return_value = mock_result

        result = await repo.get_unique_endpoints()

        assert result == ["chat/completions", "embeddings"]

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_logs(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.all.return_value = []
        mock_session.execute.return_value = mock_result

        result = await repo.get_unique_endpoints()

        assert result == []


class TestCreate:

    @pytest.mark.asyncio
    async def test_creates_log_entry(self, repo, mock_session):
        log_data = {
            "endpoint": "chat/completions",
            "model": "gpt-4",
            "status": 200,
        }
        mock_session.execute.return_value = MagicMock()

        result = await repo.create(log_data)

        mock_session.add.assert_called_once()
        mock_session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_normalizes_timezone_aware_datetimes(self, repo, mock_session):
        log_data = {
            "endpoint": "chat/completions",
            "created_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
        }

        await repo.create(log_data)

        mock_session.add.assert_called_once()


class TestGetLogsPaginatedByTenant:

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_tenant_ids(self, repo, mock_session):
        result = await repo.get_logs_paginated_by_tenant(tenant_ids=None)

        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_for_empty_tenant_ids(self, repo, mock_session):
        result = await repo.get_logs_paginated_by_tenant(tenant_ids=[])

        assert result == []


class TestCountLogsByTenant:

    @pytest.mark.asyncio
    async def test_returns_zero_when_no_tenant_ids(self, repo, mock_session):
        result = await repo.count_logs_by_tenant(tenant_ids=None)

        assert result == 0

    @pytest.mark.asyncio
    async def test_returns_zero_for_empty_tenant_ids(self, repo, mock_session):
        result = await repo.count_logs_by_tenant(tenant_ids=[])

        assert result == 0


class TestGetUniqueEndpointsByTenant:

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_tenant_ids(self, repo, mock_session):
        result = await repo.get_unique_endpoints_by_tenant(tenant_ids=None)

        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_for_empty_tenant_ids(self, repo, mock_session):
        result = await repo.get_unique_endpoints_by_tenant(tenant_ids=[])

        assert result == []


class TestGetLogsPaginatedByGroup:

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_group_ids(self, repo, mock_session):
        result = await repo.get_logs_paginated_by_group(group_ids=None)

        assert result == []

    @pytest.mark.asyncio
    async def test_returns_logs_for_group(self, repo, mock_session):
        logs = [MagicMock(spec=LLMLog)]
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = logs
        mock_session.execute.return_value = mock_result

        result = await repo.get_logs_paginated_by_group(group_ids=["g1"])

        assert len(result) == 1


class TestCountLogsByGroup:

    @pytest.mark.asyncio
    async def test_returns_zero_when_no_group_ids(self, repo, mock_session):
        result = await repo.count_logs_by_group(group_ids=None)

        assert result == 0

    @pytest.mark.asyncio
    async def test_counts_for_group(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [MagicMock()] * 7
        mock_session.execute.return_value = mock_result

        result = await repo.count_logs_by_group(group_ids=["g1"])

        assert result == 7


class TestGetUniqueEndpointsByGroup:

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_group_ids(self, repo, mock_session):
        result = await repo.get_unique_endpoints_by_group(group_ids=None)

        assert result == []

    @pytest.mark.asyncio
    async def test_returns_endpoints_for_group(self, repo, mock_session):
        mock_result = MagicMock()
        mock_result.all.return_value = [("embeddings",)]
        mock_session.execute.return_value = mock_result

        result = await repo.get_unique_endpoints_by_group(group_ids=["g1"])

        assert result == ["embeddings"]
