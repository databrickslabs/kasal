"""
Comprehensive unit tests for LLMLogService.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timezone

from src.services.log_service import LLMLogService
from src.utils.user_context import GroupContext


def make_gc(group_id="g1"):
    return GroupContext(
        group_ids=[group_id],
        group_email=f"user@{group_id}.com",
        email_domain=f"{group_id}.com",
    )


def make_log_entry(id=1, endpoint="/llm", model="gpt-4"):
    return MagicMock(id=id, endpoint=endpoint, model=model)


class TestLLMLogServiceCreation:
    def test_create_factory_method_returns_instance(self):
        """create() factory method returns configured LLMLogService."""
        mock_session = MagicMock()
        service = LLMLogService.create(mock_session)
        assert isinstance(service, LLMLogService)
        assert service.repository is not None

    def test_init_sets_repository(self):
        """__init__ stores the repository reference."""
        mock_repo = MagicMock()
        service = LLMLogService(repository=mock_repo)
        assert service.repository is mock_repo


class TestGetLogsPaginated:
    @pytest.mark.asyncio
    async def test_delegates_to_repository(self):
        """get_logs_paginated delegates to repo."""
        mock_repo = AsyncMock()
        logs = [make_log_entry()]
        mock_repo.get_logs_paginated = AsyncMock(return_value=logs)
        service = LLMLogService(repository=mock_repo)

        result = await service.get_logs_paginated(page=1, per_page=5, endpoint="/test")
        mock_repo.get_logs_paginated.assert_called_once_with(1, 5, "/test")
        assert result == logs

    @pytest.mark.asyncio
    async def test_default_parameters(self):
        """get_logs_paginated uses default params when not provided."""
        mock_repo = AsyncMock()
        mock_repo.get_logs_paginated = AsyncMock(return_value=[])
        service = LLMLogService(repository=mock_repo)

        await service.get_logs_paginated()
        mock_repo.get_logs_paginated.assert_called_once_with(0, 10, None)


class TestCountLogs:
    @pytest.mark.asyncio
    async def test_delegates_to_repo(self):
        """count_logs delegates to repository."""
        mock_repo = AsyncMock()
        mock_repo.count_logs = AsyncMock(return_value=42)
        service = LLMLogService(repository=mock_repo)

        count = await service.count_logs(endpoint="/api")
        assert count == 42
        mock_repo.count_logs.assert_called_once_with("/api")

    @pytest.mark.asyncio
    async def test_no_endpoint_filter(self):
        """count_logs passes None when no endpoint given."""
        mock_repo = AsyncMock()
        mock_repo.count_logs = AsyncMock(return_value=0)
        service = LLMLogService(repository=mock_repo)

        await service.count_logs()
        mock_repo.count_logs.assert_called_once_with(None)


class TestGetUniqueEndpoints:
    @pytest.mark.asyncio
    async def test_delegates_to_repo(self):
        """get_unique_endpoints delegates to repository."""
        mock_repo = AsyncMock()
        mock_repo.get_unique_endpoints = AsyncMock(return_value=["/api/chat", "/api/complete"])
        service = LLMLogService(repository=mock_repo)

        result = await service.get_unique_endpoints()
        assert result == ["/api/chat", "/api/complete"]


class TestCreateLog:
    @pytest.mark.asyncio
    async def test_creates_log_without_group(self):
        """create_log sends correct data to repository without group."""
        mock_repo = AsyncMock()
        log = make_log_entry()
        mock_repo.create = AsyncMock(return_value=log)
        service = LLMLogService(repository=mock_repo)

        result = await service.create_log(
            endpoint="/api/chat",
            prompt="Hello",
            response="Hi",
            model="gpt-4",
            status="success",
        )
        assert result == log
        call_args = mock_repo.create.call_args[0][0]
        assert call_args["endpoint"] == "/api/chat"
        assert call_args["model"] == "gpt-4"
        assert call_args["status"] == "success"
        assert "group_id" not in call_args

    @pytest.mark.asyncio
    async def test_creates_log_with_group_context(self):
        """create_log adds group_id when group context provided."""
        mock_repo = AsyncMock()
        mock_repo.create = AsyncMock(return_value=make_log_entry())
        service = LLMLogService(repository=mock_repo)
        gc = make_gc("corp-group")

        await service.create_log(
            endpoint="/api",
            prompt="p",
            response="r",
            model="gpt-4",
            status="success",
            group_context=gc,
        )
        call_args = mock_repo.create.call_args[0][0]
        assert call_args["group_id"] == "corp-group"
        assert "group_email" in call_args

    @pytest.mark.asyncio
    async def test_creates_log_with_context_without_group_id(self):
        """create_log skips group fields when context has no primary_group_id."""
        mock_repo = AsyncMock()
        mock_repo.create = AsyncMock(return_value=make_log_entry())
        service = LLMLogService(repository=mock_repo)
        gc = GroupContext(group_ids=None)  # No group IDs

        await service.create_log(
            endpoint="/api",
            prompt="p",
            response="r",
            model="model",
            status="success",
            group_context=gc,
        )
        call_args = mock_repo.create.call_args[0][0]
        assert "group_id" not in call_args

    @pytest.mark.asyncio
    async def test_includes_optional_fields(self):
        """create_log includes all optional fields when provided."""
        mock_repo = AsyncMock()
        mock_repo.create = AsyncMock(return_value=make_log_entry())
        service = LLMLogService(repository=mock_repo)

        await service.create_log(
            endpoint="/api",
            prompt="p",
            response="r",
            model="gpt-4",
            status="error",
            tokens_used=500,
            duration_ms=1500,
            error_message="rate limit",
            extra_data={"key": "value"},
        )
        call_args = mock_repo.create.call_args[0][0]
        assert call_args["tokens_used"] == 500
        assert call_args["duration_ms"] == 1500
        assert call_args["error_message"] == "rate limit"
        assert call_args["extra_data"] == {"key": "value"}

    @pytest.mark.asyncio
    async def test_default_extra_data_is_empty_dict(self):
        """create_log uses empty dict for extra_data when not provided."""
        mock_repo = AsyncMock()
        mock_repo.create = AsyncMock(return_value=make_log_entry())
        service = LLMLogService(repository=mock_repo)

        await service.create_log(
            endpoint="/api", prompt="p", response="r", model="m", status="success"
        )
        call_args = mock_repo.create.call_args[0][0]
        assert call_args["extra_data"] == {}


class TestGetLogStats:
    @pytest.mark.asyncio
    async def test_returns_stats_with_endpoint_counts(self):
        """get_log_stats returns aggregated stats by endpoint."""
        mock_repo = AsyncMock()
        mock_repo.count_logs = AsyncMock(side_effect=lambda ep=None: 10 if ep is None else 5)
        mock_repo.get_unique_endpoints = AsyncMock(return_value=["/api/a", "/api/b"])
        service = LLMLogService(repository=mock_repo)

        stats = await service.get_log_stats(days=7)
        assert stats["total_logs"] == 10
        assert stats["days_included"] == 7
        assert "/api/a" in stats["endpoints"]
        assert "/api/b" in stats["endpoints"]

    @pytest.mark.asyncio
    async def test_empty_endpoint_list(self):
        """get_log_stats works with no endpoints."""
        mock_repo = AsyncMock()
        mock_repo.count_logs = AsyncMock(return_value=0)
        mock_repo.get_unique_endpoints = AsyncMock(return_value=[])
        service = LLMLogService(repository=mock_repo)

        stats = await service.get_log_stats()
        assert stats["total_logs"] == 0
        assert stats["counts_by_endpoint"] == {}


class TestGroupAwareMethods:
    @pytest.mark.asyncio
    async def test_get_logs_paginated_by_group_no_context(self):
        """Returns empty list when no group context given."""
        mock_repo = AsyncMock()
        service = LLMLogService(repository=mock_repo)
        result = await service.get_logs_paginated_by_group()
        assert result == []
        mock_repo.get_logs_paginated_by_group.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_logs_paginated_by_group_empty_group_ids(self):
        """Returns empty list when group context has no group_ids."""
        mock_repo = AsyncMock()
        service = LLMLogService(repository=mock_repo)
        gc = GroupContext(group_ids=None)
        result = await service.get_logs_paginated_by_group(group_context=gc)
        assert result == []

    @pytest.mark.asyncio
    async def test_get_logs_paginated_by_group_with_context(self):
        """Delegates to repo with group context."""
        mock_repo = AsyncMock()
        logs = [make_log_entry()]
        mock_repo.get_logs_paginated_by_group = AsyncMock(return_value=logs)
        service = LLMLogService(repository=mock_repo)
        gc = make_gc("g1")

        result = await service.get_logs_paginated_by_group(
            page=0, per_page=10, endpoint="/api", group_context=gc
        )
        assert result == logs
        mock_repo.get_logs_paginated_by_group.assert_called_once_with(
            0, 10, "/api", ["g1"]
        )

    @pytest.mark.asyncio
    async def test_count_logs_by_group_no_context(self):
        """Returns 0 when no group context."""
        mock_repo = AsyncMock()
        service = LLMLogService(repository=mock_repo)
        result = await service.count_logs_by_group()
        assert result == 0

    @pytest.mark.asyncio
    async def test_count_logs_by_group_with_context(self):
        """Delegates to repo with group context."""
        mock_repo = AsyncMock()
        mock_repo.count_logs_by_group = AsyncMock(return_value=7)
        service = LLMLogService(repository=mock_repo)
        gc = make_gc()

        count = await service.count_logs_by_group(endpoint="/api", group_context=gc)
        assert count == 7

    @pytest.mark.asyncio
    async def test_get_unique_endpoints_by_group_no_context(self):
        """Returns empty list when no group context."""
        mock_repo = AsyncMock()
        service = LLMLogService(repository=mock_repo)
        result = await service.get_unique_endpoints_by_group()
        assert result == []

    @pytest.mark.asyncio
    async def test_get_unique_endpoints_by_group_with_context(self):
        """Delegates to repo with group context."""
        mock_repo = AsyncMock()
        mock_repo.get_unique_endpoints_by_group = AsyncMock(return_value=["/ep1"])
        service = LLMLogService(repository=mock_repo)
        gc = make_gc()

        result = await service.get_unique_endpoints_by_group(group_context=gc)
        assert result == ["/ep1"]

    @pytest.mark.asyncio
    async def test_get_log_stats_by_group_no_context(self):
        """Returns zeros when no group context."""
        mock_repo = AsyncMock()
        service = LLMLogService(repository=mock_repo)
        stats = await service.get_log_stats_by_group(days=30)
        assert stats["total_logs"] == 0
        assert stats["endpoints"] == []
        assert stats["counts_by_endpoint"] == {}
        assert stats["days_included"] == 30

    @pytest.mark.asyncio
    async def test_get_log_stats_by_group_with_context(self):
        """Returns aggregated stats for group."""
        mock_repo = AsyncMock()
        mock_repo.count_logs_by_group = AsyncMock(
            side_effect=lambda ep, gids: 3 if ep is None else 2
        )
        mock_repo.get_unique_endpoints_by_group = AsyncMock(return_value=["/ep1"])
        service = LLMLogService(repository=mock_repo)
        gc = make_gc()

        stats = await service.get_log_stats_by_group(days=14, group_context=gc)
        assert stats["total_logs"] == 3
        assert "/ep1" in stats["endpoints"]
        assert stats["counts_by_endpoint"]["/ep1"] == 2
        assert stats["days_included"] == 14
