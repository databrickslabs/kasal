"""Regression tests for the documentation seeder auth fail-fast (PERF-001).

Before the fix, an auth/permission failure (403/invalid token) checking the
Databricks document index was treated as "index is empty", so EVERY backend
restart re-ran the full (futile) seeding pipeline, including a 2-minute
index-readiness wait against a known-failing endpoint, and then logged
"seeded successfully" with 0 chunks written.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.seeds.documentation import (
    DocumentationIndexUnavailableError,
    seed,
)
from src.services.databricks_index_service import (
    DatabricksIndexService,
    is_auth_or_permission_error,
)


class TestAuthErrorClassifier:
    @pytest.mark.parametrize(
        "message",
        [
            "Query failed (403): Invalid access token",
            "401 Unauthorized",
            "permission denied for index",
            "Authentication failed",
            "Forbidden",
        ],
    )
    def test_auth_errors_detected(self, message):
        assert is_auth_or_permission_error(message) is True

    @pytest.mark.parametrize(
        "message",
        ["index does not exist", "not ready yet", "connection timeout", "", None],
    )
    def test_non_auth_errors_ignored(self, message):
        assert is_auth_or_permission_error(message) is False


class TestSeedSkipsOnAuthFailure:
    @pytest.mark.asyncio
    async def test_seed_skips_and_does_not_run_pipeline(self):
        """An auth failure in the existence check must SKIP seeding entirely."""
        with patch(
            "src.seeds.documentation.check_existing_documentation",
            new_callable=AsyncMock,
            side_effect=DocumentationIndexUnavailableError("403 invalid token"),
        ), patch(
            "src.seeds.documentation.seed_documentation_embeddings",
            new_callable=AsyncMock,
        ) as mock_pipeline:
            result = await seed()

        assert result is False
        mock_pipeline.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_seed_still_proceeds_on_generic_check_failure(self):
        """Non-auth check failures keep the old behavior: attempt seeding."""
        with patch(
            "src.seeds.documentation.check_existing_documentation",
            new_callable=AsyncMock,
            side_effect=RuntimeError("transient DB error"),
        ), patch(
            "src.seeds.documentation.seed_documentation_embeddings",
            new_callable=AsyncMock,
            return_value=True,
        ) as mock_pipeline:
            result = await seed()

        assert result is True
        mock_pipeline.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_seed_reports_failure_when_zero_chunks_written(self):
        """'Success' with 0 chunks written must not be reported as success."""
        with patch(
            "src.seeds.documentation.check_existing_documentation",
            new_callable=AsyncMock,
            return_value=(False, 0),
        ), patch(
            "src.seeds.documentation.seed_documentation_embeddings",
            new_callable=AsyncMock,
            return_value=False,
        ):
            result = await seed()

        assert result is False


class TestWaitForIndexReadyFailFast:
    def _service_with_repo(self, repo):
        service = DatabricksIndexService("https://example.com")
        service._get_index_repository = MagicMock(return_value=repo)
        return service

    @pytest.mark.asyncio
    async def test_auth_failure_response_aborts_wait_immediately(self):
        repo = MagicMock()
        repo.get_index = AsyncMock(
            return_value=MagicMock(
                success=False, index=None, message="403 Forbidden: Invalid access token"
            )
        )
        service = self._service_with_repo(repo)

        result = await service.wait_for_index_ready(
            workspace_url="https://example.com",
            index_name="cat.schema.idx",
            endpoint_name="ep",
            max_wait_seconds=120,
            check_interval_seconds=10,
        )

        assert result["ready"] is False
        assert result.get("auth_error") is True
        assert result["attempts"] == 1  # fail-fast: one attempt, not 12
        repo.get_index.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_auth_exception_aborts_wait_immediately(self):
        repo = MagicMock()
        repo.get_index = AsyncMock(side_effect=Exception("401 Unauthorized"))
        service = self._service_with_repo(repo)

        result = await service.wait_for_index_ready(
            workspace_url="https://example.com",
            index_name="cat.schema.idx",
            endpoint_name="ep",
            max_wait_seconds=120,
            check_interval_seconds=10,
        )

        assert result.get("auth_error") is True
        assert result["attempts"] == 1

    @pytest.mark.asyncio
    async def test_non_auth_failure_keeps_polling_until_timeout(self):
        repo = MagicMock()
        repo.get_index = AsyncMock(
            return_value=MagicMock(success=False, index=None, message="transient backend error")
        )
        service = self._service_with_repo(repo)

        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = await service.wait_for_index_ready(
                workspace_url="https://example.com",
                index_name="cat.schema.idx",
                endpoint_name="ep",
                max_wait_seconds=1,
                check_interval_seconds=1,
            )

        assert result["ready"] is False
        assert result.get("auth_error") is not True
        assert repo.get_index.await_count >= 1
