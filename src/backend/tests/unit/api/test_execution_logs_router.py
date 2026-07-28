"""Unit tests for execution_logs_router endpoints.

Tests all three routers (logs_router, runs_router, router) and their endpoints
using direct async function calls with mocked service dependencies.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock
from types import SimpleNamespace
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from src.api.execution_logs_router import (
    get_execution_logs,
    get_run_logs,
    get_execution_logs_main,
    create_execution_log,
    logs_router,
    runs_router,
    router,
)
from src.utils.user_context import GroupContext


def gc(valid=True):
    """Create a GroupContext for testing."""
    if valid:
        return GroupContext(
            group_ids=["g1"],
            group_email="u@x.com",
            email_domain="x.com",
            user_role="admin",
        )
    return GroupContext()


def make_log(content="Test log", ts=None):
    """Create a mock execution log as a dict (compatible with Pydantic models)."""
    return {"content": content, "timestamp": ts or datetime.utcnow().isoformat()}


# ---------------------------------------------------------------------------
# logs_router: GET /logs/executions/{execution_id}
# ---------------------------------------------------------------------------

class TestGetExecutionLogs:
    """Tests for get_execution_logs endpoint on logs_router."""

    @pytest.mark.asyncio
    async def test_returns_logs_successfully(self):
        svc = AsyncMock()
        logs = [make_log("log1"), make_log("log2")]
        svc.get_execution_logs_by_group = AsyncMock(return_value=logs)

        result = await get_execution_logs(
            execution_id="exec-1",
            service=svc,
            group_context=gc(),
            limit=1000,
            offset=0,
        )

        assert len(result) == 2
        assert result[0]["content"] == "log1"
        svc.get_execution_logs_by_group.assert_called_once_with(
            "exec-1", gc(), 1000, 0
        )

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_logs(self):
        svc = AsyncMock()
        svc.get_execution_logs_by_group = AsyncMock(return_value=[])

        result = await get_execution_logs(
            execution_id="exec-2",
            service=svc,
            group_context=gc(),
            limit=500,
            offset=10,
        )

        assert result == []
        svc.get_execution_logs_by_group.assert_called_once_with(
            "exec-2", gc(), 500, 10
        )

    @pytest.mark.asyncio
    async def test_propagates_service_exception(self):
        svc = AsyncMock()
        svc.get_execution_logs_by_group = AsyncMock(
            side_effect=Exception("db down")
        )

        with pytest.raises(Exception, match="db down"):
            await get_execution_logs(
                execution_id="exec-3",
                service=svc,
                group_context=gc(),
                limit=1000,
                offset=0,
            )


# ---------------------------------------------------------------------------
# runs_router: GET /runs/{run_id}/outputs
# ---------------------------------------------------------------------------

class TestGetRunLogs:
    """Tests for get_run_logs endpoint on runs_router."""

    @pytest.mark.asyncio
    async def test_returns_wrapped_response(self):
        svc = AsyncMock()
        logs = [make_log("run-log")]
        svc.get_execution_logs_by_group = AsyncMock(return_value=logs)

        result = await get_run_logs(
            run_id="run-1",
            service=svc,
            group_context=gc(),
            limit=1000,
            offset=0,
        )

        assert hasattr(result, "logs")
        assert len(result.logs) == 1
        assert result.logs[0].content == "run-log"

    @pytest.mark.asyncio
    async def test_empty_logs_wrapped(self):
        svc = AsyncMock()
        svc.get_execution_logs_by_group = AsyncMock(return_value=[])

        result = await get_run_logs(
            run_id="run-2",
            service=svc,
            group_context=gc(),
            limit=100,
            offset=0,
        )

        assert result.logs == []

    @pytest.mark.asyncio
    async def test_custom_pagination(self):
        svc = AsyncMock()
        svc.get_execution_logs_by_group = AsyncMock(return_value=[])

        await get_run_logs(
            run_id="run-3",
            service=svc,
            group_context=gc(),
            limit=500,
            offset=50,
        )

        svc.get_execution_logs_by_group.assert_called_once_with(
            "run-3", gc(), 500, 50
        )


# ---------------------------------------------------------------------------
# router: GET /execution-logs/{execution_id}
# ---------------------------------------------------------------------------

class TestGetExecutionLogsMain:
    """Tests for get_execution_logs_main endpoint on router."""

    @pytest.mark.asyncio
    async def test_returns_logs(self):
        svc = AsyncMock()
        logs = [make_log("main-log")]
        svc.get_execution_logs_by_group = AsyncMock(return_value=logs)

        result = await get_execution_logs_main(
            execution_id="exec-main-1",
            service=svc,
            group_context=gc(),
            limit=1000,
            offset=0,
        )

        assert len(result) == 1
        assert result[0]["content"] == "main-log"

    @pytest.mark.asyncio
    async def test_propagates_service_error(self):
        svc = AsyncMock()
        svc.get_execution_logs_by_group = AsyncMock(
            side_effect=RuntimeError("service error")
        )

        with pytest.raises(RuntimeError, match="service error"):
            await get_execution_logs_main(
                execution_id="exec-main-2",
                service=svc,
                group_context=gc(),
                limit=1000,
                offset=0,
            )


# ---------------------------------------------------------------------------
# router: POST /execution-logs/
# ---------------------------------------------------------------------------

class TestCreateExecutionLog:
    """Tests for create_execution_log endpoint (501 not implemented)."""

    @pytest.mark.asyncio
    async def test_returns_501(self):
        with pytest.raises(HTTPException) as exc_info:
            await create_execution_log(
                log_data={"execution_id": "e1", "content": "test"},
                group_context=gc(),
            )

        assert exc_info.value.status_code == 501
        assert "not implemented" in exc_info.value.detail.lower()


# ---------------------------------------------------------------------------
# Router configuration
# ---------------------------------------------------------------------------

class TestRouterConfiguration:
    """Tests for router prefix and tags."""

    def test_logs_router_config(self):
        assert logs_router.prefix == "/logs"
        assert "logs" in logs_router.tags

    def test_runs_router_config(self):
        assert runs_router.prefix == "/runs"
        assert "runs" in runs_router.tags

    def test_main_router_config(self):
        assert router.prefix == "/execution-logs"
        assert "execution-logs" in router.tags


# ---------------------------------------------------------------------------
# TestClient integration tests for query-parameter validation
# ---------------------------------------------------------------------------

class TestQueryParameterValidation:
    """Tests that FastAPI enforces query parameter constraints."""

    @pytest.fixture
    def client(self):
        from src.api.execution_logs_router import (
            logs_router,
            get_execution_logs_service,
        )
        from src.core.dependencies import get_group_context

        app = FastAPI()
        app.include_router(logs_router)

        mock_svc = AsyncMock()
        mock_svc.get_execution_logs_by_group = AsyncMock(return_value=[])

        app.dependency_overrides[get_execution_logs_service] = lambda: mock_svc
        app.dependency_overrides[get_group_context] = lambda: gc()

        return TestClient(app, raise_server_exceptions=False)

    def test_invalid_limit_too_low(self, client):
        response = client.get("/logs/executions/exec-1?limit=0")
        assert response.status_code == 422

    def test_invalid_limit_too_high(self, client):
        response = client.get("/logs/executions/exec-1?limit=10001")
        assert response.status_code == 422

    def test_invalid_offset_negative(self, client):
        response = client.get("/logs/executions/exec-1?offset=-1")
        assert response.status_code == 422
