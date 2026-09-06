"""The generation and stats routes are not an alternate, unowned door into
the event store (audit F04)."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.sse_router import router
from src.dependencies.providers import get_group_context, get_smart_db_session
from src.core.sse_manager import SSEEvent, sse_manager


@pytest.fixture
def client():
    from tests.unit.api.conftest import register_exception_handlers

    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)
    app.dependency_overrides[get_group_context] = lambda: SimpleNamespace(
        group_ids=["ws-a"], primary_group_id="ws-a", group_email="a@example.com"
    )
    app.dependency_overrides[get_smart_db_session] = lambda: None
    return TestClient(app)


@pytest.fixture(autouse=True)
def _clean_owners():
    sse_manager._job_owner.clear()
    yield
    sse_manager._job_owner.clear()


class TestGenerationRoutesAreOwned:
    def test_a_foreign_generations_result_is_not_found(self, client):
        sse_manager.register_job_owner("gen-b", "ws-b")
        assert client.get("/sse/generations/gen-b/result").status_code == 404

    def test_an_unknown_id_is_not_found(self, client):
        assert client.get("/sse/generations/nope/result").status_code == 404

    def test_the_foreign_stream_is_refused_before_it_streams(self, client):
        sse_manager.register_job_owner("gen-b", "ws-b")
        assert client.get("/sse/generations/gen-b/stream").status_code == 404

    @pytest.mark.asyncio
    async def test_the_own_generation_answers(self, client):
        sse_manager.register_job_owner("gen-a", "ws-a")
        assert client.get("/sse/generations/gen-a/result").json()["status"] == "pending"
        await sse_manager.broadcast_to_job(
            "gen-a",
            SSEEvent(data={"execution_id": "run-1"}, event="generation_complete"),
        )
        body = client.get("/sse/generations/gen-a/result").json()
        assert body["status"] == "completed" and body["execution_id"] == "run-1"


class TestStatsRequireASystemAdmin:
    def _as(self, system_admin: bool):
        user = SimpleNamespace(
            id="u", email="u@example.com", is_system_admin=system_admin
        )
        return patch(
            "src.dependencies.admin_auth.require_authenticated_user",
            new=AsyncMock(return_value=user),
        )

    def test_a_workspace_member_is_refused(self, client):
        with self._as(False):
            assert client.get("/sse/stats").status_code == 403

    def test_a_system_admin_is_not(self, client):
        with self._as(True):
            assert client.get("/sse/stats").status_code == 200


class TestTheStreamAllLogLine:
    def test_tokens_and_cookies_never_reach_the_log(self):
        from src.api.sse_router import _loggable_headers

        out = _loggable_headers(
            {
                "Authorization": "Bearer secret",
                "Cookie": "session=abc",
                "X-Forwarded-Access-Token": "dapi-secret",
                "User-Agent": "Mozilla",
                "Last-Event-ID": "42",
            }
        )
        assert out == {"User-Agent": "Mozilla", "Last-Event-ID": "42"}
        assert "secret" not in str(out)


class TestTheExecutionStreamNeedsAnOwner:
    """R2-01. The per-job stream denied only a POSITIVE foreign match in
    execution history, so an id with no row — a generation id handed to this
    route, a job not yet persisted — streamed to anyone."""

    async def _stream(self, job_id, row):
        from types import SimpleNamespace
        from unittest.mock import MagicMock, patch

        from src.api.sse_router import stream_execution_updates

        req = MagicMock()
        req.headers.get = lambda key, default=None: None
        ctx = SimpleNamespace(group_ids=["ws-a"], primary_group_id="ws-a")
        repo = MagicMock()
        repo.get_execution_by_job_id = AsyncMock(return_value=row)
        with (
            patch("src.api.sse_router.ExecutionHistoryRepository", return_value=repo),
            patch("src.api.sse_router.event_stream_generator", return_value=iter([])),
        ):
            return await stream_execution_updates(
                request=req, job_id=job_id, group_context=ctx, session=None
            )

    @pytest.mark.asyncio
    async def test_a_foreign_generation_id_is_refused(self):
        from src.core.exceptions import NotFoundError

        sse_manager.register_job_owner("gen-b", "ws-b")
        with pytest.raises(NotFoundError):
            await self._stream("gen-b", row=None)

    @pytest.mark.asyncio
    async def test_an_unknown_id_is_refused(self):
        from src.core.exceptions import NotFoundError

        with pytest.raises(NotFoundError):
            await self._stream("nobody-knows", row=None)

    @pytest.mark.asyncio
    async def test_an_own_pending_job_streams(self):
        sse_manager.register_job_owner("job-pending", "ws-a")
        assert await self._stream("job-pending", row=None) is not None

    @pytest.mark.asyncio
    async def test_the_persisted_row_decides_when_there_is_one(self):
        from types import SimpleNamespace

        from src.core.exceptions import NotFoundError

        assert (
            await self._stream("job-a", row=SimpleNamespace(group_id="ws-a"))
            is not None
        )
        with pytest.raises(NotFoundError):
            await self._stream("job-b", row=SimpleNamespace(group_id="ws-b"))
