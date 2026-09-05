"""The generation and stats routes are not an alternate, unowned door into
the event store (audit F04)."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.sse_router import router
from src.core.dependencies import get_group_context, get_smart_db_session
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
