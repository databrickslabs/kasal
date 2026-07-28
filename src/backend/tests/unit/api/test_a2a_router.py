"""The A2A endpoint.

Same shape of tests as the MCP server router: the boundary is where identity is
resolved, so the refusals are what matter.
"""

from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.a2a_router import router
from src.core.exceptions import KasalError
from src.services.external.identity import ExternalAuthError, ExternalCaller


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(router)

    @app.exception_handler(KasalError)
    async def _handle(_request, exc):
        from fastapi.responses import JSONResponse

        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    return TestClient(app, raise_server_exceptions=False)


def _resolved(group_ids=("acme_corp",)):
    class _Ctx:
        def __init__(self):
            self.group_ids = list(group_ids)
            self.group_email = "agent@example.com"
            self.access_token = "tok"

        @property
        def primary_group_id(self):
            return self.group_ids[0]

    return ExternalCaller(
        group_context=_Ctx(), protocol="a2a", identifier="agent@example.com"
    )


def _refuse(detail="No caller identity."):
    return patch(
        "src.api.a2a_router.resolve_caller",
        new=AsyncMock(side_effect=ExternalAuthError(detail)),
    )


def _accept(caller=None):
    return patch(
        "src.api.a2a_router.resolve_caller",
        new=AsyncMock(return_value=caller or _resolved()),
    )


class TestAuthentication:
    def test_the_agent_card_requires_identity(self, client):
        """skills[] is group-scoped, so an anonymous card would have to leak
        every workspace's capabilities or advertise none. For a multi-tenant
        host, identity-scoped discovery is the only correct reading."""
        with _refuse():
            assert client.get("/.well-known/agent.json").status_code == 401

    def test_send_message_refuses_an_unidentified_caller(self, client):
        with _refuse():
            response = client.post(
                "/a2a/v1/message:send",
                json={"message": {"role": "user", "parts": []}, "skillId": "x"},
            )
        assert response.status_code == 401

    def test_get_task_refuses_an_unidentified_caller(self, client):
        with _refuse():
            assert client.get("/a2a/v1/tasks/run-1").status_code == 401

    def test_list_tasks_refuses_an_unidentified_caller(self, client):
        """An unscoped ListTasks is a cross-tenant leak in a single call."""
        with _refuse():
            assert client.get("/a2a/v1/tasks").status_code == 401

    def test_a_caller_without_a_token_is_told_to_authenticate(self, client):
        """OBO: mid-call, require_obo_token() raises and the surface answers 401
        rather than starting a run that dies inside an agent."""
        with (
            _accept(),
            patch(
                "src.api.a2a_router.a2a_tasks.send_message",
                new=AsyncMock(side_effect=ExternalAuthError("present a token")),
            ),
        ):
            response = client.post(
                "/a2a/v1/message:send",
                json={"message": {"role": "user", "parts": []}, "skillId": "x"},
            )
        assert response.status_code == 401


class TestCard:
    def test_serves_a_card_for_a_resolved_caller(self, client):
        from src.schemas.a2a import AgentCapabilities, AgentCard

        fake = AgentCard(
            protocolVersion="1.0",
            name="Kasal",
            description="d",
            version="1.0.0",
            capabilities=AgentCapabilities(),
        )
        with (
            _accept(),
            patch(
                "src.api.a2a_router.a2a_card.build_card",
                new=AsyncMock(return_value=fake),
            ),
        ):
            response = client.get("/.well-known/agent.json")

        assert response.status_code == 200
        assert response.json()["protocolVersion"] == "1.0"

    def test_the_card_is_served_at_the_well_known_path(self):
        """Callers discover an A2A agent by convention, not configuration."""
        paths = {r.path for r in router.routes}
        assert "/.well-known/agent.json" in paths


class TestTaskOperations:
    def test_unknown_skill_is_a_404(self, client):
        from src.services.a2a.tasks import UnknownSkillError

        with (
            _accept(),
            patch(
                "src.api.a2a_router.a2a_tasks.send_message",
                new=AsyncMock(side_effect=UnknownSkillError("no such skill")),
            ),
        ):
            response = client.post(
                "/a2a/v1/message:send",
                json={"message": {"role": "user", "parts": []}, "skillId": "nope"},
            )
        assert response.status_code == 404

    def test_a_task_the_caller_may_not_see_is_a_404(self, client):
        """Same status for "does not exist" and "not yours" — task ids must not
        become an oracle for other workspaces."""
        from src.services.a2a.tasks import UnknownTaskError

        with (
            _accept(),
            patch(
                "src.api.a2a_router.a2a_tasks.get_task",
                new=AsyncMock(side_effect=UnknownTaskError("no task")),
            ),
        ):
            assert client.get("/a2a/v1/tasks/someone-elses").status_code == 404

    def test_the_endpoint_is_versioned(self):
        """External clients pin behaviour."""
        task_paths = [r.path for r in router.routes if "tasks" in r.path]
        assert all(p.startswith("/a2a/v1/") for p in task_paths)
