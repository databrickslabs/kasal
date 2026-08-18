"""The MCP server endpoint — the boundary where identity is resolved.

The endpoint is reachable by callers outside the workspace, so the tests that
matter are the refusals. A tool that runs without a resolved caller is not a
broken feature, it is a cross-tenant read.
"""

from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.mcp_server_router import router
from src.core.dependencies import get_smart_db_session
from src.core.exceptions import KasalError
from src.services.external.identity import ExternalAuthError


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(router)

    @app.exception_handler(KasalError)
    async def _handle(_request, exc):  # mirrors main.py's global handler
        from fastapi.responses import JSONResponse

        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    # These routes take a DB session. The minimal test app has no database, and
    # every one of these tests patches the service layer above it, so the
    # session is never used — but without an override FastAPI tries to open a
    # real connection and the endpoint 500s before the test's assertion runs.
    app.dependency_overrides[get_smart_db_session] = lambda: None

    return TestClient(app, raise_server_exceptions=False)


def _resolved(group_ids=("acme_corp",)):
    from src.services.external.identity import ExternalCaller

    class _Ctx:
        def __init__(self):
            self.group_ids = list(group_ids)
            self.group_email = "caller@example.com"
            # The surfaces resolve an effective role now; a context without one
            # is not a caller the app would ever produce.
            self.user_role = "admin"
            self.highest_role = "admin"
            self.current_user = None

        @property
        def primary_group_id(self):
            return self.group_ids[0]

    return ExternalCaller(
        group_context=_Ctx(), protocol="mcp", identifier="caller@example.com"
    )


class TestAuthenticationIsRequired:
    def test_tools_list_refuses_an_unidentified_caller(self, client):
        """No identity header at all — the surface must not answer."""
        with patch(
            "src.api.mcp_server_router.resolve_caller",
            new=AsyncMock(side_effect=ExternalAuthError("No caller identity.")),
        ):
            response = client.get("/mcp/v1/tools")
        assert response.status_code == 401

    def test_tool_call_refuses_an_unidentified_caller(self, client):
        with patch(
            "src.api.mcp_server_router.resolve_caller",
            new=AsyncMock(side_effect=ExternalAuthError("No caller identity.")),
        ):
            response = client.post(
                "/mcp/v1/tools/call",
                json={"name": "ask_kasal", "arguments": {"question": "hi"}},
            )
        assert response.status_code == 401

    def test_caller_belonging_to_no_workspace_is_refused(self, client):
        with patch(
            "src.api.mcp_server_router.resolve_caller",
            new=AsyncMock(
                side_effect=ExternalAuthError("Caller x belongs to no workspace.")
            ),
        ):
            response = client.post(
                "/mcp/v1/tools/call",
                json={"name": "ask_kasal", "arguments": {"question": "hi"}},
            )
        assert response.status_code == 401

    def test_listing_tools_requires_auth_even_though_the_list_is_static(self, client):
        """It is static today and becomes group-scoped with Layer-2. Requiring
        auth now means the endpoint never has to GAIN an auth requirement — the
        kind of change that ships without every client noticing."""
        with patch(
            "src.api.mcp_server_router.resolve_caller",
            new=AsyncMock(side_effect=ExternalAuthError("nope")),
        ):
            assert client.get("/mcp/v1/tools").status_code == 401


class TestResolvedCaller:
    def test_tools_are_listed_for_a_resolved_caller(self, client):
        """The fixed control tools are always present.

        The list is no longer static — Layer-2 appends one tool per published
        crew, so this asserts the fixed set is a SUBSET rather than the whole
        list. Pinning equality would fail the moment a workspace publishes
        anything, which is the normal case, not an edge one.
        """
        with (
            patch(
                "src.api.mcp_server_router.resolve_caller",
                new=AsyncMock(return_value=_resolved()),
            ),
            patch(
                "src.services.mcp.mcp_server.tools.PublicationService"
            ) as publications,
        ):
            publications.return_value.list_capabilities = AsyncMock(return_value=[])
            response = client.get("/mcp/v1/tools")

        assert response.status_code == 200
        body = response.json()
        assert {t["name"] for t in body["tools"]} >= {
            "ask_kasal",
            "create_crew",
            "get_run_status",
            "get_run_result",
            "cancel_run",
            "respond_to_run",
        }
        assert body["protocolVersion"]

    def test_a_published_crew_appears_as_its_own_tool(self, client):
        """Layer-2. A calling agent selects on descriptions, so a tool named
        after the crew is what makes it discoverable at all."""
        from src.schemas.crew_publication import PublishedCapability

        with (
            patch(
                "src.api.mcp_server_router.resolve_caller",
                new=AsyncMock(return_value=_resolved()),
            ),
            patch(
                "src.services.mcp.mcp_server.tools.PublicationService"
            ) as publications,
        ):
            publications.return_value.list_capabilities = AsyncMock(
                return_value=[
                    PublishedCapability(
                        entity_id="c1",
                        name="analyse_powerbi_model",
                        description="Analyse a PowerBI semantic model.",
                    )
                ]
            )
            response = client.get("/mcp/v1/tools")

        names = {t["name"] for t in response.json()["tools"]}
        assert "analyse_powerbi_model" in names

    def test_unknown_tool_is_a_404(self, client):
        """A name that is neither a fixed tool nor a published crew.

        With Layer-2 an unrecognised name is no longer immediately unknown — it
        is first resolved against this caller's publications, because that is
        where per-crew tool names live. Only when that also misses is it a 404,
        and the same 404 covers "another tenant published this name", so the
        surface cannot be used to enumerate other workspaces' crews.
        """
        with (
            patch(
                "src.api.mcp_server_router.resolve_caller",
                new=AsyncMock(return_value=_resolved()),
            ),
            patch(
                "src.services.mcp.mcp_server.tools.PublicationService"
            ) as publications,
        ):
            publications.return_value.resolve_capability = AsyncMock(return_value=None)
            response = client.post(
                "/mcp/v1/tools/call", json={"name": "nope", "arguments": {}}
            )
        assert response.status_code == 404

    def test_bad_arguments_are_the_callers_fault_not_a_500(self, client):
        with patch(
            "src.api.mcp_server_router.resolve_caller",
            new=AsyncMock(return_value=_resolved()),
        ):
            response = client.post(
                "/mcp/v1/tools/call",
                json={"name": "ask_kasal", "arguments": {"wrong_arg": 1}},
            )
        assert response.status_code == 422

    def test_the_resolved_caller_reaches_the_tool(self, client):
        caller = _resolved(["globex_inc"])
        seen = {}

        async def _spy(caller, name, arguments, session=None):
            seen["caller"] = caller
            return {"ok": True}

        with (
            patch(
                "src.api.mcp_server_router.resolve_caller",
                new=AsyncMock(return_value=caller),
            ),
            patch("src.api.mcp_server_router.mcp_server.call_tool", new=_spy),
        ):
            response = client.post(
                "/mcp/v1/tools/call",
                json={"name": "ask_kasal", "arguments": {"question": "hi"}},
            )

        assert response.status_code == 200
        assert seen["caller"].group_ids == ["globex_inc"]


class TestNamingSplit:
    def test_this_router_is_the_server_not_the_client_registry(self):
        """services/mcp/ + api/mcp_router.py is the CLIENT registry; this is the
        SERVER. One word apart, opposite directions."""
        from src.api.mcp_router import router as client_registry

        assert router.prefix == "/mcp/v1"
        assert client_registry.prefix != router.prefix
