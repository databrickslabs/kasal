"""The MCP Streamable HTTP transport — what a real client speaks.

These exist because the transport was verified only by hand-run curl probes.
A real client's first message is ``initialize``; if that answers wrong, nothing
else is ever attempted, and the failure looks like "Kasal doesn't work" rather
than "one method returned the wrong shape".
"""

import json
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.mcp_jsonrpc_router import SUPPORTED_PROTOCOL_VERSIONS, router
from src.core.dependencies import get_smart_db_session
from src.services.external.identity import ExternalAuthError, ExternalCaller


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_smart_db_session] = lambda: None
    return TestClient(app, raise_server_exceptions=False)


def _caller(group_ids=("acme_corp",)):
    class _Ctx:
        def __init__(self):
            self.group_ids = list(group_ids)
            self.group_email = "agent@example.com"
            self.access_token = "tok"

        @property
        def primary_group_id(self):
            return self.group_ids[0]

    return ExternalCaller(
        group_context=_Ctx(), protocol="mcp", identifier="agent@example.com"
    )


def _accept():
    return patch(
        "src.api.mcp_jsonrpc_router._resolve", new=AsyncMock(return_value=_caller())
    )


def _rpc(client, method, params=None, request_id=1, headers=None):
    body = {"jsonrpc": "2.0", "method": method}
    if request_id is not None:
        body["id"] = request_id
    if params is not None:
        body["params"] = params
    return client.post("/mcp", json=body, headers=headers or {})


class TestInitialize:
    def test_handshake_answers_with_capabilities_and_server_info(self, client):
        with _accept():
            response = _rpc(client, "initialize", {"protocolVersion": "2025-06-18"})

        result = response.json()["result"]
        # listChanged is TRUE and backed: publishing or withdrawing a capability
        # changes the tool list, and the GET stream pushes
        # notifications/tools/list_changed. It was false only while there was no
        # channel to push down.
        assert result["capabilities"]["tools"] == {"listChanged": True}
        assert result["serverInfo"]["name"] == "kasal"

    def test_echoes_a_protocol_version_it_supports(self, client):
        """A client proposes a revision; answering with a different one when we
        DO speak theirs makes them decide whether to continue for no reason."""
        with _accept():
            response = _rpc(client, "initialize", {"protocolVersion": "2025-06-18"})
        assert response.json()["result"]["protocolVersion"] == "2025-06-18"

    def test_falls_back_to_our_newest_for_an_unknown_version(self, client):
        with _accept():
            response = _rpc(client, "initialize", {"protocolVersion": "1999-01-01"})
        assert (
            response.json()["result"]["protocolVersion"]
            == SUPPORTED_PROTOCOL_VERSIONS[0]
        )

    def test_advertises_only_tools(self, client):
        """Claiming prompts or resources costs every client a round trip to
        discover they are empty."""
        with _accept():
            response = _rpc(client, "initialize", {})
        assert set(response.json()["result"]["capabilities"]) == {"tools"}


class TestNotifications:
    def test_a_notification_gets_202_and_no_body(self, client):
        """Notifications carry no id and MUST NOT be answered. Replying to one
        is a protocol violation some clients treat as fatal."""
        with _accept():
            response = client.post(
                "/mcp",
                json={"jsonrpc": "2.0", "method": "notifications/initialized"},
            )
        assert response.status_code == 202


class TestToolMethods:
    def test_tools_list_returns_the_advertised_set(self, client):
        with (
            _accept(),
            patch(
                "src.services.mcp.mcp_server.server.list_tools",
                new=AsyncMock(return_value=[{"name": "ask_kasal"}]),
            ),
        ):
            response = _rpc(client, "tools/list")
        assert response.json()["result"]["tools"] == [{"name": "ask_kasal"}]

    def test_tools_call_wraps_the_result_in_a_text_content_block(self, client):
        """A structured-only result is invisible in most clients."""
        with (
            _accept(),
            patch(
                "src.services.mcp.mcp_server.server.call_tool",
                new=AsyncMock(return_value={"run_id": "r1", "state": "completed"}),
            ),
        ):
            response = _rpc(
                client,
                "tools/call",
                {"name": "ask_kasal", "arguments": {"question": "hi"}},
            )

        result = response.json()["result"]
        assert result["isError"] is False
        assert json.loads(result["content"][0]["text"])["run_id"] == "r1"

    def test_ping_answers_empty(self, client):
        with _accept():
            assert _rpc(client, "ping").json()["result"] == {}


class TestErrors:
    def test_unknown_method_is_method_not_found(self, client):
        with _accept():
            response = _rpc(client, "resources/list")
        assert response.json()["error"]["code"] == -32601

    def test_unknown_tool_is_method_not_found(self, client):
        from src.services.mcp.mcp_server.tools import UnknownToolError

        with (
            _accept(),
            patch(
                "src.services.mcp.mcp_server.server.call_tool",
                new=AsyncMock(side_effect=UnknownToolError("nope")),
            ),
        ):
            response = _rpc(client, "tools/call", {"name": "nope"})
        assert response.json()["error"]["code"] == -32601

    def test_a_role_refusal_is_invalid_request_not_method_not_found(self, client):
        """The tool EXISTS. Answering method-not-found sends the caller hunting
        for a name that is right there in tools/list."""
        from src.services.external.permissions import ExternalPermissionError

        with (
            _accept(),
            patch(
                "src.services.mcp.mcp_server.server.call_tool",
                new=AsyncMock(
                    side_effect=ExternalPermissionError(
                        "needs admin", required_roles=["admin"], actual_role="operator"
                    )
                ),
            ),
        ):
            response = _rpc(client, "tools/call", {"name": "create_crew"})
        assert response.json()["error"]["code"] == -32600

    def test_a_tool_call_with_no_name_is_invalid_params(self, client):
        with _accept():
            response = _rpc(client, "tools/call", {})
        assert response.json()["error"]["code"] == -32602

    def test_malformed_json_is_a_parse_error(self, client):
        with _accept():
            response = client.post(
                "/mcp",
                content=b"{not json",
                headers={"Content-Type": "application/json"},
            )
        assert response.status_code == 400
        assert response.json()["error"]["code"] == -32700


class TestAuthentication:
    def test_an_unidentified_caller_gets_401_with_a_jsonrpc_body(self, client):
        """The status code is what an HTTP client acts on; the body is what an
        MCP client shows the user."""
        with patch(
            "src.api.mcp_jsonrpc_router._resolve",
            new=AsyncMock(side_effect=ExternalAuthError("No caller identity.")),
        ):
            response = _rpc(client, "tools/list")

        assert response.status_code == 401
        assert response.json()["error"]["code"] == -32600


class TestTransportShape:
    def test_mounted_at_the_domain_root(self):
        """A client configured with https://host/mcp looks there. Under the API
        prefix it 404s on initialize and never tries anything else."""
        assert "/mcp" in {r.path for r in router.routes}

    def test_get_is_refused_rather_than_held_open(self, client):
        """The transport allows a GET SSE stream. Kasal has nothing to push on
        an idle session, so holding the connection forever would be worse than
        saying so."""
        assert client.get("/mcp").status_code == 405

    def test_an_sse_only_client_gets_sse(self, client):
        with _accept():
            response = _rpc(client, "ping", headers={"Accept": "text/event-stream"})
        assert response.headers["content-type"].startswith("text/event-stream")
        assert response.text.startswith("data: ")

    def test_a_client_accepting_both_gets_json(self, client):
        """Claude Code sends both. JSON is the cheaper answer and every client
        handles it."""
        with _accept():
            response = _rpc(
                client,
                "ping",
                headers={"Accept": "application/json, text/event-stream"},
            )
        assert response.headers["content-type"].startswith("application/json")
