"""Tests for the Gmail tool (UC connections proxy, OBO-ONLY).

The proxy resolves the per-user Google credential from the CALLING Databricks
identity. In Kasal only the OBO user token is genuinely per-user — the auth
chain's "pat" is a GROUP-SHARED token (by group_id) or an SPN-derived env
token, both of which would map every caller in a group to one mailbox. So the
tool runs ONLY under OBO and refuses every other auth method.
"""
import base64
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.engines.crewai.tools.custom.gmail_tool import (
    GmailTool,
    GmailToolInput,
    _decode_base64url,
    _extract_body_text,
    _header_map,
)


def _b64(text: str) -> str:
    return base64.urlsafe_b64encode(text.encode()).decode().rstrip("=")


def _message(message_id: str, subject: str, internal_date: int = 0, body: str = "hello") -> dict:
    return {
        "id": message_id,
        "internalDate": str(internal_date),
        "snippet": f"snippet of {subject}",
        "payload": {
            "mimeType": "text/plain",
            "headers": [
                {"name": "From", "value": "alice@example.com"},
                {"name": "To", "value": "me@example.com"},
                {"name": "Subject", "value": subject},
                {"name": "Date", "value": "Fri, 12 Jun 2026 10:00:00 +0000"},
            ],
            "body": {"data": _b64(body)},
        },
    }


def _aiohttp_session(responses):
    """Fake aiohttp.ClientSession yielding queued (status, payload) per GET.

    Payloads may be dicts (JSON-encoded) or raw bytes (e.g. gzip bodies).
    """
    import json as _json

    get_cms = []
    for status, payload in responses:
        body = payload if isinstance(payload, bytes) else _json.dumps(payload).encode()
        response = MagicMock()
        response.status = status
        response.read = AsyncMock(return_value=body)
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=response)
        cm.__aexit__ = AsyncMock(return_value=False)
        get_cms.append(cm)

    session = MagicMock()
    session.get = MagicMock(side_effect=get_cms)
    session_cm = MagicMock()
    session_cm.__aenter__ = AsyncMock(return_value=session)
    session_cm.__aexit__ = AsyncMock(return_value=False)
    return session_cm, session


def _tool(user_token="obo-token", group_id="user_alice_x_com", user_email="alice@x.com") -> GmailTool:
    # Default: personal workspace (group_id == generate_individual_group_id(email)).
    return GmailTool(
        tool_config={"connection_name": "system_ai_agent_gmail", "timeout": 5},
        tool_id=96,
        user_token=user_token,
        group_id=group_id,
        user_email=user_email,
    )


def _auth_context(method="obo", workspace_url="https://ws.example.com"):
    auth = MagicMock()
    auth.auth_method = method
    auth.workspace_url = workspace_url
    auth.get_headers.return_value = {"Authorization": f"Bearer {method}-token"}
    return auth


def _patch_workspace(method="obo", workspace_url="https://ws.example.com"):
    return patch(
        "src.utils.databricks_auth.get_auth_context",
        AsyncMock(return_value=_auth_context(method, workspace_url)),
    )


class TestPersonalWorkspaceEnforcement:
    @pytest.mark.asyncio
    async def test_refuses_in_a_shared_workspace(self):
        # Shared workspace: group_id is NOT the user's personal workspace.
        tool = _tool(group_id="bi-specialist", user_email="alice@x.com")
        result = await tool._run_async(action="search")
        assert "Switch to your Personal Space to use Gmail" in result

    @pytest.mark.asyncio
    async def test_refuses_when_workspace_identity_is_unknown(self):
        tool = _tool(group_id=None, user_email=None)
        result = await tool._run_async(action="read", message_id="m1")
        assert "Switch to your Personal Space to use Gmail" in result

    @pytest.mark.asyncio
    async def test_personal_workspace_check_is_case_insensitive(self):
        tool = _tool(group_id="USER_Alice_X_Com", user_email="Alice@X.com")
        assert tool._is_personal_workspace() is True

    @pytest.mark.asyncio
    async def test_shared_workspace_check_blocks_before_network(self):
        tool = _tool(group_id="bi-specialist", user_email="alice@x.com")
        session_cm, session = _aiohttp_session([])
        with _patch_workspace(), patch("aiohttp.ClientSession", MagicMock(return_value=session_cm)):
            await tool._run_async(action="search")
        session.get.assert_not_called()


class TestAuthEnforcement:
    @pytest.mark.asyncio
    async def test_refuses_without_an_obo_user_token(self):
        # No user token → never even reaches the auth chain.
        tool = _tool(user_token=None)
        with patch("src.utils.databricks_auth.get_auth_context") as mock_auth:
            result = await tool._run_async(action="search")
        assert "on-behalf-of user token" in result
        assert "group-shared" in result
        mock_auth.assert_not_called()

    @pytest.mark.asyncio
    async def test_refuses_pat_even_with_a_token_present(self):
        # A user token was passed, but the auth chain fell back to the
        # group-shared PAT — that maps every caller to one mailbox, so refuse.
        tool = _tool(user_token="stale-token")
        with _patch_workspace(method="pat"):
            result = await tool._run_async(action="search")
        assert "on-behalf-of user token" in result

    @pytest.mark.asyncio
    async def test_refuses_spn_credentials(self):
        tool = _tool(user_token="stale-token")
        with _patch_workspace(method="service_principal"):
            result = await tool._run_async(action="read", message_id="m1")
        assert "on-behalf-of user token" in result

    @pytest.mark.asyncio
    async def test_accepts_obo_only(self):
        tool = _tool()
        with _patch_workspace(method="obo"):
            headers = await tool._get_auth()
        assert headers["Authorization"] == "Bearer obo-token"

    @pytest.mark.asyncio
    async def test_no_workspace_url_yields_error(self):
        tool = _tool()
        with _patch_workspace(method="obo", workspace_url=None):
            result = await tool._run_async(action="search")
        assert "workspace URL" in result


class TestSearch:
    @pytest.mark.asyncio
    async def test_search_lists_recent_messages_with_ids(self):
        session_cm, session = _aiohttp_session([
            (200, {"messages": [{"id": "m1"}, {"id": "m2"}]}),
            (200, _message("m1", "Quarterly numbers")),
            (200, _message("m2", "Lunch?")),
        ])
        tool = _tool()
        with _patch_workspace(), patch("aiohttp.ClientSession", MagicMock(return_value=session_cm)):
            result = await tool._run_async(action="search", max_results=10)

        assert "message_id: m1" in result
        assert "Quarterly numbers" in result
        assert "alice@example.com" in result
        assert "message_id: m2" in result
        # The list call + one detail call per message, all query-string free.
        called_paths = [c.args[0] for c in session.get.call_args_list]
        assert called_paths[0].endswith("/proxy/gmail/v1/users/me/messages")
        assert "?" not in "".join(called_paths)

    @pytest.mark.asyncio
    async def test_search_day_window_stops_at_older_messages(self):
        import time
        now_ms = int(time.time() * 1000)
        old_ms = now_ms - 5 * 86400 * 1000
        session_cm, _ = _aiohttp_session([
            (200, {"messages": [{"id": "m1"}, {"id": "m2"}]}),
            (200, _message("m1", "Fresh", internal_date=now_ms)),
            (200, _message("m2", "Stale", internal_date=old_ms)),
        ])
        tool = _tool()
        with _patch_workspace(), patch("aiohttp.ClientSession", MagicMock(return_value=session_cm)):
            result = await tool._run_async(action="search", max_results=10, days=1)

        assert "Fresh" in result
        assert "Stale" not in result

    @pytest.mark.asyncio
    async def test_search_decompresses_gzip_bodies_without_content_encoding(self):
        # The proxy relays gzip bodies but DROPS the Content-Encoding header,
        # so aiohttp leaves them compressed ("can't decode byte 0x8b").
        import gzip
        import json as _json

        gzipped_list = gzip.compress(_json.dumps({"messages": [{"id": "m1"}]}).encode())
        gzipped_detail = gzip.compress(_json.dumps(_message("m1", "Compressed")).encode())
        session_cm, session = _aiohttp_session([
            (200, gzipped_list),
            (200, gzipped_detail),
        ])
        tool = _tool()
        with _patch_workspace(), patch("aiohttp.ClientSession", MagicMock(return_value=session_cm)):
            result = await tool._run_async(action="search")

        assert "Compressed" in result
        # And every request asks for an uncompressed body up front.
        sent_headers = session.get.call_args.kwargs["headers"]
        assert sent_headers["Accept-Encoding"] == "identity"

    @pytest.mark.asyncio
    async def test_search_empty_inbox(self):
        session_cm, _ = _aiohttp_session([(200, {})])
        tool = _tool()
        with _patch_workspace(), patch("aiohttp.ClientSession", MagicMock(return_value=session_cm)):
            result = await tool._run_async(action="search")
        assert "No emails found" in result

    @pytest.mark.asyncio
    async def test_proxy_error_is_reported(self):
        session_cm, _ = _aiohttp_session([(403, {"error": "denied"})])
        tool = _tool()
        with _patch_workspace(), patch("aiohttp.ClientSession", MagicMock(return_value=session_cm)):
            result = await tool._run_async(action="search")
        assert "HTTP 403" in result

    @pytest.mark.asyncio
    async def test_unauthenticated_error_points_to_the_connection_login(self):
        session_cm, _ = _aiohttp_session([
            (401, {"error_code": "UNAUTHENTICATED", "message": "Please login first"}),
        ])
        tool = _tool()
        with _patch_workspace(), patch("aiohttp.ClientSession", MagicMock(return_value=session_cm)):
            result = await tool._run_async(action="search")
        assert "not authorized yet" in result
        assert "system_ai_agent_gmail" in result


class TestRead:
    @pytest.mark.asyncio
    async def test_read_returns_headers_and_decoded_body(self):
        session_cm, session = _aiohttp_session([
            (200, _message("m1", "Quarterly numbers", body="The numbers are up.")),
        ])
        tool = _tool()
        with _patch_workspace(), patch("aiohttp.ClientSession", MagicMock(return_value=session_cm)):
            result = await tool._run_async(action="read", message_id="m1")

        assert "Subject: Quarterly numbers" in result
        assert "From: alice@example.com" in result
        assert "The numbers are up." in result
        assert session.get.call_args.args[0].endswith("/gmail/v1/users/me/messages/m1")

    @pytest.mark.asyncio
    async def test_read_without_message_id_is_an_instruction(self):
        tool = _tool()
        result = await tool._run_async(action="read")
        assert "needs a message_id" in result

    @pytest.mark.asyncio
    async def test_read_rejects_path_traversal_message_ids(self):
        # The id is LLM-supplied and lands in the proxy URL path: a crafted
        # value must never escape the Gmail proxy and replay the Databricks
        # token against another workspace API.
        tool = _tool()
        session_cm, session = _aiohttp_session([])
        with _patch_workspace(), patch("aiohttp.ClientSession", MagicMock(return_value=session_cm)):
            for bad in ("../../../api/2.0/clusters/list", "m1/../m2", "a b", "m1?x=1", ""):
                result = await tool._run_async(action="read", message_id=bad)
                assert "invalid message_id" in result or "needs a message_id" in result
        session.get.assert_not_called()  # nothing ever reached the proxy


class TestHelpersAndSchema:
    def test_action_normalization(self):
        assert GmailToolInput(action="READ", message_id="x").action == "read"
        assert GmailToolInput(action="bogus").action == "search"
        assert GmailToolInput(action=None).action == "search"

    def test_decode_base64url_tolerates_bad_data(self):
        assert _decode_base64url(_b64("héllo")) == "héllo"
        assert _decode_base64url(None) == ""  # type: ignore[arg-type]

    def test_extract_body_walks_nested_parts(self):
        payload = {
            "mimeType": "multipart/alternative",
            "parts": [
                {"mimeType": "multipart/related", "parts": [
                    {"mimeType": "text/plain", "body": {"data": _b64("nested text")}},
                ]},
            ],
        }
        assert _extract_body_text(payload) == "nested text"
        # html fallback when no text/plain exists
        html_payload = {"mimeType": "text/html", "body": {"data": _b64("<b>hi</b>")}}
        assert _extract_body_text(html_payload) == "<b>hi</b>"
        assert _extract_body_text({}) == ""
        assert _extract_body_text("nope") == ""  # type: ignore[arg-type]

    def test_header_map_lowercases_names(self):
        payload = {"headers": [{"name": "Subject", "value": "S"}, "junk"]}
        assert _header_map(payload) == {"subject": "S"}

    def test_sync_run_outside_event_loop(self):
        tool = _tool(user_token=None)
        result = tool._run(action="search")
        assert "on-behalf-of user token" in result
