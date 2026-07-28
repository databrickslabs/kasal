"""The outbound A2A client.

Kasal issuing requests to a URL a tenant supplied — the same threat model as
``push.py``, so most of this is about what happens when the far end is hostile
or simply wrong.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.a2a import client
from src.services.external.state import ExternalTaskState


def _safe_url():
    # Patched on the CLIENT, which binds the name at import — patching the utils
    # module would leave the bound reference untouched and hit real DNS.
    return patch("src.services.a2a.client.assert_safe_outbound_url", new=AsyncMock())


def _http(payload=None, status=200, raises=None, text=None):
    response = MagicMock()
    response.is_success = 200 <= status < 300
    response.status_code = status
    response.json = MagicMock(return_value=payload if payload is not None else {})
    response.text = text or ""
    http = MagicMock()
    http.request = AsyncMock(return_value=response, side_effect=raises)
    http.__aenter__ = AsyncMock(return_value=http)
    http.__aexit__ = AsyncMock(return_value=None)
    return patch("httpx.AsyncClient", return_value=http), http


class TestUrlResolution:
    def test_a_base_url_resolves_to_the_well_known_card(self):
        """ "Paste the agent's address" is what an operator will actually do."""
        assert (
            client.card_url_for("https://agent.example.com")
            == "https://agent.example.com/.well-known/agent.json"
        )
        assert (
            client.card_url_for("https://agent.example.com/")
            == "https://agent.example.com/.well-known/agent.json"
        )

    def test_a_card_url_is_left_alone(self):
        url = "https://agent.example.com/.well-known/agent.json"
        assert client.card_url_for(url) == url

    def test_an_empty_url_is_refused_rather_than_guessed_at(self):
        with pytest.raises(client.RemoteAgentError):
            client.card_url_for("")

    def test_the_card_names_where_to_send_messages(self):
        card = {"interfaces": [{"url": "https://agent.example.com/api/v1/a2a/v1"}]}
        assert (
            client.interface_url_of(card, "https://agent.example.com")
            == "https://agent.example.com/api/v1/a2a/v1"
        )

    def test_a_card_without_an_interface_falls_back_to_its_own_host(self):
        """A card served at an agent's root means the agent is at that root."""
        assert (
            client.interface_url_of(
                {}, "https://agent.example.com/.well-known/agent.json"
            )
            == "https://agent.example.com/a2a/v1"
        )


class TestSsrf:
    @pytest.mark.asyncio
    async def test_an_unsafe_url_is_refused(self):
        from src.utils.url_security import UnsafeUrlError

        with patch(
            "src.services.a2a.client.assert_safe_outbound_url",
            new=AsyncMock(side_effect=UnsafeUrlError("loopback")),
        ):
            with pytest.raises(client.RemoteAgentError, match="Refusing to call"):
                await client.fetch_card("https://evil.example.com")

    @pytest.mark.asyncio
    async def test_the_check_runs_on_every_request_not_once_at_configuration(self):
        """DNS can change between saving a remote and calling it."""
        p_http, _ = _http({"name": "R"})
        with _safe_url() as guard, p_http:
            await client.fetch_card("https://a.example.com")
            await client.fetch_card("https://a.example.com")
        assert guard.await_count == 2

    @pytest.mark.asyncio
    async def test_redirects_are_not_followed(self):
        """A 30x is otherwise a way to reach an internal host that passed the
        pre-flight check."""
        p_http, _ = _http({})
        with _safe_url(), p_http as ctor:
            await client.fetch_card("https://a.example.com")
        assert ctor.call_args.kwargs["follow_redirects"] is False


class TestUntrustedResponses:
    @pytest.mark.asyncio
    async def test_an_error_body_is_never_echoed(self):
        """It is an untrusted response to a server-side request, and it would
        land in logs and in LLM context."""
        p_http, _ = _http({}, status=500, text="internal secret")
        with _safe_url(), p_http:
            with pytest.raises(client.RemoteAgentError) as exc:
                await client.fetch_card("https://a.example.com")
        assert "secret" not in str(exc.value)
        assert "500" in str(exc.value)

    @pytest.mark.asyncio
    async def test_a_non_json_answer_is_an_error_not_a_crash(self):
        p_http, http = _http({})
        http.request.return_value.json.side_effect = ValueError("not json")
        with _safe_url(), p_http:
            with pytest.raises(client.RemoteAgentError, match="did not answer JSON"):
                await client.fetch_card("https://a.example.com")

    @pytest.mark.asyncio
    async def test_a_json_array_is_rejected(self):
        """Every A2A payload is an object; a list would sail past a naive
        ``.get`` chain as "empty"."""
        p_http, _ = _http([1, 2, 3])
        with _safe_url(), p_http:
            with pytest.raises(client.RemoteAgentError, match="not an object"):
                await client.fetch_card("https://a.example.com")

    @pytest.mark.asyncio
    async def test_a_connection_failure_is_the_same_kind_of_error(self):
        """To a calling crew, unreachable and malformed are one event: the
        delegation did not happen."""
        p_http, _ = _http(raises=OSError("connection refused"))
        with _safe_url(), p_http:
            with pytest.raises(client.RemoteAgentError, match="Could not reach"):
                await client.fetch_card("https://a.example.com")

    def test_a_malformed_skill_does_not_make_the_agent_unusable(self):
        card = {
            "skills": [
                {"id": "good", "name": "Good", "description": "d"},
                "not a dict",
                {"name": "no-id-but-named"},
                {"description": "nameless"},
            ]
        }
        ids = [s["id"] for s in client.skills_of(card)]
        assert ids == ["good", "no-id-but-named"]


class TestStateTranslation:
    def test_wire_states_become_kasals_own_vocabulary(self):
        assert (
            client.from_wire_state("TASK_STATE_COMPLETED")
            == ExternalTaskState.COMPLETED
        )
        assert (
            client.from_wire_state("TASK_STATE_INPUT_REQUIRED")
            == ExternalTaskState.INPUT_REQUIRED
        )

    def test_an_unknown_state_is_treated_as_still_working(self):
        """A state this version has not heard of is far more likely to be a
        newer spec than a dead task; calling it failed abandons live work."""
        assert client.from_wire_state("TASK_STATE_INVENTED") == (
            ExternalTaskState.WORKING
        )
        assert client.from_wire_state(None) == ExternalTaskState.WORKING


class TestAuth:
    @pytest.mark.asyncio
    async def test_a_configured_key_wins_over_the_callers_token(self):
        """The key was set deliberately for THIS remote; the OBO token is
        ambient, and forwarding it to a third party leaks it for nothing."""
        p_http, http = _http({})
        with _safe_url(), p_http:
            await client.fetch_card(
                "https://a.example.com", api_key="k", token="user-token"
            )
        headers = http.request.await_args.kwargs["headers"]
        assert headers["Authorization"] == "Bearer k"
        assert "X-Forwarded-Access-Token" not in headers

    @pytest.mark.asyncio
    async def test_obo_forwards_the_token_both_ways(self):
        """The second header is what makes a Kasal-to-Kasal hop identify the
        end user rather than the calling workspace."""
        p_http, http = _http({})
        with _safe_url(), p_http:
            await client.fetch_card("https://a.example.com", token="user-token")
        headers = http.request.await_args.kwargs["headers"]
        assert headers["Authorization"] == "Bearer user-token"
        assert headers["X-Forwarded-Access-Token"] == "user-token"


class TestTaskText:
    def test_artifacts_come_before_the_status_message(self):
        """The caller wants the answer; the status message is what a paused or
        failed task has instead of one."""
        text = client.text_of_task(
            {
                "artifacts": [{"parts": [{"kind": "text", "text": "the answer"}]}],
                "status": {"message": {"parts": [{"kind": "text", "text": "a note"}]}},
            }
        )
        assert text.splitlines() == ["the answer", "a note"]

    def test_data_and_url_parts_are_readable(self):
        text = client.text_of_task(
            {
                "artifacts": [
                    {
                        "parts": [
                            {"kind": "data", "data": {"rows": 3}},
                            {"kind": "url", "url": "https://x.example.com/r"},
                        ]
                    }
                ]
            }
        )
        assert '{"rows": 3}' in text
        assert "https://x.example.com/r" in text

    def test_a_task_with_nothing_readable_yields_empty_not_an_error(self):
        assert client.text_of_task({}) == ""
        assert client.text_of_task({"artifacts": [{"parts": [None]}]}) == ""
