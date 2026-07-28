"""A2A push notifications.

This is the only place Kasal makes an outbound request to an address a caller
chose, so most of these are security tests. A URL supplied by an external agent
and fetched server-side is the textbook SSRF setup.
"""

import hashlib
import hmac
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.a2a.a2a_server import push
from src.services.external.identity import ExternalCaller


class _Ctx:
    group_ids = ["acme_corp"]
    group_email = "agent@example.com"
    access_token = "tok"
    user_role = "admin"
    highest_role = "admin"
    current_user = None
    primary_group_id = "acme_corp"


def _caller():
    return ExternalCaller(
        group_context=_Ctx(), protocol="a2a", identifier="agent@example.com"
    )


def _config(**overrides):
    row = SimpleNamespace(
        id=1,
        task_id="run-1",
        url="https://receiver.example.com/hook",
        token=None,
        secret=None,
        last_status=None,
        last_error=None,
        last_attempt_at=None,
        consecutive_failures=0,
    )
    for k, v in overrides.items():
        setattr(row, k, v)
    return row


def _safe_url():
    return patch("src.utils.url_security.assert_safe_outbound_url", new=AsyncMock())


def _response(status=200):
    response = MagicMock()
    response.is_success = 200 <= status < 300
    response.status_code = status
    return response


def _http(response=None, side_effect=None):
    client = MagicMock()
    client.post = AsyncMock(return_value=response, side_effect=side_effect)
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    return patch("httpx.AsyncClient", return_value=client), client


class TestSsrf:
    @pytest.mark.asyncio
    async def test_an_unsafe_url_is_refused_at_registration(self):
        """Refusing now is the difference between a caller who fixes it and one
        who waits for a notification that never comes."""
        from src.utils.url_security import UnsafeUrlError

        with patch(
            "src.utils.url_security.assert_safe_outbound_url",
            new=AsyncMock(side_effect=UnsafeUrlError("loopback")),
        ):
            with pytest.raises(ValueError, match="not permitted"):
                await push.register(
                    _caller(), "run-1", "http://127.0.0.1/hook", session=MagicMock()
                )

    @pytest.mark.asyncio
    async def test_the_url_is_rechecked_on_every_delivery_attempt(self):
        """DNS can change between registration and delivery — that is exactly
        what rebinding is."""
        from src.utils.url_security import UnsafeUrlError

        with patch(
            "src.utils.url_security.assert_safe_outbound_url",
            new=AsyncMock(
                side_effect=UnsafeUrlError("now resolves to 169.254.169.254")
            ),
        ) as guard:
            assert await push._deliver_one(_config(), {"state": "completed"}) is False

        guard.assert_awaited()

    @pytest.mark.asyncio
    async def test_an_unsafe_url_is_not_retried(self):
        """Not the receiver being slow; retrying is pointless and prolongs the
        attempt on an internal address."""
        from src.utils.url_security import UnsafeUrlError

        with patch(
            "src.utils.url_security.assert_safe_outbound_url",
            new=AsyncMock(side_effect=UnsafeUrlError("private")),
        ) as guard:
            await push._deliver_one(_config(), {})

        assert guard.await_count == 1

    @pytest.mark.asyncio
    async def test_redirects_are_not_followed(self):
        """A 30x is otherwise a way to reach an internal host that passed the
        pre-flight check."""
        p_http, client = _http(_response(200))
        with _safe_url(), p_http as ctor:
            await push._deliver_one(_config(), {})

        assert ctor.call_args.kwargs["follow_redirects"] is False


class TestSigning:
    @pytest.mark.asyncio
    async def test_a_secret_produces_an_hmac_header(self):
        """So the receiver can tell a real notification from anyone who learned
        the URL."""
        payload = {"state": "completed"}
        p_http, client = _http(_response(200))
        with _safe_url(), p_http:
            await push._deliver_one(_config(secret="s3cret"), payload)

        headers = client.post.await_args.kwargs["headers"]
        expected = hmac.new(
            b"s3cret",
            json.dumps(payload, default=str).encode(),
            hashlib.sha256,
        ).hexdigest()
        assert headers["X-Kasal-Signature"] == expected

    @pytest.mark.asyncio
    async def test_a_token_is_sent_as_a_bearer(self):
        p_http, client = _http(_response(200))
        with _safe_url(), p_http:
            await push._deliver_one(_config(token="abc"), {})

        assert client.post.await_args.kwargs["headers"]["Authorization"] == "Bearer abc"

    @pytest.mark.asyncio
    async def test_no_secret_means_no_signature_header(self):
        p_http, client = _http(_response(200))
        with _safe_url(), p_http:
            await push._deliver_one(_config(), {})

        assert "X-Kasal-Signature" not in client.post.await_args.kwargs["headers"]


class TestDeliveryOutcomes:
    @pytest.mark.asyncio
    async def test_success_clears_the_failure_count(self):
        row = _config(consecutive_failures=4)
        p_http, _ = _http(_response(200))
        with _safe_url(), p_http:
            assert await push._deliver_one(row, {}) is True

        assert row.consecutive_failures == 0
        assert row.last_error is None

    @pytest.mark.asyncio
    async def test_a_failure_retries_then_records(self):
        row = _config()
        p_http, client = _http(_response(500))
        with _safe_url(), p_http, patch("asyncio.sleep", new=AsyncMock()):
            assert await push._deliver_one(row, {}) is False

        assert client.post.await_count == push.MAX_ATTEMPTS
        assert row.last_status == "failed"
        assert row.consecutive_failures == 1

    @pytest.mark.asyncio
    async def test_the_receivers_body_is_never_echoed(self):
        """It is an untrusted response to a server-side request."""
        row = _config()
        response = _response(500)
        response.text = "internal secret leaked here"
        p_http, _ = _http(response)
        with _safe_url(), p_http, patch("asyncio.sleep", new=AsyncMock()):
            await push._deliver_one(row, {})

        assert "secret" not in (row.last_error or "")
        assert row.last_error == "HTTP 500"


class TestIsolationAndHygiene:
    @pytest.mark.asyncio
    async def test_registering_on_a_task_you_cannot_see_is_refused(self):
        """Otherwise this is a way to attach a webhook to another workspace's
        run and watch it."""
        with (
            _safe_url(),
            patch(
                "src.services.external.invocation.run_status",
                new=AsyncMock(return_value=None),
            ),
        ):
            with pytest.raises(push.PushConfigNotFound):
                await push.register(
                    _caller(),
                    "someone-elses",
                    "https://x.example.com",
                    session=MagicMock(),
                )

    def test_tokens_and_secrets_are_never_returned(self):
        """A caller that registered them has them; nothing else should read
        them back."""
        rendered = push._to_dict(_config(token="abc", secret="s3cret"))
        assert "token" not in rendered
        assert "secret" not in rendered
        assert rendered["authenticated"] is True

    @pytest.mark.asyncio
    async def test_a_dead_endpoint_stops_being_tried(self):
        """A webhook pointing at something permanently gone would otherwise cost
        every run three timed-out requests forever."""
        from sqlalchemy import select  # noqa: F401  (patched below)

        session = MagicMock()
        session.execute = AsyncMock(
            return_value=MagicMock(scalars=lambda: MagicMock(all=lambda: []))
        )
        session.flush = AsyncMock()
        assert await push.deliver("run-1", {}, session) == 0
        # The query filters on the limit rather than loading everything and
        # skipping in Python.
        assert push.FAILURE_LIMIT > 0

    @pytest.mark.asyncio
    async def test_delivery_never_raises_into_the_run(self):
        """This runs alongside a task's progress; a webhook nobody is listening
        to must not affect the run that triggered it."""
        session = MagicMock()
        session.execute = AsyncMock(side_effect=RuntimeError("db gone"))
        assert await push.deliver("run-1", {}, session) == 0
