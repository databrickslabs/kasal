"""Throttling (HTTP 429) retry: the wait each response asks for, the bounds, and no sleeping in tests."""

import asyncio
from unittest.mock import patch

import pytest

from src.services.powerbi import http_retry


class _Resp:
    def __init__(self, status_code=200, text="", headers=None):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}


@pytest.fixture
def sleeps():
    with patch("src.services.powerbi.http_retry.time.sleep") as sleep:
        yield sleep


def test_retry_after_header_in_seconds_wins_over_message():
    resp = _Resp(429, "Retry in 48 seconds.", {"Retry-After": "5"})
    assert http_retry.retry_after_seconds(resp) == 5.0


def test_retry_after_http_date_is_a_wait_from_now():
    resp = _Resp(429, "", {"Retry-After": "Wed, 21 Oct 2099 07:28:00 GMT"})
    assert http_retry.retry_after_seconds(resp) > 0


def test_retry_in_message_is_read_when_there_is_no_header():
    assert (
        http_retry.retry_after_seconds(_Resp(429, '{"message":"Retry in 48 seconds."}'))
        == 48.0
    )


def test_no_hint_gives_none():
    assert http_retry.retry_after_seconds(_Resp(429, "busy")) is None


def test_throttled_calls_wait_as_asked_and_are_retried(sleeps):
    responses = [
        _Resp(429, "Retry in 48 seconds"),
        _Resp(429, "", {"Retry-After": "5"}),
        _Resp(200),
    ]
    result = http_retry.send_with_throttle_retry(lambda: responses.pop(0), "test call")
    assert result.status_code == 200
    assert [c.args[0] for c in sleeps.call_args_list] == [49, 6]


def test_without_a_hint_the_wait_is_exponential(sleeps):
    responses = [_Resp(429) for _ in range(4)] + [_Resp(200)]
    http_retry.send_with_throttle_retry(lambda: responses.pop(0), "test call")
    assert [c.args[0] for c in sleeps.call_args_list] == [2, 4, 8, 16]


def test_attempts_are_bounded_and_the_last_response_is_returned(sleeps):
    calls = []

    def send():
        calls.append(1)
        return _Resp(429, "busy")

    result = http_retry.send_with_throttle_retry(send, "test call")
    assert result.status_code == 429
    assert len(calls) == http_retry.MAX_ATTEMPTS
    assert sleeps.call_count == http_retry.MAX_ATTEMPTS - 1


def test_a_long_wait_is_capped_per_retry(sleeps):
    responses = [_Resp(429, "Retry in 900 seconds"), _Resp(200)]
    http_retry.send_with_throttle_retry(lambda: responses.pop(0), "test call")
    assert sleeps.call_args.args[0] == http_retry.MAX_WAIT_PER_RETRY_S


def test_total_waiting_budget_stops_retries(sleeps):
    # Each wait is capped at 120s; the total budget is 240s, so the third retry is not made.
    responses = [_Resp(429, "Retry in 900 seconds") for _ in range(4)] + [_Resp(200)]
    result = http_retry.send_with_throttle_retry(lambda: responses.pop(0), "test call")
    assert result.status_code == 429
    assert sleeps.call_count == 2
    assert len(responses) == 2


def test_non_throttling_errors_are_not_retried(sleeps):
    calls = []
    result = http_retry.send_with_throttle_retry(
        lambda: calls.append(1) or _Resp(500), "test call"
    )
    assert result.status_code == 500
    assert len(calls) == 1
    sleeps.assert_not_called()


def test_async_variant_retries_without_blocking():
    responses = [_Resp(429, "Retry in 3 seconds"), _Resp(200)]

    async def send():
        return responses.pop(0)

    with patch("src.services.powerbi.http_retry.asyncio.sleep") as sleep:

        async def fake_sleep(seconds):
            sleep.seconds = seconds

        sleep.side_effect = fake_sleep
        result = asyncio.run(
            http_retry.send_with_throttle_retry_async(send, "test call")
        )
    assert result.status_code == 200
    assert sleep.seconds == 4.0
