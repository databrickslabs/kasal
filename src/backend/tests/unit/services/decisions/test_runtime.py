import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from src.config.settings import settings
from src.services.decisions import runtime
from src.services.decisions.contracts import choices_from_response
from src.services.decisions.policies import question

QUESTIONS = {"q": question("Choose", {"yes": "Yes", "no": "No"})}


def payload(confidence=0.95):
    return {
        "answers": {
            "q": {
                "type": "choice",
                "choice": "yes",
                "confidence": confidence,
                "probabilities": {"yes": 0.95, "no": 0.05},
            }
        }
    }


@pytest.fixture
def gateway(monkeypatch):
    monkeypatch.setattr(settings, "JEV_API_BASE", "https://example.com")
    with (
        patch(
            "src.services.decisions.credentials.decision_credential",
            new_callable=AsyncMock,
        ) as credential,
        patch(
            "src.services.decisions.provider.evaluate", new_callable=AsyncMock
        ) as provider,
        patch("src.services.decisions.telemetry.record_decision") as trace,
    ):
        credential.return_value = "secret"
        provider.return_value = payload()
        yield credential, provider, trace


@pytest.mark.asyncio
async def test_disabled_workspace_never_calls_provider_or_traces(gateway):
    credential, provider, trace = gateway
    credential.return_value = None
    assert await runtime.decide("test", {}, QUESTIONS, group_id="one") is None
    credential.assert_awaited_once_with("one")
    provider.assert_not_awaited()
    trace.assert_not_called()


@pytest.mark.asyncio
async def test_unconfigured_endpoint_is_off_without_reading_credentials(
    gateway, monkeypatch
):
    """No JEV_API_BASE: no credential lookup, no provider call, no trace."""
    credential, provider, trace = gateway
    monkeypatch.setattr(settings, "JEV_API_BASE", "")
    assert await runtime.decide("test", {}, QUESTIONS, group_id="one") is None
    credential.assert_not_awaited()
    provider.assert_not_awaited()
    trace.assert_not_called()


@pytest.mark.asyncio
async def test_no_workspace_does_not_read_credentials(gateway):
    with patch.object(runtime, "workspace_id", return_value=None):
        assert await runtime.decide("test", {}, QUESTIONS) is None
    gateway[0].assert_not_awaited()


@pytest.mark.asyncio
async def test_enabled_calls_jev_and_records_only_safe_metadata(gateway):
    credential, provider, trace = gateway
    result = await runtime.decide(
        "test", {"text": "private"}, QUESTIONS, group_id="one"
    )
    assert result["q"].selected == "yes"
    provider.assert_awaited_once_with("secret", {"text": "private"}, QUESTIONS)
    assert trace.call_args.args[:3] == ("test", "jev-1.13.0", "accepted")
    assert "private" not in str(trace.call_args) and "secret" not in str(
        trace.call_args
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error", [TimeoutError(), ValueError("secret"), RuntimeError("body")]
)
async def test_provider_failures_fall_back_without_leaking(gateway, error, caplog):
    gateway[1].side_effect = error
    assert await runtime.decide("test", {}, QUESTIONS, group_id="one") is None
    assert "secret" not in caplog.text and "body" not in caplog.text


@pytest.mark.asyncio
async def test_uncertain_or_malformed_answers_fall_back(gateway):
    for response in (payload(0.5), {"answers": {}}, {"answers": {"invented": {}}}):
        gateway[1].return_value = response
        assert await runtime.decide("test", {}, QUESTIONS, group_id="one") is None


@pytest.mark.asyncio
async def test_cancel_propagates(gateway):
    gateway[1].side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await runtime.decide("test", {}, QUESTIONS, group_id="one")


@pytest.mark.asyncio
async def test_invalid_state_or_limits_abstain(gateway):
    for state in ({"text": "x" * 20001}, {"object": object()}):
        assert await runtime.decide("test", state, QUESTIONS, group_id="one") is None
    gateway[1].assert_not_awaited()


@pytest.mark.asyncio
async def test_sync_gateway_never_blocks_running_loop(gateway):
    assert runtime.decide_sync("test", {}, QUESTIONS, group_id="one") is None
    gateway[0].assert_not_awaited()


@pytest.mark.parametrize(
    "field,value",
    [
        ("choice", "injected"),
        ("confidence", float("nan")),
        ("confidence", True),
        ("probabilities", {"yes": 1, "no": 1}),
        ("probabilities", {"yes": 0.1, "no": 0.9}),
    ],
)
def test_rejects_untrusted_response_shapes(field, value):
    response = payload()
    response["answers"]["q"][field] = value
    with pytest.raises(ValueError):
        choices_from_response(response, QUESTIONS)
