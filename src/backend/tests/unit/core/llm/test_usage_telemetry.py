"""Token telemetry must follow the engine's events, not litellm's callbacks.

The regression these guard against is silent: when the engine replaced
crewAI/litellm, `litellm.callbacks` stopped firing for every crew, flow and chat
call, so Databricks usage attribution reported nothing and no error was raised.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.core.events import LLMCallCompletedEvent, LLMCallType
from src.core.llm import usage_telemetry


def _event(usage):
    return LLMCallCompletedEvent(
        model="databricks/databricks-claude-sonnet-4-6",
        response="hello",
        call_type=LLMCallType.LLM_CALL,
        usage=usage,
    )


def _source(user_agent="kasal_agent/0.1.0"):
    src = MagicMock()
    src.extra_headers = {"User-Agent": user_agent} if user_agent else {}
    src.model = "databricks/databricks-claude-sonnet-4-6"
    return src


class TestShouldSend:
    def test_no_usage_is_not_sent(self):
        with patch.dict("os.environ", {"DATABRICKS_HOST": "https://example.com"}):
            assert usage_telemetry._should_send(None) is False
            assert usage_telemetry._should_send({}) is False

    def test_local_deployment_short_circuits(self):
        """No workspace and no user token: nothing to report to."""
        with patch.dict("os.environ", {}, clear=True), patch.object(
            usage_telemetry, "_resolve_user_token", return_value=None
        ):
            assert usage_telemetry._should_send({"total_tokens": 10}) is False

    def test_obo_without_databricks_host_still_sends(self):
        with patch.dict("os.environ", {}, clear=True), patch.object(
            usage_telemetry, "_resolve_user_token", return_value="tok"
        ):
            assert usage_telemetry._should_send({"total_tokens": 10}) is True


class TestProductContext:
    @pytest.mark.parametrize(
        "user_agent,expected",
        [
            ("kasal_agent/0.1.0", "agent"),
            ("kasal_chat/1.2.3", "chat"),
            ("", "llm"),
            ("no-underscore", "llm"),
        ],
    )
    def test_parsed_from_user_agent(self, user_agent, expected):
        assert usage_telemetry._product_context(_source(user_agent)) == expected

    def test_source_without_headers(self):
        src = MagicMock()
        src.extra_headers = None
        assert usage_telemetry._product_context(src) == "llm"


class TestHandler:
    def test_forwards_usage_from_the_event(self):
        usage = {"total_tokens": 42, "prompt_tokens": 30, "completion_tokens": 12}
        sent = {}

        async def _fake_send(**kwargs):
            sent.update(kwargs)

        with patch.dict("os.environ", {"DATABRICKS_HOST": "https://example.com"}), patch(
            "src.utils.telemetry.send_logfood_telemetry", _fake_send
        ), patch.object(usage_telemetry, "_resolve_user_token", return_value="tok"):
            usage_telemetry._on_llm_call_completed(_source(), _event(usage))

        assert sent["usage"] == usage
        assert sent["product_context"] == "agent"
        assert sent["user_token"] == "tok"
        # A DB session inside an LLM worker thread conflicts with the request's
        # transaction, so telemetry must never open one.
        assert sent["skip_db_auth"] is True

    def test_telemetry_failure_never_propagates(self):
        with patch.dict("os.environ", {"DATABRICKS_HOST": "https://example.com"}), patch(
            "src.utils.telemetry.send_logfood_telemetry", side_effect=RuntimeError("boom")
        ), patch.object(usage_telemetry, "_resolve_user_token", return_value="tok"):
            usage_telemetry._on_llm_call_completed(_source(), _event({"total_tokens": 1}))


class TestRegistration:
    def test_registers_on_the_engine_bus_once(self):
        from src.core.events.bus import event_bus

        usage_telemetry._registered = False
        try:
            with patch.object(event_bus, "register_handler") as reg:
                usage_telemetry.register_usage_telemetry()
                usage_telemetry.register_usage_telemetry()
            assert reg.call_count == 1
            assert reg.call_args[0][0] is LLMCallCompletedEvent
        finally:
            usage_telemetry._registered = True

    def test_importing_llm_manager_registers_it(self):
        """The listener must be live in every process that runs an LLM."""
        import src.core.llm_manager  # noqa: F401

        assert usage_telemetry._registered is True
