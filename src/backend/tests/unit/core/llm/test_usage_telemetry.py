"""Token telemetry must follow the engine's events, not litellm's callbacks.

The regression these guard against is silent: when the engine replaced
crewAI/litellm, `litellm.callbacks` stopped firing for every crew, flow and chat
call, so Databricks usage attribution reported nothing and no error was raised.
"""

import asyncio
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.core.events import LLMCallCompletedEvent, LLMCallType
from src.core.llm import usage_telemetry
from src.core.llm.transport.completion import OpenAICompletion


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
        with (
            patch.dict("os.environ", {}, clear=True),
            patch.object(usage_telemetry, "_resolve_user_token", return_value=None),
        ):
            assert usage_telemetry._should_send({"total_tokens": 10}) is False

    def test_obo_without_databricks_host_still_sends(self):
        with (
            patch.dict("os.environ", {}, clear=True),
            patch.object(usage_telemetry, "_resolve_user_token", return_value="tok"),
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
        done = threading.Event()

        async def _fake_send(**kwargs):
            sent.update(kwargs)
            done.set()

        with (
            patch.dict("os.environ", {"DATABRICKS_HOST": "https://example.com"}),
            patch("src.utils.telemetry.send_logfood_telemetry", _fake_send),
            patch.object(usage_telemetry, "_resolve_user_token", return_value="tok"),
        ):
            usage_telemetry._on_llm_call_completed(_source(), _event(usage))
            # The send happens on the telemetry worker, not inside the handler.
            assert done.wait(5)

        assert sent["usage"] == usage
        assert sent["product_context"] == "agent"
        assert sent["user_token"] == "tok"
        # A DB session inside an LLM worker thread conflicts with the request's
        # transaction, so telemetry must never open one.
        assert sent["skip_db_auth"] is True

    def test_telemetry_failure_never_propagates(self):
        with (
            patch.dict("os.environ", {"DATABRICKS_HOST": "https://example.com"}),
            patch(
                "src.utils.telemetry.send_logfood_telemetry",
                side_effect=RuntimeError("boom"),
            ),
            patch.object(usage_telemetry, "_resolve_user_token", return_value="tok"),
        ):
            usage_telemetry._on_llm_call_completed(
                _source(), _event({"total_tokens": 1})
            )


def _completion_with_usage(text="done"):
    return SimpleNamespace(
        choices=[
            SimpleNamespace(
                finish_reason="stop",
                message=SimpleNamespace(content=text, tool_calls=None),
            )
        ],
        usage=SimpleNamespace(prompt_tokens=30, completion_tokens=12, total_tokens=42),
    )


def _llm():
    """`client` is a lazy property with no setter — set the backing attribute."""
    llm = OpenAICompletion(model="test-model")
    object.__setattr__(llm, "_client", MagicMock())
    llm.client.chat.completions.create.return_value = _completion_with_usage()
    return llm


@pytest.fixture
def telemetry_on_the_bus():
    """The real bus with the real handler, as ``llm_manager`` import leaves it."""
    from src.core.events.bus import event_bus

    with (
        event_bus.scoped_handlers(),
        patch.dict("os.environ", {"DATABRICKS_HOST": "https://example.com"}),
        patch.object(usage_telemetry, "_resolve_user_token", return_value="tok"),
    ):
        event_bus.register_handler(
            LLMCallCompletedEvent, usage_telemetry._on_llm_call_completed
        )
        yield


class TestOffTheCallPath:
    """The usage GET to the workspace is telemetry. An LLM call must never wait
    for it, and nothing it does may reach the caller."""

    def test_llm_call_returns_while_the_telemetry_request_is_stuck(
        self, telemetry_on_the_bus
    ):
        started = threading.Event()
        release = threading.Event()

        async def _stuck_send(**kwargs):
            started.set()
            release.wait(3)  # a workspace API that does not answer

        with patch("src.utils.telemetry.send_logfood_telemetry", _stuck_send):
            try:
                began = time.monotonic()
                answer = _llm().call("hi")
                elapsed = time.monotonic() - began
            finally:
                release.set()
            # It was still sent — on the worker, on its own time.
            assert started.wait(5)

        assert answer == "done"
        assert elapsed < 1.0, f"LLM call waited {elapsed:.2f}s for telemetry"

    def test_a_slow_failing_send_neither_delays_nor_reaches_the_caller(
        self, telemetry_on_the_bus
    ):
        calls = []
        second = threading.Event()

        async def _slow_then_boom(**kwargs):
            calls.append(kwargs["usage"]["total_tokens"])
            if len(calls) == 1:
                await asyncio.sleep(1.5)
                raise RuntimeError("workspace API exploded")
            second.set()

        with patch("src.utils.telemetry.send_logfood_telemetry", _slow_then_boom):
            began = time.monotonic()
            first = _llm().call("hi")
            again = _llm().call("hi again")
            elapsed = time.monotonic() - began
            # The failure did not kill the worker: the next send still went out.
            assert second.wait(5)

        assert first == again == "done"
        assert elapsed < 1.0, f"LLM calls waited {elapsed:.2f}s for telemetry"
        assert calls == [42, 42]

    def test_a_full_queue_drops_instead_of_blocking(self):
        release = threading.Event()

        def _stuck():
            release.wait(5)

        try:
            began = time.monotonic()
            accepted = [
                usage_telemetry.submit_telemetry(_stuck)
                for _ in range(usage_telemetry.MAX_PENDING_SENDS + 5)
            ]
            elapsed = time.monotonic() - began
        finally:
            release.set()

        assert accepted.count(False) >= 4
        assert elapsed < 1.0


class TestFlushAtExit:
    """A run subprocess exits right after its last LLM call; the send that
    call queued must get a bounded chance to go out first."""

    def test_flush_delivers_a_queued_send(self):
        delivered = threading.Event()

        def _slowish():
            time.sleep(0.3)
            delivered.set()

        assert usage_telemetry.submit_telemetry(_slowish)
        assert usage_telemetry.flush(timeout=2.0) is True
        assert delivered.is_set()

    def test_flush_gives_up_on_time_when_the_worker_is_stuck(self):
        release = threading.Event()
        try:
            assert usage_telemetry.submit_telemetry(lambda: release.wait(5))
            began = time.monotonic()
            drained = usage_telemetry.flush(timeout=0.3)
            elapsed = time.monotonic() - began
        finally:
            release.set()

        assert drained is False
        assert elapsed < 0.8, f"flush blocked {elapsed:.2f}s past its timeout"

    def test_flush_is_registered_to_run_at_exit(self):
        with patch("atexit.register") as register:
            usage_telemetry._worker = None  # force a (re)start
            usage_telemetry.submit_telemetry(lambda: None)
        assert usage_telemetry.flush in [c.args[0] for c in register.call_args_list]


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
        import src.services.llm.manager  # noqa: F401

        assert usage_telemetry._registered is True
