"""A failing tool must not end the run.

Observed 2026-07-25 (execution 86420024): a news site answered with a
302-redirect loop and then a 404. The urllib error escaped ScrapeWebsiteTool,
propagated through LLM.call() into run_agent's retry loop, and after three full
agent turns — each re-running every tool call that had already succeeded — took
down the whole crew:

    WARNING  agent '...' LLM call failed: HTTP Error 302 ...
    WARNING  agent '...' LLM call failed: HTTP Error 302 ...
    WARNING  agent '...' LLM call failed: HTTP Error 404: Not Found
    ERROR    Process 59359 error in crew execution: HTTP Error 404: Not Found

A dead link is information the model can act on, not a crash.
"""

import urllib.error

import pytest

from src.core.llm.transport import LLM


@pytest.fixture
def llm():
    return LLM(model="test-tool-failures", api_key="k")


class TestToolErrorsBecomeResults:
    def test_http_error_is_returned_not_raised(self, llm):
        def scrape(**_kwargs):
            raise urllib.error.HTTPError(
                "https://example.com/x", 404, "Not Found", {}, None
            )

        result = llm._handle_tool_execution("scrape", {}, {"scrape": scrape})

        assert isinstance(result, str)
        assert "404" in result
        assert "scrape" in result

    def test_the_result_tells_the_model_what_to_do_next(self, llm):
        """Otherwise the model repeats the same dead call until the round cap."""

        def failing(**_kwargs):
            raise RuntimeError("boom")

        result = llm._handle_tool_execution("failing", {}, {"failing": failing})

        assert "do not repeat" in result.lower()
        assert "boom" in result

    @pytest.mark.parametrize(
        "exc",
        [
            ValueError("bad argument"),
            TypeError("missing positional"),
            urllib.error.URLError("connection refused"),
            KeyError("missing"),
        ],
    )
    def test_every_ordinary_exception_type_is_contained(self, llm, exc):
        def failing(**_kwargs):
            raise exc

        assert isinstance(llm._handle_tool_execution("t", {}, {"t": failing}), str)

    def test_a_huge_error_is_truncated(self, llm):
        """A tool echoing a whole page must not eat the context window."""

        def failing(**_kwargs):
            raise RuntimeError("x" * 10_000)

        result = llm._handle_tool_execution("t", {}, {"t": failing})
        assert len(result) < 800
        assert result.endswith("already have.") or "…" in result


class TestWhatStillPropagates:
    def test_cancellation_still_stops_the_run(self, llm):
        """CancelledError is a BaseException — stopping an execution must work."""
        import asyncio

        def cancelled(**_kwargs):
            raise asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            llm._handle_tool_execution("t", {}, {"t": cancelled})

    def test_keyboard_interrupt_still_propagates(self, llm):
        def interrupted(**_kwargs):
            raise KeyboardInterrupt()

        with pytest.raises(KeyboardInterrupt):
            llm._handle_tool_execution("t", {}, {"t": interrupted})

    def test_blocked_calls_keep_their_own_wording(self, llm):
        """Denied approval is a policy decision, not a tool malfunction."""
        from src.services.execution.runtime.executor import ToolExecutionBlockedError

        def blocked(**_kwargs):
            raise ToolExecutionBlockedError("approval denied by user")

        result = llm._handle_tool_execution("t", {}, {"t": blocked})
        assert result == "Tool call blocked: approval denied by user"

    def test_unknown_tool_still_returns_none(self, llm):
        assert llm._handle_tool_execution("nope", {}, {}) is None


class TestSafeFetchErrorMessages:
    """urllib says "HTTP Error 404: Not Found" with no URL — useless to a model
    choosing which source to try next."""

    @pytest.fixture(autouse=True)
    def _resolvable_public_host(self, monkeypatch):
        """_safe_fetch resolves the host and refuses private/reserved addresses
        before fetching. Stub resolution with an arbitrary public address so the
        test exercises the error path, not the SSRF guard. Nothing is contacted —
        urlopen is stubbed in each test."""
        from src.services.tools import web_fetch as bundled

        monkeypatch.setattr(
            bundled.socket,
            "getaddrinfo",
            lambda *a, **kw: [(2, 1, 6, "", ("23.45.67.89", 443))],
        )

    def test_http_error_names_the_url(self, monkeypatch):
        from src.services.tools import web_fetch as bundled

        def fake_urlopen(request, timeout=None):
            raise urllib.error.HTTPError(
                "https://news.example.com/article", 404, "Not Found", {}, None
            )

        monkeypatch.setattr(bundled.urllib.request, "urlopen", fake_urlopen)

        with pytest.raises(RuntimeError) as exc:
            bundled._safe_fetch("https://news.example.com/article", {})

        assert "404" in str(exc.value)
        assert "news.example.com/article" in str(exc.value)

    def test_unreachable_host_names_the_url(self, monkeypatch):
        from src.services.tools import web_fetch as bundled

        def fake_urlopen(request, timeout=None):
            raise urllib.error.URLError("connection refused")

        monkeypatch.setattr(bundled.urllib.request, "urlopen", fake_urlopen)

        with pytest.raises(RuntimeError) as exc:
            bundled._safe_fetch("https://news.example.com/article", {})

        assert "news.example.com/article" in str(exc.value)
