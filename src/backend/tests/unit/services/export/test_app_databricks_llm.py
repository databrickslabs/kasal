"""The exported app's Databricks endpoint policy, executed rather than grepped.

``agent_server/databricks_llm.py`` is the standalone stand-in for Kasal's
``DatabricksRetryLLM``, which cannot be vendored (it reaches into LLMManager,
UserContext and databricks_auth). Because it is a *reimplementation*, string
assertions in the exporter tests are not enough — these tests write the rendered
bundle to disk, import the handler on top of the vendored runtime, and exercise
the behaviours the original exists to provide.

The template dir alone is not importable here: ``kasal_runtime/`` is produced at
export time, so the bundle has to be rendered first.
"""

import importlib

import pytest


@pytest.fixture
def db_llm(app_bundle):
    """``agent_server.databricks_llm``, imported from a rendered bundle."""
    return importlib.import_module("agent_server.databricks_llm")


class TestMessageSanitization:
    """Canonical: DatabricksRetryLLM._sanitize_messages_for_databricks."""

    def test_tool_call_only_assistant_message_gets_placeholder_content(self, db_llm):
        """Databricks-served Claude rejects an assistant turn with empty content
        that carries tool_calls — which a tool loop produces constantly."""
        messages = [
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": None, "tool_calls": [{"id": "1"}]},
            {"role": "tool", "content": "result", "tool_call_id": "1"},
            {"role": "user", "content": "go on"},
        ]
        db_llm.sanitize_messages_for_databricks(messages)
        assert messages[1]["content"] == db_llm.TOOL_CALL_PLACEHOLDER
        assert messages[1]["tool_calls"] == [{"id": "1"}]

    def test_empty_assistant_message_without_tool_calls_is_dropped(self, db_llm):
        messages = [
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": "   "},
            {"role": "user", "content": "still there"},
        ]
        db_llm.sanitize_messages_for_databricks(messages)
        assert [m["role"] for m in messages] == ["user", "user"]

    def test_conversation_ending_on_assistant_gets_a_user_turn(self, db_llm):
        """Claude on Databricks does not support assistant prefill."""
        messages = [
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": "answer"},
        ]
        db_llm.sanitize_messages_for_databricks(messages)
        assert messages[-1]["role"] == "user"

    def test_cache_breakpoint_is_stripped(self, db_llm):
        """Non-Claude endpoints 400 with 'unknown field "cache_breakpoint"'."""
        messages = [{"role": "user", "content": "hi", "cache_breakpoint": True}]
        db_llm.sanitize_messages_for_databricks(messages)
        assert "cache_breakpoint" not in messages[0]
        assert messages[0]["content"] == "hi"

    def test_it_mutates_in_place(self, db_llm):
        """Callers hold a reference to the same list; returning a new one would
        silently send the unsanitized original."""
        messages = [{"role": "assistant", "content": "x"}]
        returned = db_llm.sanitize_messages_for_databricks(messages)
        assert returned is messages


class TestLlamaAlternation:
    def test_llama_gets_a_trailing_user_turn(self, db_llm):
        llm = db_llm.DatabricksLLM(model="databricks/databricks-llama-4-maverick")
        fixed = llm._fix_llama_alternation(
            [{"role": "user", "content": "a"}, {"role": "assistant", "content": "b"}]
        )
        assert fixed[-1]["role"] == "user"

    def test_non_llama_models_are_untouched(self, db_llm):
        """Other families have their own rules; adding a turn changes the prompt
        for nothing."""
        llm = db_llm.DatabricksLLM(model="databricks/databricks-claude-sonnet-4-6")
        messages = [
            {"role": "user", "content": "a"},
            {"role": "assistant", "content": "b"},
        ]
        assert llm._fix_llama_alternation(messages) == messages


class TestRetry:
    def test_transient_failure_is_retried_then_succeeds(self, db_llm, monkeypatch):
        slept = []
        monkeypatch.setattr(db_llm.time, "sleep", slept.append)
        calls = []

        def flaky(self, conversation, **kwargs):
            calls.append(conversation)
            if len(calls) < 3:
                raise RuntimeError("503 Service Unavailable")
            return "ok"

        monkeypatch.setattr(db_llm.LLM, "call", flaky)
        llm = db_llm.DatabricksLLM(model="databricks/x")
        assert llm.call([{"role": "user", "content": "hi"}]) == "ok"
        assert len(calls) == 3
        assert slept == [1.0, 2.0]  # INITIAL_BACKOFF * 2**attempt

    def test_rate_limits_back_off_far_longer(self, db_llm, monkeypatch):
        """Databricks quota windows are ~60s; a 1s retry just burns the quota."""
        slept = []
        monkeypatch.setattr(db_llm.time, "sleep", slept.append)
        calls = []

        def flaky(self, conversation, **kwargs):
            calls.append(1)
            if len(calls) < 2:
                raise RuntimeError("429 rate limit exceeded")
            return "ok"

        monkeypatch.setattr(db_llm.LLM, "call", flaky)
        llm = db_llm.DatabricksLLM(model="databricks/x")
        assert llm.call([{"role": "user", "content": "hi"}]) == "ok"
        assert slept == [30.0]

    def test_a_non_retryable_error_is_raised_immediately(self, db_llm, monkeypatch):
        monkeypatch.setattr(db_llm.time, "sleep", lambda _: pytest.fail("slept"))

        def boom(self, conversation, **kwargs):
            raise RuntimeError("400 bad request: your prompt is malformed")

        monkeypatch.setattr(db_llm.LLM, "call", boom)
        llm = db_llm.DatabricksLLM(model="databricks/x")
        with pytest.raises(RuntimeError, match="malformed"):
            llm.call([{"role": "user", "content": "hi"}])

    def test_context_length_errors_are_never_retried(self, db_llm, monkeypatch):
        """They are deterministic — retrying only burns the run's wall clock."""
        monkeypatch.setattr(db_llm.time, "sleep", lambda _: pytest.fail("slept"))

        def boom(self, conversation, **kwargs):
            raise db_llm.LLMContextLengthExceededError("too long")

        monkeypatch.setattr(db_llm.LLM, "call", boom)
        llm = db_llm.DatabricksLLM(model="databricks/x")
        with pytest.raises(db_llm.LLMContextLengthExceededError):
            llm.call([{"role": "user", "content": "hi"}])

    def test_retries_are_bounded(self, db_llm, monkeypatch):
        monkeypatch.setattr(db_llm.time, "sleep", lambda _: None)
        calls = []

        def always_fails(self, conversation, **kwargs):
            calls.append(1)
            raise RuntimeError("503 service unavailable")

        monkeypatch.setattr(db_llm.LLM, "call", always_fails)
        llm = db_llm.DatabricksLLM(model="databricks/x")
        with pytest.raises(RuntimeError):
            llm.call([{"role": "user", "content": "hi"}])
        assert len(calls) == db_llm.DatabricksLLM.MAX_RETRIES


class TestTokenRefresh:
    def test_auth_failure_refreshes_the_token_and_retries(self, db_llm, monkeypatch):
        """Databricks Apps rotate credentials; a long run outlives its token."""
        calls = []

        def flaky(self, conversation, **kwargs):
            calls.append(self.api_key)
            if self.api_key == "stale":
                raise RuntimeError("401 invalid access token")
            return "ok"

        monkeypatch.setattr(db_llm.LLM, "call", flaky)
        llm = db_llm.DatabricksLLM(
            model="databricks/x", api_key="stale", token_provider=lambda: "fresh"
        )
        assert llm.call([{"role": "user", "content": "hi"}]) == "ok"
        assert calls == ["stale", "fresh"]

    def test_the_stale_openai_client_is_dropped_on_refresh(self, db_llm):
        """The cached client holds the old bearer token; keeping it would make
        the refresh a no-op."""
        llm = db_llm.DatabricksLLM(
            model="databricks/x", api_key="stale", token_provider=lambda: "fresh"
        )
        llm._client = object()
        assert llm._refresh_token() is True
        assert llm._client is None

    def test_no_token_provider_means_no_refresh(self, db_llm):
        llm = db_llm.DatabricksLLM(model="databricks/x", api_key="stale")
        assert llm._refresh_token() is False

    def test_an_unchanged_token_is_not_treated_as_a_refresh(self, db_llm):
        """Otherwise a broken provider yields an infinite retry loop."""
        llm = db_llm.DatabricksLLM(
            model="databricks/x", api_key="same", token_provider=lambda: "same"
        )
        assert llm._refresh_token() is False


class TestStopWords:
    @pytest.mark.parametrize(
        "model,supported",
        [
            ("databricks/databricks-gpt-5", False),
            ("databricks/gpt5-custom-endpoint", False),  # no hyphen — base misses it
            ("databricks/databricks-claude-sonnet-4-6", True),
        ],
    )
    def test_stop_word_support(self, db_llm, model, supported):
        assert db_llm.DatabricksLLM(model=model).supports_stop_words() is supported


class TestCallContract:
    def test_tools_reach_the_transport(self, db_llm, monkeypatch):
        """runtime/executor.call_llm forwards only the kwargs the signature
        declares — this is the test that a **kwargs signature would fail."""
        seen = {}

        def capture(self, conversation, **kwargs):
            seen.update(kwargs)
            return "ok"

        monkeypatch.setattr(db_llm.LLM, "call", capture)
        llm = db_llm.DatabricksLLM(model="databricks/x")
        llm.call(
            [{"role": "user", "content": "hi"}],
            tools=[{"name": "search"}],
            available_functions={"search": lambda: None},
        )
        assert seen["tools"] == [{"name": "search"}]
        assert "search" in seen["available_functions"]

    def test_call_llm_would_pass_tools_to_this_signature(self, db_llm):
        """Assert against the REAL inspection the runtime performs, not a proxy."""
        import inspect

        params = set(
            inspect.signature(
                db_llm.DatabricksLLM(model="databricks/x").call
            ).parameters
        )
        assert {"tools", "available_functions", "from_task", "from_agent"} <= params
