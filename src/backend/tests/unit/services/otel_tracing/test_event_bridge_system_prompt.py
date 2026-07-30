"""The system message belongs in the trace.

The bridge captured only the last USER message as ``prompt`` while also
recording ``message_count: 2``. Together those read as "the LLM was sent one
message", which made a lossy LOG look like a missing system prompt in the
REQUEST — the request was always correct.

Everything the dropped message carried was invisible with it: the agent's
backstory, its goal, the security preamble and the date-awareness block. Only
``agent_role`` survived, as a separate attribute, which is why the trace showed
a role and nothing else about who the agent was told to be.
"""

from src.services.otel_tracing.event_bridge import _elide_middle


class TestElideMiddle:
    """Truncation must keep the END, where appended blocks live."""

    def test_short_values_pass_through(self):
        assert _elide_middle("hello", 100) == "hello"

    def test_none_becomes_empty(self):
        assert _elide_middle(None, 100) == ""

    def test_both_ends_survive(self):
        text = "START" + ("x" * 5000) + "END"
        out = _elide_middle(text, 200)
        assert out.startswith("START")
        assert out.endswith("END")
        assert "…" in out

    def test_the_result_respects_the_budget(self):
        out = _elide_middle("y" * 10_000, 400)
        assert len(out) <= 400

    def test_a_trailing_date_block_is_never_cut(self):
        """The exact failure head-truncation would cause: the date is appended
        last, so `s[:max_len]` drops the one thing being looked for."""
        system = "You are X. " + ("backstory " * 2000) + "\nCurrent date: 2026-07-30"
        assert "Current date: 2026-07-30" in _elide_middle(system, 1000)
        assert "Current date" not in system[:1000]  # what the old shape would give


class TestSystemPromptCapture:
    """The attribute the bridge now sets, and what reaches extra_data."""

    def test_the_exporter_picks_up_any_kasal_extra_key(self):
        """No per-field wiring is needed: db_exporter copies the whole
        namespace, so a new attribute lands in extra_data automatically."""
        import inspect

        from src.services.otel_tracing import db_exporter

        source = inspect.getsource(db_exporter)
        assert 'prefix = "kasal.extra."' in source

    def test_the_bridge_sets_a_system_prompt_attribute(self):
        import inspect

        from src.services.otel_tracing import event_bridge

        source = inspect.getsource(event_bridge)
        assert "kasal.extra.system_prompt" in source
        # Captured via the middle-eliding helper, not plain truncation.
        assert "_elide_middle(system_msgs[0]" in source

    def test_the_system_message_carries_what_the_trace_was_missing(self):
        """role + backstory + goal + date all live in that one message."""
        from src.services.execution.runtime import Agent
        from src.services.execution.runtime.executor import build_messages

        agent = Agent(
            role="Swiss News Aggregation Specialist",
            goal="Collect recent Swiss news",
            backstory="A veteran media analyst.",
        )
        system = build_messages(agent, "task")[0]["content"]
        assert "Swiss News Aggregation Specialist" in system
        assert "A veteran media analyst." in system
        assert "Collect recent Swiss news" in system
        assert "Current date:" in system
