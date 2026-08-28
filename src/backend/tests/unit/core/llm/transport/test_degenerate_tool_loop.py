"""Defenses against a model stuck re-issuing the same tool call.

Reproduced from a live run: an agent researched a site, closed the browser,
then called ``browser_close`` twenty more times — every result
"no browser was running" — and the turn ended by returning the model's raw
``<tool_call>`` markup as the "answer". Two defenses, tested here:
``RepeatGuard`` (stop executing identical batches, then drop the tools) and
``strip_tool_markup``/``salvage_last_assistant_text`` (markup never leaves the
transport as an answer).
"""

from src.core.llm.transport.tool_rounds import (
    REPEATED_RESULT,
    RepeatGuard,
    calls_signature,
    salvage_last_assistant_text,
    strip_tool_markup,
    stub_repeated_chat_round,
)

CLOSE = [{"id": "c1", "name": "browser_close", "arguments": '{"dummy": "close"}'}]

# The exact answer shape the broken run persisted.
OBSERVED_MARKUP = (
    "<tool_call>\n<function=browser_close>\n<parameter=dummy>\nclose\n"
    "</parameter>\n</function>\n</tool_call>"
)


class TestRepeatGuard:
    def test_counts_consecutive_identical_batches(self):
        g = RepeatGuard()
        assert g.observe(CLOSE) == 0  # first time: fresh
        assert g.observe(CLOSE) == 1
        assert g.observe(CLOSE) == 2  # third identical -> caller stubs it

    def test_a_different_batch_resets_the_count(self):
        g = RepeatGuard()
        g.observe(CLOSE)
        g.observe(CLOSE)
        other = [{"id": "x", "name": "read_url", "arguments": "{}"}]
        assert g.observe(other) == 0
        assert g.observe(CLOSE) == 0  # not consecutive any more

    def test_same_tool_different_arguments_is_not_a_repeat(self):
        g = RepeatGuard()
        g.observe([{"id": "a", "name": "read_url", "arguments": '{"url": "a"}'}])
        assert (
            g.observe([{"id": "b", "name": "read_url", "arguments": '{"url": "b"}'}])
            == 0
        )

    def test_signature_is_stable_across_id_churn(self):
        # Call ids change every round; the signature must not see them.
        a = [{"id": "c1", "name": "browser_close", "arguments": "{}"}]
        b = [{"id": "c9", "name": "browser_close", "arguments": "{}"}]
        assert calls_signature(a) == calls_signature(b)


class TestStubbedRound:
    def test_stub_answers_every_call_without_executing(self):
        conversation = []
        stub_repeated_chat_round(conversation, "Now I'll build the deck.", CLOSE)
        # Assistant turn present (a tool_calls turn without results is
        # malformed), and every call answered with the stop instruction.
        assert conversation[0]["role"] == "assistant"
        tools = [m for m in conversation if m.get("role") == "tool"]
        assert len(tools) == 1
        assert tools[0]["content"] == REPEATED_RESULT
        assert "final answer" in REPEATED_RESULT


class TestMarkupNeverBecomesTheAnswer:
    def test_pure_markup_strips_to_empty(self):
        assert strip_tool_markup(OBSERVED_MARKUP) == ""

    def test_prose_around_markup_survives(self):
        text = "Here is the deck plan.\n" + OBSERVED_MARKUP
        assert strip_tool_markup(text) == "Here is the deck plan."

    def test_clean_text_is_untouched(self):
        assert (
            strip_tool_markup("A perfectly good answer.") == "A perfectly good answer."
        )

    def test_stray_closing_fragments_are_removed(self):
        # The "green" leak: a result ending in dangling closing tags.
        assert (
            strip_tool_markup("green\n</parameter>\n</function>\n</tool_call>")
            == "green"
        )

    def test_salvages_the_last_real_assistant_sentence(self):
        conversation = [
            {"role": "user", "content": "create a presentation on kasal"},
            {
                "role": "assistant",
                "content": "Now I'll create the presentation based on kasal.io.",
            },
            {"role": "tool", "tool_call_id": "c1", "content": "no browser was running"},
            {"role": "assistant", "content": OBSERVED_MARKUP},
        ]
        assert (
            salvage_last_assistant_text(conversation)
            == "Now I'll create the presentation based on kasal.io."
        )

    def test_salvage_returns_empty_when_nothing_real_was_said(self):
        assert (
            salvage_last_assistant_text(
                [{"role": "assistant", "content": OBSERVED_MARKUP}]
            )
            == ""
        )
        assert salvage_last_assistant_text([]) == ""
