"""Defenses against a model stuck re-issuing the same tool call.

Reproduced from a live run: an agent researched a site, closed the browser,
then called ``browser_close`` twenty more times — every result
"no browser was running" — and the turn ended by returning the model's raw
``<tool_call>`` markup as the "answer". Two defenses, tested here:
``RepeatGuard`` (stop executing identical batches, then drop the tools) and
``strip_tool_markup``/``salvage_last_assistant_text`` (markup never leaves the
transport as an answer).
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.core.llm.transport.completion import OpenAICompletion
from src.core.llm.transport.exceptions import ExecutionBudgetExceededError
from src.core.llm.transport.tool_rounds import (
    NO_ANSWER_MARKUP_ONLY,
    REPEATED_RESULT,
    RepeatGuard,
    answer_without_markup,
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

    def test_stub_message_fits_both_loops_it_stops(self):
        # The browser_close loop: the work is done, so answer. The plan loop:
        # nothing has been done, so the model must be pointed at OTHER work,
        # and told to say what it could not do rather than answer with nothing.
        assert "final answer" in REPEATED_RESULT
        assert "DIFFERENT call" in REPEATED_RESULT
        assert "could not do" in REPEATED_RESULT


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


# The second observed leak. An agent whose only tool was ``todo`` (the task
# named web search; the MCP search tool was deliberately not attached) wrote
# its plan, then re-sent the identical write every round: nothing else to
# call. The guard stubbed rounds 3 and 4, dropped the tools, and the model
# wrote its fifth ``todo`` call as plain text. Every assistant turn had
# ``content=None`` — the model put all its prose in the reasoning channel — so
# there was nothing to salvage, and the ``or answer`` fallback returned the
# markup. It was persisted as the task output and written to memory.
TODO_MARKUP = (
    "<tool_call>\n<function=todo>\n<parameter=todos>\n"
    '[{"content": "Search for latest Lebanon news", "id": "1", '
    '"status": "in_progress"}]\n</parameter>\n</function>\n</tool_call>'
)


class TestAnswerWithoutMarkup:
    def test_clean_answer_is_returned_untouched(self):
        assert answer_without_markup("  A fine answer.  ", []) == "  A fine answer.  "

    def test_prose_mixed_with_markup_keeps_the_prose(self):
        assert (
            answer_without_markup("Here is the plan.\n" + TODO_MARKUP, [])
            == "Here is the plan."
        )

    def test_markup_alone_salvages_earlier_assistant_text(self):
        conversation = [
            {"role": "assistant", "content": "Now I'll gather the headlines."},
            {"role": "tool", "tool_call_id": "c1", "content": "Plan (0/1)"},
        ]
        assert (
            answer_without_markup(TODO_MARKUP, conversation)
            == "Now I'll gather the headlines."
        )

    def test_markup_alone_with_nothing_real_never_returns_the_markup(self):
        conversation = [
            {"role": "system", "content": "you are a researcher"},
            {"role": "user", "content": "gather the news"},
            {"role": "assistant", "content": None},  # prose went to reasoning
            {"role": "tool", "tool_call_id": "c1", "content": "Plan (0/1)"},
        ]
        answer = answer_without_markup(TODO_MARKUP, conversation)
        assert answer == NO_ANSWER_MARKUP_ONLY
        assert "<tool_call>" not in answer

    def test_the_stand_in_can_be_empty_for_callers_that_raise(self):
        assert answer_without_markup(TODO_MARKUP, [], when_nothing_real="") == ""

    def test_none_is_an_empty_answer(self):
        assert answer_without_markup(None, []) == ""


# ---------------------------------------------------------------------------
# The whole loop, replayed against the transport with a fake client.
# ---------------------------------------------------------------------------

MODEL = "some-unregistered-selfhosted-model-v9"
TODO_SCHEMA = [
    {
        "type": "function",
        "function": {
            "name": "todo",
            "description": "Track your plan.",
            "parameters": {"type": "object", "properties": {}},
        },
    }
]
TODO_ARGS = '{"todos": [{"id": "1", "content": "Search", "status": "in_progress"}]}'


class _Agent:
    def __init__(self, max_iter: int = 25):
        self.role = "researcher"
        self.id = "agent-1"
        self.max_context_window_size = 131072
        self.respect_context_window = True
        self.max_rpm = None
        self.max_iter = max_iter
        self.max_execution_time = None


def _llm() -> OpenAICompletion:
    llm = OpenAICompletion(model=MODEL, api_key="x", max_tokens=8192)
    object.__setattr__(llm, "_client", MagicMock())
    return llm


def _todo_call(call_id: str):
    """A round in which the model calls ``todo`` and says nothing."""
    return SimpleNamespace(
        choices=[
            SimpleNamespace(
                finish_reason="tool_calls",
                message=SimpleNamespace(
                    content=None,
                    tool_calls=[
                        SimpleNamespace(
                            id=call_id,
                            function=SimpleNamespace(name="todo", arguments=TODO_ARGS),
                        )
                    ],
                ),
            )
        ],
        usage=None,
    )


def _text(content: str):
    return SimpleNamespace(
        choices=[
            SimpleNamespace(
                finish_reason="stop",
                message=SimpleNamespace(content=content, tool_calls=None),
            )
        ],
        usage=None,
    )


def _messages():
    return [
        {"role": "system", "content": "You are a researcher. Keep a plan with todo."},
        {"role": "user", "content": "Gather today's news about Lebanon."},
    ]


class TestThePlanOnlyLoopEndToEnd:
    def test_markup_written_after_the_tools_were_dropped_is_not_the_answer(self):
        llm = _llm()
        executed = []

        # A plain function, not a MagicMock: the transport reads
        # ``result_as_answer`` off the callable, and a mock answers every
        # attribute with something truthy.
        def todo(**kwargs):
            executed.append(kwargs)
            return "Plan (0/1)"

        create = llm.client.chat.completions.create
        create.side_effect = [
            _todo_call("c1"),
            _todo_call("c2"),
            _todo_call("c3"),  # repeat 2: stubbed, not executed
            _todo_call("c4"),  # repeat 3: stubbed, tools dropped
            _text(TODO_MARKUP),  # no tools left, so the call comes out as text
        ]

        answer = llm.call(
            _messages(),
            tools=TODO_SCHEMA,
            available_functions={"todo": todo},
            from_agent=_Agent(),
        )

        assert answer == NO_ANSWER_MARKUP_ONLY
        assert "<tool_call>" not in answer
        assert len(executed) == 2, "the guard executes a batch twice, never more"
        assert create.call_count == 5
        # The fifth request went out with no tools — the guard's last resort.
        assert not create.call_args_list[-1].kwargs.get("tools")
        # And the stub the model saw pointed it at other work / an honest answer.
        sent = create.call_args_list[-1].kwargs["messages"]
        stubs = [
            m
            for m in sent
            if m.get("role") == "tool" and m["content"] == REPEATED_RESULT
        ]
        assert len(stubs) == 2

    def test_the_budget_wrap_up_still_raises_when_nothing_real_was_said(self):
        # Two rounds allowed, both spent on the plan write; the wrap-up call
        # then answers in markup. No sentinel here: the budget error carries
        # the partial and the degrade path decides — as before this change.
        llm = _llm()
        create = llm.client.chat.completions.create
        create.side_effect = [_todo_call("c1"), _todo_call("c2"), _text(TODO_MARKUP)]

        with pytest.raises(ExecutionBudgetExceededError):
            llm.call(
                _messages(),
                tools=TODO_SCHEMA,
                available_functions={"todo": lambda **kw: "Plan (0/1)"},
                from_agent=_Agent(max_iter=2),
            )
        assert create.call_count == 3
