"""The wrap-up call after a spent budget, and what happens when it says nothing.

From a live run: a research agent made 22 ``browser_search_and_read`` calls,
hit its 25-round cap, and the wrap-up call started compiling the answer — in
its REASONING channel, where it spent all 8,192 output tokens
(``finish_reason=length``, ``content=None``). Nothing reached ``content``, the
partial was empty (that model never writes prose there), and the run died with
"did not converge" after every search had succeeded.

Three outcomes are pinned here: a wrap-up that answers; a blow-out that gets
one direct retry; and two blow-outs that raise with the model's draft carried as
the budget error's partial, so a degrading Task keeps it.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.core.llm.transport.budget import (
    FORCE_FINAL_ANSWER,
    FORCE_FINAL_ANSWER_DIRECTLY,
    partial_from_reasoning,
)
from src.core.llm.transport.completion import OpenAICompletion
from src.core.llm.transport.exceptions import ExecutionBudgetExceededError
from src.core.llm.transport.response_parsing import REDACTED_REASONING

MODEL = "some-unregistered-selfhosted-model-v9"
SEARCH = [
    {
        "type": "function",
        "function": {
            "name": "search",
            "description": "s",
            "parameters": {"type": "object", "properties": {}},
        },
    }
]
DRAFT = (
    "I have gathered enough information. Let me compile the curated list.\n"
    "From trovas.ch: 1. NVIDIA GeForce RTX 3090 24GB - 650 CHF - Zurich"
)


class _Agent:
    def __init__(self, max_iter: int = 1):
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


def _tool_call(call_id: str):
    return SimpleNamespace(
        choices=[
            SimpleNamespace(
                finish_reason="tool_calls",
                message=SimpleNamespace(
                    content=None,
                    reasoning_content=None,
                    tool_calls=[
                        SimpleNamespace(
                            id=call_id,
                            function=SimpleNamespace(
                                name="search", arguments='{"q": "gpus"}'
                            ),
                        )
                    ],
                ),
            )
        ],
        usage=None,
    )


def _text(content, reasoning=None, finish_reason="stop"):
    return SimpleNamespace(
        choices=[
            SimpleNamespace(
                finish_reason=finish_reason,
                message=SimpleNamespace(
                    content=content, reasoning_content=reasoning, tool_calls=None
                ),
            )
        ],
        usage=None,
    )


def _blowout(reasoning: str):
    """Output allowance spent inside the reasoning channel: no content."""
    return _text(None, reasoning, finish_reason="length")


def _run(llm: OpenAICompletion) -> str:
    return llm.call(
        [
            {"role": "system", "content": "You are a researcher."},
            {"role": "user", "content": "List used GPUs under 1000 CHF."},
        ],
        tools=SEARCH,
        available_functions={"search": lambda **kw: "10 results"},
        from_agent=_Agent(max_iter=1),  # one round, then the budget is spent
    )


def _last_user_message(create, index: int) -> str:
    return create.call_args_list[index].kwargs["messages"][-1]["content"]


class TestTheWrapUp:
    def test_an_answer_from_the_wrap_up_is_the_answer(self):
        llm = _llm()
        create = llm.client.chat.completions.create
        create.side_effect = [_tool_call("c1"), _text("Here is what I found.")]

        assert _run(llm) == "Here is what I found."
        assert create.call_count == 2
        assert not create.call_args_list[1].kwargs.get("tools")
        assert _last_user_message(create, 1) == FORCE_FINAL_ANSWER

    def test_an_output_blowout_gets_one_retry_told_to_answer_directly(self):
        llm = _llm()
        create = llm.client.chat.completions.create
        create.side_effect = [
            _tool_call("c1"),
            _blowout(DRAFT),
            _text("1. RTX 3090 24GB - 650 CHF - Zurich"),
        ]

        assert _run(llm) == "1. RTX 3090 24GB - 650 CHF - Zurich"
        assert create.call_count == 3
        assert _last_user_message(create, 2) == FORCE_FINAL_ANSWER_DIRECTLY

    def test_two_blowouts_raise_with_the_draft_as_the_partial(self):
        llm = _llm()
        create = llm.client.chat.completions.create
        create.side_effect = [_tool_call("c1"), _blowout(DRAFT), _blowout("short")]

        with pytest.raises(ExecutionBudgetExceededError) as raised:
            _run(llm)

        assert create.call_count == 3
        partial = raised.value.partial
        assert "Recovered from the model's reasoning" in partial
        assert "ran out of output tokens" in partial
        assert DRAFT in partial  # the longer draft wins, not the retry's
        assert "did not converge within 1 rounds" in str(raised.value)

    def test_silence_without_reasoning_raises_as_before_with_no_retry(self):
        llm = _llm()
        create = llm.client.chat.completions.create
        create.side_effect = [_tool_call("c1"), _text(None)]

        with pytest.raises(ExecutionBudgetExceededError) as raised:
            _run(llm)

        assert create.call_count == 2  # finish_reason=stop: nothing to retry for
        assert raised.value.partial == ""


class TestPartialFromReasoning:
    def test_labels_the_draft_and_names_the_cause(self):
        text = partial_from_reasoning(DRAFT, "length")
        assert text.startswith("> ⚠️ Recovered from the model's reasoning")
        assert "ran out of output tokens" in text
        assert text.endswith(DRAFT)

    def test_other_finish_reasons_get_the_neutral_cause(self):
        assert "produced no answer text" in partial_from_reasoning(DRAFT, "stop")

    def test_nothing_and_the_redaction_flag_yield_nothing(self):
        assert partial_from_reasoning("", "length") == ""
        assert partial_from_reasoning(None, "length") == ""
        assert partial_from_reasoning(REDACTED_REASONING, "length") == ""
