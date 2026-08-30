"""A context rejection is recovered from, not fatal.

From a real run: 240,735 characters of tool results the estimator priced at
70,804 tokens — under the 98,200 budget, so the proactive trim stood aside — and
the server counted at 128,975, because the content was JSON-escaped Cyrillic at
1.4 chars/token. The request was refused, ``LLM.call`` re-raised it as
``LLMContextLengthExceededError``, the executor correctly declined to replay a
turn that had already run tools, and the run died holding eight tool results it
was allowed to stub.

The rejection is cheap and carries the true count. These tests pin the reactive
path — stub, calibrate, retry the same round — and that it stays out of the way
of every other error.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.core.events.types import ContextCompactionEvent
from src.core.llm.transport.completion import OpenAICompletion
from src.core.llm.transport.context_recovery import (
    MAX_REJECTIONS_PER_ROUND,
    TOOL_RESULT_STUB,
    reported_prompt_tokens,
    stub_oldest_tool_results,
)
from src.core.llm.transport.exceptions import LLMContextLengthExceededError

# Genuinely unregistered, so the window comes from the agent (131,072 → 111,411
# after the derate → 89,128 trim budget with the 0.8 margin).
MODEL = "some-unregistered-selfhosted-model-v9"


def _rejection(counted: int = 133502) -> Exception:
    """The OpenAI-style phrasing — matched by the engine's built-in list, so
    no phrase extension has to have run for the test to see a context error."""
    return Exception(
        "Error code: 400 - This model's maximum context length is 131072 "
        f"tokens. However, your messages resulted in {counted} tokens."
    )


def _response(content: str = "done"):
    return SimpleNamespace(
        choices=[
            SimpleNamespace(
                finish_reason="stop",
                message=SimpleNamespace(content=content, tool_calls=None),
            )
        ],
        usage=None,
    )


def _stream(content: str = "done"):
    return iter(
        [
            SimpleNamespace(
                choices=[
                    SimpleNamespace(
                        finish_reason="stop",
                        delta=SimpleNamespace(
                            content=content,
                            tool_calls=None,
                            reasoning_content=None,
                            reasoning=None,
                        ),
                    )
                ],
                usage=None,
            )
        ]
    )


class _Agent:
    def __init__(self, respect: bool = True):
        self.role = "researcher"
        self.id = "agent-1"
        self.max_context_window_size = 131072
        self.respect_context_window = respect
        self.max_rpm = None
        self.max_iter = 25
        self.max_execution_time = None


def _llm(**kwargs) -> OpenAICompletion:
    llm = OpenAICompletion(model=MODEL, api_key="x", max_tokens=8192, **kwargs)
    object.__setattr__(llm, "_client", MagicMock())
    return llm


def _conversation(tool_messages: int = 4, chars: int = 40000):
    """~47k estimated tokens at 4×40,000: under the 89,128 budget, so the
    proactive trim does nothing — exactly the failing run's situation."""
    return [
        {"role": "system", "content": "you are a helpful agent"},
        {"role": "user", "content": "find the listings"},
    ] + [
        {"role": "tool", "tool_call_id": f"c{i}", "content": "R" * chars}
        for i in range(tool_messages)
    ]


def _stubbed(conversation):
    return [m for m in conversation if m.get("content") == TOOL_RESULT_STUB]


@pytest.fixture
def emitted():
    captured = []
    with patch(
        "src.core.llm.transport.completion.event_bus.emit",
        side_effect=lambda source, event: captured.append(event),
    ):
        yield captured


def _compactions(emitted):
    return [e for e in emitted if isinstance(e, ContextCompactionEvent)]


class TestTheRoundIsRetriedBehindCompaction:
    def test_a_rejected_round_is_retried_with_the_oldest_results_stubbed(self, emitted):
        llm = _llm()
        agent = _Agent()
        conversation = _conversation()
        create = llm.client.chat.completions.create
        create.side_effect = [_rejection(), _response("done")]

        answer = llm.call(conversation, from_agent=agent)

        assert answer == "done"
        assert create.call_count == 2
        # The retry carried the stub, not the original result.
        retried_messages = create.call_args_list[1].kwargs["messages"]
        assert retried_messages[2]["content"] == TOOL_RESULT_STUB
        # Oldest first, newest kept: the agent's most recent evidence survives.
        assert conversation[2]["content"] == TOOL_RESULT_STUB
        assert conversation[-1]["content"] != TOOL_RESULT_STUB
        assert conversation[0]["content"] == "you are a helpful agent"

    def test_the_compaction_is_visible_in_the_trace(self, emitted):
        llm = _llm()
        conversation = _conversation()
        llm.client.chat.completions.create.side_effect = [_rejection(), _response()]

        llm.call(conversation, from_agent=_Agent())

        (event,) = _compactions(emitted)
        assert event.strategy == "tool_result_stub_after_rejection"
        assert event.messages_compacted == len(_stubbed(conversation))
        assert event.tokens_after < event.tokens_before
        assert "133502" in event.reason

    def test_the_streaming_path_recovers_too(self, emitted):
        llm = _llm(stream=True)
        conversation = _conversation()
        create = llm.client.chat.completions.create
        create.side_effect = [_rejection(), _stream("streamed")]

        assert llm.call(conversation, from_agent=_Agent()) == "streamed"
        assert create.call_count == 2
        assert _stubbed(conversation)

    def test_the_responses_api_path_recovers_too(self, emitted):
        llm = _llm(api="responses")
        conversation = [
            {"role": "user", "content": "q"},
            {"type": "function_call", "call_id": "c0", "name": "t", "arguments": "{}"},
            {"type": "function_call_output", "call_id": "c0", "output": "R" * 40000},
            {"type": "function_call", "call_id": "c1", "name": "t", "arguments": "{}"},
            {"type": "function_call_output", "call_id": "c1", "output": "R" * 40000},
        ]
        create = llm.client.responses.create
        create.side_effect = [
            _rejection(),
            SimpleNamespace(id="r1", output_text="done", output=[], usage=None),
        ]

        assert llm.call(conversation, from_agent=_Agent()) == "done"
        assert create.call_count == 2
        assert conversation[2]["output"] == TOOL_RESULT_STUB

    def test_a_rejection_does_not_consume_a_tool_round(self, emitted):
        """The retry is the SAME round: the agent's round budget is for the
        model's decisions, not for the server's refusals."""
        llm = _llm()
        agent = _Agent()
        agent.max_iter = 1
        conversation = _conversation()
        llm.client.chat.completions.create.side_effect = [_rejection(), _response()]

        assert llm.call(conversation, from_agent=agent) == "done"


class TestTheServerCountCalibratesTheEstimator:
    def test_the_trim_budget_shrinks_by_the_observed_ratio(self, emitted):
        llm = _llm()
        agent = _Agent()
        conversation = _conversation()
        estimate_before = llm._estimate_tokens(conversation)
        budget_before = llm._trim_budget(agent)
        llm.client.chat.completions.create.side_effect = [
            _rejection(counted=133502),
            _response(),
        ]

        llm.call(conversation, from_agent=agent)

        assert llm._estimate_correction == pytest.approx(133502 / estimate_before)
        assert llm._trim_budget(agent) == pytest.approx(
            budget_before / llm._estimate_correction, abs=1
        )

    def test_compaction_is_sized_to_the_corrected_budget(self, emitted):
        """Not everything, not one: exactly enough for the server's count."""
        llm = _llm()
        agent = _Agent()
        conversation = _conversation()
        llm.client.chat.completions.create.side_effect = [_rejection(), _response()]

        llm.call(conversation, from_agent=agent)

        assert llm._estimate_tokens(conversation) <= llm._trim_budget(agent)
        assert 0 < len(_stubbed(conversation)) < 4

    def test_a_count_that_does_not_exceed_the_estimate_is_not_believed(self, emitted):
        """A stray number below the estimate cannot explain the rejection;
        halve instead of calibrating on it."""
        llm = _llm()
        conversation = _conversation()
        llm.client.chat.completions.create.side_effect = [
            Exception("maximum context length exceeded: you sent 1000 tokens"),
            _response(),
        ]

        llm.call(conversation, from_agent=_Agent())

        assert llm._estimate_correction == 1.0
        assert _stubbed(conversation)

    def test_an_absurd_ratio_is_not_believed(self, emitted):
        """A timestamp-sized number would shred the context for the rest of
        the run if it were taken as a tokenizer ratio."""
        llm = _llm()
        conversation = _conversation()
        llm.client.chat.completions.create.side_effect = [
            _rejection(counted=1788088217),
            _response(),
        ]

        llm.call(conversation, from_agent=_Agent())

        assert llm._estimate_correction == 1.0
        assert 0 < len(_stubbed(conversation)) < 4

    def test_the_proactive_trim_uses_the_calibration_on_later_rounds(self, emitted):
        llm = _llm()
        agent = _Agent()
        conversation = _conversation()
        llm.client.chat.completions.create.side_effect = [_rejection(), _response()]
        llm.call(conversation, from_agent=agent)

        # A fresh conversation of the same size now trims BEFORE the request.
        later = _conversation()
        llm._trim_conversation_to_window(later, agent)

        assert _stubbed(later), "the corrected budget should trim proactively"


class TestEverythingElseStillPropagates:
    def test_a_non_context_error_is_not_retried(self, emitted):
        llm = _llm()
        conversation = _conversation()
        create = llm.client.chat.completions.create
        create.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            llm.call(conversation, from_agent=_Agent())

        assert create.call_count == 1
        assert not _stubbed(conversation)
        assert not _compactions(emitted)

    def test_an_agent_that_opted_out_of_trimming_gets_the_error(self, emitted):
        llm = _llm()
        conversation = _conversation()
        create = llm.client.chat.completions.create
        create.side_effect = _rejection()

        with pytest.raises(LLMContextLengthExceededError):
            llm.call(conversation, from_agent=_Agent(respect=False))

        assert create.call_count == 1
        assert not _stubbed(conversation)

    def test_with_nothing_left_to_stub_the_error_propagates(self, emitted):
        llm = _llm()
        conversation = _conversation(tool_messages=1)
        create = llm.client.chat.completions.create
        create.side_effect = _rejection()

        with pytest.raises(LLMContextLengthExceededError):
            llm.call(conversation, from_agent=_Agent())

        # One retry with the single result stubbed, then nothing to give.
        assert create.call_count == 2
        assert conversation[0]["content"] == "you are a helpful agent"

    def test_a_server_that_keeps_refusing_is_capped(self, emitted):
        llm = _llm()
        conversation = _conversation(tool_messages=40, chars=2000)
        create = llm.client.chat.completions.create
        create.side_effect = _rejection()

        with pytest.raises(LLMContextLengthExceededError):
            llm.call(conversation, from_agent=_Agent())

        assert create.call_count <= MAX_REJECTIONS_PER_ROUND + 1


class TestReadingTheServersCount:
    @pytest.mark.parametrize(
        ("message", "expected"),
        [
            (
                "request (133502 tokens) exceeds the available context size "
                "(131072 tokens), try increasing it",
                133502,
            ),
            (
                "This model's maximum context length is 131072 tokens. However, "
                "you requested 139516 tokens (131324 in the messages, 8192 in "
                "the completion). Please reduce the length of the messages.",
                139516,
            ),
            (
                "This model's maximum context length is 128000 tokens. However, "
                "your messages resulted in 133502 tokens.",
                133502,
            ),
            ("prompt is too long: 213462 tokens > 200000 maximum", 213462),
            ("prompt is too long: 213,462 tokens > 200,000 maximum", 213462),
        ],
    )
    def test_the_largest_number_is_what_was_sent(self, message, expected):
        assert reported_prompt_tokens(message) == expected

    def test_ids_versions_and_status_codes_are_not_counts(self):
        assert (
            reported_prompt_tokens(
                "Error code: 400 (request id req_1234567890, sdk 2.32.0): "
                "context length exceeded"
            )
            is None
        )

    def test_no_number_means_none(self):
        assert reported_prompt_tokens("context window full") is None
        assert reported_prompt_tokens("") is None


class TestStubbingOldestFirst:
    def test_stops_once_under_target_and_skips_already_stubbed(self):
        conversation = _conversation(tool_messages=3, chars=100)
        conversation[2]["content"] = TOOL_RESULT_STUB

        def estimate() -> int:
            return sum(len(m["content"]) for m in conversation)

        compacted = stub_oldest_tool_results(conversation, estimate, target=250)

        assert compacted == 1
        assert conversation[3]["content"] == TOOL_RESULT_STUB
        assert conversation[4]["content"] == "R" * 100

    def test_never_touches_system_or_user_messages(self):
        conversation = _conversation(tool_messages=2, chars=100)

        stub_oldest_tool_results(conversation, lambda: 10**9, target=0)

        assert conversation[0]["content"] == "you are a helpful agent"
        assert conversation[1]["content"] == "find the listings"
        assert len(_stubbed(conversation)) == 2
