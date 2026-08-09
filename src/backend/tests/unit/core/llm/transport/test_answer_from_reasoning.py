"""Salvaging an answer a model wrote into its reasoning channel.

Reproduced from a live run: a self-hosted vLLM endpoint returned
``content=None`` with 1,255 completion tokens and the complete JSON answer
inside ``reasoning_content``. The task received ``''`` and reported success, so
the structured output never parsed, nothing reached flow state, and a router
condition over it evaluated False with nothing saying why.
"""

import pytest

from src.core.llm.transport.response_parsing import (
    REDACTED_REASONING,
    answer_from_reasoning,
)

# The shape actually observed, trimmed.
OBSERVED = """I need to research current news articles, then classify them.
Let me gather more specific article details first.
Now I have enough information. Let me compile the classification.

```json
{
  "classification": [
    {"category": "politics", "title": "Senate Confirms Todd Blanche"},
    {"category": "sports", "title": "MLB Trade Deadline"}
  ]
}
```"""


class TestRecovery:
    def test_recovers_a_fenced_json_answer(self):
        recovered = answer_from_reasoning(OBSERVED)

        assert recovered.startswith("{")
        assert '"classification"' in recovered
        assert "Let me compile" not in recovered

    def test_the_recovered_text_is_valid_json(self):
        import json

        parsed = json.loads(answer_from_reasoning(OBSERVED))

        assert [item["category"] for item in parsed["classification"]] == [
            "politics",
            "sports",
        ]

    def test_takes_the_LAST_fenced_block(self):
        """A model may fence an example while thinking, then fence its answer."""
        reasoning = (
            'Maybe like this:\n```json\n{"draft": true}\n```\n'
            'No — the final answer is:\n```json\n{"final": true}\n```'
        )

        assert answer_from_reasoning(reasoning) == '{"final": true}'

    def test_recovers_a_bare_json_object_with_no_fence(self):
        reasoning = 'Thinking about it.\n{"classification": [{"category": "politics"}]}'

        assert answer_from_reasoning(reasoning).startswith('{"classification"')

    def test_recovers_a_bare_json_array(self):
        assert answer_from_reasoning('Here goes\n[{"a": 1}]') == '[{"a": 1}]'

    def test_a_brace_inside_a_string_does_not_unbalance_the_scan(self):
        reasoning = 'x\n{"note": "a } inside text", "ok": true}'

        assert (
            answer_from_reasoning(reasoning)
            == '{"note": "a } inside text", "ok": true}'
        )

    def test_takes_the_last_of_several_bare_objects(self):
        assert (
            answer_from_reasoning('{"first": 1}\nthen\n{"second": 2}')
            == '{"second": 2}'
        )

    def test_recovers_a_fenced_block_that_is_not_json(self):
        reasoning = "Let me write it.\n```\nThe answer is 42.\n```"

        assert answer_from_reasoning(reasoning) == "The answer is 42."


class TestRefusesToInvent:
    """Prose thinking is not an answer. Passing it off as one would replace a
    visible empty result with a plausible wrong one."""

    @pytest.mark.parametrize(
        "reasoning",
        [
            "",
            "   ",
            "I considered the options and weighed them carefully.",
            REDACTED_REASONING,
            '{"unterminated": ',
            "```\n\n```",
        ],
    )
    def test_returns_empty_when_there_is_no_delimited_payload(self, reasoning):
        assert answer_from_reasoning(reasoning) == ""

    def test_does_not_treat_the_redaction_sentinel_as_content(self):
        assert answer_from_reasoning(REDACTED_REASONING) == ""


class TestWiredIntoTheCompletion:
    """`_answer_or_recover` is the single point both the streaming and
    non-streaming paths return through."""

    @staticmethod
    def _llm():
        from unittest.mock import MagicMock

        from src.core.llm.transport.completion import OpenAICompletion

        llm = OpenAICompletion(model="test-model")
        object.__setattr__(llm, "_client", MagicMock())
        return llm

    def test_real_content_always_wins(self):
        llm = self._llm()
        llm._reasoning_text = OBSERVED

        assert (
            llm._answer_or_recover("the real answer", {}, OBSERVED) == "the real answer"
        )

    def test_empty_content_recovers_from_reasoning(self):
        llm = self._llm()
        llm._reasoning_text = OBSERVED

        recovered = llm._answer_or_recover("", {"completion_tokens": 1255}, OBSERVED)

        assert '"classification"' in recovered

    def test_empty_content_and_unsalvageable_reasoning_stays_empty(self, caplog):
        llm = self._llm()
        llm._reasoning_text = "I thought about it at length."

        with caplog.at_level("ERROR"):
            assert (
                llm._answer_or_recover(
                    "", {"completion_tokens": 900}, "I thought about it at length."
                )
                == ""
            )

        assert "produced NO answer" in caplog.text

    def test_a_genuinely_empty_call_is_not_shouted_about(self, caplog):
        """No tokens and no reasoning is a legitimately empty completion."""
        llm = self._llm()
        llm._reasoning_text = ""

        with caplog.at_level("ERROR"):
            assert llm._answer_or_recover("", {"completion_tokens": 0}, "") == ""

        assert "produced NO answer" not in caplog.text


class TestRefusesPlausibleWrongAnswers:
    """Every one of these was REPRODUCED against the first version of this
    function. A recovered wrong answer is worse than a recovered nothing: it has
    the right shape, so it validates, reaches flow state, and routes on it."""

    @pytest.mark.parametrize(
        "name,reasoning",
        [
            (
                "an echoed output schema, then a prose conclusion",
                'Shape is {"classification": [{"category": "<string>"}]} so I follow '
                "it. I could not retrieve any articles.",
            ),
            (
                "an answer followed by a rejected counter-proposal",
                'Answer: {"classification": [{"category": "politics"}]}\n'
                'Actually no, {"classification": ["support"]} would be wrong.',
            ),
            (
                "a citation marker that parses as JSON",
                "The ticket is billing related [1].",
            ),
            ("a Python repr rather than JSON", "result = {'classification': ['a']}"),
        ],
    )
    def test_returns_nothing_rather_than_something_wrong(self, name, reasoning):
        assert answer_from_reasoning(reasoning) == "", name

    def test_prefers_the_real_payload_over_an_earlier_fenced_example(self):
        reasoning = (
            'Example:\n```json\n{"classification": "not-a-list"}\n```\n'
            'Real:\n```json\n{"classification": [{"category": "politics"}]}'
        )

        recovered = answer_from_reasoning(reasoning)

        assert "not-a-list" not in recovered
        assert '"politics"' in recovered

    def test_ignores_a_code_fence_of_a_different_language(self):
        reasoning = (
            "Sketch:\n```python\nfor r in rows:\n    print(r)\n```\n"
            'Final: {"classification": [{"category": "politics"}]}'
        )

        assert answer_from_reasoning(reasoning).startswith('{"classification"')

    def test_a_backtick_inside_a_json_string_does_not_break_it(self):
        reasoning = '{"note": "wrap in ``` fences", "ok": true}'

        assert answer_from_reasoning(reasoning) == reasoning


class TestRoundScoping:
    """_reasoning_text is reset per CALL but appended per ROUND."""

    def test_an_empty_round_cannot_salvage_an_earlier_rounds_thinking(self):
        from unittest.mock import MagicMock

        from src.core.llm.transport.completion import OpenAICompletion

        llm = OpenAICompletion(model="test-model")
        object.__setattr__(llm, "_client", MagicMock())

        round_one = (
            'I need to call the tool with\n```json\n{"query": "news", "limit": 5}\n```'
        )
        round_two = "Nothing further to add."
        llm._reasoning_text = round_one + round_two

        # Only THIS round's slice is offered, which is what the call site passes.
        assert llm._answer_or_recover("", {"completion_tokens": 20}, round_two) == ""


def test_the_real_observed_shape_an_orphan_closing_fence():
    """Execution fdbfb475: the model never OPENED its block, so the reasoning
    ends with a lone ``` after the payload. Anchoring naively against that
    dropped the answer this function exists to recover."""
    reasoning = 'Let me compile.\n{"classification": [{"category": "politics"}]}\n```'

    recovered = answer_from_reasoning(reasoning)

    assert recovered == '{"classification": [{"category": "politics"}]}'


class TestTruncationIsItsOwnFailure:
    """Running out of output budget is actionable; "produced NO answer" is not.

    Observed on execution e2f03069: finish_reason=length, 8,192 completion
    tokens against an 8,192 max_output_tokens cap, 35,606 chars of reasoning,
    content None. The model spent its whole allowance thinking and was cut off
    mid-sentence.
    """

    @staticmethod
    def _llm(finish_reason=None):
        from unittest.mock import MagicMock

        from src.core.llm.transport.completion import OpenAICompletion

        llm = OpenAICompletion(model="test-model")
        object.__setattr__(llm, "_client", MagicMock())
        llm._finish_reason = finish_reason
        return llm

    def test_a_truncated_call_says_so(self, caplog):
        llm = self._llm(finish_reason="length")

        with caplog.at_level("ERROR"):
            assert (
                llm._answer_or_recover("", {"completion_tokens": 8192}, "thinking…")
                == ""
            )

        assert "ran out of output budget" in caplog.text
        assert "max_output_tokens" in caplog.text
        assert "produced NO answer" not in caplog.text

    def test_a_non_truncated_empty_answer_keeps_the_generic_message(self, caplog):
        llm = self._llm(finish_reason="stop")

        with caplog.at_level("ERROR"):
            assert (
                llm._answer_or_recover("", {"completion_tokens": 900}, "thinking…")
                == ""
            )

        assert "produced NO answer" in caplog.text
        assert "ran out of output budget" not in caplog.text

    def test_recovery_still_wins_over_the_truncation_message(self, caplog):
        """A truncated call that nevertheless left a complete payload should be
        recovered, not reported as a failure."""
        llm = self._llm(finish_reason="length")

        with caplog.at_level("WARNING"):
            recovered = llm._answer_or_recover(
                "", {"completion_tokens": 8192}, OBSERVED
            )

        assert '"classification"' in recovered
        assert "ran out of output budget" not in caplog.text
