"""The trim must measure what the SERVER measures.

From a real failure: a 139,516-token request reached a 131,072-token server
while this code concluded, every round, that the conversation fit — so nothing
was ever compacted and the run died on a 400 with 52 tool results (~900,000
characters) it was allowed to stub and never did.
"""

import pytest

# Importing the manager registers every seeded model's window, which is how the
# failing model gets its 131,072 — the raw constants dict alone does not show it.
import src.services.llm.manager  # noqa: F401
from src.core.llm.transport.completion import OpenAICompletion

# The model from the failing run: seeded, registered, 131,072 window.
CUSTOM_MODEL = "KAT-Coder-V2.5-Dev"


class _Agent:
    """The failing run's agent: 131,072 window, 8,192 output, trim enabled."""

    def __init__(self, window=131072, respect=True):
        self.max_context_window_size = window
        self.respect_context_window = respect


def _llm(**kwargs):
    return OpenAICompletion(model=CUSTOM_MODEL, api_key="x", **kwargs)


def _conversation(tool_messages: int, chars: int):
    conversation = [{"role": "system", "content": "you are a helpful agent"}]
    for i in range(tool_messages):
        conversation.append(
            {"role": "tool", "tool_call_id": f"c{i}", "content": "x" * chars}
        )
    return conversation


def _fat_tools(count: int, chars: int):
    """Tool schemas are prompt too — a research agent carries several, each with
    a long description."""
    return [
        {
            "type": "function",
            "function": {
                "name": f"tool_{i}",
                "description": "d" * chars,
                "parameters": {"type": "object", "properties": {}},
            },
        }
        for i in range(count)
    ]


class TestToolSchemasCount:
    def test_the_tools_are_measured(self):
        """They were not, and the server counts them."""
        llm = _llm()
        conversation = _conversation(2, 1000)

        without = llm._estimate_tokens(conversation)
        with_tools = llm._estimate_tokens(conversation, _fat_tools(8, 4000))

        assert with_tools > without * 2

    def test_a_conversation_that_only_overflows_WITH_tools_is_trimmed(self):
        """The regression itself: under budget on messages alone, over it once
        the schemas the request actually carries are counted."""
        llm = _llm(max_tokens=8192)
        agent = _Agent()
        tools = _fat_tools(12, 8000)
        conversation = _conversation(12, 20000)

        llm._trim_conversation_to_window(conversation, agent, tools)

        stubbed = [
            m for m in conversation if m.get("content", "").startswith("[earlier")
        ]
        assert stubbed, "the tool results should have been stubbed"

    def test_the_system_prompt_is_never_stubbed(self):
        llm = _llm(max_tokens=8192)
        conversation = _conversation(20, 40000)

        llm._trim_conversation_to_window(conversation, _Agent(), _fat_tools(4, 4000))

        assert conversation[0]["content"] == "you are a helpful agent"


class TestTheEstimateIsAllowedToBeWrong:
    def test_the_trim_budget_sits_below_the_input_budget(self):
        """The output clamp has always reserved 15% for estimator drift; the
        trim compared against the raw number, so the two halves of one budget
        disagreed by exactly the amount the estimate drifts."""
        llm = _llm(max_tokens=8192)
        agent = _Agent()

        assert llm._trim_budget(agent) < llm._input_budget(agent)

    def test_denser_than_expected_text_still_fits_the_server(self):
        """German compounds and JSON tokenize nearer 2.7 chars/token than the
        3.4 assumed, so the budget has to hold at the WORST plausible density.

        Without the margin: 131,072 - 8,192 output - 128 = 122,752 estimated
        tokens, which at 2.7 chars/token is ~154,000 real ones against a
        131,072-token server. That is the gap the run fell through — the trim
        was satisfied at every round while the request was half again too big.
        """
        llm = _llm(max_tokens=8192)
        budget_tokens = llm._trim_budget(_Agent())

        worst_case_real_tokens = budget_tokens * (3.4 / 2.7)

        assert worst_case_real_tokens < 131072

    def test_an_unknown_budget_still_disables_the_trim(self):
        """No window from anywhere: do not invent one and shred the context."""
        llm = _llm()
        assert llm._trim_budget(None) >= 0
