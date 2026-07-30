"""Date awareness: the toggle the UI shows on, that the runtime ignored.

``inject_date`` defaults True in ``schemas/agent.py`` (with a validator turning
None into True), the agent form ships the switch on, and the exported app uses
``cfg.get("inject_date", True)``. The runtime Agent defaulted False — so an
agent spec that omitted the key got no date at all, which is every generated
crew, because the generation templates instruct the model to omit fields the
platform defaults.
"""

from datetime import datetime, timezone

import pytest

from src.services.execution.runtime import Agent
from src.services.execution.runtime.executor import build_messages


def _system_prompt(**kwargs) -> str:
    agent = Agent(role="R", goal="G", backstory="B", **kwargs)
    return build_messages(agent, "do the thing")[0]["content"]


class TestDefault:
    def test_an_agent_that_says_nothing_gets_the_date(self):
        assert "Current date:" in _system_prompt()

    def test_the_runtime_default_matches_the_schema_default(self):
        """The divergence that caused this: two layers, two answers."""
        from src.schemas.agent import AgentCreate

        assert Agent.model_fields["inject_date"].default is True
        assert AgentCreate.model_fields["inject_date"].default is True


class TestItIsAnInstructionNotJustAFact:
    """A bare ``Current date: …`` line lost to the model's training prior.

    Observed on a live deep-research run: the date was demonstrably in the
    system prompt (confirmed in crew.log and by rebuilding the prompt), and the
    agent still issued ``site:arxiv.org agent memory … publication 2025 …``. It
    was not contradicting the date — it never connected the date to the query it
    was writing. So the block has to name that act.
    """

    def test_the_current_year_appears_as_a_bare_number(self):
        """The formatted date may not contain a liftable year (e.g. '%B %d, %y'),
        and the year is what ends up in a search query."""
        year = str(datetime.now(timezone.utc).year)
        assert year in _system_prompt(date_format="%B %d, %y")

    def test_it_states_that_it_outranks_training_data(self):
        prompt = _system_prompt().lower()
        assert "later than your training data" in prompt

    def test_it_forbids_a_recalled_year_in_a_search_query(self):
        prompt = _system_prompt().lower()
        assert "search query" in prompt
        assert "never a year you recall" in prompt

    def test_it_warns_against_calling_the_newest_known_thing_current(self):
        assert "not the newest that exists" in _system_prompt()

    def test_the_whole_block_is_absent_when_opted_out(self):
        prompt = _system_prompt(inject_date=False)
        for marker in ("Current date", "training data", "search query"):
            assert marker not in prompt

    def test_the_block_stays_short(self):
        """It sits after the role, backstory and the ~730-char security
        preamble; a long block there competes with what it supports."""
        from src.services.execution.runtime.executor import date_awareness

        block = date_awareness(datetime.now(timezone.utc), "%Y-%m-%d")
        assert len(block) < 500, f"date block grew to {len(block)} chars"

    def test_it_is_a_pure_function_of_the_clock(self):
        from src.services.execution.runtime.executor import date_awareness

        fixed = datetime(2031, 3, 9, tzinfo=timezone.utc)
        block = date_awareness(fixed, "%Y-%m-%d")
        assert "2031-03-09" in block
        assert "current year: 2031" in block
        assert "use 2031" in block


class TestExplicitChoice:
    def test_opting_in_injects_the_date(self):
        assert "Current date:" in _system_prompt(inject_date=True)

    def test_opting_out_is_still_honoured(self):
        """The default moved; an explicit False must not."""
        assert "Current date:" not in _system_prompt(inject_date=False)

    def test_the_date_is_today(self):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        assert f"Current date: {today}" in _system_prompt()

    @pytest.mark.parametrize(
        "fmt,expected",
        [
            ("%Y-%m-%d", "%Y-%m-%d"),
            ("%B %d, %Y", "%B %d, %Y"),
            ("%d/%m/%Y", "%d/%m/%Y"),
        ],
    )
    def test_a_custom_format_is_used(self, fmt, expected):
        rendered = datetime.now(timezone.utc).strftime(expected)
        assert f"Current date: {rendered}" in _system_prompt(date_format=fmt)


class TestSpecPassThrough:
    """kernel/agent_builder copies a spec value when it `is not None`."""

    @pytest.mark.parametrize("value,expected", [(True, True), (False, False)])
    def test_an_explicit_value_survives_the_builder(self, value, expected):
        from src.services.execution.kernel.agent_builder import (
            _ADDITIONAL_AGENT_PARAMS,
        )

        assert "inject_date" in _ADDITIONAL_AGENT_PARAMS
        spec = {"inject_date": value}
        # The builder's own condition, which is what makes False survive.
        assert (spec.get("inject_date") is not None) is True
        assert Agent(role="R", goal="G", backstory="B", **spec).inject_date is expected

    def test_a_custom_system_template_still_gets_the_date(self):
        """The date is appended after the template branch, not instead of it."""
        prompt = _system_prompt(system_template="You are {role}. Goal: {goal}.")
        assert prompt.startswith("You are R.")
        assert "Current date:" in prompt


class _CapturingLLM:
    """Records the message list the transport would have been given."""

    def __init__(self) -> None:
        self.messages = None

    def call(self, messages, tools=None, available_functions=None, **_kw):
        self.messages = messages
        return "answer"


class TestChatPath:
    """ChatMode runs ``Agent.kickoff`` in-process, not the crew path.

    It assembles messages differently — rebuilding the list around prior chat
    history — so it can regress independently by dropping the system message
    that carries the date.
    """

    def _kickoff(self, messages):
        llm = _CapturingLLM()
        agent = Agent(role="Assistant", goal="g", backstory="b", llm=llm)
        agent.kickoff(messages)
        return llm.messages

    def test_a_single_question_gets_the_date(self):
        sent = self._kickoff("what happened this week?")
        assert sent[0]["role"] == "system"
        assert "Current date:" in sent[0]["content"]

    def test_the_date_survives_prior_chat_history(self):
        """The history branch rebuilds the list; it must keep built[0]."""
        sent = self._kickoff(
            [
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": "hello"},
                {"role": "user", "content": "and today?"},
            ]
        )
        assert [m["role"] for m in sent] == ["system", "user", "assistant", "user"]
        assert "Current date:" in sent[0]["content"]

    def test_opting_out_is_honoured_on_the_chat_path_too(self):
        llm = _CapturingLLM()
        agent = Agent(
            role="Assistant", goal="g", backstory="b", llm=llm, inject_date=False
        )
        agent.kickoff("question")
        assert "Current date:" not in llm.messages[0]["content"]
