"""The judge is told when the tools failed, and the degradation says why.

Both fix the same observed behaviour. A task whose every source call returned
404/503 had no data to work with. The guardrail could see only the output text,
so it explained the shortfall with the only cause it could observe — the
writing — and asked three times for "named agents and an ordered task
sequence". The retries rewrote prose. The data still did not exist. Then
``degrade`` appended "⚠️ Unverified: <the judge's guess>", which is how a run
that never reached its source became indistinguishable from a badly written one.
"""

import pytest

from src.services.execution.runtime.executor import reset_tool_ledger, wrap_tool
from src.services.execution.runtime.guardrail import LLMGuardrail
from src.services.execution.runtime.task import Task
from src.services.execution.runtime.types import TaskOutput
from src.services.tools.base import BaseTool


def _out(raw="answer"):
    """TaskOutput needs a description; nothing here depends on its content."""
    return TaskOutput(description="Ingest listings", agent="a", raw=raw)


def _failing_tool(name="Parse_call_endpoint"):
    class _T(BaseTool):
        def _run(self, **kwargs):
            return "Tool error: 503 Site protection is blocking all proxies"

    return wrap_tool(_T(name=name, description="a source"))


class _CapturingLLM:
    """Records the judge prompt and answers however the test wants."""

    def __init__(self, verdict='{"valid": true, "feedback": "fine"}'):
        self.verdict = verdict
        self.prompts: list[str] = []

    def call(self, messages, **kwargs):
        self.prompts.append(messages[0]["content"])
        return self.verdict


@pytest.fixture(autouse=True)
def _fresh_ledger():
    reset_tool_ledger()
    yield
    reset_tool_ledger()


class TestTheJudgeIsToldAboutFailures:
    def test_a_clean_run_gets_no_tool_section(self):
        """Absence of failure must add nothing — an always-present section
        would spend tokens on every guardrail call in the product."""
        llm = _CapturingLLM()
        LLMGuardrail("Must cite sources", llm)(_out())
        assert "did not all succeed" not in llm.prompts[0]

    def test_a_failed_tool_reaches_the_prompt(self):
        _failing_tool()()
        llm = _CapturingLLM()
        LLMGuardrail("Must insert records", llm)(_out())

        prompt = llm.prompts[0]
        assert "did not all succeed" in prompt
        assert "Parse_call_endpoint" in prompt
        assert "ALL FAILED" in prompt

    def test_the_judge_is_told_not_to_ask_for_an_impossible_rewrite(self):
        """The instruction is the point. Naming the failure without it just
        gives the judge more text to reject the answer with."""
        _failing_tool()()
        llm = _CapturingLLM()
        LLMGuardrail("Must insert records", llm)(_out())
        assert "rewrite that cannot fix it" in llm.prompts[0]
        assert "name the tool" in llm.prompts[0]

    def test_the_criterion_and_output_are_still_there(self):
        """Adding context must not displace what the judge is actually for."""
        _failing_tool()()
        llm = _CapturingLLM()
        LLMGuardrail("Must cite every claim", llm)(_out("the answer text"))
        assert "Must cite every claim" in llm.prompts[0]
        assert "the answer text" in llm.prompts[0]


class TestDegradationSaysWhy:
    @staticmethod
    def _task(**kw):
        return Task(
            description="Ingest listings into Postgres",
            expected_output="rows inserted",
            guardrail_on_exhausted="degrade",
            guardrail_max_retries=0,
            **kw,
        )

    def test_a_wholly_failed_tool_becomes_the_stated_cause(self):
        """Not the judge's guess. A reader has to be able to tell "no data
        existed" from "the writing was poor"."""
        _failing_tool()()
        task = self._task(guardrail=lambda o: (False, "does not define named agents"))

        out = task._apply_guardrails(_out(), agent=None, context=None, tools=None)

        assert out.degraded is True
        assert "Parse_call_endpoint" in out.degradation_reason
        assert "never available" in out.degradation_reason
        assert "Parse_call_endpoint" in out.raw, "and it is visible in the text too"

    def test_without_tool_failures_the_judges_objection_stands(self):
        """When the tools all worked, the judge's reason IS the reason — this
        must not invent a tool excuse for a genuinely poor answer."""
        task = self._task(guardrail=lambda o: (False, "no sources cited"))

        out = task._apply_guardrails(_out(), agent=None, context=None, tools=None)

        assert out.degraded is True
        assert out.degradation_reason == "no sources cited"

    def test_a_passing_guardrail_leaves_the_output_undegraded(self):
        task = self._task(guardrail=lambda o: (True, "good"))
        out = task._apply_guardrails(_out(), agent=None, context=None, tools=None)
        assert out.degraded is False
        assert out.degradation_reason is None

    def test_degraded_is_structured_not_only_prose(self):
        """It surfaced only as a WARNING line and a string appended to raw, so
        nothing downstream could ASK whether an output was trustworthy."""
        assert "degraded" in TaskOutput.model_fields
        assert "degradation_reason" in TaskOutput.model_fields
        assert _out("x").degraded is False


class TestATaskWithNoGuardrailStillReportsADeadSource:
    """The common case, and the one that was silent.

    Only a task that HAS a guardrail went through the reporting path above, and
    most tasks do not. Without this, a task whose every source call returned 503
    produces a confident answer built on nothing and is reported as a plain
    success — which is exactly what happened.
    """

    @staticmethod
    def _task():
        return Task(description="Ingest listings", expected_output="rows")

    def test_a_wholly_failed_tool_degrades_the_output(self):
        _failing_tool()()
        out = self._task()._flag_unavailable_sources(_out())
        assert out.degraded is True
        assert "Parse_call_endpoint" in out.degradation_reason
        assert "⚠️ Unverified" in out.raw

    def test_a_working_tool_leaves_it_alone(self):
        class _Ok(BaseTool):
            def _run(self, **kwargs):
                return "results"

        wrap_tool(_Ok(name="Search", description="s"))()
        out = self._task()._flag_unavailable_sources(_out())
        assert out.degraded is False
        assert "Unverified" not in out.raw

    def test_a_tool_that_recovered_is_not_a_dead_source(self):
        """Failed twice then worked: the data arrived. Flagging this would
        train people to ignore the flag."""
        flaky = _failing_tool("Flaky")
        flaky()

        class _Ok(BaseTool):
            def _run(self, **kwargs):
                return "results"

        wrap_tool(_Ok(name="Flaky", description="s"))()
        out = self._task()._flag_unavailable_sources(_out())
        assert out.degraded is False

    def test_it_does_not_overwrite_the_guardrails_richer_reason(self):
        already = _out().model_copy(
            update={"degraded": True, "degradation_reason": "judge said X"}
        )
        _failing_tool()()
        assert (
            self._task()._flag_unavailable_sources(already).degradation_reason
            == "judge said X"
        )

    def test_it_flags_rather_than_raises(self):
        """The engine cannot know a dead tool was essential — an agent with a
        search AND a database tool may have been asked something the database
        alone answers. Raising would fail runs that legitimately succeeded,
        which is how a guard gets switched off."""
        _failing_tool()()
        self._task()._flag_unavailable_sources(_out())  # must not raise


class TestTheCrewReportsIt:
    def test_a_crew_is_degraded_when_any_task_was(self):
        """A crew reports the LAST task's output as its own, so a run whose
        task four ran on a dead source and whose task five summarised nothing
        looked entirely clean."""
        from src.services.execution.runtime.types import CrewOutput

        clean = _out("fine")
        bad = _out("built on nothing").model_copy(
            update={"degraded": True, "degradation_reason": "Parse was down"}
        )
        crew = CrewOutput(raw="fine", tasks_output=[bad, clean])

        assert crew.degraded is True
        assert crew.degradation_reasons == ["Parse was down"]

    def test_an_all_clean_crew_is_not_degraded(self):
        from src.services.execution.runtime.types import CrewOutput

        crew = CrewOutput(raw="fine", tasks_output=[_out(), _out()])
        assert crew.degraded is False
        assert crew.degradation_reasons == []

    def test_it_is_derived_not_stored(self):
        """A stored copy is one that can be set and then contradicted."""
        from src.services.execution.runtime.types import CrewOutput

        assert "degraded" not in CrewOutput.model_fields
