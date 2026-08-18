"""The degrade-on-exhausted policy, on a CrewAI task.

``guardrail_on_exhausted="degrade"`` is set automatically by the generated
research and deep answer modes, for a reason recorded where it is set: losing a
six-task run because task four could not satisfy a judge on the third attempt
throws away everything already produced.

CrewAI has no such field, so without this the same crew that degrades on one
harness aborts on the other — and it would surface as a research run that simply
failed, with nothing pointing at the harness setting as the cause.
"""

import pytest

from src.core.llm.transport.llm import LLM
from src.services.execution.harnesses import binding_for, reset_for_tests
from src.services.execution.harnesses.crewai.guardrails import (
    DEGRADED_MARKER,
    degrade_on_exhausted,
)


@pytest.fixture(autouse=True)
def _clean():
    reset_for_tests()
    yield
    reset_for_tests()


class _Output:
    def __init__(self, raw="the partial answer"):
        self.raw = raw


def _always_rejects(output):
    return False, "not good enough"


class TestDegradeOnExhausted:
    def test_it_rejects_until_the_retries_are_spent(self):
        wrapped = degrade_on_exhausted(_always_rejects, max_retries=2)
        output = _Output()
        assert [wrapped(output)[0] for _ in range(3)] == [False, False, True]

    def test_the_last_attempt_is_accepted_not_raised(self):
        """The run continues with the best attempt rather than dying."""
        wrapped = degrade_on_exhausted(_always_rejects, max_retries=1)
        output = _Output()
        wrapped(output)
        ok, value = wrapped(output)
        assert ok is True
        assert value is output

    def test_a_passing_guardrail_is_untouched(self):
        wrapped = degrade_on_exhausted(lambda o: (True, "fine"), max_retries=2)
        assert wrapped(_Output()) == (True, "fine")

    def test_the_output_says_it_is_soft_in_both_forms(self):
        """Text for a reader, structure for a recipe gate or the A2UI composer.

        Text alone would let an automated consumer treat a degraded answer as a
        clean one.
        """
        wrapped = degrade_on_exhausted(_always_rejects, max_retries=0)
        output = _Output()
        wrapped(output)
        assert DEGRADED_MARKER in output.raw
        assert output.degraded is True
        assert output.degradation_reason

    def test_the_marker_is_not_appended_twice(self):
        wrapped = degrade_on_exhausted(_always_rejects, max_retries=0)
        output = _Output()
        wrapped(output)
        wrapped(output)
        assert output.raw.count(DEGRADED_MARKER) == 1

    def test_a_guardrail_that_raises_still_raises(self):
        """Degrading is for a REJECTED output, not for a broken guardrail."""

        def broken(output):
            raise RuntimeError("the judge itself failed")

        with pytest.raises(RuntimeError):
            degrade_on_exhausted(broken, max_retries=2)(_Output())


class TestItIsWiredIntoTheTask:
    def _task(self, **overrides):
        harness = binding_for("crewai")
        agent = harness.build_agent(
            role="R", goal="G", backstory="B", llm=LLM(model="gpt-4o")
        )
        kwargs = dict(
            description="d",
            expected_output="o",
            agent=agent,
            guardrail=_always_rejects,
            max_retries=2,
        )
        kwargs.update(overrides)
        return harness.build_task(**kwargs)

    def test_degrade_wraps_the_guardrail(self):
        task = self._task(guardrail_on_exhausted="degrade")
        output = _Output()
        assert [task.guardrail(output)[0] for _ in range(3)] == [False, False, True]

    def test_the_default_policy_leaves_the_guardrail_strict(self):
        """Untouched paths must keep aborting — both harnesses default to raise."""
        task = self._task()
        output = _Output()
        assert [task.guardrail(output)[0] for _ in range(3)] == [False, False, False]

    def test_crewai_accepts_the_wrapped_guardrail(self):
        """CrewAI validates the callable's signature at construction.

        It reads the return ANNOTATION via `inspect.signature`, so a wrapper
        annotated under `from __future__ import annotations` is rejected with a
        message describing the very annotation it carries.
        """
        assert self._task(guardrail_on_exhausted="degrade").guardrail is not None
