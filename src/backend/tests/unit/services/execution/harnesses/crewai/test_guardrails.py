"""The degrade-on-exhausted policy, on a CrewAI task.

``guardrail_on_exhausted="degrade"`` is set automatically by the generated
research and deep answer modes, for a reason recorded where it is set: losing a
six-task run because task four could not satisfy a judge on the third attempt
throws away everything already produced.

CrewAI has no such field, so without this the same crew that degrades on one
harness aborts on the other — and it would surface as a research run that simply
failed, with nothing pointing at the harness setting as the cause.
"""

from unittest.mock import patch

import pytest

from src.core.llm.transport.llm import LLM
from src.services.execution.harnesses import binding_for, reset_for_tests
from src.services.execution.harnesses.crewai.guardrails import (
    DEGRADED_MARKER,
    adapt_guardrail,
    degrade_on_exhausted,
)
from src.services.execution.kernel.output_contract import apply_output_schema


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


class TestSchemaGuardrailExecution:
    """Exercise validation AND CrewAI's source-inspecting trace event (run 401)."""

    def _task(self, harness_name, plural=False, **overrides):
        harness = binding_for(harness_name)
        agent = harness.build_agent(
            role="Colour",
            goal="Return a colour",
            backstory="B",
            llm=LLM(model="gpt-4o"),
        )
        args = dict(description="Return black", expected_output="JSON", agent=agent)
        guardrail = apply_output_schema(
            args,
            {
                "output_schema_name": "ColourOutput",
                "output_schema": {
                    "type": "object",
                    "properties": {"colour": {"type": "string", "enum": ["black"]}},
                    "required": ["colour"],
                },
            },
            "colour-task",
        )
        args["guardrails" if plural else "guardrail"] = (
            [guardrail, guardrail] if plural else guardrail
        )
        return harness.build_task(**{**args, **overrides})

    @pytest.mark.parametrize("plural", [False, True])
    @pytest.mark.parametrize("raw,valid", [('{"colour":"black"}', True), ("{}", False)])
    def test_real_crewai_validation_emits_events(self, plural, raw, valid):
        from crewai.tasks.task_output import TaskOutput
        from crewai.utilities.guardrail import process_guardrail

        task = self._task("crewai", plural)
        output = TaskOutput(description="colour", raw=raw, agent="Colour")
        guards = task.guardrails if plural else [task.guardrail]
        with patch("crewai.events.event_bus.crewai_event_bus.emit") as emit:
            for guard in guards:
                result = process_guardrail(output, guard, 0, from_task=task)
                assert result.success is valid
                if valid:
                    assert result.result == output.raw
                else:
                    assert "colour" in result.error
        events = [call.args[1] for call in emit.call_args_list]
        assert len(events) == 2 * len(guards)
        assert events[0].guardrail_name == "SchemaGateGuardrail"
        assert events[-1].success is valid

    @pytest.mark.parametrize("harness_name", ["kasal", "crewai"])
    @pytest.mark.parametrize("plural", [False, True])
    def test_task_executes_schema_validation(self, harness_name, plural):
        task = self._task(harness_name, plural)
        with patch.object(
            type(task.agent), "execute_task", return_value='{"colour":"black"}'
        ):
            output = task.execute_sync()
        assert output.json_dict == {"colour": "black"}

    def test_existing_function_and_description_are_unchanged(self):
        assert adapt_guardrail(_always_rejects) is _always_rejects
        assert adapt_guardrail("Check the answer") == "Check the answer"

    @pytest.mark.parametrize("plural", [False, True])
    def test_rejected_output_is_retried_before_completing(self, plural):
        task = self._task("crewai", plural, max_retries=1)
        with patch.object(
            type(task.agent), "execute_task", side_effect=["{}", '{"colour":"black"}']
        ) as execute:
            output = task.execute_sync()
        assert execute.call_count == 2
        assert output.json_dict == {"colour": "black"}

    def test_invalid_output_still_fails_when_retries_are_exhausted(self):
        task = self._task("crewai", max_retries=0)
        with patch.object(type(task.agent), "execute_task", return_value="{}"):
            with pytest.raises(Exception, match="guardrail"):
                task.execute_sync()
