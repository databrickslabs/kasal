"""task_builder: inline schema → output_json, plus the gate layers it attaches."""

import pytest
from pydantic import BaseModel

from src.schemas.deep_research import (
    DEEP_RESEARCH_ENVELOPE_SCHEMA,
    DEFAULT_DEEP_GATE,
)
from src.services.execution.kernel.task_builder import build_task_args
from src.services.execution.runtime import Task


def _config(**overrides):
    base = {
        "name": "investigate",
        "description": "Find facts",
        "expected_output": "A report",
    }
    base.update(overrides)
    return base


@pytest.mark.asyncio
class TestInlineSchema:
    async def test_schema_routes_to_output_json_not_output_pydantic(self):
        """These are NOT interchangeable. Downstream context is built from
        ``raw``, and the engine leaves ``raw`` untouched under output_pydantic —
        the validated object lands on ``.pydantic`` and never reaches the next
        task. Only output_json rewrites raw to the JSON dump."""
        args = await build_task_args(
            _config(output_schema=DEEP_RESEARCH_ENVELOPE_SCHEMA), None, []
        )
        assert issubclass(args["output_json"], BaseModel)
        assert "output_pydantic" not in args

    async def test_the_result_actually_constructs_a_task(self):
        args = await build_task_args(
            _config(output_schema=DEEP_RESEARCH_ENVELOPE_SCHEMA), None, []
        )
        task = Task(**args)
        assert task.output_json is not None

    async def test_a_schema_gate_is_attached(self):
        args = await build_task_args(
            _config(output_schema=DEEP_RESEARCH_ENVELOPE_SCHEMA), None, []
        )
        assert "guardrail" in args or "guardrails" in args

    async def test_a_junk_schema_is_ignored_rather_than_fatal(self):
        args = await build_task_args(_config(output_schema="not a schema"), None, [])
        assert "output_json" not in args

    async def test_no_schema_means_no_contract(self):
        args = await build_task_args(_config(), None, [])
        assert "output_json" not in args
        assert "guardrail" not in args and "guardrails" not in args


@pytest.mark.asyncio
class TestGateAndStack:
    async def test_schema_and_gate_stack_cheapest_first(self):
        args = await build_task_args(
            _config(
                output_schema=DEEP_RESEARCH_ENVELOPE_SCHEMA, gate=DEFAULT_DEEP_GATE
            ),
            None,
            [],
        )
        kinds = [type(g.guardrail).__name__ for g in args["guardrails"]]
        assert kinds == ["SchemaGateGuardrail", "DetectionRuleGuardrail"]

    async def test_a_gate_with_no_requirements_attaches_nothing(self):
        args = await build_task_args(_config(gate={"require": []}), None, [])
        assert "guardrail" not in args and "guardrails" not in args

    async def test_a_json_string_gate_is_parsed(self):
        import json

        args = await build_task_args(
            _config(gate=json.dumps(DEFAULT_DEEP_GATE)), None, []
        )
        assert "guardrail" in args


@pytest.mark.asyncio
class TestDegradePolicyPassthrough:
    async def test_policies_reach_the_task(self):
        args = await build_task_args(
            _config(guardrail_on_exhausted="degrade", on_budget_exceeded="degrade"),
            None,
            [],
        )
        task = Task(**args)
        assert task.guardrail_on_exhausted == "degrade"
        assert task.on_budget_exceeded == "degrade"

    async def test_defaults(self):
        """A guardrail rejection is a verdict on the answer and still raises;
        a spent budget degrades — the run keeps the partial, annotated."""
        task = Task(**await build_task_args(_config(), None, []))
        assert task.guardrail_on_exhausted == "raise"
        assert task.on_budget_exceeded == "degrade"

    async def test_an_explicit_raise_for_the_budget_is_honoured(self):
        task = Task(
            **await build_task_args(_config(on_budget_exceeded="raise"), None, [])
        )
        assert task.on_budget_exceeded == "raise"
