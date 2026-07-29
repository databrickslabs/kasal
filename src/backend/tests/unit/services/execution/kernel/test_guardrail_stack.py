"""Guardrail stack ordering — order is cost.

The engine runs the stack in order with a retry budget each, so an expensive
check that runs before a free one pays for output the free one would have
rejected.
"""

from src.services.execution.kernel.guardrail_stack import build_guardrail_stack


def _g(name):
    def guardrail(output):
        return (True, output)

    guardrail.__qualname__ = name
    return guardrail


class TestOrdering:
    def test_cheapest_first(self):
        args = {}
        build_guardrail_stack(
            args,
            [
                ("human", _g("human")),
                ("detection", _g("detection")),
                ("schema", _g("schema")),
            ],
            "t1",
        )
        assert [g.__qualname__ for g in args["guardrails"]] == [
            "schema",
            "detection",
            "human",
        ]

    def test_an_existing_singular_guardrail_is_absorbed_as_content(self):
        """The LLM judge and factory guardrails land on the singular key; they
        belong after the free checks and before the human."""
        args = {"guardrail": _g("judge")}
        build_guardrail_stack(
            args, [("human", _g("human")), ("schema", _g("schema"))], "t1"
        )
        assert [g.__qualname__ for g in args["guardrails"]] == [
            "schema",
            "judge",
            "human",
        ]
        assert "guardrail" not in args

    def test_unknown_kinds_sort_between_free_checks_and_the_human(self):
        args = {}
        build_guardrail_stack(
            args,
            [
                ("human", _g("human")),
                ("mystery", _g("mystery")),
                ("schema", _g("schema")),
            ],
            "t1",
        )
        assert [g.__qualname__ for g in args["guardrails"]] == [
            "schema",
            "mystery",
            "human",
        ]


class TestSingleAndEmpty:
    def test_a_lone_guardrail_keeps_the_singular_key(self):
        """The crew path wires a fallback callback off `'guardrail' in
        task_args`; a single guardrail must not silently stop triggering it."""
        args = {}
        build_guardrail_stack(args, [("schema", _g("schema"))], "t1")
        assert args["guardrail"].__qualname__ == "schema"
        assert "guardrails" not in args

    def test_nothing_to_stack_leaves_the_args_alone(self):
        args = {"description": "d"}
        build_guardrail_stack(args, [], "t1")
        assert args == {"description": "d"}

    def test_none_entries_are_dropped(self):
        args = {}
        build_guardrail_stack(args, [("schema", None), ("human", _g("human"))], "t1")
        assert args["guardrail"].__qualname__ == "human"
