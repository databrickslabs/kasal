"""Guards on how the resume skip gate is WIRED, not on what it decides.

``CrewSkipPolicy.may_skip`` is unit-tested directly, which means every test
passes even when the call sites feeding it are wrong. Two real bugs got in that
way and neither turned a test red:

1. ``checkpoint_identities`` was read inside ``_create_dynamic_flow`` but only
   assigned in its caller — a NameError on every flow resume.
2. The listener call site passed ``crew_tasks``, which belongs to the
   STARTING-POINT loop, so it hashed a different crew's tasks. Silently wrong:
   no error, just the wrong verdict.

Both are invisible to behavioural tests of the gate itself, so they are checked
structurally here.
"""

import ast
import inspect
from pathlib import Path

import pytest

from src.services.flow_builder.modules import flow_builder as flow_builder_module
from src.services.flow_builder.modules.flow_builder import FlowBuilder

SOURCE = Path(inspect.getfile(flow_builder_module)).read_text()
TREE = ast.parse(SOURCE)


def _function(name):
    for node in ast.walk(TREE):
        if (
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == name
        ):
            return node
    raise AssertionError(f"{name} not found")


class TestIdentitiesAreInScope:
    def test_checkpoint_identities_is_a_parameter_not_a_global(self):
        """The NameError guard.

        If the name is resolved globally rather than as a local, it is not in
        scope where it is used — which is precisely the bug, and it only
        surfaces on a real resume.
        """
        fn = FlowBuilder._create_dynamic_flow
        code = getattr(fn, "__func__", fn).__code__

        assert "checkpoint_identities" in code.co_varnames
        assert "checkpoint_identities" not in code.co_names

    def test_the_caller_actually_passes_it(self):
        """A parameter nobody supplies is a silent None, so verification would
        quietly degrade to 'unverified' for every crew."""
        passed = [
            call.lineno
            for call in ast.walk(_function("build_flow"))
            if isinstance(call, ast.Call)
            and any(
                isinstance(kw, ast.keyword) and kw.arg == "checkpoint_identities"
                for kw in call.keywords
            )
        ]
        assert passed, "build_flow never passes checkpoint_identities"


class TestEachSiteHashesItsOwnTasks:
    """Each skip site must hash the tasks of the crew it is deciding about.

    Every skip site sits inside a `for` loop over one kind of flow node, and the
    loop unpacks that node's own task list. Sibling loops run in the SAME
    function, so a variable from an earlier loop is still bound at a later one —
    passing it compiles, runs, and silently compares the wrong crew.

    Rather than hard-code which variable belongs to which site, this derives it:
    the tasks argument must be a name that the site's OWN enclosing loop binds.
    That way a skip site added later is checked automatically, instead of only
    tripping a counter that someone can silence.
    """

    @staticmethod
    def _parents():
        parents = {}
        for node in ast.walk(_function("_create_dynamic_flow")):
            for child in ast.iter_child_nodes(node):
                parents[child] = node
        return parents

    @classmethod
    def _enclosing_loop(cls, node, parents):
        while node in parents:
            node = parents[node]
            if isinstance(node, (ast.For, ast.AsyncFor)):
                return node
        return None

    @staticmethod
    def _names_bound_by(loop):
        """Names this loop binds — its target, plus anything assigned in it.

        Nested loops are included: a name bound deeper inside the same loop
        still belongs to this iteration, unlike one from a sibling loop.
        """
        bound = set()
        for node in ast.walk(loop):
            targets = []
            if isinstance(node, (ast.For, ast.AsyncFor)):
                targets = [node.target]
            elif isinstance(node, ast.Assign):
                targets = node.targets
            for target in targets:
                for name in ast.walk(target):
                    if isinstance(name, ast.Name):
                        bound.add(name.id)
        return bound

    @classmethod
    def _skip_gate_calls(cls):
        parents = cls._parents()
        calls = []
        for node in ast.walk(_function("_create_dynamic_flow")):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "may_skip"
            ):
                # (crew_name, <tasks>, sequence)
                assert len(node.args) == 3, "unexpected skip-gate signature"
                calls.append(
                    (node, node.args[1].id, cls._enclosing_loop(node, parents))
                )
        return calls

    def test_there_is_at_least_one_skip_site(self):
        """If this fails the gate was removed, and every resume is unverified."""
        assert self._skip_gate_calls()

    def test_every_site_hashes_tasks_bound_by_its_own_loop(self):
        for call, tasks_var, loop in self._skip_gate_calls():
            assert loop is not None, (
                f"skip site at line {call.lineno} is not inside a loop; "
                f"this guard cannot tell which crew it belongs to"
            )
            bound = self._names_bound_by(loop)
            assert tasks_var in bound, (
                f"skip site at line {call.lineno} passes '{tasks_var}', which its "
                f"own loop (line {loop.lineno}) never binds — it has leaked in "
                f"from a sibling loop and is hashing a different crew's tasks. "
                f"Names this loop binds: {sorted(bound)}"
            )

    def test_no_two_sites_share_a_tasks_variable(self):
        """Two sites on one variable means at least one is hashing the wrong crew."""
        used = [tasks_var for _, tasks_var, _ in self._skip_gate_calls()]
        assert len(used) == len(set(used)), f"skip sites share a tasks variable: {used}"
