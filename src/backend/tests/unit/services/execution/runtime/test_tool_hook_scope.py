"""A tool hook runs for its own execution, for nobody's, or when none is bound.

The hook list is process-wide. A run subprocess has one execution, so every
hook in it is its own, and nothing binds an execution there: every hook keeps
running, exactly as before. In the server process several chat turns are in
flight at once; each binds its execution for the turn, and a hook registered
for one turn must not run on another turn's tool calls.
"""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.core.execution_scope import bound_execution_id, execution_scope
from src.services.execution.kernel import tool_approval
from src.services.execution.runtime import register_tool_hooks, unregister_tool_hooks
from src.services.execution.runtime.executor import wrap_tool
from src.services.tools.base import BaseTool


class _Echo(BaseTool):
    name: str = "echo"
    description: str = "Echo"

    def _run(self, text: str = "") -> str:
        return text


@pytest.fixture
def fired():
    calls: list = []
    hooks: list = []

    def register(label, execution_id=None, post=False):
        if post:

            def hook(tool, kwargs, result, agent, task):
                calls.append(label)

            register_tool_hooks(post=hook, execution_id=execution_id)
        else:

            def hook(tool, kwargs, agent, task):
                calls.append(label)

            register_tool_hooks(pre=hook, execution_id=execution_id)
        hooks.append((hook, post))

    yield calls, register
    for hook, post in hooks:
        unregister_tool_hooks(**({"post": hook} if post else {"pre": hook}))


def test_with_no_execution_bound_every_hook_runs(fired):
    """The subprocess case: one execution per process, nothing bound."""
    calls, register = fired
    register("owned", execution_id="exec-sub")
    register("unowned")

    assert bound_execution_id() is None
    wrap_tool(_Echo())(text="hi")

    assert calls == ["owned", "unowned"]


def test_a_hook_for_another_execution_is_skipped(fired):
    calls, register = fired
    register("mine", execution_id="exec-a")
    register("theirs", execution_id="exec-b")
    register("everyone")

    with execution_scope("exec-a"):
        wrap_tool(_Echo())(text="hi")

    assert calls == ["mine", "everyone"]


def test_post_hooks_are_scoped_the_same_way(fired):
    calls, register = fired
    register("mine", execution_id="exec-a", post=True)
    register("theirs", execution_id="exec-b", post=True)

    with execution_scope("exec-a"):
        wrap_tool(_Echo())(text="hi")

    assert calls == ["mine"]


def test_the_scope_is_restored_when_it_closes():
    with execution_scope("exec-a"):
        with execution_scope("exec-b"):
            assert bound_execution_id() == "exec-b"
        assert bound_execution_id() == "exec-a"
    assert bound_execution_id() is None


def test_an_unregistered_hook_forgets_its_owner(fired):
    """Re-registering the same callable without an owner makes it global."""
    calls, _ = fired

    def hook(tool, kwargs, agent, task):
        calls.append("hook")

    register_tool_hooks(pre=hook, execution_id="exec-b")
    unregister_tool_hooks(pre=hook)
    register_tool_hooks(pre=hook)
    try:
        with execution_scope("exec-a"):
            wrap_tool(_Echo())(text="hi")
    finally:
        unregister_tool_hooks(pre=hook)

    assert calls == ["hook"]


def test_the_approval_gate_still_fires_in_a_run_subprocess():
    """A subprocess installs its gate and binds nothing: the gate must apply."""
    created = []

    async def create(execution_id, group_id, gate_config):
        created.append((execution_id, group_id))
        return "approval-1"

    async def status(_approval_id):
        return "approved"

    async def noop(*_args):
        return None

    tool = _Echo()
    object.__setattr__(tool, "_approval_policy", {"scope": "call"})
    ctx = SimpleNamespace(primary_group_id="group-sub", group_ids=["group-sub"])
    with (
        patch.object(tool_approval, "_create_approval", create),
        patch.object(tool_approval, "_approval_status", status),
        patch.object(tool_approval, "_restore_running", noop),
        patch.object(tool_approval, "_prior_decision", noop),
        patch("src.services.hitl.notify.notify_input_needed"),
    ):
        uninstall = tool_approval.install_tool_approval_hook("exec-sub", ctx)
        try:
            assert wrap_tool(tool)(text="hi") == "hi"
        finally:
            uninstall()

    assert created == [("exec-sub", "group-sub")]
