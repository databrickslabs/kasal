"""A stand-in harness binding, for tests that assert on what a run BUILDS.

Before the harness layer, a test intercepted construction by patching the class
the module imported::

    with patch("src.services.execution.kernel.agent_builder.Agent") as agent_cls:
        ...
        agent_cls.assert_called_once_with(role="...", ...)

Construction now goes through the ACTIVE ENGINE's binding, because which runtime
an agent becomes is an operator setting. So the thing to intercept is the
binding's ``build_agent``, not a class the module no longer names::

    with patched_harness(MODULE) as harness:
        ...
        harness.build_agent.assert_called_once_with(role="...", ...)

The assertion is the same one, made one layer out — which is the point: what the
kernel decides about an agent is harness-neutral, and that is exactly what these
tests are for.

``patched_harness`` takes as many module paths as a test needs and gives them ALL
the same double. That matters: patching two symbols on one module used to be two
independent ``patch`` calls, and doing the equivalent by patching
``active_harness`` twice would leave the outer double orphaned while only the
inner one recorded anything — a test that passes by asserting on a mock nothing
called.
"""

from __future__ import annotations

from contextlib import ExitStack, contextmanager, nullcontext
from typing import Any, Iterator, List
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.execution.harnesses.binding import Capability, HarnessName


async def _configure_llm(
    model_name: str, group_id: str, temperature: Any = None
) -> Any:
    """What both bindings' ``build_llm`` does, imported at call time.

    Late import so a test patching ``LLMManager.configure_kasal_llm`` is the one
    that gets called — binding the name at module import would capture the real
    function and make every such patch a no-op.
    """
    from src.services.llm.manager import LLMManager

    return await LLMManager.configure_kasal_llm(model_name, group_id, temperature)


class HarnessDouble:
    """Every method of ``HarnessBinding``, as a mock a test can assert on.

    The defaults are chosen so a test that only cares about ONE of them still
    runs: ``process`` returns the name it was given and ``adapt_tools`` is the
    identity, so code downstream of an un-asserted call keeps working with a
    real value instead of a MagicMock that compares unequal to everything.
    """

    def __init__(self) -> None:
        self.name = HarnessName.KASAL
        self.version = "test"
        self.build_agent = MagicMock(name="build_agent")
        self.build_task = MagicMock(name="build_task")
        self.build_crew = MagicMock(name="build_crew")
        # Delegates rather than returning a bare mock. Configuring an LLM is
        # NOT harness-specific: both bindings resolve the model through
        # ``LLMManager`` and run the call on Kasal's own transport (that is the
        # point of ``harnesses/crewai/llm.py``). So a test that patches
        # ``LLMManager.configure_kasal_llm`` and asserts on it is asserting
        # something still true, and keeps working unchanged. Assign a fresh
        # ``AsyncMock`` here when a test wants to control the return value.
        self.build_llm = AsyncMock(name="build_llm", side_effect=_configure_llm)
        self.guardrail = MagicMock(name="guardrail")
        self.adapt_tools = MagicMock(
            name="adapt_tools", side_effect=lambda tools: list(tools or [])
        )
        self.process = MagicMock(name="process", side_effect=lambda name: name)
        self.event_bridge = MagicMock(
            name="event_bridge", side_effect=lambda: nullcontext()
        )
        self.capabilities = MagicMock(return_value=frozenset(Capability))
        self.supports = MagicMock(return_value=True)
        self.describe = MagicMock(
            return_value={"name": "kasal", "version": "test", "available": True}
        )


@contextmanager
def patched_harness(*modules: str, double: Any = None) -> Iterator[HarnessDouble]:
    """Make ``active_harness()`` return a double in each of ``modules``.

    ``modules`` are dotted module paths that imported ``active_harness`` — the
    name is patched where it is USED, not where it is defined, so a module that
    imported it directly is unaffected by patching the harnesses package.
    """
    harness = double if double is not None else HarnessDouble()
    with ExitStack() as stack:
        for module in modules:
            stack.enter_context(patch(f"{module}.active_harness", return_value=harness))
        yield harness


def engine_patch_targets(*modules: str) -> List[str]:
    """The ``active_harness`` attribute paths for ``modules``.

    For tests that assemble their own ``patch.multiple`` / ``ExitStack`` and
    need the target string rather than a managed context.
    """
    return [f"{module}.active_harness" for module in modules]


#: ``what`` → the binding attribute that builds it. Two names because the
#: guardrail factory is not called ``build_guardrail`` on the binding: a
#: guardrail is configuration for a task, not a thing the run constructs
#: alongside agents and tasks.
_BUILDER_ATTR = {
    "agent": "build_agent",
    "task": "build_task",
    "crew": "build_crew",
    "llm": "build_llm",
    "guardrail": "guardrail",
}


@contextmanager
def patch_build(module: str, what: str, **mock_kwargs: Any) -> Iterator[MagicMock]:
    """Intercept ONE thing ``module`` builds, yielding the mock that records it.

    The direct replacement for ``patch("<module>.Agent")`` and friends::

        with patch_build(MODULE, "agent") as agent_builder:
            ...
            agent_builder.assert_called_once_with(role="...")

    ``mock_kwargs`` are passed to the mock, so ``return_value=`` and
    ``side_effect=`` carry over from the old call unchanged.

    Nesting on the SAME module reuses the double already installed rather than
    shadowing it. Without that, a block patching both the crew and the task
    builder would leave the outer mock recording nothing while the test asserted
    on it — a green test that checks the wrong object.
    """
    import importlib

    attr = _BUILDER_ATTR[what]
    builder = MagicMock(name=attr, **mock_kwargs)

    installed = getattr(importlib.import_module(module), "active_harness", None)
    existing = getattr(installed, "return_value", None)
    if isinstance(existing, HarnessDouble):
        previous = getattr(existing, attr)
        setattr(existing, attr, builder)
        try:
            yield builder
        finally:
            setattr(existing, attr, previous)
        return

    harness = HarnessDouble()
    setattr(harness, attr, builder)
    with patch(f"{module}.active_harness", return_value=harness):
        yield builder


@contextmanager
def patch_flow_modules(values):
    """Patch flow construction and its extracted route execution boundary."""
    import importlib

    with ExitStack() as stack:
        for name in ("flow_builder", "route_listener"):
            module = importlib.import_module(
                f"src.services.flow_builder.modules.{name}"
            )
            selected = {
                key: value for key, value in values.items() if hasattr(module, key)
            }
            if selected:
                stack.enter_context(patch.multiple(module, **selected))
        yield


# ---------------------------------------------------------------------------
# Fake CrewAIFlow base (real class so type() works)
# ---------------------------------------------------------------------------
class _FakeFlow:
    """Minimal stand-in for crewai.flow.flow.Flow."""

    def __init__(self, **kwargs):
        self.state = {}


# Decorator stubs imitating @start, @listen, @router, and_, or_
def _fake_start(*a, **kw):
    """@start() → identity decorator."""

    def decorator(fn):
        fn._is_start_method = True
        return fn

    return decorator


def _fake_listen(target):
    """@listen(target) → identity decorator."""

    def decorator(fn):
        fn._listen_to = target
        fn._meth = fn  # needed by name-patching code
        return fn

    return decorator


def _fake_router(target):
    """@router(target) → identity decorator."""

    def decorator(fn):
        fn._router_for = target
        fn._meth = fn
        return fn

    return decorator


def _fake_and(*names):
    return ("AND", names)


def _fake_or(*names):
    return ("OR", names)


# ---------------------------------------------------------------------------
# Common patch context
# ---------------------------------------------------------------------------
def _patches():
    """Return a dict of attribute names → values for patch.multiple(MODULE, ...)."""
    return {
        "CrewAIFlow": _FakeFlow,
        "listen": _fake_listen,
        "router": _fake_router,
        "and_": _fake_and,
        "or_": _fake_or,
        "active_harness": MagicMock(return_value=HarnessDouble()),
        "FlowConfigManager": MagicMock(),
        "FlowProcessorManager": MagicMock(),
        "FlowMethodFactory": MagicMock(),
        "create_execution_callbacks": MagicMock(
            return_value=(MagicMock(), MagicMock())
        ),
        "extract_final_answer": MagicMock(return_value="answer"),
        "get_model_context_limits": AsyncMock(return_value=(128000, 16000)),
    }


def _use_engine(p, *, crew=None, task=None):
    """Point ``active_harness`` in ``p`` at a double carrying these builders.

    Mutates the double already in ``p`` rather than installing a fresh one, so
    setting the crew builder and then the task builder leaves BOTH in place —
    two independent entries would have left the first recording nothing while
    the test asserted on it.
    """
    engine = getattr(p.get("active_harness"), "return_value", None)
    if not isinstance(engine, HarnessDouble):
        engine = HarnessDouble()
        p["active_harness"] = MagicMock(return_value=engine)
    if crew is not None:
        engine.build_crew = crew
    if task is not None:
        engine.build_task = task
    return engine
