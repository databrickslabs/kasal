"""Which execution the current context is working for, in the SERVER process.

A run subprocess (Agent Builder, Flow Builder) holds one execution, so anything
process-wide in it belongs to that run. The server process does not: several
chat turns run in it at once, and the tool hooks they register share one
process-wide list (``runtime/executor``). A turn binds its execution id here for
its whole kickoff, and ``wrap_tool`` runs only the hooks that belong to it.

Nothing binds an id in a run subprocess, and nothing needs to: with no id bound
every hook runs, which is the behaviour there has always been. That is also the
fail-closed reading for any in-process path that loses the context — an
approval gate that cannot tell whose call it is still gates it.

contextvars flow into coroutines and ``asyncio.to_thread``. A plain
``ThreadPoolExecutor.submit`` or ``loop.run_in_executor`` does NOT copy them;
code that hands a tool call to one must pass ``contextvars.copy_context().run``.

Lives in ``core`` beside ``execution_stop`` because the runtime reads it and
``core`` never imports upward.
"""

import contextvars
from contextlib import contextmanager
from typing import Iterator, Optional

_current: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "kasal_current_execution", default=None
)


def bound_execution_id() -> Optional[str]:
    """The execution this context works for, or None when none is bound."""
    return _current.get()


@contextmanager
def execution_scope(execution_id: Optional[str]) -> Iterator[None]:
    """Bind ``execution_id`` for the body; the previous binding comes back after."""
    token = _current.set(execution_id)
    try:
        yield
    finally:
        _current.reset(token)
