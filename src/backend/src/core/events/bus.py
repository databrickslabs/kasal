"""Event bus for the kasal engine.

Authored module; its surface is validated against the kasal_engine datamodel
by generator/validate.py. Compatible with the crewAI event-bus API kasal
uses: on() / emit() / register() / register_handler() / off().

Native requirement #1 (context-carrying events): every BaseEvent has an
`execution_context` dict, and emit() merges the ambient context set via
`event_context(...)` into it. (Named execution_context because several
crewAI event classes already use `context` for event-specific payloads.) Kasal's crewai_patches.py memory-event
injection becomes unnecessary — callers scope their group/tenant/user
context once and every event emitted inside the scope carries it.
"""

import contextvars
import itertools
import logging
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from .types import BaseEvent

logger = logging.getLogger(__name__)

# ── Event causality ─────────────────────────────────────────────────────────
# emit() stamps every event's causality fields (parent_event_id,
# previous_event_id, triggered_by_event_id, started_event_id,
# emission_sequence) so consumers get a true execution DAG instead of a flat,
# timestamp-ordered list. All stamping is only-if-None: an emitter (or a
# replayer) that sets a field explicitly always wins.
#
# Design notes vs the crewAI implementation this replaces:
# - Closing events find their partner by TYPE (searching the scope stack from
#   the top) instead of blindly popping the top frame, so one unbalanced scope
#   (e.g. an llm_call_started whose failure path never emitted a close) cannot
#   corrupt parenting for the rest of the run — orphan frames above the match
#   are unwound and logged instead.
# - Starting events always open a scope, even when the emitter preset
#   parent_event_id.
# - Sequence/previous/triggered_by are also only-if-None, so replayed events
#   keep their recorded causality.

# closing event type -> the starting event type it pairs with
_SCOPE_CLOSERS: dict[str, str] = {
    # Flow is the OUTERMOST scope: a flow run drives one crew kickoff per
    # node, and without this pair every one of those kickoffs is a separate
    # root instead of a child of the flow.
    "flow_finished": "flow_started",
    "crew_kickoff_completed": "crew_kickoff_started",
    "agent_execution_completed": "agent_execution_started",
    "lite_agent_execution_completed": "lite_agent_execution_started",
    "lite_agent_execution_error": "lite_agent_execution_started",
    "task_completed": "task_started",
    "task_failed": "task_started",
    "tool_usage_finished": "tool_usage_started",
    "tool_usage_error": "tool_usage_started",
    "llm_call_completed": "llm_call_started",
    "llm_call_failed": "llm_call_started",
    "memory_save_completed": "memory_save_started",
    "memory_save_failed": "memory_save_started",
    "memory_query_completed": "memory_query_started",
    "memory_query_failed": "memory_query_started",
}
_SCOPE_STARTERS = frozenset(_SCOPE_CLOSERS.values())

# Process-wide monotonic order. itertools.count.__next__ is atomic in CPython,
# so no lock is needed for a plain counter read.
_emission_counter = itertools.count(1)

# Context-local causality state. Contextvars give the same propagation
# behavior as the ambient context above: asyncio tasks branch their own copy
# (concurrent runs don't cross-link), and thread-pool work sees the emitter's
# chain only when the caller copies context in (as the memory save pool does).
_scope_stack: contextvars.ContextVar[tuple[tuple[str, str], ...]] = contextvars.ContextVar(
    "kasal_engine_scope_stack", default=()
)
_last_event_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "kasal_engine_last_event_id", default=None
)
_triggering_event_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "kasal_engine_triggering_event_id", default=None
)


@contextmanager
def triggering_event(event_id: str | None) -> Iterator[None]:
    """Scope a causal trigger: events emitted within get triggered_by_event_id.

    For reactive hops the scope stack can't express — e.g. a flow method firing
    because a listened-to method completed.
    """
    token = _triggering_event_id.set(event_id)
    try:
        yield
    finally:
        _triggering_event_id.reset(token)


def reset_event_causality() -> None:
    """Reset this context's causality chain (run boundaries, tests)."""
    _scope_stack.set(())
    _last_event_id.set(None)
    _triggering_event_id.set(None)


def _stamp_causality(event: BaseEvent) -> None:
    if event.emission_sequence is None:
        event.emission_sequence = next(_emission_counter)
    if event.previous_event_id is None:
        event.previous_event_id = _last_event_id.get()
    if event.triggered_by_event_id is None:
        event.triggered_by_event_id = _triggering_event_id.get()

    etype = event.type
    stack = _scope_stack.get()
    expected_start = _SCOPE_CLOSERS.get(etype)
    if expected_start is not None:
        # Closing event: match the nearest open scope of the paired type.
        match = next(
            (i for i in range(len(stack) - 1, -1, -1) if stack[i][1] == expected_start),
            None,
        )
        if match is None:
            if event.parent_event_id is None and stack:
                event.parent_event_id = stack[-1][0]
            logger.debug("causality: %s closed with no open %s scope", etype, expected_start)
        else:
            for orphan_id, orphan_type in stack[match + 1 :]:
                logger.debug(
                    "causality: unwinding unclosed %s scope (%s) at %s",
                    orphan_type, orphan_id, etype,
                )
            if event.started_event_id is None:
                event.started_event_id = stack[match][0]
            if event.parent_event_id is None and match > 0:
                event.parent_event_id = stack[match - 1][0]
            _scope_stack.set(stack[:match])
    elif etype in _SCOPE_STARTERS:
        if event.parent_event_id is None and stack:
            event.parent_event_id = stack[-1][0]
        _scope_stack.set(stack + ((event.event_id, etype),))
    else:
        # Point event (retrieval, stream chunk, custom types).
        if event.parent_event_id is None and stack:
            event.parent_event_id = stack[-1][0]

    _last_event_id.set(event.event_id)

_ambient_context: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "kasal_engine_event_context", default=None
)


@contextmanager
def event_context(**fields: Any) -> Iterator[dict[str, Any]]:
    """Scope ambient context merged into every event emitted within it."""
    merged = dict(_ambient_context.get() or {})
    merged.update(fields)
    token = _ambient_context.set(merged)
    try:
        yield merged
    finally:
        _ambient_context.reset(token)


def set_event_context(**fields: Any) -> dict[str, Any]:
    """Unscoped update of the ambient context for the current execution context.

    For long-lived publishers (kasal's otel event bridge updates agent/task
    attribution synchronously as events fire, so upcoming memory saves —
    which snapshot contextvars — inherit it). Prefer the scoped
    event_context() manager where a scope exists.
    """
    merged = dict(_ambient_context.get() or {})
    merged.update(fields)
    _ambient_context.set(merged)
    return merged


def current_event_context() -> dict[str, Any]:
    return dict(_ambient_context.get() or {})


# BaseEvent fields back-filled from ambient context when the emitter left
# them unset — the exact semantics of kasal's memory-event patch (explicit
# values always win).
_ATTRIBUTION_FIELDS = ("agent_role", "agent_id", "task_id", "task_name")


Handler = Callable[[Any, BaseEvent], Any]


class EventsBus:
    """Thread-safe, isinstance-dispatched event bus.

    Handler failures are logged and isolated: one broken listener never
    breaks the emitting code path or other listeners.
    """

    def __init__(self) -> None:
        self._handlers: dict[type, list[Handler]] = {}
        self._lock = threading.RLock()

    def on(self, event_type: type) -> Callable[[Handler], Handler]:
        def decorator(handler: Handler) -> Handler:
            self.register_handler(event_type, handler)
            return handler

        return decorator

    def register_handler(self, event_type: type, handler: Handler) -> None:
        with self._lock:
            handlers = self._handlers.setdefault(event_type, [])
            if handler not in handlers:
                handlers.append(handler)

    def off(self, event_type: type, handler: Handler) -> bool:
        with self._lock:
            handlers = self._handlers.get(event_type, [])
            if handler in handlers:
                handlers.remove(handler)
                return True
            return False

    def register(self, listener: "BaseEventListener") -> "BaseEventListener":
        listener.setup_listeners(self)
        return listener

    def emit(self, source: Any, event: BaseEvent) -> None:
        if isinstance(event, BaseEvent):
            _stamp_causality(event)
            # Identity from the event's OWN payload beats ambient context: an
            # event carrying its task/agent object is self-attributing, while
            # the ambient values may still describe the PREVIOUS task (they are
            # refreshed by listeners only AFTER this back-fill runs — that lag
            # once mis-bucketed a second task's whole timeline under the first).
            own_task = getattr(event, "task", None) or getattr(event, "from_task", None)
            if own_task is not None:
                if event.task_id is None and getattr(own_task, "id", None) is not None:
                    event.task_id = str(own_task.id)
                if event.task_name is None:
                    task_name = getattr(own_task, "name", None) or getattr(
                        own_task, "description", None
                    )
                    if task_name:
                        event.task_name = str(task_name)
            own_agent = getattr(event, "agent", None) or getattr(event, "from_agent", None)
            if own_agent is not None:
                if event.agent_id is None and getattr(own_agent, "id", None) is not None:
                    event.agent_id = str(own_agent.id)
                if event.agent_role is None and getattr(own_agent, "role", None):
                    event.agent_role = str(own_agent.role)
            ambient = _ambient_context.get()
            if ambient:
                event.execution_context = {**ambient, **event.execution_context}
                for field in _ATTRIBUTION_FIELDS:
                    if getattr(event, field, None) is None and ambient.get(field) is not None:
                        setattr(event, field, ambient[field])
        with self._lock:
            dispatch = [
                (event_type, list(handlers))
                for event_type, handlers in self._handlers.items()
                if isinstance(event, event_type)
            ]
        for event_type, handlers in dispatch:
            for handler in handlers:
                try:
                    handler(source, event)
                except Exception:
                    logger.exception(
                        "event handler %r failed for %s",
                        getattr(handler, "__qualname__", handler),
                        type(event).__name__,
                    )

    @contextmanager
    def scoped_handlers(self) -> Iterator[None]:
        """Temporarily isolate handler registrations (test helper)."""
        with self._lock:
            saved = {k: list(v) for k, v in self._handlers.items()}
        try:
            yield
        finally:
            with self._lock:
                self._handlers = saved


EventBus = EventsBus

event_bus = EventsBus()


class BaseEventListener:
    """Subclass and override setup_listeners() to attach handlers.

    Instantiating a listener registers it on the global bus, matching the
    crewAI behavior kasal relies on; event_bus.register(listener)
    re-runs setup_listeners and is idempotent for kasal's listeners.
    """

    verbose: bool = False

    def __init__(self) -> None:
        self.setup_listeners(event_bus)

    def setup_listeners(self, event_bus: EventsBus) -> None:
        raise NotImplementedError(
            f"{type(self).__name__} must implement setup_listeners()"
        )
