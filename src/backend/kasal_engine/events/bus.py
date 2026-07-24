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
import logging
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from .types import BaseEvent

logger = logging.getLogger(__name__)

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


class CrewAIEventsBus:
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


EventBus = CrewAIEventsBus

crewai_event_bus = CrewAIEventsBus()


class BaseEventListener:
    """Subclass and override setup_listeners() to attach handlers.

    Instantiating a listener registers it on the global bus, matching the
    crewAI behavior kasal relies on; crewai_event_bus.register(listener)
    re-runs setup_listeners and is idempotent for kasal's listeners.
    """

    verbose: bool = False

    def __init__(self) -> None:
        self.setup_listeners(crewai_event_bus)

    def setup_listeners(self, crewai_event_bus: CrewAIEventsBus) -> None:
        raise NotImplementedError(
            f"{type(self).__name__} must implement setup_listeners()"
        )
