"""Flow — event-driven method orchestration (crewAI flow DSL).

Authored module; surface validated against the kasal_engine datamodel.
Supports the DSL kasal's flow_builder generates dynamically: ``@start()``,
``@listen(condition)``, ``@router(condition)``, ``and_()``/``or_()``,
class-level ``@persist``. State is a dict (unstructured, auto-carries an
``id``) or a pydantic model with an ``id`` field; ``kickoff_async`` with
``{"id": ...}`` in inputs restores state from the attached persistence
(kasal's checkpoint-resume path).
"""

import asyncio
import inspect
import logging
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any, ClassVar, Generic, TypeVar

from pydantic import BaseModel

from src.core.events.bus import event_bus
from src.core.events.types import FlowFinishedEvent, FlowStartedEvent

logger = logging.getLogger(__name__)

T = TypeVar("T")

_AND = "AND"
_OR = "OR"


def _names(condition: Any) -> list[str]:
    if isinstance(condition, str):
        return [condition]
    if callable(condition):
        return [condition.__name__]
    if isinstance(condition, dict):
        return list(condition["methods"])
    raise TypeError(f"Unsupported flow condition: {condition!r}")


def _normalize(condition: Any) -> dict[str, Any]:
    if isinstance(condition, dict) and condition.get("type") in (_AND, _OR):
        return condition
    return {"type": _OR, "methods": _names(condition)}


def and_(*conditions: Any) -> dict[str, Any]:
    methods: list[str] = []
    for condition in conditions:
        methods.extend(_names(condition))
    return {"type": _AND, "methods": methods}


def or_(*conditions: Any) -> dict[str, Any]:
    methods: list[str] = []
    for condition in conditions:
        methods.extend(_names(condition))
    return {"type": _OR, "methods": methods}


def start(condition: Any = None) -> Callable:
    def decorator(func: Callable) -> Callable:
        func.__is_start_method__ = True
        if condition is not None:
            func.__trigger__ = _normalize(condition)
        func._meth = func  # crewAI decorator-internal kasal renames through
        return func

    return decorator


def listen(condition: Any) -> Callable:
    def decorator(func: Callable) -> Callable:
        func.__trigger__ = _normalize(condition)
        func._meth = func
        return func

    return decorator


def router(condition: Any) -> Callable:
    def decorator(func: Callable) -> Callable:
        func.__trigger__ = _normalize(condition)
        func.__is_router__ = True
        func._meth = func
        return func

    return decorator


class Flow(Generic[T]):
    initial_state: ClassVar[Any] = None

    _start_methods: ClassVar[list[str]]
    _listeners: ClassVar[dict[str, dict[str, Any]]]
    _routers: ClassVar[set[str]]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls._start_methods = []
        cls._listeners = {}
        cls._routers = set()
        for name in dir(cls):
            member = getattr(cls, name, None)
            if not callable(member):
                continue
            if getattr(member, "__is_start_method__", False):
                cls._start_methods.append(name)
            trigger = getattr(member, "__trigger__", None)
            if trigger is not None:
                cls._listeners[name] = trigger
            if getattr(member, "__is_router__", False):
                cls._routers.add(name)

    def __init__(self, persistence: Any = None, **kwargs: Any) -> None:
        self._persistence = persistence or getattr(
            type(self), "_persistence_instance", None
        )
        self._state = self._build_initial_state()
        self._method_outputs: list[Any] = []
        self._completed: set[str] = set()
        self._scheduled: set[str] = set()
        self._and_progress: dict[str, set[str]] = {}
        self._last_output: Any = None

    def _build_initial_state(self) -> Any:
        initial = type(self).initial_state
        if initial is None:
            return {"id": str(uuid.uuid4())}
        if isinstance(initial, type) and issubclass(initial, BaseModel):
            state = initial()
        elif isinstance(initial, BaseModel):
            state = initial.model_copy(deep=True)
        elif isinstance(initial, dict):
            state = dict(initial)
        else:
            raise TypeError(f"Unsupported initial_state: {initial!r}")
        if isinstance(state, dict):
            state.setdefault("id", str(uuid.uuid4()))
        elif not getattr(state, "id", None):
            try:
                state.id = str(uuid.uuid4())
            except Exception:
                pass
        return state

    @property
    def state(self) -> Any:
        return self._state

    @property
    def flow_uuid(self) -> str | None:
        if isinstance(self._state, dict):
            return self._state.get("id")
        return getattr(self._state, "id", None)

    @property
    def method_outputs(self) -> list[Any]:
        return self._method_outputs

    def kickoff(self, inputs: dict[str, Any] | None = None) -> Any:
        return asyncio.run(self.kickoff_async(inputs))

    async def kickoff_async(self, inputs: dict[str, Any] | None = None) -> Any:
        if inputs:
            restore_id = inputs.get("id")
            if restore_id and self._persistence is not None:
                self._restore_state(restore_id)
            self._merge_inputs({k: v for k, v in inputs.items() if k != "id"})

        if not self._start_methods:
            raise ValueError(
                f"{type(self).__name__} has no @start() methods; nothing to run."
            )

        # Flow lifecycle events. These open/close the outermost causality scope
        # (see the bus's _SCOPE_CLOSERS), which is what makes the crew kickoffs
        # this flow drives children of the flow instead of separate roots. Emit
        # around the gather, and always close the scope — an unclosed flow scope
        # would leak into whatever ran next in the same context.
        flow_name = type(self).__name__
        event_bus.emit(self, FlowStartedEvent(flow_name=flow_name, inputs=inputs))
        try:
            await asyncio.gather(
                *(self._execute_method(name, None) for name in self._start_methods)
            )
        except Exception as e:
            event_bus.emit(
                self,
                FlowFinishedEvent(flow_name=flow_name, error=str(e)),
            )
            raise
        event_bus.emit(
            self,
            FlowFinishedEvent(flow_name=flow_name, result=self._last_output),
        )
        return self._last_output

    # ------------------------------ internals ------------------------------

    def _merge_inputs(self, inputs: dict[str, Any]) -> None:
        """Put the run's inputs into flow state.

        A key a TYPED state has no field for used to be skipped in silence.
        Combined with ``evaluate_condition`` swallowing its own errors, one
        misspelled input meant: the value vanished, the condition reading it
        evaluated to False, the flow took the wrong branch, and nothing anywhere
        said so. The run looked like a success.

        So it raises. The caller supplied a value expecting it to matter, and a
        flow that cannot receive it has not run correctly — failing at kickoff,
        naming the key and what the state does hold, costs one clear error
        instead of a plausible wrong answer.

        A DICT state accepts anything by definition, so there is nothing to check
        there; validation for that case belongs before kickoff, against the
        derived schema.
        """
        if not inputs:
            return
        if isinstance(self._state, dict):
            self._state.update(inputs)
            return

        unknown = [key for key in inputs if not hasattr(self._state, key)]
        if unknown:
            known = sorted(
                name for name in vars(self._state) if not name.startswith("_")
            )
            raise ValueError(
                f"Flow state has no field(s) {sorted(unknown)}. "
                f"This state accepts: {known}. "
                "An input the state cannot hold would be dropped silently and "
                "the flow would branch as though it had never been supplied."
            )
        for key, value in inputs.items():
            setattr(self._state, key, value)

    def _restore_state(self, restore_id: str) -> None:
        stored = self._persistence.load_state(restore_id)
        if not stored:
            logger.warning("no persisted state found for flow id %s", restore_id)
            return
        if isinstance(self._state, dict):
            self._state.update(stored)
            self._state["id"] = restore_id
        else:
            for key, value in stored.items():
                if hasattr(self._state, key):
                    setattr(self._state, key, value)
            if hasattr(self._state, "id"):
                self._state.id = restore_id

    async def _execute_method(self, name: str, previous_output: Any) -> None:
        method = getattr(self, name)
        parameters = [
            p
            for p in inspect.signature(method).parameters.values()
            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        ]
        args = (previous_output,) if parameters else ()
        result = method(*args)
        if inspect.isawaitable(result):
            result = await result

        self._method_outputs.append(result)
        self._last_output = result
        self._completed.add(name)
        self._save_state(name)

        emitted = [name]
        if name in self._routers and isinstance(result, str):
            emitted.append(result)
        for signal in emitted:
            await self._fire_listeners(signal, result)

    async def _fire_listeners(self, signal: str, output: Any) -> None:
        ready: list[str] = []
        for listener, trigger in self._listeners.items():
            if listener in self._completed or listener in self._scheduled:
                continue
            if signal not in trigger["methods"]:
                continue
            if trigger["type"] == _OR:
                ready.append(listener)
            else:
                seen = self._and_progress.setdefault(listener, set())
                seen.add(signal)
                if seen >= set(trigger["methods"]):
                    ready.append(listener)
        self._scheduled.update(ready)
        await asyncio.gather(
            *(self._execute_method(listener, output) for listener in ready)
        )

    def _save_state(self, method_name: str) -> None:
        if self._persistence is None:
            return
        flow_uuid = self.flow_uuid
        if not flow_uuid:
            return
        state_data = dict(self._state) if isinstance(self._state, dict) else self._state
        try:
            self._persistence.save_state(flow_uuid, method_name, state_data)
        except Exception:
            logger.exception("flow persistence save_state failed for %s", method_name)

    def plot(self, filename: str = "flow_plot") -> str:
        """Write a plain-text outline of the flow graph; returns the path."""
        lines = [f"Flow: {type(self).__name__}"]
        for name in self._start_methods:
            lines.append(f"  start: {name}")
        for listener, trigger in self._listeners.items():
            kind = "router" if listener in self._routers else "listen"
            joiner = " and " if trigger["type"] == _AND else " or "
            lines.append(f"  {kind}: {listener} <- {joiner.join(trigger['methods'])}")
        path = Path(f"{filename}.txt")
        path.write_text("\n".join(lines) + "\n")
        return str(path)
