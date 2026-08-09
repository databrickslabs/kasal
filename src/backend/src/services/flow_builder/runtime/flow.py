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
        # signal -> the output it carried. A SET of names was enough to know
        # an AND was satisfied, but not to hand the joining method what the
        # branches produced: only the last-arriving output was forwarded and
        # every earlier branch's work was discarded.
        self._and_progress: dict[str, dict[str, Any]] = {}
        # Signals that cannot arrive this turn — routes a router declared but
        # did not take. Without these an AND join over route listeners waits
        # forever whenever only a subset of routes matched.
        self._unreachable: set[str] = set()
        self._last_output: Any = None
        # Which methods this run is allowed to execute. Empty means "all of
        # them", which is every run that has not asked for a goal — so the
        # default behaviour is exactly what it was.
        self._required: set[str] = set()

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

    def required_for(self, targets: set[str]) -> set[str]:
        """The methods needed to produce ``targets``: the targets and their ancestors.

        Walked from the listener triggers, which already describe every edge the
        builder created. A trigger name that is not a method is a ROUTE name —
        emitted by a router rather than defined as a method — so every router is
        kept regardless: a router costs nothing to run and dropping one would
        remove the branch its target sits behind.

        An empty result means "do not narrow": a target nothing can reach is far
        more likely to be a mistake in the selection than a flow with no path to
        its own crew, and running everything is the safe reading.
        """
        known = set(self._listeners) | set(self._start_methods)
        required: set[str] = set(self._routers)
        stack = [t for t in targets if t in known]
        if not stack:
            return set()
        while stack:
            method = stack.pop()
            if method in required:
                continue
            required.add(method)
            trigger = self._listeners.get(method)
            if not trigger:
                continue
            for name in trigger["methods"]:
                if name in known:
                    stack.append(name)
        # A start method is how the graph begins; if none survived the walk the
        # targets are unreachable and narrowing would run nothing at all.
        if not required & set(self._start_methods):
            return set()
        return required

    def narrow_to(self, targets: set[str]) -> bool:
        """Run only what produces ``targets`` this turn. True when it narrowed."""
        required = self.required_for(targets or set())
        if not required:
            return False
        self._required = required
        return True

    def _may_run(self, name: str) -> bool:
        return not self._required or name in self._required

    def begin_turn(self) -> None:
        """Reset what belongs to a RUN, keeping what belongs to the thread.

        ``_completed`` exists so a listener fires once per run. It was only ever
        cleared in ``__init__``, so a second ``kickoff`` on the same instance
        re-ran the ``@start()`` methods and fired NO listeners — the run
        completed, reported success, and executed part of the graph. That is the
        failure this prevents.

        State is untouched: the whole point of a turn is that it continues from
        the last one.
        """
        self._completed.clear()
        self._scheduled.clear()
        self._and_progress.clear()
        self._unreachable.clear()
        self._method_outputs.clear()
        self._required.clear()

    async def kickoff_async(self, inputs: dict[str, Any] | None = None) -> Any:
        # A second kickoff on this instance is a new TURN, not a resumption of
        # the first one's bookkeeping.
        if self._completed:
            self.begin_turn()

        if inputs:
            restore_id = inputs.get("id")
            if restore_id:
                # Adopt the id BEFORE restoring, and whether or not anything is
                # stored. `_restore_state` returns early when the lineage is
                # empty — which is every thread's first turn — so leaving
                # adoption to it meant turn 1 saved under the random uuid4 it
                # was constructed with, turn 2 restored nothing, and every turn
                # started a fresh lineage.
                self._adopt_state_id(restore_id)
                if self._persistence is not None:
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
                *(
                    self._execute_method(name, [])
                    for name in self._start_methods
                    if self._may_run(name)
                )
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

    def _adopt_state_id(self, state_id: str) -> None:
        """Make this run part of the given lineage."""
        if isinstance(self._state, dict):
            self._state["id"] = state_id
            return
        try:
            self._state.id = state_id
        except Exception:  # noqa: BLE001 — a state without `id` stays as it is
            logger.warning("flow state has no writable id; cannot join thread")

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

        # A state with channels merges through their reducers, so a turn's new
        # message APPENDS to the restored history instead of replacing it. It
        # raises on an unknown channel for the same reason as below.
        merge = getattr(self._state, "merge", None)
        if callable(merge):
            merge(inputs)
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
                # Deliberately NOT gated on `hasattr`. The channels most worth
                # restoring are the ones an earlier turn CREATED — a crew's
                # output stored under its own name, the identity bookkeeping
                # beside it — and none of those exist on a freshly constructed
                # state, so a hasattr gate skipped exactly them. It brought back
                # the conversation (declared fields) and dropped the work, which
                # reads as a working restore: the flow remembers what was said
                # and re-runs every crew whose answer it already had.
                try:
                    setattr(self._state, key, value)
                except (ValueError, AttributeError) as exc:
                    # A state model that forbids extras rejects a channel it
                    # does not declare. Skip that one rather than abandon the
                    # whole restore.
                    logger.debug("could not restore channel %r: %s", key, exc)
            if hasattr(self._state, "id"):
                self._state.id = restore_id

    async def _execute_method(self, name: str, previous_outputs: list[Any]) -> None:
        """Run ``name``, handing it what the methods it listened to produced.

        Takes a LIST because an AND join has several upstreams and the joining
        method is meant to see all of them — the flow builder generates
        ``async def listener_method(self, *results)`` and loops over ``results``,
        storing ``previous_output_0``, ``previous_output_1``, … Passing a single
        value meant only the last-arriving branch survived, so a crew joining a
        politics and a sports branch drafted from one of them.

        VAR_POSITIONAL also has to COUNT as a parameter. Excluding it meant
        ``*results`` methods were called with no arguments at all, so every
        listener node logged "No previous outputs received" and ran with none of
        its upstream crew's work. Route listeners took a NAMED parameter, which
        is why only the plain listener nodes were starved.
        """
        method = getattr(self, name)
        parameters = [
            p
            for p in inspect.signature(method).parameters.values()
            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD, p.VAR_POSITIONAL)
        ]
        if not parameters:
            args: tuple[Any, ...] = ()
        elif any(p.kind is p.VAR_POSITIONAL for p in parameters):
            args = tuple(previous_outputs)
        else:
            # A single named parameter can only hold one; give it the first.
            args = (previous_outputs[0] if previous_outputs else None,)
        result = method(*args)
        if inspect.isawaitable(result):
            result = await result

        self._method_outputs.append(result)
        self._last_output = result
        self._completed.add(name)
        self._save_state(name)

        emitted = [name]
        if name in self._routers:
            taken = (
                [result]
                if isinstance(result, str)
                else [r for r in (result or []) if isinstance(r, str)]
            )
            # Every route this router could have taken but did not can never
            # signal, so an AND join over its route listeners must stop waiting
            # for it. The builder records the full set on the method.
            declared = getattr(getattr(self, name), "_kasal_routes", None) or []
            self._unreachable.update(set(declared) - set(taken))
            self._spread_unreachable()
            # A router's return value is a SIGNAL: route listeners register as
            # @listen(<route name>), so emitting the name it chose is what runs
            # that branch.
            #
            # A LIST of names emits several, so every route whose condition held
            # runs. Returning one name could only ever fire one branch, and on a
            # batch that genuinely contains both politics and sports the loser
            # was decided by dict ordering — silently, and never revisited.
            # Prior art agrees this should be plural: LangGraph conditional edges
            # return a Sequence, Airflow's branch operator returns a list of
            # task_ids, n8n's Switch has "send to all matching outputs".
            if isinstance(result, str):
                emitted.append(result)
            elif isinstance(result, (list, tuple)):
                emitted.extend(route for route in result if isinstance(route, str))
        # Concurrently, not one after another. Awaiting each signal in turn ran
        # a whole branch — crew and all — before the next route was even
        # emitted, so two matching routes executed SEQUENTIALLY and anything
        # joining on them with OR fired after the first finished, while the
        # second had not started. Branches are independent by construction; the
        # only ordering between them was an artefact of this loop.
        await asyncio.gather(
            *(self._fire_listeners(signal, result) for signal in emitted)
        )

    def _spread_unreachable(self) -> None:
        """Close the unreachable set over the listener graph.

        A router marks the ROUTES it did not take, but a join waits on METHOD
        names — the route listeners themselves. A listener none of whose
        triggers can arrive can never run, so its own name is unreachable too,
        and that propagates down the chain behind it. Without this the join
        still waited for a branch that had already been ruled out.
        """
        changed = True
        while changed:
            changed = False
            for listener, trigger in self._listeners.items():
                if listener in self._unreachable or listener in self._completed:
                    continue
                methods = set(trigger["methods"])
                # Dead only when NOTHING it listens to can arrive. An AND with
                # some reachable members is still satisfiable over those.
                if methods and methods <= self._unreachable:
                    self._unreachable.add(listener)
                    changed = True

    async def _fire_listeners(self, signal: str, output: Any) -> None:
        ready: list[str] = []
        for listener, trigger in self._listeners.items():
            if listener in self._completed or listener in self._scheduled:
                continue
            if not self._may_run(listener):
                # Outside this turn's required set — not needed to produce what
                # was asked for, so it does not run at all.
                continue
            if signal not in trigger["methods"]:
                continue
            if trigger["type"] == _OR:
                ready.append((listener, [output]))
            else:
                seen = self._and_progress.setdefault(listener, {})
                seen[signal] = output
                awaited = set(trigger["methods"]) - self._unreachable
                if set(seen) >= awaited:
                    # In trigger order, so the joining method sees its inputs in
                    # the order the flow declares them rather than whichever
                    # branch happened to finish first.
                    ready.append(
                        (
                            listener,
                            [seen[m] for m in trigger["methods"] if m in seen],
                        )
                    )
        self._scheduled.update(name for name, _ in ready)
        await asyncio.gather(
            *(self._execute_method(listener, outputs) for listener, outputs in ready)
        )

    def save_checkpoint(self, label: str = "turn_end") -> None:
        """Write the current state to the attached persistence.

        Public because a turn does not end at its last method: the caller
        records the answer and bounds the history AFTER the graph finishes, and
        those edits have to reach the checkpoint the next turn restores from.
        """
        self._save_state(label)

    def _save_state(self, method_name: str) -> None:
        if self._persistence is None:
            # No persistence attached — nothing is being written, and nothing
            # claims to be. Emitting here would put a checkpoint on the trace
            # for a flow that has none.
            return
        flow_uuid = self.flow_uuid
        if not flow_uuid:
            return
        state_data = dict(self._state) if isinstance(self._state, dict) else self._state
        try:
            self._persistence.save_state(flow_uuid, method_name, state_data)
        except Exception as exc:  # noqa: BLE001 — reported, never fatal
            logger.exception("flow persistence save_state failed for %s", method_name)
            self._emit_checkpoint_saved(method_name, flow_uuid, error=str(exc))
            return
        self._emit_checkpoint_saved(method_name, flow_uuid)

    def _emit_checkpoint_saved(
        self, method_name: str, flow_uuid: str, error: str | None = None
    ) -> None:
        """Put a checkpoint WRITE on the trace, succeeded or failed.

        The restore side was already traced and the write side was not, so the
        half a resume depends on was the invisible half: with nothing in
        ``flow_states`` you could not tell a checkpoint that was never written
        from one that was written and ignored, without querying the database.

        A failed write especially: it does not fail the run, so without a row
        here it is silent — and every later turn quietly starts from scratch,
        which looks exactly like a flow that simply has no memory.

        Fail-open. A trace row is never worth failing a run for.
        """
        try:
            from src.core.events.types import FlowCheckpointSavedEvent

            event_bus.emit(
                self,
                FlowCheckpointSavedEvent(
                    flow_name=type(self).__name__,
                    method_name=method_name,
                    flow_uuid=flow_uuid,
                    error=error,
                ),
            )
        except Exception:  # noqa: BLE001
            logger.debug("could not emit checkpoint-saved event", exc_info=True)

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
