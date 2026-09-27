"""Carry one deadline through nested wrap-up/retry calls and streamed requests."""

import time
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from typing import Any, Awaitable, Callable, Dict, Iterator, TypeVar, cast

from .exceptions import ExecutionBudgetExceededError

_F = TypeVar("_F", bound=Callable[..., Any])
_AF = TypeVar("_AF", bound=Callable[..., Awaitable[Any]])

_deadline: ContextVar[float | None] = ContextVar("llm_request_deadline", default=None)


def current_deadline() -> float | None:
    return _deadline.get()


@contextmanager
def run_deadline(seconds: float | None) -> Iterator[None]:
    inherited = _deadline.get()
    deadline = time.monotonic() + seconds if seconds else inherited
    if inherited is not None and deadline is not None:
        deadline = min(inherited, deadline)
    token = _deadline.set(deadline)
    try:
        yield
    finally:
        _deadline.reset(token)


def run_with_deadline(fn: _F) -> _F:
    @wraps(fn)
    def wrapped(crew: Any, *args: Any, **kwargs: Any) -> Any:
        seconds = getattr(crew, "run_max_seconds", None) or getattr(
            crew, "_kasal_run_max_seconds", None
        )
        with run_deadline(seconds):
            return fn(crew, *args, **kwargs)

    return cast(_F, wrapped)


def async_run_with_deadline(fn: _AF) -> _AF:
    @wraps(fn)
    async def wrapped(crew: Any, *args: Any, **kwargs: Any) -> Any:
        seconds = getattr(crew, "run_max_seconds", None) or getattr(
            crew, "_kasal_run_max_seconds", None
        )
        with run_deadline(seconds):
            return await fn(crew, *args, **kwargs)

    return cast(_AF, wrapped)


@contextmanager
def call_deadline(agent: Any = None) -> Iterator[None]:
    from .budget import resolve_execution_budget

    _, deadline = resolve_execution_budget(agent)
    inherited = _deadline.get()
    if inherited is not None:
        deadline = min(deadline, inherited) if deadline is not None else inherited
    token = _deadline.set(deadline)
    try:
        yield
    finally:
        _deadline.reset(token)


def check_request_deadline(partial: str = "") -> None:
    deadline = _deadline.get()
    if deadline is not None and time.monotonic() >= deadline:
        raise ExecutionBudgetExceededError(
            "Execution time limit reached.", partial=partial
        )


def bounded_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """Bound network inactivity too, so a stalled request cannot hide the cap."""
    check_request_deadline()
    deadline = _deadline.get()
    if deadline is None:
        return params
    return {**params, "timeout": max(0.001, deadline - time.monotonic())}
