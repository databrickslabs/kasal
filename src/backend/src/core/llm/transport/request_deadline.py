"""Carry one deadline through nested wrap-up/retry calls and streamed requests."""

import time
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps

from .exceptions import ExecutionBudgetExceededError

_deadline: ContextVar[float | None] = ContextVar("llm_request_deadline", default=None)


def current_deadline():
    return _deadline.get()


@contextmanager
def run_deadline(seconds):
    inherited = _deadline.get()
    deadline = time.monotonic() + seconds if seconds else inherited
    if inherited is not None and deadline is not None:
        deadline = min(inherited, deadline)
    token = _deadline.set(deadline)
    try:
        yield
    finally:
        _deadline.reset(token)


def run_with_deadline(fn):
    @wraps(fn)
    def wrapped(crew, *args, **kwargs):
        seconds = getattr(crew, "run_max_seconds", None) or getattr(
            crew, "_kasal_run_max_seconds", None
        )
        with run_deadline(seconds):
            return fn(crew, *args, **kwargs)

    return wrapped


def async_run_with_deadline(fn):
    @wraps(fn)
    async def wrapped(crew, *args, **kwargs):
        seconds = getattr(crew, "run_max_seconds", None) or getattr(
            crew, "_kasal_run_max_seconds", None
        )
        with run_deadline(seconds):
            return await fn(crew, *args, **kwargs)

    return wrapped


@contextmanager
def call_deadline(agent=None):
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


def check_request_deadline(partial=""):
    deadline = _deadline.get()
    if deadline is not None and time.monotonic() >= deadline:
        raise ExecutionBudgetExceededError(
            "Execution time limit reached.", partial=partial
        )


def bounded_params(params):
    """Bound network inactivity too, so a stalled request cannot hide the cap."""
    check_request_deadline()
    deadline = _deadline.get()
    if deadline is None:
        return params
    return {**params, "timeout": max(0.001, deadline - time.monotonic())}
