"""Task-local context for tool builders; copied by the existing async bridge."""

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Iterator

request_goal: ContextVar[str] = ContextVar("decision_request_goal", default="")


@contextmanager
def task_goal(goal: Any) -> Iterator[None]:
    token = request_goal.set(str(goal or ""))
    try:
        yield
    finally:
        request_goal.reset(token)
