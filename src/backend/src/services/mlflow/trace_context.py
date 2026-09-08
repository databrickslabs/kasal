"""Bind hosted UC tracing to the requesting async task, never another teamspace.

MLflow's public set_destination rejects UnityCatalog table-prefix locations.
set_experiment uses the destination registry internally but writes its global
slot. Keep this compatibility seam in one place and exercise it against the
installed SDK: its context-local slot is what the UC processor and exporter read.
"""

from contextvars import ContextVar
from typing import Any

_destination: ContextVar[Any] = ContextVar(
    "kasal_hosted_trace_destination", default=None
)


def bind_destination(destination: Any) -> None:
    if destination is not None:
        from mlflow.tracing.provider import _MLFLOW_TRACE_USER_DESTINATION

        _MLFLOW_TRACE_USER_DESTINATION.set(destination, context_local=True)
    _destination.set(destination)


def has_destination() -> bool:
    return _destination.get() is not None
