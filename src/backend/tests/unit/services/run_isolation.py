"""Fixtures for tests that drive the crew/flow executors in-process.

Imported by the ``conftest.py`` of the packages whose tests do that. Two
things leak out of those tests otherwise:

- the shared run gate (``run_admission``): its default limit provider reads
  the engine-config table, and its slot table is process-wide;
- what the child entry points set up for a spawned interpreter:
  ``run_crew_in_process`` / ``run_flow_in_process`` install SIGTERM/SIGINT
  handlers that exit the process, and the executors set
  ``KASAL_EXECUTION_ID``. In-process, both would land on the pytest worker.
"""

import os
import signal
from collections import deque
from unittest.mock import AsyncMock

import pytest

_SIGNALS = (signal.SIGTERM, signal.SIGINT)


@pytest.fixture(autouse=True)
def isolated_run_gate(monkeypatch):
    """A fresh, DB-free run gate at the default limit for every test."""
    from src.services.execution import run_admission as module

    gate = module.run_admission
    monkeypatch.setattr(gate, "_limit_provider", AsyncMock(return_value=None))
    monkeypatch.setattr(gate, "_active", {})
    monkeypatch.setattr(gate, "_waiters", deque())
    monkeypatch.setattr(gate, "_limit", module.DEFAULT_MAX_CONCURRENT_RUNS)
    monkeypatch.setattr(gate, "_limit_read_at", None)
    monkeypatch.setattr(gate, "_limit_known", False)
    monkeypatch.setattr(gate, "_started", set())
    monkeypatch.setattr(gate, "_stop_requested", set())
    yield gate


@pytest.fixture(autouse=True)
def restore_child_process_globals():
    """Put back the signal handlers and KASAL_EXECUTION_ID a test changed."""
    saved_handlers = {sig: signal.getsignal(sig) for sig in _SIGNALS}
    saved_execution_id = os.environ.get("KASAL_EXECUTION_ID")
    try:
        yield
    finally:
        for sig, handler in saved_handlers.items():
            if handler is not None:
                signal.signal(sig, handler)
        if saved_execution_id is None:
            os.environ.pop("KASAL_EXECUTION_ID", None)
        else:
            os.environ["KASAL_EXECUTION_ID"] = saved_execution_id
