"""Tests for the cooperative in-process stop signal (src.core.execution_stop)
and its enforcement point in the transport round loop (budget.check_stopped).

Subprocess runs die on SIGTERM; these cover the IN-PROCESS paths (chat light
agents, thread-executor crews), where Stop used to be consulted only after
kickoff returned.
"""

import asyncio
import threading

import pytest

from src.core.execution_stop import (
    bind_stop_event,
    discard_stop_event,
    request_stop,
    reset_stop_event,
    stop_event_for,
    stop_requested,
)
from src.core.llm.transport.budget import check_stopped
from src.core.llm.transport.exceptions import ExecutionStoppedError


def test_registry_get_set_discard():
    event = stop_event_for("exec-registry")
    try:
        assert stop_event_for("exec-registry") is event
        assert request_stop("exec-registry") is True
        assert event.is_set()
    finally:
        discard_stop_event("exec-registry")
    # Discarded: the endpoint no longer reaches anything.
    assert request_stop("exec-registry") is False
    discard_stop_event("exec-registry")  # idempotent


def test_request_stop_for_unknown_execution_is_false():
    assert request_stop("never-registered") is False


def test_stop_requested_reflects_only_the_bound_event():
    assert stop_requested() is False  # unbound context (every subprocess)
    event = threading.Event()
    token = bind_stop_event(event)
    try:
        assert stop_requested() is False
        event.set()
        assert stop_requested() is True
    finally:
        reset_stop_event(token)
    assert stop_requested() is False


def test_check_stopped_raises_with_the_partial_answer():
    check_stopped(0, "model-x")  # unbound: never raises
    event = threading.Event()
    event.set()
    token = bind_stop_event(event)
    try:
        with pytest.raises(ExecutionStoppedError) as excinfo:
            check_stopped(
                3, "model-x", [{"role": "assistant", "content": "partial text"}]
            )
    finally:
        reset_stop_event(token)
    assert "stopped by user" in str(excinfo.value)
    assert excinfo.value.partial == "partial text"


def test_binding_flows_into_to_thread():
    """Every runtime path runs the transport loop inside asyncio.to_thread —
    the contextvar binding must follow it there."""
    event = threading.Event()
    event.set()

    async def main():
        token = bind_stop_event(event)
        try:
            return await asyncio.to_thread(stop_requested)
        finally:
            reset_stop_event(token)

    assert asyncio.run(main()) is True
