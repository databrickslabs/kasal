"""Tests for execute_with_mlflow_trace_async's trace_label parameter.

Crew executions pass trace_label="crew_kickoff" so they aren't mislabeled as
"flow_kickoff" in MLflow; flows keep the default.
"""
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from src.services.otel_tracing import mlflow_setup


def _ready_result():
    r = MagicMock()
    r.tracing_ready = True
    r.otel_exporter_active = False
    return r


@contextmanager
def _capture_root_trace(store):
    def _factory(name, inputs):
        store["name"] = name
        store["inputs"] = inputs

        @contextmanager
        def _cm():
            yield MagicMock(name="root_span")

        return _cm()

    yield _factory


@pytest.mark.asyncio
async def test_trace_label_used_in_trace_name():
    store = {}
    with _capture_root_trace(store) as factory, \
         patch("src.services.mlflow_tracing_service.start_root_trace", factory), \
         patch.object(mlflow_setup, "set_trace_attributes"), \
         patch.object(mlflow_setup, "extract_trace_outputs", return_value=None):

        async def kickoff():
            return "ok"

        result = await mlflow_setup.execute_with_mlflow_trace_async(
            kickoff_coro_fn=kickoff,
            mlflow_result=_ready_result(),
            flow_config={"run_name": "My Run"},
            trace_label="crew_kickoff",
        )

    assert result == "ok"
    assert store["name"] == "crew_kickoff:My Run"


@pytest.mark.asyncio
async def test_default_trace_label_is_flow_kickoff():
    store = {}
    with _capture_root_trace(store) as factory, \
         patch("src.services.mlflow_tracing_service.start_root_trace", factory), \
         patch.object(mlflow_setup, "set_trace_attributes"), \
         patch.object(mlflow_setup, "extract_trace_outputs", return_value=None):

        async def kickoff():
            return "ok"

        await mlflow_setup.execute_with_mlflow_trace_async(
            kickoff_coro_fn=kickoff,
            mlflow_result=_ready_result(),
            flow_config={"run_name": "My Run"},
        )

    assert store["name"] == "flow_kickoff:My Run"


@pytest.mark.asyncio
async def test_no_tracing_just_awaits_kickoff():
    """When tracing isn't ready, the coroutine is awaited directly (no trace)."""
    async def kickoff():
        return "direct"

    result = await mlflow_setup.execute_with_mlflow_trace_async(
        kickoff_coro_fn=kickoff,
        mlflow_result=None,
        flow_config={"run_name": "X"},
        trace_label="crew_kickoff",
    )
    assert result == "direct"
