"""A stop that lands after a run is admitted but before its process starts.

The run is out of the admission queue but not yet in the executor's
``_running_processes``: ``terminate_execution`` used to find nothing, report
"not found", and the child spawned and ran to completion anyway.

The window no longer contains an ``await`` (the Lakebase env propagation is gone:
children activate Lakebase themselves), so a real stop cannot interleave there
today. ``claim_start`` still guards it, so the stop is injected the way
``terminate_execution`` records it for an admitted-but-unstarted run —
``run_admission.cancel_waiting`` — while the process object is being built,
immediately before ``claim_start``.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.execution.run_admission import run_admission


def _stop_during_prep(executor, execution_id, outcome):
    """Record a stop while the child process object is built, before claim_start."""
    build = executor._ctx.Process

    def process_factory(*args, **kwargs):
        outcome["stop"] = run_admission.cancel_waiting(execution_id)
        return build.return_value

    return patch.object(build, "side_effect", process_factory)


def _mock_context(executor):
    process = MagicMock(pid=4242, exitcode=0)
    process.is_alive.return_value = False
    executor._ctx = MagicMock()
    executor._ctx.Process.return_value = process
    executor._ctx.Queue.return_value = MagicMock()
    return process


def _group_context():
    return MagicMock(primary_group_id="group-1", access_token="token")


@pytest.mark.asyncio
async def test_crew_run_stopped_before_its_process_starts_never_spawns():
    from src.services.agent_builder.process_executor import ProcessCrewExecutor

    executor = ProcessCrewExecutor()
    process = _mock_context(executor)
    outcome = {}
    with (
        _stop_during_prep(executor, "crew-1", outcome),
        patch.object(executor, "_process_log_queue", new_callable=AsyncMock),
    ):
        result = await executor.run_crew_isolated(
            execution_id="crew-1",
            crew_config={"agents": [], "tasks": []},
            group_context=_group_context(),
        )

    assert outcome["stop"] is True  # the stop reported success...
    assert result["status"] == "STOPPED"  # ...and was honoured
    process.start.assert_not_called()
    assert "crew-1" not in executor._running_processes
    assert run_admission.active_count == 0
    assert executor._metrics["active_executions"] == 0


@pytest.mark.asyncio
async def test_flow_run_stopped_before_its_process_starts_never_spawns():
    from src.services.flow_builder.process_executor import ProcessFlowExecutor

    executor = ProcessFlowExecutor()
    process = _mock_context(executor)
    outcome = {}
    with (
        _stop_during_prep(executor, "flow-1", outcome),
        patch.object(executor, "_process_log_queue", new_callable=AsyncMock),
    ):
        result = await executor.run_flow_isolated(
            execution_id="flow-1",
            flow_config={"nodes": [], "edges": [], "flow_config": {}},
            group_context=_group_context(),
            inputs={},
        )

    assert outcome["stop"] is True
    assert result["status"] == "STOPPED"
    process.start.assert_not_called()
    assert "flow-1" not in executor._running_processes
    assert run_admission.active_count == 0
