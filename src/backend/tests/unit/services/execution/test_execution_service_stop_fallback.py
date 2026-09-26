"""stop_execution's fallback for an untracked flow run: ownership, not substrings.

It used to terminate any child of the server whose command line contained the
execution id or its first 8 characters. With an empty id, ``"" in cmdline``
is true for every child: every tenant's running crews and flows, plus the MCP
stdio servers.
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.services.execution.process_tree import (
    terminate_owned_processes as real_terminate_owned,
)
from src.services.execution.service import ExecutionService


def _service():
    with (
        patch("src.services.execution.service.ExecutionNameService"),
        patch("src.services.execution.service.KasalExecutionService"),
    ):
        return ExecutionService(session=None)


def _untracked_everywhere():
    """Neither executor tracks the run; the thread/engine paths find nothing."""
    return (
        patch(
            "src.services.flow_builder.process_executor.process_flow_executor.terminate_execution",
            new=AsyncMock(return_value=False),
        ),
        patch(
            "src.services.agent_builder.process_executor.process_crew_executor.terminate_execution",
            new=AsyncMock(return_value=False),
        ),
        patch(
            "src.services.execution.engine_service.KasalEngineService.cancel_execution",
            new=AsyncMock(return_value=False),
        ),
    )


class TestUntrackedFlowFallback:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "stop_type, graceful", [("graceful", True), ("force", False)]
    )
    async def test_uses_exact_ownership_match(self, stop_type, graceful):
        flow, crew, engine = _untracked_everywhere()
        with (
            flow,
            crew,
            engine,
            patch(
                "src.services.execution.process_tree.terminate_owned_processes",
                return_value=1,
            ) as owned,
            patch("psutil.Process", side_effect=AssertionError("cmdline scan")),
        ):
            result = await _service().stop_execution("exec-1234abcd", stop_type)

        owned.assert_called_once_with("exec-1234abcd", graceful=graceful)
        assert result["status"] == "STOPPED"

    @pytest.mark.asyncio
    async def test_empty_execution_id_stops_nothing(self):
        """The real matcher: an empty id matches no process, and walks none."""
        returned = []

        def spy(execution_id, **kwargs):
            returned.append(real_terminate_owned(execution_id, **kwargs))
            return returned[-1]

        flow, crew, engine = _untracked_everywhere()
        with (
            flow,
            crew,
            engine,
            patch(
                "src.services.execution.process_tree.terminate_owned_processes",
                side_effect=spy,
            ) as owned,
            patch("psutil.Process", side_effect=AssertionError("process lookup")),
        ):
            await _service().stop_execution("", "force")

        owned.assert_called_once_with("", graceful=False)
        assert returned == [0]
