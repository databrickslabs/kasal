"""A run's created_at is written as naive UTC, like completed_at.

``completed_at`` (``status.py``) and the column default are UTC, and the
frontend reads both as UTC. Writers that stamped ``created_at`` with the local
``datetime.now()`` made durations read 0s and start times shift by the UTC
offset. The clock below answers ``now()`` and ``utcnow()`` differently, so each
test passes only on the UTC reading whatever the machine's timezone.
"""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.schemas.execution import CrewConfig
from src.services.execution.kasal_service import KasalExecutionService, executions
from src.services.execution.service import ExecutionService

LOCAL = datetime(2026, 9, 27, 14, 0, 0)
UTC_NOW = datetime(2026, 9, 27, 12, 0, 0)


class _Clock(datetime):
    @classmethod
    def now(cls, tz=None):
        return LOCAL

    @classmethod
    def utcnow(cls):
        return UTC_NOW


def _clock(module: str):
    return patch(f"{module}.datetime", _Clock)


@pytest.mark.asyncio
async def test_create_execution_writes_utc():
    service = ExecutionService(session=MagicMock())
    config = CrewConfig(
        agents_yaml={"a": {"role": "worker"}},
        tasks_yaml={"t": {"description": "do it"}},
        execution_type="crew",
    )
    saved = dict(ExecutionService.executions)
    try:
        with (
            _clock("src.services.execution.service"),
            patch(
                "src.services.execution.status.ExecutionStatusService.create_execution",
                new_callable=AsyncMock,
                return_value=True,
            ) as create,
            patch.object(
                ExecutionService, "_run_in_background", new_callable=AsyncMock
            ),
            patch("src.services.execution.service.ExecutionNameService") as names,
        ):
            names.return_value.generate_execution_name = AsyncMock(
                return_value=MagicMock(name="run")
            )
            result = await service.create_execution(config, group_context=None)

        assert create.await_args.args[0]["created_at"] == UTC_NOW
        entry = ExecutionService.executions[result["execution_id"]]
        assert entry["created_at"] == UTC_NOW
    finally:
        ExecutionService.executions.clear()
        ExecutionService.executions.update(saved)


def test_in_memory_defaults_are_utc():
    with (
        _clock("src.services.execution.service"),
        _clock("src.services.execution.kasal_service"),
    ):
        ExecutionService.add_execution_to_memory("utc-1", "RUNNING", "r")
        KasalExecutionService.add_execution_to_memory("utc-1", "RUNNING", "r")
    try:
        assert ExecutionService.executions["utc-1"]["created_at"] == UTC_NOW
        assert executions["utc-1"]["created_at"] == UTC_NOW
    finally:
        ExecutionService.executions.pop("utc-1", None)
        executions.pop("utc-1", None)


@pytest.mark.asyncio
async def test_external_ask_writes_utc():
    from src.services.external.identity import ExternalCaller
    from src.services.external.invocation import ask

    context = MagicMock(group_ids=["g"], group_email="c@example.com")
    caller = ExternalCaller(
        group_context=context, protocol="mcp", identifier="c@example.com"
    )
    with (
        _clock("src.services.external.invocation"),
        patch(
            "src.services.execution.service.ExecutionService.run_crew_execution",
            new=AsyncMock(return_value={"status": "COMPLETED", "result": "ok"}),
        ),
        patch(
            "src.services.execution.status.ExecutionStatusService.create_execution",
            new=AsyncMock(return_value=True),
        ) as create,
    ):
        await ask(caller, "q")

    assert create.await_args.kwargs["execution_data"]["created_at"] == UTC_NOW
