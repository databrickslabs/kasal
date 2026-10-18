"""A scheduled run's record is masked by the same rule as every other run.

Secrets in the tool configs and the inputs are masked; counts such as
``max_tokens`` are kept as they are, and the run itself gets the real config.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.schemas.execution import CrewConfig
from src.services.scheduling.scheduler import SchedulerService
from src.utils.sensitive_data_utils import REDACTED_PLACEHOLDER

MODULE = "src.services.scheduling.scheduler"
SECRET = "real-secret-value"


def _config():
    return CrewConfig(
        agents_yaml={
            "writer": {
                "role": "Writer",
                "llm": "databricks-llama",
                "max_tokens": 4096,
                "tool_configs": {"PowerBITool": {"client_secret": SECRET}},
            }
        },
        tasks_yaml={"t": {"description": "Write the report", "agent": "writer"}},
        inputs={"topic": "sales", "max_tokens": "4000", "api_key": SECRET},
        model="databricks-llama",
        execution_type="crew",
    )


@pytest.mark.asyncio
async def test_the_record_keeps_counts_and_masks_secrets():
    schedule = MagicMock(id=1, group_id="g-1", created_by_email="u@example.com")
    session = MagicMock(commit=AsyncMock())
    repo = MagicMock(
        find_by_id=AsyncMock(return_value=schedule),
        update_after_execution=AsyncMock(),
    )

    async def smart_session():
        yield session

    with (
        patch(f"{MODULE}.ScheduleRepository", return_value=repo),
        patch(f"{MODULE}.get_smart_db_session", side_effect=lambda: smart_session()),
        patch(f"{MODULE}.ExecutionService") as executions,
        patch(f"{MODULE}.KasalExecutionService"),
        patch(
            "src.services.execution.config.run_freeze.resolve",
            AsyncMock(return_value=None),
        ),
        patch("src.services.execution.config.run_freeze.record"),
    ):
        executions.return_value.generate_execution_name = AsyncMock(
            return_value={"name": "Nightly"}
        )
        executions.create_run_record = AsyncMock()
        executions.run_crew_execution = AsyncMock()
        await SchedulerService(MagicMock()).run_schedule_job(
            1, _config(), datetime.now(timezone.utc)
        )

    stored = executions.create_run_record.await_args.kwargs["inputs"]
    writer = stored["agents_yaml"]["writer"]
    assert writer["max_tokens"] == 4096
    assert writer["tool_configs"]["PowerBITool"]["client_secret"] == (
        REDACTED_PLACEHOLDER
    )
    assert stored["inputs"] == {
        "topic": "sales",
        "max_tokens": "4000",
        "api_key": REDACTED_PLACEHOLDER,
    }

    handed = executions.run_crew_execution.await_args.kwargs["config"]
    assert handed.inputs["api_key"] == SECRET
    assert (
        handed.agents_yaml["writer"]["tool_configs"]["PowerBITool"]["client_secret"]
        == SECRET
    )
