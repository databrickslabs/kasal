"""A trigger-run's stored inputs carry no decrypted tool credentials.

The run itself still gets them: a saved crew's tools need the real secret.
"""

import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.triggers.queue_consumer_service import TriggerQueueConsumerService
from src.utils.sensitive_data_utils import REDACTED_PLACEHOLDER

MODULE = "src.services.triggers.queue_consumer_service"
SECRET = "sp-secret-value-123"
PAT = "dapi-pat-value-456"
#: An event input. The run's event context copies unreferenced inputs into the
#: first task's description; a secret one must be masked there.
INPUT_SECRET = "event-input-secret-789"

AGENTS = {
    "analyst": {
        "role": "r",
        "tool_configs": {"Power BI Tool": {"client_secret": SECRET, "tenant_id": "t"}},
    }
}
TASKS = {
    "extract": {
        "description": "d",
        "tool_configs": {"SerperDevTool": {"serper_api_key": SECRET}},
        "callback_config": {"token": PAT, "volume_path": "/Volumes/a/b/c"},
        "config": {"callback_config": {"token": PAT}, "timeout": 30},
    }
}


def _routed(session):
    @asynccontextmanager
    async def _cm():
        yield session

    return lambda: _cm()


async def test_stored_inputs_are_masked_and_the_run_gets_plaintext():
    service = TriggerQueueConsumerService()
    session = MagicMock(commit=AsyncMock())
    repo = MagicMock(mark_dispatched=AsyncMock())
    snap = {
        "id": 3,
        "group_id": "g1",
        "target": {"kind": "crew", "id": "crew-1"},
        "payload": {"inputs": {"topic": "t", "client_secret": INPUT_SECRET}},
        "attempts": 1,
    }
    agents, tasks = json.loads(json.dumps(AGENTS)), json.loads(json.dumps(TASKS))
    with (
        patch(f"{MODULE}.routed_scoped_session", _routed(session)),
        patch(f"{MODULE}.TriggerQueueRepository", return_value=repo),
        patch(
            "src.services.catalog.crew_config.build_crew_execution_config_by_id",
            AsyncMock(return_value=(agents, tasks)),
        ),
        patch.object(service, "_resolve_run_name", AsyncMock(return_value="run")),
        patch(
            "src.services.execution.config.run_freeze.resolve",
            AsyncMock(return_value=None),
        ),
        patch("src.services.execution.config.run_freeze.record"),
        patch(
            "src.services.execution.service.ExecutionService.create_run_record",
            new_callable=AsyncMock,
        ) as create_record,
        patch(
            "src.services.execution.service.ExecutionService.run_crew_execution",
            new_callable=AsyncMock,
        ) as run,
        patch(
            "src.services.execution.status.ExecutionStatusService."
            "broadcast_execution_created",
            new_callable=AsyncMock,
        ),
    ):
        await service._dispatch(snap)

    stored = create_record.await_args.kwargs["inputs"]
    assert SECRET not in json.dumps(stored) and PAT not in json.dumps(stored)
    assert INPUT_SECRET not in json.dumps(stored)
    pbi = stored["agents_yaml"]["analyst"]["tool_configs"]["Power BI Tool"]
    assert pbi == {"client_secret": REDACTED_PLACEHOLDER, "tenant_id": "t"}
    callback = stored["tasks_yaml"]["extract"]["callback_config"]
    assert callback["volume_path"] == "/Volumes/a/b/c"
    assert stored["tasks_yaml"]["extract"]["config"]["timeout"] == 30
    assert stored["inputs"] == {"topic": "t", "client_secret": REDACTED_PLACEHOLDER}

    launched = run.await_args.kwargs["config"]
    assert INPUT_SECRET not in launched.tasks_yaml["extract"]["description"]
    assert launched.agents_yaml == AGENTS
    for field in ("tool_configs", "callback_config", "config"):
        assert launched.tasks_yaml["extract"][field] == TASKS["extract"][field]
    assert launched.inputs["client_secret"] == INPUT_SECRET


def _event_config(inputs):
    from src.schemas.execution import CrewConfig

    return CrewConfig(
        agents_yaml={"a": {"role": "r"}},
        tasks_yaml={"t1": {"description": "Do.", "expected_output": "X"}},
        inputs=inputs,
        execution_type="crew",
    )


def test_a_secret_event_input_is_masked_in_the_description():
    config = _event_config(
        {
            "topic": "sales",
            "max_tokens": "4000",
            "api_key": INPUT_SECRET,
            "payload": {"color": "black", "access_token": INPUT_SECRET},
        }
    )
    TriggerQueueConsumerService()._inject_event_context(config, "crew")

    description = config.tasks_yaml["t1"]["description"]
    assert INPUT_SECRET not in description
    assert f"- api_key: {REDACTED_PLACEHOLDER}" in description
    assert "- topic: sales" in description
    assert "- max_tokens: 4000" in description
    assert '"color": "black"' in description
    # The run still gets every value through its inputs.
    assert config.inputs["api_key"] == INPUT_SECRET
    assert config.inputs["payload"]["access_token"] == INPUT_SECRET
