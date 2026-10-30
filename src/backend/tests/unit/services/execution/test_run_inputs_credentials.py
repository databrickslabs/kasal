"""No write path stores the caller's token in ``executionhistory.inputs``, both
input maskers drop one a legacy row still holds, and a resume never replays it.

Only the named credential keys go. ``max_tokens`` and the other numeric
``*_tokens`` settings are configuration a resume must get back unchanged.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.execution.history import ExecutionHistoryService
from src.services.execution.service import ExecutionService
from src.services.execution.status import ExecutionStatusService
from src.services.execution.stored_inputs import (
    RUN_CREDENTIAL_KEYS,
    strip_run_credentials,
)
from src.utils.sensitive_data_utils import REDACTED_PLACEHOLDER
from src.utils.user_context import GroupContext

STORED = "stored-obo-token-value"
FRESH = "fresh-obo-token-value"
CREDENTIAL_KEYS = ("user_token", "access_token", "group_context")
NEW_JOB_ID = "00000000-0000-0000-0000-00000000cafe"
AGENTS = {
    "writer": {
        "role": "Writer",
        "max_tokens": 4096,
        "thinking_budget_tokens": 2048,
        "tool_configs": {"api_key": "tool-key"},
    }
}
TASKS = {"draft": {"description": "Draft it", "agent": "writer"}}


def _legacy_inputs(**extra):
    """Inputs as a flow child stored them before the fix."""
    return {
        "agents_yaml": AGENTS,
        "tasks_yaml": TASKS,
        "inputs": {"topic": "t"},
        "model": "some-model",
        "user_token": STORED,
        "access_token": STORED,
        "group_context": "GroupContext(group_ids=['ws-a'])",
        **extra,
    }


def _assert_clean(inputs):
    for key in CREDENTIAL_KEYS:
        assert key not in inputs, key
    assert STORED not in str(inputs)
    # Only the named keys go.
    assert inputs["agents_yaml"]["writer"]["max_tokens"] == 4096
    assert inputs["agents_yaml"]["writer"]["thinking_budget_tokens"] == 2048
    assert inputs["model"] == "some-model"


class TestStripRunCredentials:
    def test_drops_exactly_the_named_keys(self):
        assert RUN_CREDENTIAL_KEYS == set(CREDENTIAL_KEYS)
        stored = {
            "user_token": STORED,
            "access_token": STORED,
            "group_context": object(),
            "max_tokens": 4096,
            "thinking_budget_tokens": 2048,
            "token_budget": 10,
            "inputs": {"topic": "t"},
        }
        assert strip_run_credentials(stored) == {
            "max_tokens": 4096,
            "thinking_budget_tokens": 2048,
            "token_budget": 10,
            "inputs": {"topic": "t"},
        }
        assert stored["user_token"] == STORED, "a copy, not an edit"

    @pytest.mark.parametrize("value", [None, {}, [], "text"])
    def test_passes_anything_else_through(self, value):
        assert strip_run_credentials(value) == value


class TestBothMaskersDropTheCredentials:
    def test_the_execution_service_masker(self):
        stored = _legacy_inputs()
        masked = ExecutionService(session=MagicMock())._mask_inputs_sensitive_data(
            stored
        )
        _assert_clean(masked)
        assert masked["agents_yaml"]["writer"]["tool_configs"] == {
            "api_key": REDACTED_PLACEHOLDER
        }
        assert stored["user_token"] == STORED, "the row itself is not mutated"

    def test_the_history_masker(self):
        stored = _legacy_inputs()
        masked = ExecutionHistoryService(
            MagicMock(), MagicMock(), MagicMock()
        )._mask_inputs_sensitive_data(stored)
        _assert_clean(masked)
        assert masked["agents_yaml"]["writer"]["tool_configs"] == {
            "api_key": REDACTED_PLACEHOLDER
        }
        assert stored["user_token"] == STORED


class TestEveryRowWriteDropsTheCredentials:
    @pytest.mark.asyncio
    async def test_create_run_record(self):
        """The scheduler, trigger, generation and new-flow-row path."""
        repo = MagicMock()
        repo.insert = AsyncMock(side_effect=lambda run, commit: run)
        with (
            patch(
                "src.repositories.execution_history_repository."
                "ExecutionHistoryRepository",
                return_value=repo,
            ),
            patch(
                "src.services.execution.harness_choice.resolve_run_harness",
                new=AsyncMock(return_value=MagicMock(value="kasal")),
            ),
        ):
            run = await ExecutionService.create_run_record(
                MagicMock(),
                job_id="job-1",
                run_name="r",
                inputs=_legacy_inputs(trigger="queue"),
                execution_type="crew",
                group_id="ws-a",
            )
        _assert_clean(run.inputs)
        assert run.inputs["trigger"] == "queue"

    @pytest.mark.asyncio
    async def test_execution_status_service_create_execution(self):
        """The API create path and the checkpoint resume both land here."""
        repo = MagicMock()
        repo.get_execution_by_job_id = AsyncMock(return_value=None)
        repo.create_execution = AsyncMock()
        session = MagicMock(commit=AsyncMock())
        with (
            patch(
                "src.repositories.execution_repository.ExecutionRepository",
                return_value=repo,
            ),
            patch.object(ExecutionStatusService, "_fill_harness", new=AsyncMock()),
            patch.object(
                ExecutionStatusService, "_broadcast_execution_created", new=AsyncMock()
            ),
        ):
            ok = await ExecutionStatusService.create_execution(
                {"job_id": "job-1", "status": "RUNNING", "inputs": _legacy_inputs()},
                session=session,
            )
        assert ok is True
        _assert_clean(repo.create_execution.await_args.kwargs["data"]["inputs"])


def _source_row(execution_type):
    row = MagicMock()
    row.id = 42
    row.job_id = "job-1"
    row.status = "FAILED"
    row.execution_type = execution_type
    row.run_name = "My Run"
    row.checkpoint_data = None
    row.checkpoint_status = None
    row.flow_uuid = None
    row.flow_id = None
    row.crew_id = None
    row.inputs = _legacy_inputs(nodes=[{"id": "n1"}], edges=[], flow_config={})
    return row


class TestResumeNeverReplaysAStoredToken:
    @pytest.mark.parametrize("execution_type", ["crew", "flow"])
    @pytest.mark.asyncio
    async def test_resume_stores_none_and_runs_on_the_fresh_one(self, execution_type):
        repo = MagicMock()
        repo.get_execution_by_job_id = AsyncMock(
            return_value=_source_row(execution_type)
        )
        repo.set_checkpoint_status = AsyncMock(return_value=True)
        session = MagicMock(commit=AsyncMock())
        resumer = GroupContext(
            group_ids=["ws-a"], group_email="b@example.com", access_token=FRESH
        )
        with (
            patch(
                "src.repositories.execution_history_repository."
                "ExecutionHistoryRepository",
                return_value=repo,
            ),
            patch.object(
                ExecutionService, "_run_in_background", new_callable=AsyncMock
            ) as runner,
            patch(
                "src.services.execution.service.ExecutionStatusService.create_execution",
                new_callable=AsyncMock,
                return_value=True,
            ) as create,
            patch("src.services.execution.service.uuid.uuid4", return_value=NEW_JOB_ID),
        ):
            await ExecutionService(session=session).resume_execution("job-1", resumer)
            await asyncio.sleep(0)
        ExecutionService.executions.pop(NEW_JOB_ID, None)

        _assert_clean(create.await_args.args[0]["inputs"])
        kwargs = runner.call_args.kwargs
        assert kwargs["group_context"].access_token == FRESH
        assert STORED not in str(kwargs["config"].model_dump())
        if execution_type == "crew":
            assert kwargs["config"].agents_yaml["writer"]["max_tokens"] == 4096
