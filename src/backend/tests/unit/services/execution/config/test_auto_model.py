"""The choke point: "auto" never survives ExecutionService.create_execution.

What leaves ``create_execution`` (the history row, the config handed to the
background runner and from there to the crew or flow subprocess) must hold a
concrete model key.
"""

import asyncio
import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.schemas.execution import CrewConfig
from src.services.decisions.model_selection import ModelSelection, current_selection
from src.services.execution.config import auto_model
from src.services.execution.config.auto_model import (
    apply_selection,
    record_selection,
    resolve_run_models,
    run_prompt,
    selection_for,
    wants_auto,
)
from src.services.execution.service import ExecutionService

PICK = ModelSelection("databricks-claude-opus-5-5", "selected", 3.0)


def auto_config(**overrides) -> CrewConfig:
    fields = dict(
        agents_yaml={
            "writer": {"role": "writer", "llm": "auto"},
            "checker": {"role": "checker", "llm": {"model": "auto", "temperature": 0}},
            "fixed": {"role": "fixed", "llm": "explicit-model"},
        },
        tasks_yaml={"t": {"description": "write the report"}},
        model="auto",
        execution_type="crew",
        user_message="write a report on churn",
    )
    fields.update(overrides)
    return CrewConfig(**fields)


class TestWalk:
    def test_concrete_config_is_left_alone(self):
        config = CrewConfig(model="m", agents_yaml={"a": {"llm": "m"}})
        assert not wants_auto(config)

    def test_every_auto_slot_gets_the_same_model(self):
        config = auto_config(
            nodes=[{"id": "n", "data": {"llm": "auto"}}, {"id": "k", "data": {}}]
        )
        assert wants_auto(config)
        apply_selection(config, "picked")
        assert config.model == "picked"
        assert config.agents_yaml["writer"]["llm"] == "picked"
        assert config.agents_yaml["checker"]["llm"] == {
            "model": "picked",
            "temperature": 0,
        }
        assert config.agents_yaml["fixed"]["llm"] == "explicit-model"
        assert config.nodes[0]["data"]["llm"] == "picked"
        assert "auto" not in json.dumps(config.model_dump(mode="json")).lower()

    def test_no_model_removes_the_slots_so_the_run_default_applies(self):
        config = auto_config()
        apply_selection(config, None)
        assert config.model is None
        assert "llm" not in config.agents_yaml["writer"]
        assert "llm" not in config.agents_yaml["checker"]
        assert config.agents_yaml["fixed"]["llm"] == "explicit-model"

    def test_run_prompt_prefers_the_users_words(self):
        assert run_prompt(auto_config()) == "write a report on churn"
        assert (
            run_prompt(auto_config(user_message=None, inputs={"user_request": "u"}))
            == "u"
        )
        assert run_prompt(auto_config(user_message=None)) == "write the report"


class TestResolve:
    @pytest.mark.asyncio
    async def test_no_auto_means_no_decision(self):
        with patch.object(auto_model, "select_for_workspace") as select:
            assert (
                await resolve_run_models(CrewConfig(model="m"), MagicMock(), None)
                is None
            )
        select.assert_not_called()

    @pytest.mark.asyncio
    async def test_one_decision_resolves_the_whole_run(self):
        config = auto_config()
        session, context = MagicMock(), MagicMock(primary_group_id="ws")
        with patch.object(
            auto_model, "select_for_workspace", new=AsyncMock(return_value=PICK)
        ) as select:
            result = await resolve_run_models(config, session, context)
        select.assert_awaited_once_with(session, context, "write a report on churn")
        assert result is PICK
        assert config.model == PICK.model
        assert config.agents_yaml["writer"]["llm"] == PICK.model

    @pytest.mark.asyncio
    async def test_without_a_request_session_it_routes_one(self):
        routed = MagicMock(name="routed")

        @asynccontextmanager
        async def fake_routed():
            yield routed

        with (
            patch("src.db.session.routed_scoped_session", fake_routed),
            patch.object(
                auto_model, "select_for_workspace", new=AsyncMock(return_value=PICK)
            ) as select,
        ):
            await resolve_run_models(auto_config(), None, MagicMock())
        assert select.await_args.args[0] is routed


class TestAttribution:
    def test_a_selection_made_here_wins(self):
        assert selection_for(CrewConfig(model="x"), PICK) is PICK

    def test_the_requests_earlier_pick_is_attributed_when_the_model_matches(self):
        token = current_selection.set(PICK)
        try:
            assert selection_for(CrewConfig(model=PICK.model), None) is PICK
            assert selection_for(CrewConfig(model="other"), None) is None
        finally:
            current_selection.reset(token)

    def test_no_pick_no_attribution(self):
        assert selection_for(CrewConfig(model="m"), None) is None


class TestTraceRecord:
    @pytest.mark.asyncio
    async def test_nothing_recorded_without_auto(self):
        with patch.object(auto_model, "_write_trace", new=AsyncMock()) as write:
            record_selection(None, "job", None)
            await asyncio.sleep(0)
        write.assert_not_called()

    @pytest.mark.asyncio
    async def test_trace_row_is_written_for_the_run(self):
        session = MagicMock(commit=AsyncMock())

        @asynccontextmanager
        async def fake_routed():
            yield session

        trace_service = MagicMock(create_trace=AsyncMock())
        with (
            patch("src.db.session.routed_scoped_session", fake_routed),
            patch(
                "src.services.trace.service.ExecutionTraceService",
                return_value=trace_service,
            ),
        ):
            record_selection(PICK, "job-9", MagicMock(primary_group_id="ws"))
            await asyncio.gather(*auto_model._pending)
        row = trace_service.create_trace.await_args.args[0]
        assert row["job_id"] == "job-9" and row["group_id"] == "ws"
        assert row["trace_metadata"]["model"] == PICK.model
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_failed_trace_write_never_fails_the_run(self):
        @asynccontextmanager
        async def broken():
            raise RuntimeError("db down")
            yield  # pragma: no cover

        with patch("src.db.session.routed_scoped_session", broken):
            record_selection(PICK, "job", None)
            await asyncio.gather(*auto_model._pending)


class TestCreateExecution:
    """The subprocess path: what the runner is handed is concrete."""

    @pytest.mark.asyncio
    async def test_auto_never_reaches_history_or_the_runner(self):
        service = ExecutionService(session=MagicMock())
        config = auto_config()
        with (
            patch.object(
                auto_model, "select_for_workspace", new=AsyncMock(return_value=PICK)
            ),
            patch.object(auto_model, "_write_trace", new=AsyncMock()) as write,
            patch(
                "src.services.execution.status.ExecutionStatusService.create_execution",
                new_callable=AsyncMock,
                return_value=True,
            ) as create,
            patch.object(
                ExecutionService, "_run_in_background", new_callable=AsyncMock
            ) as runner,
            patch("src.services.execution.service.ExecutionNameService"),
            patch.object(ExecutionService, "_generate_run_name_async", AsyncMock()),
        ):
            result = await service.create_execution(
                config, group_context=MagicMock(primary_group_id="ws")
            )
            await asyncio.gather(*auto_model._pending)
        stored = create.await_args.args[0]["inputs"]
        assert stored["model"] == PICK.model
        handed = runner.call_args.kwargs["config"]
        assert handed.model == PICK.model
        assert handed.agents_yaml["writer"]["llm"] == PICK.model
        assert "auto" not in json.dumps(handed.model_dump(mode="json")).lower()
        assert result["model_selection"] == {
            "requested": "auto",
            "model": PICK.model,
            "status": "selected",
        }
        assert write.await_args.args[1] == result["execution_id"]

    @pytest.mark.asyncio
    async def test_a_concrete_run_reports_no_selection(self):
        service = ExecutionService(session=MagicMock())
        with (
            patch.object(auto_model, "select_for_workspace") as select,
            patch(
                "src.services.execution.status.ExecutionStatusService.create_execution",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch.object(
                ExecutionService, "_run_in_background", new_callable=AsyncMock
            ),
            patch("src.services.execution.service.ExecutionNameService"),
            patch.object(ExecutionService, "_generate_run_name_async", AsyncMock()),
        ):
            result = await service.create_execution(
                CrewConfig(model="m", agents_yaml={"a": {"role": "r"}}),
                group_context=None,
            )
        select.assert_not_called()
        assert result["model_selection"] is None
