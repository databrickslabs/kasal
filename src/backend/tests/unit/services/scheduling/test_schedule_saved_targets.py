"""A schedule runs its saved crew or flow, in its own timezone.

- A schedule made from a run of a saved crew stores the crew's id. Each run is
  rebuilt from the crew (real secrets, not the run's masked copy) and records the
  crew_id, so emit-on-completion rules fire for it.
- A run with no saved crew whose stored copy was masked is refused.
- A run of a saved flow is scheduled by reference (no stored nodes).
- The scheduler loop hands the schedule's timezone to the next-run maths.
"""

import asyncio
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.core.exceptions import BadRequestError
from src.models.crew import Crew
from src.models.event_subscription import EmitRule, EventSubscription
from src.models.execution_history import ExecutionHistory
from src.models.flow import Flow
from src.models.schedule import Schedule
from src.models.schema import Schema
from src.models.trigger_queue import STATUS_PENDING, TriggerQueue
from src.repositories.event_subscription_repository import (
    EmitRuleRepository,
    EventSubscriptionRepository,
)
from src.repositories.trigger_queue_repository import TriggerQueueRepository
from src.schemas.execution import CrewConfig
from src.schemas.schedule import ScheduleCreateFromExecution
from src.services.execution.stored_inputs import mask_run_inputs
from src.services.scheduling import saved_targets
from src.services.scheduling.scheduler import SchedulerService
from src.services.triggers.emit_service import EmitService
from src.services.triggers.event_types import canonical_event_name
from src.utils.sensitive_data_utils import REDACTED_PLACEHOLDER
from src.utils.user_context import GroupContext

MODULE = "src.services.scheduling.scheduler"
BUILD = "src.services.scheduling.saved_targets.project_saved_crew"
LOOKUP = "src.services.catalog.run_lookup.crew_for_run"
CREW_ID = uuid.UUID("6f0c2a52-1b7e-4b8e-9c3a-2f6d7e8a9b10")
FLOW_ID = uuid.UUID("0d3f8e1a-7c2b-4d5e-8f9a-1b2c3d4e5f60")
GROUP = "team-a"

# The saved crew as the catalog projects it: the secret decrypted.
SAVED_AGENTS = {
    "analyst": {
        "role": "Analyst",
        "llm": "databricks-llama",
        "tool_configs": {"PowerBITool": {"client_secret": "real-secret-value"}},
    }
}
SAVED_TASKS = {"report": {"description": "Write the report", "agent": "analyst"}}

# The same crew as the run row stores it: masked.
MASKED_AGENTS = {
    "analyst": {
        "role": "Analyst",
        "llm": "databricks-llama",
        "tool_configs": {"PowerBITool": {"client_secret": REDACTED_PLACEHOLDER}},
    }
}


def _run(execution_type="crew", crew_id=None, flow_id=None, inputs=None):
    return SimpleNamespace(
        id=7,
        execution_type=execution_type,
        crew_id=crew_id,
        flow_id=flow_id,
        inputs=(
            inputs
            if inputs is not None
            else {
                "agents_yaml": MASKED_AGENTS,
                "tasks_yaml": SAVED_TASKS,
                "inputs": {"topic": "sales"},
                "model": "databricks-llama",
            }
        ),
    )


def _service(run):
    service = SchedulerService(MagicMock())
    service.repository = MagicMock(create=AsyncMock(side_effect=lambda d: d))
    service.execution_service = MagicMock(
        get_execution_record=AsyncMock(return_value=run)
    )
    return service


def _request(**extra):
    return ScheduleCreateFromExecution(
        name="Nightly report",
        cron_expression="0 9 * * *",
        execution_id=7,
        **extra,
    )


async def _create(service, request):
    with patch(f"{MODULE}.ScheduleResponse.model_validate", side_effect=lambda d: d):
        return await service.create_schedule_from_execution(
            request, GroupContext(group_ids=[GROUP], group_email="u@example.com")
        )


class TestScheduleFromARun:
    @pytest.mark.asyncio
    async def test_a_saved_crew_run_is_scheduled_by_reference(self):
        service = _service(_run(crew_id=CREW_ID))
        with patch(BUILD, new=AsyncMock(return_value=(SAVED_AGENTS, SAVED_TASKS))):
            created = await _create(service, _request(timezone="Europe/Berlin"))

        assert created["crew_id"] == CREW_ID
        assert created["timezone"] == "Europe/Berlin"
        # Stored for display, so stored masked — never the decrypted secret.
        shown = created["agents_yaml"]["analyst"]["tool_configs"]["PowerBITool"]
        assert shown["client_secret"] == REDACTED_PLACEHOLDER
        assert created["model"] == "databricks-llama"

    @pytest.mark.asyncio
    async def test_a_masked_copy_with_no_saved_crew_is_refused(self):
        service = _service(_run(execution_type="agent"))
        with pytest.raises(BadRequestError) as caught:
            await _create(service, _request())
        assert caught.value.detail == saved_targets.MASKED_CONFIG_REFUSAL
        service.repository.create.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_deleted_saved_crew_with_a_masked_copy_is_refused(self):
        service = _service(_run(crew_id=CREW_ID))
        with patch(BUILD, new=AsyncMock(return_value=None)):
            with pytest.raises(BadRequestError):
                await _create(service, _request())

    @pytest.mark.asyncio
    async def test_a_clean_chat_run_still_schedules_its_copy(self):
        clean = {
            "agents_yaml": {"assistant": {"role": "Assistant", "llm": "kat"}},
            "tasks_yaml": {"answer": {"description": "Answer"}},
            "inputs": {},
        }
        service = _service(_run(execution_type="agent", inputs=clean))
        created = await _create(service, _request(timezone="Europe/Berlin"))
        assert created["agents_yaml"] == clean["agents_yaml"]
        assert "crew_id" not in created
        assert created["timezone"] == "Europe/Berlin"

    @pytest.mark.asyncio
    async def test_masked_inputs_are_refused(self):
        inputs = {
            "agents_yaml": SAVED_AGENTS,
            "tasks_yaml": SAVED_TASKS,
            "inputs": {"api_key": REDACTED_PLACEHOLDER},
        }
        service = _service(_run(crew_id=CREW_ID, inputs=inputs))
        with patch(BUILD, new=AsyncMock(return_value=(SAVED_AGENTS, SAVED_TASKS))):
            with pytest.raises(BadRequestError) as caught:
                await _create(service, _request())
        assert caught.value.detail == saved_targets.MASKED_INPUTS_REFUSAL

    @pytest.mark.asyncio
    async def test_masking_is_judged_by_value_not_by_key(self):
        # A string under a secret-looking key, and one that only mentions the
        # placeholder, are real values; only the placeholder itself is masked.
        inputs = {
            "agents_yaml": SAVED_AGENTS,
            "tasks_yaml": SAVED_TASKS,
            "inputs": {
                "max_tokens": "4000",
                "note": f"Write {REDACTED_PLACEHOLDER} where a secret was.",
            },
        }
        service = _service(_run(crew_id=CREW_ID, inputs=inputs))
        with patch(BUILD, new=AsyncMock(return_value=(SAVED_AGENTS, SAVED_TASKS))):
            created = await _create(service, _request())
        assert created["inputs"]["max_tokens"] == "4000"

        inputs["inputs"] = {"max_tokens": REDACTED_PLACEHOLDER}
        with patch(BUILD, new=AsyncMock(return_value=(SAVED_AGENTS, SAVED_TASKS))):
            with pytest.raises(BadRequestError):
                await _create(
                    _service(_run(crew_id=CREW_ID, inputs=inputs)), _request()
                )

    @pytest.mark.asyncio
    async def test_a_run_whose_inputs_hold_a_count_can_be_scheduled(self):
        # Stored the way every launch path stores a run: through the masker.
        stored = mask_run_inputs(
            {
                "agents_yaml": SAVED_AGENTS,
                "tasks_yaml": SAVED_TASKS,
                "inputs": {"topic": "sales", "max_tokens": "4000"},
            }
        )
        service = _service(_run(crew_id=CREW_ID, inputs=stored))
        with patch(BUILD, new=AsyncMock(return_value=(SAVED_AGENTS, SAVED_TASKS))):
            created = await _create(service, _request())
        assert created["inputs"] == {"topic": "sales", "max_tokens": "4000"}

    @pytest.mark.asyncio
    async def test_a_saved_flow_run_is_scheduled_by_reference(self):
        inputs = {
            "flow_id": str(FLOW_ID),
            "nodes": [{"id": "n1", "data": {"token": REDACTED_PLACEHOLDER}}],
            "edges": [{"id": "e1"}],
            "inputs": {},
        }
        service = _service(_run(execution_type="flow", flow_id=FLOW_ID, inputs=inputs))
        created = await _create(service, _request())
        assert created["flow_id"] == FLOW_ID
        assert created["nodes"] is None and created["edges"] is None

    @pytest.mark.asyncio
    async def test_the_next_run_is_on_the_requested_clock(self):
        service = _service(_run(crew_id=CREW_ID))
        with (
            patch(BUILD, new=AsyncMock(return_value=(SAVED_AGENTS, SAVED_TASKS))),
            patch(f"{MODULE}.calculate_next_run_from_last") as next_run,
        ):
            next_run.return_value = datetime(2026, 10, 18, 7, 0)
            await _create(service, _request(timezone="Europe/Berlin"))
        next_run.assert_called_once_with("0 9 * * *", None, "Europe/Berlin")

    def test_an_unknown_timezone_is_rejected(self):
        with pytest.raises(ValueError, match="Unknown timezone"):
            _request(timezone="Mars/Olympus_Mons")


# --- a scheduled run end to end, on a real database ---------------------------


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        for model in (
            Schedule,
            ExecutionHistory,
            Flow,
            Crew,
            TriggerQueue,
            EventSubscription,
            EmitRule,
            Schema,
        ):
            await conn.run_sync(
                lambda sync, m=model: m.__table__.create(sync, checkfirst=True)
            )
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as s:
        yield s
    await engine.dispose()


async def _fire(session, schedule):
    """Run ``schedule`` once through the scheduler, as the loop would."""

    async def smart_session():
        yield session

    config = CrewConfig(
        agents_yaml=schedule.agents_yaml,
        tasks_yaml=schedule.tasks_yaml,
        inputs=schedule.inputs,
        model=schedule.model,
        execution_type="crew",
        crew_id=str(schedule.crew_id) if schedule.crew_id else None,
    )
    with (
        patch(f"{MODULE}.get_smart_db_session", side_effect=lambda: smart_session()),
        patch(f"{MODULE}.ExecutionService.generate_execution_name") as name,
        patch(
            f"{MODULE}.ExecutionService.run_crew_execution", new=AsyncMock()
        ) as start,
        patch(f"{MODULE}.KasalExecutionService"),
        patch(
            "src.services.execution.harness_choice.resolve_run_harness",
            new=AsyncMock(return_value=SimpleNamespace(value="kasal")),
        ),
        patch(BUILD, new=AsyncMock(return_value=(SAVED_AGENTS, SAVED_TASKS))),
    ):
        name.return_value = {"name": "Nightly report"}
        await SchedulerService(session).run_schedule_job(
            schedule.id, config, datetime.now(timezone.utc)
        )
    return start


class TestAScheduledRunOfASavedCrew:
    @pytest.mark.asyncio
    async def test_runs_the_saved_crew_records_its_id_and_fires_the_chain(
        self, session
    ):
        schedule = Schedule(
            name="Nightly report",
            cron_expression="0 9 * * *",
            execution_type="crew",
            agents_yaml=MASKED_AGENTS,
            tasks_yaml=SAVED_TASKS,
            inputs={"topic": "sales"},
            crew_id=CREW_ID,
            timezone="Europe/Berlin",
            group_id=GROUP,
            created_by_email="u@example.com",
        )
        session.add(schedule)
        # An emit rule on the producer crew and a subscriber listening to it.
        await EmitRuleRepository(session).insert(
            on_target={"kind": "crew", "id": str(CREW_ID)},
            event_type="completed",
            group_id=GROUP,
            enabled=True,
            schema_ref=None,
        )
        await EventSubscriptionRepository(session).insert(
            event_type=canonical_event_name("crew", str(CREW_ID), "completed"),
            target={"kind": "crew", "id": "downstream-crew"},
            group_id=GROUP,
            enabled=True,
            schema_ref=None,
            input_mapping=None,
        )
        await session.commit()

        start = await _fire(session, schedule)

        # The run used the saved crew's real configuration, not the masked copy.
        handed = start.await_args.kwargs["config"]
        secret = handed.agents_yaml["analyst"]["tool_configs"]["PowerBITool"]
        assert secret["client_secret"] == "real-secret-value"

        # The run row carries the crew, and its stored copy stays masked.
        run = (await session.execute(ExecutionHistory.__table__.select())).one()
        assert uuid.UUID(str(run.crew_id)) == CREW_ID
        assert run.trigger_type == "scheduled"
        stored = run.inputs["agents_yaml"]["analyst"]["tool_configs"]["PowerBITool"]
        assert stored["client_secret"] != "real-secret-value"

        # Completing the run emits to the subscriber: one pending trigger row.
        enqueued = await EmitService(session).emit_for_completed_run(
            execution_type=run.execution_type,
            flow_id=run.flow_id,
            crew_id=run.crew_id,
            group_id=run.group_id,
            job_id=run.job_id,
            result={"summary": "done"},
            event_type="completed",
        )
        assert enqueued == 1
        pending = await TriggerQueueRepository(session).list_for_groups(
            [GROUP], status=STATUS_PENDING
        )
        assert [row.target["id"] for row in pending] == ["downstream-crew"]

        # The next run moved on, on the schedule's clock: 09:00 Berlin.
        await session.refresh(schedule)
        assert schedule.next_run_at.hour in (7, 8)

    @pytest.mark.asyncio
    async def test_a_deleted_saved_crew_turns_the_schedule_off_and_says_why(
        self, session
    ):
        schedule = Schedule(
            name="Nightly report",
            cron_expression="0 9 * * *",
            execution_type="crew",
            agents_yaml=MASKED_AGENTS,
            tasks_yaml=SAVED_TASKS,
            crew_id=CREW_ID,
            group_id=GROUP,
        )
        session.add(schedule)
        await session.commit()
        config = CrewConfig(
            agents_yaml=MASKED_AGENTS,
            tasks_yaml=SAVED_TASKS,
            execution_type="crew",
            crew_id=str(CREW_ID),
        )

        # The real lookup, on a crews table without that crew: definitely gone.
        start = await _fire_config(session, schedule, config)

        start.assert_not_called()
        await _assert_retired(session, schedule, "saved crew")

    @pytest.mark.asyncio
    async def test_a_lookup_that_fails_skips_the_run_and_keeps_the_schedule(
        self, session
    ):
        from sqlalchemy.exc import OperationalError

        from src.utils.cron_utils import calculate_next_run_from_last

        fired = datetime.now(timezone.utc)
        # What the loop stored before handing the run over.
        upcoming = calculate_next_run_from_last("0 9 * * *", fired, "Europe/Berlin")
        schedule = Schedule(
            name="Nightly report",
            cron_expression="0 9 * * *",
            execution_type="crew",
            agents_yaml=MASKED_AGENTS,
            tasks_yaml=SAVED_TASKS,
            crew_id=CREW_ID,
            timezone="Europe/Berlin",
            next_run_at=upcoming,
            group_id=GROUP,
        )
        session.add(schedule)
        await session.commit()
        config = CrewConfig(
            agents_yaml=MASKED_AGENTS,
            tasks_yaml=SAVED_TASKS,
            execution_type="crew",
            crew_id=str(CREW_ID),
        )
        dropped = OperationalError("SELECT crews", {}, Exception("connection lost"))

        with patch(LOOKUP, new=AsyncMock(side_effect=dropped)):
            start = await _fire_config(session, schedule, config, fired)

        start.assert_not_called()
        await session.refresh(schedule)
        assert schedule.is_active is True
        assert schedule.next_run_at == upcoming
        runs = (await session.execute(ExecutionHistory.__table__.select())).all()
        assert runs == []

    @pytest.mark.asyncio
    async def test_a_deleted_saved_flow_turns_the_schedule_off_and_says_why(
        self, session
    ):
        schedule = Schedule(
            name="Morning flow",
            cron_expression="0 9 * * *",
            execution_type="flow",
            flow_id=FLOW_ID,
            group_id=GROUP,
        )
        session.add(schedule)
        await session.commit()
        config = CrewConfig(execution_type="flow", flow_id=str(FLOW_ID))

        # No flows table row: the saved flow is gone.
        start = await _fire_config(session, schedule, config)

        start.assert_not_called()
        await _assert_retired(session, schedule, "saved flow")


async def _fire_config(session, schedule, config, fired=None):
    """Run ``schedule`` once with ``config``; returns the run-start mock."""

    async def smart_session():
        yield session

    with (
        patch(f"{MODULE}.get_smart_db_session", side_effect=lambda: smart_session()),
        patch(f"{MODULE}.ExecutionService.run_crew_execution") as start,
        patch(
            "src.services.execution.harness_choice.resolve_run_harness",
            new=AsyncMock(return_value=SimpleNamespace(value="kasal")),
        ),
    ):
        await SchedulerService(session).run_schedule_job(
            schedule.id, config, fired or datetime.now(timezone.utc)
        )
    return start


async def _assert_retired(session, schedule, reason_mentions):
    """The schedule is off, and one FAILED scheduled run says why."""
    await session.refresh(schedule)
    assert schedule.is_active is False
    runs = (await session.execute(ExecutionHistory.__table__.select())).all()
    assert len(runs) == 1
    assert runs[0].status == "FAILED"
    assert runs[0].trigger_type == "scheduled"
    assert reason_mentions in runs[0].error
    assert "turned off" in runs[0].error


class TestTheSchedulerLoop:
    @pytest.mark.asyncio
    async def test_due_schedule_carries_its_crew_and_timezone(self):
        due = SimpleNamespace(
            id=3,
            name="Nightly report",
            cron_expression="0 9 * * *",
            execution_type="crew",
            agents_yaml=MASKED_AGENTS,
            tasks_yaml=SAVED_TASKS,
            inputs={},
            model="databricks-llama",
            flow_id=None,
            nodes=None,
            edges=None,
            flow_config=None,
            crew_id=CREW_ID,
            timezone="Europe/Berlin",
            next_run_at=None,
            last_run_at=None,
            is_active=True,
        )
        repo = MagicMock(
            find_due_schedules=AsyncMock(return_value=[due]),
            find_all=AsyncMock(return_value=[]),
        )
        session = MagicMock(commit=AsyncMock())

        async def smart_session():
            yield session

        service = SchedulerService(MagicMock())
        with (
            patch(f"{MODULE}.ScheduleRepository", return_value=repo),
            patch(
                f"{MODULE}.get_smart_db_session", side_effect=lambda: smart_session()
            ),
            patch.object(service, "run_schedule_job", new=AsyncMock()) as job,
            patch(f"{MODULE}.calculate_next_run_from_last") as next_run,
            patch(f"{MODULE}.asyncio.sleep", side_effect=asyncio.CancelledError),
        ):
            next_run.return_value = datetime(2026, 10, 18, 7, 0)
            with pytest.raises(asyncio.CancelledError):
                await service.check_and_run_schedules()
        for task in service._running_tasks:
            task.cancel()

        config = job.call_args.args[1]
        assert config.crew_id == str(CREW_ID)
        assert next_run.call_args.args[2] == "Europe/Berlin"
        assert due.next_run_at == datetime(2026, 10, 18, 7, 0)

    @pytest.mark.asyncio
    async def test_an_unreadable_timezone_turns_the_schedule_off_without_firing(
        self, session
    ):
        # Written straight to the table, past the API's validation.
        schedule = Schedule(
            name="Nightly report",
            cron_expression="0 9 * * *",
            execution_type="crew",
            agents_yaml=SAVED_AGENTS,
            tasks_yaml=SAVED_TASKS,
            timezone="Mars/Olympus_Mons",
            next_run_at=datetime(2026, 1, 1, 9, 0),
            is_active=True,
            group_id=GROUP,
        )
        session.add(schedule)
        await session.commit()

        async def smart_session():
            yield session

        service = SchedulerService(MagicMock())
        with (
            patch(
                f"{MODULE}.get_smart_db_session", side_effect=lambda: smart_session()
            ),
            patch.object(service, "run_schedule_job", new=AsyncMock()) as job,
            patch(f"{MODULE}.asyncio.sleep", side_effect=asyncio.CancelledError),
            patch(
                "src.services.execution.harness_choice.resolve_run_harness",
                new=AsyncMock(return_value=SimpleNamespace(value="kasal")),
            ),
        ):
            with pytest.raises(asyncio.CancelledError):
                await service.check_and_run_schedules()

        job.assert_not_called()
        assert not service._running_tasks
        await _assert_retired(session, schedule, "timezone")

    @pytest.mark.asyncio
    async def test_a_failing_retire_does_not_stop_the_next_due_schedule(self):
        def due(id_, tz):
            return SimpleNamespace(
                id=id_,
                name=f"Schedule {id_}",
                cron_expression="0 9 * * *",
                execution_type="crew",
                agents_yaml=SAVED_AGENTS,
                tasks_yaml=SAVED_TASKS,
                inputs={},
                model="databricks-llama",
                flow_id=None,
                nodes=None,
                edges=None,
                flow_config=None,
                crew_id=None,
                timezone=tz,
                next_run_at=None,
                last_run_at=None,
                is_active=True,
            )

        broken, healthy = due(1, "Mars/Olympus_Mons"), due(2, "Europe/Berlin")
        repo = MagicMock(
            find_due_schedules=AsyncMock(return_value=[broken, healthy]),
            find_all=AsyncMock(return_value=[]),
        )
        session = MagicMock(commit=AsyncMock())

        async def smart_session():
            yield session

        service = SchedulerService(MagicMock())
        with (
            patch(f"{MODULE}.ScheduleRepository", return_value=repo),
            patch(
                f"{MODULE}.get_smart_db_session", side_effect=lambda: smart_session()
            ),
            patch(
                f"{MODULE}.retire_schedule",
                new=AsyncMock(side_effect=RuntimeError("database gone")),
            ) as retire,
            patch.object(service, "run_schedule_job", new=AsyncMock()) as job,
            patch(f"{MODULE}.asyncio.sleep", side_effect=asyncio.CancelledError),
        ):
            with pytest.raises(asyncio.CancelledError):
                await service.check_and_run_schedules()
        for task in service._running_tasks:
            task.cancel()

        retire.assert_awaited_once()
        assert [c.args[0] for c in job.call_args_list] == [2]
        assert healthy.next_run_at is not None
        assert broken.next_run_at is None  # still due: retried next tick
