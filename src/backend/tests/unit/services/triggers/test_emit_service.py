"""EmitService — emit-on-completion fan-out, against real in-memory SQLite.

The choreography loop's second half, with the standard-event-type scheme: a
producer opts into a lifecycle type via an emit rule ("completed"/"failed"); the
emitted event name is the canonical ``{kind}:{id}:{type}``; a subscription listens
for that exact canonical name. Verifies fan-out, the completed/failed split, and
the no-op cases.
"""

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from src.models.event_subscription import EmitRule, EventSubscription
from src.models.schema import Schema
from src.models.trigger_queue import STATUS_PENDING, TriggerQueue
from src.repositories.event_subscription_repository import (
    EmitRuleRepository,
    EventSubscriptionRepository,
)
from src.repositories.trigger_queue_repository import TriggerQueueRepository
from src.services.triggers.emit_service import EmitService
from src.services.triggers.event_types import canonical_event_name


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(TriggerQueue.__table__.create)
        await conn.run_sync(EventSubscription.__table__.create)
        await conn.run_sync(EmitRule.__table__.create)
        await conn.run_sync(Schema.__table__.create)
    maker = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with maker() as s:
        yield s
    await engine.dispose()


async def _emit_rule(session, *, kind, target_id, event_type, group_id="g1", **kw):
    return await EmitRuleRepository(session).insert(
        on_target={"kind": kind, "id": target_id},
        event_type=event_type,  # the lifecycle TYPE (completed/failed)
        group_id=group_id,
        enabled=True,
        schema_ref=kw.get("schema_ref"),
    )


async def _subscription(session, *, event_type, kind, target_id, group_id="g1", **kw):
    return await EventSubscriptionRepository(session).insert(
        event_type=event_type,  # the canonical event name it listens for
        target={"kind": kind, "id": target_id},
        group_id=group_id,
        enabled=True,
        schema_ref=kw.get("schema_ref"),
        input_mapping=kw.get("input_mapping"),
    )


async def _pending(session, group_id="g1"):
    return await TriggerQueueRepository(session).list_for_groups(
        [group_id], status=STATUS_PENDING
    )


class TestEmitFanOut:
    @pytest.mark.asyncio
    async def test_completed_crew_enqueues_one_row_per_subscriber(self, session):
        # Producer crew 'c-src' opts into "completed"; two crews subscribe to it.
        await _emit_rule(
            session, kind="crew", target_id="c-src", event_type="completed"
        )
        canonical = canonical_event_name("crew", "c-src", "completed")
        await _subscription(session, event_type=canonical, kind="crew", target_id="c-a")
        await _subscription(session, event_type=canonical, kind="flow", target_id="f-b")
        await session.commit()

        n = await EmitService(session).emit_for_completed_run(
            execution_type="crew",
            flow_id=None,
            crew_id="c-src",
            group_id="g1",
            job_id="run-1",
            result={"summary": "done"},
            event_type="completed",
        )

        assert n == 2
        rows = await _pending(session)
        assert sorted(r.target["id"] for r in rows) == ["c-a", "f-b"]
        row = rows[0]
        assert row.event_type == canonical
        assert row.causation_run_id == "run-1"
        assert row.payload["inputs"]["payload"] == {"summary": "done"}
        assert row.payload["event"]["source_run"] == "run-1"

    @pytest.mark.asyncio
    async def test_flow_producer_matches_on_flow_id(self, session):
        await _emit_rule(
            session, kind="flow", target_id="f-src", event_type="completed"
        )
        canonical = canonical_event_name("flow", "f-src", "completed")
        await _subscription(session, event_type=canonical, kind="crew", target_id="c-x")
        await session.commit()

        n = await EmitService(session).emit_for_completed_run(
            execution_type="flow",
            flow_id="f-src",
            crew_id=None,
            group_id="g1",
            job_id="run-2",
            result="raw text",
            event_type="completed",
        )

        assert n == 1
        rows = await _pending(session)
        assert rows[0].target["id"] == "c-x"
        assert rows[0].payload["inputs"]["payload"] == "raw text"

    @pytest.mark.asyncio
    async def test_completed_and_failed_are_distinct_events(self, session):
        # Same producer opts into BOTH types; each has its own subscriber.
        await _emit_rule(
            session, kind="crew", target_id="c-src", event_type="completed"
        )
        await _emit_rule(session, kind="crew", target_id="c-src", event_type="failed")
        done = canonical_event_name("crew", "c-src", "completed")
        failed = canonical_event_name("crew", "c-src", "failed")
        await _subscription(session, event_type=done, kind="crew", target_id="c-ok")
        await _subscription(session, event_type=failed, kind="crew", target_id="c-oops")
        await session.commit()

        # A FAILED run triggers only the failed-subscriber.
        n = await EmitService(session).emit_for_completed_run(
            execution_type="crew",
            flow_id=None,
            crew_id="c-src",
            group_id="g1",
            job_id="run-3",
            result="boom",
            event_type="failed",
        )
        assert n == 1
        rows = await _pending(session)
        assert [r.target["id"] for r in rows] == ["c-oops"]

    @pytest.mark.asyncio
    async def test_input_mapping_overrides_passthrough(self, session):
        await _emit_rule(
            session, kind="crew", target_id="c-src", event_type="completed"
        )
        canonical = canonical_event_name("crew", "c-src", "completed")
        await _subscription(
            session,
            event_type=canonical,
            kind="crew",
            target_id="c-a",
            input_mapping={"topic": "fixed"},
        )
        await session.commit()

        await EmitService(session).emit_for_completed_run(
            execution_type="crew",
            flow_id=None,
            crew_id="c-src",
            group_id="g1",
            job_id="run-4",
            result={"ignored": True},
            event_type="completed",
        )
        rows = await _pending(session)
        assert rows[0].payload["inputs"] == {"topic": "fixed"}


class TestEmitNoOp:
    @pytest.mark.asyncio
    async def test_no_emit_rule_for_type_enqueues_nothing(self, session):
        # Producer opts into "completed" only; a FAILED run emits nothing.
        await _emit_rule(
            session, kind="crew", target_id="c-src", event_type="completed"
        )
        failed = canonical_event_name("crew", "c-src", "failed")
        await _subscription(session, event_type=failed, kind="crew", target_id="c-a")
        await session.commit()

        n = await EmitService(session).emit_for_completed_run(
            execution_type="crew",
            flow_id=None,
            crew_id="c-src",
            group_id="g1",
            job_id="run-5",
            result="x",
            event_type="failed",
        )
        assert n == 0
        assert await _pending(session) == []

    @pytest.mark.asyncio
    async def test_rule_with_no_subscriber_enqueues_nothing(self, session):
        await _emit_rule(
            session, kind="crew", target_id="c-src", event_type="completed"
        )
        await session.commit()

        n = await EmitService(session).emit_for_completed_run(
            execution_type="crew",
            flow_id=None,
            crew_id="c-src",
            group_id="g1",
            job_id="run-6",
            result="x",
            event_type="completed",
        )
        assert n == 0
        assert await _pending(session) == []

    @pytest.mark.asyncio
    async def test_unsaved_run_without_identity_is_skipped(self, session):
        await _emit_rule(
            session, kind="crew", target_id="c-src", event_type="completed"
        )
        canonical = canonical_event_name("crew", "c-src", "completed")
        await _subscription(session, event_type=canonical, kind="crew", target_id="c-a")
        await session.commit()

        n = await EmitService(session).emit_for_completed_run(
            execution_type="crew",
            flow_id=None,
            crew_id=None,  # ad-hoc run, no saved crew
            group_id="g1",
            job_id="run-7",
            result="x",
            event_type="completed",
        )
        assert n == 0


class TestChains:
    @pytest.mark.asyncio
    async def test_hop_cap_stops_the_chain(self, session, monkeypatch):
        monkeypatch.setenv("KASAL_EVENT_TRIGGERS_MAX_HOPS", "3")
        await _emit_rule(
            session, kind="crew", target_id="c-src", event_type="completed"
        )
        canonical = canonical_event_name("crew", "c-src", "completed")
        await _subscription(session, event_type=canonical, kind="crew", target_id="c-a")
        await session.commit()

        n = await EmitService(session).emit_for_completed_run(
            execution_type="crew",
            flow_id=None,
            crew_id="c-src",
            group_id="g1",
            job_id="run-deep",
            result="x",
            event_type="completed",
            hops=3,  # at the cap — emits nothing
        )
        assert n == 0
        assert await _pending(session) == []

    @pytest.mark.asyncio
    async def test_correlation_threads_and_hops_increment(self, session):
        await _emit_rule(
            session, kind="crew", target_id="c-src", event_type="completed"
        )
        canonical = canonical_event_name("crew", "c-src", "completed")
        await _subscription(session, event_type=canonical, kind="crew", target_id="c-a")
        await session.commit()

        n = await EmitService(session).emit_for_completed_run(
            execution_type="crew",
            flow_id=None,
            crew_id="c-src",
            group_id="g1",
            job_id="run-mid",
            result="x",
            event_type="completed",
            correlation_id="chain-origin",
            hops=1,
        )
        assert n == 1
        row = (await _pending(session))[0]
        # The chain ORIGIN is preserved; this run is only the immediate cause.
        assert row.correlation_id == "chain-origin"
        assert row.causation_run_id == "run-mid"
        assert row.payload["event"]["hops"] == 2

    @pytest.mark.asyncio
    async def test_chain_start_defaults_correlation_to_job_id(self, session):
        await _emit_rule(
            session, kind="crew", target_id="c-src", event_type="completed"
        )
        canonical = canonical_event_name("crew", "c-src", "completed")
        await _subscription(session, event_type=canonical, kind="crew", target_id="c-a")
        await session.commit()

        await EmitService(session).emit_for_completed_run(
            execution_type="crew",
            flow_id=None,
            crew_id="c-src",
            group_id="g1",
            job_id="run-first",
            result="x",
            event_type="completed",
        )
        row = (await _pending(session))[0]
        assert row.correlation_id == "run-first"
        assert row.payload["event"]["hops"] == 1


class TestIdempotentEmission:
    @pytest.mark.asyncio
    async def test_double_emission_collapses_onto_the_unique_key(self, session):
        # Two terminal-status writers racing (or a crash-retry) emit the same
        # run twice; the deterministic idempotency key must make the second a
        # no-op instead of double-firing the subscriber.
        await _emit_rule(
            session, kind="crew", target_id="c-src", event_type="completed"
        )
        canonical = canonical_event_name("crew", "c-src", "completed")
        await _subscription(session, event_type=canonical, kind="crew", target_id="c-a")
        await session.commit()

        kwargs = dict(
            execution_type="crew",
            flow_id=None,
            crew_id="c-src",
            group_id="g1",
            job_id="run-twice",
            result="x",
            event_type="completed",
        )
        assert await EmitService(session).emit_for_completed_run(**kwargs) == 1
        assert await EmitService(session).emit_for_completed_run(**kwargs) == 0
        assert len(await _pending(session)) == 1


class TestFailedEventShape:
    @pytest.mark.asyncio
    async def test_failed_event_carries_error_not_payload(self, session):
        await _emit_rule(session, kind="crew", target_id="c-src", event_type="failed")
        canonical = canonical_event_name("crew", "c-src", "failed")
        await _subscription(
            session, event_type=canonical, kind="crew", target_id="c-handler"
        )
        await session.commit()

        await EmitService(session).emit_for_completed_run(
            execution_type="crew",
            flow_id=None,
            crew_id="c-src",
            group_id="g1",
            job_id="run-f",
            result="boom: tool exploded",
            event_type="failed",
        )
        row = (await _pending(session))[0]
        # A handler crew's template can name {error} honestly.
        assert row.payload["inputs"] == {"error": "boom: tool exploded"}


class TestSelfLoopGuard:
    @pytest.mark.asyncio
    async def test_subscription_targeting_its_own_producer_is_skipped(self, session):
        # One event -> one run. A crew subscribed to its OWN completion would
        # re-run forever; the emission is refused outright, not hop-capped.
        await _emit_rule(
            session, kind="crew", target_id="c-src", event_type="completed"
        )
        canonical = canonical_event_name("crew", "c-src", "completed")
        await _subscription(
            session, event_type=canonical, kind="crew", target_id="c-src"
        )
        await session.commit()

        n = await EmitService(session).emit_for_completed_run(
            execution_type="crew",
            flow_id=None,
            crew_id="c-src",
            group_id="g1",
            job_id="run-self",
            result="x",
            event_type="completed",
        )
        assert n == 0
        assert await _pending(session) == []

    @pytest.mark.asyncio
    async def test_other_subscribers_still_fire_beside_a_self_loop(self, session):
        await _emit_rule(
            session, kind="crew", target_id="c-src", event_type="completed"
        )
        canonical = canonical_event_name("crew", "c-src", "completed")
        await _subscription(
            session, event_type=canonical, kind="crew", target_id="c-src"  # self
        )
        await _subscription(
            session, event_type=canonical, kind="crew", target_id="c-next"
        )
        await session.commit()

        n = await EmitService(session).emit_for_completed_run(
            execution_type="crew",
            flow_id=None,
            crew_id="c-src",
            group_id="g1",
            job_id="run-mixed",
            result="x",
            event_type="completed",
        )
        assert n == 1
        rows = await _pending(session)
        assert [r.target["id"] for r in rows] == ["c-next"]


async def _schema(session, name, definition, **kw):
    session.add(
        Schema(
            name=name,
            schema_type=kw.get("schema_type", "data_model"),
            description=kw.get("description", name),
            schema_definition=definition,
        )
    )
    await session.flush()


class TestSchemaShaping:
    async def _wire(self, session, *, sub_schema=None):
        await _emit_rule(
            session, kind="crew", target_id="c-src", event_type="completed"
        )
        canonical = canonical_event_name("crew", "c-src", "completed")
        await _subscription(
            session,
            event_type=canonical,
            kind="crew",
            target_id="c-next",
            schema_ref=sub_schema,
        )
        await session.commit()

    async def _emit(self, session, result):
        return await EmitService(session).emit_for_completed_run(
            execution_type="crew",
            flow_id=None,
            crew_id="c-src",
            group_id="g1",
            job_id="run-s",
            result=result,
            event_type="completed",
        )

    @pytest.mark.asyncio
    async def test_scalar_result_maps_onto_single_required_property(self, session):
        await _schema(
            session,
            "color",
            {
                "type": "object",
                "properties": {"color": {"type": "string"}},
                "required": ["color"],
            },
        )
        await self._wire(session, sub_schema="color")

        assert await self._emit(session, "green") == 1
        row = (await _pending(session))[0]
        # The schema's STRUCTURE is the contract — not an opaque payload key.
        assert row.payload["inputs"] == {"color": "green"}
        assert row.payload["event"]["schema"] == "color"

    @pytest.mark.asyncio
    async def test_dict_result_is_projected_onto_schema_fields(self, session):
        await _schema(
            session,
            "report",
            {"type": "object", "properties": {"title": {}, "body": {}}},
        )
        await self._wire(session, sub_schema="report")

        await self._emit(session, {"title": "T", "body": "B", "junk": "x"})
        row = (await _pending(session))[0]
        assert row.payload["inputs"] == {"title": "T", "body": "B"}

    @pytest.mark.asyncio
    async def test_json_string_result_parses_before_projection(self, session):
        await _schema(
            session, "color", {"properties": {"color": {}}, "required": ["color"]}
        )
        await self._wire(session, sub_schema="color")

        await self._emit(session, '{"color": "green", "junk": 1}')
        row = (await _pending(session))[0]
        assert row.payload["inputs"] == {"color": "green"}

    @pytest.mark.asyncio
    async def test_unresolvable_schema_falls_back_to_payload(self, session):
        await self._wire(session, sub_schema="does-not-exist")

        await self._emit(session, "green")
        row = (await _pending(session))[0]
        assert row.payload["inputs"] == {"payload": "green"}

    @pytest.mark.asyncio
    async def test_unshapeable_result_falls_back_to_payload(self, session):
        # Two properties, none singled out as required — a scalar can't be
        # mapped without guessing, so the raw passthrough wins.
        await _schema(session, "pair", {"properties": {"a": {}, "b": {}}})
        await self._wire(session, sub_schema="pair")

        await self._emit(session, "green")
        row = (await _pending(session))[0]
        assert row.payload["inputs"] == {"payload": "green"}

    @pytest.mark.asyncio
    async def test_input_mapping_still_beats_the_schema(self, session):
        await _schema(
            session, "color", {"properties": {"color": {}}, "required": ["color"]}
        )
        await _emit_rule(
            session, kind="crew", target_id="c-src", event_type="completed"
        )
        canonical = canonical_event_name("crew", "c-src", "completed")
        await _subscription(
            session,
            event_type=canonical,
            kind="crew",
            target_id="c-next",
            schema_ref="color",
            input_mapping={"topic": "fixed"},
        )
        await session.commit()

        await self._emit(session, "green")
        row = (await _pending(session))[0]
        assert row.payload["inputs"] == {"topic": "fixed"}
