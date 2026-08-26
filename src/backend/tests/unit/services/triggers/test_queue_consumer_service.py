"""TriggerQueueConsumerService — build/dispatch/claim, with the DB + engine mocked.

``_build_config`` is a pure function (tested directly). ``_dispatch`` and
``claim_and_dispatch`` are tested with ``routed_scoped_session`` and
``ExecutionService`` mocked, so we assert the dispatch CONTRACT (a run record is
created with ``trigger_type="lakebase_queue"`` and the run is launched) without a
real database or a real crew run.
"""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.schemas.execution import CrewConfig
from src.services.triggers.queue_consumer_service import (
    MAX_ATTEMPTS,
    TRIGGER_TYPE,
    TriggerQueueConsumerService,
)
from src.utils.user_context import GroupContext


def _ctx() -> GroupContext:
    return GroupContext(group_ids=["g1"], group_email="g1@example.com")


MODULE = "src.services.triggers.queue_consumer_service"


def _fake_routed(session):
    """A stand-in for routed_scoped_session() -> async context manager."""

    def _factory():
        @asynccontextmanager
        async def _cm():
            yield session

        return _cm()

    return _factory


@pytest.fixture
def service():
    return TriggerQueueConsumerService()


# ---------------------------------------------------------------------------
# _build_config — pure
# ---------------------------------------------------------------------------
class TestBuildConfig:
    @pytest.mark.asyncio
    async def test_flow_by_id(self, service):
        config, et = await service._build_config(
            MagicMock(),
            {"kind": "flow", "id": "flow-123"},
            {"inputs": {"topic": "x"}},
            _ctx(),
        )
        assert et == "flow"
        assert isinstance(config, CrewConfig)
        assert config.execution_type == "flow"
        assert config.flow_id == "flow-123"
        assert config.inputs == {"topic": "x"}

    @pytest.mark.asyncio
    async def test_flow_without_id_raises(self, service):
        with pytest.raises(ValueError, match="flow target requires an 'id'"):
            await service._build_config(MagicMock(), {"kind": "flow"}, {}, _ctx())

    @pytest.mark.asyncio
    async def test_inline_crew(self, service):
        target = {
            "kind": "inline",
            "config": {
                "agents_yaml": {"a": {"role": "R"}},
                "tasks_yaml": {"t": {"description": "D"}},
            },
        }
        config, et = await service._build_config(
            MagicMock(), target, {"inputs": {"k": "v"}}, _ctx()
        )
        assert et == "crew"
        assert config.agents_yaml == {"a": {"role": "R"}}
        assert config.inputs == {"k": "v"}

    @pytest.mark.asyncio
    async def test_inline_flow(self, service):
        target = {
            "kind": "inline",
            "config": {"execution_type": "flow", "flow_id": "f9"},
        }
        config, et = await service._build_config(MagicMock(), target, {}, _ctx())
        assert et == "flow"
        assert config.flow_id == "f9"

    @pytest.mark.asyncio
    async def test_harness_override_applied(self, service):
        config, _ = await service._build_config(
            MagicMock(), {"kind": "flow", "id": "f1", "harness": "crewai"}, {}, _ctx()
        )
        assert config.harness == "crewai"

    @pytest.mark.asyncio
    async def test_crew_by_id_resolves_via_catalog(self, service):
        with patch(
            "src.services.catalog.crew_config.build_crew_execution_config_by_id",
            new_callable=AsyncMock,
            return_value=({"a": {"role": "R"}}, {"t": {"description": "D"}}),
        ) as resolver:
            config, et = await service._build_config(
                MagicMock(),
                {"kind": "crew", "id": "crew-1"},
                {"inputs": {"k": "v"}},
                _ctx(),
            )
        resolver.assert_awaited_once()
        assert et == "crew"
        assert config.agents_yaml == {"a": {"role": "R"}}
        assert config.crew_id == "crew-1"
        assert config.inputs == {"k": "v"}

    @pytest.mark.asyncio
    async def test_crew_not_found_raises(self, service):
        with patch(
            "src.services.catalog.crew_config.build_crew_execution_config_by_id",
            new_callable=AsyncMock,
            return_value=None,
        ):
            with pytest.raises(ValueError, match="not found or unreadable"):
                await service._build_config(
                    MagicMock(), {"kind": "crew", "id": "gone"}, {}, _ctx()
                )

    @pytest.mark.asyncio
    async def test_crew_without_id_raises(self, service):
        with pytest.raises(ValueError, match="crew target requires an 'id'"):
            await service._build_config(MagicMock(), {"kind": "crew"}, {}, _ctx())

    @pytest.mark.asyncio
    async def test_unknown_kind_raises(self, service):
        with pytest.raises(ValueError, match="unknown target kind"):
            await service._build_config(MagicMock(), {"kind": "webhook"}, {}, _ctx())


# ---------------------------------------------------------------------------
# _dispatch
# ---------------------------------------------------------------------------
class TestDispatch:
    @pytest.mark.asyncio
    async def test_flow_dispatch_creates_run_and_marks_dispatched(self, service):
        mock_session = MagicMock()
        mock_session.commit = AsyncMock()
        repo = MagicMock()
        repo.mark_dispatched = AsyncMock()
        snap = {
            "id": 7,
            "group_id": "g1",
            "target": {"kind": "flow", "id": "flow-9"},
            "payload": {"inputs": {"topic": "news"}},
            "correlation_id": "chain-1",
            "attempts": 1,
            "hops": 2,
        }

        with (
            patch(f"{MODULE}.routed_scoped_session", _fake_routed(mock_session)),
            patch(f"{MODULE}.TriggerQueueRepository", return_value=repo),
            patch(
                "src.services.execution.service.ExecutionService.create_run_record",
                new_callable=AsyncMock,
            ) as create_rec,
            patch(
                "src.services.execution.service.ExecutionService.run_crew_execution",
                new_callable=AsyncMock,
            ) as run_exec,
            patch(
                "src.utils.databricks_auth.get_auth_context",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            await service._dispatch(snap)

        # A run record was created for this event, tagged as queue-triggered.
        create_rec.assert_awaited_once()
        kwargs = create_rec.await_args.kwargs
        assert kwargs["trigger_type"] == TRIGGER_TYPE
        assert kwargs["execution_type"] == "flow"
        assert kwargs["group_id"] == "g1"
        assert str(kwargs["flow_id"]) == "flow-9"
        # The chain envelope rides in the run's stored inputs, for the emit hook.
        assert kwargs["inputs"]["trigger_hops"] == 2
        assert kwargs["inputs"]["correlation_id"] == "chain-1"
        # The run was launched, and the row marked dispatched.
        run_exec.assert_awaited_once()
        assert run_exec.await_args.kwargs["execution_type"] == "flow"
        assert isinstance(run_exec.await_args.kwargs["config"], CrewConfig)
        repo.mark_dispatched.assert_awaited_once_with(7)

    @pytest.mark.asyncio
    async def test_failure_requeues_when_attempts_remain(self, service):
        mock_session = MagicMock()
        mock_session.commit = AsyncMock()
        repo = MagicMock()
        repo.requeue = AsyncMock()
        repo.mark_failed = AsyncMock()
        snap = {
            "id": 3,
            "group_id": "g1",
            "target": {"kind": "flow", "id": "f1"},
            "payload": {},
            "attempts": 1,  # < MAX_ATTEMPTS
        }

        with (
            patch(f"{MODULE}.routed_scoped_session", _fake_routed(mock_session)),
            patch(f"{MODULE}.TriggerQueueRepository", return_value=repo),
            patch(
                "src.services.execution.service.ExecutionService.create_run_record",
                new_callable=AsyncMock,
            ),
            patch(
                "src.services.execution.service.ExecutionService.run_crew_execution",
                new_callable=AsyncMock,
                side_effect=RuntimeError("run blew up"),
            ),
            patch(
                "src.utils.databricks_auth.get_auth_context",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            await service._dispatch(snap)

        repo.requeue.assert_awaited_once()
        assert repo.requeue.await_args.args[0] == 3
        repo.mark_failed.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_failure_dead_letters_when_attempts_exhausted(self, service):
        mock_session = MagicMock()
        mock_session.commit = AsyncMock()
        repo = MagicMock()
        repo.requeue = AsyncMock()
        repo.mark_failed = AsyncMock()
        snap = {
            "id": 4,
            "group_id": "g1",
            "target": {"kind": "flow", "id": "f1"},
            "payload": {},
            "attempts": MAX_ATTEMPTS,  # exhausted
        }

        with (
            patch(f"{MODULE}.routed_scoped_session", _fake_routed(mock_session)),
            patch(f"{MODULE}.TriggerQueueRepository", return_value=repo),
            patch(
                "src.services.execution.service.ExecutionService.create_run_record",
                new_callable=AsyncMock,
                side_effect=RuntimeError("cannot even build"),
            ),
            patch(
                "src.utils.databricks_auth.get_auth_context",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            await service._dispatch(snap)

        repo.mark_failed.assert_awaited_once()
        assert repo.mark_failed.await_args.kwargs.get("dead") is True
        repo.requeue.assert_not_awaited()


# ---------------------------------------------------------------------------
# claim_and_dispatch
# ---------------------------------------------------------------------------
class TestClaimAndDispatch:
    @pytest.mark.asyncio
    async def test_claims_then_launches_one_dispatch_per_row(self, service):
        mock_session = MagicMock()
        mock_session.commit = AsyncMock()
        rows = [
            SimpleNamespace(
                id=1,
                group_id="g1",
                target={"kind": "flow", "id": "f1"},
                payload={},
                correlation_id=None,
                causation_run_id=None,
                attempts=1,
            ),
            SimpleNamespace(
                id=2,
                group_id="g2",
                target={"kind": "flow", "id": "f2"},
                payload={},
                correlation_id=None,
                causation_run_id=None,
                attempts=1,
            ),
        ]
        repo = MagicMock()
        repo.claim = AsyncMock(return_value=rows)

        with (
            patch(f"{MODULE}.routed_scoped_session", _fake_routed(mock_session)),
            patch(f"{MODULE}.TriggerQueueRepository", return_value=repo),
            patch.object(service, "_dispatch", new_callable=AsyncMock) as dispatch,
        ):
            tasks = await service.claim_and_dispatch(5)
            for t in tasks:
                await t

        repo.claim.assert_awaited_once_with(5, group_ids=None)
        mock_session.commit.assert_awaited()
        assert dispatch.await_count == 2
        dispatched_ids = sorted(c.args[0]["id"] for c in dispatch.await_args_list)
        assert dispatched_ids == [1, 2]

    @pytest.mark.asyncio
    async def test_empty_claim_launches_nothing(self, service):
        mock_session = MagicMock()
        mock_session.commit = AsyncMock()
        repo = MagicMock()
        repo.claim = AsyncMock(return_value=[])

        with (
            patch(f"{MODULE}.routed_scoped_session", _fake_routed(mock_session)),
            patch(f"{MODULE}.TriggerQueueRepository", return_value=repo),
            patch.object(service, "_dispatch", new_callable=AsyncMock) as dispatch,
        ):
            tasks = await service.claim_and_dispatch(5)

        assert tasks == []
        dispatch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_valueerror_dead_letters_immediately(self, service):
        # A malformed target is PERMANENT — retrying five times over backoff
        # cannot fix "unknown target kind". Straight to the dead letter.
        mock_session = MagicMock()
        mock_session.commit = AsyncMock()
        repo = MagicMock()
        repo.requeue = AsyncMock()
        repo.mark_failed = AsyncMock()
        snap = {
            "id": 8,
            "group_id": "g1",
            "target": {"kind": "webhook"},  # _build_config raises ValueError
            "payload": {},
            "attempts": 1,  # attempts remain — dead-lettered anyway
        }

        with (
            patch(f"{MODULE}.routed_scoped_session", _fake_routed(mock_session)),
            patch(f"{MODULE}.TriggerQueueRepository", return_value=repo),
            patch(
                "src.utils.databricks_auth.get_auth_context",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            await service._dispatch(snap)

        repo.mark_failed.assert_awaited_once()
        assert repo.mark_failed.await_args.kwargs.get("dead") is True
        repo.requeue.assert_not_awaited()


class TestClaimScoping:
    @pytest.mark.asyncio
    async def test_group_ids_are_passed_to_the_claim(self, service):
        mock_session = MagicMock()
        mock_session.commit = AsyncMock()
        repo = MagicMock()
        repo.claim = AsyncMock(return_value=[])

        with (
            patch(f"{MODULE}.routed_scoped_session", _fake_routed(mock_session)),
            patch(f"{MODULE}.TriggerQueueRepository", return_value=repo),
        ):
            await service.claim_and_dispatch(3, group_ids=["g1", "g2"])

        repo.claim.assert_awaited_once_with(3, group_ids=["g1", "g2"])

    @pytest.mark.asyncio
    async def test_snapshot_reads_hops_from_the_event_envelope(self, service):
        mock_session = MagicMock()
        mock_session.commit = AsyncMock()
        row = SimpleNamespace(
            id=9,
            group_id="g1",
            target={"kind": "flow", "id": "f1"},
            payload={"inputs": {}, "event": {"type": "crew:x:completed", "hops": 3}},
            correlation_id="chain-z",
            causation_run_id="run-up",
            attempts=1,
        )
        repo = MagicMock()
        repo.claim = AsyncMock(return_value=[row])

        with (
            patch(f"{MODULE}.routed_scoped_session", _fake_routed(mock_session)),
            patch(f"{MODULE}.TriggerQueueRepository", return_value=repo),
            patch.object(service, "_dispatch", new_callable=AsyncMock) as dispatch,
        ):
            tasks = await service.claim_and_dispatch(5)
            for t in tasks:
                await t

        snap = dispatch.await_args.args[0]
        assert snap["hops"] == 3
        assert snap["correlation_id"] == "chain-z"
