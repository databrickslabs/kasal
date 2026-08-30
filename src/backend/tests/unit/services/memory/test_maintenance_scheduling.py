"""WHEN maintenance runs — the throttle and the registry.

``run_memory_maintenance`` used to be called only from the crew path, so a
workspace that only used chat (or only flows) never consolidated its memory and
accumulated duplicate records forever. These are the two doors the other paths
enter through:

* chat  — throttled, because a turn finishes far too often to maintain on
  every one.
* flow  — registry-based, because a flow's crews build their Memory deep inside
  the flow and the subprocess teardown has no handle on it.

(``test_maintenance.py`` covers WHAT the passes do.)
"""

import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.services.memory.engine import Memory
from src.services.memory.maintenance import passes as maintenance
from src.services.memory.maintenance.passes import (
    clear_registered_memories,
    maybe_run_memory_maintenance,
    register_memory_for_maintenance,
    reset_maintenance_throttle,
    run_maintenance_after_writes,
    run_registered_memory_maintenance,
    schedule_maintenance_after_writes,
)
from src.services.memory.storage.adapter import EngineStorageAdapter
from src.services.memory.storage.local import LocalStorageBackend


@pytest.fixture(autouse=True)
def _clean_module_state():
    reset_maintenance_throttle()
    clear_registered_memories()
    yield
    reset_maintenance_throttle()
    clear_registered_memories()


def _embedder(texts):
    return [[1.0, 0.0] for _ in texts]


def _memory(tmp_path, root_scope="/g1", name="m.db"):
    backend = LocalStorageBackend(tmp_path / name, embedder=_embedder)
    return Memory(
        storage=EngineStorageAdapter(backend),
        root_scope=root_scope,
        # The stub embedder gives every text the same vector; save-time
        # consolidation off so the maintenance pass under test is what acts.
        consolidation_threshold=0,
    )


def _with_duplicates(memory, text="User: q? Assistant: a", copies=3):
    for _ in range(copies):
        memory.remember(text)
        time.sleep(0.01)  # distinct created_at ordering
    return memory


class TestThrottle:
    def test_first_call_runs(self, tmp_path):
        memory = _with_duplicates(_memory(tmp_path))
        stats = maybe_run_memory_maintenance(memory)
        assert stats.get("skipped") is None
        assert stats["deleted"] == 2

    def test_second_call_within_interval_is_skipped(self, tmp_path):
        memory = _with_duplicates(_memory(tmp_path))
        maybe_run_memory_maintenance(memory)
        _with_duplicates(memory, text="another repeated fact")

        stats = maybe_run_memory_maintenance(memory)

        assert stats["skipped"] == 1
        assert stats["deleted"] == 0
        # The new duplicates are still there — skipped means skipped, not "no-op".
        # (1 survivor from the first pass + the 3 fresh copies.)
        assert len(memory.list_records(limit=50)) == 4

    def test_zero_interval_disables_throttling(self, tmp_path):
        memory = _with_duplicates(_memory(tmp_path))
        assert maybe_run_memory_maintenance(memory, min_interval_s=0)["deleted"] == 2
        _with_duplicates(memory, text="another repeated fact")
        assert maybe_run_memory_maintenance(memory, min_interval_s=0)["deleted"] == 2

    def test_throttle_is_per_scope(self, tmp_path):
        """One tenant's chat turn must not silence another's maintenance."""
        group_a = _with_duplicates(_memory(tmp_path, "/group-a", "a.db"))
        group_b = _with_duplicates(_memory(tmp_path, "/group-b", "b.db"))

        assert maybe_run_memory_maintenance(group_a)["deleted"] == 2
        assert maybe_run_memory_maintenance(group_b)["deleted"] == 2

    def test_interval_read_from_environment(self, tmp_path, monkeypatch):
        monkeypatch.setenv("KASAL_MEMORY_MAINTENANCE_INTERVAL", "0")
        memory = _with_duplicates(_memory(tmp_path))
        assert maybe_run_memory_maintenance(memory)["deleted"] == 2
        _with_duplicates(memory, text="another repeated fact")
        assert maybe_run_memory_maintenance(memory)["deleted"] == 2

    def test_unusable_memory_is_skipped(self):
        for sentinel in (None, True, False):
            assert maybe_run_memory_maintenance(sentinel)["skipped"] == 1


class TestRunAfterWrites:
    """The chat entry point: flush in-flight writes, then maintain."""

    @pytest.mark.asyncio
    async def test_flushes_before_maintaining(self, tmp_path):
        memory = _with_duplicates(_memory(tmp_path))
        calls = []
        with patch(
            "src.services.memory.run.persist.flush_memory_writes",
            side_effect=lambda *a, **k: calls.append("flush") or 0,
        ):
            stats = await run_maintenance_after_writes(memory, min_interval_s=0)

        assert calls == ["flush"]
        assert stats["deleted"] == 2

    @pytest.mark.asyncio
    async def test_throttled_call_does_not_even_flush(self, tmp_path):
        """A skipped turn must cost nothing — no flush, no listing."""
        memory = _with_duplicates(_memory(tmp_path))
        await run_maintenance_after_writes(memory)
        with patch("src.services.memory.run.persist.flush_memory_writes") as flush:
            stats = await run_maintenance_after_writes(memory)

        flush.assert_not_called()
        assert stats["skipped"] == 1

    @pytest.mark.asyncio
    async def test_failure_never_propagates(self, tmp_path):
        """A chat turn must not fail because maintenance did."""
        memory = MagicMock()
        memory.root_scope = "/g1"
        memory.list_records.side_effect = RuntimeError("backend down")

        stats = await run_maintenance_after_writes(memory, min_interval_s=0)

        assert stats["deleted"] == 0

    @pytest.mark.asyncio
    async def test_unusable_memory_is_skipped(self):
        assert (await run_maintenance_after_writes(None))["skipped"] == 1

    @pytest.mark.asyncio
    async def test_scheduled_task_is_strongly_referenced_until_done(self, tmp_path):
        """A bare create_task can be GC'd mid-flight — which would silently
        reintroduce the very bug this scheduling exists to fix."""
        memory = _with_duplicates(_memory(tmp_path))

        task = schedule_maintenance_after_writes(memory)

        assert task in maintenance._scheduled_tasks
        assert (await task)["deleted"] == 2
        assert task not in maintenance._scheduled_tasks

    def test_scheduling_without_a_loop_is_a_no_op(self, tmp_path):
        assert schedule_maintenance_after_writes(_memory(tmp_path)) is None

    @pytest.mark.asyncio
    async def test_scheduling_unusable_memory_is_a_no_op(self):
        assert schedule_maintenance_after_writes(None) is None


class TestRegistry:
    """The flow path: register at build time, maintain at teardown."""

    def test_registered_memory_is_maintained(self, tmp_path):
        memory = _with_duplicates(_memory(tmp_path))
        register_memory_for_maintenance(memory)

        assert run_registered_memory_maintenance()["deleted"] == 2

    def test_nothing_registered_is_a_no_op(self):
        assert run_registered_memory_maintenance() == {
            "scanned": 0,
            "deleted": 0,
            "merged_clusters": 0,
            "records_replaced": 0,
            "superseded": 0,
            "forgotten": 0,
        }

    def test_sentinels_are_not_registered(self, tmp_path):
        for sentinel in (None, True, False):
            register_memory_for_maintenance(sentinel)
        assert run_registered_memory_maintenance()["scanned"] == 0

    def test_same_memory_registers_once(self, tmp_path):
        memory = _with_duplicates(_memory(tmp_path))
        register_memory_for_maintenance(memory)
        register_memory_for_maintenance(memory)

        assert run_registered_memory_maintenance()["scanned"] == 3

    def test_one_pass_per_root_scope(self, tmp_path):
        """Several crews in a flow share a scope — do not re-scan it per crew."""
        shared = _with_duplicates(_memory(tmp_path))
        second_view = shared.scope("/g1")
        register_memory_for_maintenance(shared)
        register_memory_for_maintenance(second_view)

        assert run_registered_memory_maintenance()["scanned"] == 3

    def test_distinct_scopes_each_get_a_pass(self, tmp_path):
        register_memory_for_maintenance(
            _with_duplicates(_memory(tmp_path, "/group-a", "a.db"))
        )
        register_memory_for_maintenance(
            _with_duplicates(_memory(tmp_path, "/group-b", "b.db"))
        )

        assert run_registered_memory_maintenance()["deleted"] == 4

    def test_one_failing_memory_does_not_stop_the_rest(self, tmp_path):
        broken = SimpleNamespace(root_scope="/broken")
        broken.list_records = MagicMock(side_effect=RuntimeError("backend down"))
        register_memory_for_maintenance(broken)
        register_memory_for_maintenance(
            _with_duplicates(_memory(tmp_path, "/group-b", "b.db"))
        )

        assert run_registered_memory_maintenance()["deleted"] == 2

    def test_registry_is_bounded(self, tmp_path):
        from src.services.memory.maintenance import passes as maintenance

        for index in range(maintenance._REGISTRY_LIMIT + 5):
            register_memory_for_maintenance(
                _memory(tmp_path, f"/g{index}", f"{index}.db")
            )

        assert len(maintenance._registered_memories) == maintenance._REGISTRY_LIMIT
