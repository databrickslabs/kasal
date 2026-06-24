"""Tests for the DB-swap cache-invalidation hook on the swappable session factory.

When the underlying database is swapped at runtime (Lakebase activate/deactivate),
in-memory caches keyed to the OLD database must be flushed — otherwise a status
lookup after the swap serves/polls executions that don't exist in the new DB
(the recurring "Execution <id> not found in database" 404 storm).
"""
from src.db.session import _SwappableSessionFactory, _local_session_factory
from src.services.execution_service import ExecutionService


def _fresh_factory():
    return _SwappableSessionFactory(_local_session_factory)


def test_callback_fires_once_on_activate_state_change():
    f = _fresh_factory()
    calls = []
    f.register_on_swap(lambda: calls.append(1))

    f.activate_lakebase(_local_session_factory)   # local -> lakebase (transition)
    assert calls == [1]

    f.activate_lakebase(_local_session_factory)   # already lakebase: no transition
    assert calls == [1], "must not re-fire when state did not change"


def test_callback_fires_on_deactivate_state_change():
    f = _fresh_factory()
    calls = []
    f.register_on_swap(lambda: calls.append(1))
    f.activate_lakebase(_local_session_factory)   # -> lakebase (fire #1)
    f.deactivate_lakebase()                       # -> local (fire #2)
    assert calls == [1, 1]
    f.deactivate_lakebase()                       # already local: no transition
    assert calls == [1, 1]


def test_callback_exception_is_swallowed():
    f = _fresh_factory()
    f.register_on_swap(lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    hit = []
    f.register_on_swap(lambda: hit.append(1))
    f.activate_lakebase(_local_session_factory)   # must not raise
    assert hit == [1], "a failing hook must not stop the others"


def test_clear_in_memory_cache_drops_entries():
    ExecutionService.executions.clear()
    ExecutionService.executions["06bf4aba"] = {"execution_id": "06bf4aba", "status": "RUNNING"}
    n = ExecutionService.clear_in_memory_cache()
    assert n == 1
    assert ExecutionService.executions == {}


def test_swap_flushes_execution_registry_end_to_end():
    """Integration: a registered ExecutionService.clear_in_memory_cache hook wipes
    the stale execution cache on the first DB swap."""
    f = _fresh_factory()
    f.register_on_swap(ExecutionService.clear_in_memory_cache)
    ExecutionService.executions.clear()
    ExecutionService.executions["a5e11511"] = {"execution_id": "a5e11511", "status": "RUNNING"}

    f.activate_lakebase(_local_session_factory)

    assert ExecutionService.executions == {}, "execution cache must be cleared on DB swap"
    ExecutionService.executions.clear()
