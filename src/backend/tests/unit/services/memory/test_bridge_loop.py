"""Unit tests for the memory sync->async bridge loop, focused on clean shutdown.

``shutdown_bridge_loop`` exists so a crew/flow SUBPROCESS returns the bridge
loop's thread-local Lakebase connection cleanly at teardown. Without it the
daemon bridge thread is killed at interpreter exit, orphaning that asyncpg
connection -> "Event loop is closed" / GC "non-checked-in connection".
"""

import threading
from unittest.mock import patch

import src.services.memory.bridge_loop as bl
from src.services.memory.bridge_loop import (
    get_bridge_loop,
    run_on_bridge_loop,
    shutdown_bridge_loop,
)


class TestShutdownBridgeLoop:
    def teardown_method(self):
        # Never leak a running bridge loop into another test.
        shutdown_bridge_loop()

    def test_noop_when_no_loop(self):
        shutdown_bridge_loop()  # clear any loop a prior test started
        with bl._BRIDGE_LOCK:
            assert bl._BRIDGE_LOOP is None
        # No loop present: must not raise, singleton stays None.
        shutdown_bridge_loop()
        with bl._BRIDGE_LOCK:
            assert bl._BRIDGE_LOOP is None

    def test_stops_loop_resets_singleton_and_recreates_fresh(self):
        loop = get_bridge_loop()

        async def _echo():
            return 42

        assert run_on_bridge_loop(_echo()) == 42

        shutdown_bridge_loop(timeout=5.0)

        # Singleton cleared so the next caller builds a fresh loop.
        with bl._BRIDGE_LOCK:
            assert bl._BRIDGE_LOOP is None

        loop2 = get_bridge_loop()
        assert loop2 is not loop
        # The fresh loop is functional (proves the thread runs run_forever).
        assert run_on_bridge_loop(_echo()) == 42

    def test_disposes_thread_local_lakebase_on_the_bridge_thread(self):
        get_bridge_loop()
        seen = {}

        async def _fake_dispose():
            seen["thread"] = threading.current_thread().name

        # shutdown imports this lazily inside the coroutine it runs on the loop.
        with patch(
            "src.db.lakebase_session.dispose_thread_local_lakebase_factory",
            _fake_dispose,
        ):
            shutdown_bridge_loop(timeout=5.0)

        # Dispose must run ON the bridge thread, so its threading.local() is the
        # one holding the factory that leaks.
        assert seen.get("thread") == "kasal-memory-bridge"

    def test_never_raises_even_if_dispose_errors(self):
        get_bridge_loop()

        async def _boom():
            raise RuntimeError("dispose blew up")

        with patch(
            "src.db.lakebase_session.dispose_thread_local_lakebase_factory",
            _boom,
        ):
            # Teardown must swallow the error.
            shutdown_bridge_loop(timeout=5.0)
        with bl._BRIDGE_LOCK:
            assert bl._BRIDGE_LOOP is None
