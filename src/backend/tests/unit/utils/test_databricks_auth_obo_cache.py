"""The OBO validation cache survives writes from other threads.

``_OBO_VALIDATION_CACHE`` is a module-level dict written from request tasks, LLM
worker threads and the usage-telemetry worker. When it is full,
``_obo_cache_put`` scans it for expired entries; scanning the live dict while
another thread inserts raised ``RuntimeError: dictionary changed size during
iteration`` out of ``get_auth_context``.
"""

import sys
import threading
import time

import pytest

from src.utils import databricks_auth as auth


@pytest.fixture(autouse=True)
def _empty_cache():
    auth._OBO_VALIDATION_CACHE.clear()
    yield
    auth._OBO_VALIDATION_CACHE.clear()


class _AnotherThreadWritesDuringTheScan:
    """An expiry that, when the scan compares it, lets another thread put first.

    That is the interleaving a thread switch inside the scan produces, made
    deterministic: the other thread's ``_obo_cache_put`` runs to completion
    between two steps of this thread's iteration.
    """

    def __init__(self) -> None:
        self.fired = False

    def __le__(self, _now: float) -> bool:
        if not self.fired:
            self.fired = True
            other = threading.Thread(
                target=auth._obo_cache_put, args=("from-another-thread", "b")
            )
            other.start()
            other.join()
        return False


def test_a_put_from_another_thread_during_the_scan_does_not_raise():
    future = time.time() + 300
    for i in range(auth._OBO_VALIDATION_CACHE_MAX - 1):
        auth._OBO_VALIDATION_CACHE[f"k{i}"] = (None, future)
    trap = _AnotherThreadWritesDuringTheScan()
    auth._OBO_VALIDATION_CACHE["trap"] = (None, trap)  # type: ignore[assignment]

    auth._obo_cache_put("mine", "a@example.com")

    assert trap.fired
    assert auth._obo_cache_get("mine") == ("a@example.com",)


def test_concurrent_puts_on_a_full_cache_do_not_raise():
    errors = []
    previous = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)

    def hammer(worker: int) -> None:
        try:
            for i in range(3000):
                auth._obo_cache_put(f"{worker}-{i}", None)
        except Exception as exc:  # noqa: BLE001 - the assertion reports it
            errors.append(exc)

    try:
        threads = [threading.Thread(target=hammer, args=(w,)) for w in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    finally:
        sys.setswitchinterval(previous)

    assert errors == []
