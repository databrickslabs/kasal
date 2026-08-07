"""``EventPipeWriter.register`` must actually attach handlers.

It silently did not, for every crew and flow run. ``register()`` opened with
``from kasal_engine import events`` — a package that was flattened into the tree
and no longer exists — so it raised ImportError on its FIRST statement, before a
single ``bus.register_handler`` call. The subprocess pipe therefore carried
nothing, and live token streaming worked only on the in-process chat path.

Nothing failed loudly because both call sites wrap registration in
``except Exception`` and log "(non-fatal)". The observed symptom was one WARNING
per run:

    [SUBPROCESS] Event pipe registration failed (non-fatal):
        No module named 'kasal_engine'

so these tests assert the OUTCOME (handlers attached) rather than the absence of
an exception — a swallowed import error is invisible to the latter.
"""

import queue
import subprocess
import sys

from src.services.execution.event_pipe import _TRACE_EVENT_MAP, EventPipeWriter


class _RecordingBus:
    """Minimal stand-in for the engine event bus."""

    def __init__(self) -> None:
        self.registered: list[str] = []

    def register_handler(self, event_cls, handler) -> None:
        self.registered.append(event_cls.__name__)


class TestRegistrationAttachesHandlers:
    def test_the_token_chunk_handler_is_attached(self):
        """Without this one there is no live typing at all."""
        bus = _RecordingBus()
        EventPipeWriter(queue.Queue(), "exec-1").register(bus)

        assert "LLMStreamChunkEvent" in bus.registered

    def test_every_mapped_trace_event_is_attached(self):
        bus = _RecordingBus()
        EventPipeWriter(queue.Queue(), "exec-1").register(bus)

        missing = [name for name in _TRACE_EVENT_MAP if name not in bus.registered]
        assert not missing, f"trace events never registered: {missing}"

    def test_registration_attaches_something_at_all(self):
        """The regression was ZERO handlers, so guard the floor explicitly."""
        bus = _RecordingBus()
        EventPipeWriter(queue.Queue(), "exec-1").register(bus)

        assert len(bus.registered) >= len(_TRACE_EVENT_MAP) + 1


class TestItWorksWhereItActuallyRuns:
    def test_registration_succeeds_in_a_spawned_interpreter(self):
        """The failure only ever appeared in the crew/flow SUBPROCESS.

        In-process tests can pass while the spawned interpreter cannot resolve an
        import (see services/execution/CLAUDE.md), and this bug lived exactly
        there — so assert it in a real child process, not just in-process.
        """
        code = (
            "import queue\n"
            "from src.services.execution.event_pipe import EventPipeWriter\n"
            "class B:\n"
            "    def __init__(self): self.h = []\n"
            "    def register_handler(self, c, f): self.h.append(c.__name__)\n"
            "b = B()\n"
            "EventPipeWriter(queue.Queue(), 'x').register(b)\n"
            "assert 'LLMStreamChunkEvent' in b.h, b.h\n"
            "print(len(b.h))\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True
        )
        assert result.returncode == 0, result.stderr[-2000:]
        assert int(result.stdout.strip()) >= len(_TRACE_EVENT_MAP) + 1


class TestTheDeletedPackageStaysDeleted:
    def test_no_module_imports_kasal_engine(self):
        """``kasal_engine`` was flattened into the tree; nothing may import it.

        A stale import here is not a hard failure — it is a SILENT one, because
        the callers treat registration errors as non-fatal.
        """
        import pathlib

        src = pathlib.Path(__file__).resolve().parents[4] / "src"
        offenders = []
        for path in src.rglob("*.py"):
            for line in path.read_text(errors="ignore").splitlines():
                stripped = line.strip()
                if stripped.startswith(("from kasal_engine", "import kasal_engine")):
                    offenders.append(f"{path.relative_to(src)}: {stripped}")
        assert not offenders, (
            "kasal_engine no longer exists — these will raise ImportError at "
            "runtime, and where they are caught as non-fatal the feature simply "
            "stops working with no failure:\n  " + "\n  ".join(offenders)
        )
