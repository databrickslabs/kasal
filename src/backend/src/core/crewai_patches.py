"""CrewAI monkey-patches applied at Kasal boot time.

Currently installs one patch:

- **Memory event context propagation** — ensures ``MemorySaveCompletedEvent``,
  ``MemorySaveStartedEvent``, ``MemoryQueryCompletedEvent`` etc. carry
  ``agent_role`` / ``agent_id`` / ``task_id`` / ``task_name`` even when
  CrewAI's unified ``Memory`` emits them from a background save thread.

Why this is NOT a "last-seen" fallback:

  CrewAI 1.10+ runs ``Memory.remember_many()`` in a ``ThreadPoolExecutor``
  and — critically — already captures the caller's ``ContextVar`` state via
  ``contextvars.copy_context()`` when submitting the job (see
  ``crewai.memory.unified_memory.Memory._submit_save``). The background
  thread therefore inherits whatever ``ContextVar`` values were set in the
  caller at submit time.

  This module defines a ``ContextVar`` that Kasal's OTel event bridge keeps
  current (it's updated synchronously every time an agent/task/tool event
  fires, which is the same execution context the upcoming memory save runs
  in). We patch ``MemorySaveCompletedEvent.__init__`` so that, on the
  background thread, it reads the inherited context snapshot and fills in
  any fields CrewAI didn't populate itself. Explicit values passed by the
  caller are always respected.

  If a memory save happens outside any agent/task context (a standalone
  script calling ``Memory.remember()`` directly), the ContextVar is empty,
  the event stays free of agent/task attribution, and the trace reflects
  reality — no false attribution.
"""
from __future__ import annotations

import contextvars
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


# Fields we forward from the ContextVar into memory event constructors.
_FORWARDED_FIELDS = ("agent_role", "agent_id", "task_id", "task_name")

# Populated by the OTel event bridge on every task/agent/tool event it sees.
# Propagates into Memory's save thread pool automatically because CrewAI's
# ``Memory._submit_save`` uses ``contextvars.copy_context().run(fn)``.
memory_event_context: contextvars.ContextVar[Optional[dict]] = contextvars.ContextVar(
    "kasal_memory_event_context", default=None
)


def update_memory_event_context(**fields: Any) -> None:
    """Merge non-None fields into the current ``memory_event_context``.

    Called from the OTel event bridge whenever an incoming CrewAI event
    carries fresh agent / task provenance. Using ``set()`` (rather than
    mutating a dict in place) is intentional — ContextVars are immutable
    per-context, so ``set()`` creates a new context scope that subsequent
    code in the same context sees.
    """
    current = memory_event_context.get() or {}
    updated = dict(current)
    changed = False
    for key, value in fields.items():
        if value is None:
            continue
        if key not in _FORWARDED_FIELDS:
            continue
        if updated.get(key) != value:
            updated[key] = value
            changed = True
    if changed:
        memory_event_context.set(updated)


def install_memory_event_patches() -> bool:
    """Patch the MemorySave* / MemoryQuery* event classes.

    Returns:
        ``True`` if the patches were installed, ``False`` on any failure
        (e.g. CrewAI not importable). Safe to call multiple times — the
        sentinel attribute ``_kasal_context_patched`` prevents double-wrap.
    """
    try:
        from crewai.events.types import memory_events as _me
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Could not import crewai memory_events for patching: %s", exc)
        return False

    target_names = (
        "MemorySaveStartedEvent",
        "MemorySaveCompletedEvent",
        "MemorySaveFailedEvent",
        "MemoryQueryStartedEvent",
        "MemoryQueryCompletedEvent",
        "MemoryQueryFailedEvent",
        "MemoryRetrievalStartedEvent",
        "MemoryRetrievalCompletedEvent",
        "MemoryRetrievalFailedEvent",
    )

    patched = 0
    for name in target_names:
        cls = getattr(_me, name, None)
        if cls is None:
            continue
        if getattr(cls, "_kasal_context_patched", False):
            continue

        original_init = cls.__init__

        def _make_patched_init(orig_init):
            def _patched_init(self, **data: Any) -> None:  # type: ignore[no-untyped-def]
                ctx = memory_event_context.get()
                if ctx:
                    for field in _FORWARDED_FIELDS:
                        if data.get(field) is None and ctx.get(field):
                            data[field] = ctx[field]
                    # Promote the captured contents into the event's metadata
                    # so downstream consumers (OTel bridge, frontend) see
                    # what the agent actually wrote, not just "N memories
                    # saved".
                    saved = ctx.get("_kasal_saved_contents")
                    if saved:
                        md = data.get("metadata")
                        if md is None:
                            md = {}
                            data["metadata"] = md
                        if isinstance(md, dict):
                            md.setdefault("_kasal_saved_contents", saved)
                orig_init(self, **data)
            return _patched_init

        cls.__init__ = _make_patched_init(original_init)
        cls._kasal_context_patched = True  # type: ignore[attr-defined]
        patched += 1

    msg = (
        f"[KASAL-PATCH] Installed memory-event context patches on "
        f"{patched}/{len(target_names)} classes"
    )
    logger.info(msg)
    # In subprocesses, also print to stderr so the activity is visible even
    # before logging is wired up in the forked process. In the main process
    # the logger line above suffices — the raw print would just duplicate it.
    import os as _os
    if _os.environ.get("CREW_SUBPROCESS_MODE") == "true":
        import sys as _sys
        print(msg, file=_sys.stderr, flush=True)
    return patched > 0


def install_remember_many_patch() -> bool:
    """Capture the records being saved so the trace can show *what* was written.

    ``MemorySaveCompletedEvent`` in CrewAI's batch path only carries
    ``value="N memories saved"``. The actual content is in the ``contents``
    list passed to ``Memory.remember_many()``. We wrap that method to
    snapshot ``contents`` into the ContextVar *before* the bg submission,
    so the inherited context in the bg thread carries the payload and our
    patched ``__init__`` can stash it on the event's ``metadata`` dict.
    """
    try:
        from crewai.memory import unified_memory as _um
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Could not import unified_memory for patching: %s", exc)
        return False

    if getattr(_um.Memory, "_kasal_remember_many_patched", False):
        return True

    original_remember = _um.Memory.remember
    original_remember_many = _um.Memory.remember_many

    def _stash(contents: Any) -> None:
        try:
            if not contents:
                return
            if isinstance(contents, str):
                snapshot = [contents]
            else:
                snapshot = list(contents)
            current = memory_event_context.get() or {}
            updated = dict(current)
            # Cap the payload so we don't bloat span attributes.
            updated["_kasal_saved_contents"] = snapshot[:20]
            memory_event_context.set(updated)
        except Exception:
            pass

    def patched_remember(self, content, *args, **kwargs):  # type: ignore[no-untyped-def]
        _stash(content)
        return original_remember(self, content, *args, **kwargs)

    def patched_remember_many(self, contents, *args, **kwargs):  # type: ignore[no-untyped-def]
        _stash(contents)
        return original_remember_many(self, contents, *args, **kwargs)

    _um.Memory.remember = patched_remember
    _um.Memory.remember_many = patched_remember_many
    _um.Memory._kasal_remember_many_patched = True  # type: ignore[attr-defined]
    msg = "[KASAL-PATCH] Wrapped Memory.remember / remember_many for content capture"
    logger.info(msg)
    import os as _os
    if _os.environ.get("CREW_SUBPROCESS_MODE") == "true":
        import sys as _sys
        print(msg, file=_sys.stderr, flush=True)
    return True


def log_runtime_versions() -> str:
    """Log the actual installed versions of the CrewAI stack.

    Diagnostic for environment skew: a deployed app whose dependency build
    diverged from uv.lock (e.g. a stale requirements.txt, partial env reuse)
    produces version-mismatch failures like ``'Agent' object has no attribute
    'i18n'`` that are impossible to attribute without knowing the runtime
    versions. This line lands in every subprocess/app log.
    """
    import importlib
    import importlib.metadata

    versions = {}
    for pkg, dist in (
        ("crewai", "crewai"),
        ("crewai_core", "crewai-core"),
        ("litellm", "litellm"),
        ("mlflow", "mlflow"),
        ("openinference_crewai", "openinference-instrumentation-crewai"),
    ):
        try:
            # Not every package exposes __version__ (litellm doesn't) — fall
            # back to the installed distribution metadata.
            module = importlib.import_module(pkg) if "-" not in pkg else None
            versions[pkg] = (
                getattr(module, "__version__", None)
                or importlib.metadata.version(dist)
            )
        except Exception:
            try:
                versions[pkg] = importlib.metadata.version(dist)
            except Exception:
                versions[pkg] = "absent"
    summary = " ".join(f"{k}={v}" for k, v in versions.items())
    logger.info(f"[CrewAIPatches] Runtime versions: {summary}")
    return summary


def install_all_patches() -> None:
    """Entry point called once from the FastAPI lifespan startup."""
    log_runtime_versions()
    install_memory_event_patches()
    install_remember_many_patch()
