"""Memory — what a teamspace's agents remember, and how.

Read this package by lifecycle stage; each subpackage is one stage:

    config/       what the teamspace configured (the ``memory_backends`` rows):
                  which backend, the embedder, the Memory Tuning knobs
    storage/      where records live: LocalStorageBackend (SQLite, dev) and
                  LakebaseStorageBackend (pgvector, prod) behind one protocol,
                  the factory that builds one from a configuration, and the
                  adapter that lets the engine talk to either
    engine/       the ``Memory`` object itself — ``remember`` (classify,
                  consolidate, store) and ``recall`` (search, distil, explore)
                  over a ``MemoryRecord``
    run/          a run's memory: ``CrewMemoryService`` builds a ``Memory`` for
                  Chat, Agent Builder or Flow Builder; ``recall`` injects a
                  context block before a task; ``persist`` writes the result
                  after it, through the ``pending`` overlay and the
                  ``write_hygiene`` screen
    maintenance/  keeping the store small and true between runs: dedupe,
                  merge, supersede, forget — and the sweep that schedules them
    text.py       normalisation both sides and the storage adapter share

A record's path through a run: ``run.recall`` asks ``engine.Memory.recall``,
which searches ``storage`` — then the task runs — then ``run.persist`` hands
the output to ``engine.Memory.remember``, which labels it, folds it into a
near-duplicate if one exists, and saves it through ``storage``. Later,
``maintenance`` tidies what accumulated.

Memory is a capability, not a path: a chat turn recalls and persists exactly
as a crew does, and nothing here imports a path package. The full account,
including every tuning knob, is ``src/docs/MEMORY.md``.
"""

from src.services.memory.run.crew_memory import CrewMemoryService
from src.services.memory.storage.factory import MemoryBackendFactory

__all__ = [
    "CrewMemoryService",
    "MemoryBackendFactory",
]
