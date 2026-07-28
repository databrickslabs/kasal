"""Memory: what a crew remembers, where it is stored, and how it is maintained.

This was split across two homes. The engine held the storage backends, the
vector store, the backend factory, maintenance, and the service that builds a
run's Memory (4,688 lines); ``src/services`` held the backend configuration,
the config service and the Lakebase service (1,598 lines). One concept, two
places, and the engine half unreachable from anything that was not a crew run.

Memory is a capability. A chat turn recalls and persists exactly as a crew does
— the light path already reuses these building blocks — and crew generation or
an exported app could too. The storage backends implement ``kasal_engine``'s
``StorageBackend`` interface and speak its ``MemoryRecord``/``ScopeInfo``
types, but that is a LIBRARY dependency, the same kind as a tool's ``BaseTool``:
it says what a backend must look like, not that a crew must be running.

``hooks`` is the run-side half: recall before a task, persist after it, driven
off the event bus. It says WHEN memory is touched during a run; the rest of this
package says what memory is.
"""

from src.services.memory.backend_factory import MemoryBackendFactory
from src.services.memory.crew_memory import CrewMemoryService

__all__ = [
    "CrewMemoryService",
    "MemoryBackendFactory",
]
