"""Where records live — the ``StorageBackend`` implementations and their glue.

* ``factory`` — ``MemoryBackendFactory``: a configuration → a backend, with the
  Memory Tuning scoring weights mapped onto it.
* ``adapter`` — ``EngineStorageAdapter``: the engine speaks query TEXT, the
  backends expect a query EMBEDDING; this absorbs that in one place.
* ``local`` — ``LocalStorageBackend``: one SQLite file per teamspace, numpy
  cosine search. Development.
* ``lakebase`` — ``LakebaseStorageBackend``: Postgres + pgvector with an HNSW
  index. Production. ``lakebase_schema`` self-heals its table, ``pg_codec``
  coerces values to and from its columns, ``bridge_loop`` is the one event
  loop its sync→async bridge runs on.
"""

from src.services.memory.storage.adapter import EngineStorageAdapter
from src.services.memory.storage.factory import MemoryBackendFactory
from src.services.memory.storage.lakebase import LakebaseStorageBackend
from src.services.memory.storage.local import LocalStorageBackend

__all__ = [
    "EngineStorageAdapter",
    "LakebaseStorageBackend",
    "LocalStorageBackend",
    "MemoryBackendFactory",
]
