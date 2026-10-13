"""Build a ``GraphMemory`` from the record backend a run already resolved.

Graph memory lives in the SAME store as record memory — the teamspace's SQLite
file in development, its Lakebase instance in production — so the graph store is
derived from the record backend rather than configured separately: same file or
instance, same embedder, same tenant, same session scoping.
"""

from __future__ import annotations

from collections.abc import Mapping

from src.schemas.memory_backend import MemoryBackendConfig
from src.services.memory.engine.graph_memory import GraphMemory
from src.services.memory.engine.graph_types import (
    DEFAULT_NOTE_RETENTION_DAYS,
    GraphStore,
)
from src.services.memory.storage.local import LocalStorageBackend


def graph_enabled(memory_config: MemoryBackendConfig) -> bool:
    """Whether the teamspace's Memory Tuning turned graph memory on."""
    tuning = memory_config.cognitive_config
    return tuning is not None and tuning.graph_memory is True


def graph_store_for(
    backend: object,
    root_scope: str,
    session_id: str | None = None,
    workspace_wide: bool = True,
    note_retention_days: float = DEFAULT_NOTE_RETENTION_DAYS,
) -> GraphStore:
    """The graph store beside ``backend`` (a local or Lakebase record backend)."""
    if isinstance(backend, LocalStorageBackend):
        from src.services.memory.storage.local_graph import LocalGraphStore

        return LocalGraphStore(
            backend.db_path,
            embedder=backend.embedder,
            scope=root_scope,
            session_id=session_id,
            workspace_wide=workspace_wide,
            note_retention_days=note_retention_days,
        )
    from src.services.memory.storage.lakebase import LakebaseStorageBackend
    from src.services.memory.storage.lakebase_graph import LakebaseGraphStore

    if not isinstance(backend, LakebaseStorageBackend):
        raise TypeError(f"No graph store for {type(backend).__name__}")
    return LakebaseGraphStore(
        table_name=backend.table_name,
        group_id=backend.group_id,
        open_session=backend.open_session,
        session_id=backend.session_id,
        embedder=backend.embedder,
        embedding_dimension=backend.embedding_dimension,
        workspace_wide=backend.workspace_wide,
        note_retention_days=note_retention_days,
    )


def build_graph_memory(
    backend: object,
    memory_kwargs: Mapping[str, object],
    memory_config: MemoryBackendConfig,
    run_config: Mapping[str, object],
) -> GraphMemory:
    """``memory_kwargs`` is what ``Memory`` would have been built from; the
    parts a fact store uses are taken from it so both kinds of memory resolve
    their LLM, scope and screening the same way."""
    tuning = memory_config.cognitive_config
    root_scope = str(memory_kwargs.get("root_scope") or "/")
    session_id = run_config.get("session_id")
    workspace_scope = run_config.get("memory_workspace_scope")
    return GraphMemory(
        graph_store_for(
            backend,
            root_scope,
            session_id=session_id if isinstance(session_id, str) else None,
            workspace_wide=True if workspace_scope is None else bool(workspace_scope),
            # The existing "keep run and chat memories (days)" knob.
            note_retention_days=(
                tuning.episodic_ttl_days
                if tuning and tuning.episodic_ttl_days
                else DEFAULT_NOTE_RETENTION_DAYS
            ),
        ),
        llm=memory_kwargs.get("llm"),
        root_scope=root_scope,
        min_similarity=tuning.relevance_threshold if tuning else None,
        write_screening=str(memory_kwargs.get("write_screening") or "quarantine"),
    )
