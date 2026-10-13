"""The memory browser's read of a teamspace's graph: every entity, and the
facts between them including the ones a newer fact replaced.

Built from the teamspace's active configuration, not from a run, so it opens
the same store a run writes to without needing an embedder or an LLM — the
browser lists, it never searches.

Synchronous (the stores are); call it through ``asyncio.to_thread``.
"""

from __future__ import annotations

from src.schemas.memory_backend import (
    MemoryBackendConfig,
    MemoryBackendType,
    MemoryGraphEntity,
    MemoryGraphFact,
    MemoryGraphResponse,
)
from src.services.memory.engine.graph_types import GraphStore
from src.services.memory.storage.graph_factory import graph_enabled, graph_store_for
from src.utils.memory_paths import local_memory_store_dir


def _store_for(config: MemoryBackendConfig, group_id: str) -> GraphStore | None:
    root_scope = f"/{group_id}"
    lakebase = config.lakebase_config
    if config.backend_type == MemoryBackendType.LAKEBASE:
        if lakebase is None or not lakebase.memory_table:
            return None
        from src.services.memory.storage.lakebase import LakebaseStorageBackend

        return graph_store_for(
            LakebaseStorageBackend(
                table_name=lakebase.memory_table,
                crew_id="memory-browser",
                group_id=group_id,
                embedding_dimension=lakebase.embedding_dimension or 1024,
                instance_name=lakebase.instance_name,
            ),
            root_scope,
        )
    db_path = local_memory_store_dir(group_id) / "memory.db"
    if not db_path.exists():
        return None  # nothing written yet; do not create the file by looking
    from src.services.memory.storage.local_graph import LocalGraphStore

    return LocalGraphStore(db_path, scope=root_scope)


def graph_snapshot(
    config: MemoryBackendConfig | None, group_id: str, limit: int = 500
) -> MemoryGraphResponse:
    """Entities and facts for ``group_id``. Empty when graph memory is off."""
    backend = config.backend_type.value if config else "default"
    if config is None or not graph_enabled(config):
        return MemoryGraphResponse(backend=backend, enabled=False)
    store = _store_for(config, group_id)
    if store is None:
        return MemoryGraphResponse(backend=backend, enabled=True)
    try:
        entities = store.list_entities()
        facts = store.list_facts(limit=limit, include_retired=True)
    finally:
        store.close()
    return MemoryGraphResponse(
        backend=backend,
        enabled=True,
        entities=[
            MemoryGraphEntity(id=e.id, name=e.name, type=e.type) for e in entities
        ],
        facts=[
            MemoryGraphFact(
                id=f.id,
                subject_id=f.subject_id,
                subject=f.subject,
                relation=f.relation,
                object_id=f.object_id,
                object=f.object,
                statement=f.statement,
                source=f.source,
                created_at=f.created_at,
                valid_to=f.valid_to,
                superseded_by=f.superseded_by,
                execution_id=(
                    str(f.metadata["execution_id"])
                    if f.metadata.get("execution_id")
                    else None
                ),
                kind=f.kind,
            )
            for f in facts
        ],
    )
