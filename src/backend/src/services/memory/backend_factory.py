"""Factory for building the unified-memory ``StorageBackend`` instance.

Under the legacy per-memory-type architecture this module returned a dict with
three wrappers (``short_term``/``long_term``/``entity``). The unified memory
system uses a single ``Memory`` class backed by one ``StorageBackend``, so this
factory returns a single instance (or ``None`` to signal that the local SQLite
backend should be built by CrewMemoryService).

**Memory runs on Lakebase.** Databricks Vector Search was a third memory
backend and has been retired: a ``databricks`` memory config now degrades to the
local store rather than building a VS-backed one. The Vector Search machinery
itself is untouched and still very much in use — ``DatabricksVectorStorage``,
``DatabricksVectorIndexRepository`` and the index schemas back the KNOWLEDGE and
documentation indexes, and the ``databricks`` config row is what those services
read their workspace/endpoint from. Only memory stopped using it.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from src.core.logger import LoggerManager
from src.schemas.memory_backend import MemoryBackendConfig, MemoryBackendType

logger = LoggerManager.get_instance().crew


class MemoryBackendFactory:
    """Builds a unified ``StorageBackend`` from a ``MemoryBackendConfig``."""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @staticmethod
    async def create_unified_storage(
        config: MemoryBackendConfig,
        crew_id: str,
        group_id: str,
        embedder: Optional[Any] = None,
        user_token: Optional[str] = None,
        job_id: Optional[str] = None,
        workspace_wide: bool = True,
        session_scope_id: Optional[str] = None,
    ) -> Optional[Any]:
        """Return a ``StorageBackend`` instance, or ``None`` for the local default.

        Args:
            config: Memory backend configuration loaded from the database.
            crew_id: Deterministic crew identifier (group-scoped). Used for
                tracing/identity and write tagging — NOT for read scoping.
            group_id: Tenant group identifier used to isolate memories.
            embedder: Optional embedder used when ``MemoryRecord.embedding`` is
                empty. Unified ``Memory`` usually embeds at the Memory layer
                and passes embeddings down, but we pass this through as a
                safety net.
            user_token: Optional user OBO token for Databricks.
            job_id: Optional execution/job id (per run).
            workspace_wide: READ scope. True = recall spans the whole workspace
                (group_id); False = recall is confined to ``session_scope_id``.
            session_scope_id: Stable chat-session id used as the backend
                ``session_id`` so session-only recall stays within one
                conversation across runs. Falls back to ``job_id``.

        Returns:
            A ``StorageBackend`` instance, or ``None`` when the local default
            applies (CrewMemoryService then builds the local SQLite backend).
        """
        if config.backend_type == MemoryBackendType.DATABRICKS:
            # Vector Search memory is retired. Degrade to the local store rather
            # than raising: the ``databricks`` config row is still what the
            # knowledge/documentation indexes read their workspace and endpoint
            # from, so a tenant can legitimately have one and must keep running.
            logger.warning(
                "Databricks Vector Search memory is retired — falling back to the "
                "local store for crew %s. Configure the Lakebase memory backend "
                "for persistent, shared memory.",
                crew_id,
            )
            return None
        if config.backend_type == MemoryBackendType.LAKEBASE:
            return MemoryBackendFactory._create_lakebase_backend(
                config=config,
                crew_id=crew_id,
                group_id=group_id,
                embedder=embedder,
                job_id=job_id,
                workspace_wide=workspace_wide,
                session_scope_id=session_scope_id,
            )
        if config.backend_type == MemoryBackendType.DEFAULT:
            logger.info(
                "Using DEFAULT memory configuration (local SQLite) for crew %s",
                crew_id,
            )
            return None
        logger.warning("Unsupported memory backend type: %s", config.backend_type)
        return None

    # ------------------------------------------------------------------
    # Lakebase
    # ------------------------------------------------------------------

    @staticmethod
    def _create_lakebase_backend(
        config: MemoryBackendConfig,
        crew_id: str,
        group_id: str,
        embedder: Optional[Any],
        job_id: Optional[str],
        workspace_wide: bool = True,
        session_scope_id: Optional[str] = None,
    ) -> Any:
        if not config.lakebase_config:
            raise ValueError("Lakebase configuration is required for Lakebase backend")

        lakebase_cfg = config.lakebase_config
        table_name = lakebase_cfg.memory_table
        if not table_name:
            raise ValueError(
                "LakebaseMemoryConfig.memory_table is required for the unified "
                "memory."
            )

        from src.services.memory.lakebase_storage_backend import (
            LakebaseStorageBackend,
        )

        logger.info(
            "Creating LakebaseStorageBackend (table=%s, crew_id=%s, group_id=%s)",
            table_name,
            crew_id,
            group_id,
        )
        return LakebaseStorageBackend(
            table_name=table_name,
            crew_id=crew_id,
            group_id=group_id,
            # session_id is the memory-partition key for session-only recall.
            # Prefer the stable chat-session id; fall back to the per-run job_id
            # for non-chat executions (which run workspace-wide anyway).
            session_id=session_scope_id or job_id,
            embedder=embedder,
            embedding_dimension=lakebase_cfg.embedding_dimension or 1024,
            instance_name=getattr(lakebase_cfg, "instance_name", None),
            workspace_wide=workspace_wide,
            **MemoryBackendFactory._scoring_kwargs(config),
        )

    @staticmethod
    def _scoring_kwargs(config: MemoryBackendConfig) -> Dict[str, Any]:
        """Map the UI's memory tuning knobs onto hybrid-scoring ctor params.

        Values left unset in the config fall through to the backend defaults
        (keyword_weight has no UI knob yet and always uses the default).
        """
        tuning = getattr(config, "cognitive_config", None)
        if tuning is None:
            return {}
        kwargs: Dict[str, Any] = {}
        for field in (
            "semantic_weight",
            "recency_weight",
            "importance_weight",
            "recency_half_life_days",
            "relevance_threshold",
        ):
            value = getattr(tuning, field, None)
            if value is not None:
                kwargs[field] = float(value)
        return kwargs

    # ------------------------------------------------------------------
    # Deprecated — legacy callers should migrate to ``create_unified_storage``
    # ------------------------------------------------------------------

    @staticmethod
    async def create_memory_backends(
        config: MemoryBackendConfig,
        crew_id: str,
        embedder: Optional[Any] = None,
        user_token: Optional[str] = None,
        job_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Legacy shim. Prefer :py:meth:`create_unified_storage`.

        Returns a dict with a single ``"unified"`` key pointing at the new
        backend, mirroring the shape of the old return value so migration
        callers can be updated incrementally.
        """
        # Extract group_id from the conventional ``{group_id}_crew_{hash}`` id.
        group_id = ""
        if crew_id and "_crew_" in crew_id:
            group_id = crew_id.split("_crew_")[0]
        storage = await MemoryBackendFactory.create_unified_storage(
            config=config,
            crew_id=crew_id,
            group_id=group_id,
            embedder=embedder,
            user_token=user_token,
            job_id=job_id,
        )
        return {"unified": storage} if storage is not None else {}
