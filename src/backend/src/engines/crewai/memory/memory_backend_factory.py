"""Factory for building the unified-memory ``StorageBackend`` instance.

Under the legacy per-memory-type architecture this module returned a dict with
three wrappers (``short_term``/``long_term``/``entity``). CrewAI 1.10+ uses a
single ``Memory`` class backed by one ``StorageBackend``, so this factory now
returns a single instance (or ``None`` to signal that the CrewAI default
LanceDB backend should be used).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from src.core.logger import LoggerManager
from src.schemas.memory_backend import MemoryBackendConfig, MemoryBackendType

logger = LoggerManager.get_instance().crew


class DatabricksIndexValidationError(Exception):
    """Raised when Databricks Vector Search indexes are missing or provisioning.

    Carries ``validation_result`` so the crew-preparation layer can emit a
    trace event visible in the UI.
    """

    def __init__(self, message: str, validation_result: Dict[str, Any]):
        super().__init__(message)
        self.validation_result = validation_result
        self.error_type = validation_result.get("error_type", "unknown")
        self.missing_indexes = validation_result.get("missing_indexes", [])
        self.provisioning_indexes = validation_result.get("provisioning_indexes", [])


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
    ) -> Optional[Any]:
        """Return a ``StorageBackend`` instance, or ``None`` for CrewAI's default.

        Args:
            config: Memory backend configuration loaded from the database.
            crew_id: Deterministic crew identifier (group-scoped).
            group_id: Tenant group identifier used to isolate memories.
            embedder: Optional embedder used when ``MemoryRecord.embedding`` is
                empty. Unified ``Memory`` usually embeds at the Memory layer
                and passes embeddings down, but we pass this through as a
                safety net.
            user_token: Optional user OBO token for Databricks.
            job_id: Optional execution/job id used as ``session_id`` for
                short-term-style queries.

        Returns:
            A ``StorageBackend`` instance, or ``None`` when the unified
            ``Memory`` should fall back to its built-in LanceDB backend.

        Raises:
            DatabricksIndexValidationError: When the Databricks target index
                does not exist or is still provisioning.
        """
        if config.backend_type == MemoryBackendType.DATABRICKS:
            return await MemoryBackendFactory._create_databricks_backend(
                config=config,
                crew_id=crew_id,
                group_id=group_id,
                embedder=embedder,
                user_token=user_token,
                job_id=job_id,
            )
        if config.backend_type == MemoryBackendType.LAKEBASE:
            return MemoryBackendFactory._create_lakebase_backend(
                config=config,
                crew_id=crew_id,
                group_id=group_id,
                embedder=embedder,
                job_id=job_id,
            )
        if config.backend_type == MemoryBackendType.DEFAULT:
            logger.info(
                "Using CrewAI default unified memory (LanceDB) for crew %s",
                crew_id,
            )
            return None
        logger.warning("Unsupported memory backend type: %s", config.backend_type)
        return None

    # ------------------------------------------------------------------
    # Databricks
    # ------------------------------------------------------------------

    @staticmethod
    async def _create_databricks_backend(
        config: MemoryBackendConfig,
        crew_id: str,
        group_id: str,
        embedder: Optional[Any],
        user_token: Optional[str],
        job_id: Optional[str],
    ) -> Any:
        if not config.databricks_config:
            raise ValueError(
                "Databricks configuration is required for Databricks backend"
            )

        databricks_cfg = config.databricks_config
        index_name = databricks_cfg.memory_index
        if not index_name:
            raise ValueError(
                "DatabricksMemoryConfig.memory_index is required for the unified "
                "cognitive memory."
            )

        await MemoryBackendFactory._validate_databricks_index(
            workspace_url=databricks_cfg.workspace_url,
            endpoint_name=databricks_cfg.endpoint_name,
            index_name=index_name,
            user_token=user_token,
            group_id=group_id,
        )

        from src.engines.crewai.memory.databricks_storage_backend import (
            DatabricksStorageBackend,
        )

        logger.info(
            "Creating DatabricksStorageBackend (index=%s, crew_id=%s, group_id=%s)",
            index_name,
            crew_id,
            group_id,
        )
        return DatabricksStorageBackend(
            index_name=index_name,
            endpoint_name=databricks_cfg.endpoint_name,
            workspace_url=databricks_cfg.workspace_url or "",
            crew_id=crew_id,
            group_id=group_id,
            user_token=user_token,
            session_id=job_id,
            embedder=embedder,
            embedding_dimension=databricks_cfg.embedding_dimension or 1024,
        )

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
    ) -> Any:
        if not config.lakebase_config:
            raise ValueError(
                "Lakebase configuration is required for Lakebase backend"
            )

        lakebase_cfg = config.lakebase_config
        table_name = lakebase_cfg.memory_table
        if not table_name:
            raise ValueError(
                "LakebaseMemoryConfig.memory_table is required for the unified "
                "cognitive memory."
            )

        from src.engines.crewai.memory.lakebase_storage_backend import (
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
            session_id=job_id,
            embedder=embedder,
            embedding_dimension=lakebase_cfg.embedding_dimension or 1024,
            instance_name=getattr(lakebase_cfg, "instance_name", None),
        )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @staticmethod
    async def _validate_databricks_index(
        *,
        workspace_url: Optional[str],
        endpoint_name: str,
        index_name: str,
        user_token: Optional[str],
        group_id: Optional[str],
    ) -> None:
        """Raise ``DatabricksIndexValidationError`` if the index is not ready."""
        if not workspace_url or not endpoint_name or not index_name:
            return

        from src.repositories.databricks_vector_index_repository import (
            DatabricksVectorIndexRepository,
        )

        repository = DatabricksVectorIndexRepository(workspace_url, group_id=group_id)
        try:
            response = await repository.describe_index(
                index_name, endpoint_name, user_token
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.error(
                "Failed to describe Databricks index %s: %s", index_name, exc
            )
            validation_result = {
                "error_type": "describe_failed",
                "missing_indexes": [index_name],
                "provisioning_indexes": [],
                "error_message": str(exc),
            }
            raise DatabricksIndexValidationError(
                f"Could not describe Databricks index '{index_name}': {exc}",
                validation_result,
            ) from exc

        if not response.get("success"):
            validation_result = {
                "error_type": "missing_index",
                "missing_indexes": [index_name],
                "provisioning_indexes": [],
                "error_message": response.get("message", "Index not found"),
            }
            raise DatabricksIndexValidationError(
                f"Databricks index '{index_name}' not found", validation_result
            )

        description = response.get("description", {})
        status = description.get("status", {})
        state = (status.get("state") or "UNKNOWN").upper()
        is_ready = status.get("ready") or state in {"ONLINE", "READY"}

        if is_ready:
            logger.info(
                "Databricks unified memory index is READY: %s (state=%s)",
                index_name,
                state,
            )
            return

        if state in {"PROVISIONING", "PENDING", "CREATING"}:
            validation_result = {
                "error_type": "provisioning_indexes",
                "missing_indexes": [],
                "provisioning_indexes": [index_name],
                "error_message": (
                    f"Databricks index '{index_name}' is still provisioning "
                    f"(state={state}). Retry once the index reaches ONLINE."
                ),
            }
            raise DatabricksIndexValidationError(
                validation_result["error_message"], validation_result
            )

        validation_result = {
            "error_type": "unexpected_state",
            "missing_indexes": [index_name],
            "provisioning_indexes": [],
            "error_message": f"Databricks index '{index_name}' in unexpected state {state}",
        }
        raise DatabricksIndexValidationError(
            validation_result["error_message"], validation_result
        )

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
