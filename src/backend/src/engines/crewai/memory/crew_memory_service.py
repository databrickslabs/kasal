"""
Crew memory service for handling memory backend configuration and setup.

This service centralizes all memory-related logic including:
- Memory backend configuration fetching
- Crew ID generation
- Storage directory setup
- Memory component configuration
- Memory tracing and context setup
"""

import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from src.core.logger import LoggerManager
from src.engines.crewai.memory.memory_backend_factory import (
    DatabricksIndexValidationError,
    MemoryBackendFactory,
)
from src.schemas.memory_backend import MemoryBackendConfig, MemoryBackendType
from src.utils.memory_paths import local_memory_store_dir

logger = LoggerManager.get_instance().crew


class CrewMemoryService:
    """Handles all crew memory backend configuration and setup"""

    def __init__(self, config: Dict[str, Any], user_token: Optional[str] = None):
        """
        Initialize the CrewMemoryService

        Args:
            config: Crew configuration dictionary
            user_token: Optional user access token for OBO authentication
        """
        self.config = config
        self.user_token = user_token
        self._original_storage_dir = None

    async def fetch_memory_backend_config(self) -> Optional[Dict[str, Any]]:
        """
        Fetch memory backend configuration from database

        Returns:
            Memory backend configuration dict or None
        """
        logger.info("=" * 80)
        logger.info("FETCH_MEMORY_BACKEND_CONFIG CALLED")
        logger.info("=" * 80)
        try:
            from src.db.session import request_scoped_session
            from src.services.memory_backend_service import MemoryBackendService

            async with request_scoped_session() as session:
                service = MemoryBackendService(session)
                group_id = self.config.get("group_id")
                logger.info(
                    f"[fetch_memory_backend_config] Fetching config for group_id: {group_id}"
                )

                active_config = await service.get_active_config(group_id)
                if not active_config:
                    logger.warning(
                        "No active memory backend configuration found in database"
                    )
                    return None

                logger.info(
                    "Found active memory config: backend_type=%s",
                    active_config.backend_type,
                )

                # Convert to dict format for the factory. Cognitive tuning
                # flows through ``cognitive_config`` when set.
                memory_backend_config = {
                    "backend_type": active_config.backend_type.value,
                    "databricks_config": active_config.databricks_config,
                    "lakebase_config": active_config.lakebase_config,
                }
                cognitive_cfg = getattr(active_config, "cognitive_config", None)
                if cognitive_cfg is not None:
                    memory_backend_config["cognitive_config"] = (
                        cognitive_cfg.model_dump(exclude_none=True)
                        if hasattr(cognitive_cfg, "model_dump")
                        else cognitive_cfg
                    )
                custom_cfg = getattr(active_config, "custom_config", None)
                if custom_cfg:
                    memory_backend_config["custom_config"] = custom_cfg
                logger.info(
                    "Loaded memory backend config from database: %s",
                    memory_backend_config["backend_type"],
                )
                return memory_backend_config

        except Exception as e:
            logger.warning(f"Failed to load memory backend config from database: {e}")
            return None

    def generate_crew_id(self) -> str:
        """
        Generate a deterministic crew ID based on crew configuration.

        SECURITY: All crew_ids are prefixed with group_id to ensure complete
        tenant isolation. Group A cannot access Group B's memory even if they
        have identical crew configurations.

        Returns:
            Crew ID string (always prefixed with group_id for isolation)
        """
        # SECURITY: Always get group_id first for tenant isolation
        group_id = self.config.get("group_id") or "default"

        # Use provided crew_id if available (but always prefix with group_id for isolation)
        if self.config.get("crew_id"):
            provided_crew_id = self.config.get("crew_id")
            # SECURITY: Ensure group_id prefix for tenant isolation
            if not provided_crew_id.startswith(f"{group_id}_"):
                crew_id = f"{group_id}_{provided_crew_id}"
                logger.info(
                    f"Added group_id prefix to provided crew_id for tenant isolation: {crew_id}"
                )
            else:
                crew_id = provided_crew_id
            return crew_id

        # Check for database crew_id (always prefix with group_id for isolation)
        db_crew_id = self.config.get("database_crew_id")
        if db_crew_id:
            # SECURITY: Include group_id to prevent cross-tenant memory access
            crew_id = f"{group_id}_crew_db_{db_crew_id}"
            logger.info(f"Using database crew_id with group isolation: {crew_id}")
            return crew_id

        # Generate hash-based crew_id from configuration
        agents = self.config.get("agents", [])
        tasks = self.config.get("tasks", [])

        # Create sorted lists for stable hashing
        agent_roles = sorted(
            [agent.get("role", "") for agent in agents if isinstance(agent, dict)]
        )
        task_names = sorted(
            [
                task.get("name", task.get("description", "")[:50])
                for task in tasks
                if isinstance(task, dict)
            ]
        )

        # NOTE: run_name is intentionally NOT used for crew_id generation
        # run_name was previously included in the hash but this caused a bug where
        # each execution got a different crew_id (because run_name is auto-generated
        # uniquely for each execution). This meant Long-Term Memory was stored in different
        # directories and couldn't be found on subsequent runs.
        # The crew_id should be deterministic based on the CREW STRUCTURE (agents, tasks,
        # crew_name, model, group_id), not the execution instance (run_name).

        # Create stable identifier for hashing
        # SECURITY: group_id is included to ensure tenant isolation
        crew_identifier = {
            "agent_roles": agent_roles,
            "task_names": task_names,
            "crew_name": self.config.get(
                "name", self.config.get("crew", {}).get("name", "unnamed_crew")
            ),
            "model": self.config.get("model", "default"),
            "group_id": group_id,  # Already defaults to 'default' at top of function
        }

        # Create hash and prefix with group_id for guaranteed tenant isolation
        crew_identifier_json = json.dumps(crew_identifier, sort_keys=True)
        crew_hash = hashlib.md5(crew_identifier_json.encode()).hexdigest()[:8]
        crew_id = f"{group_id}_crew_{crew_hash}"

        # Detailed logging
        logger.info("=" * 80)
        logger.info("CREW_ID GENERATION - DETAILED DEBUG INFO")
        logger.info("=" * 80)
        logger.info(f"Crew Identifier Components (used for memory persistence):")
        logger.info(f"  - Agent Roles: {agent_roles}")
        logger.info(f"  - Task Names: {task_names}")
        logger.info(f"  - Crew Name: {crew_identifier['crew_name']}")
        logger.info(f"  - Model: {crew_identifier['model']}")
        logger.info(f"  - Group ID: {group_id} (SECURITY: ensures tenant isolation)")
        logger.info(
            f"  NOTE: run_name is NOT included - memory persists across runs with same crew structure"
        )
        logger.info(f"JSON for hashing (sorted): {crew_identifier_json}")
        logger.info(
            f"MD5 Hash: {hashlib.md5(crew_identifier_json.encode()).hexdigest()}"
        )
        logger.info(f"Hash (first 8 chars): {crew_hash}")
        logger.info(f"Generated crew_id: {crew_id}")
        logger.info(
            f"SECURITY: Memory is isolated by group_id - {group_id} cannot access other groups' memory"
        )
        logger.info(
            f"This crew_id will persist across ALL runs with the SAME crew configuration"
        )
        logger.info("=" * 80)

        return crew_id

    def setup_storage_directory(
        self, crew_id: str, memory_backend_config: Optional[Dict[str, Any]]
    ) -> None:
        """
        Setup custom storage directory for memory backends

        Args:
            crew_id: Crew identifier
            memory_backend_config: Memory backend configuration
        """
        if not memory_backend_config:
            return

        backend_type = memory_backend_config.get("backend_type")
        if backend_type not in ["databricks", "default", "lakebase"]:
            return

        # Save original value
        self._original_storage_dir = os.environ.get("CREWAI_STORAGE_DIR")

        # Set unique directory name
        if backend_type == "databricks":
            storage_dirname = f"kasal_databricks_{crew_id}"
        elif backend_type == "lakebase":
            storage_dirname = f"kasal_lakebase_{crew_id}"
        else:
            storage_dirname = self._default_storage_dirname()

        os.environ["CREWAI_STORAGE_DIR"] = storage_dirname

        # Detailed logging
        logger.info("=" * 80)
        logger.info("STORAGE PATH CONFIGURATION - DETAILED DEBUG INFO")
        logger.info("=" * 80)
        logger.info(f"Backend Type: {backend_type}")
        logger.info(
            f"CREWAI_STORAGE_DIR environment variable set to: {storage_dirname}"
        )

        # Check storage path
        from crewai.utilities.paths import db_storage_path

        storage_path = Path(db_storage_path())
        logger.info(f"Full storage path resolved by CrewAI: {storage_path.absolute()}")
        logger.info(f"Storage path exists: {storage_path.exists()}")

        if storage_path.exists():
            try:
                contents = list(storage_path.iterdir())
                logger.info(f"Storage directory contains {len(contents)} items:")
                for item in contents[:10]:
                    logger.info(
                        f"  - {item.name} ({'dir' if item.is_dir() else 'file'})"
                    )
                if len(contents) > 10:
                    logger.info(f"  ... and {len(contents) - 10} more items")
            except Exception as e:
                logger.warning(f"Could not list storage directory contents: {e}")
            logger.info("Memory will persist from previous runs")
        else:
            logger.info(
                "Creating NEW storage directory - this is the FIRST run with this configuration"
            )
        logger.info("=" * 80)

    def _default_storage_dirname(self) -> str:
        """Absolute storage dir for LOCAL (DEFAULT) memory — ONE store per group.

        Deterministic path under the known memory root (``KASAL_MEMORY_DIR``,
        default ``~/.kasal/memory``), outside the backend source tree, so the
        runtime writer and the memory browser always read/write the same place
        (CrewAI resolves a *relative* ``CREWAI_STORAGE_DIR`` inconsistently).

        Neither ``crew_id`` nor ``session_id`` is part of the directory:
        ``crew_id`` changes every chat prompt (would orphan recall), and session
        scoping is encoded in the record scope path (see ``_build_memory_kwargs``)
        so a session record stays visible workspace-wide — mirroring ChatMode.
        """
        group_id = self.config.get("group_id") or "default"
        return str(local_memory_store_dir(group_id))

    async def create_unified_storage(
        self, memory_backend_config: Dict[str, Any], crew_id: str, embedder: Any
    ) -> Optional[Any]:
        """Build the unified ``StorageBackend`` for this crew.

        Args:
            memory_backend_config: Memory backend configuration (dict from DB).
            crew_id: Deterministic crew identifier (already group-scoped).
            embedder: Embedder callable or provider config dict.

        Returns:
            A ``StorageBackend`` instance, or ``None`` when the CrewAI default
            (LanceDB) storage should be used.

        Raises:
            DatabricksIndexValidationError: If the Databricks index is missing
                or still provisioning.
        """
        if "databricks_config" in memory_backend_config and isinstance(
            memory_backend_config["databricks_config"], dict
        ):
            from src.schemas.memory_backend import DatabricksMemoryConfig

            memory_backend_config["databricks_config"] = DatabricksMemoryConfig(
                **memory_backend_config["databricks_config"]
            )

        if "lakebase_config" in memory_backend_config and isinstance(
            memory_backend_config["lakebase_config"], dict
        ):
            from src.schemas.memory_backend import LakebaseMemoryConfig

            memory_backend_config["lakebase_config"] = LakebaseMemoryConfig(
                **memory_backend_config["lakebase_config"]
            )

        memory_config = MemoryBackendConfig(**memory_backend_config)

        logger.info(
            "Creating unified memory storage for crew %s (backend=%s)",
            crew_id,
            memory_config.backend_type,
        )

        job_id = self.config.get("execution_id") or self.config.get("job_id")
        if job_id:
            logger.info(
                "Using job_id=%s as session_id for short-term-scoped queries",
                job_id,
            )

        group_id = self.config.get("group_id") or ""
        if not group_id and crew_id and "_crew_" in crew_id:
            group_id = crew_id.split("_crew_")[0]

        # Memory READ scope from the chat "Workspace memory" toggle (default ON =
        # workspace-wide). When the user turns it off, the frontend sends
        # memory_workspace_scope=False, so recall is restricted to this chat
        # SESSION (session_id) instead of the whole workspace. crew_id is only
        # used for tracing/identity — never for read scoping.
        workspace_wide = bool(self.config.get("memory_workspace_scope", True))
        # Stable chat-session id used to partition session-only recall. Falls
        # back to the per-run job_id for non-chat runs (always workspace-wide).
        session_scope_id = self.config.get("session_id") or job_id
        logger.info(
            "Memory read scope: %s (session_id=%s, crew_id=%s)",
            "workspace" if workspace_wide else "session-only",
            session_scope_id,
            crew_id,
        )

        try:
            return await MemoryBackendFactory.create_unified_storage(
                config=memory_config,
                crew_id=crew_id,
                group_id=group_id,
                embedder=embedder,
                user_token=self.user_token,
                job_id=job_id,
                workspace_wide=workspace_wide,
                session_scope_id=session_scope_id,
            )
        except DatabricksIndexValidationError as e:
            await self._emit_index_validation_trace(e)
            raise

    async def create_memory_backends(
        self, memory_backend_config: Dict[str, Any], crew_id: str, embedder: Any
    ) -> Dict[str, Any]:
        """Legacy shim — prefer :py:meth:`create_unified_storage`.

        Returns ``{"unified": storage}`` when a backend is configured, ``{}``
        when the CrewAI default should be used.
        """
        storage = await self.create_unified_storage(
            memory_backend_config, crew_id, embedder
        )
        return {"unified": storage} if storage is not None else {}

    async def _emit_index_validation_trace(
        self, error: DatabricksIndexValidationError
    ) -> None:
        """
        Emit a trace event for Databricks index validation errors.

        This makes the error visible in the UI trace view.
        """
        try:
            from datetime import datetime, timezone

            from src.db.session import request_scoped_session
            from src.services.execution_trace_service import ExecutionTraceService

            # Get job_id from config
            job_id = self.config.get("execution_id") or self.config.get("job_id")
            if not job_id:
                logger.warning(
                    "No job_id available for trace emission, skipping trace event"
                )
                return

            # Build the trace content based on error type
            if error.error_type == "missing_indexes":
                title = "⚠️ DATABRICKS MEMORY ERROR: Indexes Not Found"
                content_lines = [
                    "The following Databricks Vector Search indexes are configured but do not exist:",
                    "",
                ]
                for idx in error.missing_indexes:
                    content_lines.append(f"  ✗ {idx}")
                content_lines.extend(
                    [
                        "",
                        "RECOMMENDATION:",
                        "  1. Create the missing indexes in Databricks",
                        "  2. OR disable Databricks memory backend in settings",
                        "  3. OR use default CrewAI memory (ChromaDB + SQLite)",
                    ]
                )
            elif error.error_type == "provisioning_indexes":
                title = "⏳ DATABRICKS MEMORY ERROR: Indexes Still Provisioning"
                content_lines = [
                    "The following Databricks Vector Search indexes are still being provisioned:",
                    "",
                ]
                for idx in error.provisioning_indexes:
                    content_lines.append(f"  ⏳ {idx}")
                content_lines.extend(
                    [
                        "",
                        "Memory operations will FAIL until indexes are ready.",
                        "",
                        "RECOMMENDATION:",
                        "  1. Wait for indexes to finish provisioning (check Databricks UI)",
                        "  2. OR disable Databricks memory backend in settings temporarily",
                        "  3. OR disable memory on all agents until indexes are ready",
                    ]
                )
            else:
                title = "⚠️ DATABRICKS MEMORY ERROR"
                content_lines = [str(error)]

            content = "\n".join(content_lines)

            # Create trace data
            trace_data = {
                "job_id": job_id,
                "event_source": "Memory Backend",
                "event_context": "databricks_index_validation",
                "event_type": "memory_backend_error",
                "created_at": datetime.now(timezone.utc).replace(tzinfo=None),
                "output": {
                    "content": content,
                    "extra_data": {
                        "error_type": error.error_type,
                        "validation_result": error.validation_result,
                        "title": title,
                        "severity": "error",
                    },
                },
                "trace_metadata": {
                    "error_type": error.error_type,
                    "missing_indexes": error.missing_indexes,
                    "provisioning_indexes": error.provisioning_indexes,
                    "title": title,
                    "severity": "error",
                },
            }

            # Add group context if available
            group_id = self.config.get("group_id")
            if group_id:
                trace_data["group_id"] = group_id

            # Create the trace
            async with request_scoped_session() as session:
                trace_service = ExecutionTraceService(session)
                await trace_service.create_trace(trace_data)
                await session.commit()
                logger.info(
                    f"Emitted memory backend validation error trace for job {job_id}"
                )

        except Exception as trace_error:
            # Don't fail the main operation if trace emission fails
            logger.warning(f"Failed to emit index validation trace: {trace_error}")

    def configure_crew_memory_components(
        self,
        crew_kwargs: Dict[str, Any],
        memory_config: MemoryBackendConfig,
        storage: Optional[Any],
        crew_id: str,
        custom_embedder: Any = None,
        memory_llm_override: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Configure the crew's unified ``Memory`` instance.

        Replaces the legacy three-class setup. Instantiates a single
        ``crewai.memory.Memory`` bound to the Kasal-specific ``StorageBackend``
        (or signals CrewAI to use its default LanceDB backend).

        Args:
            crew_kwargs: Crew keyword arguments to update.
            memory_config: Resolved memory backend configuration.
            storage: ``StorageBackend`` instance returned by the factory, or
                ``None`` to fall back to the CrewAI default.
            crew_id: Deterministic crew identifier (used as root scope prefix).
            custom_embedder: Optional Databricks/other custom embedder that
                should take precedence over ``crew_kwargs["embedder"]``.

        Returns:
            Updated ``crew_kwargs``.
        """
        # DEFAULT backend with no usable embedder → disable entirely to avoid
        # the Memory LLM/embedder path hitting the OpenAI placeholder key.
        if (
            memory_config.backend_type == MemoryBackendType.DEFAULT
            and not custom_embedder
            and not crew_kwargs.get("embedder")
            and not os.environ.get("OPENAI_API_KEY")
        ):
            logger.warning(
                "DEFAULT memory backend selected but no embedder / OpenAI key "
                "available. Disabling memory to prevent fallback errors."
            )
            crew_kwargs["memory"] = False
            return crew_kwargs

        try:
            from crewai.memory import Memory
        except ImportError as exc:
            logger.error("CrewAI unified Memory class unavailable: %s", exc)
            logger.warning("Falling back to memory=False")
            crew_kwargs["memory"] = False
            return crew_kwargs

        # DEFAULT backend → let Memory use its own LanceDB backend, but pass
        # the crew's embedder/LLM so we don't implicitly require OPENAI_API_KEY.
        if storage is None:
            memory_kwargs = self._build_memory_kwargs(
                crew_kwargs=crew_kwargs,
                custom_embedder=custom_embedder,
                crew_id=crew_id,
                memory_config=memory_config,
                memory_llm_override=memory_llm_override,
            )
            try:
                crew_kwargs["memory"] = Memory(**memory_kwargs)
                logger.info(
                    "Configured unified Memory with CrewAI default storage (crew=%s)",
                    crew_id,
                )
            except Exception as exc:
                logger.error("Failed to build unified Memory: %s", exc)
                crew_kwargs["memory"] = False
            self._attach_crew_memory_to_agents(crew_kwargs)
            return crew_kwargs

        # Custom backend (Databricks / Lakebase) → wire storage into Memory.
        memory_kwargs = self._build_memory_kwargs(
            crew_kwargs=crew_kwargs,
            custom_embedder=custom_embedder,
            crew_id=crew_id,
            memory_config=memory_config,
            memory_llm_override=memory_llm_override,
        )
        memory_kwargs["storage"] = storage
        try:
            crew_kwargs["memory"] = Memory(**memory_kwargs)
            logger.info(
                "Configured unified Memory with %s storage (crew=%s)",
                memory_config.backend_type.value,
                crew_id,
            )
        except Exception as exc:
            logger.error("Failed to build unified Memory with custom storage: %s", exc)
            crew_kwargs["memory"] = False
        self._attach_crew_memory_to_agents(crew_kwargs)
        return crew_kwargs

    def _attach_crew_memory_to_agents(self, crew_kwargs: Dict[str, Any]) -> None:
        """Point each agent's ``memory`` at the crew's unified Memory instance.

        CrewAI's per-task auto-save (``Agent._save_kickoff_to_memory``, run when
        each agent finishes a task) and the recall/save tools both read
        ``agent.memory`` — NOT the crew memory. We deliberately keep agents free
        of ``memory=True`` (which would spin up a per-agent OpenAI-default
        Memory and cause 401s), but that also makes the per-task auto-save a
        no-op. Assigning the already-built crew Memory *instance* restores
        per-task writes on the configured Databricks/Lakebase backend without
        creating a default OpenAI memory.
        """
        mem = crew_kwargs.get("memory")
        if mem in (True, False, None):
            return
        for agent in crew_kwargs.get("agents", []) or []:
            try:
                agent.memory = mem
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug("Could not attach crew memory to agent: %s", exc)

    # ------------------------------------------------------------------
    # Memory kwarg helpers
    # ------------------------------------------------------------------

    def _build_memory_kwargs(
        self,
        crew_kwargs: Dict[str, Any],
        custom_embedder: Any,
        crew_id: str,
        memory_config: MemoryBackendConfig,
        memory_llm_override: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Assemble keyword arguments for the unified ``Memory`` class.

        Sources the embedder, LLM (to avoid OpenAI fallback), and cognitive
        scoring weights from ``memory_config.custom_config`` when present.
        """
        kwargs: Dict[str, Any] = {}

        embedder = custom_embedder or crew_kwargs.get("embedder")
        if embedder is not None:
            kwargs["embedder"] = embedder

        # Root scope mirrors ChatMode's workspace/session toggle. group_id is
        # ALWAYS the tenant boundary; the toggle decides whether we ALSO narrow
        # to the chat session:
        #   - workspace (default): root_scope = /<group>  → recall sees every
        #     session in the workspace (session sub-scopes nest under it).
        #   - session-only: root_scope = /<group>/<session> → recall is confined
        #     to this chat session, yet the record stays visible workspace-wide
        #     because it nests under /<group>.
        # For the custom Databricks/Lakebase backends this same intent is enforced
        # by their tenant WHERE clause; for the local LanceDB store the scope PATH
        # is the lever. crew_id is never part of the scope (it changes per prompt
        # and would wall every run off from the rest of the workspace).
        group_id = self.config.get("group_id") or "default"
        workspace_wide = bool(self.config.get("memory_workspace_scope", True))
        session_id = self.config.get("session_id")
        if workspace_wide or not session_id:
            kwargs["root_scope"] = f"/{group_id}"
        else:
            kwargs["root_scope"] = f"/{group_id}/{session_id}"

        cognitive = getattr(memory_config, "cognitive_config", None)
        cognitive_dict = (
            cognitive.model_dump(exclude_none=True)
            if cognitive is not None and hasattr(cognitive, "model_dump")
            else (cognitive or {})
        )

        # ``memory_llm_model`` is a bare model key, NOT a Memory ctor param. Drop
        # it from the cognitive kwargs and never pass it to Memory as a string:
        # CrewAI would build an unconfigured ``LLM(model=<name>)`` with no provider
        # prefix or credentials, which litellm routes to OpenAI (401 on the
        # placeholder key). Use the pre-resolved, fully-configured instance from
        # ``resolve_memory_llm_override``; otherwise fall back to the crew's LLM.
        cognitive_dict.pop("memory_llm_model", None)
        if memory_llm_override is not None:
            kwargs["llm"] = memory_llm_override
        else:
            memory_llm = self._resolve_memory_llm(crew_kwargs)
            if memory_llm is not None:
                kwargs["llm"] = memory_llm

        for key in (
            "recency_weight",
            "semantic_weight",
            "importance_weight",
            "recency_half_life_days",
            "consolidation_threshold",
            "consolidation_limit",
            "default_importance",
            "confidence_threshold_high",
            "confidence_threshold_low",
            "complex_query_threshold",
            "exploration_budget",
            "query_analysis_threshold",
        ):
            if key in cognitive_dict and cognitive_dict[key] is not None:
                kwargs[key] = cognitive_dict[key]
        return kwargs

    def _resolve_memory_llm(self, crew_kwargs: Dict[str, Any]) -> Optional[Any]:
        """Pick an LLM for memory analysis so we don't implicitly need OpenAI.

        Preference order: explicit crew ``manager_llm`` > first agent's ``llm``
        > ``None`` (lets ``Memory`` default to ``gpt-4o-mini`` if the caller
        has ``OPENAI_API_KEY`` set).
        """
        manager = crew_kwargs.get("manager_llm")
        if manager is not None:
            return manager
        agents = crew_kwargs.get("agents") or []
        for agent in agents:
            llm = getattr(agent, "llm", None)
            if llm is not None:
                return llm
        return None

    async def resolve_memory_llm_override(
        self, memory_config: "MemoryBackendConfig"
    ) -> Optional[Any]:
        """Build a fully-configured CrewAI ``LLM`` for the memory-analysis override.

        ``cognitive_config.memory_llm_model`` is a bare model key (e.g.
        ``databricks-claude-haiku-4-5``). Handing that string straight to CrewAI's
        ``Memory`` makes it construct an unconfigured ``LLM(model=<name>)`` with no
        provider prefix and no credentials, so litellm routes it to OpenAI and 401s
        on the placeholder key. Resolve it through ``LLMManager`` here so ``Memory``
        receives a ready-to-call instance (provider prefix + api_key + api_base).

        Returns ``None`` when no override is set — callers then fall back to the
        crew's own configured LLM instance via ``_resolve_memory_llm``.
        """
        cognitive = getattr(memory_config, "cognitive_config", None)
        model_name = getattr(cognitive, "memory_llm_model", None) if cognitive else None
        if not model_name:
            return None

        group_id = self.config.get("group_id") or "default"
        try:
            from src.core.llm_manager import LLMManager

            llm = await LLMManager.configure_crewai_llm(model_name, group_id)
            logger.info(
                "Resolved memory LLM override '%s' to a configured instance (group=%s)",
                model_name,
                group_id,
            )
            return llm
        except Exception as exc:  # noqa: BLE001 — degrade to the crew LLM, never break the run
            logger.warning(
                "Could not build memory LLM override '%s' (%s); "
                "falling back to the crew's LLM instance",
                model_name,
                exc,
            )
            return None

    def attach_memory_trace_context(
        self,
        crew: Any,
        memory_backend_config: Optional[Dict[str, Any]],
        crew_kwargs: Dict[str, Any],
    ) -> None:
        """
        Attach execution trace context to memory storages

        Args:
            crew: Crew instance
            memory_backend_config: Memory backend configuration
            crew_kwargs: Crew keyword arguments
        """
        try:
            exec_id = (
                self.config.get("execution_id")
                or self.config.get("run_name")
                or self.config.get("inputs", {}).get("run_name")
            )
            grp_id = self.config.get("group_id") or "default"

            trace_ctx = {
                "job_id": exec_id,
                "group_context": {"primary_group_id": grp_id},
                "execution_id": exec_id,
            }

            def set_trace_ctx(mem_obj):
                try:
                    if not mem_obj:
                        return
                    # Unified Memory exposes the backend as either ``_storage``
                    # (private attr) or ``storage`` (public field). Tag both
                    # if they're present.
                    for attr in ("_storage", "storage"):
                        storage = getattr(mem_obj, attr, None)
                        if storage is not None and hasattr(storage, "trace_context"):
                            setattr(storage, "trace_context", trace_ctx)
                    if hasattr(mem_obj, "trace_context"):
                        setattr(mem_obj, "trace_context", trace_ctx)
                except Exception:
                    pass

            # Unified Memory on CrewAI 1.10+ is stored on ``crew._memory``.
            # Legacy attrs are retained defensively in case a caller is
            # passing a pre-1.10 crew instance.
            set_trace_ctx(getattr(crew, "_memory", None))
            set_trace_ctx(getattr(crew, "_short_term_memory", None))
            set_trace_ctx(getattr(crew, "_long_term_memory", None))
            set_trace_ctx(getattr(crew, "_entity_memory", None))

            # NOTE: Direct memory tracing removed - memory events are now captured
            # by the CrewAI event bus in logging_callbacks.py with proper agent attribution.
            # The _patch_default_memory_tracing method was causing duplicate events appearing
            # under "Memory[...]" as separate agents instead of being grouped with the correct task/agent.

        except Exception as trace_ctx_err:
            logger.debug(f"Could not attach memory trace context: {trace_ctx_err}")

    def attach_tools_trace_context(
        self, crew: Any, crew_kwargs: Dict[str, Any]
    ) -> None:
        """
        Attach execution trace context to all tools in the crew.

        This enables tools to emit custom trace events (like llm_call) that appear
        in the UI technical trace.

        Args:
            crew: Crew instance with agents
            crew_kwargs: Crew keyword arguments containing execution context
        """
        try:
            exec_id = (
                self.config.get("execution_id")
                or self.config.get("run_name")
                or self.config.get("inputs", {}).get("run_name")
            )
            grp_id = self.config.get("group_id") or "default"

            trace_ctx = {
                "job_id": exec_id,
                "group_context": {"primary_group_id": grp_id},
                "execution_id": exec_id,
            }

            # Iterate through all agents and their tools
            tools_attached = 0

            if hasattr(crew, "agents") and crew.agents:
                for agent in crew.agents:
                    if hasattr(agent, "tools") and agent.tools:
                        agent_role = getattr(agent, "role", "Unknown Agent")
                        for tool in agent.tools:
                            try:
                                # Attach trace_context to the tool instance
                                if hasattr(
                                    tool, "__dict__"
                                ):  # Check if tool can have attributes set
                                    setattr(tool, "trace_context", trace_ctx)
                                    tools_attached += 1
                                    logger.debug(
                                        f"Attached trace context to tool '{getattr(tool, 'name', type(tool).__name__)}' on agent '{agent_role}'"
                                    )
                            except Exception as tool_err:
                                logger.debug(
                                    f"Could not attach trace context to tool on agent: {tool_err}"
                                )

            # Also attach trace_context to tools on tasks (CrewAI allows tools on tasks)
            if hasattr(crew, "tasks") and crew.tasks:
                for task in crew.tasks:
                    if hasattr(task, "tools") and task.tools:
                        task_desc = getattr(task, "description", "Unknown Task")[:50]
                        for tool in task.tools:
                            try:
                                # Attach trace_context to the tool instance
                                if hasattr(
                                    tool, "__dict__"
                                ):  # Check if tool can have attributes set
                                    setattr(tool, "trace_context", trace_ctx)
                                    tools_attached += 1
                                    logger.info(
                                        f"Attached trace context to tool '{getattr(tool, 'name', type(tool).__name__)}' on task '{task_desc}...'"
                                    )
                            except Exception as tool_err:
                                logger.debug(
                                    f"Could not attach trace context to tool on task: {tool_err}"
                                )

            if tools_attached > 0:
                logger.info(f"Attached trace context to {tools_attached} tool(s) total")
            else:
                logger.debug(
                    "No tools found in crew, skipping tool trace context attachment"
                )

        except Exception as trace_ctx_err:
            logger.debug(f"Could not attach tools trace context: {trace_ctx_err}")

    def set_crew_reference_on_memory(self, crew: Any) -> None:
        """Propagate the crew reference onto the unified memory storage.

        The unified Memory class keeps its storage on ``_storage`` (private
        attr). Any Kasal storage backend that exposes ``crew`` (for agent/LLM
        attribution) will receive the crew instance here.
        """
        try:
            memory_obj = getattr(crew, "_memory", None)
            if not memory_obj:
                return

            storage = (
                getattr(memory_obj, "_storage", None)
                or getattr(memory_obj, "storage", None)
            )
            if storage is None:
                return

            if hasattr(storage, "crew"):
                storage.crew = crew
                logger.info("Set crew reference on unified memory storage")

            if hasattr(storage, "set_agent_context") and getattr(crew, "agents", None):
                storage.set_agent_context(crew.agents[0])
                logger.info(
                    "Set agent context on unified memory storage: %s",
                    getattr(crew.agents[0], "role", "Unknown"),
                )

        except Exception as context_error:
            logger.warning(f"Failed to set context on memory backend: {context_error}")

    def restore_storage_directory(self) -> None:
        """Restore original storage directory environment variable"""
        if self._original_storage_dir is not None:
            os.environ["CREWAI_STORAGE_DIR"] = self._original_storage_dir
        elif "CREWAI_STORAGE_DIR" in os.environ:
            del os.environ["CREWAI_STORAGE_DIR"]
